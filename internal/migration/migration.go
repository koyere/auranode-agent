// Package migration implementa el plano de datos de las migraciones Tipo B
// (directorio) en el agente, modo relay: el backend coordina por WebSocket y reenvía
// los mensajes de datos entre los dos agentes.
//
// Roles por migración:
//   - source: estima el tamaño (migration_estimate_req), y al recibir migration_start
//     recorre el directorio y envía cada archivo en chunks con control de flujo por
//     ventana (espera migration_window_ack) e integridad (CRC32 por chunk, SHA-256 por
//     archivo verificado por el destino con migration_file_ack).
//   - dest: al recibir migration_prepare comprueba espacio y carga el manifest de
//     reanudación; recibe los archivos, los escribe, verifica integridad, actualiza el
//     manifest y emite los acks.
package migration

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

const (
	progressInterval = 5 * time.Second
	maxFileRetries   = 3
	windowAckEvery   = 1 << 20 // emite window_ack cada ~1MB escritos
)

// Manager gestiona las sesiones de migración del agente (puede ser source de una y
// dest de otra simultáneamente).
type Manager struct {
	log      *zap.Logger
	stateDir string // base para los manifests de reanudación (rol dest)

	mu     sync.Mutex
	sendFn func(any) error
	srcs   map[string]*srcSession
	dsts   map[string]*dstSession
}

func New(log *zap.Logger, stateDir string) *Manager {
	return &Manager{
		log:      log,
		stateDir: stateDir,
		srcs:     make(map[string]*srcSession),
		dsts:     make(map[string]*dstSession),
	}
}

// SetSend asigna la función de envío de la conexión activa (nil al desconectar).
func (m *Manager) SetSend(fn func(any) error) {
	m.mu.Lock()
	m.sendFn = fn
	m.mu.Unlock()
}

// Shutdown cancela todas las sesiones activas (al perder la conexión al backend).
func (m *Manager) Shutdown() {
	m.mu.Lock()
	srcs := m.srcs
	dsts := m.dsts
	m.srcs = make(map[string]*srcSession)
	m.dsts = make(map[string]*dstSession)
	m.mu.Unlock()
	for _, s := range srcs {
		s.cancel()
	}
	for _, d := range dsts {
		d.close()
	}
}

func (m *Manager) emit(msg any) {
	m.mu.Lock()
	fn := m.sendFn
	m.mu.Unlock()
	if fn != nil {
		fn(msg) //nolint:errcheck
	}
}

// Handle despacha un mensaje migration_* recibido del backend.
func (m *Manager) Handle(msgType string, raw []byte) {
	var msg proto.MigrationMsg
	if json.Unmarshal(raw, &msg) != nil || msg.MigrationID == "" {
		return
	}
	switch msgType {
	case proto.TypeMigrationEstimateReq:
		go m.estimate(msg)
	case proto.TypeMigrationStart:
		go m.startSource(msg)
	case proto.TypeMigrationPrepare:
		m.prepare(msg)
	case proto.TypeMigrationCancel:
		m.cancel(msg.MigrationID)

	// Rol source recibe del dest (relay):
	case proto.TypeMigrationFileAck:
		m.srcFileAck(msg)
	case proto.TypeMigrationWindowAck:
		m.srcWindowAck(msg)

	// Rol dest recibe del source (relay) — en orden, en la goroutine lectora:
	case proto.TypeMigrationFile:
		m.dstFile(msg)
	case proto.TypeMigrationChunk:
		m.dstChunk(msg)
	case proto.TypeMigrationFileDone:
		m.dstFileDone(msg)
	}
}

func (m *Manager) cancel(id string) {
	m.mu.Lock()
	s := m.srcs[id]
	d := m.dsts[id]
	delete(m.srcs, id)
	delete(m.dsts, id)
	m.mu.Unlock()
	if s != nil {
		s.cancel()
	}
	if d != nil {
		d.close()
	}
}

// ─── Estimación (rol source, pre-check) ────────────────────────────────────────

func (m *Manager) estimate(msg proto.MigrationMsg) {
	res := proto.MigrationMsg{
		Envelope:    proto.Envelope{Type: proto.TypeMigrationEstimateRes, Timestamp: time.Now().Unix()},
		MigrationID: msg.MigrationID,
	}
	var totalBytes int64
	var totalFiles int
	err := walkFiles(msg.SourcePath, msg.ExcludePaths, func(_ string, info os.FileInfo) error {
		totalBytes += info.Size()
		totalFiles++
		return nil
	})
	if err != nil {
		res.Code = "TRANSFER_SOURCE_FILE_MISSING"
		res.Message = fmt.Sprintf("No se pudo leer el origen: %v", err)
	} else {
		res.TotalBytes = totalBytes
		res.TotalFiles = totalFiles
	}
	m.emit(res)
}

// ─── Rol source: transferencia ─────────────────────────────────────────────────

type srcSession struct {
	id        string
	done      chan struct{}
	closeOnce sync.Once

	fileAck   chan bool  // dest verificó el archivo en curso
	windowAck chan int64 // bytes acumulados confirmados por el dest (control de flujo)
}

func (s *srcSession) cancel() { s.closeOnce.Do(func() { close(s.done) }) }

func (m *Manager) startSource(msg proto.MigrationMsg) {
	s := &srcSession{
		id:        msg.MigrationID,
		done:      make(chan struct{}),
		fileAck:   make(chan bool, 1),
		windowAck: make(chan int64, 64),
	}
	m.mu.Lock()
	if old := m.srcs[msg.MigrationID]; old != nil {
		old.cancel()
	}
	m.srcs[msg.MigrationID] = s
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if m.srcs[msg.MigrationID] == s {
			delete(m.srcs, msg.MigrationID)
		}
		m.mu.Unlock()
	}()

	chunkSize := msg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 1 << 20
	}
	window := msg.WindowBytes
	if window <= 0 {
		window = 8 << 20
	}

	// Conjunto de reanudación: archivos ya completados en el destino (path→size+mtime).
	completed := make(map[string]proto.MigrationFileInfo, len(msg.Completed))
	for _, f := range msg.Completed {
		completed[f.Path] = f
	}

	// Listado determinista de archivos.
	type fileEntry struct {
		abs  string
		rel  string
		info os.FileInfo
	}
	var files []fileEntry
	walkErr := walkFiles(msg.SourcePath, msg.ExcludePaths, func(abs string, info os.FileInfo) error {
		rel := relPath(msg.SourcePath, abs)
		files = append(files, fileEntry{abs, rel, info})
		return nil
	})
	if walkErr != nil {
		m.fail(msg.MigrationID, "TRANSFER_SOURCE_FILE_MISSING", walkErr.Error())
		return
	}

	var bytesTotal int64
	for _, f := range files {
		bytesTotal += f.info.Size()
	}

	var (
		bytesTransferred int64
		filesCompleted   int
		warnings         []proto.MigrationWarning
		fileID           uint32
	)

	// Reporte de progreso periódico.
	stopProgress := make(chan struct{})
	go m.progressLoop(s, msg.MigrationID, &bytesTransferred, bytesTotal, &filesCompleted, len(files), stopProgress)
	defer close(stopProgress)

	for _, f := range files {
		select {
		case <-s.done:
			return // cancelada
		default:
		}

		// Reanudación: saltar si ya está completo (mismo size+mtime).
		if c, ok := completed[f.rel]; ok && c.Size == f.info.Size() && c.Mtime == f.info.ModTime().Unix() {
			bytesTransferred += f.info.Size()
			filesCompleted++
			continue
		}

		fileID++
		ok, warn, err := m.sendFile(s, msg.MigrationID, f.abs, f.rel, f.info, fileID, chunkSize, window, &bytesTransferred)
		if err != nil {
			m.fail(msg.MigrationID, "TRANSFER_NETWORK_INTERRUPTED", err.Error())
			return
		}
		if warn != nil {
			warnings = append(warnings, *warn)
		}
		if ok {
			filesCompleted++
		}
	}

	status := "completed"
	if len(warnings) > 0 {
		status = "completed_with_warnings"
	}
	m.emit(proto.MigrationMsg{
		Envelope:         proto.Envelope{Type: proto.TypeMigrationDone, Timestamp: time.Now().Unix()},
		MigrationID:      msg.MigrationID,
		Status:           status,
		Warnings:         warnings,
		BytesTransferred: bytesTransferred,
		FilesCompleted:   filesCompleted,
	})
}

// sendFile envía un archivo completo con control de flujo y reintentos. Devuelve
// (ok, warning, errFatal). ok=false con warning si el archivo cambió/desapareció.
func (m *Manager) sendFile(s *srcSession, migID, abs, rel string, info os.FileInfo, fileID uint32,
	chunkSize int, window int64, bytesTransferred *int64) (bool, *proto.MigrationWarning, error) {

	for attempt := 0; attempt < maxFileRetries; attempt++ {
		// Vaciar acks colgados de un intento previo.
		drain(s.fileAck)
		drainInt(s.windowAck)

		f, err := os.Open(abs)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return false, &proto.MigrationWarning{Code: "TRANSFER_SOURCE_FILE_MISSING", File: rel,
					Message: "El archivo desapareció durante la transferencia."}, nil
			}
			return false, nil, err
		}

		// SHA-256 del archivo (primera pasada).
		sum, serrHash := fileSHA256(f)
		if serrHash != nil {
			f.Close()
			return false, nil, serrHash
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return false, nil, err
		}

		// Cabecera del archivo.
		m.emit(proto.MigrationMsg{
			Envelope:    proto.Envelope{Type: proto.TypeMigrationFile, Timestamp: time.Now().Unix()},
			MigrationID: migID,
			FileID:      fileID,
			File: &proto.MigrationFileInfo{
				Path: rel, Size: info.Size(), Mode: uint32(info.Mode().Perm()),
				Mtime: info.ModTime().Unix(), Sha256: sum,
			},
		})

		startBytes := *bytesTransferred
		sentErr := m.streamChunks(s, migID, f, fileID, chunkSize, window, startBytes, bytesTransferred)
		f.Close()
		if sentErr != nil {
			return false, nil, sentErr
		}

		// Fin de archivo → esperar verificación del dest.
		m.emit(proto.MigrationMsg{
			Envelope:    proto.Envelope{Type: proto.TypeMigrationFileDone, Timestamp: time.Now().Unix()},
			MigrationID: migID, FileID: fileID,
		})

		select {
		case <-s.done:
			return false, nil, errors.New("cancelada")
		case ok := <-s.fileAck:
			if ok {
				return true, nil, nil
			}
			// Verificación falló: reintentar el archivo (rebobinar el contador).
			*bytesTransferred = startBytes
			m.log.Warn("migration: archivo rechazado por el destino, reintentando",
				zap.String("file", rel), zap.Int("attempt", attempt+1))
		case <-time.After(2 * time.Minute):
			return false, nil, errors.New("timeout esperando verificación del destino")
		}
	}
	return false, &proto.MigrationWarning{Code: "TRANSFER_CHUNK_MAX_RETRIES", File: rel,
		Message: "El archivo falló la verificación 3 veces."}, nil
}

// streamChunks envía los chunks de un archivo respetando la ventana de control de flujo.
func (m *Manager) streamChunks(s *srcSession, migID string, f *os.File, fileID uint32,
	chunkSize int, window int64, startBytes int64, bytesTransferred *int64) error {

	buf := make([]byte, chunkSize)
	var offset int64
	var acked int64 // bytes confirmados por el dest para ESTE archivo

	for {
		// Control de flujo: no superar la ventana de bytes sin confirmar.
		for offset-acked >= window {
			select {
			case <-s.done:
				return errors.New("cancelada")
			case a := <-s.windowAck:
				if a > acked {
					acked = a
				}
			case <-time.After(2 * time.Minute):
				return errors.New("timeout de control de flujo")
			}
		}

		n, err := f.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			m.emit(proto.MigrationMsg{
				Envelope:    proto.Envelope{Type: proto.TypeMigrationChunk, Timestamp: time.Now().Unix()},
				MigrationID: migID, FileID: fileID, Offset: offset,
				Data:  base64.StdEncoding.EncodeToString(chunk),
				CRC32: crc32.ChecksumIEEE(chunk),
			})
			offset += int64(n)
			*bytesTransferred = startBytes + offset
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// Consumir acks pendientes sin bloquear (libera ventana).
		for {
			select {
			case a := <-s.windowAck:
				if a > acked {
					acked = a
				}
				continue
			default:
			}
			break
		}
	}
}

func (m *Manager) srcFileAck(msg proto.MigrationMsg) {
	m.mu.Lock()
	s := m.srcs[msg.MigrationID]
	m.mu.Unlock()
	if s == nil {
		return
	}
	select {
	case s.fileAck <- msg.OK:
	default:
	}
}

func (m *Manager) srcWindowAck(msg proto.MigrationMsg) {
	m.mu.Lock()
	s := m.srcs[msg.MigrationID]
	m.mu.Unlock()
	if s == nil {
		return
	}
	select {
	case s.windowAck <- msg.AckedBytes:
	default:
	}
}

func (m *Manager) progressLoop(s *srcSession, migID string, bytesTransferred *int64, bytesTotal int64,
	filesCompleted *int, filesTotal int, stop chan struct{}) {
	t := time.NewTicker(progressInterval)
	defer t.Stop()
	var lastBytes int64
	last := time.Now()
	for {
		select {
		case <-s.done:
			return
		case <-stop:
			return
		case now := <-t.C:
			cur := *bytesTransferred
			elapsed := now.Sub(last).Seconds()
			var speed int64
			if elapsed > 0 {
				speed = int64(float64(cur-lastBytes) / elapsed)
			}
			lastBytes, last = cur, now
			m.emit(proto.MigrationMsg{
				Envelope:         proto.Envelope{Type: proto.TypeMigrationProgress, Timestamp: now.Unix()},
				MigrationID:      migID,
				BytesTransferred: cur,
				FilesCompleted:   *filesCompleted,
				SpeedBytesPerSec: speed,
			})
		}
	}
}

func (m *Manager) fail(migID, code, message string) {
	m.emit(proto.MigrationMsg{
		Envelope:    proto.Envelope{Type: proto.TypeMigrationFailed, Timestamp: time.Now().Unix()},
		MigrationID: migID, Code: code, Message: message,
	})
}

// ─── Rol dest: recepción ───────────────────────────────────────────────────────

type dstSession struct {
	id           string
	destPath     string
	manifestPath string
	manifestFile *os.File // log append-only (JSONL) para el manifest de reanudación

	mu        sync.Mutex
	completed map[string]proto.MigrationFileInfo

	// Archivo en curso.
	curFileID uint32
	curRel    string
	curPath   string
	curFile    *os.File
	curInfo    proto.MigrationFileInfo
	curHash    hash.Hash
	curWritten int64
	curAcked   int64
	curBroken  bool
}

func (d *dstSession) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.curFile != nil {
		d.curFile.Close()
		d.curFile = nil
	}
	if d.manifestFile != nil {
		d.manifestFile.Close()
		d.manifestFile = nil
	}
}

// recordCompleted persiste un archivo completado en el log append-only del manifest.
// O(1) por archivo (a diferencia de reescribir todo el mapa), y resistente a crashes.
func (d *dstSession) recordCompleted(info proto.MigrationFileInfo) {
	d.completed[info.Path] = info
	if d.manifestFile == nil {
		return
	}
	line, err := json.Marshal(info)
	if err != nil {
		return
	}
	d.manifestFile.Write(append(line, '\n')) //nolint:errcheck
	d.manifestFile.Sync()                     //nolint:errcheck
}

func (m *Manager) prepare(msg proto.MigrationMsg) {
	res := proto.MigrationMsg{
		Envelope:    proto.Envelope{Type: proto.TypeMigrationReceiverReady, Timestamp: time.Now().Unix()},
		MigrationID: msg.MigrationID,
	}

	if !filepath.IsAbs(msg.DestPath) || filepath.Clean(msg.DestPath) != msg.DestPath {
		res.Code = "TRANSFER_PERMISSION_DENIED"
		res.Message = "La ruta de destino no es válida."
		m.emit(res)
		return
	}
	if err := os.MkdirAll(msg.DestPath, 0o755); err != nil {
		res.Code = "TRANSFER_PERMISSION_DENIED"
		res.Message = fmt.Sprintf("No se pudo crear el destino: %v", err)
		m.emit(res)
		return
	}

	avail, err := availableBytes(msg.DestPath)
	if err == nil {
		res.AvailableBytes = avail
	}

	stateDir := filepath.Join(m.stateDir, "migrations", msg.MigrationID)
	_ = os.MkdirAll(stateDir, 0o700)
	manifestPath := filepath.Join(stateDir, "manifest.jsonl")
	completed := loadManifest(manifestPath)
	mf, _ := os.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)

	d := &dstSession{
		id:           msg.MigrationID,
		destPath:     msg.DestPath,
		manifestPath: manifestPath,
		manifestFile: mf,
		completed:    completed,
	}
	m.mu.Lock()
	if old := m.dsts[msg.MigrationID]; old != nil {
		old.close()
	}
	m.dsts[msg.MigrationID] = d
	m.mu.Unlock()

	// Manifest de reanudación → el origen saltará los archivos ya completados.
	for _, f := range completed {
		res.Completed = append(res.Completed, f)
	}
	m.emit(res)
}

func (m *Manager) dstSession(id string) *dstSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dsts[id]
}

func (m *Manager) dstFile(msg proto.MigrationMsg) {
	d := m.dstSession(msg.MigrationID)
	if d == nil || msg.File == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	// Cerrar cualquier archivo previo no finalizado.
	if d.curFile != nil {
		d.curFile.Close()
		d.curFile = nil
	}

	dest, ok := safeJoin(d.destPath, msg.File.Path)
	if !ok {
		d.curBroken = true
		m.log.Warn("migration: ruta de archivo insegura, ignorada", zap.String("path", msg.File.Path))
		return
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		d.curBroken = true
		m.log.Warn("migration: no se pudo crear directorio destino", zap.Error(err))
		return
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		d.curBroken = true
		m.log.Warn("migration: no se pudo abrir archivo destino", zap.Error(err))
		return
	}
	d.curFileID = msg.FileID
	d.curRel = msg.File.Path
	d.curPath = dest
	d.curFile = f
	d.curInfo = *msg.File
	d.curHash = sha256.New()
	d.curWritten = 0
	d.curAcked = 0
	d.curBroken = false
}

func (m *Manager) dstChunk(msg proto.MigrationMsg) {
	d := m.dstSession(msg.MigrationID)
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.curBroken || d.curFile == nil || msg.FileID != d.curFileID {
		return
	}

	raw, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil || crc32.ChecksumIEEE(raw) != msg.CRC32 {
		// Chunk corrupto: marcar el archivo como roto; fallará la verificación → reenvío.
		d.curBroken = true
		return
	}
	if _, err := d.curFile.Write(raw); err != nil {
		d.curBroken = true
		m.log.Warn("migration: error escribiendo chunk", zap.Error(err))
		return
	}
	d.curHash.Write(raw) //nolint:errcheck
	d.curWritten += int64(len(raw))

	// Control de flujo: confirmar bytes escritos cada ~windowAckEvery.
	if d.curWritten-d.curAcked >= windowAckEvery {
		d.curAcked = d.curWritten
		m.emit(proto.MigrationMsg{
			Envelope:    proto.Envelope{Type: proto.TypeMigrationWindowAck, Timestamp: time.Now().Unix()},
			MigrationID: msg.MigrationID, FileID: d.curFileID, AckedBytes: d.curWritten,
		})
	}
}

func (m *Manager) dstFileDone(msg proto.MigrationMsg) {
	d := m.dstSession(msg.MigrationID)
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if msg.FileID != d.curFileID {
		return
	}

	ok := !d.curBroken && d.curFile != nil
	if d.curFile != nil {
		d.curFile.Close()
	}

	if ok {
		sum := hex.EncodeToString(d.curHash.Sum(nil))
		if d.curWritten != d.curInfo.Size || (d.curInfo.Sha256 != "" && sum != d.curInfo.Sha256) {
			ok = false
		}
	}

	if ok {
		// Restaurar permisos y mtime; registrar en el manifest.
		if d.curInfo.Mode != 0 {
			os.Chmod(d.curPath, os.FileMode(d.curInfo.Mode)) //nolint:errcheck
		}
		if d.curInfo.Mtime != 0 {
			mt := time.Unix(d.curInfo.Mtime, 0)
			os.Chtimes(d.curPath, mt, mt) //nolint:errcheck
		}
		d.recordCompleted(proto.MigrationFileInfo{
			Path: d.curRel, Size: d.curInfo.Size, Mtime: d.curInfo.Mtime, Sha256: d.curInfo.Sha256,
		})
	} else {
		// Verificación fallida: borrar el archivo parcial para reintentar limpio.
		if d.curPath != "" {
			os.Remove(d.curPath) //nolint:errcheck
		}
	}

	d.curFile = nil
	d.curFileID = 0

	m.emit(proto.MigrationMsg{
		Envelope:    proto.Envelope{Type: proto.TypeMigrationFileAck, Timestamp: time.Now().Unix()},
		MigrationID: msg.MigrationID, FileID: msg.FileID, OK: ok,
	})
}

// ─── Utilidades ────────────────────────────────────────────────────────────────

// walkFiles recorre srcPath y llama fn por cada archivo regular no excluido.
func walkFiles(srcPath string, exclude []string, fn func(abs string, info os.FileInfo) error) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fn(srcPath, info)
	}
	return filepath.Walk(srcPath, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if isExcluded(p, exclude) {
			if fi.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !fi.Mode().IsRegular() {
			return nil // saltar dirs, symlinks, sockets, devices
		}
		return fn(p, fi)
	})
}

func isExcluded(p string, exclude []string) bool {
	for _, e := range exclude {
		if e == "" {
			continue
		}
		if p == e || strings.HasPrefix(p, strings.TrimRight(e, "/")+"/") {
			return true
		}
	}
	return false
}

func relPath(base, abs string) string {
	r, err := filepath.Rel(base, abs)
	if err != nil {
		return filepath.Base(abs)
	}
	return r
}

// safeJoin une base+rel y rechaza el resultado si escapa de base (path traversal
// desde un origen comprometido). filepath.Join normaliza el ".." antes de comprobar.
func safeJoin(base, rel string) (string, bool) {
	joined := filepath.Join(base, rel)
	if joined != base && !strings.HasPrefix(joined, base+string(os.PathSeparator)) {
		return "", false
	}
	return joined, true
}

func fileSHA256(f *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func availableBytes(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// loadManifest reconstruye el conjunto de archivos completados desde el log JSONL
// append-only (una entrada por línea; la última de cada path gana).
func loadManifest(path string) map[string]proto.MigrationFileInfo {
	out := make(map[string]proto.MigrationFileInfo)
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var fi proto.MigrationFileInfo
		if json.Unmarshal([]byte(line), &fi) == nil && fi.Path != "" {
			out[fi.Path] = fi
		}
	}
	return out
}

func drain(ch chan bool) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func drainInt(ch chan int64) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
