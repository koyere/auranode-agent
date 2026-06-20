// Package fs implementa las operaciones del gestor de archivos remoto (SFTP) del
// agente AuraNode. El backend envía una proto.FSRequest por WebSocket y el agente
// responde con una proto.FSResponse. Nunca se abre un puerto de archivos al exterior.
package fs

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// MaxReadBytes es el tope absoluto de bytes que el agente devuelve en una lectura
// o acepta en una escritura por petición (el WebSocket del backend limita el
// mensaje a unos pocos MB). Transferencias mayores requieren chunking (follow-up).
const MaxReadBytes = 6 * 1024 * 1024

// caches de resolución uid/gid → nombre para no golpear getpwuid en cada entrada.
// Protegidos por cacheMu: Handle se ejecuta en una goroutine por petición, así que
// múltiples listados/stat concurrentes los acceden a la vez (un map sin protección
// provocaría "concurrent map writes" y abortaría el agente).
var (
	cacheMu    sync.RWMutex
	userCache  = map[string]string{}
	groupCache = map[string]string{}
)

// Handle ejecuta una operación de archivos y devuelve la respuesta lista para enviar.
// Nunca entra en pánico: cualquier error del sistema se traduce a resp.Error.
func Handle(req proto.FSRequest) proto.FSResponse {
	resp := proto.FSResponse{RequestID: req.RequestID}

	// Toda ruta debe ser absoluta y limpia: evita ambigüedad y traversal relativo.
	path := filepath.Clean(req.Path)
	if !filepath.IsAbs(path) {
		resp.Error = "la ruta debe ser absoluta"
		return resp
	}

	var err error
	switch req.Op {
	case proto.FSOpList:
		err = opList(path, &resp)
	case proto.FSOpStat:
		var e *proto.FSEntry
		e, err = statEntry(path)
		resp.Stat = e
	case proto.FSOpRead:
		err = opRead(path, req.MaxBytes, &resp)
	case proto.FSOpWrite:
		err = opWrite(path, req.Content)
	case proto.FSOpMkdir:
		err = os.MkdirAll(path, 0755)
	case proto.FSOpRename:
		err = opRename(path, req.NewPath)
	case proto.FSOpDelete:
		err = os.RemoveAll(path)
	case proto.FSOpChmod:
		err = opChmod(path, req.Mode)
	case proto.FSOpChown:
		err = opChown(path, req.Owner, req.Group)
	default:
		err = fmt.Errorf("operación desconocida: %s", req.Op)
	}

	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	resp.OK = true
	return resp
}

func opList(dir string, resp *proto.FSResponse) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	out := make([]proto.FSEntry, 0, len(entries))
	for _, de := range entries {
		full := filepath.Join(dir, de.Name())
		e, err := statEntry(full)
		if err != nil {
			// Un archivo ilegible (p.ej. symlink roto) no debe abortar el listado.
			continue
		}
		out = append(out, *e)
	}
	// Directorios primero, luego por nombre — orden estable y predecible en la UI.
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir
		}
		return out[i].Name < out[j].Name
	})
	resp.Entries = out
	return nil
}

func statEntry(path string) (*proto.FSEntry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	e := &proto.FSEntry{
		Name:      filepath.Base(path),
		Path:      path,
		IsDir:     info.IsDir(),
		Size:      info.Size(),
		Mode:      info.Mode().Perm().String()[1:], // descarta el primer char de tipo
		ModeOctal: fmt.Sprintf("%04o", info.Mode().Perm()),
		ModTime:   info.ModTime().Unix(),
	}

	if info.Mode()&os.ModeSymlink != 0 {
		e.IsSymlink = true
		if target, err := os.Readlink(path); err == nil {
			e.LinkTarget = target
		}
		// Resolver si el symlink apunta a un directorio para el icono correcto.
		if ti, err := os.Stat(path); err == nil {
			e.IsDir = ti.IsDir()
			e.Size = ti.Size()
		}
	}

	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		e.Owner = lookupUser(st.Uid)
		e.Group = lookupGroup(st.Gid)
	}
	return e, nil
}

func opRead(path string, maxBytes int64, resp *proto.FSResponse) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("es un directorio")
	}

	limit := maxBytes
	if limit <= 0 || limit > MaxReadBytes {
		limit = MaxReadBytes
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Leemos limit+1 bytes: si llenamos el extra, el archivo excede el límite.
	buf := make([]byte, limit+1)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return err
	}
	if int64(n) > limit {
		n = int(limit)
		resp.Truncated = true
	}
	resp.Content = base64.StdEncoding.EncodeToString(buf[:n])
	return nil
}

func opWrite(path, contentB64 string) error {
	data, err := base64.StdEncoding.DecodeString(contentB64)
	if err != nil {
		return fmt.Errorf("contenido base64 inválido: %w", err)
	}
	if int64(len(data)) > MaxReadBytes {
		return fmt.Errorf("archivo demasiado grande (máx %d bytes)", MaxReadBytes)
	}
	// Preservar permisos si el archivo ya existe; si no, 0644.
	mode := os.FileMode(0644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	return os.WriteFile(path, data, mode)
}

func opRename(oldPath, newPath string) error {
	if newPath == "" {
		return fmt.Errorf("new_path es requerido")
	}
	np := filepath.Clean(newPath)
	if !filepath.IsAbs(np) {
		return fmt.Errorf("new_path debe ser absoluta")
	}
	return os.Rename(oldPath, np)
}

func opChmod(path, mode string) error {
	if mode == "" {
		return fmt.Errorf("mode es requerido")
	}
	parsed, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return fmt.Errorf("mode octal inválido: %s", mode)
	}
	return os.Chmod(path, os.FileMode(parsed))
}

func opChown(path, owner, group string) error {
	uid, gid := -1, -1
	if owner != "" {
		u, err := user.Lookup(owner)
		if err != nil {
			if n, e := strconv.Atoi(owner); e == nil {
				uid = n
			} else {
				return fmt.Errorf("usuario desconocido: %s", owner)
			}
		} else {
			uid, _ = strconv.Atoi(u.Uid)
		}
	}
	if group != "" {
		g, err := user.LookupGroup(group)
		if err != nil {
			if n, e := strconv.Atoi(group); e == nil {
				gid = n
			} else {
				return fmt.Errorf("grupo desconocido: %s", group)
			}
		} else {
			gid, _ = strconv.Atoi(g.Gid)
		}
	}
	if uid == -1 && gid == -1 {
		return fmt.Errorf("owner o group es requerido")
	}
	return os.Chown(path, uid, gid)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func lookupUser(uid uint32) string {
	key := strconv.FormatUint(uint64(uid), 10)
	cacheMu.RLock()
	v, ok := userCache[key]
	cacheMu.RUnlock()
	if ok {
		return v
	}
	name := key
	if u, err := user.LookupId(key); err == nil {
		name = u.Username
	}
	cacheMu.Lock()
	userCache[key] = name
	cacheMu.Unlock()
	return name
}

func lookupGroup(gid uint32) string {
	key := strconv.FormatUint(uint64(gid), 10)
	cacheMu.RLock()
	v, ok := groupCache[key]
	cacheMu.RUnlock()
	if ok {
		return v
	}
	name := key
	if g, err := user.LookupGroupId(key); err == nil {
		name = g.Name
	}
	cacheMu.Lock()
	groupCache[key] = name
	cacheMu.Unlock()
	return name
}
