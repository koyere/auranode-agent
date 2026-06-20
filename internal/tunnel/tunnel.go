// Package tunnel implementa el plano de datos de port forwarding en el agente:
// multiplexa conexiones TCP sobre el WebSocket del backend.
//
// Roles por túnel:
//   - source: abre un listener TCP local (tunnel_start) y, por cada conexión
//     aceptada, crea un stream (tunnel_open) que el backend relaya al dest.
//   - dest:   recibe tunnel_open, hace dial a host:port y responde tunnel_open_ack.
//
// Una vez establecido el stream, ambos extremos ejecutan el mismo relay
// bidireccional (conn↔WS) hasta que cualquiera de los dos lo cierra.
package tunnel

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

const (
	dialTimeout  = 10 * time.Second
	ackTimeout   = 15 * time.Second
	relayBufSize = 32 * 1024 // chunk de lectura TCP
	// inboxBuffer: chunks en vuelo por stream. Amplio para absorber ráfagas sin
	// resetear conexiones legítimas cuyo consumidor va al día (32KB*2048 ≈ 64MB de
	// holgura por stream). Si aun así se llena, se resetea el stream (NO se descartan
	// bytes). El control de flujo por ventanas queda como follow-up.
	inboxBuffer = 2048
)

// Manager gestiona listeners y streams del agente.
type Manager struct {
	log *zap.Logger

	mu        sync.Mutex
	sendFn    func(any) error
	listeners map[string]net.Listener // tunnelID → listener (rol source)
	streams   map[string]*stream      // streamID → stream
}

type stream struct {
	tunnelID       string
	streamID       string
	conn           net.Conn
	inbox          chan []byte
	done           chan struct{}
	ready          chan struct{} // source: se cierra al recibir ack OK
	failed         chan struct{} // source: se cierra al recibir ack con error
	closeOnce      sync.Once
	inboxCloseOnce sync.Once

	stateMu   sync.Mutex // protege readDone/writeDone
	readDone  bool       // local→peer terminó (lector vio EOF)
	writeDone bool       // peer→local terminó (inbox cerrado y drenado)
}

// abort cierra el stream de forma dura (error de conexión, reset, shutdown): aborta
// el escritor sin drenar el inbox. Para un cierre ordenado del peer usar closeInbox.
func (s *stream) abort() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.conn != nil {
			s.conn.Close()
		}
	})
}

// closeInbox señala EOF ordenado del peer: el escritor drena los chunks pendientes y
// recién entonces cierra la conexión local (evita perder el último tramo, p.ej. el
// cuerpo de una respuesta HTTP que llega junto con el tunnel_close). Data() y
// CloseStream() se invocan desde la misma goroutine lectora del WS, así que no hay
// envío concurrente al inbox mientras se cierra.
func (s *stream) closeInbox() {
	s.inboxCloseOnce.Do(func() {
		close(s.inbox)
	})
}

func New(log *zap.Logger) *Manager {
	return &Manager{
		log:       log,
		listeners: make(map[string]net.Listener),
		streams:   make(map[string]*stream),
	}
}

// SetSend asigna la función de envío de la conexión activa. nil al desconectar.
func (m *Manager) SetSend(fn func(any) error) {
	m.mu.Lock()
	m.sendFn = fn
	m.mu.Unlock()
}

// Shutdown cierra todos los listeners y streams (al perder la conexión al backend).
func (m *Manager) Shutdown() {
	m.mu.Lock()
	for id, ln := range m.listeners {
		ln.Close()
		delete(m.listeners, id)
	}
	streams := make([]*stream, 0, len(m.streams))
	for _, s := range m.streams {
		streams = append(streams, s)
	}
	m.streams = make(map[string]*stream)
	m.mu.Unlock()

	for _, s := range streams {
		s.abort()
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

// ─── Rol source: listener ──────────────────────────────────────────────────────

// StartListener abre un listener TCP en bindAddr:localPort y reporta el estado.
// bindAddr vacío equivale a 127.0.0.1 (loopback). Los túneles remote (Tipo 2) pasan
// 0.0.0.0 u otra interfaz para exponer el puerto al exterior del VPS.
func (m *Manager) StartListener(tunnelID string, localPort int, bindAddr string) {
	if bindAddr == "" {
		bindAddr = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", bindAddr, localPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		m.log.Warn("tunnel: no se pudo abrir listener",
			zap.String("tunnel_id", tunnelID), zap.String("addr", addr), zap.Error(err))
		m.emit(proto.TunnelStatus{
			Envelope: proto.Envelope{Type: proto.TypeTunnelStatus, Timestamp: time.Now().Unix()},
			TunnelID: tunnelID, Status: "error", Error: err.Error(),
		})
		return
	}

	m.mu.Lock()
	if old, ok := m.listeners[tunnelID]; ok {
		old.Close()
	}
	m.listeners[tunnelID] = ln
	m.mu.Unlock()

	m.log.Info("tunnel: listener activo",
		zap.String("tunnel_id", tunnelID), zap.String("addr", addr))
	m.emit(proto.TunnelStatus{
		Envelope: proto.Envelope{Type: proto.TypeTunnelStatus, Timestamp: time.Now().Unix()},
		TunnelID: tunnelID, Status: "active",
	})

	go m.acceptLoop(tunnelID, ln)
}

func (m *Manager) acceptLoop(tunnelID string, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener cerrado
		}
		m.log.Debug("tunnel: conexión local aceptada", zap.String("tunnel_id", tunnelID))
		streamID := randomID()
		s := &stream{
			tunnelID: tunnelID,
			streamID: streamID,
			conn:     conn,
			inbox:    make(chan []byte, inboxBuffer),
			done:     make(chan struct{}),
			ready:    make(chan struct{}),
			failed:   make(chan struct{}),
		}
		m.register(s)

		// Pedir al backend que abra el extremo destino.
		m.emit(proto.TunnelOpen{
			Envelope: proto.Envelope{Type: proto.TypeTunnelOpen, Timestamp: time.Now().Unix()},
			TunnelID: tunnelID, StreamID: streamID,
		})

		go m.sourceStream(s)
	}
}

// sourceStream espera el ack del destino antes de empezar a relayar la conexión local.
func (m *Manager) sourceStream(s *stream) {
	select {
	case <-s.ready:
		m.relay(s)
	case <-s.failed:
		s.abort()
		m.unregister(s.streamID)
	case <-time.After(ackTimeout):
		m.log.Warn("tunnel: timeout esperando ack del destino",
			zap.String("stream_id", s.streamID))
		m.emit(proto.TunnelClose{
			Envelope: proto.Envelope{Type: proto.TypeTunnelClose, Timestamp: time.Now().Unix()},
			TunnelID: s.tunnelID, StreamID: s.streamID, Error: "ack timeout",
		})
		s.abort()
		m.unregister(s.streamID)
	case <-s.done:
		m.unregister(s.streamID)
	}
}

// StopListener cierra el listener de un túnel y todos sus streams (ambos roles).
func (m *Manager) StopListener(tunnelID string) {
	m.mu.Lock()
	if ln, ok := m.listeners[tunnelID]; ok {
		ln.Close()
		delete(m.listeners, tunnelID)
	}
	var victims []*stream
	for id, s := range m.streams {
		if s.tunnelID == tunnelID {
			victims = append(victims, s)
			delete(m.streams, id)
		}
	}
	m.mu.Unlock()

	for _, s := range victims {
		s.abort()
	}
}

// ─── Rol dest: dial ────────────────────────────────────────────────────────────

// OpenDest hace dial a host:port y, si tiene éxito, arranca el relay.
func (m *Manager) OpenDest(tunnelID, streamID, host string, port int) {
	s := &stream{
		tunnelID: tunnelID,
		streamID: streamID,
		inbox:    make(chan []byte, inboxBuffer),
		done:     make(chan struct{}),
	}
	m.register(s)

	go func() {
		addr := fmt.Sprintf("%s:%d", host, port)
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err != nil {
			m.log.Warn("tunnel: dial destino falló",
				zap.String("stream_id", streamID), zap.String("addr", addr), zap.Error(err))
			m.emit(proto.TunnelOpenAck{
				Envelope: proto.Envelope{Type: proto.TypeTunnelOpenAck, Timestamp: time.Now().Unix()},
				TunnelID: tunnelID, StreamID: streamID, OK: false, Error: err.Error(),
			})
			m.unregister(streamID)
			return
		}
		s.conn = conn
		m.emit(proto.TunnelOpenAck{
			Envelope: proto.Envelope{Type: proto.TypeTunnelOpenAck, Timestamp: time.Now().Unix()},
			TunnelID: tunnelID, StreamID: streamID, OK: true,
		})
		m.relay(s)
	}()
}

// ─── Despacho de mensajes del backend ──────────────────────────────────────────

// Ack señala el resultado del dial en el extremo destino (rol source).
func (m *Manager) Ack(streamID string, ok bool) {
	m.mu.Lock()
	s := m.streams[streamID]
	m.mu.Unlock()
	if s == nil {
		return
	}
	if ok {
		safeClose(s.ready)
	} else {
		safeClose(s.failed)
	}
}

// Data entrega un chunk recibido del extremo opuesto al stream local. NUNCA bloquea
// la goroutine lectora del WS (bloquearla causaría head-of-line/deadlock, ya que el
// propio tunnel_close que desbloquearía viaja por ese mismo lector) ni descarta bytes
// a mitad de stream (corrompería el TCP): si el buffer está lleno, resetea el stream
// completo (cierre limpio, reintentable).
func (m *Manager) Data(streamID string, b []byte) {
	m.mu.Lock()
	s := m.streams[streamID]
	m.mu.Unlock()
	if s == nil {
		return
	}
	select {
	case <-s.done:
	case s.inbox <- b:
	default:
		m.log.Warn("tunnel: inbox saturado, reseteando stream", zap.String("stream_id", streamID))
		m.emit(proto.TunnelClose{
			Envelope: proto.Envelope{Type: proto.TypeTunnelClose, Timestamp: time.Now().Unix()},
			TunnelID: s.tunnelID, StreamID: streamID, Error: "inbox overflow",
		})
		s.abort()
		m.unregister(streamID)
	}
}

// CloseStream cierra un stream por orden del extremo opuesto (EOF ordenado): drena lo
// pendiente antes de cerrar la conexión local para no truncar el último tramo.
func (m *Manager) CloseStream(streamID string) {
	m.mu.Lock()
	s := m.streams[streamID]
	delete(m.streams, streamID)
	m.mu.Unlock()
	if s != nil {
		s.closeInbox()
	}
}

// ─── Relay común ───────────────────────────────────────────────────────────────

// markDone registra el fin de una dirección del stream (read = local→peer,
// !read = peer→local). Cuando AMBAS direcciones han terminado, cierra del todo la
// conexión y desregistra el stream. Soporta half-close: un extremo puede dejar de
// enviar mientras sigue recibiendo.
func (m *Manager) markDone(s *stream, read bool) {
	s.stateMu.Lock()
	if read {
		s.readDone = true
	} else {
		s.writeDone = true
	}
	both := s.readDone && s.writeDone
	s.stateMu.Unlock()
	if both {
		s.abort()
		m.unregister(s.streamID)
	}
}

func (m *Manager) relay(s *stream) {
	// Escritor (peer→local): vuelca inbox → conn. Ante EOF ordenado del peer (inbox
	// cerrado) drena todo lo pendiente y cierra SOLO el lado de escritura (half-close),
	// dejando que la dirección contraria siga viva hasta que el peer local termine.
	go func() {
		for {
			select {
			case <-s.done:
				return
			case b, ok := <-s.inbox:
				if !ok {
					if tcp, isTCP := s.conn.(*net.TCPConn); isTCP {
						tcp.CloseWrite()
					} else if s.conn != nil {
						s.conn.Close()
					}
					m.markDone(s, false)
					return
				}
				if _, err := s.conn.Write(b); err != nil {
					s.abort()
					m.unregister(s.streamID)
					return
				}
			}
		}
	}()

	// Lector (local→peer): conn → tunnel_data. Al ver EOF señala el fin de ESTA
	// dirección con tunnel_close (no aborta: la dirección contraria puede seguir).
	buf := make([]byte, relayBufSize)
	for {
		n, err := s.conn.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			m.emit(proto.TunnelData{
				Envelope: proto.Envelope{Type: proto.TypeTunnelData, Timestamp: time.Now().Unix()},
				TunnelID: s.tunnelID, StreamID: s.streamID,
				Data: base64.StdEncoding.EncodeToString(chunk),
			})
		}
		if err != nil {
			m.emit(proto.TunnelClose{
				Envelope: proto.Envelope{Type: proto.TypeTunnelClose, Timestamp: time.Now().Unix()},
				TunnelID: s.tunnelID, StreamID: s.streamID,
			})
			m.markDone(s, true)
			return
		}
	}
}

// ─── Registro de streams ───────────────────────────────────────────────────────

func (m *Manager) register(s *stream) {
	m.mu.Lock()
	m.streams[s.streamID] = s
	m.mu.Unlock()
}

func (m *Manager) unregister(streamID string) {
	m.mu.Lock()
	delete(m.streams, streamID)
	m.mu.Unlock()
}

// ─── Utilidades ────────────────────────────────────────────────────────────────

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// safeClose cierra un channel una sola vez sin entrar en pánico si ya estaba cerrado.
func safeClose(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}
