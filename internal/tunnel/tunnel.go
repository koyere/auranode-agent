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
	// windowSize: créditos iniciales (bytes en vuelo) por stream y dirección. El emisor
	// no lee de su TCP local más allá de este margen sin crédito; el receptor concede
	// crédito (tunnel_window) a medida que drena. Aplica backpressure real al origen.
	windowSize = 256 * 1024
	// inboxBuffer: chunks en vuelo por stream. Con el control de flujo los bytes en
	// vuelo quedan acotados a windowSize, así que este buffer no se llena en operación
	// normal; el reset por overflow queda como red de seguridad (NO se descartan bytes).
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

	// Control de flujo (créditos para el lector local→peer). fc se fija ANTES de
	// arrancar el relay (en Ack/OpenDest) y sólo se lee después: sin carrera.
	fc         bool // ambos extremos soportan créditos → gating activo
	creditMu   sync.Mutex
	creditCond *sync.Cond
	sendCredit int
}

// initFlow inicializa el control de flujo del stream con la ventana completa.
func (s *stream) initFlow() {
	s.creditCond = sync.NewCond(&s.creditMu)
	s.sendCredit = windowSize
}

// takeCredit bloquea hasta que haya crédito (o el stream termine) y reserva hasta
// `max` bytes. Devuelve 0 si el stream está cerrado.
func (s *stream) takeCredit(max int) int {
	s.creditMu.Lock()
	defer s.creditMu.Unlock()
	for s.sendCredit <= 0 {
		select {
		case <-s.done:
			return 0
		default:
		}
		s.creditCond.Wait()
	}
	select {
	case <-s.done:
		return 0
	default:
	}
	n := s.sendCredit
	if n > max {
		n = max
	}
	s.sendCredit -= n
	return n
}

// addCredit suma crédito concedido por el receptor del extremo opuesto y despierta
// al lector si estaba esperando.
func (s *stream) addCredit(n int) {
	s.creditMu.Lock()
	s.sendCredit += n
	s.creditMu.Unlock()
	s.creditCond.Broadcast()
}

// abort cierra el stream de forma dura (error de conexión, reset, shutdown): aborta
// el escritor sin drenar el inbox. Para un cierre ordenado del peer usar closeInbox.
func (s *stream) abort() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.conn != nil {
			s.conn.Close()
		}
		// Despertar al lector que pudiera estar esperando crédito.
		if s.creditCond != nil {
			s.creditCond.Broadcast()
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
		s.initFlow()
		m.register(s)

		// Pedir al backend que abra el extremo destino. FC=true anuncia soporte de
		// control de flujo; sólo se activará si el dest también lo anuncia en el ack.
		m.emit(proto.TunnelOpen{
			Envelope: proto.Envelope{Type: proto.TypeTunnelOpen, Timestamp: time.Now().Unix()},
			TunnelID: tunnelID, StreamID: streamID, FC: true,
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

// OpenDest hace dial a host:port y, si tiene éxito, arranca el relay. peerFC indica si
// el source soporta control de flujo (anunciado en tunnel_open); el gating se activa
// sólo si ambos extremos lo soportan.
func (m *Manager) OpenDest(tunnelID, streamID, host string, port int, peerFC bool) {
	s := &stream{
		tunnelID: tunnelID,
		streamID: streamID,
		inbox:    make(chan []byte, inboxBuffer),
		done:     make(chan struct{}),
		fc:       peerFC, // este extremo soporta FC; activo si el source también
	}
	s.initFlow()
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
		// FC: peerFC hace eco de la capacidad negociada. Si el flag no llegó (backend
		// antiguo que lo descarta), ambos extremos quedan sin gating (fallback limpio).
		m.emit(proto.TunnelOpenAck{
			Envelope: proto.Envelope{Type: proto.TypeTunnelOpenAck, Timestamp: time.Now().Unix()},
			TunnelID: tunnelID, StreamID: streamID, OK: true, FC: peerFC,
		})
		m.relay(s)
	}()
}

// ─── Despacho de mensajes del backend ──────────────────────────────────────────

// Ack señala el resultado del dial en el extremo destino (rol source). peerFC indica
// si el dest soporta control de flujo (anunciado en el ack); se fija ANTES de cerrar
// `ready`, así el relay (que arranca tras `ready`) ya lo ve sin carrera.
func (m *Manager) Ack(streamID string, ok, peerFC bool) {
	m.mu.Lock()
	s := m.streams[streamID]
	m.mu.Unlock()
	if s == nil {
		return
	}
	if ok {
		s.fc = peerFC
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

// AddCredit aplica un crédito (tunnel_window) recibido del extremo opuesto: permite
// al lector local→peer enviar `bytes` más.
func (m *Manager) AddCredit(streamID string, bytes int) {
	m.mu.Lock()
	s := m.streams[streamID]
	m.mu.Unlock()
	if s == nil || bytes <= 0 {
		return
	}
	s.addCredit(bytes)
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
				// Conceder crédito al emisor del extremo opuesto: ya drenamos len(b).
				if s.fc {
					m.emit(proto.TunnelWindow{
						Envelope: proto.Envelope{Type: proto.TypeTunnelWindow, Timestamp: time.Now().Unix()},
						TunnelID: s.tunnelID, StreamID: s.streamID, Bytes: len(b),
					})
				}
			}
		}
	}()

	// Lector (local→peer): conn → tunnel_data. Si hay control de flujo (s.fc), antes de
	// leer espera crédito: si el receptor opuesto va lento, el crédito se agota y dejamos
	// de leer el TCP local → backpressure al origen. Sin fc (peer antiguo) lee libremente
	// (comportamiento previo). Al ver EOF señala el fin de ESTA dirección con tunnel_close
	// (no aborta: la dirección contraria puede seguir).
	buf := make([]byte, relayBufSize)
	for {
		budget := len(buf)
		if s.fc {
			budget = s.takeCredit(len(buf))
			if budget == 0 {
				return // stream cerrado mientras esperaba crédito
			}
		}
		n, err := s.conn.Read(buf[:budget])
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			m.emit(proto.TunnelData{
				Envelope: proto.Envelope{Type: proto.TypeTunnelData, Timestamp: time.Now().Unix()},
				TunnelID: s.tunnelID, StreamID: s.streamID,
				Data: base64.StdEncoding.EncodeToString(chunk),
			})
			// Devolver el crédito no usado (leímos n ≤ budget).
			if s.fc {
				if rem := budget - n; rem > 0 {
					s.addCredit(rem)
				}
			}
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
