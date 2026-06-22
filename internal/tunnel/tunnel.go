// Package tunnel implements the port-forwarding data plane in the agent:
// it multiplexes TCP connections over the backend WebSocket.
//
// Roles per tunnel:
//   - source: opens a local TCP listener (tunnel_start) and, for each accepted
//     connection, creates a stream (tunnel_open) that the backend relays to the dest.
//   - dest:   receives tunnel_open, dials host:port and replies tunnel_open_ack.
//
// Once the stream is established, both ends run the same bidirectional
// relay (conn↔WS) until either of them closes it.
package tunnel

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

const (
	dialTimeout  = 10 * time.Second
	ackTimeout   = 15 * time.Second
	relayBufSize = 32 * 1024 // TCP read chunk
	// windowSize: initial credits (in-flight bytes) per stream and direction. The sender
	// does not read from its local TCP beyond this margin without credit; the receiver
	// grants credit (tunnel_window) as it drains. Applies real backpressure to the origin.
	windowSize = 256 * 1024
	// inboxBuffer: in-flight chunks per stream. With flow control the in-flight bytes
	// are bounded by windowSize, so this buffer does not fill up in normal operation;
	// the overflow reset remains a safety net (bytes are NEVER dropped).
	inboxBuffer = 2048
)

// Manager manages the agent's listeners and streams.
type Manager struct {
	log *zap.Logger

	mu        sync.Mutex
	sendFn    func(any) error
	listeners map[string]net.Listener // tunnelID → listener (source role)
	streams   map[string]*stream      // streamID → stream
}

type stream struct {
	tunnelID       string
	streamID       string
	conn           net.Conn
	inbox          chan []byte
	done           chan struct{}
	ready          chan struct{} // source: closed on receiving an OK ack
	failed         chan struct{} // source: closed on receiving an error ack
	closeOnce      sync.Once
	inboxCloseOnce sync.Once
	inboxClosed    atomic.Bool // prevents send-on-closed-channel in Data after closeInbox

	stateMu   sync.Mutex // guards readDone/writeDone
	readDone  bool       // local→peer finished (reader saw EOF)
	writeDone bool       // peer→local finished (inbox closed and drained)

	// Flow control (credits for the local→peer reader). fc is set BEFORE
	// starting the relay (in Ack/OpenDest) and only read afterwards: no race.
	fc         bool // both ends support credits → gating active
	creditMu   sync.Mutex
	creditCond *sync.Cond
	sendCredit int
}

// initFlow initializes the stream's flow control with the full window.
func (s *stream) initFlow() {
	s.creditCond = sync.NewCond(&s.creditMu)
	s.sendCredit = windowSize
}

// takeCredit blocks until there is credit (or the stream ends) and reserves up to
// `max` bytes. Returns 0 if the stream is closed.
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

// addCredit adds credit granted by the receiver on the opposite end and wakes
// the reader if it was waiting.
func (s *stream) addCredit(n int) {
	s.creditMu.Lock()
	s.sendCredit += n
	s.creditMu.Unlock()
	s.creditCond.Broadcast()
}

// abort closes the stream hard (connection error, reset, shutdown): it aborts
// the writer without draining the inbox. For an orderly peer close use closeInbox.
func (s *stream) abort() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.conn != nil {
			s.conn.Close()
		}
		// Wake the reader that might be waiting for credit.
		if s.creditCond != nil {
			s.creditCond.Broadcast()
		}
	})
}

// closeInbox signals an orderly peer EOF: the writer drains the pending chunks and
// only then closes the local connection (avoids losing the last segment, e.g. the
// body of an HTTP response arriving together with tunnel_close). Data() and
// CloseStream() are invoked from the same WS reader goroutine, so there is no
// concurrent send to the inbox while it closes.
func (s *stream) closeInbox() {
	s.inboxCloseOnce.Do(func() {
		s.inboxClosed.Store(true)
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

// SetSend sets the send function of the active connection. nil on disconnect.
func (m *Manager) SetSend(fn func(any) error) {
	m.mu.Lock()
	m.sendFn = fn
	m.mu.Unlock()
}

// Shutdown closes all listeners and streams (when the backend connection is lost).
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

// ─── Source role: listener ─────────────────────────────────────────────────────

// StartListener opens a TCP listener on bindAddr:localPort and reports the status.
// An empty bindAddr is equivalent to 127.0.0.1 (loopback). Remote tunnels (Type 2) pass
// 0.0.0.0 or another interface to expose the port outside the VPS.
func (m *Manager) StartListener(tunnelID string, localPort int, bindAddr string) {
	if bindAddr == "" {
		bindAddr = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", bindAddr, localPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		m.log.Warn("tunnel: could not open listener",
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

	m.log.Info("tunnel: listener active",
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
			return // listener closed
		}
		m.log.Debug("tunnel: local connection accepted", zap.String("tunnel_id", tunnelID))
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

		// Ask the backend to open the destination end. FC=true advertises flow-control
		// support; it will only be enabled if the dest also advertises it in the ack.
		m.emit(proto.TunnelOpen{
			Envelope: proto.Envelope{Type: proto.TypeTunnelOpen, Timestamp: time.Now().Unix()},
			TunnelID: tunnelID, StreamID: streamID, FC: true,
		})

		go m.sourceStream(s)
	}
}

// sourceStream waits for the destination's ack before starting to relay the local connection.
func (m *Manager) sourceStream(s *stream) {
	select {
	case <-s.ready:
		m.relay(s)
	case <-s.failed:
		s.abort()
		m.unregister(s.streamID)
	case <-time.After(ackTimeout):
		m.log.Warn("tunnel: timeout waiting for the destination ack",
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

// StopListener closes a tunnel's listener and all its streams (both roles).
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

// ─── Dest role: dial ───────────────────────────────────────────────────────────

// OpenDest dials host:port and, on success, starts the relay. peerFC indicates whether
// the source supports flow control (advertised in tunnel_open); gating is enabled
// only if both ends support it.
func (m *Manager) OpenDest(tunnelID, streamID, host string, port int, peerFC bool) {
	s := &stream{
		tunnelID: tunnelID,
		streamID: streamID,
		inbox:    make(chan []byte, inboxBuffer),
		done:     make(chan struct{}),
		fc:       peerFC, // this end supports FC; active if the source does too
	}
	s.initFlow()
	m.register(s)

	go func() {
		addr := fmt.Sprintf("%s:%d", host, port)
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err != nil {
			m.log.Warn("tunnel: dial to destination failed",
				zap.String("stream_id", streamID), zap.String("addr", addr), zap.Error(err))
			m.emit(proto.TunnelOpenAck{
				Envelope: proto.Envelope{Type: proto.TypeTunnelOpenAck, Timestamp: time.Now().Unix()},
				TunnelID: tunnelID, StreamID: streamID, OK: false, Error: err.Error(),
			})
			m.unregister(streamID)
			return
		}
		s.conn = conn
		// FC: peerFC echoes the negotiated capability. If the flag did not arrive (an old
		// backend that drops it), both ends end up without gating (clean fallback).
		m.emit(proto.TunnelOpenAck{
			Envelope: proto.Envelope{Type: proto.TypeTunnelOpenAck, Timestamp: time.Now().Unix()},
			TunnelID: tunnelID, StreamID: streamID, OK: true, FC: peerFC,
		})
		m.relay(s)
	}()
}

// ─── Dispatch of backend messages ──────────────────────────────────────────────

// Ack signals the result of the dial on the destination end (source role). peerFC
// indicates whether the dest supports flow control (advertised in the ack); it is set
// BEFORE closing `ready`, so the relay (which starts after `ready`) already sees it without a race.
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

// Data delivers a chunk received from the opposite end to the local stream. It NEVER
// blocks the WS reader goroutine (blocking it would cause head-of-line/deadlock, since
// the very tunnel_close that would unblock it travels on that same reader) nor drops
// bytes mid-stream (it would corrupt the TCP): if the buffer is full, it resets the
// whole stream (clean, retryable close).
func (m *Manager) Data(streamID string, b []byte) {
	m.mu.Lock()
	s := m.streams[streamID]
	m.mu.Unlock()
	if s == nil || s.inboxClosed.Load() {
		return // stream closed in this direction: do not send (avoids panic)
	}
	select {
	case <-s.done:
	case s.inbox <- b:
	default:
		m.log.Warn("tunnel: inbox overflow, resetting stream", zap.String("stream_id", streamID))
		m.emit(proto.TunnelClose{
			Envelope: proto.Envelope{Type: proto.TypeTunnelClose, Timestamp: time.Now().Unix()},
			TunnelID: s.tunnelID, StreamID: streamID, Error: "inbox overflow",
		})
		s.abort()
		m.unregister(streamID)
	}
}

// AddCredit applies a credit (tunnel_window) received from the opposite end: it lets
// the local→peer reader send `bytes` more.
func (m *Manager) AddCredit(streamID string, bytes int) {
	m.mu.Lock()
	s := m.streams[streamID]
	m.mu.Unlock()
	if s == nil || bytes <= 0 {
		return
	}
	s.addCredit(bytes)
}

// CloseStream closes the peer→local direction of a stream (orderly peer EOF):
// it drains what is pending and does a half-close. It does NOT remove the stream from
// the map: the local→peer direction (reader) may still be active and needs to receive
// credits (tunnel_window) for that direction. The stream is removed via markDone when
// BOTH directions end.
func (m *Manager) CloseStream(streamID string) {
	m.mu.Lock()
	s := m.streams[streamID]
	m.mu.Unlock()
	if s != nil {
		s.closeInbox()
	}
}

// ─── Common relay ──────────────────────────────────────────────────────────────

// markDone records the end of one stream direction (read = local→peer,
// !read = peer→local). When BOTH directions have finished, it fully closes the
// connection and unregisters the stream. It supports half-close: one end can stop
// sending while still receiving.
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
	// Writer (peer→local): flushes inbox → conn. On an orderly peer EOF (inbox
	// closed) it drains everything pending and closes ONLY the write side (half-close),
	// leaving the opposite direction alive until the local peer finishes.
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
				// Grant credit to the sender on the opposite end: we already drained len(b).
				if s.fc {
					m.emit(proto.TunnelWindow{
						Envelope: proto.Envelope{Type: proto.TypeTunnelWindow, Timestamp: time.Now().Unix()},
						TunnelID: s.tunnelID, StreamID: s.streamID, Bytes: len(b),
					})
				}
			}
		}
	}()

	// Reader (local→peer): conn → tunnel_data. If flow control is on (s.fc), before
	// reading it waits for credit: if the opposite receiver is slow, the credit runs out and we
	// stop reading the local TCP → backpressure to the origin. Without fc (old peer) it reads freely
	// (previous behavior). On EOF it signals the end of THIS direction with tunnel_close
	// (it does not abort: the opposite direction may continue).
	buf := make([]byte, relayBufSize)
	for {
		budget := len(buf)
		if s.fc {
			budget = s.takeCredit(len(buf))
			if budget == 0 {
				return // stream closed while waiting for credit
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
			// Return the unused credit (we read n ≤ budget).
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

// ─── Stream registry ───────────────────────────────────────────────────────────

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

// ─── Utilities ─────────────────────────────────────────────────────────────────

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// safeClose closes a channel exactly once without panicking if it was already closed.
func safeClose(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}
