package tunnel

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// relay connects two managers (source and dest) in memory, emulating the backend
// relay: it routes each message emitted by one end to the corresponding method of the
// other. For tunnel_open it injects the destination host/port (what in production the
// backend adds from the tunnel's route).
type relay struct {
	mu           sync.Mutex
	source, dest *Manager
	destHost     string
	destPort     int
}

func (r *relay) fromSource(msg any) error {
	switch m := msg.(type) {
	case proto.TunnelOpen:
		r.dest.OpenDest(m.TunnelID, m.StreamID, r.destHost, r.destPort, m.FC)
	case proto.TunnelData:
		b, _ := base64.StdEncoding.DecodeString(m.Data)
		r.dest.Data(m.StreamID, b)
	case proto.TunnelClose:
		r.dest.CloseStream(m.StreamID)
	case proto.TunnelWindow:
		r.dest.AddCredit(m.StreamID, m.Bytes)
	case proto.TunnelStatus:
		// no-op
	}
	return nil
}

func (r *relay) fromDest(msg any) error {
	switch m := msg.(type) {
	case proto.TunnelOpenAck:
		r.source.Ack(m.StreamID, m.OK, m.FC)
	case proto.TunnelData:
		b, _ := base64.StdEncoding.DecodeString(m.Data)
		r.source.Data(m.StreamID, b)
	case proto.TunnelClose:
		r.source.CloseStream(m.StreamID)
	case proto.TunnelWindow:
		r.source.AddCredit(m.StreamID, m.Bytes)
	}
	return nil
}

// startEcho starts a TCP server that echoes back everything it receives.
func startEcho(t *testing.T) (string, int, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { io.Copy(c, c); c.Close() }(c)
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, func() { ln.Close() }
}

func newWiredPair(t *testing.T, destHost string, destPort int) (*Manager, *relay) {
	t.Helper()
	log := zap.NewNop()
	src := New(log)
	dst := New(log)
	r := &relay{source: src, dest: dst, destHost: destHost, destPort: destPort}
	src.SetSend(r.fromSource)
	dst.SetSend(r.fromDest)
	return src, r
}

// freePort reserves a free local port for the source's listener.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	p := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return p
}

// TestRoundTripIntegrity verifies the full source→dest→echo→source path with a
// large payload (multi-chunk), checking byte-for-byte integrity. It covers the relay,
// the ack, the chunking and, above all, the orderly inbox drain on close (the
// bug that truncated the end of the stream).
func TestRoundTripIntegrity(t *testing.T) {
	host, port, stop := startEcho(t)
	defer stop()

	src, _ := newWiredPair(t, host, port)
	lport := freePort(t)
	src.StartListener("tun-1", lport, "")
	defer src.StopListener("tun-1")
	time.Sleep(50 * time.Millisecond) // let the listener start

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", itoa(lport)))
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	defer conn.Close()

	payload := make([]byte, 1<<20) // 1 MB → ~32 chunks
	rand.Read(payload)

	// Write everything and, when done, close the write side so the echo
	// returns EOF after the last byte.
	go func() {
		conn.Write(payload)
		conn.(*net.TCPConn).CloseWrite()
	}()

	got := make([]byte, 0, len(payload))
	buf := make([]byte, 64*1024)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		n, err := conn.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			break
		}
		if len(got) >= len(payload) {
			break
		}
	}

	if len(got) != len(payload) {
		t.Fatalf("longitud: esperado %d, recibido %d", len(payload), len(got))
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("corrupt payload: the round-trip did not preserve the bytes")
	}
}

// TestDestDialFailure verifies that, if the destination does not accept connections, the
// source's stream is closed (does not hang) via tunnel_open_ack with OK=false.
func TestDestDialFailure(t *testing.T) {
	// Destination port closed: we reserve one and release it.
	deadPort := freePort(t)

	src, _ := newWiredPair(t, "127.0.0.1", deadPort)
	lport := freePort(t)
	src.StartListener("tun-2", lport, "")
	defer src.StopListener("tun-2")
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", itoa(lport)))
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	defer conn.Close()

	// The local connection must close when the destination dial fails.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err == nil {
		t.Fatal("expected the local connection to close when the destination dial fails")
	}
}

// startSender starts a TCP server that, on accept, SENDS `payload` and closes (without
// reading). Models a unidirectional server→client flow to test backpressure.
func startSender(t *testing.T, payload []byte) (string, int, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("sender listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { c.Write(payload); c.Close() }(c)
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, func() { ln.Close() }
}

// TestSlowConsumerBackpressure verifies that a sustained slow consumer does NOT
// cause a stream reset (previously the inbox saturated and reset): credit-based flow
// control applies backpressure and delivers the full payload. Unidirectional flow
// (server→client) to isolate one direction's backpressure.
func TestSlowConsumerBackpressure(t *testing.T) {
	payload := make([]byte, 4<<20) // 4 MB ≫ window (256KB): forces many credit cycles
	rand.Read(payload)

	host, port, stop := startSender(t, payload)
	defer stop()

	src, _ := newWiredPair(t, host, port)
	lport := freePort(t)
	src.StartListener("tun-slow", lport, "")
	defer src.StopListener("tun-slow")
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", itoa(lport)))
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	defer conn.Close()

	// Slow consumer: reads in small chunks with pauses. Without backpressure the inbox
	// would saturate; with credits the sender throttles and nothing is lost.
	got := make([]byte, 0, len(payload))
	buf := make([]byte, 16*1024)
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	for {
		n, err := conn.Read(buf)
		got = append(got, buf[:n]...)
		if n > 0 {
			time.Sleep(1 * time.Millisecond) // slows down consumption
		}
		if err != nil {
			break
		}
		if len(got) >= len(payload) {
			break
		}
	}

	if len(got) != len(payload) {
		t.Fatalf("longitud: esperado %d, recibido %d", len(payload), len(got))
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("corrupt payload with a slow consumer: backpressure did not preserve the bytes")
	}
}

func itoa(p int) string { return strconv.Itoa(p) }
