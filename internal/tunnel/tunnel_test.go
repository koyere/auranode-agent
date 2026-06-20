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

// relay conecta dos managers (source y dest) en memoria, emulando el relay del
// backend: enruta cada mensaje emitido por un extremo al método correspondiente del
// otro. Para tunnel_open inyecta el host/puerto destino (lo que en producción añade
// el backend a partir de la ruta del túnel).
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

// startEcho levanta un servidor TCP que devuelve todo lo que recibe.
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

// freePort reserva un puerto local libre para el listener del source.
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

// TestRoundTripIntegrity verifica el camino completo source→dest→echo→source con un
// payload grande (multi-chunk), comprobando integridad byte a byte. Cubre el relay,
// el ack, el chunking y, sobre todo, el drenaje ordenado del inbox al cerrar (el
// bug que truncaba el final del stream).
func TestRoundTripIntegrity(t *testing.T) {
	host, port, stop := startEcho(t)
	defer stop()

	src, _ := newWiredPair(t, host, port)
	lport := freePort(t)
	src.StartListener("tun-1", lport, "")
	defer src.StopListener("tun-1")
	time.Sleep(50 * time.Millisecond) // dejar que el listener arranque

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", itoa(lport)))
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	defer conn.Close()

	payload := make([]byte, 1<<20) // 1 MB → ~32 chunks
	rand.Read(payload)

	// Escribir todo y, al terminar, cerrar el lado de escritura para que el echo
	// devuelva EOF tras el último byte.
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
		t.Fatal("payload corrupto: el round-trip no preservó los bytes")
	}
}

// TestDestDialFailure verifica que, si el destino no acepta conexiones, el stream del
// source se cierra (no queda colgado) vía el tunnel_open_ack con OK=false.
func TestDestDialFailure(t *testing.T) {
	// Puerto destino cerrado: reservamos uno y lo liberamos.
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

	// La conexión local debe cerrarse al fallar el dial destino.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err == nil {
		t.Fatal("se esperaba cierre de la conexión local al fallar el dial destino")
	}
}

// startSender levanta un servidor TCP que, al aceptar, ENVÍA `payload` y cierra (sin
// leer). Modela un flujo unidireccional servidor→cliente para probar backpressure.
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

// TestSlowConsumerBackpressure verifica que un consumidor sostenidamente lento NO
// provoca reset del stream (antes el inbox se saturaba y se reseteaba): el control de
// flujo por créditos aplica backpressure y entrega el payload íntegro. Flujo
// unidireccional (servidor→cliente) para aislar el backpressure de una dirección.
func TestSlowConsumerBackpressure(t *testing.T) {
	payload := make([]byte, 4<<20) // 4 MB ≫ ventana (256KB): fuerza muchos ciclos de crédito
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

	// Consumidor lento: lee en trozos pequeños con pausas. Sin backpressure el inbox
	// se saturaría; con créditos el emisor se frena y nada se pierde.
	got := make([]byte, 0, len(payload))
	buf := make([]byte, 16*1024)
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	for {
		n, err := conn.Read(buf)
		got = append(got, buf[:n]...)
		if n > 0 {
			time.Sleep(1 * time.Millisecond) // ralentiza el consumo
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
		t.Fatal("payload corrupto con consumidor lento: backpressure no preservó los bytes")
	}
}

func itoa(p int) string { return strconv.Itoa(p) }
