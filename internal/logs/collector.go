// Package logs recolecta entradas del journal de systemd (solo lectura) y las
// transmite al backend como log_stream. El agente corre sin privilegios; para leer
// el journal del sistema necesita pertenecer al grupo `systemd-journal` (lo concede
// el instalador). Si journalctl no está disponible o no hay acceso, el colector
// simplemente no produce entradas (no es un error fatal).
package logs

import (
	"bufio"
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

const (
	flushInterval = 2 * time.Second
	maxBatchLines = 200 // tope por flush para no inundar el backend
	backlogLines  = 30  // backlog inicial al arrancar (contexto inmediato en el panel)
)

type Collector struct {
	mu       sync.Mutex
	sendFn   func(any) error
	services map[string]bool // filtro de unidades; vacío = todas
	log      *zap.Logger
	cancel   context.CancelFunc
}

func New(log *zap.Logger) *Collector {
	return &Collector{log: log, services: map[string]bool{}}
}

func (c *Collector) SetSend(fn func(any) error) {
	c.mu.Lock()
	c.sendFn = fn
	c.mu.Unlock()
}

// Configure fija el filtro de servicios (lista de unidades). Vacío = recolectar todo.
func (c *Collector) Configure(services []string) {
	c.mu.Lock()
	c.services = make(map[string]bool, len(services))
	for _, s := range services {
		c.services[strings.TrimSpace(s)] = true
	}
	c.mu.Unlock()
}

func (c *Collector) send(msg any) {
	c.mu.Lock()
	fn := c.sendFn
	c.mu.Unlock()
	if fn != nil {
		fn(msg) //nolint:errcheck
	}
}

func (c *Collector) allowed(service string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.services) == 0 {
		return true
	}
	return c.services[service] || c.services[strings.TrimSuffix(service, ".service")]
}

// Start arranca el seguimiento del journal en una goroutine. Idempotente: cancela un
// seguimiento previo. Se reinicia si journalctl termina (p. ej. rotación).
func (c *Collector) Start(parent context.Context) {
	if _, err := exec.LookPath("journalctl"); err != nil {
		c.log.Info("logs: journalctl no disponible, colector deshabilitado")
		return
	}
	c.Stop()
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
	go c.run(ctx)
}

func (c *Collector) Stop() {
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Collector) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		c.follow(ctx)
		// journalctl terminó (rotación/error): pequeña espera y reintento.
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

// follow ejecuta `journalctl -f -o json` y va emitiendo lo que lee hasta que ctx
// se cancela o el proceso termina.
func (c *Collector) follow(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "journalctl",
		"-f", "-o", "json", "--no-pager", "-n", strconv.Itoa(backlogLines))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.log.Warn("logs: stdout pipe", zap.Error(err))
		return
	}
	if err := cmd.Start(); err != nil {
		c.log.Warn("logs: no se pudo iniciar journalctl", zap.Error(err))
		return
	}
	defer func() { _ = cmd.Wait() }()

	// Acumulador agrupado por servicio + flush periódico.
	pending := map[string][]proto.LogLine{}
	var count int
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if count == 0 {
			return
		}
		for svc, lines := range pending {
			c.send(proto.LogStream{
				Envelope: proto.Envelope{Type: proto.TypeLogStream, Timestamp: time.Now().Unix()},
				Service:  svc,
				Lines:    lines,
			})
		}
		pending = map[string][]proto.LogLine{}
		count = 0
	}

	lines := make(chan journalEntry, 256)
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			var e journalEntry
			if json.Unmarshal(sc.Bytes(), &e) == nil {
				select {
				case lines <- e:
				case <-ctx.Done():
					return
				}
			}
		}
		close(lines)
	}()

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case e, ok := <-lines:
			if !ok {
				flush()
				return
			}
			svc, ll, ok := e.toLine()
			if !ok || !c.allowed(svc) {
				continue
			}
			pending[svc] = append(pending[svc], ll)
			count++
			if count >= maxBatchLines {
				flush()
			}
		}
	}
}

// journalEntry mapea los campos de `journalctl -o json` que nos interesan. journald
// emite los valores como string (o array de bytes para binarios); usamos json.RawMessage
// y los decodificamos de forma tolerante.
type journalEntry struct {
	Realtime   jsonStr `json:"__REALTIME_TIMESTAMP"`
	Priority   jsonStr `json:"PRIORITY"`
	Unit       jsonStr `json:"_SYSTEMD_UNIT"`
	Identifier jsonStr `json:"SYSLOG_IDENTIFIER"`
	Comm       jsonStr `json:"_COMM"`
	Message    jsonStr `json:"MESSAGE"`
}

func (e journalEntry) toLine() (string, proto.LogLine, bool) {
	msg := string(e.Message)
	if msg == "" {
		return "", proto.LogLine{}, false
	}
	service := firstNonEmpty(string(e.Identifier), strings.TrimSuffix(string(e.Unit), ".service"), string(e.Comm), "system")

	// __REALTIME_TIMESTAMP viene en microsegundos desde epoch.
	ts := time.Now().Unix()
	if us, err := strconv.ParseInt(string(e.Realtime), 10, 64); err == nil && us > 0 {
		ts = us / 1_000_000
	}

	return service, proto.LogLine{TS: ts, Level: priorityToLevel(string(e.Priority)), Message: msg}, true
}

func priorityToLevel(p string) string {
	switch p {
	case "0", "1", "2", "3":
		return "error"
	case "4":
		return "warn"
	case "7":
		return "debug"
	default: // 5, 6 o desconocido
		return "info"
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return "system"
}

// jsonStr decodifica un campo del journal que puede venir como string JSON o como
// array de números (bytes, para mensajes no-UTF8). Cualquier otro caso → "".
type jsonStr string

func (s *jsonStr) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || b[0] == 'n' { // null
		*s = ""
		return nil
	}
	if b[0] == '"' {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		*s = jsonStr(str)
		return nil
	}
	if b[0] == '[' { // array de bytes
		var nums []int
		if err := json.Unmarshal(b, &nums); err != nil {
			*s = ""
			return nil
		}
		buf := make([]byte, 0, len(nums))
		for _, n := range nums {
			if n >= 0 && n <= 255 {
				buf = append(buf, byte(n))
			}
		}
		*s = jsonStr(buf)
		return nil
	}
	*s = ""
	return nil
}
