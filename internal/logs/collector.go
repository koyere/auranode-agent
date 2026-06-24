// Package logs collects systemd journal entries (read-only) and streams them to
// the backend as log_stream. The agent runs unprivileged; to read the system
// journal it must belong to the `systemd-journal` group (granted by the
// installer). If journalctl is unavailable or access is denied, the collector
// simply produces no entries (not a fatal error).
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
	maxBatchLines = 200 // cap per flush to avoid flooding the backend
	backlogLines  = 30  // initial backlog at startup (immediate context in the panel)
)

type Collector struct {
	mu       sync.Mutex
	sendFn   func(any) error
	services map[string]bool // unit filter; empty = all
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

// Configure sets the service filter (list of units). Empty = collect everything.
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

// selfUnits are AuraNode's own systemd units: their logs are self-referential
// noise (WebSocket reconnections, etc.) that would skew server diagnostics, so
// they are never collected.
var selfUnits = map[string]bool{
	"auranode-agent":        true,
	"auranode-agent-helper": true,
}

func (c *Collector) allowed(service string) bool {
	base := strings.TrimSuffix(service, ".service")
	if selfUnits[base] {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.services) == 0 {
		return true
	}
	return c.services[service] || c.services[base]
}

// Start begins following the journal in a goroutine. Idempotent: it cancels a
// previous follow. It restarts if journalctl exits (e.g. rotation).
func (c *Collector) Start(parent context.Context) {
	if _, err := exec.LookPath("journalctl"); err != nil {
		c.log.Info("logs: journalctl unavailable, collector disabled")
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
		// journalctl exited (rotation/error): short wait and retry.
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

// follow runs `journalctl -f -o json` and emits what it reads until ctx is
// canceled or the process exits.
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

	// Accumulator grouped by service + periodic flush.
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

// journalEntry maps the fields of `journalctl -o json` we care about. journald
// emits values as strings (or byte arrays for binaries); we decode them
// tolerantly via jsonStr.
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

	// __REALTIME_TIMESTAMP comes in microseconds since epoch.
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
	default: // 5, 6 or unknown
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

// jsonStr decodes a journal field that may come as a JSON string or as an array
// of numbers (bytes, for non-UTF8 messages). Anything else → "".
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
