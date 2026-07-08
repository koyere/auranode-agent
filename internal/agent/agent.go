// Package agent orchestrates the full lifecycle of the AuraNode agent.
package agent

import (
	"context"
	"encoding/base64"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/internal/buffer"
	"github.com/koyere/auranode-agent/internal/collector"
	agentcfg "github.com/koyere/auranode-agent/internal/config"
	"github.com/koyere/auranode-agent/internal/connection"
	"github.com/koyere/auranode-agent/internal/executor"
	agentfs "github.com/koyere/auranode-agent/internal/fs"
	"github.com/koyere/auranode-agent/internal/logs"
	"github.com/koyere/auranode-agent/internal/migration"
	"github.com/koyere/auranode-agent/internal/privileged"
	"github.com/koyere/auranode-agent/internal/rules"
	"github.com/koyere/auranode-agent/internal/database"
	"github.com/koyere/auranode-agent/internal/terminal"
	"github.com/koyere/auranode-agent/internal/tunnel"
	"github.com/koyere/auranode-agent/internal/updater"
	"github.com/koyere/auranode-agent/pkg/proto"
)

// Agent is the root of the process.
type Agent struct {
	cfg        *agentcfg.Config
	log        *zap.Logger
	collector  *collector.Collector
	logs       *logs.Collector
	buf        *buffer.Buffer
	engine     *rules.Engine
	tunnels    *tunnel.Manager
	terminals  *terminal.Manager
	databases  *database.Manager
	migrations *migration.Manager
	updater    *updater.Updater
	ws         *connection.Client

	mu                sync.Mutex
	metricsInterval   time.Duration
	heartbeatInterval time.Duration
	sendFn            func(any) error // se asigna al conectar
}

func New(cfg *agentcfg.Config, log *zap.Logger) (*Agent, error) {
	// Open the offline buffer (creates the directory if needed)
	if err := os.MkdirAll(dirOf(cfg.DBPath), 0755); err != nil {
		log.Warn("buffer: could not create directory, using /tmp",
			zap.String("path", cfg.DBPath),
			zap.Error(err),
		)
		cfg.DBPath = "/tmp/auranode-buffer.db"
	}

	buf, err := buffer.Open(cfg.DBPath, log)
	if err != nil {
		log.Warn("buffer: could not open bbolt, running without buffer",
			zap.String("path", cfg.DBPath),
			zap.Error(err),
		)
		buf = nil
	}

	a := &Agent{
		cfg:               cfg,
		log:               log,
		collector:         collector.New(log),
		logs:              logs.New(log),
		buf:               buf,
		metricsInterval:   time.Duration(cfg.MetricsIntervalSeconds) * time.Second,
		heartbeatInterval: time.Duration(cfg.HeartbeatIntervalSeconds) * time.Second,
	}

	a.engine = rules.New(func(rf proto.RuleFired) {
		a.mu.Lock()
		fn := a.sendFn
		a.mu.Unlock()
		if fn != nil {
			fn(rf) //nolint:errcheck
		}
	}, log)

	a.tunnels = tunnel.New(log)
	a.terminals = terminal.NewManager(log)
	a.databases = database.NewManager(log)
	a.migrations = migration.New(log, dirOf(cfg.DBPath))

	// Updater check-and-notify: tells the backend when a newer version is available.
	a.updater = updater.New(cfg.Version, log, func(current, latest string) {
		a.sendUpdateAvailable(current, latest)
	})

	a.ws = connection.New(cfg.BackendURL, cfg.AgentToken, a, log)
	return a, nil
}

func (a *Agent) Run(ctx context.Context) {
	a.updater.Start(ctx)
	a.ws.Run(ctx)
}

// sendUpdateAvailable notifies the backend that a newer version is available (if
// there is an active connection; otherwise it is re-sent on the next OnConnect).
func (a *Agent) sendUpdateAvailable(current, latest string) {
	a.mu.Lock()
	fn := a.sendFn
	a.mu.Unlock()
	if fn == nil {
		return
	}
	fn(proto.UpdateAvailable{ //nolint:errcheck
		Envelope:       proto.Envelope{Type: proto.TypeUpdateAvailable, Timestamp: time.Now().Unix()},
		CurrentVersion: current,
		LatestVersion:  latest,
	})
}

// ─── connection.MessageHandler ────────────────────────────────────────────────

func (a *Agent) OnConnect(ctx context.Context, sendFn func(any) error) {
	a.mu.Lock()
	a.sendFn = sendFn
	a.mu.Unlock()
	a.tunnels.SetSend(sendFn)
	a.terminals.SetSend(sendFn)
	a.databases.SetSend(sendFn)
	a.migrations.SetSend(sendFn)
	a.logs.SetSend(sendFn)
	a.logs.Start(ctx)

	// 1. Send agent_info
	info := a.collector.SystemInfo(a.cfg.Version)
	if err := sendFn(info); err != nil {
		a.log.Warn("handshake: error enviando agent_info", zap.Error(err))
		return
	}

	// 1b. If a newer version was already detected, notify the backend again.
	if latest := a.updater.LatestKnown(); latest != "" {
		sendFn(proto.UpdateAvailable{ //nolint:errcheck
			Envelope:       proto.Envelope{Type: proto.TypeUpdateAvailable, Timestamp: time.Now().Unix()},
			CurrentVersion: a.cfg.Version,
			LatestVersion:  latest,
		})
	}

	// 2. Flush the offline buffer if there are entries
	go a.drainBuffer(ctx, sendFn)

	// 3. Start the metrics and heartbeat loops
	go a.metricsLoop(ctx, sendFn)
	go a.heartbeatLoop(ctx, sendFn)
}

func (a *Agent) OnDisconnect() {
	a.mu.Lock()
	a.sendFn = nil
	a.mu.Unlock()
	// Without a backend connection tunnels cannot relay: close everything.
	a.tunnels.SetSend(nil)
	a.tunnels.Shutdown()
	a.terminals.SetSend(nil)
	a.terminals.Shutdown()
	a.databases.SetSend(nil)
	a.logs.SetSend(nil)
	a.logs.Stop()
	// Migrations also cannot continue without the backend; they will be left INTERRUPTED.
	a.migrations.SetSend(nil)
	a.migrations.Shutdown()
	a.log.Info("agent: desconectado del backend")
}

func (a *Agent) OnConfig(cfg proto.AgentConfig) {
	a.log.Info("agent: config recibida",
		zap.Int("metrics_interval", cfg.MetricsIntervalSeconds),
		zap.Int("heartbeat_interval", cfg.HeartbeatIntervalSeconds),
	)
	a.mu.Lock()
	if cfg.MetricsIntervalSeconds > 0 {
		a.metricsInterval = time.Duration(cfg.MetricsIntervalSeconds) * time.Second
	}
	if cfg.HeartbeatIntervalSeconds > 0 {
		a.heartbeatInterval = time.Duration(cfg.HeartbeatIntervalSeconds) * time.Second
	}
	a.mu.Unlock()

	a.engine.Sync(cfg.Rules)
	a.logs.Configure(cfg.LogServices)
}

func (a *Agent) OnExec(cmd proto.ExecCommand) {
	a.log.Info("exec: comando recibido",
		zap.String("id", cmd.CommandID),
		zap.Bool("async", cmd.Async),
	)

	a.mu.Lock()
	sendFn := a.sendFn
	a.mu.Unlock()
	if sendFn == nil {
		return
	}

	// ACK inmediato
	sendFn(proto.ExecAck{ //nolint:errcheck
		Envelope:  proto.Envelope{Type: proto.TypeExecAck, Timestamp: time.Now().Unix()},
		CommandID: cmd.CommandID,
	})

	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(cmd.HardTimeoutSeconds+5)*time.Second)
		defer cancel()

		res := executor.Run(ctx, cmd.CommandID, cmd.Command, cmd.HardTimeoutSeconds)

		a.mu.Lock()
		fn := a.sendFn
		a.mu.Unlock()
		if fn == nil {
			return
		}
		fn(proto.ExecResult{ //nolint:errcheck
			Envelope:   proto.Envelope{Type: proto.TypeExecResult, Timestamp: time.Now().Unix()},
			CommandID:  res.CommandID,
			ExitStatus: res.ExitStatus,
			Output:     res.Stdout,
			Stderr:     res.Stderr,
			DurationMS: res.DurationMS,
			Async:      cmd.Async,
		})
	}

	if cmd.Async {
		go run()
	} else {
		go run() // always a goroutine so the reader is not blocked
	}
}

// OnSysAction ejecuta una acción privilegiada de la whitelist reenviándola al
// helper root local. El agente principal no escala: solo hace de puente. El helper
// revalida la acción. Siempre en goroutine para no bloquear el lector.
func (a *Agent) OnSysAction(msg proto.SysAction) {
	a.log.Info("sys_action: acción privilegiada recibida",
		zap.String("id", msg.ActionID), zap.String("action", msg.Action))

	go func() {
		resp := privileged.Execute(privileged.Request{Action: msg.Action, Args: msg.Args})

		a.mu.Lock()
		fn := a.sendFn
		a.mu.Unlock()
		if fn == nil {
			return
		}
		fn(proto.SysActionResult{ //nolint:errcheck
			Envelope:   proto.Envelope{Type: proto.TypeSysActionResult, Timestamp: time.Now().Unix()},
			ActionID:   msg.ActionID,
			OK:         resp.OK,
			Rejected:   resp.Rejected,
			ExitStatus: resp.ExitStatus,
			Stdout:     resp.Stdout,
			Stderr:     resp.Stderr,
			Error:      resp.Error,
			DurationMS: resp.DurationMS,
		})
	}()
}

func (a *Agent) OnRuleSync(rs proto.RuleSync) {
	a.log.Info("rules: sync recibido", zap.Int("count", len(rs.Rules)))
	a.engine.Sync(rs.Rules)
}

func (a *Agent) OnFS(req proto.FSRequest) {
	a.log.Info("fs: request received",
		zap.String("id", req.RequestID),
		zap.String("op", req.Op),
		zap.String("path", req.Path),
	)

	// Run in a goroutine so the reader is not blocked; the I/O operations
	// can take a while (large directories, multi-MB reads).
	go func() {
		resp := agentfs.Handle(req)
		resp.Envelope = proto.Envelope{Type: proto.TypeFSResponse, Timestamp: time.Now().Unix()}

		a.mu.Lock()
		fn := a.sendFn
		a.mu.Unlock()
		if fn != nil {
			fn(resp) //nolint:errcheck
		}
	}()
}

// ─── Port forwarding (tunnels) ─────────────────────────────────────────────────

func (a *Agent) OnTunnelStart(msg proto.TunnelStart) {
	a.log.Info("tunnel: start",
		zap.String("tunnel_id", msg.TunnelID), zap.Int("local_port", msg.LocalPort),
		zap.String("bind_addr", msg.BindAddr))
	a.tunnels.StartListener(msg.TunnelID, msg.LocalPort, msg.BindAddr)
}

func (a *Agent) OnPTYStart(msg proto.PTYStart) {
	a.log.Info("pty: start", zap.String("session_id", msg.SessionID))
	a.terminals.Start(msg)
}

func (a *Agent) OnPTYData(msg proto.PTYData)   { a.terminals.Data(msg) }
func (a *Agent) OnPTYResize(msg proto.PTYResize) { a.terminals.Resize(msg) }
// OnDB atiende una operación de base de datos (Parte 3 · D1). El Manager responde
// asíncronamente por el WS con un db_response.
func (a *Agent) OnDB(req proto.DBRequest) { a.databases.Handle(req) }

func (a *Agent) OnPTYClose(msg proto.PTYClose) {
	a.log.Info("pty: close", zap.String("session_id", msg.SessionID))
	a.terminals.Close(msg.SessionID)
}

func (a *Agent) OnTunnelStop(msg proto.TunnelStop) {
	a.log.Info("tunnel: stop", zap.String("tunnel_id", msg.TunnelID))
	a.tunnels.StopListener(msg.TunnelID)
}

func (a *Agent) OnTunnelOpen(msg proto.TunnelOpen) {
	// Dest role: the backend requests opening the connection to the target service.
	a.log.Debug("tunnel: open recibido (dest)", zap.String("stream_id", msg.StreamID),
		zap.String("host", msg.Host), zap.Int("port", msg.Port))
	a.tunnels.OpenDest(msg.TunnelID, msg.StreamID, msg.Host, msg.Port, msg.FC)
}

func (a *Agent) OnTunnelOpenAck(msg proto.TunnelOpenAck) {
	// Source role: result of the dial at the destination.
	if !msg.OK {
		a.log.Warn("tunnel: dial to target rejected",
			zap.String("stream_id", msg.StreamID), zap.String("error", msg.Error))
	}
	a.tunnels.Ack(msg.StreamID, msg.OK, msg.FC)
}

func (a *Agent) OnTunnelData(msg proto.TunnelData) {
	b, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil {
		return
	}
	a.tunnels.Data(msg.StreamID, b)
}

func (a *Agent) OnTunnelClose(msg proto.TunnelClose) {
	a.tunnels.CloseStream(msg.StreamID)
}

func (a *Agent) OnTunnelWindow(msg proto.TunnelWindow) {
	a.tunnels.AddCredit(msg.StreamID, msg.Bytes)
}

func (a *Agent) OnMigration(msgType string, raw []byte) {
	a.migrations.Handle(msgType, raw)
}

// ─── loops internos ───────────────────────────────────────────────────────────

func (a *Agent) metricsLoop(ctx context.Context, sendFn func(any) error) {
	a.mu.Lock()
	interval := a.metricsInterval
	a.mu.Unlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Re-read the interval in case it changed via OnConfig
		a.mu.Lock()
		newInterval := a.metricsInterval
		a.mu.Unlock()
		if newInterval != interval {
			interval = newInterval
			ticker.Reset(interval)
		}

		m := a.collector.Collect()

		// Evaluate rules
		go a.engine.Evaluate(ctx, m)

		// Try to send; if it fails, store in the buffer
		if err := sendFn(m); err != nil {
			if a.buf != nil {
				a.buf.Push(m)
			}
		}
	}
}

func (a *Agent) heartbeatLoop(ctx context.Context, sendFn func(any) error) {
	a.mu.Lock()
	interval := a.heartbeatInterval
	a.mu.Unlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		a.mu.Lock()
		newInterval := a.heartbeatInterval
		a.mu.Unlock()
		if newInterval != interval {
			interval = newInterval
			ticker.Reset(interval)
		}

		sendFn(proto.Heartbeat{ //nolint:errcheck
			Envelope: proto.Envelope{Type: proto.TypeHeartbeat, Timestamp: time.Now().Unix()},
		})
	}
}

func (a *Agent) drainBuffer(ctx context.Context, sendFn func(any) error) {
	if a.buf == nil {
		return
	}

	count := a.buf.Count()
	if count == 0 {
		return
	}

	a.log.Info("buffer: flushing offline metrics", zap.Int("count", count))

	entries := a.buf.Drain()
	if len(entries) == 0 {
		return
	}

	// Send as a batch
	batch := proto.MetricsBatch{
		Envelope: proto.Envelope{Type: proto.TypeMetricsBatch, Timestamp: time.Now().Unix()},
		Count:    len(entries),
		Entries:  entries,
	}

	if err := sendFn(batch); err != nil {
		a.log.Warn("buffer: error enviando batch offline", zap.Error(err))
		// Store back
		for _, m := range entries {
			a.buf.Push(m)
		}
	} else {
		a.log.Info("buffer: batch offline enviado", zap.Int("count", len(entries)))
	}
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
