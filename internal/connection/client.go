// Package connection manages the WebSocket connection to the backend with exponential reconnection.
package connection

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

const (
	maxBackoff   = 5 * time.Minute
	baseBackoff  = 2 * time.Second
	writeTimeout = 10 * time.Second
	pongTimeout  = 60 * time.Second
)

// MessageHandler processes messages received from the backend.
type MessageHandler interface {
	OnConnect(ctx context.Context, send func(any) error)
	OnDisconnect()
	OnConfig(cfg proto.AgentConfig)
	OnExec(cmd proto.ExecCommand)
	OnSysAction(msg proto.SysAction)
	OnRuleSync(rs proto.RuleSync)
	OnFS(req proto.FSRequest)
	OnTunnelStart(msg proto.TunnelStart)
	OnTunnelStop(msg proto.TunnelStop)
	OnTunnelOpen(msg proto.TunnelOpen)
	OnTunnelOpenAck(msg proto.TunnelOpenAck)
	OnTunnelData(msg proto.TunnelData)
	OnTunnelClose(msg proto.TunnelClose)
	OnTunnelWindow(msg proto.TunnelWindow)
	OnPTYStart(msg proto.PTYStart)
	OnPTYData(msg proto.PTYData)
	OnPTYResize(msg proto.PTYResize)
	OnPTYClose(msg proto.PTYClose)
	OnDB(req proto.DBRequest)
	// OnMigration receives all migration_* messages (migration sub-protocol);
	// the Manager does its own internal dispatch by type.
	OnMigration(msgType string, raw []byte)
}

type Client struct {
	url     string
	token   string
	handler MessageHandler
	log     *zap.Logger

	mu   sync.Mutex
	conn *websocket.Conn

	// writeMu serializes ALL writes to the WebSocket connection. gorilla/websocket
	// does not allow concurrent writes; without this, heartbeat/metrics and the
	// file-manager responses (fs_response) could write at the same time and
	// trigger a "concurrent write to websocket connection" panic.
	writeMu sync.Mutex

	sendCh chan any
}

func New(url, token string, handler MessageHandler, log *zap.Logger) *Client {
	return &Client{
		url:     url,
		token:   token,
		handler: handler,
		log:     log,
		sendCh:  make(chan any, 256),
	}
}

// Send queues a message to send to the backend (thread-safe).
func (c *Client) Send(msg any) {
	select {
	case c.sendCh <- msg:
	default:
		c.log.Warn("ws: send buffer full, dropping message")
	}
}

// Run connects and keeps the connection. Blocks until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.connect(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			delay := backoff(attempt)
			c.log.Warn("ws: disconnected, reintentando",
				zap.Error(err),
				zap.Duration("en", delay),
				zap.Int("intento", attempt+1),
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			attempt++
		} else {
			attempt = 0
		}
	}
}

func (c *Client) connect(ctx context.Context) error {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+c.token)

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, c.url, headers)
	if err != nil {
		return err
	}
	defer conn.Close()

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.log.Info("ws: conectado", zap.String("url", c.url))

	conn.SetReadDeadline(time.Now().Add(pongTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})

	// Send function that uses the current connection
	sendFn := func(msg any) error {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		return conn.WriteMessage(websocket.TextMessage, data)
	}

	c.handler.OnConnect(ctx, sendFn)

	// Drainer: writes everything that arrives on the channel
	writeErr := make(chan error, 1)
	go func() {
		pingTicker := time.NewTicker(30 * time.Second)
		defer pingTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				writeErr <- nil
				return
			case msg, ok := <-c.sendCh:
				if !ok {
					writeErr <- nil
					return
				}
				if err := sendFn(msg); err != nil {
					writeErr <- err
					return
				}
			case <-pingTicker.C:
				c.writeMu.Lock()
				conn.SetWriteDeadline(time.Now().Add(writeTimeout))
				err := conn.WriteMessage(websocket.PingMessage, nil)
				c.writeMu.Unlock()
				if err != nil {
					writeErr <- err
					return
				}
			}
		}
	}()

	// Reader
	readErr := make(chan error, 1)
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			conn.SetReadDeadline(time.Now().Add(pongTimeout))
			c.dispatch(data)
		}
	}()

	var connErr error
	select {
	case connErr = <-writeErr:
	case connErr = <-readErr:
	case <-ctx.Done():
		connErr = ctx.Err()
	}

	c.mu.Lock()
	c.conn = nil
	c.mu.Unlock()

	c.handler.OnDisconnect()
	return connErr
}

func (c *Client) dispatch(data []byte) {
	var env proto.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		c.log.Warn("ws: mensaje malformado", zap.Error(err))
		return
	}

	switch env.Type {
	case proto.TypeConfig:
		var cfg proto.AgentConfig
		if json.Unmarshal(data, &cfg) == nil {
			c.handler.OnConfig(cfg)
		}

	case proto.TypeExec:
		var cmd proto.ExecCommand
		if json.Unmarshal(data, &cmd) == nil {
			c.handler.OnExec(cmd)
		}

	case proto.TypeSysAction:
		var msg proto.SysAction
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnSysAction(msg)
		}

	case proto.TypeRuleSync:
		var rs proto.RuleSync
		if json.Unmarshal(data, &rs) == nil {
			c.handler.OnRuleSync(rs)
		}

	case proto.TypeFSRequest:
		var req proto.FSRequest
		if json.Unmarshal(data, &req) == nil {
			c.handler.OnFS(req)
		}

	case proto.TypeTunnelStart:
		var msg proto.TunnelStart
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnTunnelStart(msg)
		}

	case proto.TypeTunnelStop:
		var msg proto.TunnelStop
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnTunnelStop(msg)
		}

	case proto.TypeTunnelOpen:
		var msg proto.TunnelOpen
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnTunnelOpen(msg)
		}

	case proto.TypeTunnelOpenAck:
		var msg proto.TunnelOpenAck
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnTunnelOpenAck(msg)
		}

	case proto.TypeTunnelData:
		var msg proto.TunnelData
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnTunnelData(msg)
		}

	case proto.TypeTunnelClose:
		var msg proto.TunnelClose
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnTunnelClose(msg)
		}

	case proto.TypeTunnelWindow:
		var msg proto.TunnelWindow
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnTunnelWindow(msg)
		}

	case proto.TypePTYStart:
		var msg proto.PTYStart
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnPTYStart(msg)
		}

	case proto.TypePTYData:
		var msg proto.PTYData
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnPTYData(msg)
		}

	case proto.TypePTYResize:
		var msg proto.PTYResize
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnPTYResize(msg)
		}

	case proto.TypePTYClose:
		var msg proto.PTYClose
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnPTYClose(msg)
		}

	case proto.TypeDBRequest:
		var msg proto.DBRequest
		if json.Unmarshal(data, &msg) == nil {
			c.handler.OnDB(msg)
		}

	case proto.TypeAgentPing:
		// The pong is handled automatically by the WebSocket layer

	default:
		if strings.HasPrefix(env.Type, "migration_") {
			c.handler.OnMigration(env.Type, data)
			return
		}
		c.log.Debug("ws: tipo desconocido", zap.String("type", env.Type))
	}
}

func backoff(attempt int) time.Duration {
	if attempt == 0 {
		return 0
	}
	d := time.Duration(float64(baseBackoff) * math.Pow(2, float64(attempt-1)))
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}
