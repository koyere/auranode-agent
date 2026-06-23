// Package terminal implementa el lado-agente del terminal web (PTY interactivo). Por
// cada sesión, abre un bash en un pseudo-terminal y transmite stdin/stdout con el backend.
// La shell corre con el MISMO usuario del agente (no privilegiado, NoNewPrivileges) — igual
// que exec; no hay escalada de privilegios.
package terminal

import (
	"encoding/base64"
	"os"
	"os/exec"
	"sync"

	"github.com/creack/pty"
	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

const ptyReadBuf = 16 * 1024

type session struct {
	ptmx *os.File
	cmd  *exec.Cmd
}

// Manager gestiona las sesiones PTY activas del agente.
type Manager struct {
	log    *zap.Logger
	mu     sync.Mutex
	sendFn func(any) error
	sess   map[string]*session
}

func NewManager(log *zap.Logger) *Manager {
	return &Manager{log: log, sess: make(map[string]*session)}
}

func (m *Manager) SetSend(fn func(any) error) {
	m.mu.Lock()
	m.sendFn = fn
	m.mu.Unlock()
}

func (m *Manager) send(v any) {
	m.mu.Lock()
	fn := m.sendFn
	m.mu.Unlock()
	if fn != nil {
		_ = fn(v)
	}
}

// Start abre un PTY con bash para la sesión dada y arranca el bombeo de salida.
func (m *Manager) Start(msg proto.PTYStart) {
	m.mu.Lock()
	if _, exists := m.sess[msg.SessionID]; exists {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	cmd := exec.Command("bash")
	cmd.Env = append(os.Environ(), "TERM=xterm-256color")
	rows, cols := msg.Rows, msg.Cols
	if rows == 0 {
		rows = 24
	}
	if cols == 0 {
		cols = 80
	}
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: rows, Cols: cols})
	if err != nil {
		m.log.Warn("pty: no se pudo iniciar", zap.Error(err))
		m.sendClose(msg.SessionID, "no se pudo abrir la shell")
		return
	}

	s := &session{ptmx: ptmx, cmd: cmd}
	m.mu.Lock()
	m.sess[msg.SessionID] = s
	m.mu.Unlock()

	go m.readLoop(msg.SessionID, s)
}

// readLoop transmite la salida del PTY al backend hasta EOF/cierre.
func (m *Manager) readLoop(sessionID string, s *session) {
	buf := make([]byte, ptyReadBuf)
	for {
		n, err := s.ptmx.Read(buf)
		if n > 0 {
			m.send(proto.PTYData{
				Envelope:  proto.Envelope{Type: proto.TypePTYData},
				SessionID: sessionID,
				Data:      base64.StdEncoding.EncodeToString(buf[:n]),
			})
		}
		if err != nil {
			break
		}
	}
	m.cleanup(sessionID)
	m.sendClose(sessionID, "")
}

// Data escribe stdin (recibido del backend) en el PTY.
func (m *Manager) Data(msg proto.PTYData) {
	m.mu.Lock()
	s := m.sess[msg.SessionID]
	m.mu.Unlock()
	if s == nil {
		return
	}
	b, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil {
		return
	}
	_, _ = s.ptmx.Write(b)
}

// Resize ajusta el tamaño de la ventana del PTY.
func (m *Manager) Resize(msg proto.PTYResize) {
	m.mu.Lock()
	s := m.sess[msg.SessionID]
	m.mu.Unlock()
	if s == nil || msg.Rows == 0 || msg.Cols == 0 {
		return
	}
	_ = pty.Setsize(s.ptmx, &pty.Winsize{Rows: msg.Rows, Cols: msg.Cols})
}

// Close termina la sesión (orden del backend: idle/duración/cierre del navegador).
func (m *Manager) Close(sessionID string) {
	m.cleanup(sessionID)
}

func (m *Manager) cleanup(sessionID string) {
	m.mu.Lock()
	s := m.sess[sessionID]
	delete(m.sess, sessionID)
	m.mu.Unlock()
	if s == nil {
		return
	}
	if s.ptmx != nil {
		_ = s.ptmx.Close()
	}
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_, _ = s.cmd.Process.Wait()
	}
}

func (m *Manager) sendClose(sessionID, errStr string) {
	m.send(proto.PTYClose{
		Envelope:  proto.Envelope{Type: proto.TypePTYClose},
		SessionID: sessionID,
		Error:     errStr,
	})
}

// Shutdown cierra todas las sesiones (al perder la conexión con el backend).
func (m *Manager) Shutdown() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.sess))
	for id := range m.sess {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		m.cleanup(id)
	}
}
