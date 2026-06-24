package privileged

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	maxRequestBytes = 8 * 1024  // un Request es diminuto; cualquier cosa mayor es abuso
	maxOutputBytes  = 64 * 1024 // recorte de stdout/stderr devueltos
	socketGroup     = "auranode"
)

// RunHelper arranca el servidor del helper root. Bloquea hasta que ctx se cancela.
// DEBE ejecutarse como root (sin él, las acciones fallarían igual que el agente).
func RunHelper(ctx context.Context, log *zap.Logger) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("el helper privilegiado debe ejecutarse como root")
	}

	if err := os.MkdirAll(filepath.Dir(SocketPath), 0o755); err != nil {
		return fmt.Errorf("creando directorio del socket: %w", err)
	}
	// Limpiar un socket viejo de un arranque previo.
	_ = os.Remove(SocketPath)

	ln, err := net.Listen("unix", SocketPath)
	if err != nil {
		return fmt.Errorf("escuchando en %s: %w", SocketPath, err)
	}
	defer ln.Close()

	// Permitir SOLO al grupo del agente local hablar con el helper.
	if err := restrictSocket(SocketPath); err != nil {
		log.Warn("no se pudo restringir el socket al grupo del agente", zap.Error(err))
	}

	log.Info("helper privilegiado escuchando", zap.String("socket", SocketPath))

	// Cerrar el listener cuando ctx termina para desbloquear Accept.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	var mu sync.Mutex // una acción privilegiada a la vez (evita locks de apt en paralelo)
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				log.Warn("accept falló", zap.Error(err))
				continue
			}
		}
		go handleConn(ctx, conn, &mu, log)
	}
}

// restrictSocket ajusta el socket a root:auranode con modo 0660.
func restrictSocket(path string) error {
	g, err := user.LookupGroup(socketGroup)
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return err
	}
	if err := os.Chown(path, 0, gid); err != nil {
		return err
	}
	return os.Chmod(path, 0o660)
}

func handleConn(ctx context.Context, conn net.Conn, mu *sync.Mutex, log *zap.Logger) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(conn, maxRequestBytes)); err != nil && buf.Len() == 0 {
		writeResp(conn, Response{Error: "lectura de la petición falló"})
		return
	}

	var req Request
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &req); err != nil {
		writeResp(conn, Response{Error: "petición JSON inválida"})
		return
	}

	// Revalidación del lado servidor (defensa en profundidad).
	p, err := resolve(req.Action, req.Args)
	if err != nil {
		log.Warn("acción privilegiada rechazada",
			zap.String("action", req.Action), zap.Error(err))
		writeResp(conn, Response{Rejected: true, Error: err.Error()})
		return
	}

	mu.Lock()
	defer mu.Unlock()

	log.Info("ejecutando acción privilegiada",
		zap.String("action", req.Action), zap.Strings("argv", p.argv))

	resp := execPlan(ctx, p)
	resp.OK = resp.ExitStatus == 0 && resp.Error == ""
	writeResp(conn, resp)
}

func execPlan(parent context.Context, p plan) Response {
	to := p.timeoutSec
	if to <= 0 {
		to = 300
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(to)*time.Second)
	defer cancel()

	start := time.Now()
	cmd := exec.CommandContext(ctx, p.argv[0], p.argv[1:]...) //nolint:gosec // argv viene de la whitelist validada, sin shell
	cmd.Env = append(os.Environ(), p.env...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	resp := Response{
		Stdout:     truncate(stdout.String(), maxOutputBytes),
		Stderr:     truncate(stderr.String(), maxOutputBytes),
		DurationMS: time.Since(start).Milliseconds(),
	}
	if ctx.Err() == context.DeadlineExceeded {
		resp.Error = "la acción superó el tiempo máximo"
		resp.ExitStatus = 124
		return resp
	}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			resp.ExitStatus = exitErr.ExitCode()
		} else {
			resp.ExitStatus = 1
			resp.Error = err.Error()
		}
	}
	return resp
}

func writeResp(conn net.Conn, r Response) {
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	b, _ := json.Marshal(r)
	_, _ = conn.Write(b)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n[salida truncada]"
}
