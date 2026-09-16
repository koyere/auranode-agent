// Package executor runs commands received from the backend and reports the result.
package executor

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

const (
	defaultTimeoutSeconds = 300
	maxTimeoutSeconds     = 1800
	maxConcurrentCommands = 4
	maxStdoutBytes        = 64 * 1024
	maxStderrBytes        = 8 * 1024
	truncatedMarker       = "\n[truncado]"
)

var commandSlots = make(chan struct{}, maxConcurrentCommands)

type Result struct {
	CommandID  string
	ExitStatus int
	Stdout     string
	Stderr     string
	DurationMS int64
}

// Run runs the command with a hard timeout and returns the result.
// Si hardTimeout == 0, usa 300 s como fallback.
func Run(ctx context.Context, commandID, command string, hardTimeout int) Result {
	if hardTimeout <= 0 {
		hardTimeout = defaultTimeoutSeconds
	}
	if hardTimeout > maxTimeoutSeconds {
		hardTimeout = maxTimeoutSeconds
	}

	// Reject excess work instead of creating an unbounded queue of processes and
	// goroutines when the backend (or a compromised account) floods the agent.
	select {
	case commandSlots <- struct{}{}:
		defer func() { <-commandSlots }()
	default:
		return Result{
			CommandID:  commandID,
			ExitStatus: 75,
			Stderr:     "demasiados comandos concurrentes; inténtalo de nuevo",
		}
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(hardTimeout)*time.Second)
	defer cancel()

	start := time.Now()

	// Run with bash -c to support pipes, redirects, etc.
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	// Put bash and its children in their own process group. CommandContext only
	// kills the direct child by default, which would leave background descendants
	// running after a timeout.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 2 * time.Second

	stdout := newLimitedBuffer(maxStdoutBytes)
	stderr := newLimitedBuffer(maxStderrBytes)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	elapsed := time.Since(start).Milliseconds()

	exitStatus := 0
	if ctx.Err() == context.DeadlineExceeded {
		exitStatus = 124
	} else if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitStatus = exitErr.ExitCode()
		} else {
			exitStatus = 1
		}
	}

	return Result{
		CommandID:  commandID,
		ExitStatus: exitStatus,
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		DurationMS: elapsed,
	}
}

// limitedBuffer implements io.Writer while retaining at most limit bytes. It
// reports the original write length so child processes do not see a short write.
type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func newLimitedBuffer(limit int) limitedBuffer { return limitedBuffer{limit: limit} }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		keep := len(p)
		if keep > remaining {
			keep = remaining
		}
		_, _ = b.buf.Write(p[:keep])
	}
	if len(p) > remaining {
		b.truncated = true
	}
	return written, nil
}

func (b *limitedBuffer) String() string {
	s := strings.TrimRight(b.buf.String(), "\n")
	if b.truncated {
		return s + truncatedMarker
	}
	return s
}
