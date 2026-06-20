// Package executor ejecuta comandos recibidos desde el backend y reporta el resultado.
package executor

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"time"
)

type Result struct {
	CommandID  string
	ExitStatus int
	Stdout     string
	Stderr     string
	DurationMS int64
}

// Run ejecuta el comando con un timeout hard y devuelve el resultado.
// Si hardTimeout == 0, usa 300 s como fallback.
func Run(ctx context.Context, commandID, command string, hardTimeout int) Result {
	if hardTimeout <= 0 {
		hardTimeout = 300
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(hardTimeout)*time.Second)
	defer cancel()

	start := time.Now()

	// Ejecutar con bash -c para soportar pipes, redirects, etc.
	cmd := exec.CommandContext(ctx, "bash", "-c", command)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	elapsed := time.Since(start).Milliseconds()

	exitStatus := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitStatus = exitErr.ExitCode()
		} else {
			exitStatus = 1
		}
	}

	return Result{
		CommandID:  commandID,
		ExitStatus: exitStatus,
		Stdout:     truncate(stdout.String(), 64*1024),
		Stderr:     truncate(stderr.String(), 8*1024),
		DurationMS: elapsed,
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return strings.TrimRight(s, "\n")
	}
	return s[:max] + "\n[truncado]"
}
