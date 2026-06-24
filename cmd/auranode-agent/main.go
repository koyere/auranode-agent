package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/internal/agent"
	"github.com/koyere/auranode-agent/internal/config"
	"github.com/koyere/auranode-agent/internal/privileged"
)

// Overwritten at build time with -ldflags by GoReleaser.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	// Subcomando de versión/capacidades. El instalador lo usa para confirmar que el
	// binario soporta el modo privilegiado ANTES de habilitar el helper root (un
	// binario antiguo no imprimiría el marcador y el instalador abortaría).
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("auranode-agent " + version + " privileged-capable")
		return
	}

	// Subcomando del helper privilegiado (lo arranca un unit systemd aparte, como
	// root). El agente principal NUNCA entra por aquí.
	if len(os.Args) > 1 && os.Args[1] == "privileged-helper" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		log.Info("auranode-agent privileged-helper iniciado", zap.String("version", version))
		if err := privileged.RunHelper(ctx, log); err != nil {
			log.Fatal("helper privilegiado terminó con error", zap.Error(err))
		}
		return
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatal("invalid config", zap.Error(err))
	}
	cfg.Version = version

	a, err := agent.New(cfg, log)
	if err != nil {
		log.Fatal("error inicializando agente", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("auranode-agent iniciado",
		zap.String("version", version),
		zap.String("commit", commit),
		zap.String("date", date),
		zap.String("backend", cfg.BackendURL),
	)

	a.Run(ctx)

	log.Info("auranode-agent detenido")
}
