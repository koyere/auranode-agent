package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/internal/agent"
	"github.com/koyere/auranode-agent/internal/config"
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
