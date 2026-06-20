package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	BackendURL string // ws://... o wss://...
	AgentToken string // token ant_...
	Version    string

	// Defaults que el backend puede sobreescribir vía AgentConfig
	MetricsIntervalSeconds   int
	HeartbeatIntervalSeconds int
	LogBufferSize            int
	LogServices              []string

	// Paths
	DBPath string // bbolt para buffer offline
}

func Load() (*Config, error) {
	token := env("AURANODE_TOKEN", "")
	if token == "" {
		return nil, errors.New("AURANODE_TOKEN es requerido")
	}
	backendURL := env("AURANODE_BACKEND_URL", "wss://api.auranode.app/ws/agent")

	// Normalizar: si es http(s) convertir a ws(s)
	backendURL = strings.Replace(backendURL, "https://", "wss://", 1)
	backendURL = strings.Replace(backendURL, "http://", "ws://", 1)

	return &Config{
		BackendURL:               backendURL,
		AgentToken:               token,
		Version:                  env("AURANODE_VERSION", "0.1.0"),
		MetricsIntervalSeconds:   envInt("AURANODE_METRICS_INTERVAL", 60),
		HeartbeatIntervalSeconds: envInt("AURANODE_HEARTBEAT_INTERVAL", 30),
		LogBufferSize:            envInt("AURANODE_LOG_BUFFER", 1000),
		LogServices:              envList("AURANODE_LOG_SERVICES"),
		DBPath:                   env("AURANODE_DB_PATH", "/var/lib/auranode/buffer.db"),
	}, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envList(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
