package database

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// E2E real de la op redis contra un Redis DESECHABLE (nunca el de PROD).
//   AURANODE_DB_E2E=1 AURANODE_REDIS_PORT=6399 go test ./internal/database -run TestRedisE2E -v
func TestRedisE2E(t *testing.T) {
	if os.Getenv("AURANODE_DB_E2E") != "1" {
		t.Skip("set AURANODE_DB_E2E=1 to run")
	}
	port := 6379
	if p := os.Getenv("AURANODE_REDIS_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	conn := proto.DBConn{Engine: "redis", Host: "127.0.0.1", Port: port, Password: os.Getenv("AURANODE_REDIS_PASS")}
	m := NewManager(zap.NewNop())

	raw, err := m.redisStatus(context.Background(), conn)
	if err != nil {
		t.Fatalf("redisStatus: %v", err)
	}
	var d proto.DBRedisData
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	if d.Version == "" {
		t.Fatalf("esperaba versión de Redis, obtuve %+v", d)
	}
	if d.Keys < 0 {
		t.Fatalf("nº de claves inválido: %d", d.Keys)
	}
	t.Logf("redis ok: v%s, %d claves, mem=%s, %d clientes, uptime=%ds",
		d.Version, d.Keys, d.Memory, d.Connections, d.UptimeSec)
}
