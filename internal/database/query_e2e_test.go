package database

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// E2E real de la op query contra un PostgreSQL local. Guardado por AURANODE_DB_E2E.
//   AURANODE_DB_E2E=1 go test ./internal/database -run TestQueryE2E -v
// Requiere rol/BD desechables auranode_d2test (creados fuera del test).
func TestQueryE2E(t *testing.T) {
	if os.Getenv("AURANODE_DB_E2E") != "1" {
		t.Skip("set AURANODE_DB_E2E=1 to run")
	}
	m := NewManager(zap.NewNop())
	conn := proto.DBConn{Engine: "postgres", Host: "127.0.0.1", Port: 5432, User: "auranode_d2test", Password: "d2testpw"}
	ctx := context.Background()

	// 1) SELECT devuelve columnas + filas (incluye NULL).
	raw, err := m.query(ctx, proto.DBRequest{Op: proto.DBOpQuery, Conn: conn, Database: "auranode_d2test",
		SQL: "SELECT id, name, qty, note FROM items ORDER BY id", ReadOnly: true})
	if err != nil {
		t.Fatalf("query select: %v", err)
	}
	var res proto.DBQueryData
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Columns) != 4 || res.RowsReturned != 3 {
		t.Fatalf("esperaba 4 col / 3 filas, obtuve %d col / %d filas", len(res.Columns), res.RowsReturned)
	}
	if res.Rows[0][3] != nil {
		t.Fatalf("esperaba NULL en note de la primera fila, obtuve %q", *res.Rows[0][3])
	}
	if res.Rows[1][1] == nil || *res.Rows[1][1] != "beta" {
		t.Fatalf("esperaba 'beta' en fila 2, obtuve %v", res.Rows[1][1])
	}
	t.Logf("SELECT ok: %d filas, %d ms, read_only=%v", res.RowsReturned, res.DurationMS, res.ReadOnly)

	// 2) Escritura BLOQUEADA en conexión read-only.
	_, err = m.query(ctx, proto.DBRequest{Op: proto.DBOpQuery, Conn: conn, Database: "auranode_d2test",
		SQL: "INSERT INTO items(name, qty) VALUES ('delta', 40)", ReadOnly: true})
	if err == nil {
		t.Fatal("esperaba error al escribir en conexión read-only")
	}
	t.Logf("write en read-only rechazado ok: %v", err)

	// 3) Varios statements rechazados (un solo statement por ejecución).
	_, err = m.query(ctx, proto.DBRequest{Op: proto.DBOpQuery, Conn: conn, Database: "auranode_d2test",
		SQL: "SELECT 1; SELECT 2", ReadOnly: true})
	if err == nil {
		t.Fatal("esperaba error con múltiples statements")
	}
	t.Logf("múltiples statements rechazados ok: %v", err)

	// 4) Escritura PERMITIDA en conexión read-write (owner/admin).
	rawW, err := m.query(ctx, proto.DBRequest{Op: proto.DBOpQuery, Conn: conn, Database: "auranode_d2test",
		SQL: "UPDATE items SET qty = qty + 1 WHERE name = 'alpha'", ReadOnly: false})
	if err != nil {
		t.Fatalf("update read-write: %v", err)
	}
	var resW proto.DBQueryData
	_ = json.Unmarshal(rawW, &resW)
	if len(resW.Columns) != 0 {
		t.Fatalf("esperaba 0 columnas en UPDATE, obtuve %d", len(resW.Columns))
	}
	t.Logf("UPDATE en read-write ok (%d ms)", resW.DurationMS)

	_ = time.Second
}
