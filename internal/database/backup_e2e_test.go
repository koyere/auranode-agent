package database

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// E2E real de la op dump/restore (D4) contra un PostgreSQL DESECHABLE (nunca PROD).
//   AURANODE_DB_E2E=1 AURANODE_DB_ADMIN_PORT=5599 AURANODE_DB_ADMIN_USER=d3super \
//     go test ./internal/database -run TestBackupE2E -v
// Requiere pg_dump/psql en el PATH.
func TestBackupE2E(t *testing.T) {
	if os.Getenv("AURANODE_DB_E2E") != "1" {
		t.Skip("set AURANODE_DB_E2E=1 to run")
	}
	port := 5432
	if p := os.Getenv("AURANODE_DB_ADMIN_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	user := os.Getenv("AURANODE_DB_ADMIN_USER")
	if user == "" {
		user = "postgres"
	}
	os.Setenv("AURANODE_DB_BACKUP_DIR", t.TempDir())

	conn := proto.DBConn{Engine: "postgres", Host: "127.0.0.1", Port: port, User: user,
		Password: os.Getenv("AURANODE_DB_ADMIN_PASS")}
	m := NewManager(zap.NewNop())
	ctx := context.Background()

	admin := func(spec proto.DBAdminSpec) error {
		_, err := m.admin(ctx, proto.DBRequest{Op: proto.DBOpAdmin, Conn: conn, Admin: &spec})
		return err
	}
	exec1 := func(database, sql string) error {
		_, err := m.query(ctx, proto.DBRequest{Op: proto.DBOpQuery, Conn: conn, Database: database, SQL: sql, ReadOnly: false})
		return err
	}

	const src, dst = "auranode_d4test", "auranode_d4restore"
	_ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: src})
	_ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: dst})

	if err := admin(proto.DBAdminSpec{Action: proto.DBAdminCreateDatabase, Database: src}); err != nil {
		t.Fatalf("create src: %v", err)
	}
	if err := exec1(src, "CREATE TABLE items(id int, name text)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := exec1(src, "INSERT INTO items VALUES (1,'a'),(2,'b'),(3,'c')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Dump.
	raw, err := m.dump(ctx, proto.DBRequest{Op: proto.DBOpDump, Conn: conn, Database: src})
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	var dd proto.DBDumpData
	_ = json.Unmarshal(raw, &dd)
	if dd.File == "" || dd.SizeBytes == 0 {
		t.Fatalf("dump vacío: %+v", dd)
	}
	if _, err := os.Stat(filepath.Join(backupDir(), dd.File)); err != nil {
		t.Fatalf("el archivo de dump no existe: %v", err)
	}
	t.Logf("dump ok: %s (%d bytes, %d ms)", dd.File, dd.SizeBytes, dd.DurationMS)

	// Restaurar en una BD nueva y verificar filas.
	if err := admin(proto.DBAdminSpec{Action: proto.DBAdminCreateDatabase, Database: dst}); err != nil {
		t.Fatalf("create dst: %v", err)
	}
	if _, err := m.restore(ctx, proto.DBRequest{Op: proto.DBOpRestore, Conn: conn, Database: dst, DumpFile: dd.File}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	rawC, err := m.query(ctx, proto.DBRequest{Op: proto.DBOpQuery, Conn: conn, Database: dst,
		SQL: "SELECT count(*) FROM items", ReadOnly: true})
	if err != nil {
		t.Fatalf("verify query: %v", err)
	}
	var qd proto.DBQueryData
	_ = json.Unmarshal(rawC, &qd)
	if len(qd.Rows) != 1 || qd.Rows[0][0] == nil || *qd.Rows[0][0] != "3" {
		t.Fatalf("esperaba 3 filas restauradas, obtuve %v", qd.Rows)
	}
	t.Log("restore verificado: 3 filas en la BD destino")

	// Listar y eliminar el dump.
	rawL, err := m.dumps(proto.DBRequest{Op: proto.DBOpDumps})
	if err != nil {
		t.Fatalf("dumps: %v", err)
	}
	var dl proto.DBDumpsData
	_ = json.Unmarshal(rawL, &dl)
	found := false
	for _, d := range dl.Dumps {
		if d.File == dd.File {
			found = true
			if d.Database != src || d.Engine != "postgres" {
				t.Fatalf("metadatos de dump incorrectos: %+v", d)
			}
		}
	}
	if !found {
		t.Fatal("el dump no aparece en la lista")
	}
	if _, err := m.dumpDelete(proto.DBRequest{Op: proto.DBOpDumpDel, DumpFile: dd.File}); err != nil {
		t.Fatalf("dump_delete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir(), dd.File)); !os.IsNotExist(err) {
		t.Fatal("el dump debería estar eliminado")
	}

	// Rechazo de nombre con traversal.
	if _, err := safeDumpPath("../etc/passwd"); err == nil {
		t.Fatal("esperaba rechazo de path traversal")
	}

	// Limpieza.
	_ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: src})
	_ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: dst})
	t.Log("backup E2E completo: dump/restore/list/delete + validación de path")
}
