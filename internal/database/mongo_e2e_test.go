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

// E2E real de MongoDB contra un mongod DESECHABLE (nunca PROD). Requiere mongod local sin
// auth (o con AURANODE_MONGO_USER/PASS) y mongodump/mongorestore en el PATH para el backup.
//   AURANODE_DB_E2E=1 AURANODE_MONGO_PORT=27099 go test ./internal/database -run TestMongoE2E -v
func TestMongoE2E(t *testing.T) {
	if os.Getenv("AURANODE_DB_E2E") != "1" {
		t.Skip("set AURANODE_DB_E2E=1 to run")
	}
	port := 27017
	if p := os.Getenv("AURANODE_MONGO_PORT"); p != "" {
		port, _ = strconv.Atoi(p)
	}
	os.Setenv("AURANODE_DB_BACKUP_DIR", t.TempDir())
	conn := proto.DBConn{Engine: "mongodb", Host: "127.0.0.1", Port: port,
		User: os.Getenv("AURANODE_MONGO_USER"), Password: os.Getenv("AURANODE_MONGO_PASS")}
	m := NewManager(zap.NewNop())
	ctx := context.Background()

	admin := func(spec proto.DBAdminSpec) error {
		_, err := m.admin(ctx, proto.DBRequest{Op: proto.DBOpAdmin, Conn: conn, Admin: &spec})
		return err
	}
	const dbn = "auranode_mongo_e2e"
	_ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: dbn})

	// Crear BD (materializa con colección inicial) + explorar.
	if err := admin(proto.DBAdminSpec{Action: proto.DBAdminCreateDatabase, Database: dbn}); err != nil {
		t.Fatalf("create_database: %v", err)
	}
	if _, err := m.testMongo(ctx, conn); err != nil {
		t.Fatalf("test: %v", err)
	}
	raw, err := m.databasesMongo(ctx, conn)
	if err != nil {
		t.Fatalf("databases: %v", err)
	}
	var dd proto.DBDatabasesData
	_ = json.Unmarshal(raw, &dd)
	if dd.Status.Engine != "mongodb" {
		t.Fatalf("estado inesperado: %+v", dd.Status)
	}

	// Usuario + grant/revoke.
	if err := admin(proto.DBAdminSpec{Action: proto.DBAdminCreateUser, Database: dbn, Username: "e2euser", Password: "e2ePw_1", Privilege: proto.DBPrivReadWrite}); err != nil {
		t.Fatalf("create_user: %v", err)
	}
	if err := admin(proto.DBAdminSpec{Action: proto.DBAdminChangePassword, Database: dbn, Username: "e2euser", Password: "e2ePw_2"}); err != nil {
		t.Fatalf("change_password: %v", err)
	}
	if err := admin(proto.DBAdminSpec{Action: proto.DBAdminGrant, Database: dbn, Username: "e2euser", Privilege: proto.DBPrivReadOnly}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	if err := admin(proto.DBAdminSpec{Action: proto.DBAdminRevoke, Database: dbn, Username: "e2euser"}); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// Backup + restore (requiere mongodump/mongorestore).
	rawD, err := m.dump(ctx, proto.DBRequest{Op: proto.DBOpDump, Conn: conn, Database: dbn})
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	var dump proto.DBDumpData
	_ = json.Unmarshal(rawD, &dump)
	if dump.File == "" {
		t.Fatalf("dump vacío")
	}
	if _, err := m.restore(ctx, proto.DBRequest{Op: proto.DBOpRestore, Conn: conn, Database: dbn, DumpFile: dump.File}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Limpieza.
	_ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropUser, Database: dbn, Username: "e2euser"})
	_ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: dbn})
	t.Logf("mongo E2E completo: %s, dump %s", dd.Status.Version, dump.File)
}
