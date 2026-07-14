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

// E2E real de la op admin (D3) contra un PostgreSQL DESECHABLE (nunca el de PROD).
// Guardado por AURANODE_DB_E2E; usa un superusuario y un puerto aislado:
//   AURANODE_DB_E2E=1 AURANODE_DB_ADMIN_PORT=5599 AURANODE_DB_ADMIN_USER=postgres \
//     go test ./internal/database -run TestAdminE2E -v
func TestAdminE2E(t *testing.T) {
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
	conn := proto.DBConn{Engine: "postgres", Host: "127.0.0.1", Port: port, User: user,
		Password: os.Getenv("AURANODE_DB_ADMIN_PASS")}
	m := NewManager(zap.NewNop())
	ctx := context.Background()

	admin := func(spec proto.DBAdminSpec) (proto.DBAdminData, error) {
		raw, err := m.admin(ctx, proto.DBRequest{Op: proto.DBOpAdmin, Conn: conn, Admin: &spec})
		var d proto.DBAdminData
		if err == nil {
			_ = json.Unmarshal(raw, &d)
		}
		return d, err
	}

	const (
		db  = "auranode_d3test"
		usr = "auranode_d3user"
	)
	// Limpieza previa best-effort (por si un run anterior falló a medias).
	_, _ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: db})
	_, _ = admin(proto.DBAdminSpec{Action: proto.DBAdminDropUser, Username: usr})

	// 1) Crear BD y usuario.
	if d, err := admin(proto.DBAdminSpec{Action: proto.DBAdminCreateDatabase, Database: db}); err != nil {
		t.Fatalf("create_database: %v", err)
	} else {
		t.Log(d.Message)
	}
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminCreateUser, Username: usr, Password: "d3pw_Init1"}); err != nil {
		t.Fatalf("create_user: %v", err)
	}

	// 2) Cambiar contraseña.
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminChangePassword, Username: usr, Password: "d3pw_New2"}); err != nil {
		t.Fatalf("change_password: %v", err)
	}

	// 3) Grant read-only y luego revoke.
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminGrant, Database: db, Username: usr, Privilege: proto.DBPrivReadOnly}); err != nil {
		t.Fatalf("grant readonly: %v", err)
	}
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminGrant, Database: db, Username: usr, Privilege: proto.DBPrivReadWrite}); err != nil {
		t.Fatalf("grant readwrite: %v", err)
	}
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminRevoke, Database: db, Username: usr}); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// 4) Validación: identificador inválido rechazado (defensa antiinyección).
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminCreateDatabase, Database: "bad; DROP DATABASE x"}); err == nil {
		t.Fatal("esperaba rechazo de identificador no válido")
	}

	// 5) Eliminar usuario y BD (limpieza).
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminDropUser, Username: usr}); err != nil {
		t.Fatalf("drop_user: %v", err)
	}
	if _, err := admin(proto.DBAdminSpec{Action: proto.DBAdminDropDatabase, Database: db}); err != nil {
		t.Fatalf("drop_database: %v", err)
	}
	t.Log("admin E2E completo: create/alter/grant/revoke/drop + validación")
}
