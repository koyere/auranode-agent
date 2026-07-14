package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// Gestión de bases de datos (Parte 3 · D3). El agente ejecuta acciones administrativas
// acotadas sobre el motor local: crear/eliminar BD y usuario, cambiar contraseña y
// conceder/revocar privilegios básicos. Nunca administra el sistema: solo actúa como
// cliente SQL con las credenciales recibidas. Como los identificadores no pueden ir
// parametrizados en DDL, se VALIDAN (charset restringido) y se ENTRECOMILLAN por motor
// antes de interpolarse; las contraseñas van como literal entrecomillado. Toda acción
// destructiva ya trae doble confirmación desde el panel; aquí solo se ejecuta.

// identRe acota los nombres de BD/usuario a un charset seguro. Es defensa en profundidad
// (además del entrecomillado): rechaza espacios, comillas, punto y coma y control.
var identRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_$-]{0,62}$`)

func validIdent(s string) bool { return identRe.MatchString(s) }

// admin ejecuta una acción de gestión. La conexión se abre en lectura-escritura (las
// acciones son DDL); para grant/revoke se conecta a la BD objetivo para que los grants
// a nivel de esquema/tabla surtan efecto.
func (m *Manager) admin(ctx context.Context, req proto.DBRequest) (json.RawMessage, error) {
	if req.Admin == nil {
		return nil, fmt.Errorf("db: falta la especificación de gestión")
	}
	spec := *req.Admin
	switch req.Conn.Engine {
	case "postgres":
		return m.adminPostgres(ctx, req.Conn, spec)
	case "mysql":
		return m.adminMySQL(ctx, req.Conn, spec)
	case "mongodb":
		return m.adminMongo(ctx, req.Conn, spec)
	}
	return nil, fmt.Errorf("db: motor no soportado: %q", req.Conn.Engine)
}

func adminResult(msg string) (json.RawMessage, error) {
	return marshal(proto.DBAdminData{Message: msg})
}

// ─── PostgreSQL ───────────────────────────────────────────────────────────────

// pgQuoteIdent entrecomilla un identificador PostgreSQL ("" escapa la comilla doble).
func pgQuoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// pgQuoteLiteral entrecomilla un literal de cadena PostgreSQL ('' escapa la comilla).
// Con standard_conforming_strings (por defecto ON) la barra invertida es literal.
func pgQuoteLiteral(s string) string { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }

func (m *Manager) adminPostgres(ctx context.Context, conn proto.DBConn, spec proto.DBAdminSpec) (json.RawMessage, error) {
	switch spec.Action {
	case proto.DBAdminCreateDatabase:
		if !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: nombre de base de datos no válido")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		// CREATE DATABASE no admite transacción; database/sql ejecuta en autocommit.
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+pgQuoteIdent(spec.Database)); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Base de datos %q creada.", spec.Database))

	case proto.DBAdminDropDatabase:
		if !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: nombre de base de datos no válido")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "DROP DATABASE "+pgQuoteIdent(spec.Database)); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Base de datos %q eliminada.", spec.Database))

	case proto.DBAdminCreateUser:
		if !validIdent(spec.Username) {
			return nil, fmt.Errorf("db: nombre de usuario no válido")
		}
		if !validPassword(spec.Password) {
			return nil, fmt.Errorf("db: contraseña no válida")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		sqlStr := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD %s",
			pgQuoteIdent(spec.Username), pgQuoteLiteral(spec.Password))
		if _, err := db.ExecContext(ctx, sqlStr); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Usuario %q creado.", spec.Username))

	case proto.DBAdminDropUser:
		if !validIdent(spec.Username) {
			return nil, fmt.Errorf("db: nombre de usuario no válido")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "DROP ROLE "+pgQuoteIdent(spec.Username)); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Usuario %q eliminado.", spec.Username))

	case proto.DBAdminChangePassword:
		if !validIdent(spec.Username) {
			return nil, fmt.Errorf("db: nombre de usuario no válido")
		}
		if !validPassword(spec.Password) {
			return nil, fmt.Errorf("db: contraseña no válida")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		sqlStr := fmt.Sprintf("ALTER ROLE %s WITH PASSWORD %s",
			pgQuoteIdent(spec.Username), pgQuoteLiteral(spec.Password))
		if _, err := db.ExecContext(ctx, sqlStr); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Contraseña de %q actualizada.", spec.Username))

	case proto.DBAdminGrant:
		return m.grantPostgres(ctx, conn, spec)

	case proto.DBAdminRevoke:
		return m.revokePostgres(ctx, conn, spec)
	}
	return nil, fmt.Errorf("db: acción no soportada: %q", spec.Action)
}

// grantPostgres concede privilegios básicos. Se conecta a la BD objetivo para que los
// grants de esquema/tabla apliquen; también concede a nivel de BD y ajusta los
// privilegios por defecto para tablas futuras.
func (m *Manager) grantPostgres(ctx context.Context, conn proto.DBConn, spec proto.DBAdminSpec) (json.RawMessage, error) {
	if !validIdent(spec.Database) || !validIdent(spec.Username) {
		return nil, fmt.Errorf("db: base de datos o usuario no válidos")
	}
	u := pgQuoteIdent(spec.Username)
	dbn := pgQuoteIdent(spec.Database)

	db, err := open(ctx, conn, spec.Database, false)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var stmts []string
	switch spec.Privilege {
	case proto.DBPrivReadOnly:
		stmts = []string{
			fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", dbn, u),
			fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", u),
			fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", u),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s", u),
		}
	case proto.DBPrivReadWrite:
		stmts = []string{
			fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", dbn, u),
			fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", u),
			fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s", u),
			fmt.Sprintf("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %s", u),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", u),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO %s", u),
		}
	case proto.DBPrivAll:
		stmts = []string{
			fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", dbn, u),
			fmt.Sprintf("GRANT ALL ON SCHEMA public TO %s", u),
			fmt.Sprintf("GRANT ALL ON ALL TABLES IN SCHEMA public TO %s", u),
			fmt.Sprintf("GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO %s", u),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO %s", u),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO %s", u),
		}
	default:
		return nil, fmt.Errorf("db: privilegio no soportado: %q", spec.Privilege)
	}
	if err := execAll(ctx, db, stmts); err != nil {
		return nil, err
	}
	return adminResult(fmt.Sprintf("Privilegios (%s) concedidos a %q sobre %q.", spec.Privilege, spec.Username, spec.Database))
}

func (m *Manager) revokePostgres(ctx context.Context, conn proto.DBConn, spec proto.DBAdminSpec) (json.RawMessage, error) {
	if !validIdent(spec.Database) || !validIdent(spec.Username) {
		return nil, fmt.Errorf("db: base de datos o usuario no válidos")
	}
	u := pgQuoteIdent(spec.Username)
	dbn := pgQuoteIdent(spec.Database)

	db, err := open(ctx, conn, spec.Database, false)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Revierte también los privilegios POR DEFECTO que dejó el grant (ALTER DEFAULT
	// PRIVILEGES): si no, el rol conserva entradas ACL y no se puede eliminar (2BP01).
	stmts := []string{
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM %s", u),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON SEQUENCES FROM %s", u),
		fmt.Sprintf("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM %s", u),
		fmt.Sprintf("REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM %s", u),
		fmt.Sprintf("REVOKE ALL PRIVILEGES ON SCHEMA public FROM %s", u),
		fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s", dbn, u),
	}
	if err := execAll(ctx, db, stmts); err != nil {
		return nil, err
	}
	return adminResult(fmt.Sprintf("Privilegios de %q revocados sobre %q.", spec.Username, spec.Database))
}

// ─── MySQL / MariaDB ──────────────────────────────────────────────────────────

// myQuoteIdent entrecomilla un identificador MySQL con acentos graves (`` `` `` escapa).
func myQuoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// myQuoteLiteral entrecomilla un literal de cadena MySQL (escapa comilla y barra).
func myQuoteLiteral(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'"
}

// splitMyUser separa 'user@host' en (user, host); host por defecto '%'. El nombre de
// usuario debe pasar validIdent; el host se acota a caracteres de host válidos.
var myHostRe = regexp.MustCompile(`^[A-Za-z0-9_.%-]{1,255}$`)

func splitMyUser(raw string) (user, host string, ok bool) {
	host = "%"
	user = raw
	if i := strings.LastIndex(raw, "@"); i >= 0 {
		user = raw[:i]
		host = raw[i+1:]
	}
	if !validIdent(user) || !myHostRe.MatchString(host) {
		return "", "", false
	}
	return user, host, true
}

// myUserClause devuelve 'user'@'host' entrecomillado para las sentencias de usuario.
func myUserClause(user, host string) string {
	return myQuoteLiteral(user) + "@" + myQuoteLiteral(host)
}

func (m *Manager) adminMySQL(ctx context.Context, conn proto.DBConn, spec proto.DBAdminSpec) (json.RawMessage, error) {
	switch spec.Action {
	case proto.DBAdminCreateDatabase:
		if !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: nombre de base de datos no válido")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+myQuoteIdent(spec.Database)); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Base de datos %q creada.", spec.Database))

	case proto.DBAdminDropDatabase:
		if !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: nombre de base de datos no válido")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "DROP DATABASE "+myQuoteIdent(spec.Database)); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Base de datos %q eliminada.", spec.Database))

	case proto.DBAdminCreateUser:
		user, host, ok := splitMyUser(spec.Username)
		if !ok {
			return nil, fmt.Errorf("db: nombre de usuario no válido")
		}
		if !validPassword(spec.Password) {
			return nil, fmt.Errorf("db: contraseña no válida")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		sqlStr := "CREATE USER " + myUserClause(user, host) + " IDENTIFIED BY " + myQuoteLiteral(spec.Password)
		if _, err := db.ExecContext(ctx, sqlStr); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Usuario %s creado.", user+"@"+host))

	case proto.DBAdminDropUser:
		user, host, ok := splitMyUser(spec.Username)
		if !ok {
			return nil, fmt.Errorf("db: nombre de usuario no válido")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "DROP USER "+myUserClause(user, host)); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Usuario %s eliminado.", user+"@"+host))

	case proto.DBAdminChangePassword:
		user, host, ok := splitMyUser(spec.Username)
		if !ok {
			return nil, fmt.Errorf("db: nombre de usuario no válido")
		}
		if !validPassword(spec.Password) {
			return nil, fmt.Errorf("db: contraseña no válida")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		sqlStr := "ALTER USER " + myUserClause(user, host) + " IDENTIFIED BY " + myQuoteLiteral(spec.Password)
		if _, err := db.ExecContext(ctx, sqlStr); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Contraseña de %s actualizada.", user+"@"+host))

	case proto.DBAdminGrant:
		user, host, ok := splitMyUser(spec.Username)
		if !ok || !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: base de datos o usuario no válidos")
		}
		var privs string
		switch spec.Privilege {
		case proto.DBPrivReadOnly:
			privs = "SELECT"
		case proto.DBPrivReadWrite:
			privs = "SELECT, INSERT, UPDATE, DELETE"
		case proto.DBPrivAll:
			privs = "ALL PRIVILEGES"
		default:
			return nil, fmt.Errorf("db: privilegio no soportado: %q", spec.Privilege)
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		sqlStr := fmt.Sprintf("GRANT %s ON %s.* TO %s", privs, myQuoteIdent(spec.Database), myUserClause(user, host))
		if _, err := db.ExecContext(ctx, sqlStr); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Privilegios (%s) concedidos a %s sobre %q.", spec.Privilege, user+"@"+host, spec.Database))

	case proto.DBAdminRevoke:
		user, host, ok := splitMyUser(spec.Username)
		if !ok || !validIdent(spec.Database) {
			return nil, fmt.Errorf("db: base de datos o usuario no válidos")
		}
		db, err := open(ctx, conn, "", false)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		sqlStr := fmt.Sprintf("REVOKE ALL PRIVILEGES ON %s.* FROM %s", myQuoteIdent(spec.Database), myUserClause(user, host))
		if _, err := db.ExecContext(ctx, sqlStr); err != nil {
			return nil, err
		}
		return adminResult(fmt.Sprintf("Privilegios de %s revocados sobre %q.", user+"@"+host, spec.Database))
	}
	return nil, fmt.Errorf("db: acción no soportada: %q", spec.Action)
}

// ─── Utilidades ───────────────────────────────────────────────────────────────

// validPassword impone un mínimo razonable y rechaza caracteres de control (evita que
// un salto de línea o un NUL rompa el literal entrecomillado).
func validPassword(p string) bool {
	if len(p) < 1 || len(p) > 256 {
		return false
	}
	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// execAll ejecuta una lista de sentencias en orden, deteniéndose en el primer error.
func execAll(ctx context.Context, db *sql.DB, stmts []string) error {
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}
