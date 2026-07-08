package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// pgDSN construye la cadena de conexión para PostgreSQL (formato keyword/value que
// entiende pgx). Con UseLocal/Socket se conecta por socket unix (auth peer/local, sin
// contraseña); si no, por TCP con credenciales.
func pgDSN(c proto.DBConn, database string) string {
	if database == "" {
		database = "postgres"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "dbname=%s ", quotePg(database))
	if c.UseLocal || c.Socket != "" {
		host := "/var/run/postgresql"
		if c.Socket != "" {
			host = socketDir(c.Socket)
		}
		fmt.Fprintf(&b, "host=%s ", quotePg(host))
		if c.User != "" {
			fmt.Fprintf(&b, "user=%s ", quotePg(c.User))
		}
	} else {
		host := c.Host
		if host == "" {
			host = "127.0.0.1"
		}
		port := c.Port
		if port == 0 {
			port = 5432
		}
		fmt.Fprintf(&b, "host=%s port=%d user=%s password=%s sslmode=disable ",
			quotePg(host), port, quotePg(c.User), quotePg(c.Password))
	}
	b.WriteString("connect_timeout=8")
	return b.String()
}

// quotePg entrecomilla un valor keyword/value si contiene espacios o comillas.
func quotePg(s string) string {
	if s == "" {
		return "''"
	}
	if strings.ContainsAny(s, " '\\") {
		s = strings.ReplaceAll(s, "\\", "\\\\")
		s = strings.ReplaceAll(s, "'", "\\'")
		return "'" + s + "'"
	}
	return s
}

// socketDir devuelve el directorio del socket (.s.PGSQL.5432 → /var/run/postgresql).
func socketDir(sock string) string {
	if i := strings.LastIndex(sock, "/"); i > 0 {
		return sock[:i]
	}
	return sock
}

func (m *Manager) testPostgres(ctx context.Context, conn proto.DBConn) (json.RawMessage, error) {
	db, err := open(ctx, conn, "", false)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var ver string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&ver); err != nil {
		return nil, err
	}
	return marshal(map[string]string{"version": shortPgVersion(ver)})
}

func (m *Manager) databasesPostgres(ctx context.Context, conn proto.DBConn, readOnly bool) (json.RawMessage, error) {
	db, err := open(ctx, conn, "", readOnly)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	out := proto.DBDatabasesData{Databases: []proto.DBInfo{}, Users: []proto.DBUser{}}

	// Estado del motor.
	var ver string
	_ = db.QueryRowContext(ctx, "SELECT version()").Scan(&ver)
	out.Status = proto.DBEngineStatus{Engine: "postgres", Version: shortPgVersion(ver)}
	_ = db.QueryRowContext(ctx,
		"SELECT COALESCE(EXTRACT(EPOCH FROM now()-pg_postmaster_start_time())::bigint,0)").Scan(&out.Status.UptimeSec)
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&out.Status.Connections)

	// Bases de datos con tamaño.
	rows, err := db.QueryContext(ctx,
		`SELECT datname, pg_database_size(datname)
		 FROM pg_database WHERE datistemplate=false ORDER BY datname`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d proto.DBInfo
		if err := rows.Scan(&d.Name, &d.SizeBytes); err != nil {
			rows.Close()
			return nil, err
		}
		out.Databases = append(out.Databases, d)
	}
	rows.Close()

	// Roles (usuarios). Se ignora error de privilegios: pg_roles es legible por todos.
	urows, err := db.QueryContext(ctx,
		`SELECT rolname, rolcanlogin, rolsuper FROM pg_roles
		 WHERE rolname NOT LIKE 'pg\_%' ORDER BY rolname`)
	if err == nil {
		for urows.Next() {
			var u proto.DBUser
			if err := urows.Scan(&u.Name, &u.CanLogin, &u.Superuser); err == nil {
				if u.Superuser {
					u.Privileges = "SUPERUSER"
				}
				out.Users = append(out.Users, u)
			}
		}
		urows.Close()
	}
	return marshal(out)
}

func (m *Manager) tablesPostgres(ctx context.Context, conn proto.DBConn, database string, readOnly bool) (json.RawMessage, error) {
	if database == "" {
		return nil, fmt.Errorf("db: falta la base de datos")
	}
	db, err := open(ctx, conn, database, readOnly)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		`SELECT n.nspname, c.relname,
		        pg_total_relation_size(c.oid),
		        COALESCE(s.n_live_tup, c.reltuples::bigint, 0)
		 FROM pg_class c
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
		 WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')
		 ORDER BY pg_total_relation_size(c.oid) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := []proto.DBTable{}
	for rows.Next() {
		var t proto.DBTable
		if err := rows.Scan(&t.Schema, &t.Name, &t.SizeBytes, &t.RowsEst); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return marshal(tables)
}

// shortPgVersion extrae "PostgreSQL 16.3" de la cadena larga de version().
func shortPgVersion(v string) string {
	f := strings.Fields(v)
	if len(f) >= 2 {
		return f[0] + " " + f[1]
	}
	return v
}

// atoiSafe convierte a int ignorando errores (para SHOW STATUS de mysql).
func atoiSafe(s string) int { n, _ := strconv.Atoi(strings.TrimSpace(s)); return n }
