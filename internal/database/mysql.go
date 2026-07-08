package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// myDSN construye el DSN de go-sql-driver/mysql. Con UseLocal/Socket se conecta por
// socket unix; si no, por TCP con credenciales. parseTime desactivado (no lo necesita
// la exploración) y timeouts acotados.
func myDSN(c proto.DBConn, database string) string {
	params := "?timeout=8s&readTimeout=25s&writeTimeout=8s&charset=utf8mb4"
	if c.UseLocal || c.Socket != "" {
		sock := c.Socket
		if sock == "" {
			sock = "/var/run/mysqld/mysqld.sock"
		}
		user := c.User
		if user == "" {
			user = "root"
		}
		return fmt.Sprintf("%s:%s@unix(%s)/%s%s", user, c.Password, sock, database, params)
	}
	host := c.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Port
	if port == 0 {
		port = 3306
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s%s", c.User, c.Password, host, port, database, params)
}

func (m *Manager) testMySQL(ctx context.Context, conn proto.DBConn) (json.RawMessage, error) {
	db, err := open(ctx, conn, "", false)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var ver string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&ver); err != nil {
		return nil, err
	}
	return marshal(map[string]string{"version": "MySQL " + ver})
}

func (m *Manager) databasesMySQL(ctx context.Context, conn proto.DBConn, readOnly bool) (json.RawMessage, error) {
	db, err := open(ctx, conn, "", readOnly)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	out := proto.DBDatabasesData{Databases: []proto.DBInfo{}, Users: []proto.DBUser{}}

	var ver string
	_ = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&ver)
	out.Status = proto.DBEngineStatus{Engine: "mysql", Version: "MySQL " + ver}
	{
		var name, val string
		if err := db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Uptime'").Scan(&name, &val); err == nil {
			out.Status.UptimeSec = int64(atoiSafe(val))
		}
		if err := db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Threads_connected'").Scan(&name, &val); err == nil {
			out.Status.Connections = atoiSafe(val)
		}
	}

	// Tamaño por esquema (las BDs sin tablas no aparecen en information_schema.tables,
	// así que se listan todas por separado y se les asigna el tamaño si lo hay).
	sizes := map[string]int64{}
	if rows, err := db.QueryContext(ctx,
		`SELECT table_schema, COALESCE(SUM(data_length+index_length),0)
		 FROM information_schema.tables GROUP BY table_schema`); err == nil {
		for rows.Next() {
			var name string
			var size int64
			if err := rows.Scan(&name, &size); err == nil {
				sizes[name] = size
			}
		}
		rows.Close()
	}
	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		out.Databases = append(out.Databases, proto.DBInfo{Name: name, SizeBytes: sizes[name]})
	}
	rows.Close()

	// Usuarios (requiere privilegio sobre mysql.user; si falla se omite sin error).
	if urows, err := db.QueryContext(ctx,
		`SELECT User, Host FROM mysql.user ORDER BY User`); err == nil {
		for urows.Next() {
			var user, host string
			if err := urows.Scan(&user, &host); err == nil {
				out.Users = append(out.Users, proto.DBUser{Name: user + "@" + host, CanLogin: true})
			}
		}
		urows.Close()
	}
	return marshal(out)
}

func (m *Manager) tablesMySQL(ctx context.Context, conn proto.DBConn, database string, readOnly bool) (json.RawMessage, error) {
	if database == "" {
		return nil, fmt.Errorf("db: falta la base de datos")
	}
	db, err := open(ctx, conn, database, readOnly)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		`SELECT table_name, COALESCE(data_length+index_length,0), COALESCE(table_rows,0)
		 FROM information_schema.tables
		 WHERE table_schema = ?
		 ORDER BY (data_length+index_length) DESC`, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := []proto.DBTable{}
	for rows.Next() {
		var t proto.DBTable
		if err := rows.Scan(&t.Name, &t.SizeBytes, &t.RowsEst); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return marshal(tables)
}

// ─── dispatch por motor ───────────────────────────────────────────────────────

func (m *Manager) test(ctx context.Context, conn proto.DBConn) (json.RawMessage, error) {
	switch conn.Engine {
	case "postgres":
		return m.testPostgres(ctx, conn)
	case "mysql":
		return m.testMySQL(ctx, conn)
	}
	return nil, fmt.Errorf("db: motor no soportado: %q", conn.Engine)
}

func (m *Manager) databases(ctx context.Context, conn proto.DBConn, readOnly bool) (json.RawMessage, error) {
	switch conn.Engine {
	case "postgres":
		return m.databasesPostgres(ctx, conn, readOnly)
	case "mysql":
		return m.databasesMySQL(ctx, conn, readOnly)
	}
	return nil, fmt.Errorf("db: motor no soportado: %q", conn.Engine)
}

func (m *Manager) tables(ctx context.Context, conn proto.DBConn, database string, readOnly bool) (json.RawMessage, error) {
	switch strings.ToLower(conn.Engine) {
	case "postgres":
		return m.tablesPostgres(ctx, conn, database, readOnly)
	case "mysql":
		return m.tablesMySQL(ctx, conn, database, readOnly)
	}
	return nil, fmt.Errorf("db: motor no soportado: %q", conn.Engine)
}
