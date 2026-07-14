// Package database es el cliente de bases de datos del agente (AuraNode Parte 3 · D1+).
// El agente NUNCA administra el sistema: solo se conecta como cliente a los motores
// locales (MySQL/MariaDB, PostgreSQL) con drivers Go puros para detectarlos, explorar
// su esquema y (más adelante) ejecutar consultas acotadas. Es inerte sin configuración
// y las credenciales que recibe son efímeras (jamás se persisten en la VPS).
package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"

	_ "github.com/go-sql-driver/mysql"   // driver mysql (database/sql)
	_ "github.com/jackc/pgx/v5/stdlib"   // driver postgres (database/sql, nombre "pgx")
)

// Manager atiende las peticiones db_* y responde por el WS.
type Manager struct {
	log  *zap.Logger
	send func(any) error
}

func NewManager(log *zap.Logger) *Manager { return &Manager{log: log} }

// SetSend instala la función de envío al backend (patrón de los demás managers).
func (m *Manager) SetSend(fn func(any) error) { m.send = fn }

// Handle procesa una petición en su propia goroutine y responde best-effort.
func (m *Manager) Handle(req proto.DBRequest) {
	go func() {
		data, err := m.dispatch(req)
		resp := proto.DBResponse{
			Envelope:  proto.Envelope{Type: proto.TypeDBResponse, Timestamp: time.Now().UnixMilli()},
			RequestID: req.RequestID,
			OK:        err == nil,
			Data:      data,
		}
		if err != nil {
			resp.Error = err.Error()
		}
		if m.send != nil {
			_ = m.send(resp)
		}
	}()
}

func (m *Manager) dispatch(req proto.DBRequest) (json.RawMessage, error) {
	// Los dumps y restores pueden tardar bastante en BDs grandes: se les da un timeout
	// mucho más amplio que a la exploración/consola.
	timeout := 30 * time.Second
	switch req.Op {
	case proto.DBOpDump, proto.DBOpRestore:
		timeout = dbBackupTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch req.Op {
	case proto.DBOpDetect:
		return marshal(detect(ctx))
	case proto.DBOpTest:
		return m.test(ctx, req.Conn)
	case proto.DBOpDatabases:
		return m.databases(ctx, req.Conn, req.ReadOnly)
	case proto.DBOpTables:
		return m.tables(ctx, req.Conn, req.Database, req.ReadOnly)
	case proto.DBOpQuery:
		return m.query(ctx, req)
	case proto.DBOpAdmin:
		return m.admin(ctx, req)
	case proto.DBOpDump:
		return m.dump(ctx, req)
	case proto.DBOpDumps:
		return m.dumps(req)
	case proto.DBOpRestore:
		return m.restore(ctx, req)
	case proto.DBOpDumpDel:
		return m.dumpDelete(req)
	case proto.DBOpRedis:
		return m.redisStatus(ctx, req.Conn)
	default:
		return nil, fmt.Errorf("db: op no soportada: %q", req.Op)
	}
}

// ─── Detección (sin credenciales) ─────────────────────────────────────────────

// detect sondea los motores locales típicos por TCP loopback y por socket unix. No
// autentica: solo reporta si hay algo escuchando (running). La versión se obtiene
// luego en test/databases (requiere credenciales).
func detect(ctx context.Context) []proto.DetectedEngine {
	out := []proto.DetectedEngine{}

	pgSockets := []string{"/var/run/postgresql/.s.PGSQL.5432", "/tmp/.s.PGSQL.5432"}
	out = append(out, probe("postgres", 5432, pgSockets))

	mySockets := []string{"/var/run/mysqld/mysqld.sock", "/run/mysqld/mysqld.sock", "/tmp/mysql.sock"}
	out = append(out, probe("mysql", 3306, mySockets))

	// Redis: solo estado (no exploración en D1); útil que aparezca como detectado.
	out = append(out, probe("redis", 6379, []string{"/var/run/redis/redis-server.sock", "/run/redis/redis.sock"}))

	// MongoDB: exploración + gestión + backups (mongodump).
	out = append(out, probe("mongodb", 27017, []string{"/tmp/mongodb-27017.sock", "/var/run/mongodb/mongod.sock", "/run/mongodb/mongod.sock"}))

	return out
}

// probe comprueba TCP loopback y sockets; running=true si cualquiera responde/existe.
func probe(engine string, port int, sockets []string) proto.DetectedEngine {
	e := proto.DetectedEngine{Engine: engine, Port: port}
	// TCP loopback.
	d := net.Dialer{Timeout: 800 * time.Millisecond}
	if c, err := d.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
		_ = c.Close()
		e.Running = true
	}
	// Socket unix (aunque el TCP esté cerrado, muchos motores solo escuchan en socket).
	for _, s := range sockets {
		if fi, err := os.Stat(s); err == nil && fi.Mode()&os.ModeSocket != 0 {
			e.Running = true
			e.Socket = s
			break
		}
	}
	return e
}

// ─── Apertura de conexión ─────────────────────────────────────────────────────

// open abre una conexión database/sql al motor con las credenciales dadas, apuntando
// a `database` (vacío = BD administrativa por defecto). Si readOnly, impone lectura en
// la conexión (defensa a nivel de sesión, no por parseo de SQL — decisión C8).
func open(ctx context.Context, conn proto.DBConn, database string, readOnly bool) (*sql.DB, error) {
	var driver, dsn string
	switch conn.Engine {
	case "postgres":
		driver, dsn = "pgx", pgDSN(conn, database)
	case "mysql":
		driver, dsn = "mysql", myDSN(conn, database)
	default:
		return nil, fmt.Errorf("db: motor no soportado: %q", conn.Engine)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(2 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, err
	}

	if readOnly {
		stmt := "SET default_transaction_read_only = on"
		if conn.Engine == "mysql" {
			stmt = "SET SESSION transaction_read_only = ON"
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// No fatal: muchos motores lo aceptan; si no, las consultas siguen siendo
			// SELECT en la exploración. Se registra para diagnóstico.
			db.Close()
			return nil, fmt.Errorf("no se pudo imponer solo-lectura: %w", err)
		}
	}
	return db, nil
}

// ─── Consola SQL (D2) ─────────────────────────────────────────────────────────

const (
	dbQueryMaxRows  = 1000    // tope de filas devueltas (el agente trunca, no el backend)
	dbQueryMaxBytes = 1 << 20 // 1 MiB de celdas por respuesta
)

// query ejecuta UN statement SQL en la conexión y devuelve las filas como texto. La
// solo-lectura se impone en la conexión (readOnly, decisión C8), no parseando SQL. Un
// solo statement por ejecución lo garantizan los drivers: pgx usa el protocolo extendido
// y mysql no habilita multiStatements, así que ambos rechazan varios comandos. Los límites
// (filas/bytes/timeout) protegen al VPS: el agente trunca, no el backend.
func (m *Manager) query(ctx context.Context, req proto.DBRequest) (json.RawMessage, error) {
	stmt := strings.TrimSpace(req.SQL)
	if stmt == "" {
		return nil, fmt.Errorf("db: SQL vacío")
	}
	maxRows := req.MaxRows
	if maxRows <= 0 || maxRows > dbQueryMaxRows {
		maxRows = dbQueryMaxRows
	}

	db, err := open(ctx, req.Conn, req.Database, req.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	start := time.Now()
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := proto.DBQueryData{Columns: cols, Rows: [][]*string{}, ReadOnly: req.ReadOnly}

	if len(cols) > 0 {
		raw := make([]sql.RawBytes, len(cols))
		scan := make([]any, len(cols))
		for i := range raw {
			scan[i] = &raw[i]
		}
		bytesUsed := 0
		for rows.Next() {
			if len(out.Rows) >= maxRows {
				out.Truncated = true
				break
			}
			if err := rows.Scan(scan...); err != nil {
				return nil, err
			}
			row := make([]*string, len(cols))
			for i := range raw {
				if raw[i] == nil {
					continue // NULL → null en el JSON
				}
				s := string(raw[i]) // copia: RawBytes deja de ser válido en el siguiente Next()
				bytesUsed += len(s)
				row[i] = &s
			}
			out.Rows = append(out.Rows, row)
			if bytesUsed > dbQueryMaxBytes {
				out.Truncated = true
				break
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	out.RowsReturned = len(out.Rows)
	out.DurationMS = time.Since(start).Milliseconds()
	return marshal(out)
}

func marshal(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	return b, err
}
