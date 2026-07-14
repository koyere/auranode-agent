package database

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// Backups de bases de datos (Parte 3 · D4). El agente crea dumps comprimidos con las
// herramientas nativas (pg_dump/mysqldump) hacia un directorio del VPS y restaura desde
// ellos. Los dumps se descargan por el módulo de archivos existente (misma ruta). Es la
// única parte del cliente de BD que usa binarios CLI (el resto son drivers Go puros).

const dbBackupTimeout = 30 * time.Minute // dumps/restores pueden tardar en BDs grandes

// backupDir es el directorio donde viven los dumps (configurable; por defecto junto al
// resto del estado del agente).
func backupDir() string {
	if d := os.Getenv("AURANODE_DB_BACKUP_DIR"); d != "" {
		return d
	}
	return "/var/lib/auranode/db-backups"
}

func enginePrefix(engine string) string {
	if engine == "postgres" {
		return "pg"
	}
	return "my"
}

// ─── Crear dump ───────────────────────────────────────────────────────────────

func (m *Manager) dump(ctx context.Context, req proto.DBRequest) (json.RawMessage, error) {
	conn := req.Conn
	dbname := req.Database
	if !validIdent(dbname) {
		return nil, fmt.Errorf("db: nombre de base de datos no válido")
	}
	var tool string
	switch conn.Engine {
	case "postgres":
		tool = "pg_dump"
	case "mysql":
		tool = "mysqldump"
	default:
		return nil, fmt.Errorf("db: motor no soportado: %q", conn.Engine)
	}
	bin, err := exec.LookPath(tool)
	if err != nil {
		return nil, fmt.Errorf("db: %s no está instalado en el servidor", tool)
	}

	dir := backupDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("db: no se pudo crear el directorio de backups: %w", err)
	}
	fname := fmt.Sprintf("%s_%s_%s.sql.gz", enginePrefix(conn.Engine), dbname, time.Now().Format("20060102-150405"))
	full := filepath.Join(dir, fname)

	args, env := dumpCommand(conn, dbname)
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), env...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	f, err := os.OpenFile(full, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	gz := gzip.NewWriter(f)

	start := time.Now()
	if err := cmd.Start(); err != nil {
		gz.Close()
		f.Close()
		os.Remove(full)
		return nil, err
	}
	setNice(cmd.Process.Pid)
	_, copyErr := io.Copy(gz, stdout)
	waitErr := cmd.Wait()
	gzErr := gz.Close()
	f.Close()

	if waitErr != nil || copyErr != nil || gzErr != nil {
		os.Remove(full)
		if waitErr != nil {
			return nil, fmt.Errorf("db: el dump falló: %s", strings.TrimSpace(shorten(stderr.String(), 4096)))
		}
		return nil, fmt.Errorf("db: el dump falló al escribir: %v", firstErr(copyErr, gzErr))
	}

	fi, err := os.Stat(full)
	if err != nil {
		return nil, err
	}
	return marshal(proto.DBDumpData{
		File:       fname,
		SizeBytes:  fi.Size(),
		DurationMS: time.Since(start).Milliseconds(),
		Message:    fmt.Sprintf("Dump de %q creado (%s).", dbname, humanSize(fi.Size())),
	})
}

// dumpCommand construye los argumentos y el entorno para pg_dump/mysqldump. La contraseña
// viaja por variable de entorno (no en la línea de comandos, para que no aparezca en ps).
func dumpCommand(c proto.DBConn, dbname string) (args, env []string) {
	switch c.Engine {
	case "postgres":
		args = []string{"--no-owner", "--no-privileges"}
		if c.UseLocal || c.Socket != "" {
			host := "/var/run/postgresql"
			if c.Socket != "" {
				host = socketDir(c.Socket)
			}
			args = append(args, "-h", host)
		} else {
			host := c.Host
			if host == "" {
				host = "127.0.0.1"
			}
			port := c.Port
			if port == 0 {
				port = 5432
			}
			args = append(args, "-h", host, "-p", strconv.Itoa(port))
			env = append(env, "PGPASSWORD="+c.Password)
		}
		if c.User != "" {
			args = append(args, "-U", c.User)
		}
		args = append(args, "-d", dbname)
	case "mysql":
		args = []string{"--single-transaction", "--quick", "--no-tablespaces"}
		if c.UseLocal || c.Socket != "" {
			sock := c.Socket
			if sock == "" {
				sock = "/var/run/mysqld/mysqld.sock"
			}
			user := c.User
			if user == "" {
				user = "root"
			}
			args = append(args, "--socket="+sock, "-u", user)
		} else {
			host := c.Host
			if host == "" {
				host = "127.0.0.1"
			}
			port := c.Port
			if port == 0 {
				port = 3306
			}
			args = append(args, "-h", host, "-P", strconv.Itoa(port), "-u", c.User)
		}
		if c.Password != "" {
			env = append(env, "MYSQL_PWD="+c.Password)
		}
		args = append(args, dbname)
	}
	return args, env
}

// ─── Restaurar ────────────────────────────────────────────────────────────────

func (m *Manager) restore(ctx context.Context, req proto.DBRequest) (json.RawMessage, error) {
	conn := req.Conn
	dbname := req.Database
	if !validIdent(dbname) {
		return nil, fmt.Errorf("db: nombre de base de datos no válido")
	}
	full, err := safeDumpPath(req.DumpFile)
	if err != nil {
		return nil, err
	}
	var tool string
	switch conn.Engine {
	case "postgres":
		tool = "psql"
	case "mysql":
		tool = "mysql"
	default:
		return nil, fmt.Errorf("db: motor no soportado: %q", conn.Engine)
	}
	bin, err := exec.LookPath(tool)
	if err != nil {
		return nil, fmt.Errorf("db: %s no está instalado en el servidor", tool)
	}

	f, err := os.Open(full)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("db: el dump no es un gzip válido: %w", err)
	}
	defer gz.Close()

	args, env := restoreCommand(conn, dbname)
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdin = gz
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	start := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	setNice(cmd.Process.Pid)
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("db: la restauración falló: %s", strings.TrimSpace(shorten(stderr.String(), 4096)))
	}
	return marshal(proto.DBDumpData{
		File:       filepath.Base(full),
		DurationMS: time.Since(start).Milliseconds(),
		Message:    fmt.Sprintf("Base de datos %q restaurada desde %q.", dbname, filepath.Base(full)),
	})
}

func restoreCommand(c proto.DBConn, dbname string) (args, env []string) {
	switch c.Engine {
	case "postgres":
		args = []string{"--set=ON_ERROR_STOP=1", "-q"}
		if c.UseLocal || c.Socket != "" {
			host := "/var/run/postgresql"
			if c.Socket != "" {
				host = socketDir(c.Socket)
			}
			args = append(args, "-h", host)
		} else {
			host := c.Host
			if host == "" {
				host = "127.0.0.1"
			}
			port := c.Port
			if port == 0 {
				port = 5432
			}
			args = append(args, "-h", host, "-p", strconv.Itoa(port))
			env = append(env, "PGPASSWORD="+c.Password)
		}
		if c.User != "" {
			args = append(args, "-U", c.User)
		}
		args = append(args, "-d", dbname)
	case "mysql":
		if c.UseLocal || c.Socket != "" {
			sock := c.Socket
			if sock == "" {
				sock = "/var/run/mysqld/mysqld.sock"
			}
			user := c.User
			if user == "" {
				user = "root"
			}
			args = append(args, "--socket="+sock, "-u", user)
		} else {
			host := c.Host
			if host == "" {
				host = "127.0.0.1"
			}
			port := c.Port
			if port == 0 {
				port = 3306
			}
			args = append(args, "-h", host, "-P", strconv.Itoa(port), "-u", c.User)
		}
		if c.Password != "" {
			env = append(env, "MYSQL_PWD="+c.Password)
		}
		args = append(args, dbname)
	}
	return args, env
}

// ─── Listar / eliminar ────────────────────────────────────────────────────────

func (m *Manager) dumps(_ proto.DBRequest) (json.RawMessage, error) {
	dir := backupDir()
	out := proto.DBDumpsData{Dir: dir, Dumps: []proto.DBDumpInfo{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return marshal(out) // aún no hay dumps
		}
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql.gz") {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		engine, db := parseDumpName(e.Name())
		out.Dumps = append(out.Dumps, proto.DBDumpInfo{
			File: e.Name(), Database: db, Engine: engine,
			SizeBytes: fi.Size(), ModifiedUnix: fi.ModTime().Unix(),
			Path: filepath.Join(dir, e.Name()),
		})
	}
	return marshal(out)
}

func (m *Manager) dumpDelete(req proto.DBRequest) (json.RawMessage, error) {
	full, err := safeDumpPath(req.DumpFile)
	if err != nil {
		return nil, err
	}
	if err := os.Remove(full); err != nil {
		return nil, err
	}
	return marshal(proto.DBDumpData{File: filepath.Base(full), Message: "Dump eliminado."})
}

// ─── Utilidades ───────────────────────────────────────────────────────────────

// safeDumpPath valida que el nombre no escape del directorio de backups y que exista.
func safeDumpPath(name string) (string, error) {
	if name == "" || name != filepath.Base(name) || !strings.HasSuffix(name, ".sql.gz") {
		return "", fmt.Errorf("db: nombre de dump no válido")
	}
	full := filepath.Join(backupDir(), name)
	if _, err := os.Stat(full); err != nil {
		return "", fmt.Errorf("db: dump no encontrado")
	}
	return full, nil
}

// parseDumpName extrae (engine, database) de "<pg|my>_<db>_<ts>.sql.gz". El nombre de la
// BD puede llevar guiones bajos; el prefijo y el timestamp son el primero y el último.
func parseDumpName(name string) (engine, db string) {
	base := strings.TrimSuffix(name, ".sql.gz")
	parts := strings.Split(base, "_")
	if len(parts) < 3 {
		return "", base
	}
	switch parts[0] {
	case "pg":
		engine = "postgres"
	case "my":
		engine = "mysql"
	}
	db = strings.Join(parts[1:len(parts)-1], "_")
	return engine, db
}

// setNice baja la prioridad de CPU del proceso hijo (best-effort) para no penalizar al
// VPS durante un dump/restore grande.
func setNice(pid int) {
	_ = syscall.Setpriority(syscall.PRIO_PROCESS, pid, 19)
}

func shorten(s string, max int) string {
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
