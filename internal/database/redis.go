package database

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// Estado de Redis (Parte 3). Solo lectura: INFO + nº de claves. Se habla RESP directamente
// (sin dependencia de cliente Redis) porque solo necesitamos un puñado de campos de INFO.

// redisStatus conecta a Redis (TCP o socket), autentica si hay contraseña y devuelve el
// resumen de estado. No explora claves individuales (decisión: Redis es panel de estado).
func (m *Manager) redisStatus(ctx context.Context, conn proto.DBConn) (json.RawMessage, error) {
	network, addr := "tcp", ""
	if conn.UseLocal || conn.Socket != "" {
		network = "unix"
		addr = conn.Socket
		if addr == "" {
			addr = "/var/run/redis/redis-server.sock"
		}
	} else {
		host := conn.Host
		if host == "" {
			host = "127.0.0.1"
		}
		port := conn.Port
		if port == 0 {
			port = 6379
		}
		addr = net.JoinHostPort(host, strconv.Itoa(port))
	}

	d := net.Dialer{Timeout: 5 * time.Second}
	c, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("db: no se pudo conectar a Redis: %w", err)
	}
	defer c.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	} else {
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	}

	r := bufio.NewReader(c)
	if conn.Password != "" {
		if _, err := fmt.Fprintf(c, "AUTH %s\r\n", conn.Password); err != nil {
			return nil, err
		}
		if line, err := r.ReadString('\n'); err != nil || !strings.HasPrefix(line, "+") {
			return nil, fmt.Errorf("db: autenticación de Redis rechazada")
		}
	}

	info, err := redisInfo(c, r)
	if err != nil {
		return nil, err
	}
	return marshal(parseRedisInfo(info))
}

// redisInfo envía INFO y lee la respuesta (bulk string RESP: $<len>\r\n<data>\r\n).
func redisInfo(c net.Conn, r *bufio.Reader) (string, error) {
	if _, err := fmt.Fprint(c, "INFO\r\n"); err != nil {
		return "", err
	}
	header, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, "$") {
		return "", fmt.Errorf("db: respuesta de Redis inesperada")
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil || n < 0 {
		return "", fmt.Errorf("db: respuesta de Redis inválida")
	}
	buf := make([]byte, n)
	if _, err := readFull(r, buf); err != nil {
		return "", err
	}
	_, _ = r.Discard(2) // \r\n final
	return string(buf), nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		nn, err := r.Read(buf[total:])
		total += nn
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// parseRedisInfo extrae los campos de interés del volcado de INFO (formato "clave:valor").
func parseRedisInfo(info string) proto.DBRedisData {
	fields := map[string]string{}
	var keys int64
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.IndexByte(line, ':')
		if i < 0 {
			continue
		}
		k, v := line[:i], line[i+1:]
		fields[k] = v
		// Sección Keyspace: dbN:keys=..,expires=..,avg_ttl=..
		if strings.HasPrefix(k, "db") {
			for _, part := range strings.Split(v, ",") {
				if strings.HasPrefix(part, "keys=") {
					if kv, err := strconv.ParseInt(part[len("keys="):], 10, 64); err == nil {
						keys += kv
					}
				}
			}
		}
	}
	out := proto.DBRedisData{
		Version:     fields["redis_version"],
		Memory:      fields["used_memory_human"],
		Mode:        fields["redis_mode"],
		Keys:        keys,
		UptimeSec:   atoi64(fields["uptime_in_seconds"]),
		MemoryBytes: atoi64(fields["used_memory"]),
		Connections: atoi64(fields["connected_clients"]),
	}
	return out
}

func atoi64(s string) int64 { n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64); return n }
