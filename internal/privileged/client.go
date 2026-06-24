package privileged

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// Available indica si el helper root está instalado y escuchando (es decir, si el
// usuario ejecutó `--enable-privileged` en la VPS). Lo reporta el agente al backend.
func Available() bool {
	fi, err := os.Stat(SocketPath)
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeSocket != 0
}

// Execute envía una acción al helper y espera el resultado. El agente principal
// (sin privilegios) llama a esto; la ejecución real ocurre en el helper root, que
// revalida la acción contra la whitelist.
func Execute(req Request) Response {
	if !Available() {
		return Response{Error: "el modo privilegiado no está instalado en este servidor"}
	}

	conn, err := net.DialTimeout("unix", SocketPath, 5*time.Second)
	if err != nil {
		return Response{Error: fmt.Sprintf("no se pudo contactar al helper: %v", err)}
	}
	defer conn.Close()

	// El helper puede tardar (apt upgrade): deadline amplio.
	_ = conn.SetDeadline(time.Now().Add(31 * time.Minute))

	body, _ := json.Marshal(req)
	if _, err := conn.Write(body); err != nil {
		return Response{Error: fmt.Sprintf("envío al helper falló: %v", err)}
	}
	// Cerrar el lado de escritura para que el helper sepa que terminó la petición.
	if uc, ok := conn.(*net.UnixConn); ok {
		_ = uc.CloseWrite()
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(conn, 256*1024)); err != nil {
		return Response{Error: fmt.Sprintf("lectura de la respuesta del helper falló: %v", err)}
	}

	var resp Response
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &resp); err != nil {
		return Response{Error: "respuesta del helper ilegible"}
	}
	return resp
}
