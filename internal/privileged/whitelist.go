// Package privileged implementa el modo privilegiado acotado (variante A).
//
// Modelo de seguridad:
//   - El agente principal corre SIN privilegios. NUNCA ejecuta root directamente.
//   - Un helper root separado (auranode-agent-helper) escucha en un socket Unix
//     local y ejecuta ÚNICAMENTE las acciones de esta whitelist, con argumentos
//     estrictamente validados y SIN shell (argv explícito → sin inyección).
//   - Toda acción se resuelve aquí (resolve), de modo que el helper revalida del
//     lado servidor aunque el agente estuviera comprometido (defensa en profundidad).
//
// Esto NO es "sudo libre": no hay forma de pasar un comando arbitrario.
package privileged

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

// SocketPath es el socket Unix donde escucha el helper root. El directorio
// /run/auranode lo crea systemd (RuntimeDirectory) y el helper ajusta el socket a
// grupo `auranode`, modo 0660, para que solo el agente local pueda hablarle.
const SocketPath = "/run/auranode/helper.sock"

// Request es lo que el agente envía al helper por el socket.
type Request struct {
	Action string            `json:"action"`
	Args   map[string]string `json:"args,omitempty"`
}

// Response es lo que el helper devuelve.
type Response struct {
	OK         bool   `json:"ok"`
	Rejected   bool   `json:"rejected,omitempty"` // bloqueado por una guarda (no se ejecutó)
	Error      string `json:"error,omitempty"`
	ExitStatus int    `json:"exit_status"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	DurationMS int64  `json:"duration_ms"`
}

// Acciones de la whitelist. Son claves estables que el backend y el panel también
// conocen; los argumentos van validados.
const (
	ActionPkgUpdate      = "pkg_update"      // refrescar índices de paquetes
	ActionPkgUpgrade     = "pkg_upgrade"     // actualizar todos los paquetes
	ActionPkgInstall     = "pkg_install"     // instalar paquete(s) — arg "packages"
	ActionPkgAutoremove  = "pkg_autoremove"  // limpiar paquetes huérfanos
	ActionServiceStatus  = "service_status"  // estado de un servicio (solo lectura) — arg "unit"
	ActionServiceStart   = "service_start"   // arrancar un servicio — arg "unit"
	ActionServiceStop    = "service_stop"    // detener un servicio — arg "unit"
	ActionServiceReload  = "service_reload"  // recargar un servicio — arg "unit"
	ActionServiceRestart = "service_restart" // reiniciar un servicio — arg "unit"
)

// Action describe una acción de la whitelist (metadatos para el panel).
type Action struct {
	Key         string `json:"key"`
	NeedsArg    string `json:"needs_arg,omitempty"`   // nombre del argumento requerido ("packages"|"unit")
	Destructive bool   `json:"destructive,omitempty"` // requiere confirmación reforzada en el panel
}

// Catalog es la lista pública de acciones permitidas (la consume el panel).
var Catalog = []Action{
	{Key: ActionPkgUpdate},
	{Key: ActionPkgUpgrade, Destructive: true},
	{Key: ActionPkgInstall, NeedsArg: "packages"},
	{Key: ActionPkgAutoremove, Destructive: true},
	{Key: ActionServiceStatus, NeedsArg: "unit"},
	{Key: ActionServiceStart, NeedsArg: "unit"},
	{Key: ActionServiceStop, NeedsArg: "unit", Destructive: true},
	{Key: ActionServiceReload, NeedsArg: "unit"},
	{Key: ActionServiceRestart, NeedsArg: "unit", Destructive: true},
}

var (
	pkgRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9+._-]{0,99}$`)
	unitRe = regexp.MustCompile(`^[a-zA-Z0-9@:._-]{1,100}(\.(service|socket|timer|target|path))?$`)
)

// selfUnits: jamás se gestionan desde aquí (administrarías al administrador / te
// cortarías a ti mismo el helper).
var selfUnits = map[string]bool{
	"auranode-agent":        true,
	"auranode-agent-helper": true,
}

// criticalStopUnits: detener estos dejaría la VPS inaccesible o sin gestión.
// Se bloquea STOP (reiniciar sigue permitido salvo selfUnits).
var criticalStopUnits = map[string]bool{
	"ssh":              true,
	"sshd":             true,
	"systemd-journald": true,
	"systemd-logind":   true,
	"dbus":             true,
	"networkmanager":   true,
	"networking":       true,
	"systemd-networkd": true,
	"systemd-resolved": true,
}

// unitBase normaliza "nginx.service" → "nginx" (en minúsculas) para comparar guardas.
func unitBase(unit string) string {
	u := strings.ToLower(unit)
	for _, suf := range []string{".service", ".socket", ".timer", ".target", ".path"} {
		u = strings.TrimSuffix(u, suf)
	}
	return u
}

// detectPkgManager elige el gestor de paquetes disponible.
func detectPkgManager() (string, error) {
	for _, pm := range []string{"apt-get", "dnf", "yum"} {
		if _, err := exec.LookPath(pm); err == nil {
			return pm, nil
		}
	}
	return "", fmt.Errorf("no se encontró un gestor de paquetes soportado (apt-get/dnf/yum)")
}

// plan es el resultado de resolver una acción: el argv exacto y el entorno extra.
type plan struct {
	argv       []string
	env        []string
	timeoutSec int
}

// resolve traduce (action, args) → comando concreto aplicando TODAS las guardas.
// Devuelve un error si la acción no existe, los argumentos son inválidos o una
// guarda la bloquea. NUNCA usa shell: el primer elemento es el binario y el resto
// argumentos literales.
func resolve(action string, args map[string]string) (plan, error) {
	switch action {
	case ActionPkgUpdate, ActionPkgUpgrade, ActionPkgInstall, ActionPkgAutoremove:
		return resolvePackages(action, args)
	case ActionServiceStatus, ActionServiceStart, ActionServiceStop, ActionServiceReload, ActionServiceRestart:
		return resolveService(action, args)
	default:
		return plan{}, fmt.Errorf("acción no permitida: %q", action)
	}
}

func resolvePackages(action string, args map[string]string) (plan, error) {
	pm, err := detectPkgManager()
	if err != nil {
		return plan{}, err
	}
	env := []string{"DEBIAN_FRONTEND=noninteractive"}

	switch action {
	case ActionPkgUpdate:
		if pm == "apt-get" {
			return plan{argv: []string{pm, "update"}, env: env, timeoutSec: 300}, nil
		}
		return plan{argv: []string{pm, "-y", "makecache"}, env: env, timeoutSec: 300}, nil

	case ActionPkgUpgrade:
		if pm == "yum" {
			return plan{argv: []string{pm, "-y", "update"}, env: env, timeoutSec: 1800}, nil
		}
		return plan{argv: []string{pm, "-y", "upgrade"}, env: env, timeoutSec: 1800}, nil

	case ActionPkgAutoremove:
		return plan{argv: []string{pm, "-y", "autoremove"}, env: env, timeoutSec: 600}, nil

	case ActionPkgInstall:
		raw := strings.TrimSpace(args["packages"])
		if raw == "" {
			return plan{}, fmt.Errorf("falta el argumento 'packages'")
		}
		fields := strings.FieldsFunc(raw, func(r rune) bool { return r == ' ' || r == ',' || r == '\t' })
		if len(fields) == 0 {
			return plan{}, fmt.Errorf("no se indicó ningún paquete")
		}
		if len(fields) > 20 {
			return plan{}, fmt.Errorf("demasiados paquetes (máx. 20)")
		}
		pkgs := make([]string, 0, len(fields))
		for _, p := range fields {
			if !pkgRe.MatchString(p) {
				return plan{}, fmt.Errorf("nombre de paquete inválido: %q", p)
			}
			pkgs = append(pkgs, p)
		}
		argv := append([]string{pm, "-y", "install"}, pkgs...)
		return plan{argv: argv, env: env, timeoutSec: 1800}, nil
	}
	return plan{}, fmt.Errorf("acción de paquetes no permitida: %q", action)
}

func resolveService(action string, args map[string]string) (plan, error) {
	unit := strings.TrimSpace(args["unit"])
	if unit == "" {
		return plan{}, fmt.Errorf("falta el argumento 'unit'")
	}
	if !unitRe.MatchString(unit) {
		return plan{}, fmt.Errorf("nombre de servicio inválido: %q", unit)
	}
	base := unitBase(unit)

	if selfUnits[base] {
		return plan{}, fmt.Errorf("no se permite gestionar el servicio %q (el agente se administra solo)", base)
	}

	verb := map[string]string{
		ActionServiceStatus:  "status",
		ActionServiceStart:   "start",
		ActionServiceStop:    "stop",
		ActionServiceReload:  "reload",
		ActionServiceRestart: "restart",
	}[action]

	if (action == ActionServiceStop) && criticalStopUnits[base] {
		return plan{}, fmt.Errorf("detener %q dejaría el servidor inaccesible o sin gestión; acción bloqueada", base)
	}

	if action == ActionServiceStatus {
		// status no escribe nada; --no-pager para salida acotada.
		return plan{argv: []string{"systemctl", "status", "--no-pager", "--lines=40", unit}, timeoutSec: 30}, nil
	}
	return plan{argv: []string{"systemctl", verb, unit}, timeoutSec: 120}, nil
}
