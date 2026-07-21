package collector

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/process"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// SystemHealth reúne señales de salud del sistema, todas de solo lectura. Es de baja
// frecuencia (en cada conexión y cada hora): puede ejecutar apt-check/systemctl, que
// tardan algo más que leer /proc, pero nunca cambian el estado del sistema.
func (c *Collector) SystemHealth() proto.SystemHealth {
	h := proto.SystemHealth{
		Envelope: proto.Envelope{
			Type:      proto.TypeSystemHealth,
			Timestamp: time.Now().Unix(),
		},
		ZombieProcesses: countZombies(),
		RebootRequired:  fileExists("/var/run/reboot-required") || fileExists("/run/reboot-required"),
	}
	h.PendingUpdates, h.SecurityUpdates = pendingUpdates()
	h.FailedUnits = failedUnits()
	return h
}

// countZombies cuenta procesos en estado zombie (Z). gopsutil expone el estado como
// una lista de letras de /proc/<pid>/stat.
func countZombies() int {
	procs, err := process.Processes()
	if err != nil {
		return 0
	}
	n := 0
	for _, p := range procs {
		st, err := p.Status()
		if err != nil {
			continue
		}
		for _, s := range st {
			if s == process.Zombie || strings.EqualFold(s, "z") {
				n++
				break
			}
		}
	}
	return n
}

// pendingUpdates devuelve (total, seguridad) de paquetes actualizables en Debian/Ubuntu
// vía update-notifier (apt-check imprime "updates;security" en stderr, sin root). En
// otras plataformas devuelve (-1, -1) para indicar "desconocido".
func pendingUpdates() (int, int) {
	const aptCheck = "/usr/lib/update-notifier/apt-check"
	if !fileExists(aptCheck) {
		return -1, -1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	// apt-check escribe el resultado en stderr con formato "N;M".
	out, err := exec.CommandContext(ctx, aptCheck).CombinedOutput()
	if err != nil && len(out) == 0 {
		return -1, -1
	}
	// Buscar la última línea con el patrón "N;M" (apt-check puede emitir warnings antes).
	total, sec := -1, -1
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		a, b, ok := strings.Cut(line, ";")
		if !ok {
			continue
		}
		ai, err1 := strconv.Atoi(strings.TrimSpace(a))
		bi, err2 := strconv.Atoi(strings.TrimSpace(b))
		if err1 == nil && err2 == nil {
			total, sec = ai, bi
		}
	}
	return total, sec
}

// failedUnits cuenta units de systemd en estado failed (read-only). Devuelve -1 si
// systemctl no está disponible.
func failedUnits() int {
	path, err := exec.LookPath("systemctl")
	if err != nil {
		return -1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path,
		"list-units", "--state=failed", "--no-legend", "--plain", "--no-pager").Output()
	if err != nil {
		return -1
	}
	n := 0
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			n++
		}
	}
	return n
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
