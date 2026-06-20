// Package updater implementa el modelo check-and-notify: el agente comprueba
// periódicamente si hay una versión más reciente en GitHub Releases y lo notifica
// (log + mensaje al backend). NO se auto-reemplaza el binario, para preservar el
// hardening del servicio (usuario no-root, binario root, ProtectSystem=strict).
package updater

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const githubLatestURL = "https://api.github.com/repos/koyere/auranode-agent/releases/latest"

// Updater consulta GitHub y avisa cuando hay una versión más reciente.
type Updater struct {
	current    string
	log        *zap.Logger
	httpClient *http.Client
	notify     func(current, latest string)
	checkEvery time.Duration
	firstDelay time.Duration

	mu          sync.Mutex
	latestKnown string // última versión detectada (>actual); "" si no hay
}

// New crea un Updater. notify se invoca (si no es nil) cuando se detecta por
// primera vez una versión más reciente.
func New(currentVersion string, log *zap.Logger, notify func(current, latest string)) *Updater {
	return &Updater{
		current:    currentVersion,
		log:        log,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		notify:     notify,
		checkEvery: 6 * time.Hour,
		firstDelay: 1 * time.Hour,
	}
}

// Start lanza el chequeo periódico en background (primera comprobación tras
// firstDelay para no golpear GitHub en cada arranque/reinicio).
func (u *Updater) Start(ctx context.Context) {
	if u.current == "" || u.current == "dev" {
		u.log.Debug("updater: versión de desarrollo, auto-check deshabilitado")
		return
	}
	go func() {
		timer := time.NewTimer(u.firstDelay)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				u.checkOnce(ctx)
				timer.Reset(u.checkEvery)
			}
		}
	}()
}

// LatestKnown devuelve la última versión más reciente detectada (>actual), o "".
func (u *Updater) LatestKnown() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.latestKnown
}

func (u *Updater) checkOnce(ctx context.Context) {
	latest, err := u.fetchLatest(ctx)
	if err != nil {
		u.log.Debug("updater: no se pudo consultar la última versión", zap.Error(err))
		return
	}
	if !isNewer(u.current, latest) {
		return
	}
	u.mu.Lock()
	changed := latest != u.latestKnown
	u.latestKnown = latest
	u.mu.Unlock()
	if !changed {
		return
	}
	u.log.Warn("hay una versión más reciente del agente disponible",
		zap.String("current", u.current),
		zap.String("latest", latest),
		zap.String("howto", "reinstala: curl -fsSL https://get.auranode.app/agent | sudo -E bash"),
	)
	if u.notify != nil {
		u.notify(u.current, latest)
	}
}

func (u *Updater) fetchLatest(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubLatestURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := u.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("github status %d", resp.StatusCode)
	}
	var body struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	tag := strings.TrimSpace(strings.TrimPrefix(body.TagName, "v"))
	if tag == "" {
		return "", fmt.Errorf("tag_name vacío")
	}
	return tag, nil
}

// isNewer compara semver simple (major.minor.patch). Las pre-releases se
// ignoran (se comparan solo los tres números).
func isNewer(current, latest string) bool {
	c := parseSemver(current)
	l := parseSemver(latest)
	for i := 0; i < 3; i++ {
		if l[i] != c[i] {
			return l[i] > c[i]
		}
	}
	return false
}

func parseSemver(v string) [3]int {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	parts := strings.SplitN(v, ".", 3)
	var out [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		out[i], _ = strconv.Atoi(parts[i])
	}
	return out
}
