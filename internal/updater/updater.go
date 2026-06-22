// Package updater implements the check-and-notify model: the agent checks
// periodically whether a newer version exists on GitHub Releases and notifies
// (log + message to the backend). It does NOT self-replace the binary, to preserve the
// service hardening (non-root user, root binary, ProtectSystem=strict).
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

// Updater queries GitHub and reports when a newer version is available.
type Updater struct {
	current    string
	log        *zap.Logger
	httpClient *http.Client
	notify     func(current, latest string)
	checkEvery time.Duration
	firstDelay time.Duration

	mu          sync.Mutex
	latestKnown string // latest detected version (>current); "" if none
}

// New creates an Updater. notify is invoked (if not nil) when a newer version
// is detected for the first time.
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

// Start launches the periodic check in the background (first check after
// firstDelay to avoid hitting GitHub on every start/restart).
func (u *Updater) Start(ctx context.Context) {
	if u.current == "" || u.current == "dev" {
		u.log.Debug("updater: development version, auto-check disabled")
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

// LatestKnown returns the latest newer version detected (>current), or "".
func (u *Updater) LatestKnown() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.latestKnown
}

func (u *Updater) checkOnce(ctx context.Context) {
	latest, err := u.fetchLatest(ctx)
	if err != nil {
		u.log.Debug("updater: could not query the latest version", zap.Error(err))
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
	u.log.Warn("a newer agent version is available",
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
		return "", fmt.Errorf("empty tag_name")
	}
	return tag, nil
}

// isNewer compara semver simple (major.minor.patch). Las pre-releases se
// are ignored (only the three numbers are compared).
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
