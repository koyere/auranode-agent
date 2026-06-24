// Package rules evaluates auto-remediation rules over real-time metrics.
package rules

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/internal/executor"
	"github.com/koyere/auranode-agent/pkg/proto"
)

type Engine struct {
	mu      sync.Mutex
	rules   []proto.RuleDefinition
	state   map[string]*ruleState // keyed by RuleID
	sendFn  func(proto.RuleFired)
	log     *zap.Logger
}

type ruleState struct {
	conditionSince time.Time // when the condition started being met
	lastFired      time.Time
	firedToday     int
	lastFiredDay   int // day of the year
}

func New(sendFn func(proto.RuleFired), log *zap.Logger) *Engine {
	return &Engine{
		state:  make(map[string]*ruleState),
		sendFn: sendFn,
		log:    log,
	}
}

// Sync replaces the active rules.
func (e *Engine) Sync(rules []proto.RuleDefinition) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = rules
}

// Evaluate evaluates the rules against the received metrics snapshot.
func (e *Engine) Evaluate(ctx context.Context, m proto.Metrics) {
	e.mu.Lock()
	rules := make([]proto.RuleDefinition, len(e.rules))
	copy(rules, e.rules)
	e.mu.Unlock()

	now := time.Now()

	for _, r := range rules {
		if !r.Enabled {
			continue
		}

		value, ok := extractMetric(m, r.TriggerMetric)
		if !ok {
			continue
		}

		condMet := evalOp(value, r.TriggerOp, r.TriggerValue)

		e.mu.Lock()
		st, exists := e.state[r.RuleID]
		if !exists {
			st = &ruleState{}
			e.state[r.RuleID] = st
		}

		if condMet {
			if st.conditionSince.IsZero() {
				st.conditionSince = now
			}
		} else {
			st.conditionSince = time.Time{}
			e.mu.Unlock()
			continue
		}

		// Check the condition duration
		condDuration := now.Sub(st.conditionSince)
		minDuration := time.Duration(r.ConditionDurationSeconds) * time.Second
		if condDuration < minDuration {
			e.mu.Unlock()
			continue
		}

		// Cooldown
		if !st.lastFired.IsZero() && now.Sub(st.lastFired) < time.Duration(r.CooldownSeconds)*time.Second {
			e.mu.Unlock()
			continue
		}

		// Max per day
		todayYearDay := now.YearDay()
		if r.MaxPerDay > 0 {
			if st.lastFiredDay != todayYearDay {
				st.firedToday = 0
				st.lastFiredDay = todayYearDay
			}
			if st.firedToday >= r.MaxPerDay {
				e.mu.Unlock()
				continue
			}
		}

		st.lastFired = now
		st.firedToday++
		st.conditionSince = time.Time{} // reset so it does not refire immediately
		e.mu.Unlock()

		// Run the action asynchronously
		go e.fire(ctx, r, value)
	}
}

func (e *Engine) fire(ctx context.Context, r proto.RuleDefinition, triggerValue float64) {
	var exitStatus int
	var actionTaken string

	switch r.ActionType {
	case "command":
		if r.ActionCommand != "" {
			actionTaken = r.ActionCommand
			res := executor.Run(ctx, r.RuleID, r.ActionCommand, 60)
			exitStatus = res.ExitStatus
			e.log.Info("rule fired: command executed",
				zap.String("rule_id", r.RuleID),
				zap.Int("exit", exitStatus),
			)
		}
	case "webhook":
		actionTaken = "webhook:" + r.ActionWebhookURL
		if r.ActionWebhookURL != "" {
			exitStatus = e.postWebhook(ctx, r, triggerValue)
			e.log.Info("rule fired: webhook sent",
				zap.String("rule_id", r.RuleID),
				zap.Int("status", exitStatus),
			)
		}
	default:
		actionTaken = r.ActionType
	}

	e.sendFn(proto.RuleFired{
		Envelope: proto.Envelope{
			Type:      proto.TypeRuleFired,
			Timestamp: time.Now().Unix(),
		},
		RuleID:       r.RuleID,
		TriggerValue: triggerValue,
		ActionTaken:  actionTaken,
		ExitStatus:   exitStatus,
	})
}

// webhookClient reutiliza conexiones y aplica un timeout duro.
var webhookClient = &http.Client{Timeout: 10 * time.Second}

// postWebhook envía un POST JSON con los datos del disparo. Devuelve un "exit
// status" estilo comando: 0 si la respuesta es 2xx; el código HTTP (p. ej. 404,
// 500) si no; 1 si el envío falló (DNS, timeout, conexión). Así el panel muestra
// un resultado útil en la columna de código de salida.
func (e *Engine) postWebhook(ctx context.Context, r proto.RuleDefinition, triggerValue float64) int {
	body, _ := json.Marshal(map[string]any{
		"rule_id":   r.RuleID,
		"metric":    r.TriggerMetric,
		"operator":  r.TriggerOp,
		"threshold": r.TriggerValue,
		"value":     triggerValue,
		"fired_at":  time.Now().UTC().Format(time.RFC3339),
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.ActionWebhookURL, bytes.NewReader(body))
	if err != nil {
		e.log.Warn("webhook: petición inválida", zap.String("rule_id", r.RuleID), zap.Error(err))
		return 1
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "AuraNode-Agent")

	resp, err := webhookClient.Do(req)
	if err != nil {
		e.log.Warn("webhook: envío falló", zap.String("rule_id", r.RuleID), zap.Error(err))
		return 1
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return 0
	}
	return resp.StatusCode
}

// extractMetric extracts the numeric value of the given metric.
func extractMetric(m proto.Metrics, name string) (float64, bool) {
	switch name {
	case "cpu":
		return m.CPU.UsagePercent, true
	case "ram":
		if m.RAM.TotalMB == 0 {
			return 0, false
		}
		return float64(m.RAM.UsedMB) / float64(m.RAM.TotalMB) * 100, true
	case "swap":
		if m.RAM.SwapTotalMB == 0 {
			return 0, false
		}
		return float64(m.RAM.SwapUsedMB) / float64(m.RAM.SwapTotalMB) * 100, true
	case "load_1m":
		return m.LoadAvg.M1, true
	case "load_5m":
		return m.LoadAvg.M5, true
	case "load_15m":
		return m.LoadAvg.M15, true
	}
	// disk_MOUNT (e.g. disk_/)
	const diskPrefix = "disk_"
	if len(name) > len(diskPrefix) && name[:len(diskPrefix)] == diskPrefix {
		mount := name[len(diskPrefix):]
		for _, d := range m.Disk {
			if d.Mount == mount {
				return d.UsedPercent, true
			}
		}
	}
	return 0, false
}

func evalOp(value float64, op string, threshold float64) bool {
	switch op {
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	case "==":
		return value == threshold
	}
	return false
}
