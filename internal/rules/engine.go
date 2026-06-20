// Package rules evalúa reglas de auto-remediación sobre métricas en tiempo real.
package rules

import (
	"context"
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
	conditionSince time.Time // cuándo empezó a cumplirse la condición
	lastFired      time.Time
	firedToday     int
	lastFiredDay   int // día del año
}

func New(sendFn func(proto.RuleFired), log *zap.Logger) *Engine {
	return &Engine{
		state:  make(map[string]*ruleState),
		sendFn: sendFn,
		log:    log,
	}
}

// Sync reemplaza las reglas activas.
func (e *Engine) Sync(rules []proto.RuleDefinition) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = rules
}

// Evaluate evalúa las reglas contra el snapshot de métricas recibido.
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

		// Verificar duración de la condición
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

		// Max por día
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
		st.conditionSince = time.Time{} // reset para no refired inmediatamente
		e.mu.Unlock()

		// Ejecutar acción de forma asíncrona
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
		e.log.Info("rule fired: webhook (not implemented)", zap.String("rule_id", r.RuleID))
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

// extractMetric extrae el valor numérico de la métrica indicada.
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
