package logs

import (
	"testing"

	"go.uber.org/zap"
)

func TestAllowedExcludesSelfUnits(t *testing.T) {
	c := New(zap.NewNop())

	// No filter (collect everything): the agent's own units are still excluded.
	cases := []struct {
		service string
		allowed bool
	}{
		{"sshd", true},
		{"cron", true},
		{"nginx.service", true},
		{"auranode-agent", false},
		{"auranode-agent.service", false},
		{"auranode-agent-helper", false},
		{"auranode-agent-helper.service", false},
	}
	for _, tc := range cases {
		if got := c.allowed(tc.service); got != tc.allowed {
			t.Errorf("allowed(%q) = %v, want %v (no filter)", tc.service, got, tc.allowed)
		}
	}

	// With an allowlist: only configured units, and never the own units even if listed.
	c.Configure([]string{"sshd", "auranode-agent"})
	if !c.allowed("sshd") {
		t.Error("allowed(sshd) = false with allowlist, want true")
	}
	if c.allowed("nginx") {
		t.Error("allowed(nginx) = true with an allowlist that excludes it, want false")
	}
	if c.allowed("auranode-agent") {
		t.Error("allowed(auranode-agent) = true even though it is in the allowlist, want false (self-exclusion)")
	}
}
