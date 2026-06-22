package updater

import "testing"

func TestIsNewer(t *testing.T) {
	cases := []struct {
		current, latest string
		want            bool
	}{
		{"1.0.0", "1.0.1", true},
		{"1.0.0", "1.1.0", true},
		{"1.0.0", "2.0.0", true},
		{"1.0.0", "1.0.0", false},
		{"1.2.0", "1.1.9", false},
		{"2.0.0", "1.9.9", false},
		{"v1.0.0", "v1.0.1", true},
		{"1.0.0", "1.0.1-beta.1", true}, // pre-release is ignored: 1.0.1 > 1.0.0
		{"1.0.0-beta", "1.0.0", false},  // misma triada 1.0.0
		{"1.0.0", "1.1.0-rc.1", true},   // minor mayor aunque sea rc
	}
	for _, c := range cases {
		if got := isNewer(c.current, c.latest); got != c.want {
			t.Errorf("isNewer(%q,%q)=%v want %v", c.current, c.latest, got, c.want)
		}
	}
}

func TestParseSemver(t *testing.T) {
	got := parseSemver("v2.3.4-rc.1+build5")
	want := [3]int{2, 3, 4}
	if got != want {
		t.Errorf("parseSemver=%v want %v", got, want)
	}
}
