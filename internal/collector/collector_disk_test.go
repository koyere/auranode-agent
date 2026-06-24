package collector

import "testing"

func TestIsPseudoFS(t *testing.T) {
	cases := []struct {
		fstype string
		mount  string
		pseudo bool
	}{
		// Real filesystems → reported.
		{"ext4", "/", false},
		{"ext4", "/boot", false},
		{"xfs", "/data", false},
		{"vfat", "/boot/efi", false},
		{"btrfs", "/mnt/backup", false},
		// Pseudo → skipped.
		{"squashfs", "/snap/core22/2411", true}, // snaps: always 100%
		{"tmpfs", "/run/user/1000", true},
		{"devtmpfs", "/dev", true},
		{"overlay", "/var/lib/docker/overlay2/x/merged", true},
		{"proc", "/proc", true},
		{"sysfs", "/sys", true},
		// Even with an unknown fstype, the /snap/ path skips it.
		{"", "/snap/lxd/38800", true},
		{"SquashFS", "/snap/snapd/26865", true}, // case-insensitive
	}
	for _, c := range cases {
		if got := isPseudoFS(c.fstype, c.mount); got != c.pseudo {
			t.Errorf("isPseudoFS(%q, %q) = %v, want %v", c.fstype, c.mount, got, c.pseudo)
		}
	}
}
