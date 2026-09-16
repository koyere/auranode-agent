package executor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRunBoundsOutputWhileCommandRuns(t *testing.T) {
	res := Run(context.Background(), "large-output",
		"yes x | head -c 131072; yes e | head -c 32768 >&2", 10)

	if res.ExitStatus != 0 {
		t.Fatalf("unexpected exit status: %d (%s)", res.ExitStatus, res.Stderr)
	}
	if !strings.HasSuffix(res.Stdout, truncatedMarker) {
		t.Fatalf("stdout was not marked as truncated")
	}
	if !strings.HasSuffix(res.Stderr, truncatedMarker) {
		t.Fatalf("stderr was not marked as truncated")
	}
	if got, max := len(res.Stdout), maxStdoutBytes+len(truncatedMarker); got > max {
		t.Fatalf("stdout retained %d bytes, max %d", got, max)
	}
	if got, max := len(res.Stderr), maxStderrBytes+len(truncatedMarker); got > max {
		t.Fatalf("stderr retained %d bytes, max %d", got, max)
	}
}

func TestRunTimeoutKillsCommandPromptly(t *testing.T) {
	start := time.Now()
	res := Run(context.Background(), "timeout", "sleep 30 & wait", 1)
	if res.ExitStatus != 124 {
		t.Fatalf("exit status = %d, want 124", res.ExitStatus)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("timed out command took too long: %s", elapsed)
	}
}

func TestLimitedBufferReportsFullWrites(t *testing.T) {
	b := newLimitedBuffer(4)
	if n, err := b.Write([]byte("abcdefgh")); err != nil || n != 8 {
		t.Fatalf("Write() = (%d, %v), want (8, nil)", n, err)
	}
	if got := b.String(); got != "abcd"+truncatedMarker {
		t.Fatalf("String() = %q", got)
	}
}
