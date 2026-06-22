package migration

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// relayBus connects two Managers (source and dest) routing the data messages between
// them —as the backend relay does— and capturing the control messages.
type relayBus struct {
	t    *testing.T
	src  *Manager
	dst  *Manager
	mu   sync.Mutex
	done chan proto.MigrationMsg
	fail chan proto.MigrationMsg
	prep chan proto.MigrationMsg
}

func (b *relayBus) fromSource(msg any) error { return b.route(true, msg) }
func (b *relayBus) fromDest(msg any) error   { return b.route(false, msg) }

func (b *relayBus) route(fromSrc bool, msg any) error {
	m, ok := msg.(proto.MigrationMsg)
	if !ok {
		return nil
	}
	raw, _ := json.Marshal(m)
	switch m.Type {
	// Data: forward to the opposite end.
	case proto.TypeMigrationFile, proto.TypeMigrationChunk, proto.TypeMigrationFileDone:
		b.dst.Handle(m.Type, raw)
	case proto.TypeMigrationFileAck, proto.TypeMigrationWindowAck:
		b.src.Handle(m.Type, raw)
	// Control: capture.
	case proto.TypeMigrationReceiverReady:
		b.prep <- m
	case proto.TypeMigrationDone:
		b.done <- m
	case proto.TypeMigrationFailed:
		b.fail <- m
	case proto.TypeMigrationProgress:
		// ignored in the test
	}
	return nil
}

func writeRandom(t *testing.T, path string, size int) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, size)
	rand.Read(data)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestMigrationRoundTrip(t *testing.T) {
	log := zap.NewNop()
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "out")
	stateDir := t.TempDir()

	// Test tree: small files, nested, empty and one larger than the window.
	want := map[string]string{}
	want["a.txt"] = writeRandom(t, filepath.Join(srcDir, "a.txt"), 1234)
	want["nested/b.bin"] = writeRandom(t, filepath.Join(srcDir, "nested", "b.bin"), 500*1024)
	want["empty.dat"] = writeRandom(t, filepath.Join(srcDir, "empty.dat"), 0)
	want["big.bin"] = writeRandom(t, filepath.Join(srcDir, "big.bin"), 20*1024*1024) // > 8MB window

	src := New(log, stateDir)
	dst := New(log, stateDir)
	bus := &relayBus{
		t: t, src: src, dst: dst,
		done: make(chan proto.MigrationMsg, 1),
		fail: make(chan proto.MigrationMsg, 1),
		prep: make(chan proto.MigrationMsg, 1),
	}
	src.SetSend(bus.fromSource)
	dst.SetSend(bus.fromDest)

	const migID = "mig_test_1"

	// 1. prepare on the dest → receiver_ready.
	prepRaw, _ := json.Marshal(proto.MigrationMsg{
		Envelope: proto.Envelope{Type: proto.TypeMigrationPrepare}, MigrationID: migID, DestPath: dstDir,
	})
	dst.Handle(proto.TypeMigrationPrepare, prepRaw)

	var ready proto.MigrationMsg
	select {
	case ready = <-bus.prep:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando receiver_ready")
	}
	if len(ready.Completed) != 0 {
		t.Fatalf("initial manifest should be empty, got %d", len(ready.Completed))
	}

	// 2. start on the source.
	startRaw, _ := json.Marshal(proto.MigrationMsg{
		Envelope: proto.Envelope{Type: proto.TypeMigrationStart}, MigrationID: migID,
		SourcePath: srcDir, DestPath: dstDir, ChunkSize: 1 << 20, WindowBytes: 8 << 20, VerifyChecksum: true,
	})
	src.Handle(proto.TypeMigrationStart, startRaw)

	select {
	case d := <-bus.done:
		if d.Status != "completed" {
			t.Fatalf("status inesperado: %s (warnings=%v)", d.Status, d.Warnings)
		}
	case f := <-bus.fail:
		t.Fatalf("migration failed: %s %s", f.Code, f.Message)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout esperando done")
	}

	// 3. Verify the integrity of each file at the destination.
	for rel, sum := range want {
		got := sha256File(t, filepath.Join(dstDir, rel))
		if got != sum {
			t.Errorf("%s: sha mismatch\n want %s\n got  %s", rel, sum, got)
		}
	}
}

func TestMigrationResumeSkipsCompleted(t *testing.T) {
	log := zap.NewNop()
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "out")
	stateDir := t.TempDir()

	writeRandom(t, filepath.Join(srcDir, "x.bin"), 256*1024)
	bigSum := writeRandom(t, filepath.Join(srcDir, "y.bin"), 2*1024*1024)

	src := New(log, stateDir)
	dst := New(log, stateDir)

	// Manifest pre-existente: marca x.bin como ya completado (mismo size+mtime).
	info, _ := os.Stat(filepath.Join(srcDir, "x.bin"))
	manDir := filepath.Join(stateDir, "migrations", "mig_resume")
	os.MkdirAll(manDir, 0o700)
	mb, _ := json.Marshal(proto.MigrationFileInfo{Path: "x.bin", Size: info.Size(), Mtime: info.ModTime().Unix()})
	os.WriteFile(filepath.Join(manDir, "manifest.jsonl"), append(mb, '\n'), 0o600)

	var receivedFiles []string
	var mu sync.Mutex
	done := make(chan proto.MigrationMsg, 1)

	src.SetSend(func(msg any) error {
		m := msg.(proto.MigrationMsg)
		raw, _ := json.Marshal(m)
		switch m.Type {
		case proto.TypeMigrationFile:
			mu.Lock()
			receivedFiles = append(receivedFiles, m.File.Path)
			mu.Unlock()
			dst.Handle(m.Type, raw)
		case proto.TypeMigrationChunk, proto.TypeMigrationFileDone:
			dst.Handle(m.Type, raw)
		case proto.TypeMigrationDone:
			done <- m
		}
		return nil
	})
	dst.SetSend(func(msg any) error {
		m := msg.(proto.MigrationMsg)
		raw, _ := json.Marshal(m)
		if m.Type == proto.TypeMigrationFileAck || m.Type == proto.TypeMigrationWindowAck {
			src.Handle(m.Type, raw)
		}
		return nil
	})

	prepRaw, _ := json.Marshal(proto.MigrationMsg{
		Envelope: proto.Envelope{Type: proto.TypeMigrationPrepare}, MigrationID: "mig_resume", DestPath: dstDir,
	})
	dst.Handle(proto.TypeMigrationPrepare, prepRaw)

	info2, _ := os.Stat(filepath.Join(srcDir, "x.bin"))
	startRaw, _ := json.Marshal(proto.MigrationMsg{
		Envelope: proto.Envelope{Type: proto.TypeMigrationStart}, MigrationID: "mig_resume",
		SourcePath: srcDir, DestPath: dstDir, ChunkSize: 1 << 20, WindowBytes: 8 << 20,
		Completed: []proto.MigrationFileInfo{{Path: "x.bin", Size: info2.Size(), Mtime: info2.ModTime().Unix()}},
	})
	src.Handle(proto.TypeMigrationStart, startRaw)

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedFiles) != 1 || receivedFiles[0] != "y.bin" {
		t.Fatalf("should send only y.bin (x.bin skipped), sent: %v", receivedFiles)
	}
	if got := sha256File(t, filepath.Join(dstDir, "y.bin")); got != bigSum {
		t.Errorf("y.bin sha mismatch")
	}
}

func TestSafeJoin(t *testing.T) {
	base := "/var/lib/dest"
	cases := []struct {
		rel  string
		want bool
	}{
		{"a/b.txt", true},
		{"./a.txt", true},
		{"../escape.txt", false},
		{"a/../../escape", false},
		{"/abs/inside", true}, // se reabsolutiza bajo base
	}
	for _, c := range cases {
		_, ok := safeJoin(base, c.rel)
		if ok != c.want {
			t.Errorf("safeJoin(%q)=%v, want %v", c.rel, ok, c.want)
		}
	}
}
