package fs

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/koyere/auranode-agent/pkg/proto"
)

func do(op, path string, mut func(*proto.FSRequest)) proto.FSResponse {
	req := proto.FSRequest{Op: op, Path: path, RequestID: "test"}
	if mut != nil {
		mut(&req)
	}
	return Handle(req)
}

func TestLifecycle(t *testing.T) {
	dir := t.TempDir()

	// mkdir
	sub := filepath.Join(dir, "sub")
	if r := do(proto.FSOpMkdir, sub, nil); !r.OK {
		t.Fatalf("mkdir: %s", r.Error)
	}
	if fi, err := os.Stat(sub); err != nil || !fi.IsDir() {
		t.Fatalf("mkdir no creó el directorio: %v", err)
	}

	// write
	file := filepath.Join(sub, "hello.txt")
	content := base64.StdEncoding.EncodeToString([]byte("hola mundo"))
	if r := do(proto.FSOpWrite, file, func(req *proto.FSRequest) { req.Content = content }); !r.OK {
		t.Fatalf("write: %s", r.Error)
	}

	// read
	r := do(proto.FSOpRead, file, nil)
	if !r.OK {
		t.Fatalf("read: %s", r.Error)
	}
	got, _ := base64.StdEncoding.DecodeString(r.Content)
	if string(got) != "hola mundo" {
		t.Fatalf("read content = %q, quería 'hola mundo'", got)
	}

	// stat
	r = do(proto.FSOpStat, file, nil)
	if !r.OK || r.Stat == nil {
		t.Fatalf("stat: %s", r.Error)
	}
	if r.Stat.IsDir || r.Stat.Size != int64(len("hola mundo")) {
		t.Fatalf("stat inesperado: %+v", r.Stat)
	}

	// list
	r = do(proto.FSOpList, sub, nil)
	if !r.OK || len(r.Entries) != 1 || r.Entries[0].Name != "hello.txt" {
		t.Fatalf("list inesperado: %+v (%s)", r.Entries, r.Error)
	}

	// chmod
	if r := do(proto.FSOpChmod, file, func(req *proto.FSRequest) { req.Mode = "600" }); !r.OK {
		t.Fatalf("chmod: %s", r.Error)
	}
	if fi, _ := os.Stat(file); fi.Mode().Perm() != 0600 {
		t.Fatalf("chmod no aplicó 0600")
	}

	// rename
	moved := filepath.Join(sub, "renamed.txt")
	if r := do(proto.FSOpRename, file, func(req *proto.FSRequest) { req.NewPath = moved }); !r.OK {
		t.Fatalf("rename: %s", r.Error)
	}
	if _, err := os.Stat(moved); err != nil {
		t.Fatalf("rename no movió el archivo: %v", err)
	}

	// delete
	if r := do(proto.FSOpDelete, sub, nil); !r.OK {
		t.Fatalf("delete: %s", r.Error)
	}
	if _, err := os.Stat(sub); !os.IsNotExist(err) {
		t.Fatalf("delete no eliminó el directorio")
	}
}

func TestRejectsRelativePath(t *testing.T) {
	if r := do(proto.FSOpStat, "etc/passwd", nil); r.OK || r.Error == "" {
		t.Fatalf("debió rechazar ruta relativa, got OK=%v", r.OK)
	}
}

func TestReadTruncation(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "big.bin")
	if err := os.WriteFile(file, make([]byte, 2048), 0644); err != nil {
		t.Fatal(err)
	}
	r := do(proto.FSOpRead, file, func(req *proto.FSRequest) { req.MaxBytes = 1024 })
	if !r.OK {
		t.Fatalf("read: %s", r.Error)
	}
	if !r.Truncated {
		t.Fatalf("esperaba Truncated=true")
	}
	got, _ := base64.StdEncoding.DecodeString(r.Content)
	if len(got) != 1024 {
		t.Fatalf("esperaba 1024 bytes, got %d", len(got))
	}
}

func TestReadDirFails(t *testing.T) {
	dir := t.TempDir()
	if r := do(proto.FSOpRead, dir, nil); r.OK {
		t.Fatalf("read de un directorio debió fallar")
	}
}
