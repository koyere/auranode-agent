// Package fs implements the remote file-manager (SFTP) operations of the
// AuraNode agent. The backend sends a proto.FSRequest over WebSocket and the agent
// responds with a proto.FSResponse. No file port is ever opened to the outside.
package fs

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"

	"github.com/koyere/auranode-agent/pkg/proto"
)

// MaxReadBytes is the absolute cap on bytes the agent returns in a read
// or accepts in a write per request (the backend WebSocket limits the
// message to a few MB). Larger transfers require chunking (follow-up).
const MaxReadBytes = 6 * 1024 * 1024

// uid/gid → name resolution caches to avoid hitting getpwuid on every entry.
// Guarded by cacheMu: Handle runs in a goroutine per request, so multiple
// concurrent list/stat calls access them at once (an unguarded map would
// trigger "concurrent map writes" and crash the agent).
var (
	cacheMu    sync.RWMutex
	userCache  = map[string]string{}
	groupCache = map[string]string{}
)

// Handle runs a file operation and returns the response ready to send.
// It never panics: any system error is translated into resp.Error.
func Handle(req proto.FSRequest) proto.FSResponse {
	resp := proto.FSResponse{RequestID: req.RequestID}

	// Toda ruta debe ser absoluta y limpia: evita ambigüedad y traversal relativo.
	path := filepath.Clean(req.Path)
	if !filepath.IsAbs(path) {
		resp.Error = "the path must be absolute"
		return resp
	}

	var err error
	switch req.Op {
	case proto.FSOpList:
		err = opList(path, &resp)
	case proto.FSOpStat:
		var e *proto.FSEntry
		e, err = statEntry(path)
		resp.Stat = e
	case proto.FSOpRead:
		err = opRead(path, req.MaxBytes, &resp)
	case proto.FSOpWrite:
		err = opWrite(path, req.Content)
	case proto.FSOpMkdir:
		err = os.MkdirAll(path, 0755)
	case proto.FSOpRename:
		err = opRename(path, req.NewPath)
	case proto.FSOpDelete:
		err = os.RemoveAll(path)
	case proto.FSOpChmod:
		err = opChmod(path, req.Mode)
	case proto.FSOpChown:
		err = opChown(path, req.Owner, req.Group)
	default:
		err = fmt.Errorf("unknown operation: %s", req.Op)
	}

	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	resp.OK = true
	return resp
}

func opList(dir string, resp *proto.FSResponse) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	out := make([]proto.FSEntry, 0, len(entries))
	for _, de := range entries {
		full := filepath.Join(dir, de.Name())
		e, err := statEntry(full)
		if err != nil {
			// An unreadable file (e.g. a broken symlink) must not abort the listing.
			continue
		}
		out = append(out, *e)
	}
	// Directories first, then by name — stable, predictable order in the UI.
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir
		}
		return out[i].Name < out[j].Name
	})
	resp.Entries = out
	return nil
}

func statEntry(path string) (*proto.FSEntry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	e := &proto.FSEntry{
		Name:      filepath.Base(path),
		Path:      path,
		IsDir:     info.IsDir(),
		Size:      info.Size(),
		Mode:      info.Mode().Perm().String()[1:], // drops the leading type char
		ModeOctal: fmt.Sprintf("%04o", info.Mode().Perm()),
		ModTime:   info.ModTime().Unix(),
	}

	if info.Mode()&os.ModeSymlink != 0 {
		e.IsSymlink = true
		if target, err := os.Readlink(path); err == nil {
			e.LinkTarget = target
		}
		// Resolve whether the symlink points to a directory for the correct icon.
		if ti, err := os.Stat(path); err == nil {
			e.IsDir = ti.IsDir()
			e.Size = ti.Size()
		}
	}

	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		e.Owner = lookupUser(st.Uid)
		e.Group = lookupGroup(st.Gid)
	}
	return e, nil
}

func opRead(path string, maxBytes int64, resp *proto.FSResponse) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("es un directorio")
	}

	limit := maxBytes
	if limit <= 0 || limit > MaxReadBytes {
		limit = MaxReadBytes
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read limit+1 bytes: if we fill the extra one, the file exceeds the limit.
	buf := make([]byte, limit+1)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return err
	}
	if int64(n) > limit {
		n = int(limit)
		resp.Truncated = true
	}
	resp.Content = base64.StdEncoding.EncodeToString(buf[:n])
	return nil
}

func opWrite(path, contentB64 string) error {
	data, err := base64.StdEncoding.DecodeString(contentB64)
	if err != nil {
		return fmt.Errorf("invalid base64 content: %w", err)
	}
	if int64(len(data)) > MaxReadBytes {
		return fmt.Errorf("file too large (max %d bytes)", MaxReadBytes)
	}
	// Preserve permissions if the file already exists; otherwise 0644.
	mode := os.FileMode(0644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	}
	return os.WriteFile(path, data, mode)
}

func opRename(oldPath, newPath string) error {
	if newPath == "" {
		return fmt.Errorf("new_path is required")
	}
	np := filepath.Clean(newPath)
	if !filepath.IsAbs(np) {
		return fmt.Errorf("new_path debe ser absoluta")
	}
	return os.Rename(oldPath, np)
}

func opChmod(path, mode string) error {
	if mode == "" {
		return fmt.Errorf("mode is required")
	}
	parsed, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return fmt.Errorf("invalid octal mode: %s", mode)
	}
	return os.Chmod(path, os.FileMode(parsed))
}

func opChown(path, owner, group string) error {
	uid, gid := -1, -1
	if owner != "" {
		u, err := user.Lookup(owner)
		if err != nil {
			if n, e := strconv.Atoi(owner); e == nil {
				uid = n
			} else {
				return fmt.Errorf("usuario desconocido: %s", owner)
			}
		} else {
			uid, _ = strconv.Atoi(u.Uid)
		}
	}
	if group != "" {
		g, err := user.LookupGroup(group)
		if err != nil {
			if n, e := strconv.Atoi(group); e == nil {
				gid = n
			} else {
				return fmt.Errorf("grupo desconocido: %s", group)
			}
		} else {
			gid, _ = strconv.Atoi(g.Gid)
		}
	}
	if uid == -1 && gid == -1 {
		return fmt.Errorf("owner or group is required")
	}
	return os.Chown(path, uid, gid)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func lookupUser(uid uint32) string {
	key := strconv.FormatUint(uint64(uid), 10)
	cacheMu.RLock()
	v, ok := userCache[key]
	cacheMu.RUnlock()
	if ok {
		return v
	}
	name := key
	if u, err := user.LookupId(key); err == nil {
		name = u.Username
	}
	cacheMu.Lock()
	userCache[key] = name
	cacheMu.Unlock()
	return name
}

func lookupGroup(gid uint32) string {
	key := strconv.FormatUint(uint64(gid), 10)
	cacheMu.RLock()
	v, ok := groupCache[key]
	cacheMu.RUnlock()
	if ok {
		return v
	}
	name := key
	if g, err := user.LookupGroupId(key); err == nil {
		name = g.Name
	}
	cacheMu.Lock()
	groupCache[key] = name
	cacheMu.Unlock()
	return name
}
