package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fdTestIndexer struct {
	mu    sync.Mutex
	paths []string
}

func (r *fdTestIndexer) IndexFile(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths = append(r.paths, path)
	return nil
}

func (r *fdTestIndexer) saw(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.paths {
		if p == path {
			return true
		}
	}
	return false
}

func openFDCount(t *testing.T) int {
	t.Helper()
	// Readdirnames, not ReadDir: ReadDir lstats each entry, which fails on
	// macOS for the descriptor of the directory handle itself.
	f, err := os.Open("/dev/fd")
	if err != nil {
		t.Skipf("/dev/fd unavailable: %v", err)
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		t.Skipf("/dev/fd unreadable: %v", err)
	}
	return len(names)
}

func TestMemWatcher_BulkDirsDoNotHoldFDs(t *testing.T) {
	mem := t.TempDir()
	var bulk []string
	for _, d := range []string{"observations", "sessions", "episodic/big"} {
		dir := filepath.Join(mem, d)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		bulk = append(bulk, dir)
		for i := 0; i < 1700; i++ {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d.cog.md", i)), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	small := filepath.Join(mem, "semantic")
	if err := os.MkdirAll(small, 0o755); err != nil {
		t.Fatal(err)
	}

	idx := &fdTestIndexer{}
	w := NewMemWatcher(mem, idx)
	w.pollInterval = 100 * time.Millisecond

	before := openFDCount(t)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	after := openFDCount(t)
	t.Logf("fds before=%d after=%d", before, after)

	if delta := after - before; delta > 32 {
		t.Fatalf("fd count rose by %d (before=%d after=%d); bulk dirs must not be watched", delta, before, after)
	}

	// A file created in a skipped dir is noticed by the poll.
	created := filepath.Join(bulk[0], "fresh.cog.md")
	if err := os.WriteFile(created, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !idx.saw(created) {
		if time.Now().After(deadline) {
			t.Fatal("file created in skipped dir was never indexed via poll")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// A file in a small, fsnotify-watched dir is still picked up.
	hot := filepath.Join(small, "hot.cog.md")
	if err := os.WriteFile(hot, []byte("hot"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for !idx.saw(hot) {
		if time.Now().After(deadline) {
			t.Fatal("file in small watched dir was never indexed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
