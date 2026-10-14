package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
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

func TestMemWatcher_GrowingDirIsDemotedToPolling(t *testing.T) {
	mem := t.TempDir()
	grow := filepath.Join(mem, "growing")
	if err := os.MkdirAll(grow, 0o755); err != nil {
		t.Fatal(err)
	}
	idx := &fdTestIndexer{}
	w := NewMemWatcher(mem, idx)
	w.pollInterval = 100 * time.Millisecond

	// Unit step: drive demoteOversized synchronously against a real watcher.
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer fsw.Close()
	if err := fsw.Add(mem); err != nil {
		t.Fatal(err)
	}
	w.watchDir(fsw, grow)
	watched := func() bool {
		for _, p := range fsw.WatchList() {
			if p == grow {
				return true
			}
		}
		return false
	}
	if !watched() {
		t.Fatal("small dir should start fsnotify-watched")
	}
	for i := 0; i <= memWatcherDirEntryCap; i++ {
		if err := os.WriteFile(filepath.Join(grow, fmt.Sprintf("f%d.cog.md", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	w.demoteOversized(fsw)
	if watched() {
		t.Fatal("dir past the cap is still fsnotify-watched")
	}
	if _, ok := w.polled[grow]; !ok {
		t.Fatal("dir past the cap was not registered for polling")
	}
	if len(w.polled[grow]) != memWatcherDirEntryCap+1 {
		t.Fatalf("polled seed has %d entries, want %d", len(w.polled[grow]), memWatcherDirEntryCap+1)
	}
	if !watchedRoot(fsw, mem) {
		t.Fatal("root watch must survive demotion")
	}

	// End-to-end step: a live watcher demotes on its tick and the poll then
	// indexes new files.
	live := filepath.Join(mem, "live")
	if err := os.MkdirAll(live, 0o755); err != nil {
		t.Fatal(err)
	}
	w2 := NewMemWatcher(mem, idx)
	w2.pollInterval = 100 * time.Millisecond
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()
	for i := 0; i <= memWatcherDirEntryCap; i++ {
		if err := os.WriteFile(filepath.Join(live, fmt.Sprintf("g%d.txt", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(400 * time.Millisecond) // let at least one tick demote it
	fresh := filepath.Join(live, "fresh.cog.md")
	if err := os.WriteFile(fresh, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for !idx.saw(fresh) {
		if time.Now().After(deadline) {
			t.Fatal("file in demoted dir was never indexed")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func watchedRoot(fsw *fsnotify.Watcher, root string) bool {
	for _, p := range fsw.WatchList() {
		if p == root {
			return true
		}
	}
	return false
}
