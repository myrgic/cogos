// mem_watcher.go — real-time FTS currency watcher for the .cog/mem/ corpus.
//
// macOS kqueue (and Windows ReadDirectoryChangesW) is non-recursive: adding a
// parent directory to an fsnotify watcher does NOT automatically watch newly
// created subdirectories.  When a caller creates a new memory sector directory
// (e.g. .cog/mem/semantic/newkind/) none of its files will be indexed until
// the daemon restarts or a reindex is triggered.
//
// MemWatcher fixes this by:
//
//  1. Walking .cog/mem/ at startup and adding every existing subdirectory to
//     the fsnotify watcher.
//  2. On a Create event whose target is a directory, immediately calling
//     watcher.Add on the new directory so its descendants are watched.
//  3. On a Create or Write event whose target is a *.cog.md file, calling
//     indexer.IndexFile after a 500ms debounce window (coalesces rapid saves).
//
// Directories with more than memWatcherDirEntryCap entries are not handed to
// fsnotify (kqueue holds one fd per file in a watched dir); they are polled.
//
// The watcher is started from Boot() after WireConstellationIndexer runs.
// When pkgFTSRepairIndexer is nil (tests, CLI) the watcher is not started.
package engine

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	memWatcherDebounce = 500 * time.Millisecond

	// memWatcherDirEntryCap is the largest directory (by entry count) that is
	// handed to fsnotify.  fsnotify's kqueue backend (macOS/BSD) opens one fd
	// per file in every watched directory, so bulk corpus directories would
	// hold fds proportional to corpus size and starve the HTTP listener.
	// Directories over the cap are covered by a cheap periodic poll instead.
	memWatcherDirEntryCap = 256

	// memWatcherPollInterval is how often skipped (oversized) directories are
	// re-listed and diffed.
	memWatcherPollInterval = 30 * time.Second
)

// polledFile is the (mtime, size) fingerprint used to diff a polled directory.
type polledFile struct {
	mtime time.Time
	size  int64
	dir   bool
}

// MemWatcher watches .cog/mem/ for new subdirectories and .cog.md writes,
// forwarding index updates to the wired ConstellationIndexer.
type MemWatcher struct {
	memDir  string
	indexer ConstellationIndexer
	stopCh  chan struct{}

	// pollInterval is the re-scan period for oversized directories.
	pollInterval time.Duration
	// polled maps each oversized directory to its last-seen listing.  Owned by
	// the event-loop goroutine once Start has returned.
	polled map[string]map[string]polledFile

	mu      sync.Mutex
	running bool
}

// NewMemWatcher creates a watcher for the given memory directory.
// indexer must be non-nil.
func NewMemWatcher(memDir string, indexer ConstellationIndexer) *MemWatcher {
	return &MemWatcher{
		memDir:  memDir,
		indexer: indexer,
		stopCh:  make(chan struct{}),

		pollInterval: memWatcherPollInterval,
		polled:       make(map[string]map[string]polledFile),
	}
}

// Start begins watching memDir.  Non-blocking: the event loop runs in a
// goroutine.  Returns an error if the directory does not exist or fsnotify
// cannot be initialised.  Idempotent: calling Start on an already-running
// watcher returns an error.
func (w *MemWatcher) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return nil
	}

	if _, err := os.Stat(w.memDir); err != nil {
		return err
	}

	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	// Add the root mem dir.  The root is always watched, regardless of size.
	if err := fsw.Add(w.memDir); err != nil {
		fsw.Close()
		return err
	}

	// Walk existing subdirectories and add each one so the watcher is
	// immediately current for all existing sector directories.
	_ = filepath.WalkDir(w.memDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == w.memDir {
			return nil
		}
		w.watchDir(fsw, path)
		return nil
	})

	w.running = true
	go w.loop(fsw)
	return nil
}

// Stop halts the event loop and releases fsnotify resources.
func (w *MemWatcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running {
		return
	}
	close(w.stopCh)
	w.running = false
}

// loop is the main event loop.  Each file-system event is classified:
//   - directory Create → add new dir to the watcher immediately (no debounce).
//   - *.cog.md Create/Write → enqueue path for debounced IndexFile call.
//
// The debounce map coalesces rapid writes within a 500ms window so that a
// fast sequence of saves (e.g. editor write + rename swap) results in a single
// IndexFile call rather than N redundant upserts.
func (w *MemWatcher) loop(fsw *fsnotify.Watcher) {
	defer fsw.Close()

	// debounce tracks paths queued for indexing: path → timer.
	debounce := make(map[string]*time.Timer)
	var mu sync.Mutex // guards debounce map

	// Poll tick for oversized directories; a no-op when none are registered.
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.demoteOversized(fsw)
			w.pollOversized(fsw, debounce, &mu)

		case <-w.stopCh:
			mu.Lock()
			for _, t := range debounce {
				t.Stop()
			}
			mu.Unlock()
			return

		case event, ok := <-fsw.Events:
			if !ok {
				return
			}

			path := event.Name

			switch {
			// ── New directory: add to watcher immediately ────────────────────
			case event.Has(fsnotify.Create):
				info, err := os.Lstat(path)
				if err != nil {
					break
				}
				if info.IsDir() {
					w.watchDir(fsw, path)
					break
				}
				// File create — fall through to the *.cog.md check below.
				if !strings.HasSuffix(path, ".cog.md") {
					break
				}
				w.scheduleIndex(path, debounce, &mu)

			// ── File write: debounced index call ─────────────────────────────
			case event.Has(fsnotify.Write):
				if !strings.HasSuffix(path, ".cog.md") {
					break
				}
				w.scheduleIndex(path, debounce, &mu)
			}

		case err, ok := <-fsw.Errors:
			if !ok {
				return
			}
			slog.Debug("mem_watcher: fsnotify error", "err", err)
		}
	}
}

// watchDir decides how a directory is covered.  Small directories get an
// fsnotify watch; directories with more than memWatcherDirEntryCap entries are
// registered for periodic polling instead, so fd use stays independent of
// corpus size on kqueue platforms.  Linux behaves the same (inotify is cheap
// per file, but the poll keeps the two platforms equivalent and bounded).
func (w *MemWatcher) watchDir(fsw *fsnotify.Watcher, path string) {
	if dirOverCap(path, memWatcherDirEntryCap) {
		if _, ok := w.polled[path]; !ok {
			w.polled[path] = w.listDir(path)
			slog.Warn("mem_watcher: directory too large for fsnotify, polling instead",
				"path", path, "cap", memWatcherDirEntryCap, "interval", w.pollInterval)
		}
		return
	}
	if err := fsw.Add(path); err != nil {
		slog.Debug("mem_watcher: could not add dir", "path", path, "err", err)
		return
	}
	slog.Debug("mem_watcher: watching dir", "path", path)
}

// demoteOversized re-checks every fsnotify-watched directory against the entry
// cap.  A directory that grew past it one Create at a time would otherwise keep
// its watch (and its per-file kqueue fds) forever; it is moved to the polled
// set, seeded with the current listing so no spurious events fire.  One-way:
// polled directories are never promoted back.  The root is always watched.
func (w *MemWatcher) demoteOversized(fsw *fsnotify.Watcher) {
	for _, dir := range fsw.WatchList() {
		if dir == w.memDir || !dirOverCap(dir, memWatcherDirEntryCap) {
			continue
		}
		if err := fsw.Remove(dir); err != nil {
			slog.Debug("mem_watcher: could not remove watch", "path", dir, "err", err)
			continue
		}
		w.polled[dir] = w.listDir(dir)
		slog.Warn("mem_watcher: directory grew past fsnotify cap, polling instead",
			"path", dir, "cap", memWatcherDirEntryCap, "interval", w.pollInterval)
	}
}

// dirOverCap reports whether dir holds more than limit entries.  It reads at
// most limit+1 names so the cost is bounded even for huge directories.
func dirOverCap(dir string, limit int) bool {
	f, err := os.Open(dir)
	if err != nil {
		return false
	}
	defer f.Close()
	names, _ := f.Readdirnames(limit + 1)
	return len(names) > limit
}

// listDir snapshots dir as name → fingerprint.
func (w *MemWatcher) listDir(dir string) map[string]polledFile {
	out := make(map[string]polledFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return out
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		out[e.Name()] = polledFile{mtime: info.ModTime(), size: info.Size(), dir: e.IsDir()}
	}
	return out
}

// pollOversized re-lists every polled directory and emits the same events the
// fsnotify path would: new/changed *.cog.md files are scheduled for indexing
// and new subdirectories are routed through watchDir.
func (w *MemWatcher) pollOversized(fsw *fsnotify.Watcher, debounce map[string]*time.Timer, mu *sync.Mutex) {
	for dir, prev := range w.polled {
		cur := w.listDir(dir)
		if len(cur) == 0 {
			if _, err := os.Stat(dir); err != nil {
				delete(w.polled, dir)
				continue
			}
		}
		w.polled[dir] = cur
		for name, fp := range cur {
			old, seen := prev[name]
			if seen && old == fp {
				continue
			}
			full := filepath.Join(dir, name)
			switch {
			case fp.dir:
				if !seen {
					w.watchDir(fsw, full)
				}
			case strings.HasSuffix(name, ".cog.md"):
				w.scheduleIndex(full, debounce, mu)
			}
		}
	}
}

// scheduleIndex enqueues path for a debounced IndexFile call.  If the path is
// already queued the existing timer is reset, coalescing rapid writes into a
// single upsert.
func (w *MemWatcher) scheduleIndex(path string, debounce map[string]*time.Timer, mu *sync.Mutex) {
	mu.Lock()
	if t, ok := debounce[path]; ok {
		t.Stop()
	}
	debounce[path] = time.AfterFunc(memWatcherDebounce, func() {
		mu.Lock()
		delete(debounce, path)
		mu.Unlock()

		if err := w.indexer.IndexFile(path); err != nil {
			slog.Warn("mem_watcher: IndexFile failed", "path", path, "err", err)
		} else {
			slog.Debug("mem_watcher: indexed", "path", path)
		}
	})
	mu.Unlock()
}
