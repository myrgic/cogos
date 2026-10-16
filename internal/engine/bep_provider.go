// bep_provider.go — BEPProvider: agent definition distribution across nodes.
// Extracted from root package main into internal/engine as Phase 2 S1.
//
// The provider watches .cog/bin/agents/definitions/ for CRD file changes and
// invokes the onChange callback when files are created, modified, or deleted.

package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"

	bep "github.com/myrgic/cogos/pkg/substrate/bep"
)

// ─── Type aliases ────────────────────────────────────────────────────────────────

// BEPPeer is an alias for the canonical bep.Peer type.
type BEPPeer = bep.Peer

// BEPConfig is an alias for the canonical bep.Config type.
type BEPConfig = bep.Config

// BEPSyncStatus is an alias for the canonical bep.SyncStatus type.
type BEPSyncStatus = bep.SyncStatus

// ─── BEPProvider ─────────────────────────────────────────────────────────────────

// BEPProvider manages agent definition distribution across nodes.
// Phase 1: file watching + local change detection.
// Phase 2 (current): integrated with BEPEngine for cross-node sync.
type BEPProvider struct {
	root             string // workspace root
	watchDir         string // .cog/bin/agents/definitions/
	peers            []bep.Peer
	mu               sync.Mutex
	running          bool
	stopCh           chan struct{}
	onChange         func(filename string)   // primary callback when a CRD file changes
	onChangeHandlers []func(filename string) // additional change handlers (e.g., BEPEngine)
	lastSync         time.Time
	watcher          *fsnotify.Watcher // nil if fsnotify unavailable; falls back to polling
	receiver         *receiverState    // ring buffer for received CRD events

	// recheckInterval is how often runFSNotify re-checks watchDir against the
	// entry cap; 0 uses memWatcherPollInterval.
	recheckInterval time.Duration
	// pollInterval overrides the polling period; 0 uses 5s for the fsnotify
	// failure fallback and memWatcherPollInterval for an oversized directory.
	pollInterval time.Duration
}

// ─── Constructor ────────────────────────────────────────────────────────────────

// NewBEPProvider creates a new provider for the given workspace root.
// The watch directory defaults to {root}/.cog/bin/agents/definitions/.
func NewBEPProvider(root string) *BEPProvider {
	return &BEPProvider{
		root:     root,
		watchDir: filepath.Join(root, ".cog", "bin", "agents", "definitions"),
		stopCh:   make(chan struct{}),
		receiver: &receiverState{
			history:    make([]bep.ReceivedEvent, 0, receiverMaxHistory),
			maxHistory: receiverMaxHistory,
		},
	}
}

// ─── Configuration ──────────────────────────────────────────────────────────────

// LoadConfig reads cluster config from .cog/config/cluster.yaml.
// Returns a default (disabled) config if the file does not exist.
func (p *BEPProvider) LoadConfig() (*bep.Config, error) {
	cfgPath := filepath.Join(p.root, ".cog", "config", "cluster.yaml")
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &bep.Config{Enabled: false, Discovery: "static"}, nil
		}
		return nil, fmt.Errorf("read cluster config: %w", err)
	}

	var cfg bep.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse cluster config: %w", err)
	}

	// An enabled node that omits listenPort gets the package default
	// (bep.DefaultListenPort), not an OS-assigned random port — peers need a
	// stable address to dial. Tests that want a random port construct
	// bep.Config{} directly and bypass this path. See #336.
	if cfg.Enabled && cfg.ListenPort == 0 {
		cfg.ListenPort = bep.DefaultListenPort
	}

	return &cfg, nil
}

// ─── Lifecycle ──────────────────────────────────────────────────────────────────

// Start begins watching for CRD file changes in the definitions directory.
func (p *BEPProvider) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("bep provider already running")
	}

	// Ensure the watch directory exists.
	if err := os.MkdirAll(p.watchDir, 0755); err != nil {
		return fmt.Errorf("create watch dir: %w", err)
	}

	// Try fsnotify first; fall back to polling if it fails.
	// An oversized definitions directory is polled instead: kqueue holds one
	// fd per file in a watched dir.
	oversized := dirOverCap(p.watchDir, memWatcherDirEntryCap)
	fsWatcher, err := fsnotify.NewWatcher()
	if oversized {
		log.Printf("[bep] %s has more than %d entries, using polling fallback", p.watchDir, memWatcherDirEntryCap)
		if err == nil {
			fsWatcher.Close()
		}
		fsWatcher = nil
	} else if err != nil {
		log.Printf("[bep] fsnotify unavailable (%v), using polling fallback", err)
		fsWatcher = nil
	} else {
		if err := fsWatcher.Add(p.watchDir); err != nil {
			log.Printf("[bep] cannot watch %s (%v), using polling fallback", p.watchDir, err)
			fsWatcher.Close()
			fsWatcher = nil
		}
	}
	p.watcher = fsWatcher

	// Load peers from config.
	cfg, err := p.LoadConfig()
	if err != nil {
		log.Printf("[bep] warning: could not load cluster config: %v", err)
	} else {
		p.peers = cfg.Peers
	}

	p.running = true
	p.stopCh = make(chan struct{})

	if fsWatcher != nil {
		go p.runFSNotify(fsWatcher)
	} else if oversized {
		go p.runPolling(memWatcherPollInterval)
	} else {
		go p.runPolling(5 * time.Second)
	}

	log.Printf("[bep] provider started, watching %s", p.watchDir)
	return nil
}

// Stop halts the file watcher and cleans up resources.
func (p *BEPProvider) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return
	}

	close(p.stopCh)
	if p.watcher != nil {
		p.watcher.Close()
		p.watcher = nil
	}
	p.running = false
	log.Printf("[bep] provider stopped")
}

// ─── Callbacks ──────────────────────────────────────────────────────────────────

// OnFileChange sets the primary callback for CRD file changes.
func (p *BEPProvider) OnFileChange(fn func(filename string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onChange = fn
}

// AddChangeHandler registers an additional change handler.
func (p *BEPProvider) AddChangeHandler(fn func(filename string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onChangeHandlers = append(p.onChangeHandlers, fn)
}

// ─── Queries ────────────────────────────────────────────────────────────────────

// ListPeers returns known peer nodes from the loaded configuration.
func (p *BEPProvider) ListPeers() []bep.Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]bep.Peer, len(p.peers))
	copy(out, p.peers)
	return out
}

// Status returns current sync state.
func (p *BEPProvider) Status() bep.SyncStatus {
	p.mu.Lock()
	defer p.mu.Unlock()

	cfg, _ := p.LoadConfig()
	deviceID := ""
	enabled := false
	if cfg != nil {
		deviceID = cfg.DeviceID
		enabled = cfg.Enabled
	}

	return bep.SyncStatus{
		Enabled:   enabled,
		DeviceID:  deviceID,
		PeerCount: len(p.peers),
		WatchDir:  p.watchDir,
		LastSync:  p.lastSync,
	}
}

// ─── Internal: fsnotify event loop ──────────────────────────────────────────────

func (p *BEPProvider) runFSNotify(w *fsnotify.Watcher) {
	const debounce = 500 * time.Millisecond

	// All debounce state (pending set, timer) is owned exclusively by this
	// goroutine. The debounce timer does NOT mutate shared state from its own
	// goroutine; instead it sends a tick on flushCh which this loop selects on.
	// This keeps every read/write of `pending` and `timer` on a single
	// goroutine — no lock needed and no data race (the previous design had the
	// time.AfterFunc closure write `pending` while the loop read it).
	pending := make(map[string]struct{})
	var timer *time.Timer
	flushCh := make(chan struct{}, 1)

	// The definitions dir can grow past the entry cap after Start; re-check on
	// a tick and fall over to polling for the rest of the process life.
	recheck := p.recheckInterval
	if recheck == 0 {
		recheck = memWatcherPollInterval
	}
	recheckTicker := time.NewTicker(recheck)
	defer recheckTicker.Stop()

	flush := func() {
		if len(pending) == 0 {
			return
		}

		p.mu.Lock()
		if !p.running {
			p.mu.Unlock()
			return
		}
		cb := p.onChange
		extraHandlers := make([]func(string), len(p.onChangeHandlers))
		copy(extraHandlers, p.onChangeHandlers)
		p.lastSync = time.Now()
		p.mu.Unlock()

		for name := range pending {
			log.Printf("[bep] change detected: %s", name)
			if cb != nil {
				cb(name)
			}
			for _, h := range extraHandlers {
				h(name)
			}
		}
		pending = make(map[string]struct{})
	}

	for {
		select {
		case <-p.stopCh:
			if timer != nil {
				timer.Stop()
			}
			return

		case <-recheckTicker.C:
			if !dirOverCap(p.watchDir, memWatcherDirEntryCap) {
				continue
			}
			// Stop closes stopCh and the watcher under p.mu; only the caller
			// that still owns the watcher closes it.
			select {
			case <-p.stopCh:
				return
			default:
			}
			p.mu.Lock()
			if !p.running || p.watcher != w {
				p.mu.Unlock()
				return
			}
			p.watcher = nil
			p.mu.Unlock()
			log.Printf("[bep] %s grew past %d entries, switching to polling", p.watchDir, memWatcherDirEntryCap)
			if timer != nil {
				timer.Stop()
			}
			flush()
			w.Close()
			p.runPolling(memWatcherPollInterval)
			return

		case <-flushCh:
			// Debounce window elapsed: process accumulated changes. Runs on
			// this loop goroutine, so touching `pending` here is race-free.
			flush()

		case event, ok := <-w.Events:
			if !ok {
				return
			}
			base := filepath.Base(event.Name)
			if !bep.IsAgentCRDFile(base) {
				continue
			}
			if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			pending[base] = struct{}{}
			if timer != nil {
				timer.Stop()
			}
			// The timer fires a non-blocking signal; it never touches shared
			// state. The buffered channel + default in the closure ensure we
			// never block the timer goroutine and never enqueue more than one
			// pending tick.
			timer = time.AfterFunc(debounce, func() {
				select {
				case flushCh <- struct{}{}:
				default:
				}
			})

		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			log.Printf("[bep] fsnotify error: %v", err)
		}
	}
}

// ─── Internal: polling fallback ─────────────────────────────────────────────────

func (p *BEPProvider) runPolling(interval time.Duration) {
	if p.pollInterval > 0 {
		interval = p.pollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	snapshot := p.scanModTimes()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			current := p.scanModTimes()
			p.diffAndNotify(snapshot, current)
			snapshot = current
		}
	}
}

func (p *BEPProvider) scanModTimes() map[string]time.Time {
	result := make(map[string]time.Time)
	entries, err := os.ReadDir(p.watchDir)
	if err != nil {
		return result
	}
	for _, entry := range entries {
		if entry.IsDir() || !bep.IsAgentCRDFile(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		result[entry.Name()] = info.ModTime()
	}
	return result
}

func (p *BEPProvider) diffAndNotify(old, current map[string]time.Time) {
	p.mu.Lock()
	cb := p.onChange
	extraHandlers := make([]func(string), len(p.onChangeHandlers))
	copy(extraHandlers, p.onChangeHandlers)
	p.mu.Unlock()

	if cb == nil && len(extraHandlers) == 0 {
		return
	}

	notifyAll := func(name string) {
		if cb != nil {
			cb(name)
		}
		for _, h := range extraHandlers {
			h(name)
		}
	}

	changed := false

	for name, modTime := range current {
		oldTime, existed := old[name]
		if !existed || !modTime.Equal(oldTime) {
			log.Printf("[bep] change detected (poll): %s", name)
			notifyAll(name)
			changed = true
		}
	}

	for name := range old {
		if _, exists := current[name]; !exists {
			log.Printf("[bep] deletion detected (poll): %s", name)
			notifyAll(name)
			changed = true
		}
	}

	if changed {
		p.mu.Lock()
		p.lastSync = time.Now()
		p.mu.Unlock()
	}
}

// ─── Test accessors ──────────────────────────────────────────────────────────────
// These methods are used by tests in root package main that need to inspect or
// set provider state without accessing unexported fields directly.

// IsRunning reports whether the provider is currently running.
func (p *BEPProvider) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// HasFSWatcher reports whether an fsnotify watcher is active (vs polling).
func (p *BEPProvider) HasFSWatcher() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.watcher != nil
}

// WatchDir returns the directory being watched for agent CRD changes.
func (p *BEPProvider) WatchDir() string {
	return p.watchDir
}

// Root returns the workspace root directory.
func (p *BEPProvider) Root() string {
	return p.root
}

// ScanModTimes returns a map of CRD filename → last-modified time for all
// agent CRD files currently present in the watch directory.
// This is exported for use by tests.
func (p *BEPProvider) ScanModTimes() map[string]time.Time {
	return p.scanModTimes()
}

// DiffAndNotify compares old and current mod-time maps and fires the change
// callback for any files that were added, modified, or deleted.
// Exported for use by tests.
func (p *BEPProvider) DiffAndNotify(old, current map[string]time.Time) {
	p.diffAndNotify(old, current)
}

// SetPeers replaces the peer list. Used by tests to set up state without
// calling Start() (which would load peers from cluster config on disk).
func (p *BEPProvider) SetPeers(peers []bep.Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = peers
}

// InitPollingForTest sets the provider into running/polling state without
// using fsnotify, and starts the polling goroutine. The caller is responsible
// for calling Stop() when done. This exists solely for the polling-fallback
// integration test in the root package.
func (p *BEPProvider) InitPollingForTest() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("bep provider already running")
	}

	if err := os.MkdirAll(p.watchDir, 0755); err != nil {
		return fmt.Errorf("create watch dir: %w", err)
	}

	p.running = true
	p.stopCh = make(chan struct{})
	p.watcher = nil // nil watcher → polling mode

	go p.runPolling(5 * time.Second)
	return nil
}
