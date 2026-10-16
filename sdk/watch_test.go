package sdk

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseWatchPattern(t *testing.T) {
	tests := []struct {
		name      string
		pattern   string
		wantNS    string
		wantPath  string
		wantWild  bool
		wantRecur bool
		wantErr   bool
	}{
		{
			name:    "simple namespace",
			pattern: "cog://mem",
			wantNS:  "mem",
		},
		{
			name:     "namespace with path",
			pattern:  "cog://mem/semantic/insights",
			wantNS:   "mem",
			wantPath: "semantic/insights",
		},
		{
			name:     "wildcard path",
			pattern:  "cog://mem/semantic/*",
			wantNS:   "mem",
			wantPath: "semantic",
			wantWild: true,
		},
		{
			name:      "recursive wildcard",
			pattern:   "cog://mem/semantic/**",
			wantNS:    "mem",
			wantPath:  "semantic",
			wantWild:  true,
			wantRecur: true,
		},
		{
			name:    "signals namespace",
			pattern: "cog://signals/*",
			wantNS:  "signals",
			wantPath: "",
			wantWild: true,
		},
		{
			name:    "invalid scheme",
			pattern: "http://mem",
			wantErr: true,
		},
		{
			name:    "unknown namespace",
			pattern: "cog://unknown/*",
			wantErr: true,
		},
		// myrgic/cogos#489 round 5: parseWatchPattern must reject '..'
		// segments the same way sdk/uri.go's ParseURI does, mirroring the
		// defense-in-depth layering used everywhere else in this module.
		{
			name:    "traversal segment forward slash",
			pattern: "cog://ledger/../../../../etc",
			wantErr: true,
		},
		{
			name:    "traversal segment backslash-only",
			pattern: `cog://ledger/..\..\..\etc`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWatchPattern(tt.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseWatchPattern() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if got.namespace != tt.wantNS {
				t.Errorf("namespace = %q, want %q", got.namespace, tt.wantNS)
			}
			if got.path != tt.wantPath {
				t.Errorf("path = %q, want %q", got.path, tt.wantPath)
			}
			if got.wildcard != tt.wantWild {
				t.Errorf("wildcard = %v, want %v", got.wildcard, tt.wantWild)
			}
			if got.recursive != tt.wantRecur {
				t.Errorf("recursive = %v, want %v", got.recursive, tt.wantRecur)
			}
		})
	}
}

func TestWatchEventTypes(t *testing.T) {
	if WatchCreated != "created" {
		t.Errorf("WatchCreated = %q, want %q", WatchCreated, "created")
	}
	if WatchModified != "modified" {
		t.Errorf("WatchModified = %q, want %q", WatchModified, "modified")
	}
	if WatchDeleted != "deleted" {
		t.Errorf("WatchDeleted = %q, want %q", WatchDeleted, "deleted")
	}
}

func TestWatcher_Close(t *testing.T) {
	// Create test workspace
	tmpDir := t.TempDir()
	cogDir := filepath.Join(tmpDir, ".cog")
	memDir := filepath.Join(cogDir, "mem", "semantic")

	if err := os.MkdirAll(memDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create id.cog
	idPath := filepath.Join(cogDir, "id.cog")
	if err := os.WriteFile(idPath, []byte("---\nid: test\n---\n# Test"), 0644); err != nil {
		t.Fatal(err)
	}

	kernel, err := Connect(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer kernel.Close()

	ctx := context.Background()
	watcher, err := kernel.WatchURI(ctx, "cog://mem/semantic/*")
	if err != nil {
		t.Fatal(err)
	}

	// Close should not error
	if err := watcher.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Double close should be safe
	if err := watcher.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestWatcher_DetectsFileChanges(t *testing.T) {
	// Create test workspace
	tmpDir := t.TempDir()
	cogDir := filepath.Join(tmpDir, ".cog")
	memDir := filepath.Join(cogDir, "mem", "semantic")

	if err := os.MkdirAll(memDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create id.cog
	idPath := filepath.Join(cogDir, "id.cog")
	if err := os.WriteFile(idPath, []byte("---\nid: test\n---\n# Test"), 0644); err != nil {
		t.Fatal(err)
	}

	kernel, err := Connect(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer kernel.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watcher, err := kernel.WatchURI(ctx, "cog://mem/semantic/*")
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	// Create a file in the watched directory
	testFile := filepath.Join(memDir, "test-insight.cog.md")
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.WriteFile(testFile, []byte("---\ntype: insight\nid: test\ntitle: Test\ncreated: 2026-01-10\n---\n# Test"), 0644)
	}()

	// Wait for event
	select {
	case event := <-watcher.Events:
		if event.Type != WatchCreated {
			t.Errorf("event.Type = %q, want %q", event.Type, WatchCreated)
		}
		if event.FilePath != testFile {
			t.Errorf("event.FilePath = %q, want %q", event.FilePath, testFile)
		}
	case <-ctx.Done():
		t.Error("timeout waiting for watch event")
	}
}

func TestWatcher_ContextCancellation(t *testing.T) {
	// Create test workspace
	tmpDir := t.TempDir()
	cogDir := filepath.Join(tmpDir, ".cog")
	memDir := filepath.Join(cogDir, "mem", "semantic")

	if err := os.MkdirAll(memDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create id.cog
	idPath := filepath.Join(cogDir, "id.cog")
	if err := os.WriteFile(idPath, []byte("---\nid: test\n---\n# Test"), 0644); err != nil {
		t.Fatal(err)
	}

	kernel, err := Connect(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer kernel.Close()

	ctx, cancel := context.WithCancel(context.Background())

	watcher, err := kernel.WatchURI(ctx, "cog://mem/semantic/*")
	if err != nil {
		t.Fatal(err)
	}

	// Cancel context
	cancel()

	// Give event loop time to process cancellation
	time.Sleep(100 * time.Millisecond)

	// Events channel should eventually close
	select {
	case _, ok := <-watcher.Events:
		if ok {
			// Got an event before close, that's fine
		}
		// Channel closed, as expected
	case <-time.After(time.Second):
		t.Error("timeout waiting for events channel to close")
	}

	watcher.Close()
}

func TestKernel_WatchURI_NotConnected(t *testing.T) {
	// Create and immediately close a kernel
	tmpDir := t.TempDir()
	cogDir := filepath.Join(tmpDir, ".cog")
	if err := os.MkdirAll(cogDir, 0755); err != nil {
		t.Fatal(err)
	}
	idPath := filepath.Join(cogDir, "id.cog")
	if err := os.WriteFile(idPath, []byte("---\nid: test\n---\n# Test"), 0644); err != nil {
		t.Fatal(err)
	}

	kernel, err := Connect(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	kernel.Close()

	// Should error since kernel is closed
	_, err = kernel.WatchURI(context.Background(), "cog://mem/*")
	if err == nil {
		t.Error("expected error for closed kernel")
	}
}

// TestResolveWatchPathsSanitizesNTFSIllegalChars regression-tests
// myrgic/cogos#489 round 5: resolveWatchPaths joined pattern.path into a
// filesystem path with no NTFS-illegal-character sanitization (and no
// traversal guard at all — parseWatchPattern never rejected '..' segments
// the way sdk/uri.go's ParseURI did), the same class of bug this module's
// sibling projectors (memoryProjector, ledgerProjector, ...) were fixed for
// one round earlier. Constructs the ParsedURI-equivalent (*watchPattern)
// directly to exercise resolveWatchPaths in isolation, mirroring
// pathtraversal_test.go's approach for the resource projectors.
func TestResolveWatchPathsSanitizesNTFSIllegalChars(t *testing.T) {
	kernel := newTestKernel(t)

	cases := []struct {
		name      string
		namespace string
	}{
		{"memory", "mem"},
		{"ledger", "ledger"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pattern := &watchPattern{namespace: tc.namespace, path: "http:cog", raw: "cog://" + tc.namespace + "/http:cog"}
			paths, _, err := kernel.resolveWatchPaths(pattern)
			if err != nil {
				t.Fatalf("resolveWatchPaths: %v", err)
			}
			for _, p := range paths {
				if strings.Contains(p, "http:cog") {
					t.Errorf("resolveWatchPaths(%q) returned path %q still containing the raw colon-bearing component — NTFS-illegal, myrgic/cogos#489", tc.namespace, p)
				}
				if !strings.Contains(p, "http%3Acog") {
					t.Errorf("resolveWatchPaths(%q) returned path %q missing expected sanitized form", tc.namespace, p)
				}
			}
		})
	}
}

// TestResolveWatchPathsRejectsTraversal regression-tests the same round-5
// finding for the traversal shape: an unauthenticated
// GET /ws/watch?uri=cog:mem/../../../../ reaches resolveWatchPaths via
// Kernel.WatchURI with the raw '..' segments intact before this fix,
// registering a filesystem watch outside the workspace root.
func TestResolveWatchPathsRejectsTraversal(t *testing.T) {
	kernel := newTestKernel(t)

	pattern := &watchPattern{namespace: "mem", path: "../../../../secret", raw: "cog://mem/../../../../secret"}
	paths, _, err := kernel.resolveWatchPaths(pattern)
	if err != nil {
		t.Fatalf("resolveWatchPaths: %v", err)
	}
	for _, p := range paths {
		rel, relErr := filepath.Rel(kernel.MemoryDir(), p)
		if relErr != nil {
			t.Fatalf("filepath.Rel: %v", relErr)
		}
		if strings.HasPrefix(rel, "..") {
			t.Fatalf("resolveWatchPaths allowed escape: path=%q is outside MemoryDir %q (rel=%q)", p, kernel.MemoryDir(), rel)
		}
	}
}

func TestPathToURI(t *testing.T) {
	// Create test workspace
	tmpDir := t.TempDir()
	cogDir := filepath.Join(tmpDir, ".cog")
	memDir := filepath.Join(cogDir, "mem")

	if err := os.MkdirAll(filepath.Join(memDir, "semantic"), 0755); err != nil {
		t.Fatal(err)
	}

	// Create id.cog
	idPath := filepath.Join(cogDir, "id.cog")
	if err := os.WriteFile(idPath, []byte("---\nid: test\n---\n# Test"), 0644); err != nil {
		t.Fatal(err)
	}

	kernel, err := Connect(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer kernel.Close()

	// Create a mock watcher with the kernel
	w := &Watcher{kernel: kernel}

	pattern := &watchPattern{namespace: "mem"}
	mapping := map[string]string{
		memDir: "cog:mem",
	}

	tests := []struct {
		filePath string
		wantURI  string
	}{
		{
			filePath: filepath.Join(memDir, "semantic", "test.cog.md"),
			wantURI:  "cog:mem/semantic/test",
		},
		{
			filePath: filepath.Join(memDir, "semantic", "test.md"),
			wantURI:  "cog:mem/semantic/test",
		},
		{
			filePath: filepath.Join(memDir, "episodic", "session.cog.md"),
			wantURI:  "cog:mem/episodic/session",
		},
	}

	for _, tt := range tests {
		t.Run(tt.filePath, func(t *testing.T) {
			got := w.pathToURI(tt.filePath, pattern, mapping)
			if got != tt.wantURI {
				t.Errorf("pathToURI() = %q, want %q", got, tt.wantURI)
			}
		})
	}
}

// An oversized directory is polled instead of handed to fsnotify (kqueue would
// hold one fd per file in it), and the poll still reports new files.
func TestWatcher_OversizedDirIsPolled(t *testing.T) {
	old := watchPollInterval
	watchPollInterval = 50 * time.Millisecond
	defer func() { watchPollInterval = old }()

	tmpDir := t.TempDir()
	cogDir := filepath.Join(tmpDir, ".cog")
	bulk := filepath.Join(cogDir, "mem", "bulk")
	if err := os.MkdirAll(bulk, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cogDir, "id.cog"), []byte("---\nid: test\n---\n# Test"), 0644); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= watchDirEntryCap; i++ {
		name := filepath.Join(bulk, "doc-"+strings.Repeat("x", i%7)+string(rune('a'+i%26))+"-"+time.Duration(i).String()+".cog.md")
		if err := os.WriteFile(name, []byte("# x"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	kernel, err := Connect(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer kernel.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watcher, err := kernel.WatchURI(ctx, "cog:mem/**")
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	// The event loop owns watcher.polled, so assert through fsnotify's own
	// list; the polled create event below proves the poll path.
	for _, p := range watcher.fsWatch.WatchList() {
		if strings.HasPrefix(p, bulk) {
			t.Fatalf("fsnotify watches %s inside the oversized dir", p)
		}
	}

	newFile := filepath.Join(bulk, "new-insight.cog.md")
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.WriteFile(newFile, []byte("# new"), 0644)
	}()
	for {
		select {
		case ev := <-watcher.Events:
			if ev.FilePath == newFile {
				if ev.Type != WatchCreated {
					t.Errorf("event.Type = %q, want %q", ev.Type, WatchCreated)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for polled create event")
		}
	}
}
