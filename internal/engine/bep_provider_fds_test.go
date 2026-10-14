package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func writeBEPCRDs(t *testing.T, dir string, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("a%d.agent.yaml", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBEPProvider_OversizedDirStartsInPolling(t *testing.T) {
	p := NewBEPProvider(t.TempDir())
	if err := os.MkdirAll(p.watchDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeBEPCRDs(t, p.watchDir, 0, memWatcherDirEntryCap+1)
	p.pollInterval = 50 * time.Millisecond
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	p.mu.Lock()
	watching := p.watcher != nil
	p.mu.Unlock()
	if watching {
		t.Fatal("oversized definitions dir was handed to fsnotify")
	}
}

func TestBEPProvider_GrowingDirSwitchesToPolling(t *testing.T) {
	p := NewBEPProvider(t.TempDir())
	p.recheckInterval = 100 * time.Millisecond
	p.pollInterval = 100 * time.Millisecond
	var mu sync.Mutex
	changed := map[string]int{}
	p.OnFileChange(func(name string) {
		mu.Lock()
		changed[name]++
		mu.Unlock()
	})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	p.mu.Lock()
	if p.watcher == nil {
		p.mu.Unlock()
		t.Skip("fsnotify unavailable; nothing to switch from")
	}
	p.mu.Unlock()

	writeBEPCRDs(t, p.watchDir, 0, memWatcherDirEntryCap+1)
	deadline := time.Now().Add(4 * time.Second)
	for {
		p.mu.Lock()
		switched := p.watcher == nil
		p.mu.Unlock()
		if switched {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("provider never switched to polling after definitions dir crossed the cap")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Let the poller take its baseline, then a later change must still reload.
	time.Sleep(400 * time.Millisecond)
	time.Sleep(1100 * time.Millisecond) // mtime granularity
	writeBEPCRDs(t, p.watchDir, 9000, 9001)
	deadline = time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := changed["a9000.agent.yaml"]
		mu.Unlock()
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("polling never reported the later file change")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
