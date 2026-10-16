package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileWatcherDetectsAppendedLines(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, "events.jsonl")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	watcher := NewFileWatcher(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan string, 2)
	errCh := make(chan error, 1)
	go func() {
		errCh <- watcher.Watch(ctx, path, func(line []byte) error {
			received <- string(line)
			return nil
		})
	}()

	appendLine(t, path, `{"event":"one"}`+"\n")
	if got := waitForString(t, received); got != `{"event":"one"}` {
		t.Fatalf("first line = %q; want %q", got, `{"event":"one"}`)
	}

	appendLine(t, path, `{"event":"two"}`+"\n")
	if got := waitForString(t, received); got != `{"event":"two"}` {
		t.Fatalf("second line = %q; want %q", got, `{"event":"two"}`)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Watch returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("watcher did not stop after cancellation")
	}
}

func TestTailerManagerRunsMultipleTailers(t *testing.T) {
	t.Parallel()

	out := make(chan CogBlock, 4)
	manager := NewTailerManager(out)

	if err := manager.Register(&stubStreamTailer{
		name: "claude-code",
		blocks: []CogBlock{{
			ID:        "claude-1",
			Kind:      BlockImport,
			Timestamp: time.Now().UTC(),
		}},
	}, "/tmp/claude.jsonl"); err != nil {
		t.Fatalf("Register claude-code: %v", err)
	}

	if err := manager.Register(&stubStreamTailer{
		name: "headless",
		blocks: []CogBlock{{
			ID:        "headless-1",
			Kind:      BlockImport,
			Timestamp: time.Now().UTC(),
		}},
	}, "/tmp/headless.jsonl"); err != nil {
		t.Fatalf("Register headless: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(ctx)
	}()

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case block := <-out:
			seen[block.ID] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for tailer output")
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not stop after cancellation")
	}

	stats := manager.Stats()
	if stats["claude-code"].EventsIngested != 1 {
		t.Fatalf("claude-code events = %d; want 1", stats["claude-code"].EventsIngested)
	}
	if stats["headless"].EventsIngested != 1 {
		t.Fatalf("headless events = %d; want 1", stats["headless"].EventsIngested)
	}
	if stats["claude-code"].LastEventTime.IsZero() {
		t.Fatal("claude-code last event time should be set")
	}
	if stats["headless"].LastEventTime.IsZero() {
		t.Fatal("headless last event time should be set")
	}
}

func TestTailerManagerGracefulShutdown(t *testing.T) {
	t.Parallel()

	out := make(chan CogBlock, 1)
	manager := NewTailerManager(out)

	stopped := make(chan struct{})
	if err := manager.Register(&blockingTailer{name: "cursor", stopped: stopped}, "/tmp/cursor.jsonl"); err != nil {
		t.Fatalf("Register cursor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Run(ctx)
	}()

	cancel()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("tailer did not observe cancellation")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not return after cancellation")
	}

	stats := manager.Stats()
	if stats["cursor"].Errors != 0 {
		t.Fatalf("cursor errors = %d; want 0", stats["cursor"].Errors)
	}
}

type stubStreamTailer struct {
	name   string
	blocks []CogBlock
}

func (s *stubStreamTailer) Name() string { return s.name }

func (s *stubStreamTailer) Tail(ctx context.Context, path string, out chan<- CogBlock) error {
	for _, block := range s.blocks {
		select {
		case out <- block:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

type blockingTailer struct {
	name    string
	stopped chan struct{}
}

func (b *blockingTailer) Name() string { return b.name }

func (b *blockingTailer) Tail(ctx context.Context, path string, out chan<- CogBlock) error {
	<-ctx.Done()
	close(b.stopped)
	return nil
}

func appendLine(t *testing.T, path, content string) {
	t.Helper()

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()

	if _, err := file.WriteString(content); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
}

func waitForString(t *testing.T, values <-chan string) string {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watched line")
		return ""
	}
}
