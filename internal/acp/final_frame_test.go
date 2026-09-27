package acp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A subprocess that writes its last frame and exits immediately must still
// deliver that frame. Before the fix, Wait ran concurrently with readLoop and
// cmd.Wait closed the stdout pipe at process exit, discarding unread output:
// the final frame was lost in 300/300 runs of this shape, and the SIGINT
// cancellation test flaked in CI when its trap's "cancelled" frame lost the
// same race.
func TestEvents_FinalFrameSurvivesImmediateExit(t *testing.T) {
	script := filepath.Join(t.TempDir(), "burst.sh")
	body := "#!/bin/bash\n" +
		"for i in $(seq 1 200); do echo '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"x\"}'; done\n" +
		"echo '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"last\"}'\n" +
		"exit 0\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	const runs = 20
	lost := 0
	for i := 0; i < runs; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		sp, err := Spawn(ctx, SpawnOpts{ClaudePath: script})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		// A slow consumer: the 32-slot event buffer fills and readLoop blocks
		// on send while the process has already exited. This is the shape
		// that lost the frame (a relaying reader such as ManagedSession's
		// pump is exactly this).
		time.Sleep(50 * time.Millisecond)
		seen := false
		for ev := range sp.Events() { // documented order: drain, then Wait
			if ev.Result != nil && ev.Result.Result == "last" {
				seen = true
			}
		}
		if err := sp.Wait(); err != nil {
			t.Fatalf("run %d: wait: %v", i, err)
		}
		if !seen {
			lost++
		}
		cancel()
	}
	if lost > 0 {
		t.Fatalf("final frame lost in %d/%d runs", lost, runs)
	}
}
