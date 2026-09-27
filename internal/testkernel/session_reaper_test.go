package testkernel_test

import (
	"context"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/testkernel"
)

// Review finding on #622: Boot (the `cogos serve` path) called srv.Serve
// directly, so the session reaper wired into Server.Start never ran in
// production. Boot a real kernel and require the reaper to be running while
// it serves, and stopped after Stop.
func TestBoot_RunsSessionReaper(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	k, err := testkernel.Boot(ctx, t)
	if err != nil {
		t.Fatalf("testkernel.Boot: %v", err)
	}
	if !k.SessionReaperRunning() {
		_ = k.Stop()
		t.Fatal("booted kernel is serving but its session reaper is not running")
	}
	if err := k.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if k.SessionReaperRunning() {
		t.Fatal("session reaper still running after Stop")
	}
}
