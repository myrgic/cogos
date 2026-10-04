//go:build darwin

package engine

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLaunchctlController_Restart_RealJob exercises Restart against a real,
// throwaway launchd job: it must observe a new PID and set Restarted. Opt-in
// (COGOS_LAUNCHD_TESTS=1) because it loads a job into the user's launchd.
func TestLaunchctlController_Restart_RealJob(t *testing.T) {
	if os.Getenv("COGOS_LAUNCHD_TESTS") != "1" {
		t.Skip("set COGOS_LAUNCHD_TESTS=1 to run against real launchd")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	label := "com.cogos.test.restart." + strings.ReplaceAll(filepath.Base(t.TempDir()), "_", "")
	plist := filepath.Join(home, "Library", "LaunchAgents", label+".plist")
	body := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>` + label + `</string>
<key>ProgramArguments</key><array><string>/bin/sleep</string><string>600</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
</dict></plist>`
	if err := os.WriteFile(plist, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	uid := currentUID()
	t.Cleanup(func() {
		_ = exec.Command("launchctl", "bootout", "gui/"+uid+"/"+label).Run()
		_ = os.Remove(plist)
	})
	if out, err := exec.Command("launchctl", "bootstrap", "gui/"+uid, plist).CombinedOutput(); err != nil {
		t.Fatalf("bootstrap: %v %s", err, out)
	}

	c := NewLaunchctlController()
	def := ServiceDef{Kind: ServiceKindManaged, Launchd: label}
	ctx := context.Background()

	var before *ServiceStatus
	for i := 0; i < 40; i++ {
		before, _ = c.Status(ctx, "t", def)
		if before != nil && before.Running {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if before == nil || !before.Running {
		t.Fatalf("job never started: %+v", before)
	}

	st, err := c.Restart(ctx, "t", def)
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !st.Restarted || st.PID == before.PID || st.PreviousPID != before.PID {
		t.Fatalf("restart not confirmed: before=%d after=%+v", before.PID, st)
	}
}
