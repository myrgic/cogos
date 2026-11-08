//go:build !windows

package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fakeProc builds a fake /proc tree in a temp dir.
type fakeProc struct {
	t     *testing.T
	root  string
	tcp   []string
	inode int
}

func newFakeProc(t *testing.T) *fakeProc {
	t.Helper()
	f := &fakeProc{t: t, root: t.TempDir(), inode: 1000}
	if err := os.MkdirAll(filepath.Join(f.root, "net"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.flushTCP()
	return f
}

func (f *fakeProc) flushTCP() {
	hdr := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	if err := os.WriteFile(filepath.Join(f.root, "net", "tcp"), []byte(hdr+strings.Join(f.tcp, "\n")+"\n"), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

// add creates /proc/<pid>; listenPort>0 also makes it the listener on that port.
func (f *fakeProc) add(pid, uid int, comm string, env map[string]string, cgroup string, listenPort int) {
	f.t.Helper()
	d := filepath.Join(f.root, strconv.Itoa(pid))
	if err := os.MkdirAll(filepath.Join(d, "fd"), 0o755); err != nil {
		f.t.Fatal(err)
	}
	var envb strings.Builder
	for k, v := range env {
		envb.WriteString(k + "=" + v + "\x00")
	}
	w := func(name, content string) {
		if err := os.WriteFile(filepath.Join(d, name), []byte(content), 0o644); err != nil {
			f.t.Fatal(err)
		}
	}
	w("environ", envb.String())
	w("comm", comm+"\n")
	w("status", fmt.Sprintf("Name:\t%s\nUid:\t%d\t%d\t%d\t%d\n", comm, uid, uid, uid, uid))
	if cgroup != "" {
		w("cgroup", "0::"+cgroup+"\n")
	}
	if listenPort > 0 {
		f.inode++
		f.tcp = append(f.tcp, fmt.Sprintf("   0: 0100007F:%04X 00000000:0000 0A 00000000:00000000 00:00000000 00000000  %d        0 %d 1 0000000000000000 100 0 0 10 0", listenPort, uid, f.inode))
		f.flushTCP()
		if err := os.Symlink(fmt.Sprintf("socket:[%d]", f.inode), filepath.Join(d, "fd", "3")); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *fakeProc) host(home string) *doctorHost {
	return &doctorHost{procRoot: f.root, goos: "linux", home: home, uid: 1000}
}

// writeGrantLedger writes identity.grant.* events for the given
// (surface, token, event type) rows into <root>/.cog/ledger/identity-grants.
func writeGrantLedger(t *testing.T, root string, rows ...[3]string) {
	t.Helper()
	dir := filepath.Join(root, ".cog", "ledger", identityGrantsLedgerSession)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	exp := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	for i, r := range rows {
		env := map[string]any{"hashed_payload": map[string]any{
			"type":       r[2],
			"session_id": identityGrantsLedgerSession,
			"data": map[string]any{
				"grant_id": fmt.Sprintf("g%d-%s", i, r[0]), "surface": r[0],
				"integrity_hash": sha256Hex(r[1]),
				"issued_at":      time.Now().UTC().Format(time.RFC3339), "expires_at": exp,
				"scope": []string{"all"},
			},
		}}
		line, _ := json.Marshal(env)
		b.Write(line)
		b.WriteString("\n")
	}
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeFileMode(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func groupCheck(g *DoctorGroup, prefix string) *DoctorCheck {
	for i := range g.Checks {
		if strings.HasPrefix(g.Checks[i].Name, prefix) {
			return &g.Checks[i]
		}
	}
	return nil
}

func allDetail(g *DoctorGroup) string {
	var b strings.Builder
	for _, c := range g.Checks {
		b.WriteString(c.Name + ": " + string(c.Status) + ": " + c.Detail + "\n")
	}
	return b.String()
}

// secretCanary must never appear in any doctor output.
const secretCanary = "canary-secret-grant-value-0123456789"
