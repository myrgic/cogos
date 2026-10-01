// ingest_watermark_test.go — delta ingest: an append is read as a delta, not a
// re-parse of the source; the result equals a full re-parse; the fallbacks
// (no watermark, rewritten file, mapping change) still produce the full answer.
package conversations

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

var timeZero time.Time

func reconcileActionForTest(source string, files []string) reconcile.Action {
	return reconcile.Action{
		Action:       reconcile.ActionUpdate,
		ResourceType: "conversations",
		Name:         source,
		Details:      map[string]any{"is_ingest": true, "ingest_files": files},
	}
}

func deltaRec(sess, id, text string) string {
	return makeIngestRecord("hermes-x", sess, "user", text, "2026-10-01T10:00:00Z",
		map[string]any{"refs": map[string]any{"stable_id": "hermes-x:" + id}})
}

func appendLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(strings.Join(lines, "\n") + "\n"); err != nil {
		t.Fatal(err)
	}
}

// deltaWorkspace builds a workspace with one ingest source of nFiles files,
// each holding 3 sessions x 4 turns, plus a fresh provider.
func deltaWorkspace(t *testing.T, nFiles int) (root, ingest string, files []string) {
	t.Helper()
	root = t.TempDir()
	ingest = filepath.Join(root, ".cog", "observatory", "ingest")
	for f := 0; f < nFiles; f++ {
		var lines []string
		for s := 0; s < 3; s++ {
			for k := 0; k < 4; k++ {
				id := string(rune('a'+f)) + "-" + string(rune('0'+s)) + "-" + string(rune('0'+k))
				lines = append(lines, deltaRec("s"+string(rune('0'+s))+"-f"+string(rune('a'+f)), id, "turn "+id))
			}
		}
		files = append(files, writeIngestDir(t, ingest, "hermes-x", "2026100"+string(rune('0'+f)), lines))
	}
	writeObservatoryConfigFull(t, root, nil, []string{ingest})
	return root, ingest, files
}

// indexSnapshot returns session -> turn texts in order, for every session.
func indexSnapshot(p *Provider) map[string][]string {
	out := map[string][]string{}
	for _, m := range p.index.ListSessions(timeZero, timeZero, "") {
		var texts []string
		for _, tr := range p.index.SessionTurns(m.SessionID) {
			texts = append(texts, tr.UUID+"="+tr.Text)
		}
		out[m.SessionID] = texts
	}
	return out
}

func TestDeltaIngest_AppendReadsOnlyTheDelta(t *testing.T) {
	root, _, files := deltaWorkspace(t, 4)
	p := NewProvider()
	reconcileOnce(t, p, root) // first cycle: full parse, writes the watermark

	if _, err := os.Stat(watermarkPath(root)); err != nil {
		t.Fatalf("no watermark after first cycle: %v", err)
	}
	before := map[string]int64{}
	for sid := range indexSnapshot(p) {
		fi, _ := os.Stat(p.index.turnsPath(sid))
		before[sid] = fi.ModTime().UnixNano()
	}

	// One new turn in an existing session, in the newest file.
	appendLines(t, files[3], deltaRec("s1-fd", "new-1", "the new turn"))

	cfg, _ := p.LoadConfig(root)
	live, _ := p.FetchLive(context.Background(), cfg)
	plan, _ := p.ComputePlan(cfg, live, nil)
	if plan.Summary.Updates != 1 {
		t.Fatalf("updates=%d want 1", plan.Summary.Updates)
	}
	var action = plan.Actions[0]
	for _, a := range plan.Actions {
		if a.Name == "hermes-x" {
			action = a
		}
	}
	p.mu.Lock()
	wm := p.watermarks["hermes-x"]
	p.mu.Unlock()
	delta, err := applyIngestSourceDelta(p.index, action, p.ontology, nil, wm)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	line := deltaRec("s1-fd", "new-1", "the new turn") + "\n"
	if delta.FilesRead != 1 || delta.BytesRead != int64(len(line)) {
		t.Fatalf("read %d files / %d bytes, want 1 file / %d bytes (only the appended line)", delta.FilesRead, delta.BytesRead, len(line))
	}
	if delta.SessionsTouch != 1 || delta.TurnsAdded != 1 {
		t.Fatalf("touched %d sessions / added %d turns, want 1 / 1", delta.SessionsTouch, delta.TurnsAdded)
	}

	// Through the provider: only that session's turns file is rewritten.
	p.mu.Lock()
	p.watermarks["hermes-x"] = wm // apply again via ApplyPlan from the same mark
	p.mu.Unlock()
	if _, err := p.ApplyPlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	after := indexSnapshot(p)
	got := after["hermes-x/s1-fd"]
	if len(got) != 5 || got[4] != "hermes-x:new-1=the new turn" {
		t.Fatalf("s1-fd turns = %v", got)
	}
	changed := 0
	for sid, mt := range before {
		fi, _ := os.Stat(p.index.turnsPath(sid))
		if fi.ModTime().UnixNano() != mt {
			changed++
			if sid != "hermes-x/s1-fd" {
				t.Errorf("session %s rewritten, only hermes-x/s1-fd gained a turn", sid)
			}
		}
	}
	if changed != 1 {
		t.Fatalf("%d turns files rewritten, want 1", changed)
	}

	// Next cycle: in sync, nothing to do.
	cfg, _ = p.LoadConfig(root)
	live, _ = p.FetchLive(context.Background(), cfg)
	plan, _ = p.ComputePlan(cfg, live, nil)
	if plan.Summary.Updates != 0 || plan.Summary.Creates != 0 {
		t.Fatalf("after delta, plan = %+v, want all skip", plan.Summary)
	}
}

func TestDeltaIngest_EqualsFullReparse(t *testing.T) {
	root, ingest, files := deltaWorkspace(t, 3)
	p := NewProvider()
	reconcileOnce(t, p, root)

	// Appends across the cycle: a new turn in an old session, a re-emitted
	// (duplicate) record, a brand-new session, a new file, and a partial line.
	appendLines(t, files[0], deltaRec("s0-fa", "late", "late turn"), deltaRec("s2-fb", "b-2-0", "turn b-2-0"))
	reconcileOnce(t, p, root)
	writeIngestDir(t, ingest, "hermes-x", "20261009", []string{
		deltaRec("brand-new", "n1", "first"), deltaRec("s0-fc", "c-tail", "tail of c"),
	})
	f, _ := os.OpenFile(files[2], os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(deltaRec("s1-fc", "whole", "whole line") + "\n" + `{"schema":"cogos.observatory.conversations/v0.1","sou`)
	f.Close()
	reconcileOnce(t, p, root)
	// Finish the partial line; the delta must pick it up from the right offset.
	f, _ = os.OpenFile(files[2], os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`rce":"hermes-x","session_id":"s1-fc","role":"user","timestamp":"2026-10-01T10:00:00Z","text":"finished","refs":{"stable_id":"hermes-x:fin"}}` + "\n")
	f.Close()
	reconcileOnce(t, p, root)
	deltaView := indexSnapshot(p)

	// Reference: a fresh workspace view, full parse, no watermark.
	os.Remove(watermarkPath(root))
	os.RemoveAll(filepath.Join(root, ".cog", "state", "conversations"))
	q := NewProvider()
	reconcileOnce(t, q, root)
	fullView := indexSnapshot(q)

	if !reflect.DeepEqual(deltaView, fullView) {
		keys := func(m map[string][]string) []string {
			var k []string
			for s := range m {
				k = append(k, s)
			}
			sort.Strings(k)
			return k
		}
		t.Fatalf("delta index != full re-parse\ndelta sessions %v\nfull sessions  %v\ns1-fc delta %v\ns1-fc full  %v",
			keys(deltaView), keys(fullView), deltaView["hermes-x/s1-fc"], fullView["hermes-x/s1-fc"])
	}
	if got := deltaView["hermes-x/s1-fc"]; got[len(got)-1] != "hermes-x:fin=finished" {
		t.Fatalf("partial line not completed: %v", got)
	}
}

func TestDeltaIngest_RewrittenFileFallsBackToFull(t *testing.T) {
	root, _, files := deltaWorkspace(t, 2)
	p := NewProvider()
	reconcileOnce(t, p, root)

	// Rewrite (shrink) a consumed file: the watermark cannot be trusted.
	if err := os.WriteFile(files[0], []byte(deltaRec("s0-fa", "only", "only turn")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, _ := p.LoadConfig(root)
	live, _ := p.FetchLive(context.Background(), cfg)
	plan, _ := p.ComputePlan(cfg, live, nil)
	var action = plan.Actions[0]
	for _, a := range plan.Actions {
		if a.Name == "hermes-x" {
			action = a
		}
	}
	p.mu.Lock()
	wm := p.watermarks["hermes-x"]
	p.mu.Unlock()
	if _, err := applyIngestSourceDelta(p.index, action, p.ontology, nil, wm); err != errNeedFull {
		t.Fatalf("delta on a shrunk file: err=%v, want errNeedFull", err)
	}
	reconcileOnce(t, p, root)
	if got := indexSnapshot(p)["hermes-x/s0-fa"]; len(got) != 1 || got[0] != "hermes-x:only=only turn" {
		t.Fatalf("after rewrite, s0-fa = %v, want the rewritten content only", got)
	}
}

func TestDeltaIngest_MappingChangeFallsBackToFull(t *testing.T) {
	wm := &sourceWatermark{Fingerprint: "old", Files: map[string]fileMark{}}
	var lo *LoadedOntology
	action := reconcileActionForTest("hermes-x", nil)
	if _, err := applyIngestSourceDelta(nil, action, lo, nil, wm); err != errNeedFull {
		t.Fatalf("fingerprint mismatch: err=%v, want errNeedFull", err)
	}
}

func TestDeltaIngest_ColdProcessSkipDoesNotReparse(t *testing.T) {
	root, _, files := deltaWorkspace(t, 2)
	p := NewProvider()
	reconcileOnce(t, p, root)
	want := p.Coverage()["hermes-x"]

	// A brand-new process (the CLI case): coverage must come from the
	// watermark. Make the files unreadable to prove nothing re-parses them.
	for _, f := range files {
		os.Chmod(f, 0o000)
	}
	defer func() {
		for _, f := range files {
			os.Chmod(f, 0o644)
		}
	}()
	q := NewProvider()
	cfg, err := q.LoadConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	live, _ := q.FetchLive(context.Background(), cfg)
	plan, _ := q.ComputePlan(cfg, live, nil)
	if plan.Summary.Updates != 0 || plan.Summary.Creates != 0 {
		t.Fatalf("plan = %+v, want all skip", plan.Summary)
	}
	if _, err := q.ApplyPlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if errs := q.lastErrors; len(errs) != 0 {
		t.Fatalf("cold skip touched the files: %v", errs)
	}
	got := q.Coverage()["hermes-x"]
	if got.Mapped != want.Mapped || got.Quarantined != want.Quarantined || got.Degenerate != want.Degenerate {
		t.Fatalf("cold coverage %+v, want %+v", got, want)
	}
}
