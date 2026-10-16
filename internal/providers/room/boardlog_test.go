package room

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
)

func TestBoardLogRoundTripAndGolden(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "hi")
	raw, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseEntry(raw)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := l.ID()
	b, _ := back.ID()
	if a != b {
		t.Fatalf("id changed over the wire: %s vs %s", a, b)
	}
	if _, err := ParseEntry([]byte(strings.Replace(string(raw), `"kind"`, `"extra":1,"kind"`, 1))); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := ParseEntry([]byte(strings.Replace(string(raw), `"seq":2`, `"seq":2,"seq":3`, 1))); err == nil {
		t.Fatal("duplicate key accepted")
	}

	// A fixed entry has fixed canonical bytes (and so a fixed id): this is the
	// wire contract the Python and Go sides must agree on.
	prev := strings.Repeat("a", 64)
	g := &Entry{
		Room: strings.Repeat("b", 64), Log: "LOG", Seq: 2, Prev: &prev,
		Deps:  map[string]Dep{"LOG": {Seq: 1, ID: strings.Repeat("c", 64)}},
		Spec:  strings.Repeat("d", 64),
		Actor: Actor{Person: "vroku"}, Kind: KindLine,
		Body: map[string]any{"to": []any{}, "text": "x<y&z"},
		HLC:  "2026-10-15T18:02:11.204Z/3", Sig: strings.Repeat("0", 128),
	}
	got, err := g.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"actor":{"person":"vroku"},"body":{"text":"x<y&z","to":[]},"deps":{"LOG":{"id":"` +
		strings.Repeat("c", 64) + `","seq":1}},"hlc":"2026-10-15T18:02:11.204Z/3","kind":"line","log":"LOG","prev":"` +
		strings.Repeat("a", 64) + `","room":"` + strings.Repeat("b", 64) + `","seq":2,"spec":"` + strings.Repeat("d", 64) + `"}`
	if string(got) != want {
		t.Fatalf("canonical form changed:\n got %s\nwant %s", got, want)
	}
}

func TestBoardLogSignatures(t *testing.T) {
	w := newWorld(t, nil)
	good := w.line("node-a", chaz, "good")
	forged := w.line("node-a", chaz, "forged")
	forged.Body["text"] = "changed after signing"

	// Bad signature.
	o := w.eval(w.g, good, forged)
	w.accepted(o, good)
	v := o.Verdicts[2]
	if v.Status != StatusRejected || v.Code != CodeSigBad {
		t.Fatalf("forged: %+v", v)
	}

	// High-S twin: rejected as non-canonical; the low-S original is accepted,
	// and re-signing the same body (a different low-S signature) is a
	// duplicate, never equivocation.
	hs := flipS(good)
	resigned := *good
	if err := resigned.Sign(testNodeSet["node-a"].key); err != nil {
		t.Fatal(err)
	}
	if resigned.Sig == good.Sig {
		t.Fatal("expected a fresh signature")
	}
	o = w.eval(w.g, good, hs, &resigned)
	gid, _ := good.ID()
	if v, _ := o.Verdict(gid); v.Status != StatusAccepted {
		t.Fatalf("original: %+v", v)
	}
	if v := o.Verdicts[2]; v.Status != StatusRejected || v.Code != CodeSigNotLowS {
		t.Fatalf("high-S twin: %+v", v)
	}
	// Whichever low-S copy sorts first is the primary; the other is a duplicate.
	st1, st3 := o.Verdicts[1].Status, o.Verdicts[3].Status
	if !(st1 == StatusAccepted && st3 == StatusDuplicate) && !(st1 == StatusDuplicate && st3 == StatusAccepted) {
		t.Fatalf("low-S twins: %s and %s", st1, st3)
	}
	for _, ev := range o.Events {
		if ev.Type == EventLogEquivocation {
			t.Fatal("a signature twin is not equivocation")
		}
	}
	// Only the high-S version in hand: not stored, nothing accepted for the id.
	o = w.eval(w.g, hs)
	if v := o.Verdicts[1]; v.Status != StatusRejected || v.Code != CodeSigNotLowS {
		t.Fatalf("lone high-S: %+v", v)
	}

	// The id does not cover the signature.
	a, _ := good.ID()
	b, _ := hs.ID()
	if a != b {
		t.Fatal("entry id must exclude sig")
	}

	// Unknown certificate: held, not rejected.
	p := w.params()
	p.Certs = nil
	o = Evaluate(p, []*Entry{w.g, good})
	if o.Verdicts[0].Status != StatusHeld || o.Verdicts[0].Code != CodeCertMissing {
		t.Fatalf("no cert: %+v", o.Verdicts[0])
	}
}

func TestBoardLogNonCanonicalLog(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "x")
	lower := *l
	lower.Log = strings.ToLower(l.Log)
	if err := lower.Sign(testNodeSet["node-a"].key); err != nil {
		t.Fatal(err)
	}
	nodash := *l
	nodash.Log = strings.ReplaceAll(l.Log, "-", "")
	if err := nodash.Sign(testNodeSet["node-a"].key); err != nil {
		t.Fatal(err)
	}
	o := w.eval(w.g, &lower, &nodash)
	for _, i := range []int{1, 2} {
		if v := o.Verdicts[i]; v.Status != StatusRejected || v.Code != CodeLogNonCanon {
			t.Fatalf("variant %d: %+v", i, v)
		}
	}
}

func TestBoardLogSeqGap(t *testing.T) {
	w := newWorld(t, nil)
	e2 := w.line("node-a", chaz, "two")
	e3 := w.line("node-a", chaz, "three")
	e4 := w.line("node-a", chaz, "four")

	o := w.eval(w.g, e3, e4)
	w.expect(o, e3, StatusHeld, CodeSeqGap)
	w.expect(o, e4, StatusHeld, CodeSeqGap)

	o = w.eval(w.g, e2, e3, e4)
	w.accepted(o, e2)
	w.accepted(o, e3)
	w.accepted(o, e4)

	// A wrong prev at an existing predecessor is a rejection, not a hold.
	bad := w.mk("node-a", KindLine, chaz, map[string]any{"text": "x"}, func(e *Entry) {
		p := strings.Repeat("0", 64)
		e.Prev = &p
	})
	o = w.eval(w.g, e2, e3, e4, bad)
	w.rejected(o, bad, CodePrevBad)
}

func TestBoardLogDeps(t *testing.T) {
	w := newWorld(t, nil)
	x := w.line("node-b", vroku, "from node-b")
	w.sync()
	d1 := w.line("node-a", chaz, "saw it")
	// d2 drops node-b from its deps: deps regress along node-a's log.
	d2 := w.mk("node-a", KindLine, chaz, map[string]any{"text": "regress"}, func(e *Entry) {
		delete(e.Deps, testNodeSet["node-b"].dev)
	})
	o := w.eval(w.g, x, d1, d2)
	w.accepted(o, x)
	w.accepted(o, d1)
	w.rejected(o, d2, CodeDepsRegress)

	// A dep that cites an id not held waits.
	o = w.eval(w.g, d1)
	w.expect(o, d1, StatusHeld, CodeDepsUnheld)

	// A dep that names a held seq with a different id is bad.
	w2 := newWorld(t, nil)
	y := w2.line("node-b", vroku, "y")
	w2.sync()
	z := w2.mk("node-a", KindLine, chaz, map[string]any{"text": "z"}, func(e *Entry) {
		k := testNodeSet["node-b"].dev
		e.Deps[k] = Dep{Seq: e.Deps[k].Seq, ID: strings.Repeat("9", 64)}
	})
	o = w2.eval(w2.g, y, z)
	w2.rejected(o, z, CodeDepsBad)

	// A dep may not name the author's own future.
	w3 := newWorld(t, nil)
	f := w3.mk("node-a", KindLine, chaz, map[string]any{"text": "f"}, func(e *Entry) {
		e.Deps[e.Log] = Dep{Seq: e.Seq, ID: strings.Repeat("1", 64)}
	})
	o = w3.eval(w3.g, f)
	w3.rejected(o, f, CodeDepsBad)
}

func TestBoardLogEquivocation(t *testing.T) {
	w := newWorld(t, nil)
	a := w.line("node-a", chaz, "one")
	next := w.line("node-a", chaz, "after")
	// A second entry at a's (log, seq) with a different body, validly signed.
	b := *a
	b.Body = map[string]any{"text": "two", "to": []any{}}
	if err := b.Sign(testNodeSet["node-a"].key); err != nil {
		t.Fatal(err)
	}

	for name, set := range map[string][]*Entry{
		"ab": {w.g, a, &b, next}, "ba": {next, &b, a, w.g},
	} {
		o := w.eval(set...)
		ida, _ := a.ID()
		idb, _ := b.ID()
		idn, _ := next.ID()
		for _, id := range []string{ida, idb, idn} {
			v, _ := o.Verdict(id)
			if v.Status != StatusRejected || v.Code != CodeEquivocation {
				t.Fatalf("%s: %s: %+v", name, id, v)
			}
		}
		var found *Event
		for i := range o.Events {
			if o.Events[i].Type == EventLogEquivocation {
				found = &o.Events[i]
			}
		}
		if found == nil || found.Room != w.room || found.Actor == "" {
			t.Fatalf("%s: equivocation event missing room/actor: %+v", name, found)
		}
	}
	// The genesis log is not touched on the other node: node-b still works.
	w.sync()
}

// The outcome is a function of the set, not of arrival order.
func TestBoardLogOrderIndependent(t *testing.T) {
	w := newWorld(t, nil)
	_, item := w.createItem("node-a", chaz, nil)
	w.sync()
	w.mk("node-b", KindItemSet, vroku, map[string]any{"item": item, "field": "state", "value": "done"}) // denied
	w.mk("node-b", KindItemSet, vroku, map[string]any{"item": item, "field": "title", "value": "t"})
	w.line("node-b", guest, "hello")
	w.sync()
	w.line("node-a", chaz, "a")
	w.mk("node-a", KindApprovalAnswer, chaz, map[string]any{"request": "r", "session": "s"})
	w.mk("node-a", KindItemSet, vroku, map[string]any{"item": item, "field": "x", "value": 1}) // not homed
	// Review cases: a rejected spec in the past, a forged agent reply feeding
	// the default claimant, and a floor claim over both.
	forgedSpec := defaultGenesisBody()
	forgedSpec["gen"] = 99
	delete(forgedSpec, "name")
	w.mk("node-b", KindSpec, guest, forgedSpec)
	root := w.line("node-a", chaz, "who answers")
	w.mk("node-a", KindLine, sage, map[string]any{"text": "r", "to": []any{}, "in_reply_to": idOfEntry(root)})
	w.sync()
	w.mk("node-a", KindFloorClaim, cog, map[string]any{"line": idOfEntry(root), "default": true, "fallback": false, "name_hit": false})
	w.mk("node-b", KindFloorClaim, sage, map[string]any{"line": idOfEntry(root), "default": true, "fallback": false, "name_hit": false})

	base := w.eval()
	want := verdictMap(base)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 25; i++ {
		perm := append([]*Entry(nil), w.all...)
		rng.Shuffle(len(perm), func(a, b int) { perm[a], perm[b] = perm[b], perm[a] })
		got := verdictMap(w.eval(perm...))
		for id, v := range want {
			if got[id] != v {
				t.Fatalf("perm %d: %s: got %s want %s", i, id, got[id], v)
			}
		}
	}
	// Incremental replica agrees with the batch.
	r := NewReplica(w.params())
	for _, e := range w.all {
		r.Add(e)
	}
	got := verdictMap(r.Outcome())
	for id, v := range want {
		if got[id] != v {
			t.Fatalf("replica: %s: got %s want %s", id, got[id], v)
		}
	}
}

func verdictMap(o *Outcome) map[string]string {
	m := map[string]string{}
	for _, v := range o.Verdicts {
		if v.Status == StatusDuplicate {
			continue
		}
		m[v.ID] = v.Status.String() + "/" + v.Code
	}
	return m
}

func TestBoardLogEventsCarryRoomAndActor(t *testing.T) {
	w := newWorld(t, nil)
	bad := w.mk("node-b", KindLine, chaz, map[string]any{"text": "chaz from node-b"})
	o := w.eval(w.g, bad)
	var n int
	for _, ev := range o.Events {
		if ev.Type != EventEntryRejected {
			continue
		}
		n++
		if ev.Room != w.room || ev.Actor != "person:chaz" || ev.Data["code"] != CodeActorNotHomed {
			t.Fatalf("event: %+v", ev)
		}
	}
	if n != 1 {
		t.Fatalf("want 1 rejection event, got %d", n)
	}
}
