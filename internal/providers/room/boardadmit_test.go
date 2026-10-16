package room

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/myrgic/cogos/pkg/substrate/bep"
)

func TestBoardAdmitBaseline(t *testing.T) {
	w := newWorld(t, nil)
	_, item := w.createItem("node-a", chaz, nil)
	w.sync()
	e1 := w.line("node-b", vroku, "hello")
	e2 := w.line("node-b", guest, "hi")
	e3 := w.mk("node-b", KindItemSet, vroku, map[string]any{"item": item, "field": "title", "value": "t"})
	e4 := w.mk("node-a", KindItemSet, chaz, map[string]any{"item": item, "field": "state", "value": "planned"})
	o := w.eval()
	for _, e := range []*Entry{w.g, e1, e2, e3, e4} {
		w.accepted(o, e)
	}
	if len(o.Chain) != 1 || o.Chain[0].Gen != 1 {
		t.Fatalf("chain: %+v", o.Chain)
	}
	if o.ItemAuthors[item] != "person:chaz" {
		t.Fatalf("derived author: %v", o.ItemAuthors)
	}
}

func TestBoardAdmitMembershipAndHomes(t *testing.T) {
	w := newWorld(t, nil)
	rogue := w.app("rogue")
	_ = rogue
	notMember := w.mk("node-b", KindLine, Actor{Person: "mallory"}, map[string]any{"text": "x"})
	// A person written by a node outside their homes.
	wrongNode := w.mk("node-a", KindLine, vroku, map[string]any{"text": "x"})
	// An agent written by a node other than its own.
	agentWrong := w.mk("node-b", KindLine, cog, map[string]any{"text": "x"})
	// A signer not in spec.nodes at all.
	offNodes := w.mk("rogue", KindLine, chaz, map[string]any{"text": "x"})
	// An agent named as a person, and a person named as an agent.
	typeMix := w.mk("node-b", KindLine, Actor{Agent: "vroku"}, map[string]any{"text": "x"})
	o := w.eval()
	w.rejected(o, notMember, CodeNotAMember)
	w.rejected(o, wrongNode, CodeActorNotHomed)
	w.rejected(o, agentWrong, CodeActorNotHomed)
	w.rejected(o, offNodes, CodeLogNotInNodes)
	w.rejected(o, typeMix, CodeNotAMember)
}

func TestBoardAdmitGenesis(t *testing.T) {
	// A trusted peer with no person field authors an owner genesis: refused.
	w := newWorld(t, nil)
	w.binding = StubBinding{} // node-a is enrolled but binds to no one
	l := w.line("node-a", chaz, "x")
	o := w.eval()
	w.rejected(o, w.g, CodeGenesisOwnerUnbond)
	w.rejected(o, l, CodeGenesisRejected)

	// Bound to the wrong node.
	w.binding = StubBinding{"chaz": {testNodeSet["node-b"].dev}}
	w.rejected(w.eval(), w.g, CodeGenesisOwnerUnbond)

	// No binding at all fails closed.
	p := w.params()
	p.Binding = nil
	o = Evaluate(p, w.all)
	w.rejected(o, w.g, CodeGenesisOwnerUnbond)

	// A genesis owned by chaz from node-b (not chaz's home) is refused even
	// if node-b were bound.
	w2 := newWorld(t, func(b map[string]any) {
		b["members"].(map[string]any)["chaz"] = map[string]any{"type": "person", "role": "owner", "homes": []any{"node-b"}}
	})
	w2.binding = StubBinding{"chaz": {testNodeSet["node-a"].dev}}
	// node-a authors; chaz's home is node-b -> not homed
	w2.rejected(w2.eval(), w2.g, CodeGenesisOwnerUnbond)
}

func TestBoardAdmitSecondGenesisSameNameIsAnotherRoom(t *testing.T) {
	a := newWorld(t, nil)
	b := newWorld(t, func(body map[string]any) { body["dispatch"] = map[string]any{"node": "node-b"} })
	if !LabelClash(a.g, b.g) {
		t.Fatal("want a label clash")
	}
	la := a.line("node-a", chaz, "in a")
	lb := b.line("node-a", chaz, "in b")
	// Replica of room a holds both worlds' entries: b's are another room.
	pa := a.params()
	o := Evaluate(pa, []*Entry{a.g, b.g, la, lb})
	a.accepted(o, la)
	if v := o.Verdicts[1]; v.Status != StatusRejected || v.Code != CodeRoomMismatch {
		t.Fatalf("other genesis: %+v", v)
	}
	if v := o.Verdicts[3]; v.Status != StatusRejected || v.Code != CodeRoomMismatch {
		t.Fatalf("other room's entry: %+v", v)
	}
	pb := b.params()
	ob := Evaluate(pb, []*Entry{a.g, b.g, la, lb})
	b.accepted(ob, lb)
	b.accepted(ob, b.g)
}

func TestBoardAdmitRolesTable(t *testing.T) {
	w := newWorld(t, nil)
	_, item := w.createItem("node-a", chaz, nil)
	w.sync()
	// floor.claim by a person.
	l := w.line("node-a", chaz, "who is there")
	w.sync()
	claimByPerson := w.mk("node-b", KindFloorClaim, vroku, map[string]any{
		"line": idOfEntry(l), "default": false, "fallback": false, "name_hit": false})
	// approval.answer by an agent, by a collaborator, by the owner.
	ansAgent := w.mk("node-a", KindApprovalAnswer, cog, map[string]any{"request": "r", "session": "s"})
	ansCollab := w.mk("node-b", KindApprovalAnswer, vroku, map[string]any{"request": "r", "session": "s"})
	ansGuest := w.mk("node-b", KindApprovalAnswer, guest, map[string]any{"request": "r", "session": "s"})
	ansOwner := w.mk("node-a", KindApprovalAnswer, chaz, map[string]any{"request": "r", "session": "s"})
	// pipeline.request.
	pipeUnlisted := w.mk("node-b", KindPipelineRequest, vroku, map[string]any{"pipeline": "deploy"})
	pipeCollab := w.mk("node-b", KindPipelineRequest, vroku, map[string]any{"pipeline": "review"})
	pipeSage := w.mk("node-b", KindPipelineRequest, sage, map[string]any{"pipeline": "review"})
	pipeCog := w.mk("node-a", KindPipelineRequest, cog, map[string]any{"pipeline": "review"}) // full class, not collab
	pipeGuest := w.mk("node-b", KindPipelineRequest, guest, map[string]any{"pipeline": "review"})
	// work.request.
	workGuest := w.mk("node-b", KindWorkRequest, guest, map[string]any{"text": "x"})
	workCog := w.mk("node-a", KindWorkRequest, cog, map[string]any{"text": "x"})
	// spec and genesis kinds.
	specByCollab := w.mk("node-b", KindSpec, vroku, map[string]any{"gen": 2})
	genesisAgain := w.mk("node-a", KindGenesis, chaz, map[string]any{"gen": 1})

	o := w.eval()
	w.rejected(o, claimByPerson, CodeRoleDenied)
	w.rejected(o, ansAgent, CodeRoleDenied)
	w.rejected(o, ansCollab, CodeRoleDenied)
	w.rejected(o, ansGuest, CodeRoleDenied)
	w.accepted(o, ansOwner)
	w.rejected(o, pipeUnlisted, CodePipelineUnlisted)
	w.accepted(o, pipeCollab)
	w.accepted(o, pipeSage)
	w.rejected(o, pipeCog, CodeRoleDenied)
	w.rejected(o, pipeGuest, CodeRoleDenied)
	w.rejected(o, workGuest, CodeRoleDenied)
	w.accepted(o, workCog)
	w.rejected(o, specByCollab, CodeRoleDenied)
	w.rejected(o, genesisAgain, CodeMalformed)
	_ = item
}

func idOfEntry(e *Entry) string { id, _ := e.ID(); return id }

func TestBoardAdmitArchiveClosure(t *testing.T) {
	w := newWorld(t, nil)
	_, i1 := w.createItem("node-a", chaz, nil)
	_, i2 := w.createItem("node-a", chaz, nil)
	_, i3 := w.createItem("node-a", chaz, nil)
	_, i4 := w.createItem("node-a", chaz, nil)
	w.sync()
	none := w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": i1})
	killedNoReason := w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": i2, "closure": "killed"})
	killed := w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": i2, "closure": "killed", "reason": "superseded"})
	handedNoNext := w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": i3, "closure": "handed-off", "owner": "cog"})
	handed := w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": i3, "closure": "handed-off", "owner": "cog", "next": "ship"})
	done := w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": i4, "closure": "done"})
	bogus := w.mk("node-a", KindItemArchive, chaz, map[string]any{"item": i1, "closure": "finished"})
	guestArch := w.mk("node-b", KindItemArchive, guest, map[string]any{"item": i1, "closure": "done"})
	collabOthers := w.mk("node-b", KindItemArchive, vroku, map[string]any{"item": i1, "closure": "done"})
	o := w.eval()
	w.rejected(o, none, CodeClosureMissing)
	w.rejected(o, killedNoReason, CodeClosureMissing)
	w.accepted(o, killed)
	w.rejected(o, handedNoNext, CodeClosureMissing)
	w.accepted(o, handed)
	w.accepted(o, done)
	w.rejected(o, bogus, CodeClosureMissing)
	w.rejected(o, guestArch, CodeRoleDenied)
	w.rejected(o, collabOthers, CodeRoleDenied)
}

// Effect-bearing fields: the workers RFC turns these into effects, so only
// the owner, or the author of the item, may set them.
func TestBoardAdmitEffectFields(t *testing.T) {
	w := newWorld(t, nil)
	_, ownerItem := w.createItem("node-a", chaz, map[string]any{"state": "killed", "brief": "b"})
	_, vrokuItem := w.createItem("node-b", vroku, nil)
	w.sync()
	set := func(node string, a Actor, item, field string, v any) *Entry {
		return w.mk(node, KindItemSet, a, map[string]any{"item": item, "field": field, "value": v})
	}
	guestState := set("node-b", guest, ownerItem, "state", "planned")
	guestBrief := set("node-b", guest, ownerItem, "brief", "steal")
	var guestRest []*Entry
	for _, f := range []string{"owner", "workflow", "model", "worktree", "section", "text", "executor"} {
		guestRest = append(guestRest, set("node-b", guest, ownerItem, f, "x"))
	}
	guestPlain := set("node-b", guest, ownerItem, "title", "fine")
	collabOthers := set("node-b", vroku, ownerItem, "state", "planned")
	collabOwn := set("node-b", vroku, vrokuItem, "state", "planned")
	ownerSet := set("node-a", chaz, vrokuItem, "state", "done")
	agentOwn := set("node-b", sage, vrokuItem, "title", "x")
	agentOther := set("node-b", sage, ownerItem, "brief", "x")
	authorSet := set("node-b", vroku, ownerItem, "author", "vroku")
	afterAuthor := set("node-b", vroku, ownerItem, "state", "planned")
	unknown := set("node-a", chaz, "nope-1", "state", "x")

	o := w.eval()
	w.rejected(o, guestState, CodeEffectFieldDenied)
	w.rejected(o, guestBrief, CodeEffectFieldDenied)
	for _, e := range guestRest {
		w.rejected(o, e, CodeEffectFieldDenied)
	}
	w.accepted(o, guestPlain)
	w.rejected(o, collabOthers, CodeEffectFieldDenied)
	w.accepted(o, collabOwn)
	w.accepted(o, ownerSet)
	w.accepted(o, agentOwn)
	w.rejected(o, agentOther, CodeEffectFieldDenied)
	w.rejected(o, authorSet, CodeAuthorNotSettable)
	w.rejected(o, afterAuthor, CodeEffectFieldDenied)
	w.rejected(o, unknown, CodeItemUnknown)

	// Stub reconciler: it dispatches a planned, briefed item only when the
	// last editor of every effect field is trusted. Guest edits are rejected
	// entries, so the owner's killed item is neither revived nor steered.
	if got := stubDispatch(w, o); len(got) != 0 {
		t.Fatalf("stub dispatched %v", got)
	}
}

// stubDispatch folds the accepted item.set entries of every item and reports
// the items the workers reconciler would start: state planned, brief set, and
// every effect field last edited by a trusted actor.
func stubDispatch(w *world, o *Outcome) []string {
	type last struct {
		val any
		by  Actor
		log string
	}
	fields := map[string]map[string]last{}
	authors := map[string]Actor{}
	for _, e := range w.all {
		id, _ := e.ID()
		v, ok := o.Verdict(id)
		if !ok || v.Status != StatusAccepted {
			continue
		}
		switch e.Kind {
		case KindItemCreate:
			item := e.Body["id"].(string)
			authors[item] = e.Actor
			fields[item] = map[string]last{}
			for k, val := range e.Body {
				if IsEffectField(k) {
					fields[item][k] = last{val, e.Actor, e.Log}
				}
			}
		case KindItemSet:
			item := e.Body["item"].(string)
			f := e.Body["field"].(string)
			if IsEffectField(f) {
				fields[item][f] = last{e.Body["value"], e.Actor, e.Log}
			}
		}
	}
	var out []string
	spec := o.Chain[len(o.Chain)-1]
	for item, fs := range fields {
		if fs["state"].val != "planned" || fs["brief"].val == nil {
			continue
		}
		trusted := true
		for _, l := range fs {
			switch {
			case l.by.Person == spec.Owner:
			case l.by == authors[item]:
			case l.by.Node != "" && l.by.Node == spec.Dispatch.Node:
			default:
				trusted = false
			}
		}
		if trusted {
			out = append(out, item)
		}
	}
	return out
}

func TestBoardAdmitStubReconciler(t *testing.T) {
	w := newWorld(t, nil)
	_, item := w.createItem("node-a", chaz, map[string]any{"state": "killed", "brief": "do it"})
	w.sync()
	// The guest tries to revive it; the owner then really plans it; the
	// dispatch node records the executor and still counts as trusted.
	w.mk("node-b", KindItemSet, guest, map[string]any{"item": item, "field": "state", "value": "planned"})
	o := w.eval()
	if got := stubDispatch(w, o); len(got) != 0 {
		t.Fatalf("guest revived a killed item: %v", got)
	}
	w.sync()
	w.mk("node-a", KindItemSet, chaz, map[string]any{"item": item, "field": "state", "value": "planned"})
	w.sync()
	ex := w.mk("node-a", KindItemSet, Actor{Node: "node-a"}, map[string]any{"item": item, "field": "executor", "value": "run-1"})
	o = w.eval()
	w.accepted(o, ex)
	if got := stubDispatch(w, o); len(got) != 1 || got[0] != item {
		t.Fatalf("owner plan + dispatch-node executor must dispatch once: %v", got)
	}
}

func TestBoardAdmitItemIDs(t *testing.T) {
	w := newWorld(t, nil)
	_, ownerItem := w.createItem("node-a", chaz, nil)
	w.sync()
	clash := w.mk("node-b", KindItemCreate, vroku, map[string]any{"id": ownerItem})
	bad := w.mk("node-b", KindItemCreate, vroku, map[string]any{"id": "my-item"})
	missing := w.mk("node-b", KindItemCreate, vroku, map[string]any{})
	good, _ := w.createItem("node-b", vroku, nil)
	o := w.eval()
	w.rejected(o, clash, CodeItemIDClash)
	w.rejected(o, bad, CodeItemIDBad)
	w.rejected(o, missing, CodeBodyBad)
	w.accepted(o, good)
}

func TestBoardAdmitSpecChain(t *testing.T) {
	// The owner is homed on two nodes, so two specs of one gen can be
	// written concurrently.
	w := newWorld(t, func(b map[string]any) {
		b["members"].(map[string]any)["chaz"] = map[string]any{"type": "person", "role": "owner", "homes": []any{"node-a", "node-b"}}
	})
	specBody := func(mod func(m map[string]any)) map[string]any {
		b := defaultGenesisBody()
		b["members"].(map[string]any)["chaz"] = map[string]any{"type": "person", "role": "owner", "homes": []any{"node-a", "node-b"}}
		b["gen"] = 2
		delete(b, "name")
		mod(b)
		return b
	}
	old := w.line("node-b", vroku, "still gen 1")
	sa := w.mk("node-a", KindSpec, chaz, specBody(func(m map[string]any) { m["quota"] = map[string]any{"logBytes": 1 << 20} }))
	sb := w.mk("node-b", KindSpec, chaz, specBody(func(m map[string]any) { m["quota"] = map[string]any{"logBytes": 2 << 20} }))
	// Entries citing each.
	citeA := w.mk("node-a", KindLine, chaz, map[string]any{"text": "a"}, func(e *Entry) { e.Spec = idOfEntry(sa) })
	citeB := w.mk("node-b", KindLine, chaz, map[string]any{"text": "b"}, func(e *Entry) { e.Spec = idOfEntry(sb) })
	o := w.eval()
	w.accepted(o, old)
	ia, ib := idOfEntry(sa), idOfEntry(sb)
	winner, loser := sa, sb
	cw, cl := citeA, citeB
	if ib < ia {
		winner, loser = sb, sa
		cw, cl = citeB, citeA
	}
	w.accepted(o, winner)
	w.rejected(o, loser, CodeSpecConflict)
	w.accepted(o, cw)
	w.rejected(o, cl, CodeSpecLost)
	if len(o.Chain) != 2 || o.Chain[1].ID != idOfEntry(winner) {
		t.Fatalf("chain: %d", len(o.Chain))
	}

	// Same outcome from the other arrival order.
	rev := make([]*Entry, len(w.all))
	for i, e := range w.all {
		rev[len(w.all)-1-i] = e
	}
	o2 := w.eval(rev...)
	if verdictMap(o)[idOfEntry(winner)] != verdictMap(o2)[idOfEntry(winner)] ||
		verdictMap(o)[idOfEntry(loser)] != verdictMap(o2)[idOfEntry(loser)] {
		t.Fatal("spec conflict depends on arrival order")
	}
}

// ID0 is a tiny test convenience.
func (e *Entry) ID0(t *testing.T) string {
	id, err := e.ID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestBoardAdmitSpecStaleAndGen(t *testing.T) {
	w := newWorld(t, nil)
	next := defaultGenesisBody()
	next["gen"] = 2
	delete(next, "name")
	s2 := w.mk("node-a", KindSpec, chaz, next)
	w.sync()
	// Cites gen 1 although its deps cover the gen 2 spec entry.
	stale := w.mk("node-b", KindLine, vroku, map[string]any{"text": "x"}, func(e *Entry) { e.Spec = w.g.ID0(t) })
	// A spec entry with the wrong gen.
	skip := defaultGenesisBody()
	skip["gen"] = 5
	delete(skip, "name")
	bad := w.mk("node-a", KindSpec, chaz, skip, func(e *Entry) { e.Spec = idOfEntry(s2) })
	_ = bad
	o := w.eval()
	w.accepted(o, s2)
	w.rejected(o, stale, CodeSpecStale)
	w.rejected(o, bad, CodeSpecGenBad)

	// A cited spec this replica does not hold waits.
	o = w.eval(w.g, stale)
	w.expect(o, stale, StatusHeld, CodeDepsUnheld)
}

func TestBoardAdmitRemovalCutoffs(t *testing.T) {
	w := newWorld(t, nil)
	before := w.line("node-b", vroku, "last words")
	w.sync()
	rm := defaultGenesisBody()
	rm["gen"] = 2
	delete(rm, "name")
	delete(rm["members"].(map[string]any), "vroku")
	rm["cutoffs"] = map[string]any{"vroku": map[string]any{
		testNodeSet["node-a"].dev: 1,
		testNodeSet["node-b"].dev: before.Seq,
	}}
	removal := w.mk("node-a", KindSpec, chaz, rm)
	// vroku writes again on node-b, citing the old spec and omitting the
	// removal from deps: the node-b appender has not observed it.
	after := w.mk("node-b", KindLine, vroku, map[string]any{"text": "sneaky"})
	// Citing the new spec, he is simply not a member.
	w.sync()
	newer := w.mk("node-b", KindLine, vroku, map[string]any{"text": "again"}, func(e *Entry) { e.Spec = idOfEntry(removal) })
	o := w.eval()
	w.accepted(o, removal)
	v, _ := o.Verdict(idOfEntry(before))
	if v.Status != StatusAccepted || !v.BeforeRemoval {
		t.Fatalf("at or below the cutoff: %+v", v)
	}
	w.rejected(o, after, CodeAfterRemoval)
	w.rejected(o, newer, CodeNotAMember)

	// A removal without cutoffs is not a valid spec.
	w2 := newWorld(t, nil)
	rm2 := defaultGenesisBody()
	rm2["gen"] = 2
	delete(rm2, "name")
	delete(rm2["members"].(map[string]any), "vroku")
	bad := w2.mk("node-a", KindSpec, chaz, rm2)
	w2.rejected(w2.eval(), bad, CodeSpecBad)
}

func TestBoardAdmitSurfaceAttestationAndHeads(t *testing.T) {
	w := newWorld(t, nil)
	att := func(subject string) func(*Entry) {
		return func(e *Entry) { e.Attested = &Attested{Subject: subject, Surface: "discord"} }
	}
	ownerSpoken := w.mk("node-a", KindLine, chaz, map[string]any{"text": "x"}, att("chaz"))
	agentAtt := w.mk("node-b", KindLine, sage, map[string]any{"text": "x"}, att("sage@node-b"))
	guestAtt := w.mk("node-b", KindLine, guest, map[string]any{"text": "x"}, att("guest1"))
	mismatch := w.mk("node-b", KindLine, guest, map[string]any{"text": "x"}, att("vroku"))
	withHead := w.mk("node-b", KindLine, vroku, map[string]any{"text": "x"}, func(e *Entry) {
		e.Head = []byte(`{"link":"x"}`)
		e.Proof = []byte(`{"links":[]}`)
	})
	plain := w.line("node-b", vroku, "no head is fine while spec.identity.heads is empty")
	o := w.eval()
	w.rejected(o, ownerSpoken, CodeOwnerViaSurface)
	w.rejected(o, agentAtt, CodeOwnerViaSurface)
	w.accepted(o, guestAtt)
	w.rejected(o, mismatch, CodeAttestedBad)
	w.rejected(o, withHead, CodeHeadUnexpected)
	w.accepted(o, plain)

	// With heads on for approval.answer (and spec), an unheaded one has no
	// head to check and a headed one needs the identity slice: fail closed.
	w2 := newWorld(t, func(b map[string]any) {
		b["identity"] = map[string]any{"heads": []any{"spec", "approval.answer"}}
	})
	noHead := w2.mk("node-a", KindApprovalAnswer, chaz, map[string]any{"request": "r", "session": "s"})
	headed := w2.mk("node-a", KindApprovalAnswer, chaz, map[string]any{"request": "r", "session": "s"}, func(e *Entry) {
		e.Head = []byte(`{"link":"x"}`)
	})
	o = w2.eval()
	w2.rejected(o, noHead, CodeHeadMissing)
	w2.rejected(o, headed, CodeHeadUnsupported)
}

func TestBoardAdmitNodeActors(t *testing.T) {
	w := newWorld(t, nil)
	_, item := w.createItem("node-a", chaz, nil)
	w.sync()
	exec := func(node string, actorNode string) *Entry {
		return w.mk(node, KindItemSet, Actor{Node: actorNode}, map[string]any{"item": item, "field": "executor", "value": "run-1"})
	}
	ok := exec("node-a", "node-a")
	notDispatch := exec("node-b", "node-b")
	forged := exec("node-b", "node-a")
	otherKind := w.mk("node-a", KindLine, Actor{Node: "node-a"}, map[string]any{"text": "x"})
	otherField := w.mk("node-a", KindItemSet, Actor{Node: "node-a"}, map[string]any{"item": item, "field": "state", "value": "done"})
	evidenceNoHeads := w.mk("node-a", KindIdentityEvidence, Actor{Node: "node-a"}, map[string]any{"equivocation": map[string]any{}})
	o := w.eval()
	w.accepted(o, ok)
	w.rejected(o, notDispatch, CodeExecNotDispatch)
	w.rejected(o, forged, CodeExecNotDispatch)
	w.rejected(o, otherKind, CodeNotAMember)
	w.rejected(o, otherField, CodeNotAMember)
	w.rejected(o, evidenceNoHeads, CodeKindNotAllowed)

	// Evidence is admitted only in rooms with heads on, as exactly one body.
	w2 := newWorld(t, func(b map[string]any) { b["identity"] = map[string]any{"heads": []any{"spec"}} })
	ev := w2.mk("node-a", KindIdentityEvidence, Actor{Node: "node-a"}, map[string]any{"post_end": map[string]any{}})
	two := w2.mk("node-a", KindIdentityEvidence, Actor{Node: "node-a"}, map[string]any{"post_end": 1, "headers": 2})
	o = w2.eval()
	w2.accepted(o, ev)
	w2.rejected(o, two, CodeEvidenceBad)
}

func TestBoardAdmitQuota(t *testing.T) {
	w := newWorld(t, func(b map[string]any) {
		b["quota"] = map[string]any{"entryBodyBytes": 2048, "logBytes": 12000}
	})
	big := w.mk("node-b", KindLine, vroku, map[string]any{"text": strings.Repeat("x", 3000)})
	small := w.line("node-b", vroku, "ok")
	// A node appends lines until the log prefix passes logBytes; rejected
	// entries count too, so the log is cut there for good.
	var flood []*Entry
	for i := 0; i < 20; i++ {
		flood = append(flood, w.mk("node-a", KindLine, chaz, map[string]any{"text": strings.Repeat("y", 1000)}))
	}
	o := w.eval()
	w.rejected(o, big, CodeQuotaExceeded)
	w.accepted(o, small)
	cut := -1
	for i, e := range flood {
		v, _ := o.Verdict(idOfEntry(e))
		if v.Status == StatusRejected {
			if v.Code != CodeQuotaExceeded {
				t.Fatalf("flood %d: %+v", i, v)
			}
			if cut < 0 {
				cut = i
			}
		} else if cut >= 0 {
			t.Fatalf("entry %d accepted after the log was cut at %d", i, cut)
		}
	}
	if cut <= 0 || cut >= len(flood) {
		t.Fatalf("expected a cut inside the flood, got %d", cut)
	}
}

func TestBoardAdmitFloorClaims(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "hello Sage, what do you think")
	w.sync()
	lid := idOfEntry(l)
	claim := func(node string, a Actor, def, fb, nh bool, pme *float64) *Entry {
		body := map[string]any{"line": lid, "default": def, "fallback": fb, "name_hit": nh}
		if pme != nil {
			body["p_me"] = *pme
		}
		return w.mk(node, KindFloorClaim, a, body)
	}
	half := 0.5
	sageOK := claim("node-b", sage, false, false, true, &half)
	sageNameWrong := claim("node-b", sage, false, false, false, &half)
	sageDefault := claim("node-b", sage, true, false, true, nil) // sage is not the default: cog is
	cogOK := claim("node-a", cog, true, false, false, nil)
	cogDefaultWrong := claim("node-a", cog, false, false, false, nil)
	fbWithPMe := claim("node-b", sage, false, true, true, &half)
	fbOK := claim("node-b", sage, false, true, true, nil)
	o := w.eval()
	w.accepted(o, sageOK)
	w.rejected(o, sageNameWrong, CodeClaimNameHitWrong)
	w.rejected(o, sageDefault, CodeClaimDefaultWrong)
	w.accepted(o, cogOK)
	w.rejected(o, cogDefaultWrong, CodeClaimDefaultWrong)
	w.rejected(o, fbWithPMe, CodeClaimFallbackPMe)
	w.accepted(o, fbOK)

	// A claim whose deps cover a reply to L is late.
	w2 := newWorld(t, nil)
	l2 := w2.line("node-a", chaz, "anyone?")
	w2.sync()
	reply := w2.mk("node-a", KindLine, cog, map[string]any{"text": "me", "to": []any{}, "in_reply_to": idOfEntry(l2)})
	w2.sync()
	late := w2.mk("node-b", KindFloorClaim, sage, map[string]any{
		"line": idOfEntry(l2), "default": false, "fallback": false, "name_hit": false})
	o = w2.eval()
	w2.accepted(o, reply)
	w2.rejected(o, late, CodeClaimAfterReply)

	// A claim on a line the claimant never saw.
	w3 := newWorld(t, nil)
	l3 := w3.line("node-a", chaz, "hey")
	unseen := w3.mk("node-b", KindFloorClaim, sage, map[string]any{
		"line": idOfEntry(l3), "default": false, "fallback": false, "name_hit": false}, func(e *Entry) {
		delete(e.Deps, testNodeSet["node-a"].dev)
	})
	w3.rejected(w3.eval(), unseen, CodeClaimLineUnknown)
}

func TestBoardAdmitDefaultClaimantFollowsReplies(t *testing.T) {
	w := newWorld(t, nil)
	l1 := w.line("node-a", chaz, "first")
	w.sync()
	// sage replied to chaz earlier: sage becomes the default for chaz's next line.
	w.mk("node-b", KindLine, sage, map[string]any{"text": "r", "to": []any{}, "in_reply_to": idOfEntry(l1)})
	w.sync()
	l2 := w.line("node-a", chaz, "second")
	w.sync()
	sageDefault := w.mk("node-b", KindFloorClaim, sage, map[string]any{
		"line": idOfEntry(l2), "default": true, "fallback": false, "name_hit": false})
	cogNotDefault := w.mk("node-a", KindFloorClaim, cog, map[string]any{
		"line": idOfEntry(l2), "default": true, "fallback": false, "name_hit": false})
	o := w.eval()
	w.accepted(o, sageDefault)
	w.rejected(o, cogNotDefault, CodeClaimDefaultWrong)
}

func TestBoardAdmitPickWinner(t *testing.T) {
	p := func(f float64) *float64 { return &f }
	prio := []string{"cog@node-a", "sage@node-b", "zed@node-b"}
	cases := []struct {
		name   string
		claims []Claim
		want   string
	}{
		{"name hit wins outright", []Claim{
			{Member: "cog@node-a", PMe: p(0.99), Default: true},
			{Member: "sage@node-b", PMe: p(0.1), NameHit: true}}, "sage@node-b"},
		{"higher p_me wins", []Claim{
			{Member: "cog@node-a", PMe: p(0.2), Default: true},
			{Member: "sage@node-b", PMe: p(0.7)}}, "sage@node-b"},
		{"within 0.05 falls to default", []Claim{
			{Member: "cog@node-a", PMe: p(0.66), Default: true},
			{Member: "sage@node-b", PMe: p(0.70)}}, "cog@node-a"},
		{"fallback and default both claiming: default wins", []Claim{
			{Member: "sage@node-b", Fallback: true},
			{Member: "cog@node-a", Default: true}}, "cog@node-a"},
		{"no default: firm beats fallback", []Claim{
			{Member: "cog@node-a", Fallback: true},
			{Member: "sage@node-b"}}, "sage@node-b"},
		{"priority breaks the rest", []Claim{
			{Member: "zed@node-b"}, {Member: "sage@node-b"}}, "sage@node-b"},
		{"unlisted ranks after, by id", []Claim{
			{Member: "b@x"}, {Member: "a@x"}}, "a@x"},
		{"three-way tie uses the max, not pairwise", []Claim{
			{Member: "cog@node-a", PMe: p(0.60)},
			{Member: "sage@node-b", PMe: p(0.64)},
			{Member: "zed@node-b", PMe: p(0.68)}}, "sage@node-b"},
	}
	for _, c := range cases {
		got, _ := PickWinner(c.claims, prio)
		if got != c.want {
			t.Errorf("%s: got %s want %s", c.name, got, c.want)
		}
	}
}

func TestBoardAdmitNameHit(t *testing.T) {
	for text, want := range map[string]bool{
		"hello Sage":        true,
		"sage, thoughts?":   true,
		"@sage ping":        true,
		"a message":         false,
		"sagely advice":     false,
		"the_sage_is":       false,
		"What about SAGE?!": true,
	} {
		if got := NameHit(text, "sage@node-b"); got != want {
			t.Errorf("%q: got %v", text, got)
		}
	}
}

func TestBoardAdmitSpecValidate(t *testing.T) {
	w := newWorld(t, nil)
	s, err := SpecFromEntry(w.g)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(nil); err != nil {
		t.Fatal(err)
	}
	// A non-empty identity.heads must list spec.
	s.Identity.Heads = []string{"approval.answer"}
	if err := s.Validate(nil); err == nil {
		t.Fatal("heads without spec accepted")
	}
	s.Identity.Heads = []string{"spec", "approval.answer"}
	if err := s.Validate(nil); err != nil {
		t.Fatal(err)
	}
	// The set cannot shrink in place.
	next := *s
	next.Gen = 2
	next.Identity.Heads = []string{"spec"}
	if err := next.Validate(s); err == nil {
		t.Fatal("heads shrank in place")
	}
	// homes(R) is the union of member homes and agent nodes.
	if got := strings.Join(s.Homes(), ","); got != "node-a,node-b" {
		t.Fatalf("homes: %s", got)
	}
}

func TestBoardAdmitItemAppendAndMarks(t *testing.T) {
	w := newWorld(t, nil)
	_, item := w.createItem("node-a", chaz, nil)
	w.sync()
	app := w.mk("node-b", KindItemAppend, guest, map[string]any{"item": item, "field": "comments", "value": "hi"})
	appUnknown := w.mk("node-b", KindItemAppend, guest, map[string]any{"item": "nope-1", "field": "comments", "value": "hi"})
	mark := w.mk("node-b", KindMark, guest, map[string]any{"x": 1, "y": 2})
	// An item created concurrently is not in the setter's past.
	_, itemNodeB := w.createItem("node-b", vroku, nil)
	notSeen := w.mk("node-a", KindItemSet, chaz, map[string]any{"item": itemNodeB, "field": "title", "value": "t"})
	o := w.eval()
	w.accepted(o, app)
	w.rejected(o, appUnknown, CodeItemUnknown)
	w.accepted(o, mark)
	w.rejected(o, notSeen, CodeItemUnknown)
}

// assertOrderIndependent re-evaluates the world's entries in n random arrival
// orders and demands the verdicts of the first, whatever the order.
func assertOrderIndependent(t *testing.T, w *world, n int) {
	t.Helper()
	want := verdictMap(w.eval())
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < n; i++ {
		perm := append([]*Entry(nil), w.all...)
		rng.Shuffle(len(perm), func(a, b int) { perm[a], perm[b] = perm[b], perm[a] })
		got := verdictMap(w.eval(perm...))
		for id, v := range want {
			if got[id] != v {
				t.Fatalf("perm %d: %s: got %s want %s", i, id, got[id], v)
			}
		}
	}
}

// P4-1 adversarial review, break 1: a rejected spec entry must not make later
// entries spec_stale. Only accepted chain specs count.
func TestBoardAdmitRejectedSpecDoesNotBrickRoom(t *testing.T) {
	w := newWorld(t, nil)
	forged := defaultGenesisBody()
	forged["gen"] = 99
	delete(forged, "name")
	bad := w.mk("node-b", KindSpec, guest, forged)
	// Same log, so the guest's spec is in the causal past of what follows.
	after := w.line("node-b", vroku, "still here")
	w.sync()
	next := defaultGenesisBody()
	next["gen"] = 2
	delete(next, "name")
	s2 := w.mk("node-a", KindSpec, chaz, next) // deps include the guest's head
	o := w.eval()
	w.rejected(o, bad, CodeRoleDenied)
	w.accepted(o, after)
	w.accepted(o, s2)
	if len(o.Chain) != 2 {
		t.Fatalf("chain: %d", len(o.Chain))
	}
	assertOrderIndependent(t, w, 25)
}

// Break 2: a node dropped from spec.nodes cannot keep writing by citing an
// older spec and leaving the removal out of its deps (node_cutoffs).
func TestBoardAdmitNodeCutoffs(t *testing.T) {
	phone := testNodeSet["phone"].dev
	w := newWorld(t, func(b map[string]any) {
		b["nodes"].(map[string]any)["phone"] = phone
		b["members"].(map[string]any)["chaz"] = map[string]any{"type": "person", "role": "owner", "homes": []any{"node-a", "phone"}}
	})
	w.app("phone")
	kept := w.line("phone", chaz, "last from the phone")
	rm := defaultGenesisBody()
	rm["gen"] = 2
	delete(rm, "name")
	rm["node_cutoffs"] = map[string]any{phone: kept.Seq}
	removal := w.mk("node-a", KindSpec, chaz, rm)
	// The phone has not observed the removal and cites gen 1.
	sneaky := w.mk("phone", KindLine, chaz, map[string]any{"text": "still writing"})
	w.sync()
	// Even citing the new spec, the phone's log is closed above the cutoff.
	newer := w.mk("phone", KindLine, chaz, map[string]any{"text": "again"}, func(e *Entry) { e.Spec = idOfEntry(removal) })
	o := w.eval()
	w.accepted(o, removal)
	w.accepted(o, kept)
	w.rejected(o, sneaky, CodeNodeRemoved)
	w.rejected(o, newer, CodeNodeRemoved)
	assertOrderIndependent(t, w, 25)

	// A spec that drops a node without a cutoff for it is not a valid spec.
	w2 := newWorld(t, func(b map[string]any) {
		b["nodes"].(map[string]any)["phone"] = phone
	})
	rm2 := defaultGenesisBody()
	rm2["gen"] = 2
	delete(rm2, "name")
	bad := w2.mk("node-a", KindSpec, chaz, rm2)
	w2.rejected(w2.eval(), bad, CodeSpecBad)
}

// Break 3: a rejected entry cannot decide the default claimant.
func TestBoardAdmitDefaultClaimantIgnoresRejected(t *testing.T) {
	w := newWorld(t, nil)
	l1 := w.line("node-a", chaz, "first")
	// Forged agent reply: sage's line written by node-a (actor_not_homed).
	forged := w.mk("node-a", KindLine, sage, map[string]any{"text": "r", "to": []any{}, "in_reply_to": idOfEntry(l1)})
	l2 := w.line("node-a", chaz, "second")
	w.sync()
	body := func() map[string]any {
		return map[string]any{"line": idOfEntry(l2), "default": true, "fallback": false, "name_hit": false}
	}
	cogDefault := w.mk("node-a", KindFloorClaim, cog, body())
	sageDefault := w.mk("node-b", KindFloorClaim, sage, body())
	o := w.eval()
	w.rejected(o, forged, CodeActorNotHomed)
	w.accepted(o, cogDefault)
	w.rejected(o, sageDefault, CodeClaimDefaultWrong)
	assertOrderIndependent(t, w, 25)
}

// Break 4: one byte string means one entry. Keys must be exact-case known
// fields at every object level the entry defines.
func TestBoardLogParseRejectsCaseFoldedKeys(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "hi")
	raw, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseEntry(raw); err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	dev := testNodeSet["node-a"].dev
	for name, mut := range map[string]string{
		"top-level kind": strings.Replace(s, `"kind":"line"`, `"kind":"line","KIND":"approval.answer"`, 1),
		"top-level body": strings.Replace(s, `"body":{`, `"Body":{"text":"APPROVE EVERYTHING"},"body":{`, 1),
		"only-folded":    strings.Replace(s, `"kind":`, `"Kind":`, 1),
		"actor":          strings.Replace(s, `"person":"chaz"`, `"person":"chaz","Person":"mallory"`, 1),
		"deps value":     strings.Replace(s, `"seq":1,`, `"seq":1,"SEQ":2,`, 1),
	} {
		if mut == s {
			t.Fatalf("%s: mutation did not apply (dev %s): %s", name, dev, s)
		}
		if _, err := ParseEntry([]byte(mut)); err == nil {
			t.Errorf("%s: case-folded key accepted: %s", name, mut)
		}
	}
}

// Break 5: two accepted creates cannot hold one item id. The short form is cut
// to one byte so a real collision costs a few dozen certificates.
func TestBoardAdmitItemIDClashBetweenCreates(t *testing.T) {
	old := logShortBytes
	logShortBytes = 1
	defer func() { logShortBytes = old }()

	dir := t.TempDir()
	seen := map[string]*node{}
	var c1, c2 *node
	for i := 0; c2 == nil; i++ {
		if i > 2000 {
			t.Fatal("no 1-byte collision found")
		}
		d := filepath.Join(dir, fmt.Sprint(i))
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := bep.GenerateBEPCert(d); err != nil {
			t.Fatal(err)
		}
		cert, err := bep.LoadBEPCert(d)
		if err != nil {
			t.Fatal(err)
		}
		id, err := bep.DeviceIDFromTLSCert(&cert)
		if err != nil {
			t.Fatal(err)
		}
		n := &node{name: fmt.Sprint("c", i), key: cert.PrivateKey.(*ecdsa.PrivateKey),
			der: cert.Certificate[0], dev: bep.FormatDeviceID(id)}
		sh, _ := LogShort(n.dev)
		if p, ok := seen[sh]; ok {
			c1, c2 = p, n
			break
		}
		seen[sh] = n
	}
	c1.name, c2.name = "col1", "col2"
	testNodeSet["col1"], testNodeSet["col2"] = c1, c2
	defer delete(testNodeSet, "col1")
	defer delete(testNodeSet, "col2")

	w := newWorld(t, func(b map[string]any) {
		b["nodes"].(map[string]any)["col1"] = c1.dev
		b["nodes"].(map[string]any)["col2"] = c2.dev
		b["members"].(map[string]any)["chaz"] = map[string]any{"type": "person", "role": "owner", "homes": []any{"node-a", "col1", "col2"}}
	})
	e1, id1 := w.createItem("col1", chaz, nil)
	e2, id2 := w.createItem("col2", chaz, nil)
	if id1 != id2 {
		t.Fatalf("collision not built: %s %s", id1, id2)
	}
	o := w.eval()
	winner, loser := e1, e2
	if idOfEntry(e2) < idOfEntry(e1) {
		winner, loser = e2, e1
	}
	w.accepted(o, winner)
	w.rejected(o, loser, CodeItemIDClash)
	if got := o.ItemAuthors[id1]; got != "person:chaz" {
		t.Fatalf("author: %q", got)
	}
	assertOrderIndependent(t, w, 25)
}

// Round 2, 3b: a rejected line cannot choose the default claimant through
// in_reply_to. cog legitimately replied to chaz; node-b forges a line as chaz
// (actor_not_homed) and sage, legitimate on node-b, replies to the forgery.
func TestBoardAdmitDefaultClaimantIgnoresRejectedReplyTarget(t *testing.T) {
	w := newWorld(t, nil)
	l0 := w.line("node-a", chaz, "first")
	w.sync()
	w.mk("node-a", KindLine, cog, map[string]any{"text": "r", "to": []any{}, "in_reply_to": idOfEntry(l0)})
	w.sync()
	forged := w.line("node-b", chaz, "forged") // node-b is not chaz's home
	w.sync()
	w.mk("node-b", KindLine, sage, map[string]any{"text": "r", "to": []any{}, "in_reply_to": idOfEntry(forged)})
	w.sync()
	l := w.line("node-a", chaz, "second")
	w.sync()
	body := func() map[string]any {
		return map[string]any{"line": idOfEntry(l), "default": true, "fallback": false, "name_hit": false}
	}
	cogDefault := w.mk("node-a", KindFloorClaim, cog, body())
	sageDefault := w.mk("node-b", KindFloorClaim, sage, body())
	o := w.eval()
	w.rejected(o, forged, CodeActorNotHomed)
	w.accepted(o, cogDefault)
	w.rejected(o, sageDefault, CodeClaimDefaultWrong)
	assertOrderIndependent(t, w, 25)
}

// Round 2, 3c: a reply rejected at Layer 2 has no stored verdict, and a
// missing verdict must not read as accepted (StatusAccepted is the zero
// value), or it would make a later claim claim_after_reply.
func TestBoardAdmitLayer2RejectedReplyIsNotAReply(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "anyone there")
	w.mk("node-b", KindLine, vroku, map[string]any{"text": "x", "to": []any{}})
	w.sync()
	p := w.mk("node-a", KindLine, cog, map[string]any{"text": "p", "to": []any{}})
	w.sync()
	// r drops the node-b dep its predecessor p carries: deps_regress.
	r := w.mk("node-a", KindLine, cog, map[string]any{"text": "me", "to": []any{}, "in_reply_to": idOfEntry(l)},
		func(e *Entry) { delete(e.Deps, testNodeSet["node-b"].dev) })
	w.sync()
	claim := w.mk("node-b", KindFloorClaim, sage, map[string]any{
		"line": idOfEntry(l), "default": false, "fallback": false, "name_hit": false})
	o := w.eval()
	w.accepted(o, p)
	w.rejected(o, r, CodeDepsRegress)
	w.accepted(o, claim)
	assertOrderIndependent(t, w, 25)
}

// Round 2, 4b: spec and genesis bodies are read with exact-case keys at every
// object level the RFC defines.
func TestBoardAdmitSpecBodyRejectsFoldedKeys(t *testing.T) {
	cases := map[string]func(b map[string]any){
		"top-level GEN only": func(b map[string]any) { delete(b, "gen"); b["GEN"] = 1 },
		"repeated Members": func(b map[string]any) {
			b["Members"] = map[string]any{"mallory": map[string]any{"type": "person", "role": "collaborator", "homes": []any{"node-b"}}}
		},
		"long s scope": func(b map[string]any) { delete(b, "scope"); b["ſcope"] = map[string]any{"pipelines": []any{"review"}} },
		"identity Heads": func(b map[string]any) {
			b["floor"] = map[string]any{"priority": []any{"cog@node-a"}}
			b["identity"] = map[string]any{"Heads": []any{"spec"}}
		},
		"member TYPE ROLE": func(b map[string]any) {
			b["members"].(map[string]any)["vroku"] = map[string]any{"TYPE": "person", "ROLE": "collaborator", "homes": []any{"node-b"}}
		},
		"dispatch Node": func(b map[string]any) { b["dispatch"] = map[string]any{"Node": "node-a"} },
		"quota":         func(b map[string]any) { b["quota"] = map[string]any{"LogBytes": 5} },
		"member Homes": func(b map[string]any) {
			b["members"].(map[string]any)["chaz"] = map[string]any{"type": "person", "role": "owner", "Homes": []any{"node-a"}}
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t, mut)
			w.rejected(w.eval(), w.g, CodeGenesisBad)
		})
	}
	// The same on a gen-2 spec entry.
	w := newWorld(t, nil)
	b := defaultGenesisBody()
	b["gen"] = 2
	delete(b, "name")
	b["Quota"] = map[string]any{"logBytes": 1}
	s2 := w.mk("node-a", KindSpec, chaz, b)
	o := w.eval()
	w.rejected(o, s2, CodeSpecBad)
	assertOrderIndependent(t, w, 25)
}

// Round 2, 6: node_cutoffs must name a node the previous spec listed and this
// spec no longer lists.
func TestBoardAdmitNodeCutoffsMustMatchDroppedNodes(t *testing.T) {
	phone := testNodeSet["phone"].dev
	rogue := testNodeSet["rogue"].dev
	w := newWorld(t, func(b map[string]any) { b["nodes"].(map[string]any)["phone"] = phone })
	// Cutoff for a node the same spec still lists.
	still := defaultGenesisBody()
	still["gen"] = 2
	delete(still, "name")
	still["nodes"].(map[string]any)["phone"] = phone
	still["node_cutoffs"] = map[string]any{phone: 3}
	selfContradict := w.mk("node-a", KindSpec, chaz, still)
	// Cutoff for a node the previous spec never listed.
	never := defaultGenesisBody()
	never["gen"] = 2
	delete(never, "name")
	never["nodes"].(map[string]any)["phone"] = phone
	never["node_cutoffs"] = map[string]any{rogue: 3}
	unknown := w.mk("node-a", KindSpec, chaz, never)
	o := w.eval()
	w.rejected(o, selfContradict, CodeSpecBad)
	w.rejected(o, unknown, CodeSpecBad)
	assertOrderIndependent(t, w, 25)

	// Unit form.
	prev := &Spec{Gen: 1, Owner: "chaz", Nodes: map[string]string{"a": testNodeSet["node-a"].dev, "p": phone},
		Members: map[string]Member{"chaz": {Type: TypePerson, Role: RoleOwner, Homes: []string{"a"}}}}
	next := &Spec{Gen: 2, Owner: "chaz", Nodes: map[string]string{"a": testNodeSet["node-a"].dev, "p": phone},
		Members:     prev.Members,
		NodeCutoffs: map[string]uint64{phone: 1}}
	if next.Validate(prev) == nil {
		t.Fatal("a cutoff for a node the spec still lists must be refused")
	}
}

// Pilot ruling R12: a node cutoff is permanent. A later spec cannot list a
// DeviceID that has a cutoff anywhere in the chain; the node returns under a
// new DeviceID.
func TestBoardAdmitCutoffNodeCannotRejoin(t *testing.T) {
	phone := testNodeSet["phone"].dev
	rogue := testNodeSet["rogue"].dev
	w := newWorld(t, func(b map[string]any) { b["nodes"].(map[string]any)["phone"] = phone })
	w.app("phone")
	rm := defaultGenesisBody()
	rm["gen"] = 2
	delete(rm, "name")
	rm["node_cutoffs"] = map[string]any{phone: 5}
	removal := w.mk("node-a", KindSpec, chaz, rm)
	w.sync()
	again := defaultGenesisBody()
	again["gen"] = 3
	delete(again, "name")
	again["nodes"].(map[string]any)["phone"] = phone
	rejoin := w.mk("node-a", KindSpec, chaz, again, func(e *Entry) { e.Spec = idOfEntry(removal) })
	o := w.eval()
	w.accepted(o, removal)
	w.rejected(o, rejoin, CodeSpecBad)
	assertOrderIndependent(t, w, 25)

	// Under a new DeviceID the node comes back.
	w2 := newWorld(t, func(b map[string]any) { b["nodes"].(map[string]any)["phone"] = phone })
	rm2 := defaultGenesisBody()
	rm2["gen"] = 2
	delete(rm2, "name")
	rm2["node_cutoffs"] = map[string]any{phone: 5}
	removal2 := w2.mk("node-a", KindSpec, chaz, rm2)
	w2.sync()
	back := defaultGenesisBody()
	back["gen"] = 3
	delete(back, "name")
	back["nodes"].(map[string]any)["phone"] = rogue
	ok := w2.mk("node-a", KindSpec, chaz, back, func(e *Entry) { e.Spec = idOfEntry(removal2) })
	o2 := w2.eval()
	w2.accepted(o2, ok)
	assertOrderIndependent(t, w2, 25)

	// Unit form.
	hist := &Spec{Gen: 2, NodeCutoffs: map[string]uint64{phone: 5}}
	s := &Spec{Gen: 1, Owner: "chaz", Nodes: map[string]string{"a": testNodeSet["node-a"].dev, "p": phone},
		Members: map[string]Member{"chaz": {Type: TypePerson, Role: RoleOwner, Homes: []string{"a"}}}}
	if s.Validate(nil, hist) == nil {
		t.Fatal("a DeviceID with a cutoff in the chain must not be listed again")
	}
}

// Round 3, 3d: defaultClaimant step 1 reads an in_reply_to target only when
// the target is in the claim line's causal past. Here sage replies to a line
// t written on a log the claim line never saw; whether t is held must not
// change the verdict on a claim whose whole past is held.
func TestBoardAdmitDefaultClaimantReplyTargetMustBeInPast(t *testing.T) {
	phone := testNodeSet["phone"].dev
	w := newWorld(t, func(b map[string]any) {
		b["nodes"].(map[string]any)["phone"] = phone
		b["members"].(map[string]any)["chaz"] = map[string]any{"type": "person", "role": "owner", "homes": []any{"node-a", "phone"}}
	})
	w.app("node-b")
	w.app("phone")
	// t lives on the phone log and is never synced to anyone.
	tgt := w.mk("phone", KindLine, chaz, map[string]any{"text": "unseen target", "to": []any{}})
	cogLine := w.mk("node-a", KindLine, cog, map[string]any{"text": "hello", "to": []any{}})
	sageReply := w.mk("node-b", KindLine, sage, map[string]any{"text": "r", "to": []any{}, "in_reply_to": idOfEntry(tgt)})
	a, b := w.apps["node-a"], w.apps["node-b"]
	a.Observe(sageReply)
	b.Observe(cogLine)
	l := w.mk("node-a", KindLine, chaz, map[string]any{"text": "question", "to": []any{}})
	b.Observe(l)
	body := func() map[string]any {
		return map[string]any{"line": idOfEntry(l), "default": true, "fallback": false, "name_hit": false}
	}
	cogDefault := w.mk("node-a", KindFloorClaim, cog, body())
	sageDefault := w.mk("node-b", KindFloorClaim, sage, body())

	var withoutT []*Entry
	for _, e := range w.all {
		if e != tgt {
			withoutT = append(withoutT, e)
		}
	}
	for name, set := range map[string][]*Entry{"t held": w.all, "t absent": withoutT} {
		o := w.eval(set...)
		w.accepted(o, cogDefault)
		w.rejected(o, sageDefault, CodeClaimDefaultWrong)
		_ = name
	}
	assertOrderIndependent(t, w, 25)
}

// Round 3, 6b: a genesis carries no cutoffs or node_cutoffs, and every
// cutoffs key of a spec names a member the previous spec has and this spec
// removes.
func TestBoardAdmitGenesisAndSpecCutoffsAreBounded(t *testing.T) {
	nodeB := testNodeSet["node-b"].dev
	w := newWorld(t, func(b map[string]any) { b["node_cutoffs"] = map[string]any{nodeB: 0} })
	w.rejected(w.eval(), w.g, CodeGenesisBad)
	w = newWorld(t, func(b map[string]any) {
		b["cutoffs"] = map[string]any{"vroku": map[string]any{nodeB: 0}}
	})
	w.rejected(w.eval(), w.g, CodeGenesisBad)

	// Specs: a cutoffs key for a member that stays, and one that never existed.
	w2 := newWorld(t, nil)
	stays := defaultGenesisBody()
	stays["gen"] = 2
	delete(stays, "name")
	stays["cutoffs"] = map[string]any{"vroku": map[string]any{testNodeSet["node-a"].dev: 1, nodeB: 1}}
	s1 := w2.mk("node-a", KindSpec, chaz, stays)
	ghost := defaultGenesisBody()
	ghost["gen"] = 2
	delete(ghost, "name")
	ghost["cutoffs"] = map[string]any{"ghost": map[string]any{nodeB: 1}}
	s2 := w2.mk("node-a", KindSpec, chaz, ghost)
	o := w2.eval()
	w2.rejected(o, s1, CodeSpecBad)
	w2.rejected(o, s2, CodeSpecBad)
	assertOrderIndependent(t, w2, 25)
}

// Round 3, member re-add (pilot ruling R12): a removed member may be re-added
// under the same id. Entries above the cutoff are admitted only when they cite
// a spec whose gen is at least the re-adding spec's gen; an older spec stays
// after_removal.
func TestBoardAdmitMemberReaddCitesReaddingSpec(t *testing.T) {
	devA, devB := testNodeSet["node-a"].dev, testNodeSet["node-b"].dev
	w := newWorld(t, nil)
	before := w.line("node-b", vroku, "last words")
	w.sync()
	rm := defaultGenesisBody()
	rm["gen"] = 2
	delete(rm, "name")
	delete(rm["members"].(map[string]any), "vroku")
	rm["cutoffs"] = map[string]any{"vroku": map[string]any{devA: 1, devB: before.Seq}}
	removal := w.mk("node-a", KindSpec, chaz, rm)
	// Written on node-b before it has seen the removal: cites gen 1.
	stale := w.mk("node-b", KindLine, vroku, map[string]any{"text": "old spec", "to": []any{}})
	w.sync()
	readd := defaultGenesisBody()
	readd["gen"] = 3
	delete(readd, "name")
	readdE := w.mk("node-a", KindSpec, chaz, readd, func(e *Entry) { e.Spec = idOfEntry(removal) })
	w.sync()
	back := w.mk("node-b", KindLine, vroku, map[string]any{"text": "i am back", "to": []any{}}, func(e *Entry) { e.Spec = idOfEntry(readdE) })
	mid := w.mk("node-b", KindLine, vroku, map[string]any{"text": "gen 2", "to": []any{}}, func(e *Entry) { e.Spec = idOfEntry(removal) })
	o := w.eval()
	w.accepted(o, removal)
	w.accepted(o, readdE)
	w.accepted(o, before)
	w.rejected(o, stale, CodeAfterRemoval)
	w.accepted(o, back)
	w.rejected(o, mid, CodeSpecStale)
	assertOrderIndependent(t, w, 25)

	// Removed again: the second cutoff binds the gen-3 citations above it.
	rm2 := defaultGenesisBody()
	rm2["gen"] = 4
	delete(rm2, "name")
	delete(rm2["members"].(map[string]any), "vroku")
	rm2["cutoffs"] = map[string]any{"vroku": map[string]any{devA: 1, devB: back.Seq}}
	removal2 := w.mk("node-a", KindSpec, chaz, rm2, func(e *Entry) { e.Spec = idOfEntry(readdE) })
	late := w.mk("node-b", KindLine, vroku, map[string]any{"text": "gen 3 again", "to": []any{}}, func(e *Entry) { e.Spec = idOfEntry(readdE) })
	o = w.eval()
	w.accepted(o, removal2)
	w.accepted(o, back)
	w.rejected(o, late, CodeAfterRemoval)
	assertOrderIndependent(t, w, 25)
}
