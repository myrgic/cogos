package room

import (
	"crypto/ecdsa"
	"fmt"
)

// Appender builds and signs the entries of one node's log for one room. It is
// the authoring side: it holds no store and does no IO. P4-3 persists what it
// returns.
type Appender struct {
	Room string
	Log  string
	Priv *ecdsa.PrivateKey

	seq   uint64
	prev  string
	heads map[string]Dep
}

// NewAppender starts a log for a room whose genesis id is known. The first
// entry's prev is the genesis id (RFC section 6.9.1).
func NewAppender(room, log string, priv *ecdsa.PrivateKey) *Appender {
	return &Appender{Room: room, Log: log, Priv: priv, prev: room, heads: map[string]Dep{}}
}

// Genesis writes the room's first entry on this node's log. The room id is
// the genesis entry id, so the Appender adopts it.
func (a *Appender) Genesis(actor Actor, body map[string]any, hlc string) (*Entry, error) {
	if a.seq != 0 {
		return nil, fmt.Errorf("log already started")
	}
	e := &Entry{Log: a.Log, Seq: 1, Prev: nil, Deps: map[string]Dep{}, Actor: actor,
		Kind: KindGenesis, Body: body, HLC: hlc}
	if err := e.Sign(a.Priv); err != nil {
		return nil, err
	}
	id, err := e.ID()
	if err != nil {
		return nil, err
	}
	a.Room, a.seq, a.prev = id, 1, id
	a.heads[a.Log] = Dep{Seq: 1, ID: id}
	return e, nil
}

// Next appends one entry under the given spec entry id.
func (a *Appender) Next(kind Kind, actor Actor, spec string, body map[string]any, hlc string) (*Entry, error) {
	if a.Room == "" {
		return nil, fmt.Errorf("no room yet")
	}
	prev := a.prev
	deps := make(map[string]Dep, len(a.heads))
	for l, d := range a.heads {
		deps[l] = d
	}
	e := &Entry{Room: a.Room, Log: a.Log, Seq: a.seq + 1, Prev: &prev, Deps: deps, Spec: spec,
		Actor: actor, Kind: kind, Body: body, HLC: hlc}
	if err := e.Sign(a.Priv); err != nil {
		return nil, err
	}
	id, err := e.ID()
	if err != nil {
		return nil, err
	}
	a.seq, a.prev = e.Seq, id
	a.heads[a.Log] = Dep{Seq: e.Seq, ID: id}
	return e, nil
}

// NextSeq is the seq the next Next call will use. It lets a caller compute an
// item.create id (ItemID(log, seq)) before handing the id to Next, which signs
// the body as given.
func (a *Appender) NextSeq() uint64 { return a.seq + 1 }

// Observe records an entry as accepted, so later entries carry it in deps.
// Heads never regress. An entry on this appender's OWN log also advances its
// position (seq and prev), which is what lets a restarted node resume a log it
// already wrote: Observe every stored entry and the next Next continues at the
// stored head instead of re-signing — and so replacing — an existing entry.
// Observing an entry already seen is a no-op.
func (a *Appender) Observe(e *Entry) error {
	id, err := e.ID()
	if err != nil {
		return err
	}
	if e.Log == a.Log && e.Seq > a.seq {
		prev := id
		a.seq, a.prev = e.Seq, prev
	}
	if cur, ok := a.heads[e.Log]; !ok || e.Seq > cur.Seq {
		a.heads[e.Log] = Dep{Seq: e.Seq, ID: id}
	}
	return nil
}

// Replica is an in-memory set of entries for one room. Add re-evaluates the
// whole set, so the result is order-independent; it is a model for tests and
// for the P4-3 store to check itself against, not an index.
type Replica struct {
	p       Params
	entries []*Entry
}

// NewReplica starts an empty replica.
func NewReplica(p Params) *Replica { return &Replica{p: p} }

// AddCert registers a certificate delivered with a batch.
func (r *Replica) AddCert(der []byte) { r.p.Certs = append(r.p.Certs, der) }

// Add inserts an entry and returns its verdict against the new set.
func (r *Replica) Add(e *Entry) Verdict {
	r.entries = append(r.entries, e)
	o := r.Outcome()
	return o.Verdicts[len(o.Verdicts)-1]
}

// Outcome evaluates the current set.
func (r *Replica) Outcome() *Outcome { return Evaluate(r.p, r.entries) }
