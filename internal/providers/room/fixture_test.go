package room

import (
	"crypto/ecdsa"
	"crypto/tls"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/myrgic/cogos/pkg/substrate/bep"
)

type node struct {
	name string
	key  *ecdsa.PrivateKey
	der  []byte
	dev  string // canonical DeviceID
}

var testNodeSet map[string]*node

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "room-test-certs")
	if err != nil {
		panic(err)
	}
	testNodeSet = map[string]*node{}
	for _, n := range []string{"darkstar", "eclipse", "rogue", "phone"} {
		d := filepath.Join(dir, n)
		if err := os.MkdirAll(d, 0o700); err != nil {
			panic(err)
		}
		if err := bep.GenerateBEPCert(d); err != nil {
			panic(err)
		}
		var cert tls.Certificate
		if cert, err = bep.LoadBEPCert(d); err != nil {
			panic(err)
		}
		id, err := bep.DeviceIDFromTLSCert(&cert)
		if err != nil {
			panic(err)
		}
		testNodeSet[n] = &node{name: n, key: cert.PrivateKey.(*ecdsa.PrivateKey),
			der: cert.Certificate[0], dev: bep.FormatDeviceID(id)}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// world is a small constellation: darkstar (owner chaz, agent cog), eclipse
// (collaborator vroku, guest, agent sage). rogue and phone hold keys but are
// not in spec.nodes unless a test adds them.
type world struct {
	t       *testing.T
	room    string
	g       *Entry
	spec    string // id of the spec entry in force for new entries
	apps    map[string]*Appender
	all     []*Entry
	binding StubBinding
	n       int
}

func defaultGenesisBody() map[string]any {
	return map[string]any{
		"name":  "oracles",
		"gen":   1,
		"owner": "chaz",
		"nodes": map[string]any{
			"darkstar": testNodeSet["darkstar"].dev,
			"eclipse":  testNodeSet["eclipse"].dev,
		},
		"members": map[string]any{
			"chaz":         map[string]any{"type": "person", "role": "owner", "homes": []any{"darkstar"}},
			"vroku":        map[string]any{"type": "person", "role": "collaborator", "homes": []any{"eclipse"}},
			"guest1":       map[string]any{"type": "person", "role": "guest", "homes": []any{"eclipse"}},
			"cog@darkstar": map[string]any{"type": "agent", "role": "agent", "node": "darkstar", "class": "full"},
			"sage@eclipse": map[string]any{"type": "agent", "role": "agent", "node": "eclipse", "class": "collab"},
		},
		"dispatch": map[string]any{"node": "darkstar"},
		"floor":    map[string]any{"priority": []any{"cog@darkstar", "sage@eclipse"}},
		"scope":    map[string]any{"pipelines": []any{"review"}},
	}
}

func newWorld(t *testing.T, mutate func(body map[string]any)) *world {
	t.Helper()
	w := &world{t: t, apps: map[string]*Appender{}}
	body := defaultGenesisBody()
	if mutate != nil {
		mutate(body)
	}
	d := testNodeSet["darkstar"]
	w.binding = StubBinding{"chaz": {d.dev}}
	app := NewAppender("", d.dev, d.key)
	g, err := app.Genesis(Actor{Person: "chaz"}, body, w.hlc())
	if err != nil {
		t.Fatal(err)
	}
	w.g, w.room, w.spec = g, app.Room, app.Room
	w.apps["darkstar"] = app
	w.all = []*Entry{g}
	return w
}

func (w *world) hlc() string {
	w.n++
	return fmt.Sprintf("2026-10-15T18:%02d:%02d.000Z/0", (w.n/60)%60, w.n%60)
}

func (w *world) app(name string) *Appender {
	if a, ok := w.apps[name]; ok {
		return a
	}
	n := testNodeSet[name]
	a := NewAppender(w.room, n.dev, n.key)
	// A node joining sees everything already written.
	for _, e := range w.all {
		a.Observe(e)
	}
	w.apps[name] = a
	return a
}

// sync lets every node see every entry written so far.
func (w *world) sync() {
	for _, a := range w.apps {
		for _, e := range w.all {
			a.Observe(e)
		}
	}
}

// mk appends an entry on a node. opts may mutate it; it is then re-signed and
// the node's appender is repointed at the result, so later entries chain.
func (w *world) mk(nodeName string, kind Kind, actor Actor, body map[string]any, opts ...func(*Entry)) *Entry {
	w.t.Helper()
	a := w.app(nodeName)
	if body == nil {
		body = map[string]any{}
	}
	e, err := a.Next(kind, actor, w.spec, body, w.hlc())
	if err != nil {
		w.t.Fatal(err)
	}
	if len(opts) > 0 {
		for _, o := range opts {
			o(e)
		}
		w.resign(nodeName, e)
	}
	w.all = append(w.all, e)
	return e
}

func (w *world) resign(nodeName string, e *Entry) {
	w.t.Helper()
	if err := e.Sign(testNodeSet[nodeName].key); err != nil {
		w.t.Fatal(err)
	}
	id, _ := e.ID()
	a := w.apps[nodeName]
	if e.Seq == a.seq {
		a.prev = id
		a.heads[e.Log] = Dep{Seq: e.Seq, ID: id}
	}
}

func (w *world) certs() [][]byte {
	var out [][]byte
	for _, n := range testNodeSet {
		out = append(out, n.der)
	}
	return out
}

func (w *world) params() Params {
	return Params{Room: w.room, Certs: w.certs(), Binding: w.binding}
}

// eval evaluates the whole world (or the entries given).
func (w *world) eval(entries ...*Entry) *Outcome {
	if len(entries) == 0 {
		entries = w.all
	}
	return Evaluate(w.params(), entries)
}

func (w *world) createItem(nodeName string, actor Actor, extra map[string]any, opts ...func(*Entry)) (*Entry, string) {
	a := w.app(nodeName)
	id, err := ItemID(testNodeSet[nodeName].dev, a.seq+1)
	if err != nil {
		w.t.Fatal(err)
	}
	body := map[string]any{"id": id, "text": "an item"}
	for k, v := range extra {
		body[k] = v
	}
	return w.mk(nodeName, KindItemCreate, actor, body, opts...), id
}

func (w *world) line(nodeName string, actor Actor, text string, to ...string) *Entry {
	t := []any{}
	for _, x := range to {
		t = append(t, x)
	}
	return w.mk(nodeName, KindLine, actor, map[string]any{"text": text, "to": t})
}

func (w *world) expect(o *Outcome, e *Entry, st Status, code string) {
	w.t.Helper()
	id, _ := e.ID()
	v, ok := o.Verdict(id)
	if !ok {
		w.t.Fatalf("no verdict for %s %s seq %d", e.Kind, e.Actor, e.Seq)
	}
	if v.Status != st || v.Code != code {
		w.t.Fatalf("%s by %s (seq %d): got %s/%q, want %s/%q", e.Kind, e.Actor, e.Seq, v.Status, v.Code, st, code)
	}
}

func (w *world) accepted(o *Outcome, e *Entry) { w.t.Helper(); w.expect(o, e, StatusAccepted, "") }
func (w *world) rejected(o *Outcome, e *Entry, code string) {
	w.t.Helper()
	w.expect(o, e, StatusRejected, code)
}

func flipS(e *Entry) *Entry {
	c := *e
	raw := []byte(e.Sig)
	r, s := raw[:64], raw[64:]
	sv, _ := new(big.Int).SetString(string(s), 16)
	ns := new(big.Int).Sub(curveN, sv)
	c.Sig = string(r) + fmt.Sprintf("%064x", ns)
	return &c
}

var (
	chaz  = Actor{Person: "chaz"}
	vroku = Actor{Person: "vroku"}
	guest = Actor{Person: "guest1"}
	cog   = Actor{Agent: "cog@darkstar"}
	sage  = Actor{Agent: "sage@eclipse"}
)
