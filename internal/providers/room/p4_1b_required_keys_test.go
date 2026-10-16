package room

import (
	"encoding/json"
	"testing"
)

// P4-1b (R5-6): a key the RFC (6.9.1) makes required must be present on the
// wire. A missing deps and a deps of {} (or a missing prev on a genesis and
// prev:null) parse to the same struct and hash to the same id, so ParseEntry
// must refuse the absent form, not default it.

// dropKey re-encodes raw with one top-level key removed. Other keys keep their
// bytes; only the presence of key changes.
func dropKey(t *testing.T, raw []byte, key string) []byte {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m[key]; !ok {
		t.Fatalf("entry has no %q key to drop", key)
	}
	delete(m, key)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBoardP4_1b_RequiredKeys(t *testing.T) {
	w := newWorld(t, nil)
	line := w.line("node-a", chaz, "hi")
	cases := []struct {
		name string
		e    *Entry
		keys []string
	}{
		{"genesis", w.g, []string{"log", "seq", "prev", "deps", "actor", "kind", "body", "hlc", "sig"}},
		{"line", line, []string{"room", "log", "seq", "prev", "deps", "spec", "actor", "kind", "body", "hlc", "sig"}},
	}
	for _, c := range cases {
		raw, err := json.Marshal(c.e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseEntry(raw); err != nil {
			t.Fatalf("%s: clean entry rejected: %v", c.name, err)
		}
		for _, k := range c.keys {
			mut := dropKey(t, raw, k)
			if _, err := ParseEntry(mut); err == nil {
				t.Errorf("%s: entry without required key %q accepted: %s", c.name, k, mut)
			}
		}
	}
}

// The two keys named by R5-6, pinned on their own so the failure is legible:
// a missing deps is not the same entry as "deps":{}, and a genesis with no
// prev is not the same entry as "prev":null.
func TestBoardP4_1b_MissingDepsAndGenesisPrev(t *testing.T) {
	w := newWorld(t, nil)
	raw, err := json.Marshal(w.g)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseEntry(dropKey(t, raw, "prev")); err == nil {
		t.Errorf("genesis without prev accepted (must be prev:null, not absent)")
	}
	line := w.line("node-a", chaz, "hi")
	raw, err = json.Marshal(line)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseEntry(dropKey(t, raw, "deps")); err == nil {
		t.Errorf("entry without deps accepted (must be deps:{}, not absent)")
	}
}
