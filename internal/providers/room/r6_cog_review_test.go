package room

import (
	"encoding/json"
	"strings"
	"testing"
)

// cog-review on a576d63: resolve() walked e.Deps (a Go map) and returned on the
// first problem, so an entry with one held dep and one bad dep came out held
// or rejected depending on map iteration order. A bad dep must always win.
func TestBoardR6_DepsVerdictIndependentOfMapOrder(t *testing.T) {
	w := newWorld(t, nil)
	b := w.line("node-b", vroku, "seen")
	w.sync()
	var badID string
	x := w.mk("node-a", KindLine, chaz, map[string]any{"text": "x", "to": []any{}}, func(e *Entry) {
		// a dep on a node-b seq that exists, under the wrong id: deps_bad
		d := e.Deps[testNodeSet["node-b"].dev]
		badID = strings.Repeat("0", len(idOfEntry(b)))
		d.ID = badID
		e.Deps[testNodeSet["node-b"].dev] = d
		// and a dep on a phone seq nobody holds yet: deps_unheld
		e.Deps[testNodeSet["phone"].dev] = Dep{Seq: 7, ID: strings.Repeat("1", len(idOfEntry(b)))}
	})
	_ = badID
	for i := 0; i < 60; i++ {
		o := w.eval()
		w.rejected(o, x, CodeDepsBad)
	}
}

// cog-review on a576d63: the 2^53-1 integer guard only looked at plain digit
// literals, so an integer-valued number in exponent form passed it.
func TestBoardR6_ExponentIntegersAboveSafeRangeRejected(t *testing.T) {
	for _, lit := range []string{"1e+300", "9.007199254740994e+15", "-1e+20"} {
		if err := exactNumbers(json.Number(lit)); err == nil {
			t.Errorf("number %s accepted, want rejected (above 2^53-1)", lit)
		}
	}
	for _, lit := range []string{"1.5", "9007199254740991", "-3", "0.000001"} {
		if err := exactNumbers(json.Number(lit)); err != nil {
			t.Errorf("number %s rejected: %v", lit, err)
		}
	}
}
