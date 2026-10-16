package room

import (
	"encoding/json"
	"strings"
	"testing"
)

// R4-4: two byte strings that differ only in a body number the float64 cannot
// hold must not both parse (they would collapse to one value and one id).
func TestBoardR4_R4_4(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "hi")
	l.Body["n"] = json.Number("1")
	raw, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseEntry(raw); err != nil {
		t.Fatalf("exact small integer rejected: %v", err)
	}
	for _, lit := range []string{"9007199254740993", "9007199254740992", "1.0000000000000001", "1e400"} {
		mut := strings.Replace(string(raw), `"n":1`, `"n":`+lit, 1)
		if mut == string(raw) {
			t.Fatal("test did not mutate the entry")
		}
		if _, err := ParseEntry([]byte(mut)); err == nil {
			t.Fatalf("inexact body number %s accepted", lit)
		}
	}
}
