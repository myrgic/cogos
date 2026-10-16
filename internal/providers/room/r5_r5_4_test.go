package room

import (
	"encoding/json"
	"strings"
	"testing"
)

// R5-4: omitempty plus ID() over the re-marshaled struct let null or empty
// known fields collapse onto the clean form. attested:null and
// actor.agent:"" must not parse.
func TestBoardR5_R5_4(t *testing.T) {
	w := newWorld(t, nil)
	l := w.line("node-a", chaz, "hi")
	raw, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseEntry(raw); err != nil {
		t.Fatalf("clean entry rejected: %v", err)
	}
	cases := map[string]string{
		"attested null":     strings.Replace(string(raw), `"kind":`, `"attested":null,"kind":`, 1),
		"actor agent empty": strings.Replace(string(raw), `"actor":{`, `"actor":{"agent":"",`, 1),
	}
	for name, mut := range cases {
		if mut == string(raw) {
			t.Fatalf("%s: test did not mutate the entry", name)
		}
		if _, err := ParseEntry([]byte(mut)); err == nil {
			t.Fatalf("%s accepted: %s", name, mut)
		}
	}
}
