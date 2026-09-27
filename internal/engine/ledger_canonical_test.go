package engine

import (
	"encoding/json"
	"testing"
)

// cogos#614, as a class: every write-side value type that is not one of the
// generic types json.Unmarshal produces must canonicalize to the same bytes
// before and after the JSONL round trip. Exercised through the live ledger
// (AppendEvent, then QueryLedger's chain verification), not just
// canonicalJSON.
func TestLedger_NonGenericDataVerifiesAfterRoundTrip(t *testing.T) {
	type args struct {
		Zeta  int    `json:"zeta"`
		Alpha string `json:"alpha"`
	}
	cases := map[string]interface{}{
		"raw message":  json.RawMessage(`{"zeta":1,"alpha":{"y":2,"b":[3,{"d":4,"c":5}]}}`),
		"struct":       args{Zeta: 1, Alpha: "a"},
		"typed map":    map[string]int{"zeta": 1, "alpha": 2},
		"typed slice":  []args{{Zeta: 2, Alpha: "b"}},
		"nested typed": map[string]interface{}{"inner": map[string]string{"z": "1", "a": "2"}},
		"int":          int64(42),
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			// AppendEvent caches the last event per session id process-wide,
			// so each subtest needs its own id.
			sid := "canon-" + name
			for i := 0; i < 2; i++ { // two links so prior_hash is checked too
				env := &EventEnvelope{HashedPayload: EventPayload{
					Type:      "tool.call",
					Timestamp: "2026-09-27T00:00:0" + string(rune('0'+i)) + "Z",
					SessionID: sid,
					Data:      map[string]interface{}{"arguments": value},
				}}
				if err := AppendEvent(root, sid, env); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			res, err := QueryLedger(root, LedgerQuery{SessionID: sid, VerifyChain: true})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if !res.Verification.Valid || res.Verification.TotalChecked != 2 {
				t.Fatalf("chain did not verify after round trip: checked=%d errors=%v",
					res.Verification.TotalChecked, res.Verification.Errors)
			}
		})
	}
}

// The read path only ever sees generic types; its canonical bytes must be
// exactly what they were before this change, or already-written ledgers
// would stop verifying.
func TestCanonicalJSON_GenericInputUnchanged(t *testing.T) {
	var v interface{}
	if err := json.Unmarshal([]byte(`{"b":[1,2.5,"x",null,true,{"d":1,"c":2}],"a":"<&>"}`), &v); err != nil {
		t.Fatal(err)
	}
	got, err := canonicalJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"a":"\u003c\u0026\u003e","b":[1,2.5,"x",null,true,{"c":2,"d":1}]}`
	if string(got) != want {
		t.Fatalf("canonical bytes changed for generic input:\n got %s\nwant %s", got, want)
	}
}
