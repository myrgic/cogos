package cogblock

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// legacyCanonicalJSON is the pre-fix canonicalJSON, kept here only as an
// oracle: non-generic values fell through to json.Marshal verbatim.
func legacyCanonicalJSON(v interface{}) ([]byte, error) {
	switch value := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			kj, _ := json.Marshal(k)
			vj, err := legacyCanonicalJSON(value[k])
			if err != nil {
				return nil, err
			}
			parts = append(parts, string(kj)+":"+string(vj))
		}
		return []byte("{" + strings.Join(parts, ",") + "}"), nil
	case []interface{}:
		parts := make([]string, 0, len(value))
		for _, item := range value {
			ij, err := legacyCanonicalJSON(item)
			if err != nil {
				return nil, err
			}
			parts = append(parts, string(ij))
		}
		return []byte("[" + strings.Join(parts, ",") + "]"), nil
	default:
		return json.Marshal(v)
	}
}

// Why this is not a CanonForm bump (review of ac56b0c). RFC-0003 Refinement
// 4 versions the algorithm so that hashes already written stay verifiable.
// The property that protects them: for every input where the legacy
// algorithm was self-consistent (its write-time bytes equal what a verifier
// computes after decoding the stored JSON), the fixed algorithm produces the
// SAME bytes. Every verifiable "rfc8785-v1" hash is therefore unchanged. The
// inputs whose bytes do change are exactly those where legacy write and read
// disagreed, i.e. events no binary could ever verify; there is no valid hash
// to preserve for them, and a "v1" that reproduced their bytes would pin an
// output that is not RFC 8785 (keys unsorted).
func TestCanonicalJSON_FixPreservesEveryVerifiableV1Hash(t *testing.T) {
	type args struct {
		Zeta  int    `json:"zeta"`
		Alpha string `json:"alpha"`
	}
	type sorted struct {
		Alpha string `json:"alpha"`
		Zeta  int    `json:"zeta"`
	}
	var generic interface{}
	if err := json.Unmarshal([]byte(`{"b":[1,2.5,"x",null,true,{"d":1,"c":2}],"a":"<&>"}`), &generic); err != nil {
		t.Fatal(err)
	}
	inputs := map[string]interface{}{
		"generic (decoded JSON)":  generic,
		"string":                  "s",
		"float":                   2.5,
		"nil":                     nil,
		"int":                     int64(42),
		"typed map (Go sorts it)": map[string]int{"zeta": 1, "alpha": 2},
		"struct, fields sorted":   sorted{Alpha: "a", Zeta: 1},
		"struct, fields unsorted": args{Zeta: 1, Alpha: "a"},
		"raw message, unsorted":   json.RawMessage(`{"zeta":1,"alpha":2}`),
		"raw message, sorted":     json.RawMessage(`{"alpha":2,"zeta":1}`),
		"raw message, whitespace": json.RawMessage(`{ "alpha": 2 }`),
		"typed slice of structs":  []args{{Zeta: 2, Alpha: "b"}},
		"nested typed in generic": map[string]interface{}{"inner": map[string]string{"z": "1", "a": "2"}},
	}
	changed, preserved := 0, 0
	for name, val := range inputs {
		payload := map[string]interface{}{"arguments": val}
		legacyWrite, err := legacyCanonicalJSON(payload)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		raw, _ := json.Marshal(payload)
		var decoded interface{}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		legacyRead, _ := legacyCanonicalJSON(decoded)
		fixed, err := canonicalJSON(payload)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if string(fixed) != string(legacyRead) {
			t.Errorf("%s: fixed write bytes %s != verifier bytes %s", name, fixed, legacyRead)
		}
		if string(legacyWrite) == string(legacyRead) {
			preserved++
			if string(fixed) != string(legacyWrite) {
				t.Errorf("%s: legacy v1 was verifiable but its bytes changed:\n new %s\n old %s", name, fixed, legacyWrite)
			}
		} else {
			changed++
		}
	}
	// Guard against a vacuous pass: both classes must be exercised.
	if preserved == 0 || changed == 0 {
		t.Fatalf("preserved=%d changed=%d; both classes must be covered", preserved, changed)
	}
}
