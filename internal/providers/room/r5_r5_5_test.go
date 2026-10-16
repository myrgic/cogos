package room

import "testing"

// R5-5: invalid UTF-8 and unpaired surrogate escapes decode to U+FFFD, so
// several byte forms share one id. parseStrict must reject them. The valid
// forms must still parse, and the escaped backslash case must not be
// mistaken for a surrogate escape.
func TestBoardR5_R5_5(t *testing.T) {
	bad := map[string]string{
		"invalid utf-8 byte":   "{\"text\":\"a\xffb\"}",
		"lone high surrogate":  `{"text":"\ud83d"}`,
		"lone low surrogate":   `{"text":"\ude00"}`,
		"high then non-low":    `{"text":"\ud83dA"}`,
		"invalid utf-8 in key": "{\"k\xfe\":1}",
	}
	for name, raw := range bad {
		if _, err := CanonicalizeJSON([]byte(raw)); err == nil {
			t.Errorf("%s: accepted %q", name, raw)
		}
	}
	good := map[string]string{
		"valid surrogate pair": `{"text":"😀"}`,
		"escaped backslash":    `{"text":"\\ud83d"}`,
		"plain utf-8":          "{\"text\":\"café\"}",
	}
	for name, raw := range good {
		if _, err := CanonicalizeJSON([]byte(raw)); err != nil {
			t.Errorf("%s: rejected %q: %v", name, raw, err)
		}
	}
}
