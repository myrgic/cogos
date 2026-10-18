package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The scrub net is the whole security claim this file makes (see the package
// doc GUARANTEE in session_journal.go). These tests are the adversarial round
// that tries to break it on the CLASSES rather than on one path: a secret in a
// text field, in escaped/double-encoded JSON, under a key name the list misses,
// in an oversized payload, and in a value the net truncates.

// journalLineFor drives one journal through the ring with the appends given,
// closes it, and returns the whole file as text.
func journalLineFor(t *testing.T, id string, appends func(*EventRing)) string {
	t.Helper()
	dir := t.TempDir()
	j, err := NewSessionJournal(dir, id, "", "")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring := NewEventRing(id, 32)
	ring.SetJournal(j)
	appends(ring)
	j.Close()
	b, err := os.ReadFile(filepath.Join(dir, journalFileName(id)+".jsonl"))
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if strings.TrimSpace(string(b)) == "" {
		t.Fatalf("journal is empty: the probe never reached the file")
	}
	return string(b)
}

// Class: a key name the list still misses. journalSecretKey lowercased the name
// but did not canonicalise separators, so every hyphenated spelling of a
// fragment already in the list leaked — api-key, x-api-key, private-key,
// session-key — along with names outside the list that carry a credential
// (cookie).
func TestSessionJournal_HyphenatedAndMissingSecretKeysAreRedacted(t *testing.T) {
	secrets := []string{
		"sk-HYPHEN-XAPIKEY", "sk-HYPHEN-APIKEY", "sk-HYPHEN-PRIVATEKEY",
		"sk-HYPHEN-SESSIONKEY", "sk-HYPHEN-COOKIE", "sk-HYPHEN-TEXTCURL",
	}
	got := journalLineFor(t, "sess-hyphen", func(ring *EventRing) {
		ring.Append(MSEventSessionUpdate, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "c1",
			"title":         "http_request",
			"rawInput": map[string]any{
				"headers": map[string]any{
					"x-api-key":   "sk-HYPHEN-XAPIKEY",
					"api-key":     "sk-HYPHEN-APIKEY",
					"private-key": "sk-HYPHEN-PRIVATEKEY",
					"session-key": "sk-HYPHEN-SESSIONKEY",
					"cookie":      "session=sk-HYPHEN-COOKIE",
				},
			},
		})
		ring.Append(MSEventSessionUpdate, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": "ran curl --api-key=sk-HYPHEN-TEXTCURL -s https://example.test"},
		})
	})
	for _, s := range secrets {
		if strings.Contains(got, s) {
			t.Errorf("journal leaked %q under a hyphenated (or listed-nowhere) key name — class: key name the list misses:\n%s", s, got)
		}
	}
}

// Class: a truncated value. The value net ended a value at the first whitespace,
// so the credential behind a scheme word survived whole while only the scheme
// word was redacted: "Authorization: Bearer <jwt>" and "Authorization: Basic
// <b64>" both leaked, as did a passphrase with spaces.
func TestSessionJournal_MultiTokenValuesAreRedactedWhole(t *testing.T) {
	secrets := []string{"sk-SCHEME-BEARER", "c2VjcmV0-SCHEME-BASIC", "sk-SCHEME-QUOTED", "sk-SCHEME-PASSPHRASE"}
	got := journalLineFor(t, "sess-scheme", func(ring *EventRing) {
		ring.Append(MSEventSessionUpdate, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content": map[string]any{"type": "text",
				"text": "request failed: Authorization: Bearer sk-SCHEME-BEARER (408)"},
		})
		ring.Append(MSEventTurnEnd, map[string]any{
			"turn_id": "t1",
			"error":   "proxy said: Authorization: Basic c2VjcmV0-SCHEME-BASIC",
		})
		ring.Append(MSEventSessionUpdate, map[string]any{
			"sessionUpdate": "agent_thought_chunk",
			"content": map[string]any{"type": "text",
				"text": `curl -H '{"authorization":"Bearer sk-SCHEME-QUOTED"}' https://example.test`},
		})
		ring.Append(MSEventTurnStart, map[string]any{
			"turn_id": "t2",
			"prompt":  "passphrase: correct horse sk-SCHEME-PASSPHRASE",
		})
	})
	for _, s := range secrets {
		if strings.Contains(got, s) {
			t.Errorf("journal leaked %q: the value was cut at the first whitespace — class: truncated value:\n%s", s, got)
		}
	}
}

// Class: escaped / double-encoded JSON. The key walk stepped over exactly ONE
// backslash before the closing quote, so a second layer of escaping walked out
// from under it and the secret survived; a value with an internal space inside
// an escaped (quoted) form leaked its tail for the same reason.
func TestSessionJournal_NestedEscapedJSONIsRedacted(t *testing.T) {
	inner := `{"api_key":"sk-ESC-TRIPLE"}`
	mid, err := json.Marshal(inner) // "{\"api_key\":\"sk-ESC-TRIPLE\"}"
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	deep, err := json.Marshal(string(mid)) // one layer further down
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := journalLineFor(t, "sess-esc", func(ring *EventRing) {
		ring.Append(MSEventTurnStart, map[string]any{"turn_id": "t1", "prompt": string(deep)})
		ring.Append(MSEventTurnStart, map[string]any{"turn_id": "t2", "prompt": string(mid)})
		ring.Append(MSEventSessionUpdate, map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content": map[string]any{"type": "text",
				"text": `resp {\"api_key\":\"sk-ESC-SPACE TAIL\"}`},
		})
	})
	for _, s := range []string{"sk-ESC-TRIPLE", "sk-ESC-SPACE TAIL"} {
		if strings.Contains(got, s) {
			t.Errorf("journal leaked %q — class: escaped/double-encoded JSON:\n%s", s, got)
		}
	}
}

// Class: an oversized payload. The bound must hold and must not become a way
// past the net: a secret inside the bound is redacted, a secret past the cut is
// not on the line at all.
func TestSessionJournal_OversizedPayloadIsBoundedAndRedacted(t *testing.T) {
	bulk := strings.Repeat("x", 64000)
	got := journalLineFor(t, "sess-over", func(ring *EventRing) {
		ring.Append(MSEventSessionUpdate, map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "c1",
			"title":         "run",
			"rawOutput":     "api_key=sk-OVER-FRONT\n" + bulk + " api_key=sk-OVER-BACK",
		})
	})
	for _, s := range []string{"sk-OVER-FRONT", "sk-OVER-BACK"} {
		if strings.Contains(got, s) {
			t.Errorf("journal leaked %q from an oversized payload — class: oversized payload:\n%.400s", s, got)
		}
	}
	for _, ln := range nonEmptyLines(got) {
		if len(ln) > 16384 {
			t.Errorf("journal line is not bounded: %d bytes", len(ln))
		}
	}
}
