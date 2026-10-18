package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The tests below each target a leak a verify REPRODUCED against
// 91c100d2 in <workspace>/.cog/run/sessions/<id>.jsonl — a real file, not a toy.
// Each docstring records what reverting the fix does to that test. Together
// they are the pre-fix control: the fail-to-pass run the round-3 do seat used,
// done from this side with git stash (the production file only; these tests are
// untracked, so they stay):
//
//	git stash push -- internal/engine/session_journal.go
//	HOME=/private/tmp/oc-go-home go test ./internal/engine/ \
//	  -run 'SessionJournal_(PromptTextAssignments|DoubleEncodedJSONSecret|WidenedSecretKeyNames|NarrowedGuarantee)' -count=1
//	git stash pop
//
// All four FAIL against the stashed file, each naming the leaked bytes; all
// four pass after journalTruncateText routes through the one scrub helper and
// writeRecord scrubs the whole record. The tests use only the pre-fix symbol
// set on purpose, so the control run compiles against the un-fixed file instead
// of failing to build — a build error would prove nothing about the leak.

// readJournalLinesAt reads the journal file the kernel really writes,
// <dir>/<journalFileName(session)>.jsonl, and returns it raw plus its lines.
func readJournalLinesAt(t *testing.T, dir, session string) (string, []string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, journalFileName(session)+".jsonl"))
	if err != nil {
		t.Fatalf("read journal %s: %v", session, err)
	}
	return string(b), nonEmptyLines(string(b))
}

// TestSessionJournal_PromptTextAssignmentsRedacted — CLASS 1: text fields are
// never scrubbed. Pre-fix, the "text": journalTruncateText(...) call sites
// (MSEventTurnStart's prompt, the message chunk text) rune-truncate only, so a
// pasted env dump is written to the journal verbatim — the leak that matters
// most, because a prompt is the most common carrier. Reverting the fix
// (journalTruncateText back to a rune-only truncate) FAILS this test with the
// leaked byte strings in the failure message.
func TestSessionJournal_PromptTextAssignmentsRedacted(t *testing.T) {
	dir := t.TempDir()
	ring := NewEventRing("sess-leak-text", 8)
	j, err := NewSessionJournal(dir, "sess-leak-text", "room-a", "hermes")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring.SetJournal(j)
	ring.Append(MSEventTurnStart, map[string]any{
		"turn_id": "t1",
		"prompt":  "here is my env dump\nAPI_KEY=sk-live-PROMPTLEAK123\nDB_PASSWORD=hunter2\n",
	})
	ring.Append(MSEventSessionUpdate, map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]any{"type": "text", "text": `echo of the paste: "api_key":"sk-live-MESSAGELEAK456"`},
	})
	j.Close()

	raw, lines := readJournalLinesAt(t, dir, "sess-leak-text")
	if len(lines) != 2 {
		t.Fatalf("want 2 journal lines, got %d:\n%s", len(lines), raw)
	}
	for _, leak := range []string{"sk-live-PROMPTLEAK123", "hunter2", "sk-live-MESSAGELEAK456"} {
		if strings.Contains(raw, leak) {
			t.Fatalf("class 1: a text field leaked %q into the journal:\n%s", leak, raw)
		}
	}
	if !strings.Contains(raw, "[redacted]") {
		t.Fatalf("the assignment was not redacted but dropped or missed; the text net must replace the value:\n%s", raw)
	}
}

// TestSessionJournal_DoubleEncodedJSONSecretRedacted — CLASS 2/3: a secret under
// a secret-ish key INSIDE a JSON string (tool rawInput/rawOutput is often a
// string of JSON, so the key arrives escaped, \"api_key\") is invisible to the
// value walk and is copied through by a text scan with no escaped form. Both
// sizes the round-3 verify reproduced are covered: the small shape, and one
// whose marshalled form crosses sessionJournalMaxText (>2000 bytes) so the
// bounded path — the truncated \"api_key\":\"sk-live shape with no closing quote
// — is exercised too, with the secret asserted to sit INSIDE the bound (or the
// test would prove only that truncation ate it). Reverting the fix FAILS at
// both sizes.
func TestSessionJournal_DoubleEncodedJSONSecretRedacted(t *testing.T) {
	small := `{"api_key":"sk-live-DOUBLELEAKSMALL","note":"` + strings.Repeat("y", 180) + `"}`
	large := `{"api_key":"sk-live-DOUBLELEAKLARGE","note":"` + strings.Repeat("x", 2200) + `"}`

	largeEnc, err := json.Marshal(large)
	if err != nil {
		t.Fatalf("marshal the large double-encoded payload: %v", err)
	}
	if len(largeEnc) <= sessionJournalMaxText {
		t.Fatalf("the large case must cross the text bound to exercise the bounded path: %d bytes", len(largeEnc))
	}
	if idx := strings.Index(string(largeEnc), "sk-live-DOUBLELEAKLARGE"); idx < 0 || idx > sessionJournalMaxText {
		t.Fatalf("the large secret must fall inside the text bound, else the test proves only that truncation dropped it (index %d)", idx)
	}

	dir := t.TempDir()
	ring := NewEventRing("sess-leak-double", 8)
	j, err := NewSessionJournal(dir, "sess-leak-double", "", "hermes")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring.SetJournal(j)
	ring.Append(MSEventSessionUpdate, map[string]any{
		"sessionUpdate": "tool_call", "toolCallId": "c1", "title": "bash", "rawInput": small,
	})
	ring.Append(MSEventSessionUpdate, map[string]any{
		"sessionUpdate": "tool_call", "toolCallId": "c2", "title": "bash", "rawOutput": large,
	})
	j.Close()

	raw, lines := readJournalLinesAt(t, dir, "sess-leak-double")
	if len(lines) != 2 {
		t.Fatalf("want 2 journal lines, got %d:\n%s", len(lines), raw)
	}
	for _, leak := range []string{"sk-live-DOUBLELEAKSMALL", "sk-live-DOUBLELEAKLARGE"} {
		if strings.Contains(raw, leak) {
			t.Fatalf("class 2/3: a double-encoded secret leaked %q into the journal:\n%s", leak, raw)
		}
	}
	if !strings.Contains(raw, "[redacted]") {
		t.Fatalf("the doubled-encoded payload was not redacted by key; a decode-and-walk (or the escaped text net) must catch it:\n%s", raw)
	}
}

// TestSessionJournal_WidenedSecretKeyNamesRedacted — CLASS 4: journalSecretKey
// missed credential, private_key, auth, bearer, pwd and sessionkey, so a value
// under any of them reached the file verbatim. Both channels are covered: the
// value walk (a map under such a key) and the text net (escaped
// \"credential\":\"...\" inside a text field). Reverting the fix FAILS: the
// markers LEAKCRED / LEAKPRIVKEY / LEAKPWD / LEAKBEARER / LEAKSESSKEY appear in
// the journal (LEAKCRED also matches the LEAKCREDTEXT marker of the text
// channel, so both channels are asserted).
func TestSessionJournal_WidenedSecretKeyNamesRedacted(t *testing.T) {
	dir := t.TempDir()
	ring := NewEventRing("sess-leak-keys", 8)
	j, err := NewSessionJournal(dir, "sess-leak-keys", "", "hermes")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring.SetJournal(j)
	ring.Append(MSEventSessionUpdate, map[string]any{
		"sessionUpdate": "tool_call", "toolCallId": "c1", "title": "cat",
		"rawInput": map[string]any{
			"credential":  "LEAKCRED",
			"private_key": "LEAKPRIVKEY",
			"pwd":         "LEAKPWD",
			"bearer":      "LEAKBEARER",
			"sessionkey":  "LEAKSESSKEY",
		},
	})
	ring.Append(MSEventTurnStart, map[string]any{
		"turn_id": "t1",
		"prompt": `config dump: \"credential\":\"LEAKCREDTEXT\", \"private_key\":\"LEAKPRIVTEXT\", ` +
			`pwd=LEAKPWDTEXT, bearer=LEAKBEARTEXT, sessionkey=LEAKSESSKEYTEXT, auth=LEAKAUTHTEXT`,
	})
	j.Close()

	raw, lines := readJournalLinesAt(t, dir, "sess-leak-keys")
	if len(lines) != 2 {
		t.Fatalf("want 2 journal lines, got %d:\n%s", len(lines), raw)
	}
	for _, leak := range []string{"LEAKCRED", "LEAKPRIV", "LEAKPWD", "LEAKBEARER", "LEAKSESSKEY", "LEAKAUTH"} {
		if strings.Contains(raw, leak) {
			t.Fatalf("class 4: a secret under a widened key name leaked %q into the journal:\n%s", leak, raw)
		}
	}
}

// TestSessionJournal_NarrowedGuaranteeMatchesCode asserts the doc's narrowed
// wording is what the code does. Doc side: the package doc no longer claims an
// unbounded "bounded, secret-free" line, it names the limit (key-name-driven
// detection — a bare high-entropy string with no secret-ish key name is NOT
// redacted), and it states the single-entry-point guarantee. Code side, through
// the real write path: a bare high-entropy value under no secret-ish key name
// survives (the documented limit, asserted rather than hidden), while a
// key-carried secret in the same field is redacted. Reverting the fix fails the
// code side (the key-carried secret leaks); the doc side pins the wording so
// the over-claim cannot come back unnoticed.
func TestSessionJournal_NarrowedGuaranteeMatchesCode(t *testing.T) {
	src, err := os.ReadFile("session_journal.go")
	if err != nil {
		t.Fatalf("read session_journal.go for the doc claim: %v", err)
	}
	doc := strings.Join(strings.Fields(docCommentText(string(src))), " ")
	if strings.Contains(doc, "bounded, secret-free") {
		t.Fatalf("the package doc still claims an unbounded \"bounded, secret-free\" line, which the code does not satisfy")
	}
	if !strings.Contains(doc, "KNOWN LIMIT") || !strings.Contains(doc, "bare high-entropy") ||
		!strings.Contains(doc, "no secret-ish key name is NOT redacted") {
		t.Fatalf("the package doc does not NAME the limit of the guarantee (detection is key-name-driven)")
	}
	// The earlier version of this test pinned "passes through the single helper journalScrubText", which the round-4
	// verify showed is not what the code does: writeRecord scrubs via journalScrubValue/journalScrubTextDepth and text
	// fields via journalTruncateText/journalScrubText. So the doc pins the ENFORCEMENT POINTS instead of one helper.
	if !strings.Contains(doc, "record choke point") ||
		!strings.Contains(doc, "cannot add a field that reaches disk unscrubbed") {
		t.Fatalf("the package doc does not state where the scrub is enforced")
	}
	if !strings.Contains(doc, `\uXXXX`) || !strings.Contains(doc, "%XX") {
		t.Fatalf("the package doc does not name the escape spellings it normalises, so its bound is unreadable")
	}

	const bare = "sk-live-AAAABBBBCCCCDDDDEEEEFFFFGGGG" // no secret-ish key name
	dir := t.TempDir()
	ring := NewEventRing("sess-guarantee", 4)
	j, err := NewSessionJournal(dir, "sess-guarantee", "", "hermes")
	if err != nil {
		t.Fatalf("NewSessionJournal: %v", err)
	}
	ring.SetJournal(j)
	ring.Append(MSEventTurnStart, map[string]any{
		"turn_id": "t1",
		"prompt":  "the hash is " + bare + " and api_key=sk-live-LEAKBYKEY",
	})
	j.Close()

	raw, _ := readJournalLinesAt(t, dir, "sess-guarantee")
	if !strings.Contains(raw, bare) {
		t.Fatalf("the doc says a bare high-entropy value under no secret-ish key name is NOT redacted; the code hid it, so the doc and the code disagree:\n%s", raw)
	}
	if strings.Contains(raw, "sk-live-LEAKBYKEY") {
		t.Fatalf("a key-carried secret reached the journal, so the narrowed guarantee is not what the code does:\n%s", raw)
	}
	if !strings.Contains(raw, "api_key=[redacted]") {
		t.Fatalf("the key-carried value must be replaced in place, not dropped:\n%s", raw)
	}
}

// docCommentText returns only the full-line // comments of a Go source file,
// joined, so a test can assert the doc's wording without depending on wrapping.
func docCommentText(src string) string {
	var out []string
	for _, ln := range strings.Split(src, "\n") {
		s := strings.TrimSpace(ln)
		if strings.HasPrefix(s, "//") {
			out = append(out, strings.TrimSpace(strings.TrimPrefix(s, "//")))
		}
	}
	return strings.Join(out, " ")
}

// TestSessionJournal_EscapedKeyOrSeparatorRedacted — CLASS 4: the key name or the
// separator itself escaped, so no literal = or : reaches the scanner. Pre-fix,
// api\u005fkey\u003dsk-LEAK1 (\u005f is _, \u003d is =), the percent form
// auth%5Ftoken%3Dsk-LEAK2 and api\x5fkey\x3dsk-LEAK3 all walked through verbatim —
// the round-4 verify's repros. Reverting journalNormalizeEscapes FAILS this test
// with the leaked bytes named.
func TestSessionJournal_EscapedKeyOrSeparatorRedacted(t *testing.T) {
	for _, in := range []string{
		`api\u005fkey\u003dsk-LEAK1`,
		`auth%5Ftoken%3Dsk-LEAK2`,
		`api\x5fkey\x3dsk-LEAK3`,
		`{"prompt":"api\u005fkey\u003dsk-LEAK4"}`,
	} {
		out := journalRedactAssignments(in)
		if strings.Contains(out, "sk-LEAK") {
			t.Fatalf("escape-encoded assignment leaked: %q -> %q", in, out)
		}
	}
	// A literal assignment is still redacted IN PLACE (value only), not dropped whole.
	out := journalRedactAssignments("api_key=sk-live-abcdef")
	if strings.Contains(out, "sk-live-abcdef") {
		t.Fatalf("literal assignment leaked: %q", out)
	}
	if !strings.HasPrefix(out, "api_key=") {
		t.Fatalf("a literal assignment should redact in place, got %q", out)
	}
}
