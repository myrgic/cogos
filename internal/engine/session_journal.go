// session_journal.go — the kernel writes the session record where it drives it.
//
// WHY: the kernel is the ACP *client* for every managed session, so it already
// observes every relayed event (ACP session/update, permission request/
// resolution, turn start/end, lifecycle state). Historically the observatory's
// turn log was reconstructed *afterwards* by a Python observer reading whatever
// a harness left behind (adapter-shaped: a Hermes state.db, a Claude journal, a
// driver directory). This file writes ONE bounded JSON line per relayed event,
// synchronously with the drive, into a node directory:
//
//	<workspace>/.cog/run/sessions/<session>.jsonl
//
// Path rationale: the kernel already keeps its append-only turn ledger at
// <workspace>/.cog/run/turns/<session>.jsonl (see the kernel-turns observer).
// Keeping the per-session ACP record under the same <workspace>/.cog/run root,
// in sessions/, gives both kernel-authored records one home — and roots the
// journal at the WORKSPACE, so every workspace keeps its own journal and the
// observatory's cog://workspace/.cog/run/sessions/ ingest source resolves to
// exactly what the kernel wrote. One file per session because the
// ACP session id is the natural key and the file is append-only. Override the
// directory with COGOS_SESSION_JOURNAL_DIR.
//
// Line shape (bounded, redacted to exactly the GUARANTEE below):
//
//	{"ts":"<RFC3339Nano>","session":"<id>","room":"<room, blank when unbound>",
//	 "actor":"<who the event belongs to>","kind":"turn|message|tool|permission|plan|state|end|gap",
//	 "payload":{...}}
//
// The room key is always present: an unbound session writes it blank, so a
// blank room is deliberate and visible rather than an absent key that is
// indistinguishable from a line written by an older writer. A record that
// follows events dropped by a full queue carries a "gap" count, the way the
// event ring reports its own eviction gap, so the incompleteness is visible.
//
// GUARANTEE — one sentence: every string that reaches a journal line is scrubbed
// before it is written, at the record choke point (writeRecord → journalScrubValue
// → journalScrubTextDepth) and again for text fields (journalTruncateText →
// journalScrubText), so a caller cannot add a field that reaches disk unscrubbed.
//
// What the scrub does: it redacts the value of any assignment whose key name is
// secret-ish (journalSecretKey — the list and its canonicalisation live there),
// in plain (api_key=sk-...), JSON-escaped at any escape depth (\"api_key\":\"sk-...),
// and truncated (\"key\":\"val) form; a text leaf whose first non-space byte is { or [
// is decoded ONCE and walked by key; and a string whose key name or separator is
// itself escaped (\uXXXX, \xNN or %XX) is detected on a normalised copy and then
// dropped whole, since mapping a redaction back through that copy would be
// guesswork and over-redaction is the accepted direction.
//
// KNOWN LIMITS, without which no line is claimed secret-free: detection is
// key-name-driven, so a bare high-entropy value under no secret-ish key name is
// NOT redacted; the assignment must carry a literal = or : or one of those three
// normalised spellings; and an encoding outside those three is not normalised.
//
// SAFETY: the write path is non-blocking (a bounded queue drained by one
// goroutine) and failure-tolerant — a write error is logged and the session
// continues. Long text is truncated; secret-ish key assignments and values are
// redacted under the GUARANTEE above.
package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	sessionJournalMaxText = 2000
	sessionJournalQueue   = 1024

	// journalTextScanDepth bounds how many times a string that is itself JSON
	// is decoded and re-walked by the value walk before the text net is
	// applied, so a double-encoded secret is redacted as JSON without
	// unbounded work.
	journalTextScanDepth = 3

	// SessionJournalDirEnv overrides the default journal directory.
	SessionJournalDirEnv = "COGOS_SESSION_JOURNAL_DIR"
)

// SessionJournal appends one JSON line per relayed session event.
type SessionJournal struct {
	sessionID string
	room      string
	actor     string
	path      string

	mu     sync.Mutex
	file   *os.File
	ch     chan ManagedSessionEvent
	done   chan struct{}
	closed bool
	wg     sync.WaitGroup

	// dropped counts events lost to a full queue since the last written
	// record. Nonzero is reported as "gap" on the next record (and as a
	// synthetic "gap" record when the queue drains), so a dropped event is
	// never silent.
	dropped int
}

// sessionJournalRoot is the workspace root the default journal directory is
// rooted at. Set at boot/serve by ConfigureSessionJournal, before any session
// is driven.
var sessionJournalRoot string

// ConfigureSessionJournal roots the default session-journal directory at the
// workspace root and returns the resolved directory. It is hooked from Boot
// (internal/engine/boot.go, Kernel startup) with cfg.WorkspaceRoot, so a
// kernel that knows its workspace writes <workspace>/.cog/run/sessions — the
// same root the turn ledger uses (turnSidecarPath) and the root the
// observatory's cog://workspace/.cog/run/sessions/ ingest source resolves
// against. COGOS_SESSION_JOURNAL_DIR still overrides everything.
func ConfigureSessionJournal(workspaceRoot string) string {
	if workspaceRoot != "" {
		sessionJournalRoot = workspaceRoot
	}
	return DefaultSessionJournalDir()
}

// DefaultSessionJournalDir returns the directory the journal appends to:
// COGOS_SESSION_JOURNAL_DIR when set, else <workspace>/.cog/run/sessions once
// a workspace root is known (ConfigureSessionJournal), else — only when no
// root is known at all, e.g. the standalone helpers that never boot a kernel —
// <home>/.cog/run/sessions. $HOME is a last resort, never the fallback when a
// workspace root is available. Empty means "no journal".
func DefaultSessionJournalDir() string {
	if d := os.Getenv(SessionJournalDirEnv); d != "" {
		return d
	}
	if r := sessionJournalRoot; r != "" {
		return filepath.Join(r, ".cog", "run", "sessions")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".cog", "run", "sessions")
}

// NewSessionJournal opens (creating if needed) <dir>/<session>.jsonl.
func NewSessionJournal(dir, sessionID, room, actor string) (*SessionJournal, error) {
	if dir == "" || sessionID == "" {
		return nil, fmt.Errorf("session journal: empty dir or session id")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, journalFileName(sessionID)+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	j := &SessionJournal{
		sessionID: sessionID,
		room:      room,
		actor:     actor,
		path:      f.Name(),
		file:      f,
		ch:        make(chan ManagedSessionEvent, sessionJournalQueue),
		done:      make(chan struct{}),
	}
	j.wg.Add(1)
	go j.loop()
	return j, nil
}

// Path returns the file the journal appends to.
func (j *SessionJournal) Path() string { return j.path }

// Write enqueues an event. It never blocks the caller and never returns an
// error: a full queue drops the event and logs, and the session continues.
func (j *SessionJournal) Write(ev ManagedSessionEvent) {
	if j == nil {
		return
	}
	j.mu.Lock()
	closed := j.closed
	j.mu.Unlock()
	if closed {
		return
	}
	select {
	case j.ch <- ev:
	case <-j.done:
	default:
		j.mu.Lock()
		j.dropped++
		j.mu.Unlock()
		log.Printf("session journal: queue full, dropping kind=%s session=%s (gap recorded)", ev.Kind, j.sessionID)
	}
}

// Close drains the queue and closes the file. Idempotent.
func (j *SessionJournal) Close() {
	if j == nil {
		return
	}
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return
	}
	j.closed = true
	close(j.done)
	j.mu.Unlock()
	j.wg.Wait()
	j.mu.Lock()
	if j.file != nil {
		j.file.Close()
		j.file = nil
	}
	j.mu.Unlock()
}

func (j *SessionJournal) loop() {
	defer j.wg.Done()
	for {
		select {
		case ev := <-j.ch:
			j.writeLine(ev)
		case <-j.done:
			for {
				select {
				case ev := <-j.ch:
					j.writeLine(ev)
				default:
					// Any drop that no later record absorbed is announced
					// by its own terminal gap record, so the tail is not
					// silently incomplete either.
					j.mu.Lock()
					dropped := j.dropped
					j.dropped = 0
					j.mu.Unlock()
					if dropped > 0 {
						j.writeGap(dropped)
					}
					return
				}
			}
		}
	}
}

func (j *SessionJournal) writeLine(ev ManagedSessionEvent) {
	kind, payload := journalKindPayload(ev)
	// The room key is always written: blank for an unbound session, so the
	// absence of a binding is explicit and a blank room is never
	// indistinguishable from a line written by an older writer that omitted
	// the key.
	rec := map[string]any{
		"ts":      ev.Time.UTC().Format(time.RFC3339Nano),
		"session": j.sessionID,
		"room":    j.room,
		"kind":    kind,
		"payload": payload,
	}
	if j.actor != "" {
		rec["actor"] = j.actor
	}
	// A record written after dropped events is not contiguous with its
	// predecessor: mark the gap instead of eliding the loss.
	j.mu.Lock()
	if j.dropped > 0 {
		rec["gap"] = j.dropped
		j.dropped = 0
	}
	j.mu.Unlock()
	j.writeRecord(rec)
}

// writeGap writes a synthetic record announcing events a full queue dropped,
// so a gap at the tail of the stream is visible even when no later event
// follows. Its shape matches a normal line (session, room, actor).
func (j *SessionJournal) writeGap(dropped int) {
	rec := map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"session": j.sessionID,
		"room":    j.room,
		"kind":    "gap",
		"payload": map[string]any{"dropped": dropped},
	}
	if j.actor != "" {
		rec["actor"] = j.actor
	}
	j.writeRecord(rec)
}

func (j *SessionJournal) writeRecord(rec map[string]any) {
	// The single choke point of the package-doc GUARANTEE: every string that
	// reaches a line — ts/session/room/actor included — passes through
	// journalScrubValue → journalScrubText here, so a caller cannot add a
	// field that reaches disk unscrubbed.
	if scrubbed, ok := journalScrubValue(rec, 0).(map[string]any); ok {
		rec = scrubbed
	}
	b, err := json.Marshal(rec)
	if err != nil {
		log.Printf("session journal: marshal failed (session continues): %v", err)
		return
	}
	b = append(b, '\n')
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return
	}
	if _, err := j.file.Write(b); err != nil {
		log.Printf("session journal: write failed (session continues): %v", err)
	}
}

// newSessionJournalForSession builds a journal for a driven session, or nil
// when journalling is unavailable (logged, never fatal).
func newSessionJournalForSession(sess *HermesACPSession) *SessionJournal {
	if sess == nil {
		return nil
	}
	dir := DefaultSessionJournalDir()
	if dir == "" {
		return nil
	}
	j, err := NewSessionJournal(dir, sess.id, sess.room, sess.actor)
	if err != nil {
		log.Printf("session journal: disabled for session %s: %v", sess.id, err)
		return nil
	}
	return j
}

// Kinds — the bounded vocabulary, one meaning each:
//
//	turn       a turn started (MSEventTurnStart)
//	message    a user/agent/thought text chunk
//	tool       a tool call or a tool-call update
//	permission a permission request or resolution
//	plan       a plan update
//	state      a managed-session lifecycle transition, NON-terminal (the
//	           routine starting→live fired on every create, restart re-apply)
//	end        a genuine turn or session ending (MSEventTurnEnd)
//	gap        synthetic: N prior events were dropped by a full queue
//
// journalKindPayload maps a relayed ring event onto the bounded observatory
// kind vocabulary and a bounded payload redacted under the package-doc
// GUARANTEE (key-name-driven: a bare high-entropy value under no secret-ish key
// name is the documented limit, not scrubbed).
func journalKindPayload(ev ManagedSessionEvent) (string, any) {
	switch ev.Kind {
	case MSEventTurnStart:
		d := journalDecode(ev.Data)
		return "turn", map[string]any{
			"turn_id": journalStr(d["turn_id"]),
			"text":    journalTruncateText(journalStr(d["prompt"])),
		}
	case MSEventTurnEnd:
		d := journalDecode(ev.Data)
		return "end", map[string]any{
			"turn_id":     journalStr(d["turn_id"]),
			"stop_reason": journalStr(d["stop_reason"]),
			"error":       journalTruncateText(journalStr(d["error"])),
		}
	case MSEventPermissionRequest:
		d := journalDecode(ev.Data)
		params := journalDecode(journalRaw(d["params"]))
		return "permission", map[string]any{
			"event":      "request",
			"request_id": journalStr(d["request_id"]),
			"options":    journalBoundAny(params["options"]),
		}
	case MSEventPermissionResolved:
		d := journalDecode(ev.Data)
		return "permission", map[string]any{
			"event":      "resolved",
			"request_id": journalStr(d["request_id"]),
			"outcome":    journalStr(d["outcome"]),
			"option_id":  journalStr(d["option_id"]),
			"by":         journalStr(d["by"]),
		}
	case MSEventState:
		// A lifecycle transition is not a turn ending. Kind "state" keeps
		// the routine starting→live transition fired on every create
		// (managed_session_hermes.go ~990) and the best-effort restart
		// re-apply failures (~659/664) out of the "end" stream, which is
		// reserved for genuine turn/session endings (MSEventTurnEnd).
		d := journalDecode(ev.Data)
		return "state", map[string]any{
			"state": journalStr(d["state"]),
			"error": journalTruncateText(journalStr(d["error"])),
		}
	case MSEventSessionUpdate:
		return journalSessionUpdate(ev.Data)
	default:
		return ev.Kind, map[string]any{"raw": journalBoundAny(journalDecode(ev.Data))}
	}
}

func journalSessionUpdate(data json.RawMessage) (string, any) {
	u := journalDecode(data)
	variant := journalStr(u["sessionUpdate"])
	switch variant {
	case "agent_message_chunk", "user_message_chunk":
		return "message", map[string]any{"update": variant, "text": journalContentText(u)}
	case "agent_thought_chunk":
		return "message", map[string]any{"update": variant, "thought": journalContentText(u)}
	case "tool_call":
		return "tool", map[string]any{
			"tool":         journalFirstNonEmpty(journalStr(u["title"]), journalStr(u["kind"]), journalStr(u["toolName"])),
			"tool_call_id": journalStr(u["toolCallId"]),
			"summary":      journalSummarizeToolCall(u),
		}
	case "tool_call_update":
		return "tool", map[string]any{
			"tool_call_id": journalStr(u["toolCallId"]),
			"status":       journalStr(u["status"]),
			"summary":      journalSummarizeToolCall(u),
		}
	case "plan":
		return "plan", map[string]any{"entries": journalBoundAny(u["entries"])}
	default:
		if variant == "" {
			variant = "session_update"
		}
		return variant, map[string]any{"raw": journalBoundAny(u)}
	}
}

// -- bounded / redacted serialisation helpers --------------------------------

func journalFileName(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	s := b.String()
	if s == "" || s == "." || s == ".." {
		return "session"
	}
	return s
}

func journalDecode(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]any{"raw": journalTruncateText(string(raw))}
	}
	return m
}

func journalRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

func journalStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func journalFirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// journalTruncateText is the single entry point every text field routes
// through: it redacts secret-ish assignments, decodes a string that is itself
// JSON (bounded), and only then truncates — redaction before the bound, so a
// redaction is never cut in half and a value cannot be truncated into
// visibility. writeRecord scrubs the whole assembled record with
// journalScrubValue for the same reason: a field added later cannot reach a
// line by forgetting to scrub.
func journalTruncateText(s string) string { return journalScrubText(s) }

// journalScrubText is the ONE scrub helper (journalScrubTextDepth at depth 0).
func journalScrubText(s string) string { return journalScrubTextDepth(s, 0) }

func journalScrubTextDepth(s string, depth int) string {
	if s == "" {
		return s
	}
	if depth < journalTextScanDepth {
		if v, ok := journalDecodeTextValue(s); ok {
			if b, err := json.Marshal(journalScrubValue(v, depth+1)); err == nil {
				s = string(b)
			}
		}
	}
	return journalTruncateRunes(journalRedactAssignments(s))
}

// journalDecodeTextValue reports whether s is itself a JSON object or array —
// a double-encoded payload, the shape tool rawInput/rawOutput usually takes —
// and returns the decoded value, so such a payload is redacted by the key walk
// instead of being guessed at by the text net against escaped bytes.
func journalDecodeTextValue(s string) (any, bool) {
	t := strings.TrimSpace(s)
	if len(t) < 2 || (t[0] != '{' && t[0] != '[') {
		return nil, false
	}
	var v any
	if err := json.Unmarshal([]byte(t), &v); err != nil {
		return nil, false
	}
	switch v.(type) {
	case map[string]any, []any:
		return v, true
	}
	return nil, false
}

func journalTruncateRunes(s string) string {
	r := []rune(s)
	if len(r) <= sessionJournalMaxText {
		return s
	}
	return string(r[:sessionJournalMaxText]) + "…"
}

// journalSecretKey reports whether a key name is secret-ish. Detection is
// key-name-driven and this list is the whole net: the names the earlier cuts
// missed (credential, private_key, auth, bearer, pwd, sessionkey) are in it,
// plus passphrase and cookie, and substring matching is loose on purpose —
// over-redaction is safe, an unredacted secret is not.
//
// The name is canonicalised by journalSecretName first, because one credential
// is spelled with '-' in a header and '_' in a config. Lower-casing alone
// matched api_key but not api-key, so x-api-key, api-key, private-key and
// session-key all walked past the net — the miss this round's adversarial test
// pins.
func journalSecretKey(name string) bool {
	n := journalSecretName(name)
	for _, frag := range []string{
		"token", "secret", "password", "passphrase", "pwd", "passwd",
		"authorization", "auth", "bearer", "apikey", "credential",
		"privatekey", "sessionkey", "accesskey", "clientsecret", "cookie",
	} {
		if strings.Contains(n, frag) {
			return true
		}
	}
	return false
}

// journalSecretName canonicalises a key name for matching: lowercase, with the
// separators a name is spelled with (-, _, ., /, :, space) dropped, so
// x-api-key, x_api_key and X-API-KEY all reduce to xapikey and reach the same
// fragment. Dropping a separator can only widen the net, and over-redaction is
// the safe direction.
func journalSecretName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range strings.ToLower(name) {
		switch r {
		case '-', '_', '.', '/', ':', ' ':
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// journalBoundAny marshals v, bounds it to the text bound, redacts secret-ish
// keys, and returns a JSON-safe value. The bounded path is scrubbed like the
// whole path: a cut that lands mid-JSON still yields a string the text net
// inspects, so bounding never becomes a way past the net.
func journalBoundAny(v any) any {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	if len(b) > sessionJournalMaxText {
		trimmed := string(b[:sessionJournalMaxText])
		var out any
		if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
			return journalTruncateText(trimmed) + "…"
		}
		return journalScrubValue(out, 0)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return journalTruncateText(string(b))
	}
	return journalScrubValue(out, 0)
}

// journalScrubValue redacts by key name at any depth and applies the text net
// to every string leaf — including a leaf that is itself JSON, which is decoded
// (bounded by journalTextScanDepth) and redacted as JSON. writeRecord runs it
// over the WHOLE assembled record, so the choke point is the record, not each
// caller's memory to scrub.
func journalScrubValue(v any, depth int) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if journalSecretKey(k) {
				t[k] = "[redacted]"
				continue
			}
			t[k] = journalScrubValue(val, depth)
		}
		return t
	case []any:
		for i := range t {
			t[i] = journalScrubValue(t[i], depth)
		}
		return t
	case string:
		return journalScrubTextDepth(t, depth)
	default:
		return v
	}
}

// journalRedaction is the marker the net substitutes for a value. It doubles as
// the guard against cutting an already-redacted value a second time: the net
// runs once when the payload is built and again at the record choke point, and
// re-cutting the marker left a stray bracket behind.
const journalRedaction = "[redacted]"

// journalRedactAssignments is the text net: a secret-ish key name followed by
// '=' or ':' has its value replaced with journalRedaction. It covers the plain
// form (API_KEY=sk-live-...), the quoted form ("api_key":"sk-..."), the
// JSON-escaped form (\"api_key\":\"sk-...\") at any escape depth, and the
// truncated \"api_key\":\"sk-live shape with no closing quote. The value runs to
// its closing quote when quoted and to the end of the line when not, so a
// multi-token value (Authorization: Bearer sk-..., a passphrase with spaces) is
// redacted whole instead of being cut at its first whitespace. It cannot see a
// bare high-entropy value under no secret-ish key name — one of the documented
// limits of the guarantee in the package doc.
func journalRedactAssignments(s string) string {
	if s == "" {
		return s
	}
	out := journalRedactPrecise(s)
	// The key name or its separator can itself be escaped — api\u005fkey\u003dsk-..., auth%5Ftoken%3Dsk-..., api\x5fkey\x3dsk-... —
	// so no literal = or : ever reaches the scanner above and it finds nothing. Detect on a normalised copy and, on a hit,
	// drop the WHOLE string: the policy here is that over-redaction is safe and an unredacted secret is not, and mapping a
	// redaction back through a normalised copy would be guesswork.
	if n := journalNormalizeEscapes(s); n != s && journalRedactPrecise(n) != n {
		return journalRedaction + "(escape-encoded secret-ish assignment)"
	}
	return out
}

// journalNormalizeEscapes decodes the three escape spellings a key name or its separator can wear — \uXXXX, \xNN and
// %XX — so the assignment scanner sees the characters it matches on. DETECTION ONLY: the result is never written, and a
// redaction found in it drops the original string entire.
func journalNormalizeEscapes(s string) string {
	if !strings.ContainsAny(s, `\%`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+6 <= len(s) && (s[i+1] == 'u' || s[i+1] == 'U') {
			if v, err := strconv.ParseUint(s[i+2:i+6], 16, 32); err == nil {
				b.WriteRune(rune(v))
				i += 6
				continue
			}
		}
		if s[i] == '\\' && i+4 <= len(s) && (s[i+1] == 'x' || s[i+1] == 'X') {
			if v, err := strconv.ParseUint(s[i+2:i+4], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 4
				continue
			}
		}
		if s[i] == '%' && i+3 <= len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// journalRedactPrecise is the literal-form scan: it walks the string for a = or : separator, reads the key name before it
// (stepping over escape depth) and replaces the value that follows when the name is secret-ish.
func journalRedactPrecise(s string) string {
	if s == "" {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	last := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '=' && s[i] != ':' {
			continue
		}
		keyStart, ok := journalKeyBefore(s, i)
		if !ok || !journalSecretKey(s[keyStart:i]) {
			continue
		}
		valStart, quoted := journalValueStart(s, i+1)
		if strings.HasPrefix(s[valStart:], journalRedaction) {
			continue
		}
		valEnd := journalValueEnd(s, valStart, quoted)
		if valEnd <= valStart {
			continue
		}
		b.WriteString(s[last:valStart])
		b.WriteString(journalRedaction)
		last = valEnd
		i = valEnd - 1
	}
	b.WriteString(s[last:])
	return b.String()
}

// journalKeyBefore reads the key name that ends just before the separator at
// sep, stepping over an optional closing quote — a plain " or an escaped \" at
// ANY escape depth, so the key in a doubly-encoded \"api_key\" is still read as
// api_key. A single-level step walked out from under the second layer of
// escaping and let the secret through.
func journalKeyBefore(s string, sep int) (int, bool) {
	j := sep
	for j > 0 && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	if j > 0 && s[j-1] == '"' {
		j--
		for j > 0 && s[j-1] == '\\' {
			j--
		}
	}
	end := j
	for j > 0 && journalNameByte(s[j-1]) {
		j--
	}
	if j == end {
		return 0, false
	}
	return j, true
}

func journalNameByte(c byte) bool {
	return c == '_' || c == '-' || c == '.' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// journalValueStart skips whitespace and reports where the value starts and
// whether it is quoted — an opening " or an escaped \" at any escape depth. A
// quoted value is then redacted through its closing quote, so a value with
// internal whitespace (Bearer eyJ..., a passphrase) is covered whole.
func journalValueStart(s string, i int) (int, bool) {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	if i < len(s) && s[i] == '"' {
		return i + 1, true
	}
	if i < len(s) && s[i] == '\\' {
		j := i
		for j < len(s) && s[j] == '\\' {
			j++
		}
		if j < len(s) && s[j] == '"' {
			return j + 1, true
		}
	}
	return i, false
}

// journalValueEnd ends the value. Quoted: at its closing quote (plain " or an
// escaped \" at any escape depth) or at the end of the string, the latter being
// the truncated \"key\":\"val shape with no close. Unquoted: at the first hard
// delimiter — whitespace, a line break, a comma, a closing brace/bracket or a
// semicolon — so a multi-token value is redacted whole rather than cut at its
// first word.
func journalValueEnd(s string, i int, quoted bool) int {
	for i < len(s) {
		c := s[i]
		if quoted {
			if c == '"' {
				return i
			}
			if c == '\\' {
				j := i
				for j < len(s) && s[j] == '\\' {
					j++
				}
				if j < len(s) && s[j] == '"' {
					return i
				}
				i = j
				continue
			}
			i++
			continue
		}
		switch c {
		case '\n', '\r', ',', '}', ']', ';', '"':
			return i
		}
		i++
	}
	return i
}

func journalContentText(u map[string]any) string {
	switch c := u["content"].(type) {
	case map[string]any:
		if t, ok := c["text"].(string); ok {
			return journalTruncateText(t)
		}
	case []any:
		var sb strings.Builder
		for _, item := range c {
			if m, ok := item.(map[string]any); ok {
				if t, ok := m["text"].(string); ok {
					sb.WriteString(t)
				}
			}
		}
		return journalTruncateText(sb.String())
	case string:
		return journalTruncateText(c)
	}
	return journalTruncateText(journalStr(u["text"]))
}

func journalSummarizeToolCall(u map[string]any) string {
	for _, key := range []string{"rawOutput", "content", "result"} {
		if v, ok := u[key]; ok {
			if b, err := json.Marshal(journalBoundAny(v)); err == nil {
				return journalTruncateText(strings.Trim(string(b), "\""))
			}
		}
	}
	if v, ok := u["rawInput"]; ok {
		if b, err := json.Marshal(journalBoundAny(v)); err == nil {
			return journalTruncateText(string(b))
		}
	}
	return ""
}
