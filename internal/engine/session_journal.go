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
//	<state>/sessions/<session>.jsonl
//
// Path rationale: the kernel already keeps its append-only turn ledger at
// <state>/run/turns/<session>.jsonl (see the kernel-turns observer). Keeping
// the per-session ACP record under the same <state>/run root, in sessions/,
// gives both kernel-authored records one home. One file per session because the
// ACP session id is the natural key and the file is append-only. Override the
// directory with COGOS_SESSION_JOURNAL_DIR.
//
// Line shape (bounded, secret-free):
//
//	{"ts":"<RFC3339Nano>","session":"<id>","room":"<room, blank when unbound>",
//	 "actor":"<who the event belongs to>","kind":"turn|message|tool|permission|plan|end|gap",
//	 "payload":{...}}
//
// The room key is always present: an unbound session writes it blank, so a
// blank room is deliberate and visible rather than an absent key that is
// indistinguishable from a line written by an older writer. A record that
// follows events dropped by a full queue carries a "gap" count, the way the
// event ring reports its own eviction gap, so the incompleteness is visible.
//
// SAFETY: the write path is non-blocking (a bounded queue drained by one
// goroutine) and failure-tolerant — a write error is logged and the session
// continues. Long text is truncated; values under secret-ish keys are redacted.
package engine

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	sessionJournalMaxText = 2000
	sessionJournalQueue   = 1024

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

// DefaultSessionJournalDir returns <home>/.cog/run/sessions, or the value of
// COGOS_SESSION_JOURNAL_DIR when set. Empty means "no journal".
func DefaultSessionJournalDir() string {
	if d := os.Getenv(SessionJournalDirEnv); d != "" {
		return d
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

// journalKindPayload maps a relayed ring event onto the bounded observatory
// kind vocabulary and a bounded, secret-free payload.
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
		d := journalDecode(ev.Data)
		return "end", map[string]any{
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

func journalTruncateText(s string) string {
	r := []rune(s)
	if len(r) <= sessionJournalMaxText {
		return s
	}
	return string(r[:sessionJournalMaxText]) + "…"
}

// journalBoundAny marshals v, truncates it to the text bound, redacts
// secret-ish keys, and returns a JSON-safe value.
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
			return trimmed + "…"
		}
		return journalScrubSecrets(out)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return string(b)
	}
	return journalScrubSecrets(out)
}

func journalScrubSecrets(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			lk := strings.ToLower(k)
			if strings.Contains(lk, "token") || strings.Contains(lk, "secret") ||
				strings.Contains(lk, "password") || strings.Contains(lk, "authorization") ||
				strings.Contains(lk, "api_key") || strings.Contains(lk, "apikey") {
				t[k] = "[redacted]"
				continue
			}
			t[k] = journalScrubSecrets(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = journalScrubSecrets(t[i])
		}
		return t
	default:
		return v
	}
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
