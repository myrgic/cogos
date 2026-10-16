// ledger_query.go — read-side query API for the hash-chained event ledger.
//
// The write side (AppendEvent, CanonicalizeEvent, HashEvent) lives in ledger.go.
// This file exposes filtered reads for MCP tools and HTTP endpoints without
// reimplementing any of the canonicalization or hashing logic.
package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// ErrSessionNotFound is returned when QueryLedger is scoped to a session that
// has no events.jsonl on disk. Handlers map it to HTTP 404.
var ErrSessionNotFound = errors.New("ledger: session not found")

// ErrAfterSeqRequiresSession is returned when after_seq is set without
// session_id. Cross-session sequence numbers are not monotonic so paginating
// by seq only makes sense within a single session.
var ErrAfterSeqRequiresSession = errors.New("ledger: after_seq requires session_id")

// LedgerQuery selects and filters events. All fields are optional.
type LedgerQuery struct {
	SessionID      string // filter to a single session (empty = all sessions)
	EventType      string // exact match, or "prefix.*" wildcard
	AfterSeq       int64  // return events with seq > this (requires SessionID)
	SinceTimestamp string // RFC3339; return events with timestamp >= this
	Room           string // match the event data's "room" field (board events)
	Actor          string // match the event data's "actor" field (board events)
	Limit          int    // default 100, capped at 1000
	VerifyChain    bool   // recompute hashes + validate prior_hash links
}

// LedgerEvent is the public JSON shape for a returned event. It flattens the
// internal EventEnvelope so clients don't need to know about the two-level
// hashed_payload/metadata split. Room and Actor are lifted out of the event's
// data when it carries them (board events carry both, R10c).
type LedgerEvent struct {
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp"`
	SessionID string                 `json:"session_id"`
	Seq       int64                  `json:"seq"`
	Hash      string                 `json:"hash"`
	PriorHash string                 `json:"prior_hash,omitempty"`
	Source    string                 `json:"source,omitempty"`
	Room      string                 `json:"room,omitempty"`
	Actor     string                 `json:"actor,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// LedgerVerification reports the result of optional chain verification.
// When requested=false, only `requested` is meaningful.
type LedgerVerification struct {
	Requested          bool     `json:"requested"`
	TotalChecked       int      `json:"total_checked"`
	Valid              bool     `json:"valid"`
	FirstBrokenSeq     int64    `json:"first_broken_seq,omitempty"`
	FirstBrokenSession string   `json:"first_broken_session,omitempty"`
	Errors             []string `json:"errors,omitempty"`
}

// LedgerQueryResult is the QueryLedger output.
type LedgerQueryResult struct {
	Count        int                `json:"count"`
	Events       []LedgerEvent      `json:"events"`
	NextAfterSeq int64              `json:"next_after_seq,omitempty"`
	Truncated    bool               `json:"truncated"`
	Verification LedgerVerification `json:"verification"`
}

const (
	defaultLedgerLimit = 100
	maxLedgerLimit     = 1000
	ledgerScanBufSize  = 1 << 20 // 1 MiB per line, matches readLastJSONLEntries
)

// QueryLedger reads the hash-chained ledger under workspaceRoot and returns
// events matching q. It never reimplements hashing — verification, when
// requested, recomputes hashes via CanonicalizeEvent + HashEvent.
//
// Ordering:
//   - Single-session query: ascending seq (file order).
//   - Multi-session query: session mtime desc, then intra-session ascending seq.
func QueryLedger(workspaceRoot string, q LedgerQuery) (*LedgerQueryResult, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLedgerLimit
	}
	if limit > maxLedgerLimit {
		limit = maxLedgerLimit
	}

	if q.AfterSeq > 0 && q.SessionID == "" {
		return nil, ErrAfterSeqRequiresSession
	}

	var sinceTime time.Time
	var haveSince bool
	if q.SinceTimestamp != "" {
		ts, err := time.Parse(time.RFC3339, q.SinceTimestamp)
		if err != nil {
			return nil, fmt.Errorf("ledger: parse since_timestamp: %w", err)
		}
		sinceTime = ts
		haveSince = true
	}

	typeMatch, err := compileEventTypeMatcher(q.EventType)
	if err != nil {
		return nil, err
	}

	sessions, err := listLedgerSessions(workspaceRoot, q.SessionID)
	if err != nil {
		return nil, err
	}
	if q.SessionID != "" && len(sessions) == 0 {
		return nil, ErrSessionNotFound
	}

	hashAlg := GetHashAlgorithm(workspaceRoot)

	result := &LedgerQueryResult{
		Events:       []LedgerEvent{},
		Verification: LedgerVerification{Requested: q.VerifyChain},
	}

	chainValid := true

readLoop:
	for _, sid := range sessions {
		path := filepath.Join(workspaceRoot, ".cog", "ledger", sid, "events.jsonl")
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("ledger: open %s: %w", sid, err)
		}

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), ledgerScanBufSize)

		var lastHash string // chain cursor — advances on every parsed line, even filtered-out
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			var env EventEnvelope
			if err := json.Unmarshal(line, &env); err != nil {
				// Malformed line: note it under errors but keep going so the
				// rest of the file remains visible. We can't verify the chain
				// past this point, but we don't want to hide data.
				if q.VerifyChain {
					result.Verification.Errors = append(result.Verification.Errors,
						fmt.Sprintf("session=%s: unmarshal line: %v", sid, err))
					chainValid = false
					if result.Verification.FirstBrokenSession == "" {
						result.Verification.FirstBrokenSession = sid
					}
				}
				continue
			}

			if q.VerifyChain {
				result.Verification.TotalChecked++
				if vErr := verifyEventAgainstChain(&env, lastHash, hashAlg); vErr != nil {
					if chainValid {
						result.Verification.FirstBrokenSeq = env.Metadata.Seq
						result.Verification.FirstBrokenSession = sid
					}
					chainValid = false
					result.Verification.Errors = append(result.Verification.Errors,
						fmt.Sprintf("session=%s seq=%d: %v", sid, env.Metadata.Seq, vErr))
				}
			}

			// Advance the chain cursor regardless of whether this event
			// survives filtering — prior_hash linkage must stay accurate.
			lastHash = env.Metadata.Hash

			// Apply filters.
			if typeMatch != nil && !typeMatch(env.HashedPayload.Type) {
				continue
			}
			if q.Room != "" && dataString(env.HashedPayload.Data, "room") != q.Room {
				continue
			}
			if q.Actor != "" && dataString(env.HashedPayload.Data, "actor") != q.Actor {
				continue
			}
			if q.AfterSeq > 0 && env.Metadata.Seq <= q.AfterSeq {
				continue
			}
			if haveSince {
				ts, err := time.Parse(time.RFC3339, env.HashedPayload.Timestamp)
				if err != nil || ts.Before(sinceTime) {
					continue
				}
			}

			result.Events = append(result.Events, envelopeToLedgerEvent(&env))
			if len(result.Events) >= limit {
				result.Truncated = true
				f.Close()
				break readLoop
			}
		}
		if err := scanner.Err(); err != nil {
			f.Close()
			return nil, fmt.Errorf("ledger: scan %s: %w", sid, err)
		}
		f.Close()
	}

	result.Count = len(result.Events)
	if q.VerifyChain {
		result.Verification.Valid = chainValid
	}
	if q.SessionID != "" && len(result.Events) > 0 {
		result.NextAfterSeq = result.Events[len(result.Events)-1].Seq
	}

	return result, nil
}

// envelopeToLedgerEvent flattens the on-disk envelope into the public shape.
func envelopeToLedgerEvent(env *EventEnvelope) LedgerEvent {
	return LedgerEvent{
		Type:      env.HashedPayload.Type,
		Timestamp: env.HashedPayload.Timestamp,
		SessionID: env.HashedPayload.SessionID,
		Seq:       env.Metadata.Seq,
		Hash:      env.Metadata.Hash,
		PriorHash: env.HashedPayload.PriorHash,
		Source:    env.Metadata.Source,
		Room:      dataString(env.HashedPayload.Data, "room"),
		Actor:     dataString(env.HashedPayload.Data, "actor"),
		Data:      env.HashedPayload.Data,
	}
}

// dataString returns the string value at key in an event's data map, or ""
// when the map is nil or the value is absent/not a string.
func dataString(data map[string]interface{}, key string) string {
	if data == nil {
		return ""
	}
	s, _ := data[key].(string)
	return s
}

// verifyEventAgainstChain recomputes the canonical hash for env and checks
// both that metadata.hash matches and that hashed_payload.prior_hash == the
// previous event's hash (lastHash). Returns nil if the event is valid.
func verifyEventAgainstChain(env *EventEnvelope, lastHash, hashAlg string) error {
	canonical, err := CanonicalizeEvent(&env.HashedPayload)
	if err != nil {
		return fmt.Errorf("canonicalize: %w", err)
	}
	computed, err := HashEvent(canonical, hashAlg)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	if computed != env.Metadata.Hash {
		return fmt.Errorf("hash mismatch: computed=%s stored=%s", computed, env.Metadata.Hash)
	}
	if env.HashedPayload.PriorHash != lastHash {
		return fmt.Errorf("prior_hash mismatch: stored=%s expected=%s",
			env.HashedPayload.PriorHash, lastHash)
	}
	return nil
}

// compileEventTypeMatcher returns a predicate over event type strings.
// An empty pattern matches everything. A trailing ".*" (or bare "*") acts as
// a prefix wildcard; anything else is an exact match.
func compileEventTypeMatcher(pattern string) (func(string) bool, error) {
	if pattern == "" {
		return nil, nil
	}
	if pattern == "*" {
		return func(string) bool { return true }, nil
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		if prefix == "" {
			return func(string) bool { return true }, nil
		}
		// Match "prefix" itself or "prefix.<anything>" — callers expect
		// "attention.*" to catch "attention.boost" and friends.
		return func(t string) bool {
			return t == prefix || strings.HasPrefix(t, prefix+".")
		}, nil
	}
	exact := pattern
	return func(t string) bool { return t == exact }, nil
}

// listLedgerSessions returns the session IDs to scan, in the order to scan
// them. If filter is non-empty, returns just that session (iff its
// events.jsonl exists). Otherwise returns all non-genesis sessions sorted by
// events.jsonl mtime descending — "what happened recently" first.
//
// filter is a caller-supplied session key (e.g. an HTTP query param) that
// may contain characters SanitizeComponent rewrites; the returned ID must
// match the sanitized on-disk directory name — the same form the no-filter
// branch below returns via e.Name() — so downstream filepath.Join callers
// (QueryLedger, QueryToolCalls) build the identical path either way.
func listLedgerSessions(workspaceRoot, filter string) ([]string, error) {
	base := filepath.Join(workspaceRoot, ".cog", "ledger")

	if filter != "" {
		sanitizedFilter := pathsafe.SanitizeComponent(filter)
		eventsPath := filepath.Join(base, sanitizedFilter, "events.jsonl")
		if _, err := os.Stat(eventsPath); err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("ledger: stat %s: %w", filter, err)
		}
		return []string{sanitizedFilter}, nil
	}

	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("ledger: read %s: %w", base, err)
	}

	type sessionMtime struct {
		id    string
		mtime int64
	}
	var sessions []sessionMtime
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if e.Name() == "genesis" {
			continue
		}
		eventsPath := filepath.Join(base, e.Name(), "events.jsonl")
		info, err := os.Stat(eventsPath)
		if err != nil {
			continue
		}
		sessions = append(sessions, sessionMtime{id: e.Name(), mtime: info.ModTime().UnixNano()})
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].mtime > sessions[j].mtime
	})

	out := make([]string, len(sessions))
	for i, s := range sessions {
		out[i] = s.id
	}
	return out, nil
}
