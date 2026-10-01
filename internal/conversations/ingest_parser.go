// ingest_parser.go — streaming parser for the normalized ingest surface.
//
// Handles records conforming to cogos.observatory.conversations/v0.1.
// Records with an unknown schema value are rejected and logged; the parser
// never guesses at schema semantics.
//
// An ingest FILE is a transport artifact (one file per observer run) that may
// contain records from MANY sessions interleaved — the schema carries
// session_id per record precisely for this. The ingestAccumulator therefore
// groups records by composite key "<source>/<session_id>" while consuming
// files, and a session may SPAN files across observer runs (incremental runs
// append later messages for the same session_id). Dedup spans files within a
// session:
//   - When refs contains a "stable_id" key: dedup by that value.
//   - Otherwise: dedup by SHA-256 of "<role>\x00<timestamp>\x00<text>".
//
// Turn UUID is the raw stable_id value when present (e.g. "hermes-cog:1"),
// else the content hash. Internal dedup-key namespacing never leaks into the
// uuid.
//
// Monotonic turn_index is assigned per (source, session_id) when absent from
// the record.
package conversations

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
)

// knownIngestSchemas is the set of schema values this parser speaks.
// Records declaring any other schema value are rejected.
var knownIngestSchemas = map[string]bool{
	"cogos.observatory.conversations/v0.1": true,
}

// validIngestRoles is the allowed set of role values per the schema contract.
var validIngestRoles = map[string]Role{
	"user":       RoleUser,
	"assistant":  RoleAssistant,
	"tool":       RoleTool,
	"system":     RoleSystem,
	"user-draft": RoleUserDraft,
}

// ingestRecord is the decoded form of one line in a normalized ingest JSONL.
type ingestRecord struct {
	Schema       string          `json:"schema"`
	Source       string          `json:"source"`
	SessionID    string          `json:"session_id"`
	SessionTitle string          `json:"session_title,omitempty"`
	TurnIndex    *int            `json:"turn_index,omitempty"` // pointer so we can detect absence
	Role         string          `json:"role"`
	Timestamp    string          `json:"timestamp"`
	Text         string          `json:"text"`
	Identity     string          `json:"identity,omitempty"`
	Refs         json.RawMessage `json:"refs,omitempty"`
}

// ingestRefs is the optional refs object within an ingest record.
type ingestRefs struct {
	StableID  any    `json:"stable_id,omitempty"` // stable per-record id for dedup
	DB        string `json:"db,omitempty"`
	MessageID any    `json:"message_id,omitempty"`
}

// ingestSessionAccum collects the meta + turns for one (source, session_id)
// while files are being consumed.
type ingestSessionAccum struct {
	Meta  SessionMeta
	Turns []Turn

	// seen holds internal dedup keys for this session. The map spans files:
	// incremental observer runs may re-emit records already ingested from an
	// earlier file.
	seen map[string]struct{}

	// added counts turns appended by this accumulator (not seeded from the
	// index). The delta path upserts only sessions with added > 0.
	added int
}

// ingestAccumulator consumes ingest JSONL files and groups records into
// per-session accumulators keyed by "<source>/<session_id>".
type ingestAccumulator struct {
	maxTurnLen int

	// sessions maps composite key → accumulator.
	sessions map[string]*ingestSessionAccum

	// order records first-seen order of session keys for deterministic output.
	order []string

	// RejectedSchemas counts records rejected for an unknown schema value.
	RejectedSchemas int

	// Quarantined counts records routed to the quarantine surface.
	Quarantined int

	// Ontology is the loaded L1+L2 ontology set. When non-nil, records whose
	// component class no mapping speaks are quarantined instead of dropped.
	Ontology *LoadedOntology

	// Quarantine is the quarantine writer. When non-nil, quarantined records
	// are appended to <workspace>/.cog/observatory/quarantine/<source>/.
	Quarantine *QuarantineWriter

	// Coverage accumulates per-source coverage metrics.
	Coverage *CoverageTracker

	// existing, when set, seeds a session the first time this accumulator sees
	// it with the meta + turns already in the index (delta ingest,
	// ingest_watermark.go), so new records append after them and re-emitted
	// records dedup against them instead of becoming duplicate turns.
	existing func(key string) (SessionMeta, []Turn, bool)
}

// newIngestAccumulator returns an empty accumulator.
func newIngestAccumulator(maxTurnLen int) *ingestAccumulator {
	if maxTurnLen <= 0 {
		maxTurnLen = defaultMaxTurnLen
	}
	return &ingestAccumulator{
		maxTurnLen: maxTurnLen,
		sessions:   make(map[string]*ingestSessionAccum),
	}
}

// ConsumeFile streams one ingest JSONL from r, validating and routing each
// record into its session accumulator. r is typically an os.File; it is NOT
// closed — the caller controls open/close.
//
// Behaviour per record:
//   - unknown schema → rejected, logged, counted in RejectedSchemas
//   - missing required fields (source, session_id, role, timestamp, text) → rejected, logged
//   - unknown role → rejected, logged
//   - duplicate (by stable_id or content hash, within session) → silently skipped
//   - turn_index absent → assigned monotonically per session
//   - text longer than maxTurnLen → truncated with " [truncated]" suffix
func (a *ingestAccumulator) ConsumeFile(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, maxScannerTokenSize), maxScannerTokenSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var rec ingestRecord
		if jsonErr := json.Unmarshal(line, &rec); jsonErr != nil {
			// Skip unparseable lines gracefully.
			continue
		}

		// Schema validation — reject and log unknown schemas.
		if !knownIngestSchemas[rec.Schema] {
			log.Printf("conversations/ingest: rejected record with unknown schema %q (source=%q session_id=%q)", rec.Schema, rec.Source, rec.SessionID)
			a.RejectedSchemas++
			continue
		}

		// Required field validation.
		if rec.Source == "" || rec.SessionID == "" || rec.Role == "" || rec.Timestamp == "" || rec.Text == "" {
			log.Printf("conversations/ingest: rejected record missing required fields (source=%q session_id=%q role=%q timestamp=%q text_len=%d)",
				rec.Source, rec.SessionID, rec.Role, rec.Timestamp, len(rec.Text))
			continue
		}

		// Role validation.
		role, roleOK := validIngestRoles[rec.Role]
		if !roleOK {
			log.Printf("conversations/ingest: rejected record with unknown role %q (source=%q session_id=%q)", rec.Role, rec.Source, rec.SessionID)
			continue
		}

		// Draft roles are recognized but are NOT conversation turns: an unsent
		// composer draft is not something the user actually said, so it must
		// never become a session.turn. Route it to the quarantine surface with
		// provenance and count it toward coverage, so the source reaches a
		// fully-accounted, converged state instead of re-surfacing the same
		// records — logging a rejection for each — on every reconcile cycle.
		if role == RoleUserDraft {
			if a.Quarantine != nil {
				prov := QuarantineProvenance{
					Reason:    QuarantineReasonDraftRole,
					Component: "session.turn",
				}
				if qErr := a.Quarantine.WriteRecord(rec.Source, json.RawMessage(line), prov); qErr != nil {
					log.Printf("conversations/ingest: quarantine write error: %v", qErr)
				}
			}
			if a.Coverage != nil {
				a.Coverage.RecordQuarantined(rec.Source, "session.turn")
			}
			a.Quarantined++
			continue
		}

		// ── v0.2 ontology enforcement ────────────────────────────────────────
		// v0.1 records are all session.turn — the schema only carries role +
		// text, which maps cleanly to session.turn. When a loaded mapping is
		// present we set the component class and L3 version tags; when no
		// mapping exists for this source, we quarantine with provenance.
		componentClass := "session.turn" // v0.1 invariant
		ontRef := ""
		mappingRef := ""

		if a.Ontology != nil && a.Ontology.L1 != nil {
			ontRef = a.Ontology.OntologyRef
			mappingRef = a.Ontology.MappingVersionRef(rec.Source)

			if mappingRef == "" {
				// No mapping speaks this source — quarantine with provenance.
				if a.Quarantine != nil {
					prov := QuarantineProvenance{
						Reason:      QuarantineReasonUnmappedComponent,
						Component:   componentClass,
						OntologyRef: ontRef,
						MappingRef:  "",
					}
					if qErr := a.Quarantine.WriteRecord(rec.Source, json.RawMessage(line), prov); qErr != nil {
						log.Printf("conversations/ingest: quarantine write error: %v", qErr)
					}
				}
				if a.Coverage != nil {
					a.Coverage.RecordQuarantined(rec.Source, componentClass)
					a.Coverage.SetRefs(rec.Source, ontRef, "")
				}
				a.Quarantined++
				continue
			}

			// Mapping present — record coverage.
			// Check whether this record matches a degenerate rule in the L2
			// mapping (e.g. role='tool' rows in hermes-statedb mapped to
			// session.turn instead of tool.result per text_tool_degenerate).
			if a.Coverage != nil {
				if a.Ontology.IsDegenerateRecord(rec.Source, rec.Role) {
					a.Coverage.RecordDegenerate(rec.Source)
				} else {
					a.Coverage.RecordMapped(rec.Source)
				}
				a.Coverage.SetRefs(rec.Source, ontRef, mappingRef)
			}
		}
		// ─────────────────────────────────────────────────────────────────────

		key := indexKeyForIngest(rec.Source, rec.SessionID)
		sess, ok := a.sessions[key]
		if !ok {
			sess = &ingestSessionAccum{
				Meta: SessionMeta{
					SessionID: key,
					Source:    rec.Source,
				},
				seen: make(map[string]struct{}),
			}
			if a.existing != nil {
				if meta, turns, found := a.existing(key); found {
					sess.Meta = meta
					sess.Turns = turns
					for _, t := range turns {
						sess.seen[ingestDedupKeyForTurn(t)] = struct{}{}
					}
				}
			}
			a.sessions[key] = sess
			a.order = append(a.order, key)
		}

		// Dedup within the session (spans files).
		turnUUID, dedupKey := ingestRecordID(rec)
		if _, dup := sess.seen[dedupKey]; dup {
			continue
		}
		sess.seen[dedupKey] = struct{}{}

		// Assign monotonic turn_index when absent.
		idx := len(sess.Turns)
		if rec.TurnIndex != nil {
			idx = *rec.TurnIndex
		}

		// Truncate text.
		text := rec.Text
		if len(text) > a.maxTurnLen {
			text = text[:a.maxTurnLen] + " [truncated]"
		}

		ts := parseTimestamp(rec.Timestamp)
		updateTimeBounds(&sess.Meta, ts)

		// Session title / identity propagate from any record that carries them.
		if rec.SessionTitle != "" && sess.Meta.Title == "" {
			sess.Meta.Title = rec.SessionTitle
		}
		if rec.Identity != "" && sess.Meta.Identity == "" {
			sess.Meta.Identity = rec.Identity
		}

		sess.Turns = append(sess.Turns, Turn{
			UUID:            turnUUID,
			SessionID:       key,
			TurnIndex:       idx,
			Role:            role,
			Timestamp:       ts,
			Text:            text,
			Component:       componentClass,
			OntologyVersion: ontRef,
			MappingVersion:  mappingRef,
		})
		sess.Meta.TurnCount = len(sess.Turns)
		sess.added++
	}

	return scanner.Err()
}

// Sessions returns the accumulated sessions in first-seen order.
func (a *ingestAccumulator) Sessions() []*ingestSessionAccum {
	out := make([]*ingestSessionAccum, 0, len(a.order))
	for _, key := range a.order {
		out = append(out, a.sessions[key])
	}
	return out
}

// ingestRecordID returns the turn UUID and the internal dedup key for an
// ingest record.
//
// UUID priority:
//  1. refs.stable_id verbatim (observers emit "<source>:<message_id>",
//     e.g. "hermes-cog:1") — never re-prefixed.
//  2. First 16 hex chars of SHA-256("<role>\x00<timestamp>\x00<text>").
//
// The dedup key carries an internal namespace prefix ("s:" / "h:") so a
// stable_id can never collide with a content hash, but that prefix never
// appears in the UUID.
func ingestRecordID(rec ingestRecord) (uuid string, dedupKey string) {
	if stable := ingestStableID(rec); stable != "" {
		return stable, "s:" + stable
	}

	h := sha256.New()
	h.Write([]byte(rec.Role))
	h.Write([]byte{0})
	h.Write([]byte(rec.Timestamp))
	h.Write([]byte{0})
	h.Write([]byte(rec.Text))
	sum := hex.EncodeToString(h.Sum(nil))[:16]
	return sum, "h:" + sum
}

// ingestDedupKeyForTurn rebuilds the dedup key ingestRecordID produced for an
// already-indexed turn. Its UUID is the stable_id verbatim when the record had
// one, else the 16-hex content hash; the two namespaces cannot be confused
// because stable ids are "<source>:<id>" and never 16 bare hex characters.
func ingestDedupKeyForTurn(t Turn) string {
	if isContentHashUUID(t.UUID) {
		return "h:" + t.UUID
	}
	return "s:" + t.UUID
}

func isContentHashUUID(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ingestStableID extracts refs.stable_id as a string, or "" when absent.
func ingestStableID(rec ingestRecord) string {
	if len(rec.Refs) == 0 {
		return ""
	}
	var refs ingestRefs
	if json.Unmarshal(rec.Refs, &refs) != nil || refs.StableID == nil {
		return ""
	}
	switch v := refs.StableID.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// indexKeyForIngest builds the composite session key used in the index for
// normalized ingest sessions: "<source>/<session_id>".
func indexKeyForIngest(source, sessionID string) string {
	return source + "/" + sessionID
}
