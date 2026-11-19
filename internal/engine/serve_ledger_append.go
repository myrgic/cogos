// serve_ledger_append.go - POST /v1/ledger/append and GET /v1/ledger/entry.
//
// The append-and-read-back mechanism for RFC-kernel-event-ledger-as-
// observation-point (D2): a caller hands the kernel an envelope (source,
// room, content, metadata, observed_at); the kernel appends ONE hash-chained
// event through the existing AppendEvent path (same canonicalization, same
// prior_hash chaining, same .cog/ledger/<bucket>/events.jsonl layout) and
// returns an identifier. GET /v1/ledger/entry fetches the entry back by that
// identifier and recomputes both hashes so the caller can verify, byte for
// byte, that what landed is what it sent, before trusting the archive.
//
// Content addressing follows the content-addressed substrate design (hash
// algorithm: hex SHA-256 of the raw content bytes, via HashEvent -- the same
// function the chain uses). That design (the private corpus's ADR-047) has
// NOT been publicly ported into this repo's docs/adrs/ (which runs 090-104,
// 121; see docs/rfcs/0002-public-corpus-migration.md's inventory, which
// lists ADR-047 as part of the still-private 89-ADR corpus and states a
// privately-accepted ADR ports as public draft, not canonical). cog-review
// (PR #730, 2026-11-19) correctly caught an earlier draft of this file
// citing ADR-047 as "canonical, shipped, in-repo" -- that was not verifiable
// from this repository and is corrected here: cite the mechanism actually
// visible in this tree (HashEvent, AppendEvent, the existing hash-chain
// implementation in ledger.go), not an ADR number this repo cannot resolve.
//
// PROVISIONAL BUCKET CHOICE (pending RR-1): which ledger bucket a room's
// content belongs in is an OPEN question (RR-1). Chaz said "build the
// mechanism first" (via Claude Web, 2026-11-18). So the destination bucket is
// a request field (`bucket`) with a kernel-side default, DefaultLedgerAppendBucket
// = "room-archive". That default is a PROVISIONAL placeholder, not a decision:
// it is a new bucket rather than the real P4-3 `rooms` bucket because `rooms`
// is folded by RoomBoardStore on boot and must stay board.entry-only. Changing
// the default (or routing to the general session ledger) is a one-line change.
// The bucket itself is an ALLOWLIST (see allowedLedgerAppendBuckets below), not
// a denylist -- a denylist of fixed names cannot enumerate the kernel's
// open-ended dynamic bucket namespace (live session IDs, "genesis"), which
// cog-review also caught: an earlier denylist draft let this route splice a
// chain-valid event into any live session's real ledger, or into "genesis".
// The route additionally requires ScopeLedgerAppend (serve_grant_auth.go) --
// an earlier draft had no scope mapping at all, so any live grant of any
// scope could reach it; also caught by cog-review and fixed the same way
// managed-sessions' identical gap (#346 in that file) was fixed before.
package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// DefaultLedgerAppendBucket is the bucket used when a request names none.
// PROVISIONAL pending RR-1; see the file header.
const DefaultLedgerAppendBucket = "room-archive"

// LedgerAppendEventType is the ledger event type of an appended envelope.
const LedgerAppendEventType = "ledger.append"

// maxLedgerAppendBody bounds the request body (content included).
const maxLedgerAppendBody = 8 << 20

// Append/read-back errors, mapped to HTTP statuses by the handlers.
var (
	ErrLedgerAppendInvalid = errors.New("ledger append: invalid request")
	ErrLedgerEntryNotFound = errors.New("ledger entry: not found")
)

// LedgerAppendRequest is the POST /v1/ledger/append body.
type LedgerAppendRequest struct {
	Bucket      string                 `json:"bucket,omitempty"`       // destination bucket; default DefaultLedgerAppendBucket
	SourceURI   string                 `json:"source_uri,omitempty"`   // where the content came from
	RoomID      string                 `json:"room_id,omitempty"`      // room the content belongs to
	Content     *string                `json:"content,omitempty"`      // the content itself (pointer: "" is valid content)
	ContentHash string                 `json:"content_hash,omitempty"` // hex sha256 of content; required if content is omitted
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	ObservedAt  string                 `json:"observed_at,omitempty"` // RFC3339; default now
}

// LedgerAppendResult is the POST response: the identifier for read-back.
type LedgerAppendResult struct {
	Bucket      string `json:"bucket"`
	EventHash   string `json:"event_hash"` // the id: metadata.hash of the appended event
	Seq         int64  `json:"seq"`
	ContentHash string `json:"content_hash"`
}

// LedgerEntryResult is the GET /v1/ledger/entry response.
type LedgerEntryResult struct {
	Bucket           string                 `json:"bucket"`
	EventHash        string                 `json:"event_hash"`
	Seq              int64                  `json:"seq"`
	SourceURI        string                 `json:"source_uri,omitempty"`
	RoomID           string                 `json:"room_id,omitempty"`
	Content          *string                `json:"content,omitempty"`
	ContentHash      string                 `json:"content_hash,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
	ObservedAt       string                 `json:"observed_at,omitempty"`
	EventHashValid   bool                   `json:"event_hash_valid"`   // recomputed chain hash == stored
	ContentHashValid bool                   `json:"content_hash_valid"` // recomputed sha256(content) == stored (true when no content held)
	Verified         bool                   `json:"verified"`           // both of the above
	Errors           []string               `json:"errors,omitempty"`
}

// allowedLedgerAppendBuckets is an ALLOWLIST, not a denylist: this route
// appends an attacker/peer-controlled, chain-valid event into whatever
// bucket it resolves to, and AppendEvent's bucket/sessionID namespace is
// shared with an open-ended set of dynamic buckets elsewhere in the kernel
// (live per-conversation sessions keyed by arbitrary session IDs, and the
// literal "genesis" bucket GetHashAlgorithm reads). A denylist of fixed
// names (the prior reservedLedgerBuckets) cannot enumerate that dynamic set,
// so it let a caller splice a fabricated entry into any live session's real
// hash chain, or into "genesis" -- found by cog-review (PR #730, 2026-11-19)
// and confirmed by reading GetHashAlgorithm (ledger.go:334-351). Add a bucket
// here only when a real caller needs it; "room-archive" is the only one
// tonight (stage's archive_room, RFC-kernel-event-ledger-as-observation-point
// D2) and its own name is PROVISIONAL pending RR-1.
var allowedLedgerAppendBuckets = map[string]bool{
	"room-archive": true,
}

// resolveLedgerAppendBucket validates a requested bucket (default applied).
func resolveLedgerAppendBucket(b string) (string, error) {
	if b == "" {
		b = DefaultLedgerAppendBucket
	}
	if pathsafe.SanitizeComponent(b) != b || b == "." || b == ".." {
		return "", fmt.Errorf("%w: bucket %q is not a plain name", ErrLedgerAppendInvalid, b)
	}
	if !allowedLedgerAppendBuckets[b] {
		return "", fmt.Errorf("%w: bucket %q is not allowlisted for this route", ErrLedgerAppendInvalid, b)
	}
	return b, nil
}

func ledgerContentHash(content string) (string, error) {
	return HashEvent([]byte(content), "sha256")
}

// AppendLedgerEnvelope validates req and appends it to its bucket.
func AppendLedgerEnvelope(workspaceRoot string, req LedgerAppendRequest) (*LedgerAppendResult, error) {
	bucket, err := resolveLedgerAppendBucket(req.Bucket)
	if err != nil {
		return nil, err
	}
	if req.Content == nil && req.ContentHash == "" {
		return nil, fmt.Errorf("%w: content or content_hash is required", ErrLedgerAppendInvalid)
	}
	if req.SourceURI == "" && req.RoomID == "" {
		return nil, fmt.Errorf("%w: source_uri or room_id is required", ErrLedgerAppendInvalid)
	}
	contentHash := strings.TrimPrefix(req.ContentHash, "sha256:")
	if req.Content != nil {
		computed, err := ledgerContentHash(*req.Content)
		if err != nil {
			return nil, err
		}
		if contentHash != "" && contentHash != computed {
			return nil, fmt.Errorf("%w: content_hash does not match content", ErrLedgerAppendInvalid)
		}
		contentHash = computed
	}
	observed := req.ObservedAt
	if observed == "" {
		observed = nowISO()
	} else if _, err := time.Parse(time.RFC3339, observed); err != nil {
		return nil, fmt.Errorf("%w: observed_at: %v", ErrLedgerAppendInvalid, err)
	}

	data := map[string]interface{}{
		"bucket":       bucket,
		"content_hash": contentHash,
		"observed_at":  observed,
	}
	if req.SourceURI != "" {
		data["source_uri"] = req.SourceURI
	}
	if req.RoomID != "" {
		data["room"] = req.RoomID
	}
	if req.Content != nil {
		data["content"] = *req.Content
	}
	if len(req.Metadata) > 0 {
		data["metadata"] = req.Metadata
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      LedgerAppendEventType,
			Timestamp: nowISO(),
			SessionID: bucket,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "ledger-append"},
	}
	if err := AppendEvent(workspaceRoot, bucket, env); err != nil {
		return nil, fmt.Errorf("ledger append: %w", err)
	}
	return &LedgerAppendResult{
		Bucket: bucket, EventHash: env.Metadata.Hash, Seq: env.Metadata.Seq, ContentHash: contentHash,
	}, nil
}

// ReadLedgerEntry finds the event with metadata.hash == eventHash in bucket
// and re-verifies it from the bytes on disk. A mismatch is reported in the
// result (Verified=false), never silently accepted.
func ReadLedgerEntry(workspaceRoot, bucket, eventHash string) (*LedgerEntryResult, error) {
	bucket, err := resolveLedgerAppendBucket(bucket)
	if err != nil {
		return nil, err
	}
	if eventHash == "" {
		return nil, fmt.Errorf("%w: hash is required", ErrLedgerAppendInvalid)
	}
	path := filepath.Join(workspaceRoot, ".cog", "ledger", pathsafe.SanitizeComponent(bucket), "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrLedgerEntryNotFound
		}
		return nil, err
	}
	defer f.Close()

	hashAlg := GetHashAlgorithm(workspaceRoot)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), maxLedgerAppendBody+(1<<20))
	for sc.Scan() {
		var env EventEnvelope
		if json.Unmarshal(sc.Bytes(), &env) != nil || env.Metadata.Hash != eventHash {
			continue
		}
		return verifyLedgerEntry(bucket, &env, hashAlg), nil
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return nil, ErrLedgerEntryNotFound
}

func verifyLedgerEntry(bucket string, env *EventEnvelope, hashAlg string) *LedgerEntryResult {
	d := env.HashedPayload.Data
	res := &LedgerEntryResult{
		Bucket: bucket, EventHash: env.Metadata.Hash, Seq: env.Metadata.Seq,
		SourceURI: dataString(d, "source_uri"), RoomID: dataString(d, "room"),
		ContentHash: dataString(d, "content_hash"), ObservedAt: dataString(d, "observed_at"),
	}
	res.Metadata, _ = d["metadata"].(map[string]interface{})
	if c, ok := d["content"].(string); ok {
		res.Content = &c
	}

	if canon, err := CanonicalizeEvent(&env.HashedPayload); err != nil {
		res.Errors = append(res.Errors, "canonicalize: "+err.Error())
	} else if h, err := HashEvent(canon, hashAlg); err != nil {
		res.Errors = append(res.Errors, "hash: "+err.Error())
	} else if h != env.Metadata.Hash {
		res.Errors = append(res.Errors, fmt.Sprintf("event hash mismatch: computed=%s stored=%s", h, env.Metadata.Hash))
	} else {
		res.EventHashValid = true
	}

	res.ContentHashValid = true
	if res.Content != nil {
		if h, err := ledgerContentHash(*res.Content); err != nil || h != res.ContentHash {
			res.ContentHashValid = false
			res.Errors = append(res.Errors, fmt.Sprintf("content hash mismatch: computed=%s stored=%s", h, res.ContentHash))
		}
	}
	res.Verified = res.EventHashValid && res.ContentHashValid
	return res
}

// handleLedgerAppend serves POST /v1/ledger/append.
//
//	200 → LedgerAppendResult   400 → invalid   413 → too large   500 → append failure
func (s *Server) handleLedgerAppend(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req LedgerAppendRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxLedgerAppendBody))
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeLedgerError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeLedgerError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	res, err := AppendLedgerEnvelope(s.cfg.WorkspaceRoot, req)
	if err != nil {
		if errors.Is(err, ErrLedgerAppendInvalid) {
			writeLedgerError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeLedgerError(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(res)
}

// handleLedgerEntry serves GET /v1/ledger/entry?bucket=…&hash=….
//
//	200 → LedgerEntryResult (check "verified")   400 → invalid   404 → not found
func (s *Server) handleLedgerEntry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query()
	res, err := ReadLedgerEntry(s.cfg.WorkspaceRoot, q.Get("bucket"), q.Get("hash"))
	switch {
	case errors.Is(err, ErrLedgerAppendInvalid):
		writeLedgerError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrLedgerEntryNotFound):
		writeLedgerError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeLedgerError(w, http.StatusInternalServerError, err.Error())
	default:
		_ = json.NewEncoder(w).Encode(res)
	}
}
