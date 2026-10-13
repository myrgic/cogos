// serve_identity_grants.go — kernel-issued identity grants (board task 60,
// chunk 1: "kernel as identity seat"; chunk 2: ledger-backed grants + revoke).
// Design doc: cog://mem/working/2026-07-21-kernel-identity-seat-design (cog
// workspace), "CHUNK 1 VERIFICATION NOTES" section at its bottom.
//
// Operator's ruling that supersedes this design's own §4 chunk-1 scope: "I
// don't want to have to paste anything" (2026-07-21). Chunk 1 as originally
// scoped only migrated the *verification* step; the zero-paste bootstrap UX
// was deferred to chunk 4. That deferral is overridden here — chunk 1 now
// also has to make the token acquirable with no operator action, which is
// why /v1/identity/grants/current exists below (a surface can ask "what is MY
// currently-live grant" without minting a new one every restart). It was a
// gate-exempt GET through 2026-09; ledger L03 made it a POST behind the gate,
// and unauthenticated bootstrap now goes through the vault file only.
//
// Mechanism (per design §3.1-§3.3, chunk-1-sized; §3.2 ledger-first for
// chunk 2):
//
//	POST /v1/identity/grants        — mint a grant for {surface, scope[]}.
//	                                   Idempotent per surface: if a live,
//	                                   unexpired grant already exists for
//	                                   that surface AND its raw token is
//	                                   still held in memory, it is returned
//	                                   again rather than minting a second one
//	                                   (see design §4 chunk-1 verify-teeth
//	                                   item 5 — a restart must not silently
//	                                   invalidate what the operator already
//	                                   has pasted/bootstrapped elsewhere).
//	POST /v1/identity/verify        — {surface, token} -> {valid, scope[],
//	                                   expires_at} | 401. The verification
//	                                   authority for every migrated surface.
//	                                   Verifies by hashing the presented
//	                                   token and comparing against the
//	                                   grant's ledger-recorded integrity
//	                                   hash — works identically whether the
//	                                   grant was minted this boot or
//	                                   reconstructed from the ledger after a
//	                                   restart (chunk 2).
//	POST /v1/identity/grants/{id}/revoke — revoke a live grant by id. Chunk
//	                                   2. Appends identity.grant.revoked to
//	                                   the ledger and drops the grant from
//	                                   the live index immediately; subsequent
//	                                   verifies for that token fail.
//	GET  /v1/identity/grants        — operator-facing inventory. NEVER
//	                                   returns a token value, per design §3.1.
//	POST /v1/identity/grants/current {"surface":"X"} — returns the live grant's
//	                                   token for a named surface, if one
//	                                   exists AND its raw value is still
//	                                   cached in this process's memory. This
//	                                   is chunk 1's zero-paste primitive: a
//	                                   surface that already holds a grant
//	                                   (minted at its own boot) can be asked
//	                                   again by anything that shares the
//	                                   loopback bind, without a second
//	                                   /grants POST re-minting a second
//	                                   credential. Loopback-only threat model
//	                                   throughout (design §3.6); this
//	                                   endpoint returning a live raw token
//	                                   value is exactly as sensitive as
//	                                   /v1/identity/grants' POST response
//	                                   already is, not a new exposure class.
//	                                   Post-restart (chunk 2), a grant
//	                                   reconstructed from the ledger has NO
//	                                   raw token cached (the ledger, per
//	                                   ADR-091's hash-not-value discipline,
//	                                   §3.2, never stores the raw value) —
//	                                   this endpoint 404s in that case
//	                                   rather than lying with an empty
//	                                   string, same "shown once" discipline
//	                                   §3.2 already names.
//
// Chunk 2 — ledger-backed grants (design §3.2, "CHUNK 2 VERIFICATION NOTES"):
// every mint and revoke is a write-ahead ledger event on the kernel's
// existing hash-chained ledger (ledger.go's AppendEvent, same mechanism
// worktree_spawn.go's FilesystemLedgerWriter already uses for its own
// dedicated session bucket) BEFORE the in-memory index is mutated — the
// ledger is the source of truth, the in-memory IdentityGrantRegistry is a
// derived, rebuildable cache. A fresh mint for a not-yet-tracked surface
// writes identity.grant.issued; an explicit revoke writes
// identity.grant.revoked; a re-mint that SUPERSEDES an already-tracked
// surface's grant (scope change, or a lost-raw-token reissue — see
// MintOrReuse's doc comment) writes a single identity.grant.superseded event
// carrying both the retired grant_id and the new grant's full record, rather
// than two independent appends (cog-review finding, PR #472,
// serve_identity_grants.go:302 as of commit de5cbfe — two independently-
// fallible appends left a window where a durable revoke could land with no
// corresponding reissue if the second append failed transiently). Every
// event carries {grant_id, surface, scope, integrity_hash, issued_at/
// expires_at or revoked_at/superseded_at} — NEVER the raw token value
// (ADR-091 §5; same verifyKeyIntegrity hash-not-value pattern
// identity_provider.go already established). A fourth event,
// identity.grant.extended (added for node-root TTL renewal — see
// ExtendGrant and boot_node_root_grant.go's background ticker), advances an
// already-tracked grant's expires_at WITHOUT touching its grant_id, scope, or
// integrity_hash — unlike supersession, extension is not a new credential, it
// is the same one living longer, which is exactly why it gets its own event
// type instead of reusing identity.grant.superseded. On kernel boot,
// RebuildIdentityGrantRegistryFromLedger replays every identity.grant.*
// event in the dedicated "identity-grants" ledger session bucket to
// reconstruct the live grant set — this is what makes a previously-issued
// grant still verify after a kernel restart (the chunk-1-to-2 verify tooth,
// and the fix for both filed chunk-1 lockout notes: the post-merge
// env-pin/restart lockout, since verify no longer depends on in-memory state
// surviving a restart; and the capacity-fill+restart lockout, since revoke
// now frees exactly one surface's slot without requiring a destructive
// full-registry-wiping kernel restart to "reset" the store).
//
// Honest limitation, not a bug: a grant reconstructed from the ledger has no
// raw token in memory (only its hash) — Verify() doesn't need it (hash
// comparison), but a re-mint of the SAME surface+scope after a kernel
// restart cannot honestly hand back the previously-issued raw bytes (the
// kernel deliberately never persisted them). MintOrReuse detects this case
// (tracked, unexpired, same scope, but Token == "") and mints a fresh grant,
// revoking the old grant_id first so the ledger and in-memory state agree
// (see MintOrReuse's doc comment). This only affects a surface that calls
// POST /v1/identity/grants again after a kernel restart; a surface merely
// presenting its already-held token to POST /v1/identity/verify is
// completely unaffected (that's the tooth chunk 2 must pass).
package engine

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// identityGrantsLedgerSession is the dedicated ledger session bucket for
// identity-grant events, following the same "infra events get a fixed
// session id, not a per-request one" convention worktree_spawn.go's
// FilesystemLedgerWriter already established ("worktree-reconciler"). Events
// land at .cog/ledger/identity-grants/events.jsonl.
const identityGrantsLedgerSession = "identity-grants"

// defaultGrantTTL matches nothing in particular yet (rotation cadence is an
// OPEN GATE, design §5.4) — 30 days is a reasonable default that outlives any
// single chat-server.py boot without being "forever."
const defaultGrantTTL = 30 * 24 * time.Hour

// IdentityGrant is the kernel's record of a surface-scoped credential.
// IntegrityHash (sha256 hex of the raw token) is the durable, ledger-backed
// source of truth for verification (design §3.2, chunk 2) — it survives a
// kernel restart via RebuildIdentityGrantRegistryFromLedger. Token is a
// same-boot-cycle-only cache of the raw value: populated when this process
// itself minted the grant, empty ("") when the grant was reconstructed from
// the ledger after a restart (the ledger never stores the raw value, per
// ADR-091 §5 and identity_provider.go's verifyKeyIntegrity precedent).
// Callers must check Token != "" before treating a grant as
// bootstrap-usable (see Current/handleIdentityGrantCurrent); Verify never
// needs Token, only IntegrityHash.
//
// IMMUTABLE AFTER PUBLICATION (registry invariant, cog-review finding, PR
// #551 round 5): once a *IdentityGrant is stored in the registry's
// bySurface/byGrantID maps, its fields must never be mutated in place.
// Verify, VerifyAny, and Current all return that same pointer to callers
// (handleIdentityVerify, handleIdentityGrantCurrent, ...) who read its
// fields AFTER releasing the registry's lock — mutating a published grant
// concurrently with those unsynchronized reads is a data race, caught by
// go test -race, and undefined per the Go memory model regardless. Every
// method that changes a grant's state (ExtendGrant's ExpiresAt bump,
// RecoverToken's Token cache-fill, MintOrReuse's supersession, the ledger
// replay path in applyIdentityGrantLedgerEvent) must instead allocate a NEW
// *IdentityGrant with the updated field(s) and REPLACE the map entries
// under the lock, exactly like MintOrReuse's supersession path already did
// before this note existed. This held accidentally-but-safely before this
// PR because grants were immutable after mint in practice (Revoke deletes
// map entries outright; the old MintOrReuse supersession path already
// allocated fresh); the TTL-renewal ticker this PR adds was what first made
// a mutation-in-place path (the original ExtendGrant) run concurrently with
// live HTTP reads, which is what turned an implicit convention into a
// finding. Next contributor: if you're about to write `grant.SomeField =
// newValue` anywhere in this file, don't — copy the struct, change the
// field on the copy, and store the copy's pointer instead.
type IdentityGrant struct {
	GrantID       string    `json:"grant_id"`
	Surface       string    `json:"surface"`
	Actor         string    `json:"actor,omitempty"` // person this grant acts for ("chaz"); "" = legacy surface grant
	Scope         []string  `json:"scope"`
	Token         string    `json:"-"` // never serialized; empty after ledger rebuild
	IntegrityHash string    `json:"-"` // never serialized; sha256 hex of Token
	IssuedAt      time.Time `json:"issued_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}

func (g *IdentityGrant) expired(now time.Time) bool {
	return now.After(g.ExpiresAt)
}

// IdentityGrantRegistry is the in-memory store, keyed by surface (chunk 1 —
// one live grant per surface at a time, matching the "idempotent reuse on
// restart" verify-tooth). A secondary index by grant_id backs GET-by-id
// lookups (revoke, chunk 2) and by-token backs verify.
//
// workspaceRoot (chunk 2) is where AppendEvent writes identity.grant.*
// events; empty ("") means "no ledger" — mint/revoke still work but stay
// in-memory only, matching chunk 1's original behavior exactly (used by
// existing unit tests that construct the registry directly and don't care
// about ledger durability).
//
// appendEvent is the ledger-write seam every mutation calls through instead
// of calling the package-level AppendEvent directly. It defaults to
// AppendEvent itself (see the constructors below) and exists so a test can
// substitute a failing writer to prove the write-ahead / atomic-supersession
// invariants hold under a mid-sequence append failure (cog-review finding,
// PR #472 — see TestIdentityGrantMint_SupersedeAppendFailureLeavesOldGrantLive).
type IdentityGrantRegistry struct {
	mu            sync.RWMutex
	bySurface     map[string]*IdentityGrant
	byGrantID     map[string]*IdentityGrant
	workspaceRoot string
	appendEvent   identityGrantAppendFunc
}

// identityGrantAppendFunc matches AppendEvent's signature. See
// IdentityGrantRegistry.appendEvent's doc comment.
type identityGrantAppendFunc func(workspaceRoot, sessionID string, envelope *EventEnvelope) error

// NewIdentityGrantRegistry returns an empty, ledger-less registry (in-memory
// only — chunk 1's original behavior, preserved for existing unit tests and
// any caller that doesn't need restart-safety).
func NewIdentityGrantRegistry() *IdentityGrantRegistry {
	return NewIdentityGrantRegistryWithLedger("")
}

// NewIdentityGrantRegistryWithLedger returns an empty registry that appends
// identity.grant.issued/identity.grant.revoked/identity.grant.superseded
// events to the ledger under workspaceRoot on every mint/revoke (chunk 2,
// design §3.2). Pass "" for the same ledger-less behavior as
// NewIdentityGrantRegistry.
func NewIdentityGrantRegistryWithLedger(workspaceRoot string) *IdentityGrantRegistry {
	return &IdentityGrantRegistry{
		bySurface:     make(map[string]*IdentityGrant),
		byGrantID:     make(map[string]*IdentityGrant),
		workspaceRoot: workspaceRoot,
		appendEvent:   AppendEvent,
	}
}

// maxLiveGrantSurfaces bounds the number of distinct surfaces the registry
// will track at once. `surface` is an unauthenticated, caller-supplied
// string with no allowlist (cog-review finding, PR #471 second pass,
// serve_identity_grants.go:118 as of commit 7918055): without a cap, any
// loopback caller could grow bySurface/byGrantID without bound simply by
// varying the surface name on every mint call, eventually exhausting kernel
// memory. This is a coarse safety net appropriate to chunk 1's loopback-only
// threat model — a real allowlist/auth story is chunk 4's job, not a
// retrofit here.
const maxLiveGrantSurfaces = 256

// ErrGrantStoreAtCapacity is returned (wrapped) by MintOrReuse when a mint
// for a not-yet-tracked surface would exceed maxLiveGrantSurfaces, so the
// HTTP handler can map it to a distinct status/error code (429) rather than
// a generic 400.
var ErrGrantStoreAtCapacity = errors.New("grant store at capacity")

// ErrGrantLedgerAppendFailed is returned (wrapped) by MintOrReuse and Revoke
// when the write-ahead ledger append itself fails — a server-side durability
// fault (disk full, fd exhaustion, transient I/O), NOT a caller mistake. The
// HTTP handlers map it to 503 Service Unavailable so a caller/monitor sees a
// retryable infra failure, never a 400 "invalid_request" (mint) or a 404
// "already gone" (revoke) that misdescribes what happened (cog-review
// findings, PR #472 second pass, serve_identity_grants.go:444/:850 as of
// commit 280aa1a). Errors are wrapped with %w on both this sentinel and the
// underlying append error, so errors.Is works against either.
var ErrGrantLedgerAppendFailed = errors.New("identity-grants ledger append failed")

// ErrGrantNotFound is returned by Revoke when grantID names no currently-
// live grant (already revoked, expired-and-evicted, or never existed). It
// exists so the HTTP handler can distinguish "genuinely gone" (404) from a
// ledger-append failure (503) — previously both collapsed into the same
// (nil, false), and a caller trying to kill a leaked credential could be
// told it was already gone while the grant stayed fully live and verifying
// (cog-review finding, PR #472 second pass, serve_identity_grants.go:444 as
// of commit 280aa1a).
var ErrGrantNotFound = errors.New("no live grant with that id")

// MintOrReuse returns the live, unexpired grant for surface if one exists,
// its scope matches the requested scope exactly (as a set), AND its raw
// token is still cached in this process's memory; otherwise mints a fresh
// one with the given scope and TTL, replacing whatever grant (if any)
// previously lived at that surface key.
//
// The scope check matters: reusing a live grant regardless of what scope the
// *current* request asked for would silently echo back a stale, possibly
// broader-or-narrower scope than requested — contradicting this file's own
// "the response never echoes back a broader scope than the caller requested"
// invariant (handleIdentityGrantMint's doc comment, design §4 chunk-1
// verify-tooth #1) the moment a caller re-mints with a *different* scope
// than what's already live. A same-scope re-mint (the restart case, verify
// teeth #5) still reuses the existing token unchanged; a different-scope
// re-mint mints fresh rather than lying about what scope the returned token
// actually carries.
//
// The Token != "" check matters too (chunk 2, new): a grant reconstructed
// from the ledger after a kernel restart has a live, correctly-scoped entry
// in bySurface but NO cached raw token (the ledger never stores it — design
// §3.2). Reusing such an entry would either panic on an empty string or,
// worse, silently hand back "" as if it were a real credential. Instead this
// falls through to the mint-fresh path below, same as a scope change.
//
// Bounding: a mint for a surface NOT already tracked first evicts any
// expired entries (reclaiming space before growing), then rejects with an
// error if the store is still at maxLiveGrantSurfaces — this caps unbounded
// growth from an unauthenticated caller varying the surface string (cog-
// review finding, PR #471 second pass). A re-mint of an ALREADY-tracked
// surface (scope change, or lost-raw-token reissue) never counts against the
// cap, since it doesn't grow the number of distinct surfaces. Chunk 2 closes
// the "capacity-fill + restart" lockout note this way too: revoke (below)
// now frees a surface's slot without needing a destructive full-registry
// restart to reset the store, and a restart no longer silently resets the
// cap at all (ledger replay repopulates the same live set).
//
// byGrantID leak fix: when a re-mint supersedes an existing grant for the
// same surface, the superseded grant's byGrantID entry is deleted before the
// new one is inserted — otherwise byGrantID would grow by one permanently
// unreachable entry per scope-changing re-mint (cog-review finding, PR #471
// second pass, serve_identity_grants.go:147 as of commit 7918055). Chunk 2
// extends this: the supersession is also recorded in the ledger, so a future
// ledger replay reconstructs the same single-live-grant-per-surface invariant
// instead of resurrecting both the old and new grant_id into byGrantID.
//
// Ledger-write-ahead (design §3.2 / ADR-091 §5): every ledger append below
// happens BEFORE the corresponding in-memory mutation. If an append fails,
// MintOrReuse returns the error without touching bySurface/byGrantID, so the
// in-memory index never claims a grant the ledger doesn't also record.
//
// Atomicity of the supersession append (cog-review finding, PR #472,
// serve_identity_grants.go:302 as of commit de5cbfe): an EARLIER version of
// this method wrote the supersession as two independent, sequential ledger
// appends — identity.grant.revoked for the old grant, then
// identity.grant.issued for the new one — and only rolled back the in-memory
// mutation (not the first, already-durable append) if the SECOND append
// failed. A transient failure isolated to the second append (disk fills up
// between the two writes, fd exhaustion, any I/O hiccup) left the ledger
// durably recording the old grant as revoked while the in-memory index kept
// serving it as live — the old token kept verifying right up until the next
// restart, at which point RebuildIdentityGrantRegistryFromLedger replayed the
// revoke with no corresponding reissue and the surface silently lost a still-
// valid credential. That is the exact lockout class this chunk exists to
// close. The fix is atomicity by construction: appendSupersessionEventLocked
// below performs exactly ONE ledger append carrying both the retired
// grant_id and the new grant's full record (identity.grant.superseded). One
// append has exactly one failure mode — it either lands completely or not at
// all — so there is no window in which the ledger can disagree with itself
// about which grant is live for this surface.
func (r *IdentityGrantRegistry) MintOrReuse(surface string, scope []string, ttl time.Duration) (*IdentityGrant, error) {
	return r.MintOrReuseActor(surface, "", scope, ttl)
}

// MintOrReuseActor is MintOrReuse for a grant that names the person it acts
// for. actor is part of the reuse identity: a live grant is only reused when
// its actor matches too, so re-minting a surface for a different actor (or
// for none) supersedes rather than echoing back a token that carries the
// wrong person. The registry is still keyed by surface (one live grant per
// surface), so two actors on one surface need distinct surface names.
func (r *IdentityGrantRegistry) MintOrReuseActor(surface, actor string, scope []string, ttl time.Duration) (*IdentityGrant, error) {
	if surface == "" {
		return nil, fmt.Errorf("surface is required")
	}
	if actor != "" && !validActorID(actor) {
		return nil, fmt.Errorf("actor must match %s", actorIDPattern)
	}
	now := time.Now().UTC()

	r.mu.Lock()
	defer r.mu.Unlock()

	existing, tracked := r.bySurface[surface]
	if tracked && !existing.expired(now) && existing.Token != "" && existing.Actor == actor && scopeSetEqual(existing.Scope, scope) {
		return existing, nil
	}

	if !tracked {
		r.evictExpiredLocked(now)
		if len(r.bySurface) >= maxLiveGrantSurfaces {
			return nil, fmt.Errorf("%w: grant store at capacity (%d live surfaces); cannot mint a grant for a new surface", ErrGrantStoreAtCapacity, maxLiveGrantSurfaces)
		}
	}

	token, err := mintGrantToken()
	if err != nil {
		return nil, fmt.Errorf("mint token: %w", err)
	}
	if ttl <= 0 {
		ttl = defaultGrantTTL
	}
	grant := &IdentityGrant{
		GrantID:       "grant-" + mustHex(6),
		Surface:       surface,
		Actor:         actor,
		Scope:         scope,
		Token:         token,
		IntegrityHash: sha256Hex(token),
		IssuedAt:      now,
		ExpiresAt:     now.Add(ttl),
	}

	if tracked {
		// Write-ahead, atomic by construction: ONE ledger append carries both
		// the retired grant_id and the new grant's full record, so there is
		// no window between "old revoked" and "new issued" where a transient
		// failure could land the first durably and never the second (cog-
		// review finding, PR #472; see the doc comment above).
		if err := r.appendSupersessionEventLocked(existing, grant, now); err != nil {
			return nil, fmt.Errorf("%w: ledger supersede: %w", ErrGrantLedgerAppendFailed, err)
		}
	} else {
		if err := r.appendGrantEventLocked("identity.grant.issued", grant, now); err != nil {
			return nil, fmt.Errorf("%w: ledger issue: %w", ErrGrantLedgerAppendFailed, err)
		}
	}

	if tracked {
		// Supersede: remove the old grant's byGrantID entry so the index
		// doesn't leak an unreachable entry on every scope-changing re-mint.
		delete(r.byGrantID, existing.GrantID)
	}
	r.bySurface[surface] = grant
	r.byGrantID[grant.GrantID] = grant
	return grant, nil
}

// evictExpiredLocked removes every expired grant from both bySurface and
// byGrantID. Caller must hold r.mu for writing. This is the store's only
// reclamation path for chunk 1 (no ledger, no revoke/rotate yet) — it runs
// opportunistically before a new-surface mint would otherwise grow the
// store, so a churn of short-TTL grants across many surface names doesn't
// accumulate permanently between restarts.
func (r *IdentityGrantRegistry) evictExpiredLocked(now time.Time) {
	for surface, g := range r.bySurface {
		if g.expired(now) {
			delete(r.bySurface, surface)
			delete(r.byGrantID, g.GrantID)
		}
	}
}

// Verify checks a presented token for a named surface against the live
// grant. Returns (grant, true) only when the surface has a live, unexpired
// grant AND the presented token's hash matches the grant's IntegrityHash.
//
// Hashing the presented token and comparing hashes (rather than comparing
// raw token to raw Token, as chunk 1 did) is the chunk-2 change that makes
// verification restart-safe: a grant reconstructed from the ledger has
// IntegrityHash populated but Token == "" (design §3.2), and this is the
// only method that needs to keep working identically in that case — the
// tooth "mint -> restart kernel -> grant still verifies" turns entirely on
// this comparison being hash-based, not raw-value-based.
func (r *IdentityGrantRegistry) Verify(surface, token string) (*IdentityGrant, bool) {
	if surface == "" || token == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.bySurface[surface]
	if !ok || g.expired(time.Now().UTC()) {
		return nil, false
	}
	if !constantTimeEqual(g.IntegrityHash, sha256Hex(token)) {
		return nil, false
	}
	return g, true
}

// VerifyAny checks a presented token against every live grant regardless of
// surface, returning the first match. This is the primitive the write-route
// grant-auth middleware (serve_grant_auth.go) verifies against: that
// middleware's question is "is the caller holding SOME live kernel-issued
// grant", not "is the caller the node-root surface specifically" — a
// dashboard-scoped or mod3-scoped grant satisfies it exactly as well as
// node-root's own. Same hash-based comparison as Verify, so it is equally
// restart-safe (works against ledger-reconstructed grants with no cached raw
// Token). Read-locked, safe for concurrent use on the request hot path;
// maxLiveGrantSurfaces (256) bounds the scan cost.
func (r *IdentityGrantRegistry) VerifyAny(token string) (*IdentityGrant, bool) {
	if token == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now().UTC()
	hash := sha256Hex(token)
	for _, g := range r.bySurface {
		if g.expired(now) {
			continue
		}
		if constantTimeEqual(g.IntegrityHash, hash) {
			return g, true
		}
	}
	return nil, false
}

// RecoverToken re-populates the in-memory raw-token cache for a live,
// ledger-reconstructed grant when the presented token's hash matches the
// grant's IntegrityHash. Used only at boot (ensureNodeRootGrant, see
// boot_node_root_grant.go) to recover a grant whose raw value survived in the
// vault-file fallback without re-minting — re-minting would supersede the
// grant and invalidate it for every OTHER local consumer that already cached
// the previous token, which a routine kernel restart should not do. Returns
// (grant, true) on success; (nil, false) when surface has no live grant, it
// is expired, or the token's hash does not match (vault file stale/corrupt —
// caller should mint fresh in that case, exactly as MintOrReuse would after
// a lost-raw-token restart).
//
// Allocates a NEW *IdentityGrant carrying the recovered token rather than
// mutating the existing one in place — see IdentityGrant's doc comment,
// "IMMUTABLE AFTER PUBLICATION." No ledger write here (recovery isn't a
// state change the ledger needs to know about — the grant's durable record
// already carries this IntegrityHash from its original mint), so this is
// just a map-entry replacement, not a new appendXEventLocked call.
func (r *IdentityGrantRegistry) RecoverToken(surface, token string) (*IdentityGrant, bool) {
	if surface == "" || token == "" {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.bySurface[surface]
	if !ok || g.expired(time.Now().UTC()) {
		return nil, false
	}
	if !constantTimeEqual(g.IntegrityHash, sha256Hex(token)) {
		return nil, false
	}
	recovered := &IdentityGrant{
		GrantID:       g.GrantID,
		Surface:       g.Surface,
		Actor:         g.Actor,
		Scope:         g.Scope,
		Token:         token,
		IntegrityHash: g.IntegrityHash,
		IssuedAt:      g.IssuedAt,
		ExpiresAt:     g.ExpiresAt,
	}
	r.bySurface[surface] = recovered
	r.byGrantID[recovered.GrantID] = recovered
	return recovered, true
}

// Current returns the live grant for a surface (including its raw token) —
// the zero-paste primitive: a surface's own page can ask the kernel "what do
// I currently hold" instead of the operator pasting anything. Returns
// (nil, false) if no live grant exists for that surface, it has expired, OR
// (chunk 2) its raw token isn't cached in this process's memory — a grant
// reconstructed from the ledger after a restart has no raw value to return
// (design §3.2's "shown once" discipline), so this deliberately reports
// "not currently available" rather than handing back an empty string as if
// it were a real credential. The caller (or an operator) should re-mint in
// that case, which MintOrReuse handles correctly (see its doc comment).
func (r *IdentityGrantRegistry) Current(surface string) (*IdentityGrant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.bySurface[surface]
	if !ok || g.expired(time.Now().UTC()) || g.Token == "" {
		return nil, false
	}
	return g, true
}

// GrantExpiry returns the live grant's ExpiresAt for surface, if any.
// Unlike Current (the zero-paste-bootstrap primitive, which deliberately
// reports "unavailable" whenever Token == "" for a ledger-reconstructed
// grant — see its doc comment), this does not require the raw token to be
// cached in memory: it exists for callers that only need to know WHEN a
// live grant expires, not the token itself. The node-root renewal ticker
// (boot_node_root_grant.go's maybeRenewNodeRootGrant) uses this to decide
// whether a renewal is due — extending is conditional on remaining
// lifetime, so it has to know the expiry before deciding whether to call
// ExtendGrant at all. Returns (zero, false) if surface has no live,
// unexpired grant (never minted, already expired, or revoked).
func (r *IdentityGrantRegistry) GrantExpiry(surface string) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.bySurface[surface]
	if !ok || g.expired(time.Now().UTC()) {
		return time.Time{}, false
	}
	return g.ExpiresAt, true
}

// Revoke removes the live grant identified by grantID, appending an
// identity.grant.revoked ledger event before mutating the in-memory index
// (write-ahead, same discipline as MintOrReuse). Returns (grant, nil) on
// success. Returns (nil, ErrGrantNotFound) if grantID names no currently-
// live grant (already revoked, expired-and-evicted, or never existed) — the
// handler maps that to 404. Returns (nil, error wrapping
// ErrGrantLedgerAppendFailed) when the write-ahead append itself fails: the
// grant is then STILL LIVE and its token still verifies, and the caller must
// be told so — collapsing this case into the not-found result (as an earlier
// version did) made the revoke endpoint 404 as if the credential were
// already dead while it kept working, the same error-masking class
// MintOrReuse's supersession fix closed (cog-review finding, PR #472 second
// pass, serve_identity_grants.go:444 as of commit 280aa1a).
// GrantByID returns the live grant identified by grantID, if any — a
// read-only lookup used by handleIdentityGrantRevoke's surface-match check
// (serve_grant_auth.go's file header, "SURFACE-MATCH" section) to learn the
// TARGET grant's surface before deciding whether the presented caller may
// revoke it. Deliberately separate from Revoke itself: the caller needs to
// inspect the target without mutating anything if the surface check is
// going to reject the request.
func (r *IdentityGrantRegistry) GrantByID(grantID string) (*IdentityGrant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.byGrantID[grantID]
	return g, ok
}

func (r *IdentityGrantRegistry) Revoke(grantID string) (*IdentityGrant, error) {
	if grantID == "" {
		return nil, ErrGrantNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	g, ok := r.byGrantID[grantID]
	if !ok {
		return nil, ErrGrantNotFound
	}
	if err := r.appendGrantEventLocked("identity.grant.revoked", g, time.Now().UTC()); err != nil {
		slog.Warn("identity_grants: ledger revoke append failed; grant remains live", "grant_id", grantID, "err", err)
		return nil, fmt.Errorf("%w: ledger revoke: %w", ErrGrantLedgerAppendFailed, err)
	}
	delete(r.byGrantID, grantID)
	// Only clear bySurface if this grant is still the CURRENT live one for
	// its surface — a superseded grant (already replaced by a later mint)
	// no longer occupies bySurface[surface], and revoking its stale
	// grant_id must not accidentally delete whatever surface entry now
	// lives there.
	if current, ok := r.bySurface[g.Surface]; ok && current.GrantID == grantID {
		delete(r.bySurface, g.Surface)
	}
	return g, nil
}

// ExtendGrant extends the live grant for surface by ttl (from now), appending
// an identity.grant.extended ledger event (write-ahead, same discipline as
// MintOrReuse/Revoke) before replacing the live index entry. GrantID,
// Token, and IntegrityHash are left completely unchanged — this is the
// mechanism for board-task-60-followup's node-root TTL renewal
// (boot_node_root_grant.go's background ticker): consumers of a long-lived
// grant (the dashboard, canvas, THESEUS) cache the RAW TOKEN VALUE, so
// renewal must EXTEND the existing credential's expiry rather than mint a
// replacement — a replacement would silently invalidate every cached copy
// the moment it superseded the old grant_id, exactly the churn MintOrReuse's
// own doc comment already documents avoiding on ordinary restarts.
//
// Allocates a NEW *IdentityGrant with the bumped ExpiresAt and REPLACES the
// map entries, rather than mutating the live grant's ExpiresAt field in
// place — see IdentityGrant's doc comment, "IMMUTABLE AFTER PUBLICATION."
// Verify/VerifyAny/Current hand the previous pointer to callers who read its
// fields after this lock is released; mutating it here concurrently with
// those reads was a data race (cog-review finding, PR #551 round 5) that
// only became reachable once this ticker started running against a live
// HTTP server.
//
// ttl <= 0 falls back to defaultGrantTTL, matching MintOrReuse's own
// zero-means-default convention.
//
// If the grant is already expired, this is deliberately NOT a renewal:
// ErrGrantNotFound is returned, same sentinel and same shape as Revoke's
// "no live grant with that id" — an expired grant has already stopped
// verifying, so every consumer holding its token is already 401ing, and
// silently restoring its old expiry with the same token would un-expire a
// credential the kernel itself no longer tracked as live. The documented
// choice (per the build directive) is that the caller falls through to
// MintOrReuse/ensureNodeRootGrant's ordinary mint-or-recover path instead,
// establishing a genuinely fresh grant rather than resurrecting a stale one.
func (r *IdentityGrantRegistry) ExtendGrant(surface string, ttl time.Duration) (*IdentityGrant, error) {
	if surface == "" {
		return nil, fmt.Errorf("surface is required")
	}
	now := time.Now().UTC()
	if ttl <= 0 {
		ttl = defaultGrantTTL
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	g, ok := r.bySurface[surface]
	if !ok || g.expired(now) {
		return nil, ErrGrantNotFound
	}

	newExpiresAt := now.Add(ttl)
	if err := r.appendExtendEventLocked(g, newExpiresAt, now); err != nil {
		return nil, fmt.Errorf("%w: ledger extend: %w", ErrGrantLedgerAppendFailed, err)
	}
	extended := &IdentityGrant{
		GrantID:       g.GrantID,
		Surface:       g.Surface,
		Actor:         g.Actor,
		Scope:         g.Scope,
		Token:         g.Token,
		IntegrityHash: g.IntegrityHash,
		IssuedAt:      g.IssuedAt,
		ExpiresAt:     newExpiresAt,
	}
	r.bySurface[surface] = extended
	r.byGrantID[extended.GrantID] = extended
	return extended, nil
}

// appendExtendEventLocked writes one identity.grant.extended ledger event for
// grant, carrying its (unchanged) grant_id/surface/scope/integrity_hash plus
// the new expires_at, mirroring appendGrantEventLocked's shape exactly so
// RebuildIdentityGrantRegistryFromLedger's replay handling for this event
// type stays trivial (see applyIdentityGrantLedgerEvent's "extended" case).
// Caller must hold r.mu (for writing). No-op when r.workspaceRoot == "",
// same ledger-less behavior as every other appendXEventLocked helper.
func (r *IdentityGrantRegistry) appendExtendEventLocked(grant *IdentityGrant, newExpiresAt, now time.Time) error {
	if r.workspaceRoot == "" {
		return nil
	}
	data := map[string]interface{}{
		"grant_id":       grant.GrantID,
		"surface":        grant.Surface,
		"actor":          grant.Actor,
		"scope":          grant.Scope,
		"integrity_hash": grant.IntegrityHash,
		"issued_at":      grant.IssuedAt.Format(time.RFC3339),
		"expires_at":     newExpiresAt.Format(time.RFC3339),
		"extended_at":    now.Format(time.RFC3339),
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "identity.grant.extended",
			Timestamp: now.Format(time.RFC3339),
			SessionID: identityGrantsLedgerSession,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "identity-grants"},
	}
	return r.appendEvent(r.workspaceRoot, identityGrantsLedgerSession, env)
}

// Snapshot returns every live grant for the operator-facing inventory (GET
// /v1/identity/grants) — never includes Token (design §3.1: "NEVER the
// token value").
func (r *IdentityGrantRegistry) Snapshot() []*IdentityGrant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now().UTC()
	out := make([]*IdentityGrant, 0, len(r.bySurface))
	for _, g := range r.bySurface {
		if g.expired(now) {
			continue
		}
		out = append(out, g)
	}
	return out
}

// appendGrantEventLocked writes one identity.grant.issued/identity.grant.
// revoked ledger event for grant. Caller must hold r.mu (for writing, since
// this is always called from a mutating path). No-op (returns nil) when
// r.workspaceRoot == "" — the ledger-less mode existing unit tests and any
// caller that doesn't want restart-safety rely on (see
// NewIdentityGrantRegistry's doc comment).
//
// The event payload carries {grant_id, surface, scope, integrity_hash,
// issued_at, expires_at} for BOTH event types (not just issued) — per the
// build directive, so RebuildIdentityGrantRegistryFromLedger can reconstruct
// full grant state from a revoked event alone (e.g. to answer "what was this
// grant's scope before it was revoked") without needing to cross-reference
// an already-possibly-evicted issued event. NEVER includes the raw token.
func (r *IdentityGrantRegistry) appendGrantEventLocked(eventType string, grant *IdentityGrant, now time.Time) error {
	if r.workspaceRoot == "" {
		return nil
	}
	data := map[string]interface{}{
		"grant_id":       grant.GrantID,
		"surface":        grant.Surface,
		"actor":          grant.Actor,
		"scope":          grant.Scope,
		"integrity_hash": grant.IntegrityHash,
		"issued_at":      grant.IssuedAt.Format(time.RFC3339),
		"expires_at":     grant.ExpiresAt.Format(time.RFC3339),
	}
	if eventType == "identity.grant.revoked" {
		data["revoked_at"] = now.Format(time.RFC3339)
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      eventType,
			Timestamp: now.Format(time.RFC3339),
			SessionID: identityGrantsLedgerSession,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "identity-grants"},
	}
	return r.appendEvent(r.workspaceRoot, identityGrantsLedgerSession, env)
}

// appendSupersessionEventLocked writes ONE identity.grant.superseded ledger
// event carrying both supersededGrant's id (the grant being retired) and
// grant's full record (the new live grant for the same surface) as a single
// atomic-by-construction append. Caller must hold r.mu (for writing). No-op
// (returns nil) when r.workspaceRoot == "", same ledger-less behavior as
// appendGrantEventLocked.
//
// This is the fix for the cog-review finding on PR #472
// (serve_identity_grants.go:302 as of commit de5cbfe): MintOrReuse's
// supersession path used to make two independent appendGrantEventLocked
// calls (revoke-old, then issue-new), each independently fallible, and only
// rolled back the in-memory mutation if the SECOND one failed — leaving a
// durable, un-reissued revoke on disk if only the first succeeded. Folding
// both into one event removes that window entirely: AppendEvent either
// writes this single line or it doesn't, so MintOrReuse's caller-visible
// contract ("if the append fails, bySurface/byGrantID are untouched") now
// actually holds for the supersession path too.
func (r *IdentityGrantRegistry) appendSupersessionEventLocked(supersededGrant, grant *IdentityGrant, now time.Time) error {
	if r.workspaceRoot == "" {
		return nil
	}
	data := map[string]interface{}{
		"superseded_grant_id": supersededGrant.GrantID,
		"grant_id":            grant.GrantID,
		"surface":             grant.Surface,
		"actor":               grant.Actor,
		"scope":               grant.Scope,
		"integrity_hash":      grant.IntegrityHash,
		"issued_at":           grant.IssuedAt.Format(time.RFC3339),
		"expires_at":          grant.ExpiresAt.Format(time.RFC3339),
		"superseded_at":       now.Format(time.RFC3339),
	}
	env := &EventEnvelope{
		HashedPayload: EventPayload{
			Type:      "identity.grant.superseded",
			Timestamp: now.Format(time.RFC3339),
			SessionID: identityGrantsLedgerSession,
			Data:      data,
		},
		Metadata: EventMetadata{Source: "identity-grants"},
	}
	return r.appendEvent(r.workspaceRoot, identityGrantsLedgerSession, env)
}

// sha256Hex (defined in process.go) is the integrity-hash primitive this
// file uses in place of raw-token comparison (design §3.2, same
// hash-not-value shape identity_provider.go's verifyKeyIntegrity already
// established for resolved key bytes).

// RebuildIdentityGrantRegistryFromLedger reconstructs the live grant set by
// replaying every identity.grant.issued/identity.grant.revoked/identity.
// grant.superseded event from the "identity-grants" ledger session bucket,
// in file order (the ledger is
// append-only, so file order is event order). This is what makes a
// previously-issued grant still verify after a kernel restart — the design's
// chunk-1-to-2 verify tooth — and is called once, from NewServer, in place
// of a bare NewIdentityGrantRegistry() (chunk 1's behavior).
//
// A missing ledger file (no grant has ever been issued in this workspace) is
// not an error — returns an empty, ledger-backed registry so the FIRST mint
// in a fresh workspace still writes forward correctly. A malformed line is
// skipped (matches worktree_spawn.go's scanWorktreeEventsFile precedent:
// "skip malformed rows; do not fail the whole scan") rather than aborting
// the whole rebuild over one corrupt entry.
//
// Reconstructed grants have Token == "" (the ledger never stores the raw
// value) and IntegrityHash populated — sufficient for Verify, insufficient
// for Current/mint-reuse, exactly per this file's documented chunk-2
// semantics.
func RebuildIdentityGrantRegistryFromLedger(workspaceRoot string) (*IdentityGrantRegistry, error) {
	reg := NewIdentityGrantRegistryWithLedger(workspaceRoot)
	if workspaceRoot == "" {
		return reg, nil
	}
	path := filepath.Join(workspaceRoot, ".cog", "ledger", identityGrantsLedgerSession, "events.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		return reg, fmt.Errorf("open identity-grants ledger: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Ledger lines can carry sizeable Data payloads; match the default
	// growth ceiling other ledger scanners in this package tolerate rather
	// than the bufio.Scanner default (64KiB), which is plenty here but kept
	// explicit so a future larger scope list doesn't silently truncate.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var env EventEnvelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			continue // skip malformed rows; do not fail the whole rebuild
		}
		applyIdentityGrantLedgerEvent(reg, &env)
	}
	if err := scanner.Err(); err != nil {
		return reg, fmt.Errorf("scan identity-grants ledger: %w", err)
	}
	return reg, nil
}

// applyIdentityGrantLedgerEvent replays a single ledger event into reg's
// in-memory index. Unlocked mutation directly on the maps is safe here: this
// runs only during RebuildIdentityGrantRegistryFromLedger, before reg is
// published to any concurrent caller.
func applyIdentityGrantLedgerEvent(reg *IdentityGrantRegistry, env *EventEnvelope) {
	data := env.HashedPayload.Data
	grantID, _ := data["grant_id"].(string)
	surface, _ := data["surface"].(string)
	if grantID == "" || surface == "" {
		return
	}

	switch env.HashedPayload.Type {
	case "identity.grant.issued":
		scope := stringSliceFromAny(data["scope"])
		actor, _ := data["actor"].(string)
		integrityHash, _ := data["integrity_hash"].(string)
		issuedAt := parseRFC3339Lenient(data["issued_at"])
		expiresAt := parseRFC3339Lenient(data["expires_at"])
		grant := &IdentityGrant{
			GrantID:       grantID,
			Surface:       surface,
			Actor:         actor,
			Scope:         scope,
			Token:         "", // never persisted — see file header
			IntegrityHash: integrityHash,
			IssuedAt:      issuedAt,
			ExpiresAt:     expiresAt,
		}
		reg.bySurface[surface] = grant
		reg.byGrantID[grantID] = grant
	case "identity.grant.revoked":
		delete(reg.byGrantID, grantID)
		if current, ok := reg.bySurface[surface]; ok && current.GrantID == grantID {
			delete(reg.bySurface, surface)
		}
	case "identity.grant.superseded":
		// One event, two effects: retire supersededGrantID (if any is named
		// — defensively tolerant of an empty value rather than failing the
		// whole replay) exactly like a revoke, AND install grant_id as the
		// new live grant for surface exactly like an issue — atomically,
		// since both effects come from replaying this single ledger line
		// (see appendSupersessionEventLocked's doc comment).
		supersededGrantID, _ := data["superseded_grant_id"].(string)
		scope := stringSliceFromAny(data["scope"])
		actor, _ := data["actor"].(string)
		integrityHash, _ := data["integrity_hash"].(string)
		issuedAt := parseRFC3339Lenient(data["issued_at"])
		expiresAt := parseRFC3339Lenient(data["expires_at"])
		grant := &IdentityGrant{
			GrantID:       grantID,
			Surface:       surface,
			Actor:         actor,
			Scope:         scope,
			Token:         "", // never persisted — see file header
			IntegrityHash: integrityHash,
			IssuedAt:      issuedAt,
			ExpiresAt:     expiresAt,
		}
		if supersededGrantID != "" {
			delete(reg.byGrantID, supersededGrantID)
		}
		reg.bySurface[surface] = grant
		reg.byGrantID[grantID] = grant
	case "identity.grant.extended":
		// Advance ExpiresAt on the already-reconstructed grant — GrantID/
		// Scope/IntegrityHash are unchanged by construction (see
		// ExtendGrant's doc comment), so replay only needs to advance the
		// expiry. Guarded by "is this still the CURRENT live grant for its
		// surface" the same way identity.grant.revoked is, so an extend event
		// for a since-superseded grant_id cannot resurrect stale state into
		// bySurface.
		//
		// Allocates a NEW *IdentityGrant and replaces both map entries rather
		// than mutating the existing one's ExpiresAt field in place — see
		// IdentityGrant's doc comment, "IMMUTABLE AFTER PUBLICATION." This
		// specific call site runs only during RebuildIdentityGrantRegistryFromLedger,
		// before reg is published to any concurrent reader (see this
		// function's own doc comment), so an in-place mutation here was never
		// actually racy — but the invariant is a registry-wide rule, not a
		// per-call-site risk assessment, so this follows the same allocate-
		// and-replace pattern as every other mutation site rather than being
		// the one exception a future refactor could copy-paste from.
		if current, ok := reg.byGrantID[grantID]; ok && current.Surface == surface {
			extended := &IdentityGrant{
				GrantID:       current.GrantID,
				Surface:       current.Surface,
				Actor:         current.Actor,
				Scope:         current.Scope,
				Token:         current.Token, // "" during replay — never persisted, see file header
				IntegrityHash: current.IntegrityHash,
				IssuedAt:      current.IssuedAt,
				ExpiresAt:     parseRFC3339Lenient(data["expires_at"]),
			}
			reg.bySurface[surface] = extended
			reg.byGrantID[grantID] = extended
		}
	}
}

// stringSliceFromAny converts a decoded JSON []interface{} (json.Unmarshal's
// shape for a JSON array into map[string]interface{}) into []string,
// skipping any non-string element rather than failing the whole replay.
func stringSliceFromAny(v interface{}) []string {
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// parseRFC3339Lenient parses a decoded JSON string field as RFC3339,
// returning the zero time.Time on any failure (malformed ledger entry) so
// replay degrades to "already expired" rather than panicking.
func parseRFC3339Lenient(v interface{}) time.Time {
	s, ok := v.(string)
	if !ok {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// scopeSetEqual compares two scope lists as sets (order-independent,
// duplicate-insensitive) — a re-mint request listing the same capabilities
// in a different order is still a "same scope" reuse, not a re-scope.
func scopeSetEqual(a, b []string) bool {
	setA := make(map[string]struct{}, len(a))
	for _, s := range a {
		setA[s] = struct{}{}
	}
	setB := make(map[string]struct{}, len(b))
	for _, s := range b {
		setB[s] = struct{}{}
	}
	if len(setA) != len(setB) {
		return false
	}
	for s := range setA {
		if _, ok := setB[s]; !ok {
			return false
		}
	}
	return true
}

func mintGrantToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func mustHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	const alphabet = "0123456789abcdef"
	out := make([]byte, n*2)
	for i, c := range b {
		out[i*2] = alphabet[c>>4]
		out[i*2+1] = alphabet[c&0x0f]
	}
	return string(out)
}

// constantTimeEqual avoids a timing side-channel on token comparison. Small
// inline helper rather than importing crypto/subtle's ConstantTimeCompare
// for a length-mismatch case, which subtle.ConstantTimeCompare handles
// differently (returns 0 immediately on length mismatch — fine here too,
// but written out for a one-file, no-new-import chunk).
func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

// ─── route registration ──────────────────────────────────────────────────────

// registerIdentityGrantRoutes attaches the /v1/identity/* routes onto mux.
// Called from NewServer alongside the other register*Routes calls.
func (s *Server) registerIdentityGrantRoutes(mux *http.ServeMux) {
	s.route(mux, "POST /v1/identity/grants", s.handleIdentityGrantMint)
	s.route(mux, "GET /v1/identity/grants", s.handleIdentityGrantList)
	// POST, not GET (ledger L03): this route returns a live grant's RAW
	// TOKEN, and the grant-auth middleware exempts GET on most paths. Making
	// it a POST means it cannot be reached by the blanket GET exemption even
	// if a future edit to isGrantExemptRequest drops the /v1/identity carve-
	// out, and it cannot be triggered by a cross-origin <img>/<script>/form
	// navigation. It is additionally gated: /v1/identity/* is never exempt.
	s.route(mux, "POST /v1/identity/grants/current", s.handleIdentityGrantCurrent)
	s.route(mux, "POST /v1/identity/verify", s.handleIdentityVerify)
	s.route(mux, "POST /v1/identity/grants/{id}/revoke", s.handleIdentityGrantRevoke)
}

// ─── wire types ───────────────────────────────────────────────────────────────

// identityGrantCurrentRequest is the JSON body shape accepted by
// POST /v1/identity/grants/current. Optional — the `surface` query param is
// still honored so the GET→POST migration is a one-word change at call
// sites.
type identityGrantCurrentRequest struct {
	Surface string `json:"surface"`
}

type identityGrantMintRequest struct {
	Surface  string   `json:"surface"`
	Actor    string   `json:"actor,omitempty"`
	Scope    []string `json:"scope,omitempty"`
	TTLHours float64  `json:"ttl_hours,omitempty"`
}

type identityGrantMintResponse struct {
	GrantID   string   `json:"grant_id"`
	Token     string   `json:"token"`
	Surface   string   `json:"surface"`
	Actor     string   `json:"actor,omitempty"`
	Scope     []string `json:"scope,omitempty"`
	ExpiresAt string   `json:"expires_at"`
}

type identityGrantListEntry struct {
	GrantID   string   `json:"grant_id"`
	Surface   string   `json:"surface"`
	Actor     string   `json:"actor,omitempty"`
	Scope     []string `json:"scope,omitempty"`
	IssuedAt  string   `json:"issued_at"`
	ExpiresAt string   `json:"expires_at"`
}

type identityVerifyRequest struct {
	Surface string `json:"surface"`
	Token   string `json:"token"`
}

type identityVerifyResponse struct {
	Valid     bool     `json:"valid"`
	Actor     string   `json:"actor,omitempty"`
	Scope     []string `json:"scope,omitempty"`
	ExpiresAt string   `json:"expires_at,omitempty"`
}

// ─── POST /v1/identity/grants ─────────────────────────────────────────────────

// handleIdentityGrantMint mints (or idempotently re-returns) a grant for the
// requested surface. The response never echoes back a broader scope than
// the caller requested (design §4 verify-tooth #1) — scope is stored
// exactly as requested, no server-side widening.
//
// Surface-match hardening (cog-review finding, PR #551 round 2; see
// serve_grant_auth.go's file header "SURFACE-MATCH" section): this route is
// no longer bootstrap-exempt — grantAuthMiddleware already requires a valid
// presented grant to reach here at all — but VerifyAny alone would still let
// ANY live grant mint for ANY surface, including a throwaway surface an
// attacker minted for itself and then used to supersede node-root's grant.
// The presented grant (grantFromContext) must therefore be the node-root
// admin grant, or already belong to the SAME surface being minted
// (self-service rotation of a surface's own credential). Minimal rule, not a
// full scope model — Wave 6b is where per-surface authorization gets
// designed properly.
//
// This check applies ONLY when the gate itself is enabled (round 3 fix,
// cog-review finding, PR #551 round 3): grantAuthMiddleware only populates
// the context grant when it actually ran the VerifyAny check, so when
// Config.WriteRouteGrantAuthDisabled is true the middleware short-circuits
// before ever setting one, and grantFromContext always returns (nil, false)
// — enforcing surface-match unconditionally would 403 every mint while the
// operator's own disable knob is supposed to restore pre-grant-auth
// behavior on every write route (see the field's doc comment). Deferring to
// grantAuthDisabled (the same accessor grantAuthMiddleware itself consults)
// keeps the two in agreement structurally instead of by convention.
func (s *Server) handleIdentityGrantMint(w http.ResponseWriter, r *http.Request) {
	var req identityGrantMintRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON")
		return
	}
	if req.Surface == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "surface is required")
		return
	}
	if req.Actor != "" && !validActorID(req.Actor) {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "actor must match "+actorIDPattern)
		return
	}
	if !s.grantAuthDisabled() {
		presented, ok := grantFromContext(r.Context())
		if !ok || (presented.Surface != nodeRootSurface && presented.Surface != req.Surface) {
			writeJSONError(w, http.StatusForbidden, "surface_mismatch",
				"the presented grant may not mint a grant for surface "+req.Surface)
			return
		}
		// Scope attenuation: a non-root caller may only mint scopes it already
		// holds. Without this, a grant issued with Scope=["admin"] (narrowly, to
		// manage its own credential) could self-mint a replacement carrying
		// "inference" or "write" and escalate. Node-root is the issuing
		// authority and is exempt. (Review finding on ledger L02.)
		if ok && presented.Surface != nodeRootSurface {
			// Actor attenuation, same shape as scope attenuation: a non-root
			// caller may only mint the actor it already is. Otherwise a
			// self-service rotation of its own surface could name any person.
			if req.Actor != presented.Actor {
				writeJSONError(w, http.StatusForbidden, "actor_escalation",
					"the presented grant may not mint a grant for a different actor")
				return
			}
			for _, want := range req.Scope {
				if !grantHasScope(presented, want) {
					writeJSONError(w, http.StatusForbidden, "scope_escalation",
						"the presented grant does not hold scope "+want+" and may not mint it")
					return
				}
			}
		}
	}
	ttl := time.Duration(0)
	if req.TTLHours > 0 {
		ttl = time.Duration(req.TTLHours * float64(time.Hour))
	}
	grant, err := s.identityGrants.MintOrReuseActor(req.Surface, req.Actor, req.Scope, ttl)
	if err != nil {
		if errors.Is(err, ErrGrantStoreAtCapacity) {
			writeJSONError(w, http.StatusTooManyRequests, "capacity_exceeded", err.Error())
			return
		}
		if errors.Is(err, ErrGrantLedgerAppendFailed) {
			// Server-side durability fault (disk full, transient I/O), not a
			// caller mistake: 503 so a caller/monitor sees a retryable infra
			// failure, not a non-retryable 400 (cog-review finding, PR #472
			// second pass, serve_identity_grants.go:850 as of commit 280aa1a).
			writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable", err.Error())
			return
		}
		writeJSONError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, identityGrantMintResponse{
		GrantID:   grant.GrantID,
		Token:     grant.Token,
		Surface:   grant.Surface,
		Actor:     grant.Actor,
		Scope:     grant.Scope,
		ExpiresAt: grant.ExpiresAt.Format(time.RFC3339),
	})
}

// ─── GET /v1/identity/grants ──────────────────────────────────────────────────

// handleIdentityGrantList is the operator-facing inventory — the kernel-
// native replacement for "grep 6 files for state/*-token" (design §3.1).
// Never includes a token value.
func (s *Server) handleIdentityGrantList(w http.ResponseWriter, r *http.Request) {
	snap := s.identityGrants.Snapshot()
	out := make([]identityGrantListEntry, 0, len(snap))
	for _, g := range snap {
		out = append(out, identityGrantListEntry{
			GrantID:   g.GrantID,
			Surface:   g.Surface,
			Actor:     g.Actor,
			Scope:     g.Scope,
			IssuedAt:  g.IssuedAt.Format(time.RFC3339),
			ExpiresAt: g.ExpiresAt.Format(time.RFC3339),
		})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"grants": out})
}

// ─── POST /v1/identity/grants/current ────────────────────────────────────────

// handleIdentityGrantCurrent is the zero-paste primitive (operator ruling
// 2026-07-21, see file header): returns the live grant's raw token for a
// named surface, if one exists, so a same-loopback page can bootstrap
// without any operator paste. 404 when no live grant exists for that surface
// (caller should mint one via POST /v1/identity/grants, or degrade).
//
// It is a POST behind the grant gate rather than the unauthenticated GET it
// used to be (ledger L03). The old shape meant any process that could open a
// loopback socket could read the node-root token — the kernel's admin
// credential — out of a GET body with no credential of its own, which made
// every other check in serve_grant_auth.go decorative. The primitive itself
// survives, because the caller that needs it (a same-loopback surface
// already holding SOME grant, wanting the current token for a surface) can
// present that grant; the caller with no grant at all bootstraps from
// ~/.cog/vault/node-root-grant (0600) instead.
//
// Surface may arrive as a `surface` query param (unchanged from the GET
// shape, so callers only change the verb) or as {"surface": "..."} in a JSON
// body. Query param wins when both are present.
func (s *Server) handleIdentityGrantCurrent(w http.ResponseWriter, r *http.Request) {
	surface := r.URL.Query().Get("surface")
	if surface == "" && r.Body != nil {
		var req identityGrantCurrentRequest
		// Best-effort: an absent/!JSON body just leaves surface empty and
		// falls into the 400 below with the same message as before.
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			surface = req.Surface
		}
	}
	if surface == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "surface query param is required")
		return
	}
	grant, ok := s.identityGrants.Current(surface)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "not_found", "no live grant for surface "+surface)
		return
	}
	writeJSONResp(w, http.StatusOK, identityGrantMintResponse{
		GrantID:   grant.GrantID,
		Token:     grant.Token,
		Surface:   grant.Surface,
		Actor:     grant.Actor,
		Scope:     grant.Scope,
		ExpiresAt: grant.ExpiresAt.Format(time.RFC3339),
	})
}

// ─── POST /v1/identity/verify ─────────────────────────────────────────────────

// handleIdentityVerify is the verification authority every migrated surface
// calls instead of a local string-compare. Always 200 with {"valid":false}
// on a bad/missing/expired token (not 401) so the response shape is uniform
// JSON for every caller — surfaces branch on the `valid` field, matching the
// {valid, scope[], expires_at} | 401 shape the design's §3.1 table describes
// loosely; chunk 1 picks the always-200-with-valid-bool variant since every
// consumer here is a same-process JSON decode, not a bearer-auth middleware
// that needs the HTTP status itself to carry meaning.
func (s *Server) handleIdentityVerify(w http.ResponseWriter, r *http.Request) {
	var req identityVerifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "body must be JSON")
		return
	}
	grant, ok := s.identityGrants.Verify(req.Surface, req.Token)
	if !ok {
		writeJSONResp(w, http.StatusOK, identityVerifyResponse{Valid: false})
		return
	}
	writeJSONResp(w, http.StatusOK, identityVerifyResponse{
		Valid:     true,
		Actor:     grant.Actor,
		Scope:     grant.Scope,
		ExpiresAt: grant.ExpiresAt.Format(time.RFC3339),
	})
}

// ─── POST /v1/identity/grants/{id}/revoke ────────────────────────────────────

type identityGrantRevokeResponse struct {
	Revoked bool   `json:"revoked"`
	GrantID string `json:"grant_id"`
	Surface string `json:"surface"`
}

// handleIdentityGrantRevoke is chunk 2's mechanism per design §3.4 (rotation
// = issue new + revoke old, using this same endpoint). Appends
// identity.grant.revoked to the ledger and drops the grant from the live
// index immediately (design §3.2: "verify always checks current ledger
// state") — a subsequent POST /v1/identity/verify for this grant's token
// fails right away; the only latency is whatever cache TTL a calling surface
// applies on its own side (design §3.4 note, not this endpoint's concern).
// 404 when grantID names no currently-live grant; 503 when the write-ahead
// ledger append fails — in that case the grant is STILL LIVE and still
// verifying, and telling the caller 404 ("already gone") would mask a failed
// revoke of a possibly-leaked credential (cog-review finding, PR #472 second
// pass, serve_identity_grants.go:444 as of commit 280aa1a).
//
// Surface-match hardening (cog-review unverified note, PR #551 round 2; see
// serve_grant_auth.go's file header "SURFACE-MATCH" section): VerifyAny
// accepts any live grant regardless of surface, so without an additional
// check here a caller holding a grant for an unrelated, throwaway surface
// could revoke node-root's grant outright (its grant_id was visible via the
// then-gate-exempt GET /v1/identity/grants; ledger L03 has since gated all of
// /v1/identity/*, but this check stands on its own). Before mutating anything, this looks
// up the TARGET grant by id (GrantByID, read-only) and requires the
// presented grant (grantFromContext) to be node-root or to already match the
// target's own surface (self-service: a surface may revoke/rotate its own
// grant). A grantID that names no live grant skips the check entirely and
// falls through to Revoke's own ErrGrantNotFound → 404, same as before —
// this hardening only ever narrows an outcome that would otherwise succeed,
// never changes a not-found into something else.
func (s *Server) handleIdentityGrantRevoke(w http.ResponseWriter, r *http.Request) {
	grantID := r.PathValue("id")
	if grantID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "grant id is required")
		return
	}
	// Surface-match check applies only while the gate is enabled — see
	// handleIdentityGrantMint's doc comment for why (round 3 fix, cog-review
	// finding, PR #551 round 3): with Config.WriteRouteGrantAuthDisabled true,
	// grantAuthMiddleware never populates a context grant, so enforcing this
	// unconditionally would 403 every revoke while the disable knob is
	// supposed to restore pre-grant-auth behavior.
	if !s.grantAuthDisabled() {
		if target, found := s.identityGrants.GrantByID(grantID); found {
			presented, ok := grantFromContext(r.Context())
			if !ok || (presented.Surface != nodeRootSurface && presented.Surface != target.Surface) {
				writeJSONError(w, http.StatusForbidden, "surface_mismatch",
					"the presented grant may not revoke a grant for surface "+target.Surface)
				return
			}
		}
	}
	grant, err := s.identityGrants.Revoke(grantID)
	if err != nil {
		if errors.Is(err, ErrGrantLedgerAppendFailed) {
			writeJSONError(w, http.StatusServiceUnavailable, "ledger_unavailable",
				"revoke NOT applied — grant "+grantID+" is still live: "+err.Error())
			return
		}
		writeJSONError(w, http.StatusNotFound, "not_found", "no live grant with id "+grantID)
		return
	}
	writeJSONResp(w, http.StatusOK, identityGrantRevokeResponse{
		Revoked: true,
		GrantID: grant.GrantID,
		Surface: grant.Surface,
	})
}

// actorIDPattern bounds a grant's actor (a person id such as "chaz").
const actorIDPattern = `^[a-z0-9][a-z0-9_-]{0,62}$`

var actorIDRe = regexp.MustCompile(actorIDPattern)

func validActorID(s string) bool { return actorIDRe.MatchString(s) }
