package uri

// Namespaces is the canonical, single-source-of-truth whitelist of valid cog:
// namespace (projection) names.  Both pkg/uri and sdk/uri reference this map;
// sdk/uri.go re-exports it as sdk.Namespaces.
//
// Projection names are reserved per ADR-067: workspace names must not shadow
// them because the resolver uses projections as the discriminator.
var Namespaces = map[string]bool{
	// ── Core memory / knowledge ────────────────────────────────────────────────
	"mem":      true, // cog:mem/* → CogDocs memory corpus
	"adr":      true, // cog:adr/* → Architecture Decision Records
	"docs":     true, // cog:docs/* → Documentation
	"ontology": true, // cog:ontology/* → Ontology definitions

	// ── Config / kernel internals ──────────────────────────────────────────────
	"conf":      true, // cog:conf/* → Configuration files (.cog/config/)
	"config":    true, // cog:config/* → Configuration (alias for conf)
	"kernel":    true, // cog:kernel/* → Kernel internal paths
	"canonical": true, // cog:canonical → Holographic baseline hash

	// ── Identity / session ────────────────────────────────────────────────────
	"identity":  true, // cog:identity → Workspace identity
	"src":       true, // cog:src → SRC constants
	"coherence": true, // cog:coherence → Coherence state

	// ── Hooks / lifecycle ─────────────────────────────────────────────────────
	"hooks": true, // cog:hooks/* → Hook definitions

	// ── Ledger / crystal ──────────────────────────────────────────────────────
	"ledger":  true, // cog:ledger/* → Event ledger
	"crystal": true, // cog:crystal → Ledger crystal state

	// ── Specs / status ────────────────────────────────────────────────────────
	"spec":   true, // cog:spec/* → Specifications
	"specs":  true, // cog:specs/* → Specifications (plural alias)
	"status": true, // cog:status/* → Status snapshots (JSON)
	"work":   true, // cog:work/* → Work items

	// ── Agents / roles / skills ────────────────────────────────────────────────
	"agent":  true, // cog:agent/* → Agent definitions
	"agents": true, // cog:agents/* → Agents (plural alias)
	"role":   true, // cog:role/* → Role definitions
	"roles":  true, // cog:roles/* → Roles (plural alias)
	"skill":  true, // cog:skill/* → Skill definitions
	"skills": true, // cog:skills/* → Skills (plural alias)

	// ── Voice assets ──────────────────────────────────────────────────────────
	// cog:voices/<name>                  → VoiceProfile record (generative + discriminative heads)
	// cog:voices/<name>/ecapa-embedding  → ECAPA-TDNN speaker embedding vector
	// Resolution: the kernel projects cog:voices/* to ~/.mod3/voices/<name>.{json,safetensors}.
	// Pattern mirrors cog:skills/* and cog:mem/* — voice assets are
	// substrate-addressable records, not hidden name lookups.
	"voices": true, // cog:voices/* → Voice profile records

	// ── Registered aliases (URI fix U3) ───────────────────────────────────────
	"memory":       true, // cog://memory/* → alias for cog:mem/*
	"I":            true, // cog://I/ → Identity card
	"architecture": true, // cog://architecture/* → architecture/ tree
	"rfc":          true, // cog://rfc/* → architecture/rfcs/

	// ── Handoffs / artifacts ──────────────────────────────────────────────────
	"handoff":   true, // cog:handoff/* → Handoff documents
	"handoffs":  true, // cog:handoffs/* → Handoffs (plural alias)
	"artifact":  true, // cog:artifact/* → Artifacts
	"artifacts": true, // cog:artifacts/* → Artifacts (plural alias)

	// ── Context / signal / thread (SDK-layer namespaces) ──────────────────────
	"context":   true, // cog:context → 4-tier context assembly
	"signals":   true, // cog:signals/* → Signal field
	"thread":    true, // cog:thread/* → Conversation threads
	"inference": true, // cog:inference → Inference endpoint
}

// IsValidNamespace reports whether ns is a recognized cog: namespace.
func IsValidNamespace(ns string) bool {
	return Namespaces[ns]
}
