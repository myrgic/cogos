package sdk

import (
	"fmt"
	"net/url"
	"strings"
)

// ParsedURI represents a parsed cog: URI with its components.
//
// URI format: cog:namespace/path[?query][#fragment]
//
// Examples:
//
//	cog:mem/semantic/insights/eigenform
//	cog:signals/inference?above=0.3
//	cog:context?budget=50000&model=sonnet
//	cog:thread/current#last-10
//	cog:coherence
//	cog:src
type ParsedURI struct {
	// Namespace is the first path component (memory, signals, context, etc.)
	Namespace string

	// Path is everything after the namespace (may be empty).
	Path string

	// Query contains parsed query parameters.
	Query url.Values

	// Fragment is the portion after # (may be empty).
	Fragment string

	// Raw is the original unparsed URI string.
	Raw string
}

// Namespaces is the SDK-layer copy of the canonical namespace whitelist.
// SINGLE SOURCE OF TRUTH: pkg/substrate/uri/namespace.go — keep in sync with
// that file. The sdk module cannot import pkg/substrate/uri directly (separate
// Go modules); this copy must be identical to pkg/substrate/uri.Namespaces.
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

	// ── Handoffs / artifacts ──────────────────────────────────────────────────
	"handoff":   true, // cog:handoff/* → Handoff documents
	"handoffs":  true, // cog:handoffs/* → Handoffs (plural alias)
	"artifact":  true, // cog:artifact/* → Artifacts
	"artifacts": true, // cog:artifacts/* → Artifacts (plural alias)

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

	// ── Context / signal / thread (SDK-layer namespaces) ──────────────────────
	"context":   true, // cog:context → 4-tier context assembly
	"signals":   true, // cog:signals/* → Signal field
	"thread":    true, // cog:thread/* → Conversation threads
	"inference": true, // cog:inference → Inference endpoint
}

// ParseURI parses a cog: URI into its components.
// Both the bare form (cog:namespace/path) and the legacy authority form
// (cog://namespace/path) are accepted per ADR-067.
//
// Returns ErrInvalidURI if the URI is malformed or uses an unknown scheme.
// Returns ErrUnknownNamespace if the namespace is not recognized.
//
// Example:
//
//	parsed, err := sdk.ParseURI("cog:mem/semantic/insights?q=topic&limit=10")
//	// parsed.Namespace = "mem"
//	// parsed.Path = "semantic/insights"
//	// parsed.Query = {"q": ["topic"], "limit": ["10"]}
func ParseURI(rawURI string) (*ParsedURI, error) {
	if rawURI == "" {
		return nil, InvalidURIError(rawURI, "empty URI")
	}

	if !strings.HasPrefix(rawURI, "cog:") {
		return nil, InvalidURIError(rawURI, "must start with cog:")
	}

	// Fail-closed on digest integrity constraint.
	if strings.Contains(rawURI, "?") {
		query := rawURI[strings.Index(rawURI, "?")+1:]
		// strip fragment from query if present
		if idx := strings.IndexByte(query, '#'); idx >= 0 {
			query = query[:idx]
		}
		for _, param := range strings.Split(query, "&") {
			if strings.HasPrefix(param, "digest=") {
				return nil, InvalidURIError(rawURI, "digest verification not implemented: fail-closed per ADR-067")
			}
		}
	}

	// Normalise both cog://namespace/... and cog:namespace/... to http:// for parsing.
	var httpURI string
	if strings.HasPrefix(rawURI, "cog://") {
		httpURI = "http://" + strings.TrimPrefix(rawURI, "cog://")
	} else {
		// Bare form: cog:namespace/path → treat first segment as host in http://
		bare := strings.TrimPrefix(rawURI, "cog:")
		httpURI = "http://" + bare
	}

	parsed, err := url.Parse(httpURI)
	if err != nil {
		return nil, InvalidURIError(rawURI, err.Error())
	}

	// The "host" in our scheme is the namespace
	namespace := parsed.Host
	if namespace == "" {
		return nil, InvalidURIError(rawURI, "missing namespace")
	}

	// Validate namespace
	if !Namespaces[namespace] {
		return nil, &SDKError{
			Op:    "ParseURI",
			URI:   rawURI,
			Cause: ErrUnknownNamespace,
		}
	}

	// Path is everything after the namespace
	path := strings.TrimPrefix(parsed.Path, "/")

	// Reject '..' path-traversal segments at parse time. This is defense in
	// depth alongside the per-projector sanitization in pathsafe.go
	// (myrgic/cogos#489 round 2): url.Parse has already percent-decoded
	// parsed.Path, so this also catches the %2e%2e-encoded form of the same
	// attack, and it protects every projector uniformly rather than relying
	// on each one to sanitize its own filesystem joins.
	//
	// Split on BOTH '/' and '\' (myrgic/cogos#489 round 4): a raw path like
	// "..\\..\\etc" contains no '/', so splitting on '/' alone treats it as
	// one opaque segment ("..\..\etc") that never equals ".." and sails
	// through this check — filepath.Join on Windows then interprets the
	// embedded backslashes as real separators and the traversal succeeds
	// there even though it was rejected in intent everywhere else. Every
	// projector below additionally runs its own per-segment sanitization
	// (pathsafe.go), which independently escapes backslashes inside a
	// component; this check is the first, URI-level layer and should reject
	// the same set that layer would otherwise have to clean up after.
	for _, seg := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == ".." {
			return nil, InvalidURIError(rawURI, "path segment '..' is not allowed")
		}
	}

	return &ParsedURI{
		Namespace: namespace,
		Path:      path,
		Query:     parsed.Query(),
		Fragment:  parsed.Fragment,
		Raw:       rawURI,
	}, nil
}

// String returns the canonical string representation of the URI.
// Always emits the bare cog: form (no //) per ADR-067.
func (p *ParsedURI) String() string {
	var sb strings.Builder
	sb.WriteString("cog:")
	sb.WriteString(p.Namespace)
	if p.Path != "" {
		sb.WriteString("/")
		sb.WriteString(p.Path)
	}
	if len(p.Query) > 0 {
		sb.WriteString("?")
		sb.WriteString(p.Query.Encode())
	}
	if p.Fragment != "" {
		sb.WriteString("#")
		sb.WriteString(p.Fragment)
	}
	return sb.String()
}

// WithQuery returns a new ParsedURI with additional query parameters.
func (p *ParsedURI) WithQuery(key, value string) *ParsedURI {
	newURI := *p
	newURI.Query = make(url.Values)
	for k, v := range p.Query {
		newURI.Query[k] = v
	}
	newURI.Query.Set(key, value)
	return &newURI
}

// GetQuery returns a query parameter value, or empty string if not present.
func (p *ParsedURI) GetQuery(key string) string {
	return p.Query.Get(key)
}

// GetQueryInt returns a query parameter as int, or default if not present/invalid.
func (p *ParsedURI) GetQueryInt(key string, defaultVal int) int {
	val := p.Query.Get(key)
	if val == "" {
		return defaultVal
	}
	var result int
	if _, err := fmt.Sscanf(val, "%d", &result); err != nil {
		return defaultVal
	}
	return result
}

// GetQueryFloat returns a query parameter as float64, or default if not present/invalid.
func (p *ParsedURI) GetQueryFloat(key string, defaultVal float64) float64 {
	val := p.Query.Get(key)
	if val == "" {
		return defaultVal
	}
	var result float64
	if _, err := fmt.Sscanf(val, "%f", &result); err != nil {
		return defaultVal
	}
	return result
}

// GetQueryBool returns a query parameter as bool.
// Returns true for "true", "1", "yes"; false otherwise.
func (p *ParsedURI) GetQueryBool(key string) bool {
	val := strings.ToLower(p.Query.Get(key))
	return val == "true" || val == "1" || val == "yes"
}

// HasPath returns true if the URI has a non-empty path.
func (p *ParsedURI) HasPath() bool {
	return p.Path != ""
}

// PathSegments returns the path split by "/".
func (p *ParsedURI) PathSegments() []string {
	if p.Path == "" {
		return nil
	}
	return strings.Split(p.Path, "/")
}

// IsNamespace returns true if this URI refers to just a namespace (no path).
func (p *ParsedURI) IsNamespace() bool {
	return p.Path == ""
}
