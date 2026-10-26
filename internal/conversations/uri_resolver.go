// uri_resolver.go — cog:conversations URI resolver (RFC-query-aware-conversation-uris, R1-R6).
//
// Path forms (§2):
//
//	cog:conversations                          — whole observatory
//	cog:conversations/<source>                 — all sessions from one source
//	cog:conversations/<source>/<session_id>    — one session
//
// Query params (§3):
//
//	q=<terms>          — term-AND; double-quoted phrase = exact substring
//	role=<csv>         — user,assistant,tool,system  (comma-list)
//	since=<RFC3339>    — lower timestamp bound
//	until=<RFC3339>    — upper timestamp bound (also gates content_hash)
//	limit=<int>        — default 20, server-capped at 200
//	offset=<int>       — skip first N results
//	order=asc|desc     — default asc
//	res=pointer|abstract|full  — resolution ladder (ADR-066)
//	fields=<csv>       — field projection (applied to output)
//
// Reserved (§3, reject with explicit error):
//
//	component=         — reserved for v0.2 ontology-as-class
//	ontology=          — reserved for v0.2 ontology-as-class
//
// ALL unknown params are errors (never silently ignored).
//
// Fragments (§4):
//
//	#id-<record-id>    — canonical by turn UUID
//	#turn-N            — positional (convenience)
//	#turn-N..M         — positional range
//
// Bounded-slice rule (§5): content_hash is only computed when the slice is
// bounded (until= set, or a fully id/positional fragment). Unbounded slices
// must NOT be hashed — resolver returns an error rather than silently hashing.
package conversations

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrURIReservedParam is retained for backward compatibility. It is no longer
// returned by the parser — component= and ontology= are now active params.
// Callers that import this error for equality checks will see no behaviour
// change since the parser never returns it from v0.2 onwards.
var ErrURIReservedParam = errors.New("reserved param: not yet supported")

// ErrURIUnknownParam is returned for any unrecognised query parameter.
var ErrURIUnknownParam = errors.New("unknown query parameter")

// ErrURIUnboundedHash is returned when the caller requests a content_hash on a
// slice that is not fully bounded (no until= and no fully-bounding fragment).
var ErrURIUnboundedHash = errors.New("cannot hash an unbounded slice: set until= or use a fully-bounding fragment")

// ErrURIMixedParams is returned when uri= is combined with other filter params.
var ErrURIMixedParams = errors.New("uri param must not be combined with other filter params")

// ResolutionLevel is the res= parameter value (ADR-066 resolution ladder).
type ResolutionLevel string

const (
	ResPointer  ResolutionLevel = "pointer"  // refs + metadata only
	ResAbstract ResolutionLevel = "abstract" // first ~200 chars per turn
	ResFull     ResolutionLevel = "full"     // full turn text
)

// abstractMaxLen is the number of characters kept per turn in abstract mode.
const abstractMaxLen = 200

// URIQuery is the parsed, validated form of a cog:conversations URI.
type URIQuery struct {
	// Path components.
	Source    string // empty = whole observatory
	SessionID string // empty = all sessions from Source

	// Filters.
	Query  string    // q=
	Roles  []Role    // role= comma-list; nil = all roles
	Since  time.Time // since= RFC3339
	Until  time.Time // until= RFC3339; non-zero → bounded slice
	Limit  int       // default 20, capped at 200
	Offset int       // skip first N
	Order  string    // "asc" | "desc"; default "asc"

	// Resolution.
	Res    ResolutionLevel // pointer|abstract|full; default full
	Fields []string        // fields= projection (empty = all)

	// Fragment addressing.
	Fragment *FragmentSpec

	// v0.2 ontology-as-class params (activated in v0.2 groundwork).
	// ComponentClass filters records by L1 component class.
	// Only records whose Turn.Component matches are returned.
	// Note: v0.1 records are all session.turn — say so in responses.
	ComponentClass string // component=<class>; empty = all classes

	// OntologyVersion is the requested L1 version for validation.
	// The resolver rejects mismatches with an explicit error.
	OntologyVersion string // ontology=<id>@<version>; empty = no validation
}

// FragmentSpec describes the resolved fragment form.
type FragmentSpec struct {
	// ID is set when the fragment is #id-<uuid>.
	ID string
	// TurnN is set (≥0) when the fragment is #turn-N or #turn-N..M.
	TurnN int
	// TurnM is set when the fragment is a range #turn-N..M; -1 = not a range.
	TurnM int
}

// ResolvedSlice is the output of ResolveConversationURI.
type ResolvedSlice struct {
	// Metadata fields.
	URI        string    `json:"uri"`
	ResolvedAt time.Time `json:"resolved_at"`
	Count      int       `json:"count"`
	Sources    []string  `json:"sources"`
	Bounded    bool      `json:"bounded"`

	// ContentHash is non-empty only when Bounded=true.
	ContentHash string `json:"content_hash,omitempty"`

	// Turns carries the resolved content. Shape depends on Res.
	Turns []ResolvedTurn `json:"turns"`
}

// ResolvedTurn is one turn in the resolved slice.
// Which fields are populated depends on the res= parameter.
type ResolvedTurn struct {
	// Always present.
	SessionID string `json:"session_id"`
	TurnIndex int    `json:"turn_index"`
	UUID      string `json:"uuid"`
	Role      string `json:"role"`
	Timestamp string `json:"timestamp,omitempty"`
	// Canonical id-anchor for durable re-referencing (§4).
	IDAnchor string `json:"id_anchor"` // e.g. "#id-<uuid>"

	// Populated for res=abstract and res=full.
	Text string `json:"text,omitempty"`

	// Source is the observer source id (empty for CC sessions).
	Source string `json:"source,omitempty"`

	// v0.2 L3 version tags — populated on records indexed with ontology
	// enforcement enabled. Empty for records indexed before v0.2.
	// Component is always "session.turn" for v0.1 records.
	Component       string `json:"component,omitempty"`
	OntologyVersion string `json:"ontology_version,omitempty"`
	MappingVersion  string `json:"mapping_version,omitempty"`
}

// ParseConversationURI parses and validates a cog:conversations URI.
// Only validates — does not touch the index.
func ParseConversationURI(raw string) (*URIQuery, error) {
	const scheme = "cog:conversations"
	if !strings.HasPrefix(raw, scheme) {
		return nil, fmt.Errorf("not a conversations URI: %q", raw)
	}
	rest := strings.TrimPrefix(raw, scheme)

	// Split fragment first (RFC 3986 ordering: fragment follows query).
	fragment := ""
	if idx := strings.IndexByte(rest, '#'); idx >= 0 {
		fragment = rest[idx+1:]
		rest = rest[:idx]
	}

	// Split query.
	queryStr := ""
	if idx := strings.IndexByte(rest, '?'); idx >= 0 {
		queryStr = rest[idx+1:]
		rest = rest[:idx]
	}

	// Path: rest is now just the path component after "cog:conversations".
	// Valid forms: "", "/<source>", "/<source>/<session_id>".
	uq := &URIQuery{
		Limit: 20,
		Order: "asc",
		Res:   ResFull,
	}

	path := strings.TrimPrefix(rest, "/")
	if path != "" {
		parts := strings.SplitN(path, "/", 2)
		uq.Source = parts[0]
		if len(parts) == 2 {
			uq.SessionID = parts[1]
		}
	}

	// Parse query params.
	if err := parseConversationQueryParams(queryStr, uq); err != nil {
		return nil, err
	}

	// Parse fragment.
	if fragment != "" {
		fs, err := parseFragment(fragment)
		if err != nil {
			return nil, err
		}
		uq.Fragment = fs
	}

	return uq, nil
}

// parseConversationQueryParams validates and populates uq from the query string.
func parseConversationQueryParams(queryStr string, uq *URIQuery) error {
	if queryStr == "" {
		return nil
	}

	// Known params — all processed below (component= and ontology= now active).
	known := map[string]bool{
		"q": true, "role": true, "since": true, "until": true,
		"limit": true, "offset": true, "order": true,
		"res": true, "fields": true,
		"component": true, "ontology": true, // v0.2 activated
	}

	for _, pair := range strings.Split(queryStr, "&") {
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)

		if !known[k] {
			return fmt.Errorf("%w: %q", ErrURIUnknownParam, k)
		}

		switch k {
		case "q":
			uq.Query = v
		case "role":
			roles, err := parseRoles(v)
			if err != nil {
				return err
			}
			uq.Roles = roles
		case "since":
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return fmt.Errorf("invalid since %q: %w", v, err)
			}
			uq.Since = t
		case "until":
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return fmt.Errorf("invalid until %q: %w", v, err)
			}
			uq.Until = t
		case "limit":
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return fmt.Errorf("invalid limit %q: must be a non-negative integer", v)
			}
			if n > 200 {
				n = 200 // server cap
			}
			uq.Limit = n
		case "offset":
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return fmt.Errorf("invalid offset %q: must be a non-negative integer", v)
			}
			uq.Offset = n
		case "order":
			if v != "asc" && v != "desc" {
				return fmt.Errorf("invalid order %q: must be asc or desc", v)
			}
			uq.Order = v
		case "res":
			switch ResolutionLevel(v) {
			case ResPointer, ResAbstract, ResFull:
				uq.Res = ResolutionLevel(v)
			default:
				return fmt.Errorf("invalid res %q: must be pointer, abstract, or full", v)
			}
		case "fields":
			uq.Fields = strings.Split(v, ",")
		case "component":
			// component= filters by L1 component class.
			// Validation against the loaded ontology happens at resolve time.
			if v == "" {
				return fmt.Errorf("invalid component= value: must be a non-empty component class name")
			}
			uq.ComponentClass = v
		case "ontology":
			// ontology= is validated against the loaded L1 at resolve time.
			if v == "" {
				return fmt.Errorf("invalid ontology= value: must be a non-empty version reference")
			}
			uq.OntologyVersion = v
		}
	}
	return nil
}

// parseRoles parses a comma-separated role list. Returns an error for any
// unrecognised role value.
func parseRoles(v string) ([]Role, error) {
	parts := strings.Split(v, ",")
	out := make([]Role, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		switch Role(p) {
		case RoleUser, RoleAssistant, RoleTool, RoleSystem:
			out = append(out, Role(p))
		default:
			return nil, fmt.Errorf("invalid role %q: must be one of user, assistant, tool, system", p)
		}
	}
	return out, nil
}

// parseFragment parses a fragment string into a FragmentSpec.
// Valid forms:
//
//	id-<uuid>           → FragmentSpec{ID: "<uuid>"}
//	turn-<N>            → FragmentSpec{TurnN: N, TurnM: -1}
//	turn-<N>..<M>       → FragmentSpec{TurnN: N, TurnM: M}
func parseFragment(frag string) (*FragmentSpec, error) {
	if strings.HasPrefix(frag, "id-") {
		id := strings.TrimPrefix(frag, "id-")
		if id == "" {
			return nil, fmt.Errorf("invalid fragment %q: empty id after id-", frag)
		}
		return &FragmentSpec{ID: id, TurnM: -1}, nil
	}
	if strings.HasPrefix(frag, "turn-") {
		rest := strings.TrimPrefix(frag, "turn-")
		if strings.Contains(rest, "..") {
			parts := strings.SplitN(rest, "..", 2)
			n, err := strconv.Atoi(parts[0])
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid fragment %q: bad turn index %q", frag, parts[0])
			}
			m, err := strconv.Atoi(parts[1])
			if err != nil || m < 0 {
				return nil, fmt.Errorf("invalid fragment %q: bad turn index %q", frag, parts[1])
			}
			if m < n {
				return nil, fmt.Errorf("invalid fragment %q: range end %d < start %d", frag, m, n)
			}
			return &FragmentSpec{TurnN: n, TurnM: m}, nil
		}
		n, err := strconv.Atoi(rest)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid fragment %q: bad turn index %q", frag, rest)
		}
		return &FragmentSpec{TurnN: n, TurnM: -1}, nil
	}
	return nil, fmt.Errorf("invalid fragment %q: must start with id- or turn-", frag)
}

// isBounded returns true when the URI query describes a bounded slice:
//   - until= is set, OR
//   - the fragment fully bounds the slice (id- or turn-N or turn-N..M).
func (uq *URIQuery) isBounded() bool {
	if !uq.Until.IsZero() {
		return true
	}
	if uq.Fragment != nil {
		return true // id-anchor or positional form — both fully bound
	}
	return false
}

// ResolveConversationURI resolves a cog:conversations URI against the given
// index and returns the resulting slice. ErrUnknownAuthority is returned for
// cross-workspace authority forms.
//
// Deprecated: use ResolveConversationURIWithOntology when ontology enforcement
// is available. This variant passes nil ontology (component= and ontology= params
// accepted in the URI but not validated).
func ResolveConversationURI(raw string, idx *Index) (*ResolvedSlice, error) {
	return ResolveConversationURIWithOntology(raw, idx, nil)
}

// ResolveConversationURIWithOntology resolves a cog:conversations URI against
// the given index, optionally validating component= and ontology= URI params
// against the loaded ontology. lo may be nil — when nil, the params are parsed
// but not validated against a loaded L1 (component= filters by string match
// only; ontology= is accepted and recorded but not verified).
func ResolveConversationURIWithOntology(raw string, idx *Index, lo *LoadedOntology) (*ResolvedSlice, error) {
	uq, err := ParseConversationURI(raw)
	if err != nil {
		return nil, err
	}

	// v0.2 ontology= validation: if an ontology version is requested, verify
	// it matches the loaded L1. Explicit mismatch → error.
	if uq.OntologyVersion != "" {
		if lo == nil || lo.L1 == nil {
			return nil, fmt.Errorf("ontology=%q requested but no ontology is loaded; "+
				"set ontology_dir in observatory config", uq.OntologyVersion)
		}
		if err := lo.OntologyVersionCheck(uq.OntologyVersion); err != nil {
			return nil, err
		}
	}

	// v0.2 component= validation: if a component class is requested and an
	// ontology is loaded, verify the class exists in the L1.
	if uq.ComponentClass != "" && lo != nil && lo.L1 != nil {
		if err := lo.ComponentClass(uq.ComponentClass); err != nil {
			return nil, err
		}
	}

	return resolveQuery(raw, uq, idx)
}

// resolveQuery executes the validated URIQuery against the index.
func resolveQuery(rawURI string, uq *URIQuery, idx *Index) (*ResolvedSlice, error) {
	// Build role set for fast lookup.
	var roleSet map[Role]struct{}
	if len(uq.Roles) > 0 {
		roleSet = make(map[Role]struct{}, len(uq.Roles))
		for _, r := range uq.Roles {
			roleSet[r] = struct{}{}
		}
	}

	// Parse search terms.
	terms := parseSearchQuery(uq.Query)

	// Determine session scope. Snapshot candidate sids and their metas under a
	// single short RLock, then release it: turns are fetched per session below
	// without holding idx.mu across disk reads.
	var sids []string
	metas := make(map[string]SessionMeta)
	idx.mu.RLock()
	if uq.SessionID != "" {
		// One session. Compose the index key: ingest sessions are keyed as
		// "<source>/<session_id>" when Source is set.
		key := uq.SessionID
		if uq.Source != "" {
			key = uq.Source + "/" + uq.SessionID
		}
		sids = []string{key}
		if m, ok := idx.sessions[key]; ok {
			metas[key] = m
		}
	} else {
		for sid, m := range idx.sessions {
			// Source filter.
			if uq.Source != "" {
				// CC sessions have empty Source; ingest sessions have Source set.
				if m.Source != uq.Source && sid != uq.Source+"/"+sid {
					// Check if sid starts with "<source>/".
					if !strings.HasPrefix(sid, uq.Source+"/") && m.Source != uq.Source {
						continue
					}
				}
			}
			metas[sid] = m
			sids = append(sids, sid)
		}
		sortStrings(sids)
	}
	idx.mu.RUnlock()

	// Collect matching turns.
	var allTurns []Turn
	sourcesSeen := make(map[string]struct{})

	for _, sid := range sids {
		turns, ok := idx.peekOrReadTurns(sid)
		if !ok {
			continue
		}
		meta := metas[sid]

		// Track sources for response metadata.
		src := meta.Source
		if src == "" {
			src = "claude-code"
		}
		sourcesSeen[src] = struct{}{}

		for _, t := range turns {
			// Role filter.
			if roleSet != nil {
				if _, ok := roleSet[t.Role]; !ok {
					continue
				}
			}
			// Time filters.
			if !uq.Since.IsZero() && t.Timestamp.Before(uq.Since) {
				continue
			}
			if !uq.Until.IsZero() && t.Timestamp.After(uq.Until) {
				continue
			}
			// Text filter.
			if !matchesAllTerms(t.Text, terms) {
				continue
			}
			// Fragment filter.
			if uq.Fragment != nil {
				if !matchesFragment(t, uq.Fragment) {
					continue
				}
			}
			// v0.2 component= filter: filter records that carry a component
			// class. Records ingested before v0.2 have an empty Component
			// field and are included when component= is set (the filter is
			// applied as a best-effort: if the field is unpopulated, it is
			// treated as session.turn per v0.1 invariant).
			if uq.ComponentClass != "" {
				effectiveClass := t.Component
				if effectiveClass == "" {
					effectiveClass = "session.turn" // v0.1 invariant
				}
				if effectiveClass != uq.ComponentClass {
					continue
				}
			}
			allTurns = append(allTurns, t)
		}
	}

	// Order.
	if uq.Order == "desc" {
		reverseInPlace(allTurns)
	}

	// Offset + Limit.
	total := len(allTurns)
	_ = total
	if uq.Offset > 0 {
		if uq.Offset >= len(allTurns) {
			allTurns = nil
		} else {
			allTurns = allTurns[uq.Offset:]
		}
	}
	if uq.Limit > 0 && len(allTurns) > uq.Limit {
		allTurns = allTurns[:uq.Limit]
	}

	// Build resolved turns.
	resolved := make([]ResolvedTurn, 0, len(allTurns))
	for _, t := range allTurns {
		rt := ResolvedTurn{
			SessionID: t.SessionID,
			TurnIndex: t.TurnIndex,
			UUID:      t.UUID,
			Role:      string(t.Role),
			IDAnchor:  "#id-" + t.UUID,
		}
		if !t.Timestamp.IsZero() {
			rt.Timestamp = t.Timestamp.Format(time.RFC3339)
		}
		if meta, ok := metas[t.SessionID]; ok {
			rt.Source = meta.Source
		}
		switch uq.Res {
		case ResFull:
			rt.Text = t.Text
		case ResAbstract:
			rt.Text = abstractText(t.Text)
		case ResPointer:
			// No text — refs+metadata only.
		}
		// v0.2 L3 tags: carry component/ontology/mapping versions when present.
		if t.Component != "" {
			rt.Component = t.Component
			rt.OntologyVersion = t.OntologyVersion
			rt.MappingVersion = t.MappingVersion
		}
		resolved = append(resolved, rt)
	}

	// Compute sources list.
	sources := make([]string, 0, len(sourcesSeen))
	for s := range sourcesSeen {
		sources = append(sources, s)
	}
	sortStrings(sources)

	bounded := uq.isBounded()

	slice := &ResolvedSlice{
		URI:        rawURI,
		ResolvedAt: time.Now().UTC(),
		Count:      len(resolved),
		Sources:    sources,
		Bounded:    bounded,
		Turns:      resolved,
	}

	// Content hash — only for bounded slices.
	if bounded {
		h, err := computeSliceHash(resolved)
		if err == nil {
			slice.ContentHash = "sha256:" + h
		}
	}

	return slice, nil
}

// matchesFragment returns true when the turn satisfies the fragment spec.
func matchesFragment(t Turn, fs *FragmentSpec) bool {
	if fs.ID != "" {
		return t.UUID == fs.ID
	}
	// Positional form.
	if fs.TurnM < 0 {
		// #turn-N — single turn.
		return t.TurnIndex == fs.TurnN
	}
	// #turn-N..M — range.
	return t.TurnIndex >= fs.TurnN && t.TurnIndex <= fs.TurnM
}

// abstractText returns the first abstractMaxLen characters of text.
func abstractText(text string) string {
	if len(text) <= abstractMaxLen {
		return text
	}
	return text[:abstractMaxLen] + "…"
}

// computeSliceHash returns a hex-encoded SHA-256 of the JSON serialisation of
// the resolved turns. Used only for bounded slices.
func computeSliceHash(turns []ResolvedTurn) (string, error) {
	b, err := json.Marshal(turns)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:]), nil
}

// reverseInPlace reverses a Turn slice in place.
func reverseInPlace(turns []Turn) {
	for i, j := 0, len(turns)-1; i < j; i, j = i+1, j-1 {
		turns[i], turns[j] = turns[j], turns[i]
	}
}

// sortStrings sorts a string slice in place. (Avoids importing sort at top level.)
func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j] < ss[j-1]; j-- {
			ss[j], ss[j-1] = ss[j-1], ss[j]
		}
	}
}
