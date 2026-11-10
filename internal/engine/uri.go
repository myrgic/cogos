// uri.go — cog: URI projection system for CogOS v3
//
// A cog: URI has the form (ADR-067):
//
//	cog:projection/path[?query][#fragment]      (local — no authority)
//	cog://workspace/projection/path[?query][#fragment]  (cross-workspace authority form)
//
// Both forms are accepted.  The bare form (no //) is canonical for local
// references; the authority form is used for cross-workspace refs.
//
// Examples:
//
//	cog:mem/semantic/insights/eigenform.cog.md        → .cog/mem/semantic/insights/eigenform.cog.md
//	cog:mem/semantic/insights/eigenform.cog.md#Seed   → same path, anchor "Seed"
//	cog:conf/kernel.yaml                              → .cog/config/kernel.yaml
//	cog:crystal                                       → .cog/ledger/crystal.json
//	cog://workspace/mem/semantic/x                    → cross-workspace (ErrUnknownAuthority unless #167 merges)
package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/myrgic/cogos/pkg/pathsafe"
)

// resolveURIContext is the context passed to URIRegistry.Resolve when
// ResolveURI delegates to the registry for cross-workspace URIs.  It is
// background because ResolveURI has no caller-supplied context.
var resolveURIContext = context.Background()

// ErrUnknownAuthority is returned when a cog://workspace/... URI references a
// workspace name that is not registered in the local URIRegistry.  The URI
// registry implementation lives in #167; until that lands the error is always
// returned for any cross-workspace authority form.
var ErrUnknownAuthority = errors.New("unknown workspace authority")

// ErrDigestNotVerified is returned when a URI carries a ?digest=sha256:...
// query parameter but the resolver does not implement digest verification.
// Per ADR-067 the resolver MUST fail-closed rather than silently ignoring the
// integrity constraint.
var ErrDigestNotVerified = errors.New("digest verification not implemented: fail-closed per ADR-067")

// ── Projection ───────────────────────────────────────────────────────────────

// Projection defines how a cog:// URI type maps to the filesystem.
type Projection struct {
	// Base is the workspace-relative prefix under the workspace root
	// (e.g. ".cog/mem/"). Mutually exclusive with ExtBase.
	Base string
	// ExtBase is a workspace-root-relative prefix for paths that live
	// outside .cog/ (e.g. ".claude/skills/").
	ExtBase string
	// Pattern controls resolution: "direct" | "directory" | "glob" | "singleton".
	Pattern string
	// Suffix is appended to the resolved path for "direct" patterns
	// (e.g. ".cog.md" for specs).
	Suffix string
	// SuffixRetry lists suffixes tried, in order, when a "direct" path with no
	// forced Suffix does not exist as written (e.g. ".cog.md" so that
	// cog:mem/x and cog:mem/x.md find x.cog.md).  The as-written path always
	// wins when it exists; when nothing exists the as-written path is
	// returned unchanged (so writers still target exactly what was named).
	SuffixRetry []string
	// GlobPat is a fmt.Sprintf template (one %s) for "glob" patterns.
	// E.g. "%s-*.md" matches numbered ADR files.
	GlobPat string
}

// projections maps every known cog:// type to its filesystem projection.
// Aliases (e.g. "role"/"roles") point to equivalent Projections.
var projections = map[string]*Projection{
	// ── Memory corpus ────────────────────────────────────────────────────
	"mem": {Base: ".cog/mem/", Pattern: "direct", SuffixRetry: []string{".cog.md", ".md"}},

	// ── Architecture decision records ────────────────────────────────────
	"adr": {Base: ".cog/adr/", Pattern: "glob", GlobPat: "%s-*.md"},

	// ── Role definitions ─────────────────────────────────────────────────
	"role":  {Base: ".cog/roles/", Pattern: "directory"},
	"roles": {Base: ".cog/roles/", Pattern: "directory"},

	// ── Skills (portable — live outside .cog/) ───────────────────────────
	"skill":  {ExtBase: ".claude/skills/", Pattern: "directory"},
	"skills": {ExtBase: ".claude/skills/", Pattern: "directory"},

	// ── Agent definitions ────────────────────────────────────────────────
	"agent":  {Base: ".cog/bin/agents/", Pattern: "directory"},
	"agents": {Base: ".cog/bin/agents/", Pattern: "directory"},

	// ── Specs / RFCs ─────────────────────────────────────────────────────
	"spec":  {Base: ".cog/specs/", Pattern: "direct", Suffix: ".cog.md"},
	"specs": {Base: ".cog/specs/", Pattern: "direct", Suffix: ".cog.md"},

	// ── Status snapshots ─────────────────────────────────────────────────
	"status": {Base: ".cog/status/", Pattern: "direct", Suffix: ".json"},

	// ── Event ledger ─────────────────────────────────────────────────────
	"ledger": {Base: ".cog/ledger/", Pattern: "directory"},

	// ── Crystal state (singleton) ────────────────────────────────────────
	// cog://crystal resolves to .cog/ledger/crystal.json (no path component).
	"crystal": {Base: ".cog/ledger/crystal.json", Pattern: "singleton"},

	// ── Raw .cog/ paths ──────────────────────────────────────────────────
	"kernel":    {Base: ".cog/", Pattern: "direct"},
	"canonical": {Base: ".cog/", Pattern: "direct"},

	// Config aliases.
	"conf":   {Base: ".cog/config/", Pattern: "direct"},
	"config": {Base: ".cog/config/", Pattern: "direct"},

	// ── Ontology definitions ─────────────────────────────────────────────
	"ontology": {Base: ".cog/ontology/", Pattern: "direct", Suffix: ".cog.md"},

	// ── Work items ───────────────────────────────────────────────────────
	"work": {Base: ".cog/work/", Pattern: "direct"},

	// ── Handoff documents ────────────────────────────────────────────────
	"handoff":  {Base: ".cog/handoffs/", Pattern: "direct", Suffix: ".md"},
	"handoffs": {Base: ".cog/handoffs/", Pattern: "directory"},

	// ── Artifacts ────────────────────────────────────────────────────────
	"artifact":  {Base: ".cog/ledger/", Pattern: "glob", GlobPat: "*/artifacts/%s.*"},
	"artifacts": {Base: ".cog/ledger/", Pattern: "glob", GlobPat: "*/artifacts/%s.*"},

	// ── Docs ─────────────────────────────────────────────────────────────
	"docs": {Base: ".cog/docs/", Pattern: "direct"},

	// ── Hooks ────────────────────────────────────────────────────────────
	"hooks": {Base: ".cog/hooks/", Pattern: "direct"},

	// ── Registered aliases (URI fix U3) ──────────────────────────────────
	// cog://memory is the pre-projection name for cog://mem (cogdoc refs).
	"memory": {Base: ".cog/mem/", Pattern: "direct"},
	// cog://I/ is the identity card; the path component is ignored.
	"I": {Base: ".cog/bin/agents/identities/identity_cog_interface.md", Pattern: "singleton"},
	// cog://architecture and cog://rfc live in the workspace-root architecture tree.
	"architecture": {ExtBase: "architecture/", Pattern: "direct"},
	"rfc":          {ExtBase: "architecture/rfcs/", Pattern: "direct"},
}

// ── Resolution ───────────────────────────────────────────────────────────────

// URIResolution is the result of resolving a cog:// URI to the filesystem.
type URIResolution struct {
	// Path is the absolute filesystem path.
	Path string
	// Fragment is the section anchor stripped from the URI (empty if none).
	Fragment string
}

// cogURIPattern matches cog: URI references embedded in document content.
// Accepts both the bare form (cog:projection/path) and the authority form
// (cog://projection/path) per ADR-067.
var cogURIPattern = regexp.MustCompile(
	`cog:(?://)?` + // scheme — cog:// or cog:
		`\w+` + // type (required)
		`(?:/[\w./_-]*)?` + // /path (optional)
		`(?:#[\w-]*)?`, // #fragment (optional)
)

// ResolveURI resolves a cog: URI to an absolute filesystem path.
// Both the bare form (cog:projection/path) and the authority form
// (cog://projection/path) are accepted per ADR-067.
// The #fragment part (section anchor) is separated and returned in
// URIResolution.Fragment without modifying the path resolution.
//
// For cross-workspace URIs (cog://non-projection-authority/...) the function
// delegates to URIRegistry when it is non-nil.  This means all canonical paths
// (cogdoc patching, content service, etc.) benefit from the registry chain
// without needing to call it directly.
func ResolveURI(workspaceRoot, uri string) (*URIResolution, error) {
	if !strings.HasPrefix(uri, "cog:") {
		return nil, fmt.Errorf("not a cog: URI: %q", uri)
	}

	// Strip scheme — normalise both cog://... and cog:... to a bare path.
	var rest string
	if strings.HasPrefix(uri, "cog://") {
		rest = strings.TrimPrefix(uri, "cog://")
	} else {
		rest = strings.TrimPrefix(uri, "cog:")
	}

	// RFC 3986 ordering requires ?query before #fragment.  Reject malformed
	// URIs where '#' appears before '?' — they would cause the fragment stripper
	// to consume the query string, silently bypassing digest fail-closed (ADR-067
	// §170).  Malformed input is not required to be supported; rejecting it
	// keeps the fail-closed contract unambiguous.
	if qIdx := strings.IndexByte(rest, '?'); qIdx >= 0 {
		if hIdx := strings.IndexByte(rest, '#'); hIdx >= 0 && hIdx < qIdx {
			return nil, fmt.Errorf("malformed cog: URI (fragment before query): %q", uri)
		}
	}

	// Split off query string first (RFC 3986: query precedes fragment).
	// Check for digest fail-closed (ADR-067 §170): a ?digest=sha256:... param
	// signals an integrity constraint.  The local projection resolver does not
	// implement digest verification, so it MUST error rather than silently
	// ignoring the integrity constraint.
	//
	// Fragment capture: when the URI is well-formed (?query#fragment), the
	// fragment follows the query string in the tail.  We capture it here so that
	// truncating `rest` at '?' does not lose the fragment (the subsequent
	// fragment-extraction pass below would find no '#' in the already-truncated
	// rest and silently drop it — regression introduced by PR #176).
	fragment := ""
	if idx := strings.IndexByte(rest, '?'); idx >= 0 {
		query := rest[idx+1:]
		rest = rest[:idx]
		// Separate fragment from query tail if present (well-formed: ?q#frag).
		// Capture the fragment text before stripping it from the query.
		if fIdx := strings.IndexByte(query, '#'); fIdx >= 0 {
			fragment = query[fIdx+1:]
			query = query[:fIdx]
		}
		for _, param := range strings.Split(query, "&") {
			if strings.HasPrefix(param, "digest=") {
				return nil, fmt.Errorf("%w: %q", ErrDigestNotVerified, uri)
			}
		}
		// Other query params (e.g. ?ref=, ?format=) are silently ignored at the
		// filesystem level; higher-level handlers may interpret them.
	}

	// Split off fragment (always follows query per RFC 3986).
	// This handles the no-query case: cog:mem/foo#section.
	// If a fragment was already captured from the query tail above, this block
	// is a no-op (rest contains no '#' after the query was stripped).
	if idx := strings.IndexByte(rest, '#'); idx >= 0 {
		fragment = rest[idx+1:]
		rest = rest[:idx]
	}

	// For the authority form (cog://...) the first component might be a workspace
	// name (cross-workspace reference) or a projection name.  Discriminate by
	// checking projections first.  If the first component is not a known projection,
	// and the original URI used the authority form, it is a cross-workspace ref.
	//
	// Fallback chain:
	//  1. projection lookup (local, fast path — always checked first)
	//  2. URIRegistry delegation (handles aliases + workspace registry)
	//  3. ErrUnknownAuthority if registry is nil or also unknown
	isAuthorityForm := strings.HasPrefix(uri, "cog://")

	// Split type from path (path may be empty for singletons like cog:crystal).
	uriType, uriPath, _ := strings.Cut(rest, "/")

	proj, ok := projections[uriType]
	if !ok {
		if isAuthorityForm {
			// Not a known projection — attempt registry delegation before giving up.
			if URIRegistry != nil {
				content, regErr := URIRegistry.Resolve(resolveURIContext, uri)
				if regErr == nil {
					path, _ := content.Metadata["path"].(string)
					frag, _ := content.Metadata["fragment"].(string)
					if frag == "" {
						frag = fragment
					}
					return &URIResolution{Path: path, Fragment: frag}, nil
				}
				// If the registry also can't resolve it, fall through to
				// ErrUnknownAuthority (preserves fail-closed semantics).
			}
			return nil, fmt.Errorf("%w: workspace %q in URI %q — cross-workspace registry not implemented; for a local doc use the bare form: cog:mem/<path>", ErrUnknownAuthority, uriType, uri)
		}
		return nil, fmt.Errorf("unknown cog: projection %q in URI %q", uriType, uri)
	}

	path, err := resolveProjection(workspaceRoot, proj, uriPath)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", uri, err)
	}
	return &URIResolution{Path: path, Fragment: fragment}, nil
}

// resolveProjection applies a Projection to produce an absolute filesystem path.
func resolveProjection(workspaceRoot string, proj *Projection, uriPath string) (string, error) {
	// Reject path traversal in the untrusted URI component before it reaches the
	// filepath.Join sites below (direct/directory/glob all interpolate uriPath).
	if strings.Contains(uriPath, "..") || filepath.IsAbs(uriPath) {
		return "", fmt.Errorf("cog uri path %q: traversal not permitted", uriPath)
	}

	// Sanitize NTFS-illegal characters per segment (myrgic/cogos#489 round
	// 5). The ".." / absolute-path check above stops traversal, but did
	// nothing about a colon or other NTFS-illegal byte reaching the
	// filesystem raw — this is the single chokepoint every cog: projection
	// in this module resolves through (mem, adr, role, skill, agent, spec,
	// status, ledger, kernel, canonical, conf, ontology, work, handoff,
	// artifact, docs, hooks), so a session-ID-shaped uriPath (e.g.
	// "cog:ledger/http:cog") reached filepath.Join with the raw colon
	// intact via any of the three branches below. pathsafe.SanitizeRelPath
	// is idempotent and a no-op on already-safe input (plain slugs/UUIDs),
	// so this does not change resolution for any existing well-formed URI.
	uriPath = pathsafe.SanitizeRelPath(uriPath)

	// Compute base directory.
	var base string
	if proj.ExtBase != "" {
		base = filepath.Join(workspaceRoot, proj.ExtBase)
	} else {
		base = filepath.Join(workspaceRoot, proj.Base)
	}

	switch proj.Pattern {
	case "singleton":
		// Base already encodes the complete workspace-relative path.
		return filepath.Join(workspaceRoot, proj.Base), nil

	case "direct":
		p := filepath.Join(base, uriPath) + proj.Suffix
		if len(proj.SuffixRetry) > 0 && proj.Suffix == "" && uriPath != "" {
			p = resolveSuffixVariants(p, proj.SuffixRetry)
		}
		return p, nil

	case "directory":
		if uriPath == "" {
			return base + string(filepath.Separator), nil
		}
		return filepath.Join(base, uriPath) + string(filepath.Separator), nil

	case "glob":
		pattern := filepath.Join(base, fmt.Sprintf(proj.GlobPat, uriPath))
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return "", fmt.Errorf("glob %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			return "", fmt.Errorf("no files match glob %q", pattern)
		}
		// Return the first (alphabetically first) match for determinism.
		return matches[0], nil

	default:
		return "", fmt.Errorf("unknown projection pattern %q", proj.Pattern)
	}
}

// resolveSuffixVariants returns p when it exists as written; otherwise the
// first existing variant among: p+retry[i], and — for a p ending in a bare
// ".md" (not ".cog.md") — p with ".md" swapped for each retry suffix.  This
// makes cog:mem/x, cog:mem/x.md, cog:mem/x.cog.md all find x.cog.md, the form
// docs actually use.  When no variant exists p is returned unchanged.
func resolveSuffixVariants(p string, retry []string) string {
	if _, err := os.Stat(p); err == nil {
		return p
	}
	cands := make([]string, 0, 2*len(retry))
	for _, sfx := range retry {
		cands = append(cands, p+sfx)
	}
	if strings.HasSuffix(p, ".md") && !strings.HasSuffix(p, ".cog.md") {
		stem := strings.TrimSuffix(p, ".md")
		for _, sfx := range retry {
			cands = append(cands, stem+sfx)
		}
	}
	for _, c := range cands {
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			return c
		}
	}
	return p
}

// ── Reverse mapping (path → URI) ─────────────────────────────────────────────

// uriMapping maps a workspace-relative filesystem path prefix to a cog: URI prefix.
type uriMapping struct {
	// pathPrefix is workspace-relative (e.g. ".cog/mem/").
	pathPrefix string
	// uriPrefix is the canonical cog: prefix including trailing slash (e.g. "cog:mem/").
	// Uses the bare form (no //) per ADR-067 — projections are local references.
	uriPrefix string
	// stripExts lists file extensions to remove from the name component.
	// Applied in order; first match wins.
	stripExts []string
}

// uriMappings ordered longest-prefix-first so specific paths take precedence
// over generic catch-alls (e.g. ".cog/bin/agents/" before ".cog/").
var uriMappings = []uriMapping{
	{".cog/bin/agents/", "cog:agents/", []string{".md"}},
	{".cog/handoffs/", "cog:handoffs/", []string{".md"}},
	{".cog/ontology/", "cog:ontology/", []string{".cog.md", ".md"}},
	{".cog/config/", "cog:conf/", nil},
	{".cog/hooks/", "cog:hooks/", nil},
	{".cog/specs/", "cog:specs/", []string{".cog.md", ".md"}},
	{".cog/roles/", "cog:roles/", []string{".md"}},
	{".cog/status/", "cog:status/", []string{".json"}},
	{".cog/ledger/", "cog:ledger/", nil},
	{".cog/work/", "cog:work/", nil},
	{".cog/docs/", "cog:docs/", nil},
	{".cog/adr/", "cog:adr/", []string{".md"}},
	{".cog/mem/", "cog:mem/", nil}, // keep full extension for mem
	{".cog/", "cog:kernel/", nil},
	{".claude/skills/", "cog:skills/", nil},
	{".claude/agents/", "cog:agents/", nil},
}

// PathToURI converts an absolute (or workspace-relative) filesystem path to a
// cog:// URI using the longest-matching prefix rule.
// Returns an error if no mapping covers the path.
func PathToURI(workspaceRoot, path string) (string, error) {
	// Normalise to absolute.
	abs := path
	if !filepath.IsAbs(abs) {
		abs = filepath.Join(workspaceRoot, path)
	}

	for _, m := range uriMappings {
		// Build the absolute prefix string, then append separator so we don't
		// accidentally match ".cog/mem" against ".cog/memory/".
		prefix := filepath.Join(workspaceRoot, m.pathPrefix)
		if !strings.HasSuffix(prefix, string(filepath.Separator)) {
			prefix += string(filepath.Separator)
		}
		if !strings.HasPrefix(abs, prefix) {
			continue
		}

		rel := strings.TrimPrefix(abs, prefix)
		// Strip recognised extensions.
		for _, ext := range m.stripExts {
			if strings.HasSuffix(rel, ext) {
				rel = rel[:len(rel)-len(ext)]
				break
			}
		}
		return m.uriPrefix + rel, nil
	}
	return "", fmt.Errorf("no cog:// mapping for path %q", path)
}

// ── Inline reference extraction ──────────────────────────────────────────────

// ExtractInlineRefs scans document content for embedded cog:// URIs and
// returns a deduplicated, sorted slice of every unique URI found.
func ExtractInlineRefs(content string) []string {
	raw := cogURIPattern.FindAllString(content, -1)
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	sort.Strings(out)
	return out
}
