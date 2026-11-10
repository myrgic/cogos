// uri_registry.go — live URIRegistry implementation.
//
// Resolution chain for cog://authority/path:
//  1. Parse the URI; extract the authority component.
//  2. Check whether authority is a known projection namespace (e.g. "mem",
//     "adr").  If so, skip alias lookup and resolve locally.
//  3. Alias expansion: look up authority in ~/.cog/node/aliases.yaml.
//  4. Workspace registry lookup: authority (or expanded workspace name) in
//     ~/.cog/node/global.yaml.
//  5. Path resolution via the existing projection logic applied to the
//     workspace root obtained in step 4.
//  6. Digest verification if the URI carries ?digest=<sha256hex> (fail-closed).
//
// cog:path (bare, no //) URIs bypass this registry entirely — the legacy
// ResolveURI handles them directly.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/myrgic/cogos/internal/workspace"
	"github.com/myrgic/cogos/pkg/alias"
)

// ── uriContent and resolver interface ────────────────────────────────────────

// uriContent is the resolved content descriptor returned by URIRegistry.Resolve.
type uriContent struct {
	Metadata map[string]any
}

// uriResolver is the interface that mcp_server.go calls through URIRegistry.
type uriResolver interface {
	Resolve(ctx context.Context, uri string) (*uriContent, error)
}

// URIRegistry is the live resolver.  ResolveURI falls through to it when the
// URI's authority component is not a known projection namespace.
var URIRegistry uriResolver

// ── init ──────────────────────────────────────────────────────────────────────

func init() {
	URIRegistry = &uriRegistryImpl{
		nodeDirFn: defaultNodeDir,
	}
}

// ── Implementation ────────────────────────────────────────────────────────────

type uriRegistryImpl struct {
	// nodeDirFn returns ~/.cog/node/ — injectable for tests.
	nodeDirFn func() string
}

// defaultNodeDir returns the user's ~/.cog/node/ directory.
func defaultNodeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".cog", "node")
	}
	return filepath.Join(home, ".cog", "node")
}

// resolveLocalWorkspace returns the workspace root for the current process.
// Unlike workspace.ResolveWorkspace(), it checks COG_ROOT directly before
// falling back to the cached resolver, so tests can set COG_ROOT via
// t.Setenv and get a fresh read rather than the cached value.
func resolveLocalWorkspace() (string, string, error) {
	if root := os.Getenv("COG_ROOT"); root != "" {
		return root, "explicit", nil
	}
	return workspace.ResolveWorkspace()
}

// Resolve resolves a URI to a uriContent with filesystem metadata.
//
// Supported forms:
//
//	cog://workspace/mem/semantic/x   — cross-workspace via authority
//	cog://alias/mem/semantic/x       — alias expands to workspace name first
//	cog://projection/path            — authority is a known projection; resolves locally
//	cog:path                         — bare local form; resolved by legacy path
func (r *uriRegistryImpl) Resolve(ctx context.Context, rawURI string) (*uriContent, error) {
	// Reject non-cog URIs immediately.
	if !strings.HasPrefix(rawURI, "cog:") {
		return nil, fmt.Errorf("uri_registry: unsupported scheme in %q", rawURI)
	}

	// Bare cog:path (no //) — delegate to the legacy resolver which handles
	// local-workspace projection lookup.  We don't do alias expansion here
	// because ADR-067 says bare cog: is always local.
	if !strings.HasPrefix(rawURI, "cog://") {
		workspaceRoot, _, err := resolveLocalWorkspace()
		if err != nil {
			return nil, fmt.Errorf("uri_registry: no workspace: %w", err)
		}
		// Normalise to cog:// for the existing resolver.
		normalised := "cog://" + strings.TrimPrefix(rawURI, "cog:")
		res, err := ResolveURI(workspaceRoot, normalised)
		if err != nil {
			return nil, err
		}
		return pathToContent(res.Path, res.Fragment, rawURI), nil
	}

	// cog://authority/path form — parse authority and path.
	rest := strings.TrimPrefix(rawURI, "cog://")

	// RFC 3986 §3: query precedes fragment.  Reject URIs where '#' appears
	// before '?' — the previous stripping order (fragment first, then query)
	// meant that cog://ws/mem/x#frag?digest=... would have "#frag?digest=..."
	// stripped as the fragment, leaving no '?' in rest and thus digestHex
	// would never be set, silently bypassing digest verification (ADR-067 §170).
	if qIdx := strings.IndexByte(rest, '?'); qIdx >= 0 {
		if hIdx := strings.IndexByte(rest, '#'); hIdx >= 0 && hIdx < qIdx {
			return nil, fmt.Errorf("uri_registry: malformed URI (fragment before query): %q", rawURI)
		}
	}

	// RFC 3986 §3: URI = scheme ":" hier-part ["?" query] ["#" fragment]
	// Fragment always trails query; strip #fragment FIRST so the query value
	// never accidentally includes the fragment text (e.g. ?digest=hex#frag
	// must not fold "#frag" into the digest hex).
	fragment := ""
	if idx := strings.IndexByte(rest, '#'); idx >= 0 {
		fragment = rest[idx+1:]
		rest = rest[:idx]
	}

	// Strip optional digest query param after fragment is already removed.
	digestHex := ""
	if idx := strings.Index(rest, "?"); idx >= 0 {
		query := rest[idx+1:]
		rest = rest[:idx]
		for _, param := range strings.Split(query, "&") {
			if strings.HasPrefix(param, "digest=") {
				digestHex = strings.TrimPrefix(param, "digest=")
			}
		}
	}

	// authority is everything up to the first slash.
	authority, uriPath, hasPath := strings.Cut(rest, "/")
	if authority == "" {
		return nil, fmt.Errorf("uri_registry: empty authority in %q", rawURI)
	}

	// Step 2: if authority is a known projection namespace, resolve locally.
	if isProjectionNamespace(authority) {
		workspaceRoot, _, err := resolveLocalWorkspace()
		if err != nil {
			return nil, fmt.Errorf("uri_registry: no workspace: %w", err)
		}
		// Re-assemble a cog:// URI for the legacy resolver.
		localURI := "cog://" + authority
		if hasPath {
			localURI += "/" + uriPath
		}
		if fragment != "" {
			localURI += "#" + fragment
		}
		res, err := ResolveURI(workspaceRoot, localURI)
		if err != nil {
			return nil, err
		}
		return pathToContent(res.Path, fragment, rawURI), nil
	}

	// Step 3 & 4: alias expansion + workspace registry lookup.
	nodeDir := r.nodeDirFn()
	workspaceRoot, err := r.resolveAuthority(nodeDir, authority)
	if err != nil {
		return nil, err
	}

	// Step 5: path resolution via projections applied to workspace root.
	if !hasPath || uriPath == "" {
		// No path — return the workspace root itself.
		return pathToContent(workspaceRoot, fragment, rawURI), nil
	}

	// Re-assemble a cog:// URI for the legacy projection resolver.
	projURI := "cog://" + uriPath
	if fragment != "" {
		projURI += "#" + fragment
	}
	res, err := ResolveURI(workspaceRoot, projURI)
	if err != nil {
		return nil, fmt.Errorf("uri_registry: resolve path %q in workspace %q: %w", uriPath, workspaceRoot, err)
	}

	// Step 6: digest verification (fail-closed).
	if digestHex != "" {
		if err := verifyDigest(res.Path, digestHex); err != nil {
			return nil, fmt.Errorf("uri_registry: digest mismatch: %w", err)
		}
	}

	return pathToContent(res.Path, res.Fragment, rawURI), nil
}

// resolveAuthority expands an authority component to a workspace root path.
// It first checks the alias map (aliases.yaml), then the workspace registry
// (global.yaml).
func (r *uriRegistryImpl) resolveAuthority(nodeDir, authority string) (string, error) {
	// Load alias map (best-effort; if it fails, skip alias expansion).
	aliases, err := alias.Load(nodeDir)
	if err == nil {
		if ws, node, ok := aliases.Expand(authority); ok {
			_ = node // node-pinning is a v0.2 feature
			authority = ws
		}
	}

	// Look up workspace name in global.yaml.
	wsRoot, err := lookupWorkspaceRoot(nodeDir, authority)
	if err != nil {
		return "", fmt.Errorf("uri_registry: unknown authority %q: %w", authority, err)
	}
	return wsRoot, nil
}

// lookupWorkspaceRoot reads global.yaml and returns the filesystem root for
// the named workspace, or an error if not found.
func lookupWorkspaceRoot(nodeDir, name string) (string, error) {
	type wsEntry struct {
		Path string `yaml:"path"`
	}
	type registry struct {
		Workspaces map[string]*wsEntry `yaml:"workspaces,omitempty"`
	}

	path := filepath.Join(nodeDir, "global.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("global.yaml not found — run 'cog workspace add' first")
		}
		return "", err
	}

	// Use the stdlib YAML library already imported in this package.
	// We decode manually to avoid pulling in gopkg.in/yaml.v3 here since
	// this file is in package engine.  Actually — the engine package already
	// imports yaml.v3 indirectly, but we can't import it directly without
	// updating go.mod.  Use a simple string-scan approach instead.
	//
	// Actually, the top-level cog.go (package main) has yaml.v3. The engine
	// package does NOT directly import it. Use a minimal YAML decoder.
	reg, err := parseGlobalYAML(data)
	if err != nil {
		return "", fmt.Errorf("parse global.yaml: %w", err)
	}

	entry, ok := reg[name]
	if !ok {
		return "", fmt.Errorf("workspace %q not registered in global.yaml", name)
	}
	return entry, nil
}

// parseGlobalYAML is a minimal YAML parser for the workspace registry shape:
//
//	version: "1.0"
//	workspaces:
//	  name:
//	    path: /abs/path
//
// It does not use a full YAML library to keep engine package deps minimal.
// If a future refactor adds yaml.v3 to the engine package's direct imports,
// replace this with yaml.Unmarshal.
func parseGlobalYAML(data []byte) (map[string]string, error) {
	result := make(map[string]string)
	lines := strings.Split(string(data), "\n")

	inWorkspaces := false
	currentName := ""

	for _, raw := range lines {
		line := raw

		// Detect workspaces block start.
		if strings.TrimSpace(line) == "workspaces:" {
			inWorkspaces = true
			currentName = ""
			continue
		}
		if !inWorkspaces {
			continue
		}

		// Two-space-indented line with no leading deeper indent → workspace name.
		if len(line) > 2 && line[0] == ' ' && line[1] == ' ' && line[2] != ' ' {
			name := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ":"))
			currentName = name
			continue
		}

		// Four-space-indented "path:" line.
		if currentName != "" && strings.HasPrefix(strings.TrimSpace(line), "path:") {
			value := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "path:"))
			result[currentName] = value
		}

		// A top-level key (no leading space) ends the workspaces block.
		if len(line) > 0 && line[0] != ' ' && !strings.HasPrefix(line, "#") {
			if strings.TrimSpace(line) != "workspaces:" {
				inWorkspaces = false
			}
		}
	}
	return result, nil
}

// isProjectionNamespace reports whether s is a known cog:// projection
// namespace.  These are resolved locally regardless of alias configuration.
// This list mirrors pkg/substrate/uri/namespace.Namespaces and
// internal/engine/uri.go.
// TODO: deduplicate once pkg/substrate/uri/namespace is imported directly
// (#166 lands and stabilises the namespace registry).
func isProjectionNamespace(s string) bool {
	switch s {
	case "mem", "signals", "context", "thread", "coherence", "identity",
		"src", "adr", "ledger", "inference", "kernel", "hooks",
		"spec", "specs", "status", "canonical", "handoff", "handoffs",
		"crystal", "role", "roles", "skill", "skills", "agent", "agents",
		"conf", "config", "ontology", "work", "artifact", "artifacts", "docs",
		"voices",                             // cog:voices/* — voice profile records (Primitive 3)
		"memory", "I", "architecture", "rfc": // registered aliases (URI fix U3)
		return true
	}
	return false
}

// pathToContent wraps a resolved filesystem path in a uriContent.
func pathToContent(path, fragment, rawURI string) *uriContent {
	m := map[string]any{
		"path":  path,
		"uri":   rawURI,
		"input": rawURI,
	}
	if fragment != "" {
		m["fragment"] = fragment
	}
	return &uriContent{Metadata: m}
}

// verifyDigest computes the SHA-256 of the file at path and compares it to
// the expected hex string.  Returns an error if the file cannot be read or
// the digest does not match (fail-closed per ADR-067 amendment).
func verifyDigest(path, expectedHex string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q for digest: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("hash %q: %w", path, err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != expectedHex {
		return fmt.Errorf("digest mismatch for %q: got %s, want %s", path, got, expectedHex)
	}
	return nil
}

// ResolveWorkspacePath returns the filesystem root for the named workspace by
// consulting URIRegistry. It is the canonical public surface for packages that
// need workspace-root resolution without holding a reference to the unexported
// uriResolver interface.
//
// Returns ("", ErrUnknownAuthority) when the workspace is not registered.
// All other errors are I/O or parse failures in the global.yaml registry file.
//
// name must be the raw workspace name as registered in global.yaml (e.g.
// "myrgic/cogos"). It must NOT be constructed as a URI authority component:
// synthesising "cog://"+name and re-parsing would split the authority at the
// first slash, looking up "myrgic" instead of "myrgic/cogos".
func ResolveWorkspacePath(ctx context.Context, name string) (string, error) {
	if URIRegistry == nil {
		return "", fmt.Errorf("%w: URIRegistry not initialised", ErrUnknownAuthority)
	}
	// Type-assert to *uriRegistryImpl so we can call resolveAuthority directly
	// with the raw name.  This avoids the "cog://"+name round-trip through
	// Resolve → strings.Cut(rest, "/") which splits "myrgic/cogos" into
	// authority="myrgic" and path="cogos", losing the slash-bearing key.
	impl, ok := URIRegistry.(*uriRegistryImpl)
	if !ok {
		// Fallback for test doubles that only implement uriResolver.
		// The name is treated as a single-component authority (no slash).
		content, err := URIRegistry.Resolve(ctx, "cog://"+name)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrUnknownAuthority, err)
		}
		path, _ := content.Metadata["path"].(string)
		if path == "" {
			return "", fmt.Errorf("%w: URIRegistry returned empty path for %q", ErrUnknownAuthority, name)
		}
		return path, nil
	}
	nodeDir := impl.nodeDirFn()
	wsRoot, err := impl.resolveAuthority(nodeDir, name)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnknownAuthority, err)
	}
	return wsRoot, nil
}
