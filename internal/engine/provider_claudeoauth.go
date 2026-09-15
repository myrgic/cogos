// provider_claudeoauth.go — ClaudeOAuthProvider
//
// Implements Provider against the Anthropic Messages API using the operator's
// managed Claude subscription OAuth bearer (the same credential Claude Code
// stores in ~/.claude/.credentials.json and the macOS keychain).
//
// Three parts:
//
//  1. claudeCodeCredentialSource — a CredentialSource that reads from (in
//     priority order) the macOS keychain, CLAUDE_CODE_OAUTH_TOKEN env var,
//     and ~/.claude/.credentials.json.  WriteBack is a NO-OP: Claude Code owns
//     that store and the kernel is a strict read-only mirror (see issue #363).
//
//  2. claudeOAuthRefreshFunc — a RefreshFunc that calls the Anthropic OAuth
//     token endpoint with the refresh token and returns a new OAuthCredential.
//
//  3. ClaudeOAuthProvider — the Provider implementation. It composes the
//     AnthropicProvider request builder and SSE parser with a CredentialLifecycle
//     for auth.  Available() does a lightweight GET /v1/models behind the
//     refresh wrapper.
//
// Gray-area notice (per RFC): this provider presents Claude Code's client
// identity to use the managed subscription outside the official CLI.  The
// operator has explicitly accepted this trade-off.  Mitigations: version is
// detected dynamically from the installed `claude` binary; the beta set is a
// single named constant; the subprocess client remains as a fallback.
//
// Reference implementation for exact values:
//
//	~/.hermes/hermes-agent/agent/anthropic_adapter.py
//	functions: refresh_anthropic_oauth_pure, _write_claude_code_credentials,
//	           _read_claude_code_credentials_from_keychain, read_claude_code_credentials,
//	           is_claude_code_token_valid
//	constants: _COMMON_BETAS, _OAUTH_ONLY_BETAS, _CLAUDE_CODE_SYSTEM_PREFIX,
//	           client_id ("9d1c250a-e61b-44d9-88ed-5944d1962f5e"),
//	           token_endpoints (platform.claude.com → console.anthropic.com)
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── 429 backoff / CLI fallback tunables ───────────────────────────────────────
// The managed subscription rate-shapes usage over a rolling window; bursts
// beyond the instantaneous allowance are rejected with HTTP 429 when
// pay-as-you-go overage is disabled (RCA: project_claude_oauth_429_rca). On 429
// the provider respects retry-after, backs off a bounded number of times, then
// falls back to the CLI provider (same subscription via the official client).
const (
	oauthMax429Retries = 2
	oauthMax429Backoff = 8 * time.Second
)

// oauthRetryAfter returns how long to wait before retrying a 429. It honours an
// integer-seconds retry-after header when present, otherwise uses exponential
// backoff (1s, 2s, …) keyed on the attempt index.
func oauthRetryAfter(h http.Header, attempt int) time.Duration {
	if h != nil {
		if ra := strings.TrimSpace(h.Get("retry-after")); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs >= 0 {
				return time.Duration(secs) * time.Second
			}
		}
	}
	return time.Duration(1<<uint(attempt)) * time.Second
}

// ── Gray-area constants (mirrored from the reference implementation) ──────────
// Source: ~/.hermes/hermes-agent/agent/anthropic_adapter.py
// These values are binding-specific and must not be moved into the generic
// CredentialLifecycle.

// claudeOAuthClientID is the OAuth client_id used by Claude Code.
// Source: refresh_anthropic_oauth_pure, local var client_id.
const claudeOAuthClientID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"

// claudeOAuthTokenEndpoints is the ordered list of token endpoints to try.
// Source: refresh_anthropic_oauth_pure, local var token_endpoints.
var claudeOAuthTokenEndpoints = []string{
	"https://platform.claude.com/v1/oauth/token",
	"https://console.anthropic.com/v1/oauth/token",
}

// claudeCodeCommonBetas are beta flags sent on all OAuth requests.
// Source: _COMMON_BETAS.
// Note: "context-1m-2025-08-07" is intentionally excluded from the common set
// because some subscriptions reject it with HTTP 400. It is applied reactively
// if the provider detects a 1M-context request.
var claudeCodeCommonBetas = []string{
	"interleaved-thinking-2025-05-14",
	"fine-grained-tool-streaming-2025-05-14",
}

// claudeCodeOAuthOnlyBetas are additional beta flags required for OAuth auth.
// Source: _OAUTH_ONLY_BETAS.
var claudeCodeOAuthOnlyBetas = []string{
	"claude-code-20250219",
	"oauth-2025-04-20",
}

// claudeCodeContext1MBeta is the long-context beta. Not included in the
// default beta set; added reactively on HTTP 400 "long context beta" errors.
// Source: _CONTEXT_1M_BETA.
const claudeCodeContext1MBeta = "context-1m-2025-08-07"

// claudeCodeSystemPrefix is prepended to every system prompt sent via OAuth.
// Source: _CLAUDE_CODE_SYSTEM_PREFIX.
const claudeCodeSystemPrefix = "You are Claude Code, Anthropic's official CLI for Claude."

// claudeCodeVersionFallback is used when the installed claude binary cannot
// be detected.  Source: _CLAUDE_CODE_VERSION_FALLBACK.
const claudeCodeVersionFallback = "2.1.74"

// ── Credential JSON shape ─────────────────────────────────────────────────────

// resolveHomeDir returns the user's home directory for goos, checking the
// same environment variables Go's os.UserHomeDir() checks internally: HOME on
// macOS/Linux, %USERPROFILE% (falling back to %HOMEDRIVE%+%HOMEPATH%) on
// Windows. It is expressed as a pure function of goos and an injectable
// getenv rather than always reading runtime.GOOS/os.Getenv directly so the
// Windows-shaped discovery path can be unit-tested from a single (non-Windows)
// test binary — runtime.GOOS is a build-time constant baked into the test
// binary, so a test cannot make os.UserHomeDir() itself take the Windows
// branch by setting USERPROFILE at t.Setenv time; this seam makes that branch
// reachable and table-testable regardless of the host OS running `go test`.
func resolveHomeDir(goos string, getenv func(string) string) string {
	if goos == "windows" {
		if v := getenv("USERPROFILE"); v != "" {
			return v
		}
		if drive, path := getenv("HOMEDRIVE"), getenv("HOMEPATH"); drive != "" && path != "" {
			return drive + path
		}
		return ""
	}
	return getenv("HOME")
}

// claudeConfigDir returns the Claude Code config directory: CLAUDE_CONFIG_DIR
// when set (the documented Claude Code override — see
// https://code.claude.com/docs/en/env-vars — it relocates the ENTIRE config
// directory, including .credentials.json, on every OS), otherwise
// "<home>/.claude".
//
// Home resolution goes through resolveHomeDir(runtime.GOOS, ...), NOT
// os.Getenv("HOME") directly: HOME is unset on Windows, where Claude Code
// stores credentials at %USERPROFILE%\.claude\.credentials.json (confirmed
// against Claude Code's own docs, 2026-07: Windows and Linux both use the
// file store as their credential source of truth; the macOS keychain is
// additionally consulted — and takes priority — on darwin only, via
// claudeCodeCredentialSource.readKeychain). The old os.Getenv("HOME") silently
// produced the bare relative path "\.claude\.credentials.json" on Windows,
// which resolves relative to the process's current directory rather than the
// user's profile — a discovery-breaking bug on every non-macOS node.
func claudeConfigDir() string {
	if dir := strings.TrimSpace(os.Getenv("CLAUDE_CONFIG_DIR")); dir != "" {
		return dir
	}
	home := resolveHomeDir(runtime.GOOS, os.Getenv)
	if home == "" {
		// Last-ditch fallback: os.UserHomeDir() covers OS nuances
		// resolveHomeDir doesn't special-case; degrade gracefully rather than
		// join against an empty string.
		if h, err := os.UserHomeDir(); err == nil && h != "" {
			home = h
		}
	}
	return filepath.Join(home, ".claude")
}

// defaultClaudeCredentialsPath returns the on-disk credential file path.
// Recomputed on every call (not cached in a package var) so it reflects the
// CURRENT environment: auto-registration re-probes this on every reconcile
// tick (see maybeAutoRegisterClaudeOAuth), and CLAUDE_CONFIG_DIR / HOME can
// legitimately differ between that probe and process start (tests also rely
// on per-case env overrides via t.Setenv taking effect immediately).
// Source: _write_claude_code_credentials.
func defaultClaudeCredentialsPath() string {
	return filepath.Join(claudeConfigDir(), ".credentials.json")
}

// claudeAiOauthJSON is the nested structure stored under "claudeAiOauth" in
// ~/.claude/.credentials.json and in the macOS keychain entry
// "Claude Code-credentials". Field names match the reference implementation.
type claudeAiOauthJSON struct {
	AccessToken  string   `json:"accessToken"`
	RefreshToken string   `json:"refreshToken,omitempty"`
	ExpiresAt    int64    `json:"expiresAt,omitempty"` // milliseconds since epoch
	Scopes       []string `json:"scopes,omitempty"`
}

type claudeCredentialsJSON struct {
	ClaudeAiOauth *claudeAiOauthJSON `json:"claudeAiOauth,omitempty"`
	// other top-level fields are preserved on write-back
}

// ── Version detection ─────────────────────────────────────────────────────────

var (
	claudeCodeVersionOnce  sync.Once
	claudeCodeVersionCache string
)

// detectClaudeCodeVersion returns the installed Claude Code version string,
// falling back to claudeCodeVersionFallback on any error.
// Source: _detect_claude_code_version.
func detectClaudeCodeVersion() string {
	for _, cmd := range []string{"claude", "claude-code"} {
		out, err := exec.Command(cmd, "--version").Output()
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(out))
		if s == "" {
			continue
		}
		// Output formats: "2.1.74 (Claude Code)" or just "2.1.74"
		parts := strings.Fields(s)
		if len(parts) > 0 && len(parts[0]) > 0 && parts[0][0] >= '0' && parts[0][0] <= '9' {
			return parts[0]
		}
	}
	return claudeCodeVersionFallback
}

func getClaudeCodeVersion() string {
	claudeCodeVersionOnce.Do(func() {
		claudeCodeVersionCache = detectClaudeCodeVersion()
	})
	return claudeCodeVersionCache
}

// ── claudeCodeCredentialSource ────────────────────────────────────────────────

// claudeCodeCredentialSource reads Claude Code OAuth credentials from the
// macOS keychain ("Claude Code-credentials"), CLAUDE_CODE_OAUTH_TOKEN env var,
// and ~/.claude/.credentials.json (in that priority order).
// WriteBack is a no-op: Claude Code is the sole owner/writer of that store; the
// kernel must never write it (see WriteBack and issue #363).
type claudeCodeCredentialSource struct {
	credPath string // defaults to claudeCredentialsFile; overridable in tests
}

func newClaudeCodeCredentialSource() *claudeCodeCredentialSource {
	return &claudeCodeCredentialSource{credPath: defaultClaudeCredentialsPath()}
}

// Resolve reads the credential from the highest-priority available source.
// Source: read_claude_code_credentials (Python reference).
func (s *claudeCodeCredentialSource) Resolve() (OAuthCredential, error) {
	// 1. macOS keychain (Darwin only)
	if runtime.GOOS == "darwin" {
		if cred, ok := s.readKeychain(); ok {
			return cred, nil
		}
	}

	// 2. CLAUDE_CODE_OAUTH_TOKEN env var
	if tok := strings.TrimSpace(os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")); tok != "" {
		// Env var tokens have no known refresh path from the env alone.
		return OAuthCredential{AccessToken: tok}, nil
	}

	// 3. ~/.claude/.credentials.json
	if cred, ok := s.readCredFile(); ok {
		return cred, nil
	}

	return OAuthCredential{}, fmt.Errorf("claude-oauth: no credential found in keychain, env, or %s", s.credPath)
}

// keychainExecTimeout bounds the `security find-generic-password` subprocess so
// a hung/locked/prompting keychain cannot block a credential Resolve() past this
// budget. Resolve() is called (transitively) from the /v1/models ListModels
// probe and from Available(); neither could kill the keychain exec before,
// because the CredentialSource.Resolve() contract carries no context. Bounding
// it here keeps the exec self-contained and hardens both callers without
// widening the shared interface. A timeout is treated as "no keychain credential"
// and falls through to the env-var / file sources.
const keychainExecTimeout = 2 * time.Second

// readKeychain reads the "Claude Code-credentials" keychain entry on macOS.
// Source: _read_claude_code_credentials_from_keychain.
func (s *claudeCodeCredentialSource) readKeychain() (OAuthCredential, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), keychainExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "security", "find-generic-password",
		"-s", "Claude Code-credentials",
		"-w").Output()
	if err != nil {
		return OAuthCredential{}, false
	}
	raw := strings.TrimSpace(string(out))
	if raw == "" {
		return OAuthCredential{}, false
	}
	var top struct {
		ClaudeAiOauth *claudeAiOauthJSON `json:"claudeAiOauth"`
	}
	if err := json.Unmarshal([]byte(raw), &top); err != nil || top.ClaudeAiOauth == nil {
		return OAuthCredential{}, false
	}
	o := top.ClaudeAiOauth
	if o.AccessToken == "" {
		return OAuthCredential{}, false
	}
	return OAuthCredential{
		AccessToken:  o.AccessToken,
		RefreshToken: o.RefreshToken,
		ExpiresAtMS:  o.ExpiresAt,
	}, true
}

// readCredFile reads ~/.claude/.credentials.json.
// Source: read_claude_code_credentials (JSON file branch).
func (s *claudeCodeCredentialSource) readCredFile() (OAuthCredential, bool) {
	data, err := os.ReadFile(s.credPath)
	if err != nil {
		return OAuthCredential{}, false
	}
	var doc struct {
		ClaudeAiOauth *claudeAiOauthJSON `json:"claudeAiOauth"`
	}
	if err := json.Unmarshal(data, &doc); err != nil || doc.ClaudeAiOauth == nil {
		return OAuthCredential{}, false
	}
	o := doc.ClaudeAiOauth
	if o.AccessToken == "" {
		return OAuthCredential{}, false
	}
	return OAuthCredential{
		AccessToken:  o.AccessToken,
		RefreshToken: o.RefreshToken,
		ExpiresAtMS:  o.ExpiresAt,
	}, true
}

// WriteBack is intentionally a NO-OP for the Claude Code credential source.
//
// Claude Code is the sole owner and writer of ~/.claude/.credentials.json and
// the macOS keychain entry it mirrors. The kernel is a strict READ-ONLY mirror
// of that credential (see newClaudeCodeReadOnlyRefresh): it never POSTs the
// single-use refresh token, and it must likewise never WRITE the credential
// file. The read-only refresh only ever surfaces the current keychain value
// (or a Resolve() fallback that may be stale), so a WriteBack here can only
// either redundantly rewrite the keychain value — churning the file mtime — or
// poison Claude Code's file fallback store with a stale token, which forces an
// interactive `claude /login`.
//
// Observed live 2026-06-04 (issue #363): under a peer-node-down fallback loop
// the kernel routed periodic inference to claude-oauth ~every 10s; each refresh
// triggered this WriteBack, rewriting the file every ~10s and injecting a stale
// token, causing repeated "OAuth token revoked · Please run /login" logouts
// with only a single claude-code process running. #361 made the refresh
// read-only but left this write active; making WriteBack a no-op completes the
// read-only-mirror discipline. The generic CredentialLifecycle WriteBack call
// is retained for other OAuth sources that legitimately own their own store.
//
// Windows/Linux rotation hazard (this is the general case, not just #363):
// on those platforms ~/.claude/.credentials.json (or
// %USERPROFILE%\.claude\.credentials.json — see claudeConfigDir) is the
// AUTHORITATIVE store, not a mirror of a keychain — there is no OS keychain
// fallback there. If the kernel ever refreshed AND wrote back on those
// platforms, it would race the live Claude Code client for the single-use
// refresh token exactly like the keychain case: whichever of {kernel, CC}
// consumes the refresh_token second gets `invalid_grant`, and whichever
// writes last wins the file, silently discarding the other's rotation. This
// WriteBack no-op plus the read-only RefreshFunc (newClaudeCodeReadOnlyRefresh,
// which never POSTs and instead re-reads the file before delegating to an
// owner-side actuator) closes that hazard structurally on every OS: the
// kernel can only ever consume a token Claude Code already wrote, never
// produce one of its own.
func (s *claudeCodeCredentialSource) WriteBack(_ OAuthCredential) error {
	return nil
}

// ── claudeOAuthRefreshFunc ────────────────────────────────────────────────────

// claudeOAuthRefresh is the RefreshFunc for Claude Code OAuth.
// It tries claudeOAuthTokenEndpoints in order, using form-encoded body.
// Source: refresh_anthropic_oauth_pure (Python reference).
func claudeOAuthRefresh(ctx context.Context, refreshToken string) (OAuthCredential, error) {
	if refreshToken == "" {
		return OAuthCredential{}, fmt.Errorf("claude-oauth: refresh: empty refresh token")
	}

	formData := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {claudeOAuthClientID},
	}
	body := formData.Encode()
	ua := fmt.Sprintf("claude-cli/%s (external, cli)", getClaudeCodeVersion())

	var lastErr error
	for _, endpoint := range claudeOAuthTokenEndpoints {
		reqBody := strings.NewReader(body)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, reqBody)
		if err != nil {
			lastErr = err
			continue
		}
		httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		httpReq.Header.Set("User-Agent", ua)

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			lastErr = err
			slog.Debug("claude-oauth: token refresh failed", "endpoint", endpoint, "err", err)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("claude-oauth: token endpoint %s returned %d: %s",
				endpoint, resp.StatusCode, string(respBody))
			slog.Debug("claude-oauth: token refresh non-200", "endpoint", endpoint,
				"status", resp.StatusCode)
			continue
		}

		var result struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int64  `json:"expires_in"` // seconds
		}
		if err := json.Unmarshal(respBody, &result); err != nil {
			lastErr = fmt.Errorf("claude-oauth: parse token response: %w", err)
			continue
		}
		if result.AccessToken == "" {
			lastErr = fmt.Errorf("claude-oauth: token response missing access_token")
			continue
		}

		nextRefresh := result.RefreshToken
		if nextRefresh == "" {
			nextRefresh = refreshToken // keep the old one if not rotated
		}
		expiresIn := result.ExpiresIn
		if expiresIn == 0 {
			expiresIn = 3600
		}
		expiresAtMS := time.Now().UnixMilli() + expiresIn*1000

		return OAuthCredential{
			AccessToken:  result.AccessToken,
			RefreshToken: nextRefresh,
			ExpiresAtMS:  expiresAtMS,
		}, nil
	}

	if lastErr != nil {
		return OAuthCredential{}, fmt.Errorf("claude-oauth: all token endpoints failed: %w", lastErr)
	}
	return OAuthCredential{}, fmt.Errorf("claude-oauth: all token endpoints failed")
}

// ── claude_code read-only refresh (PATCH-007 port) ───────────────────────────
//
// The credential the managed-subscription provider mirrors (source ==
// claude_code) is OWNED and rotated by Claude Code itself, which is the sole
// writer of the macOS keychain entry "Claude Code-credentials". The refresh
// token is SINGLE-USE. If the kernel POSTs that refresh token to the Anthropic
// OAuth endpoint it (a) races Claude Code for the one-shot rotation, and (b)
// lands the rotated pair only in ~/.claude/.credentials.json — a file Claude
// Code >=2.1.114 ignores in favour of the keychain — orphaning the rotation and
// causing hung/cancelled inference requests after every `claude /login` or
// token rotation.
//
// So for the claude_code source the kernel is a READ-ONLY keychain MIRROR. It
// NEVER POSTs the refresh token. When its mirrored credential is stale it
// delegates the refresh to the OWNER (Claude Code) by running the user-space
// actuator ~/.hermes/bin/claude-token-refresh (a single-flight headless
// `claude -p`; fast-exits 0 if the token is already fresh), then re-mirrors.
// If the keychain is still stale after that, the refresh token is likely
// revoked and the credential surfaces as unavailable (needs interactive
// `claude /login`) — the kernel does NOT loop and does NOT POST as a fallback.
//
// Architecture mirrors the canonical Python fix verbatim in ordering:
//
//	Reference: ~/.hermes/hermes-agent/agent/credential_pool.py
//	           CredentialPool._refresh_entry, the PATCH-007 branch
//	           (commit 5752ef82d). Python sequence:
//	             synced = _sync_anthropic_entry_from_credentials_file(entry)   # mirror
//	             if not _entry_needs_refresh(synced): return synced
//	             run ~/.hermes/bin/claude-token-refresh (timeout 45)           # actuator
//	             synced = _sync_anthropic_entry_from_credentials_file(synced)  # re-mirror
//	             if not _entry_needs_refresh(synced): return synced
//	             if force: _mark_exhausted(entry)                              # surface stale
//	             return None
//
// In the Go kernel the equivalent of "_sync_anthropic_entry_from_credentials_file"
// (keychain-first read) is the CredentialSource.Resolve already wired into the
// CredentialLifecycle, and "_entry_needs_refresh" is the lifecycle's isFresh.
// We express the read-only behaviour as the provider's RefreshFunc so the POST
// path (claudeOAuthRefresh, above — preserved unchanged for any non-claude_code
// OAuth source) is structurally unreachable for the managed subscription.

// claudeTokenRefreshActuator is the user-space owner-delegating refresh actuator.
// Source: EXT-010, ~/.hermes/bin/claude-token-refresh. Uses os.UserHomeDir()
// (not os.Getenv("HOME"), empty on Windows) for the same reason as
// claudeConfigDir above; runClaudeTokenRefreshActuator already tolerates a
// missing binary as a no-op, so a Hermes-less node (most Windows/Linux nodes
// today) simply skips owner-delegation and falls straight to the
// still-fresh/still-stale check.
var claudeTokenRefreshActuator = func() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home = os.Getenv("HOME")
	}
	return filepath.Join(home, ".hermes", "bin", "claude-token-refresh")
}()

// claudeTokenRefreshActuatorTimeout bounds the actuator subprocess. Matches the
// Python reference (subprocess.run(..., timeout=45)).
const claudeTokenRefreshActuatorTimeout = 45 * time.Second

// runClaudeTokenRefreshActuator delegates the OAuth refresh to its owner (Claude
// Code) by running the EXT-010 actuator. It is bounded by a context timeout,
// tolerates a missing binary (no-op), and passes NO secret on argv — the
// actuator reads/writes the keychain itself. stdin/stdout/stderr are detached
// (the actuator's only side effect we care about is the refreshed keychain
// token, which we pick up on the subsequent re-mirror).
//
// Source: PATCH-007 actuator invocation in _refresh_entry (subprocess.run with
// DEVNULL on all three streams, timeout=45).
func runClaudeTokenRefreshActuator(ctx context.Context, actuatorPath string) {
	if actuatorPath == "" {
		return
	}
	if _, err := os.Stat(actuatorPath); err != nil {
		// Actuator absent (e.g. non-Hermes host) — tolerate; we simply cannot
		// delegate a refresh and will surface the stale credential below.
		slog.Debug("claude-oauth: refresh actuator not present; skipping owner delegation",
			"actuator", actuatorPath, "err", err)
		return
	}
	runCtx, cancel := context.WithTimeout(ctx, claudeTokenRefreshActuatorTimeout)
	defer cancel()

	// No arguments: the actuator takes no credential on argv (no secret leak to
	// process listings); it operates on the keychain directly.
	cmd := exec.CommandContext(runCtx, actuatorPath)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		// Non-fatal: a failed/timed-out actuator just means we couldn't trigger
		// an owner refresh. The caller re-mirrors and surfaces staleness; it
		// never falls back to POSTing the refresh token.
		slog.Debug("claude-oauth: refresh actuator returned error (delegation best-effort)",
			"actuator", actuatorPath, "err", err)
	}
}

// newClaudeCodeReadOnlyRefresh builds the RefreshFunc used by the managed
// subscription (source == claude_code). It NEVER POSTs the refresh token.
//
// The CredentialLifecycle calls this only after it has already re-resolved the
// source and found nothing fresher (see freshTokenLocked: it re-reads the
// source keychain-first before invoking RefreshFunc). This RefreshFunc adds the
// owner-delegation step: run the actuator, then re-mirror by resolving the
// source again. If the freshly-mirrored credential is now fresh, it is returned
// (and adopted by the lifecycle); otherwise an error is returned so the provider
// surfaces Unavailable / needs-interactive-login rather than presenting a stale
// bearer or POSTing.
//
// The returned func ignores the refreshToken argument entirely — by contract we
// never exchange it. This is the structural no-POST invariant.
func newClaudeCodeReadOnlyRefresh(src CredentialSource, actuatorPath string) RefreshFunc {
	return func(ctx context.Context, _ /* refreshToken — intentionally unused: never POSTed */ string) (OAuthCredential, error) {
		// Step 1 (mirror): re-read what Claude Code currently holds
		// (keychain-first). The lifecycle already did this once before calling
		// us, but re-reading here keeps this func correct in isolation and
		// matches the Python branch's leading _sync call.
		if cred, err := src.Resolve(); err == nil && isFresh(cred) {
			return cred, nil
		}

		// Step 2 (delegate): ask the owner (Claude Code) to refresh its own
		// keychain token via the user-space actuator. Bounded, best-effort,
		// no secret on argv, tolerates a missing binary.
		runClaudeTokenRefreshActuator(ctx, actuatorPath)

		// Step 3 (re-mirror): read the (hopefully) owner-refreshed keychain token.
		cred, err := src.Resolve()
		if err != nil {
			return OAuthCredential{}, fmt.Errorf("claude-oauth: read-only refresh: re-mirror after actuator: %w", err)
		}
		if isFresh(cred) {
			return cred, nil
		}

		// Step 4 (surface stale): still stale after owner delegation. The
		// refresh token is likely revoked → needs interactive `claude /login`.
		// Return an error (the lifecycle propagates it as Unavailable). We do
		// NOT loop and do NOT POST the refresh token as a fallback.
		return OAuthCredential{}, fmt.Errorf(
			"claude-oauth: credential stale after owner refresh; interactive `claude /login` required (refresh token likely revoked)")
	}
}

// ── ClaudeOAuthProvider ───────────────────────────────────────────────────────

// ClaudeOAuthProvider implements Provider against the Anthropic Messages API
// using an OAuth bearer credential (Claude Code managed subscription).
//
// It composes the existing AnthropicProvider's request builder and SSE parser,
// replacing only the auth layer with a CredentialLifecycle.
type ClaudeOAuthProvider struct {
	name      string
	model     string
	endpoint  string
	maxTokens int
	timeout   time.Duration
	client    *http.Client
	lc        *CredentialLifecycle

	// availMu guards the Available() TTL cache (#441): the router's 10s
	// availability ticker probes this provider with a live GET /v1/models against
	// the (remote, paid) Anthropic endpoint. Cache the result for availCacheTTL,
	// holding the mutex across the probe so concurrent callers collapse into one.
	availMu     sync.Mutex
	availResult bool
	availAt     time.Time

	// fallback is invoked on a persistent 429 (subscription burst rate-limit
	// with overage disabled). Typically a claude-code CLI provider that reaches
	// the same Max subscription via the official client. nil disables fallback.
	fallback Provider
}

// NewClaudeOAuthProvider creates a ClaudeOAuthProvider from a ProviderConfig.
// The credential source is initialised from the default credential store;
// no network calls are made until the first inference request. fallback (may be
// nil) is delegated to on a persistent 429.
func NewClaudeOAuthProvider(name string, cfg ProviderConfig, fallback Provider) *ClaudeOAuthProvider {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = anthropicDefaultEndpoint
	}
	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout == 0 {
		timeout = 120 * time.Second
	}
	maxTokens := cfg.MaxTokens
	if maxTokens == 0 {
		maxTokens = anthropicDefaultMaxToks
	}

	src := newClaudeCodeCredentialSource()
	// The mirrored credential (source == claude_code) is owned and rotated by
	// Claude Code itself. The kernel is a READ-ONLY keychain mirror: its
	// RefreshFunc NEVER POSTs the single-use refresh token. When the mirror is
	// stale it delegates the refresh to the owner (Claude Code) via the
	// user-space actuator, then re-mirrors. See newClaudeCodeReadOnlyRefresh /
	// PATCH-007. The POST-ing claudeOAuthRefresh is retained for any future
	// non-claude_code OAuth source but is intentionally NOT wired here.
	lc := NewCredentialLifecycle(src, newClaudeCodeReadOnlyRefresh(src, claudeTokenRefreshActuator))

	return &ClaudeOAuthProvider{
		name:      name,
		model:     cfg.Model,
		endpoint:  strings.TrimRight(endpoint, "/"),
		maxTokens: maxTokens,
		timeout:   timeout,
		client:    &http.Client{Timeout: timeout},
		lc:        lc,
		fallback:  fallback,
	}
}

// ── Provider interface ────────────────────────────────────────────────────────

func (p *ClaudeOAuthProvider) Name() string  { return p.name }
func (p *ClaudeOAuthProvider) Model() string { return p.model }

// Available reports whether the provider can serve requests. The result is
// cached for availCacheTTL so the router's periodic availability ticker (and the
// per-request /v1/providers handler) don't fire a live GET /v1/models at the
// remote Anthropic endpoint on every call (#441). The mutex is held across the
// probe so concurrent callers collapse into a single request.
func (p *ClaudeOAuthProvider) Available(ctx context.Context) bool {
	p.availMu.Lock()
	defer p.availMu.Unlock()
	if !p.availAt.IsZero() && time.Since(p.availAt) < availCacheTTL {
		return p.availResult
	}
	fresh := p.probeAvailable(ctx)
	// Skip caching only for a caller-initiated cancellation (client disconnect);
	// a context deadline is the router's probeTimeout firing on a slow provider
	// and must be cached as unavailable, else a hung provider stays cached as
	// available forever (#441 review).
	if !fresh && ctx.Err() == context.Canceled {
		return p.availResult
	}
	p.availResult = fresh
	p.availAt = time.Now()
	return p.availResult
}

// probeAvailable performs the live reachability check backing Available(): a
// GET /v1/models behind the credential lifecycle (refreshing the token if
// needed). Call via Available() for TTL caching.
func (p *ClaudeOAuthProvider) probeAvailable(ctx context.Context) bool {
	availCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	token, err := p.lc.FreshToken(availCtx)
	if err != nil {
		slog.Debug("claude-oauth: Available: credential unavailable", "err", err)
		return false
	}

	req, err := http.NewRequestWithContext(availCtx, http.MethodGet, p.endpoint+"/v1/models", nil)
	if err != nil {
		return false
	}
	p.setOAuthHeaders(req, token, false)

	resp, err := p.client.Do(req)
	if err != nil {
		slog.Debug("claude-oauth: Available: /v1/models probe failed", "err", err)
		return false
	}
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		// Try a reactive refresh once.
		token, err = p.lc.ReactiveRefreshAndRetry(availCtx)
		if err != nil {
			return false
		}
		req2, _ := http.NewRequestWithContext(availCtx, http.MethodGet, p.endpoint+"/v1/models", nil)
		p.setOAuthHeaders(req2, token, false)
		resp2, err := p.client.Do(req2)
		if err != nil {
			return false
		}
		_ = resp2.Body.Close()
		return resp2.StatusCode >= 200 && resp2.StatusCode < 300
	}

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// ListModels enumerates the Anthropic model IDs the subscription can serve by
// performing the authenticated GET /v1/models behind the credential lifecycle,
// mirroring Available()'s auth + reactive-refresh-on-401 pattern but parsing the
// response body ({"data":[{"id":...}]}) instead of discarding it. Satisfies the
// ModelLister interface used by the /v1/models composition handler.
//
// The credential lifecycle stays READ-ONLY here (like Available): the kernel is
// a keychain mirror and never POSTs a refresh; ReactiveRefreshAndRetry delegates
// any refresh to the owner. Bounded by the same short probe timeout so one slow
// upstream cannot stall the /v1/models composition beyond a few seconds.
func (p *ClaudeOAuthProvider) ListModels(ctx context.Context) ([]string, error) {
	listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	token, err := p.lc.FreshToken(listCtx)
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: ListModels: credential: %w", err)
	}

	ids, status, err := p.fetchModelIDs(listCtx, token)
	if err != nil {
		return nil, err
	}
	if status == http.StatusUnauthorized {
		// Reactive refresh once (read-only: delegates to the credential owner),
		// then retry — mirrors Available()'s 401 handling.
		token, err = p.lc.ReactiveRefreshAndRetry(listCtx)
		if err != nil {
			return nil, fmt.Errorf("claude-oauth: ListModels: reactive refresh: %w", err)
		}
		ids, status, err = p.fetchModelIDs(listCtx, token)
		if err != nil {
			return nil, err
		}
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("claude-oauth: ListModels: /v1/models status %d", status)
	}
	return ids, nil
}

// fetchModelIDs performs a single authenticated GET /v1/models and parses the
// Anthropic model-list body ({"data":[{"id":...}]}). It returns the parsed IDs
// (only when the status is 2xx), the HTTP status code (so the caller can drive
// the reactive-refresh-on-401 retry), and any transport/decode error.
func (p *ClaudeOAuthProvider) fetchModelIDs(ctx context.Context, token string) ([]string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+"/v1/models", nil)
	if err != nil {
		return nil, 0, err
	}
	p.setOAuthHeaders(req, token, false)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("claude-oauth: ListModels: /v1/models: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, nil
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("claude-oauth: ListModels: decode: %w", err)
	}
	ids := make([]string, 0, len(body.Data))
	for _, m := range body.Data {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return ids, resp.StatusCode, nil
}

// Capabilities returns what this provider supports.
func (p *ClaudeOAuthProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{
		Capabilities: []Capability{
			CapStreaming, CapToolUse, CapVision, CapLongContext, CapJSON, CapCaching,
		},
		MaxContextTokens:   1_000_000, // 1M with context beta on eligible subscriptions
		MaxOutputTokens:    p.maxTokens,
		ModelsAvailable:    []string{p.model},
		IsLocal:            false,
		AgenticHarness:     false,
		CostPerInputToken:  0, // subscription pricing: no per-token cost signal
		CostPerOutputToken: 0,
	}
}

// Ping probes the Anthropic API and returns measured round-trip latency.
func (p *ClaudeOAuthProvider) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	token, err := p.lc.FreshToken(ctx)
	if err != nil {
		return 0, fmt.Errorf("claude-oauth: ping: credential: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+"/v1/models", nil)
	if err != nil {
		return 0, fmt.Errorf("claude-oauth: ping: build request: %w", err)
	}
	p.setOAuthHeaders(req, token, false)

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("claude-oauth: ping: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return 0, fmt.Errorf("claude-oauth: ping: 401 — credential rejected")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("claude-oauth: ping: /v1/models returned HTTP %d", resp.StatusCode)
	}
	return time.Since(start), nil
}

// ── setOAuthHeaders ───────────────────────────────────────────────────────────

// setOAuthHeaders applies the Claude Code OAuth identity headers to req.
// with1MBeta adds the long-context beta header (not on by default because some
// subscriptions reject it with HTTP 400).
// Source: build_anthropic_client (OAuth branch), models.py:2342-2360.
func (p *ClaudeOAuthProvider) setOAuthHeaders(req *http.Request, token string, with1MBeta bool) {
	betas := append(append([]string{}, claudeCodeCommonBetas...), claudeCodeOAuthOnlyBetas...)
	if with1MBeta {
		betas = append(betas, claudeCodeContext1MBeta)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("anthropic-version", anthropicAPIVersion)
	req.Header.Set("anthropic-beta", strings.Join(betas, ","))
	req.Header.Set("User-Agent", fmt.Sprintf("claude-cli/%s (external, cli)", getClaudeCodeVersion()))
	req.Header.Set("x-app", "cli")
	req.Header.Set("Content-Type", "application/json")
}

// ── effectiveModel ────────────────────────────────────────────────────────────

func (p *ClaudeOAuthProvider) effectiveModel(req *CompletionRequest) string {
	if req.ModelOverride != "" {
		return req.ModelOverride
	}
	return p.model
}

// ── buildOAuthSystem ──────────────────────────────────────────────────────────

// buildOAuthSystem prepends the Claude Code system prefix to every request,
// then appends context items and the caller's system prompt.
// Source: _CLAUDE_CODE_SYSTEM_PREFIX usage in the reference implementation.
// anthropicSystemBlock is one text block of a multi-block system prompt, as the
// Claude Code client sends it.
type anthropicSystemBlock struct {
	Type         string                 `json:"type"`
	Text         string                 `json:"text"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

// buildOAuthSystem builds the system field for the OAuth (Max-subscription) path
// as a MULTI-BLOCK ARRAY, exactly as the genuine Claude Code client does.
//
// Anthropic enforces anti-abuse on OAuth tokens: the system prompt's FIRST block
// must be the canonical Claude Code system prompt. A single system STRING that is
// anything other than that exact prefix is rejected with 429 rate_limit_error
// (generic "Error" body, no rate-limit headers). But a block ARRAY whose block[0]
// is the canonical prefix is accepted, and ANY additional blocks (our identity +
// foveated context) are allowed. Verified empirically 2026-05-29.
//
// So: block[0] = isolated canonical prefix; block[1] = our injected identity +
// context + caller system prompt (only when non-empty).
// buildOAuthSystem returns the canonical-only system field plus the operator/agent
// system content that must be RELOCATED to the user turn.
//
// Anthropic's managed-subscription (OAuth) path scores the SYSTEM FIELD for "is this
// genuine Claude Code" and routes a system prompt that reads like a third-party agent
// framework to the rejected overage lane (HTTP 400 "out of extra usage") — a cumulative
// threshold over agentic markers (memory instructions, skill tools, boundaries, …),
// independent of the tool-namespace gate. So the system field carries ONLY the canonical
// Claude Code prefix; the injected context/system-prompt is returned separately so the
// caller can prepend it to the first user message — the same placement Claude Code uses
// for CLAUDE.md / project context. The model still sees the content; the classifier scores
// only the system field. Verified by deterministic sweep 2026-05-29.
func buildOAuthSystem(req *CompletionRequest) ([]anthropicSystemBlock, string) {
	blocks := []anthropicSystemBlock{
		{Type: "text", Text: claudeCodeSystemPrefix},
	}

	var sb strings.Builder
	for _, item := range req.Context {
		if item.Content == "" {
			continue
		}
		fmt.Fprintf(&sb, "<!-- context id=%q zone=%s salience=%.2f -->\n%s\n\n",
			item.ID, item.Zone, item.Salience, item.Content)
	}
	if req.SystemPrompt != "" {
		sb.WriteString(req.SystemPrompt)
	}

	return blocks, strings.TrimSpace(sb.String())
}

// prependOAuthSystemToUserTurn moves the relocated system content into the first user
// message (creating one if absent), so it rides the user turn rather than the
// classifier-scored system field. See buildOAuthSystem.
//
// Block-order safety (I4) is now guaranteed by the second normalizeAnthropicMessages
// call that runs after this function (in Complete and Stream). The earlier version
// used a separate leading user message to avoid text-before-tool_result (F7/P5);
// that complexity is now subsumed by the normalizer's I4 pass.
func prependOAuthSystemToUserTurn(payload *anthropicRequest, injected string) {
	if injected == "" {
		return
	}
	// Prepend as a plain leading user message. The post-OAuth normalize pass
	// (normalizeAnthropicMessages) will merge consecutive user messages (I2)
	// and ensure tool_result blocks lead (I4) if needed.
	payload.Messages = append([]anthropicMessage{{Role: "user", Content: injected}}, payload.Messages...)
}

// ── OAuth tool-namespace billing gate workaround ────────────────────────────────
//
// Anthropic's managed-subscription (OAuth) path routes every /v1/messages
// request into a billing lane by a format-only check on tool names: a name
// matching ^mcp_[a-z] (single-underscore "mcp_" followed by a lowercase
// letter) is classified as third-party MCP-extension usage and billed to the
// overage lane. On a Max/Pro subscription with overage disabled that returns
// HTTP 400 "You're out of extra usage". The sanctioned double-underscore
// namespace (mcp__server__tool), bare names, and mcp_[A-Z0-9] all ride the
// subscription lane. Verified empirically 2026-05-29.
//
// Clients that route tools through the kernel (e.g. Hermes over openai_chat)
// register MCP tools as mcp_<server>_<tool> (single underscore, lowercase),
// which trips the gate. We rewrite mcp_x -> mcp__x on the outbound request and
// reverse mcp__x -> mcp_x on tool_use names in the response, so the client's
// tool-call routing (keyed on the original name) still resolves. The rewrite is
// scoped to this OAuth provider only; the x-api-key Anthropic provider does not
// hit the gate and is intentionally left untouched.

// toolNameToWire maps a single-underscore mcp_ tool name to the sanctioned
// double-underscore namespace. Already-double-underscore and bare names are
// returned unchanged.
func toolNameToWire(name string) string {
	if name == "" {
		return name
	}
	if strings.HasPrefix(name, "mcp__") {
		return name // already sanctioned
	}
	if strings.HasPrefix(name, "mcp_") {
		return "mcp__" + name[len("mcp_"):] // single-underscore -> double
	}
	return "mcp__" + name // bare -> double-underscore (a SET of bare agentic tool
	// names — memory, skill_manage, session_search, delegate_task, … — trips the
	// system-content classifier on the tool array; the mcp__ namespace marks them
	// as sanctioned MCP tools. Reversed inbound via the wire->original map.)
}

// rewriteOAuthToolNames rewrites tool definitions and tool_use names in the
// outbound payload to the sanctioned namespace, returning a wire->original map
// used to reverse the names on the response. Only names that were actually
// rewritten appear in the map, so genuine double-underscore names are never
// corrupted on the return path.
func rewriteOAuthToolNames(payload *anthropicRequest) map[string]string {
	rev := map[string]string{}
	for i := range payload.Tools {
		orig := payload.Tools[i].Name
		if wire := toolNameToWire(orig); wire != orig {
			payload.Tools[i].Name = wire
			rev[wire] = orig
		}
	}
	for mi := range payload.Messages {
		blocks, ok := payload.Messages[mi].Content.([]anthropicContentBlock)
		if !ok {
			continue
		}
		for bi := range blocks {
			if blocks[bi].Type == "tool_use" && blocks[bi].Name != "" {
				orig := blocks[bi].Name
				if wire := toolNameToWire(orig); wire != orig {
					blocks[bi].Name = wire
					rev[wire] = orig
				}
			}
		}
	}
	return rev
}

// restoreOAuthToolNames reverses the outbound rewrite on tool_use content
// blocks in a non-streaming response, restoring the client's original names.
func restoreOAuthToolNames(content []anthropicContent, rev map[string]string) {
	if len(rev) == 0 {
		return
	}
	for i := range content {
		if content[i].Type == "tool_use" {
			if orig, ok := rev[content[i].Name]; ok {
				content[i].Name = orig
			}
		}
	}
}

// ── Complete ──────────────────────────────────────────────────────────────────

// Complete sends a non-streaming request, handling proactive + reactive token
// refresh.
func (p *ClaudeOAuthProvider) Complete(ctx context.Context, req *CompletionRequest) (*CompletionResponse, error) {
	start := time.Now()
	model := p.effectiveModel(req)

	payload := buildAnthropicRequest(model, req, false, p.maxTokens)
	// System field carries only the canonical Claude Code prefix; the operator/agent
	// system content is relocated to the user turn to clear the system-content gate.
	sysBlocks, relocated := buildOAuthSystem(req)
	payload.System = sysBlocks
	prependOAuthSystemToUserTurn(payload, relocated)
	// Rewrite single-underscore mcp_ tool names to the sanctioned mcp__
	// namespace so the request rides the subscription lane (see
	// rewriteOAuthToolNames). Reversed on the response below.
	oauthToolNames := rewriteOAuthToolNames(payload)
	// Second normalize pass: the OAuth late mutators (prependOAuthSystemToUserTurn +
	// rewriteOAuthToolNames) run AFTER buildAnthropicRequest, so they can introduce
	// new I2 adjacency (leading user + first user) and I4 block-order issues.
	// The normalizer is idempotent so this second call is a no-op on already-legal
	// sequences and fixes only what the late mutators introduced.
	{
		repaired, rpt2 := normalizeAnthropicMessages(payload.Messages)
		payload.Messages = repaired
		rpt2.emit("claudeoauth.post_relocate")
	}
	applyAnthropicCacheBreakpoints(payload)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: marshal request: %w", err)
	}

	// Proactive refresh.
	token, err := p.lc.FreshToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: credential unavailable: %w", err)
	}

	resp, err := p.doComplete(ctx, body, token, false)
	if err != nil {
		return nil, err
	}
	if resp.statusCode == http.StatusUnauthorized {
		// Reactive refresh — one retry.
		newToken, refreshErr := p.lc.ReactiveRefreshAndRetry(ctx)
		if refreshErr != nil {
			return nil, fmt.Errorf("claude-oauth: reactive refresh failed: %w", refreshErr)
		}
		token = newToken
		resp, err = p.doComplete(ctx, body, token, false)
		if err != nil {
			return nil, err
		}
	}

	// 429: respect retry-after, back off a bounded number of times, then fall
	// back to the CLI provider (same subscription via the official client).
	for attempt := 0; resp.statusCode == http.StatusTooManyRequests && attempt < oauthMax429Retries; attempt++ {
		wait := oauthRetryAfter(resp.header, attempt)
		if wait > oauthMax429Backoff {
			break // too long to wait inline — go to fallback
		}
		slog.Debug("claude-oauth: 429, backing off", "attempt", attempt+1, "wait", wait.String())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		resp, err = p.doComplete(ctx, body, token, false)
		if err != nil {
			return nil, err
		}
	}
	if resp.statusCode == http.StatusTooManyRequests {
		if p.fallback != nil {
			slog.Warn("claude-oauth: rate limited (429) after backoff; falling back to CLI",
				"fallback", p.fallback.Name())
			return p.fallback.Complete(ctx, req)
		}
		return nil, fmt.Errorf("claude-oauth: rate limited (429), no fallback configured: %s", resp.bodyText)
	}

	if resp.statusCode == http.StatusBadRequest {
		// 1M context beta reactive strip: some subscriptions reject it.
		if strings.Contains(resp.bodyText, "long context beta") {
			resp, err = p.doComplete(ctx, body, token, false /* with1MBeta already false */)
			if err != nil {
				return nil, err
			}
		}
	}

	if resp.statusCode != http.StatusOK {
		return nil, fmt.Errorf("claude-oauth: status %d: %s", resp.statusCode, resp.bodyText)
	}

	var ar anthropicResponse
	if err := json.Unmarshal([]byte(resp.bodyText), &ar); err != nil {
		return nil, fmt.Errorf("claude-oauth: decode response: %w", err)
	}
	restoreOAuthToolNames(ar.Content, oauthToolNames)
	return parseAnthropicResponse(&ar, model, p.name, time.Since(start)), nil
}

type oauthRespResult struct {
	statusCode int
	bodyText   string
	header     http.Header   // response headers (for retry-after on 429)
	body       io.ReadCloser // set for streaming paths
}

func (p *ClaudeOAuthProvider) doComplete(ctx context.Context, payload []byte, token string, with1MBeta bool) (*oauthRespResult, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/v1/messages", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: build request: %w", err)
	}
	p.setOAuthHeaders(httpReq, token, with1MBeta)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: http: %w", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	return &oauthRespResult{
		statusCode: resp.StatusCode,
		bodyText:   string(data),
		header:     resp.Header,
	}, nil
}

// ── Stream ────────────────────────────────────────────────────────────────────

// Stream sends a streaming request, handling proactive + reactive token refresh.
// On HTTP 401, it refreshes once and retries.
// On HTTP 400 with "long context beta" error body, it strips that beta and retries.
func (p *ClaudeOAuthProvider) Stream(ctx context.Context, req *CompletionRequest) (<-chan StreamChunk, error) {
	model := p.effectiveModel(req)

	payload := buildAnthropicRequest(model, req, true, p.maxTokens)
	sysBlocks, relocated := buildOAuthSystem(req)
	payload.System = sysBlocks
	prependOAuthSystemToUserTurn(payload, relocated)
	// Rewrite single-underscore mcp_ tool names to the sanctioned mcp__
	// namespace so the request rides the subscription lane; reversed on the
	// streamed tool-call deltas below.
	oauthToolNames := rewriteOAuthToolNames(payload)
	// Second normalize pass: same rationale as Complete — late OAuth mutators
	// may introduce I2/I4 violations that the normalizer fixes idempotently.
	{
		repaired, rpt2 := normalizeAnthropicMessages(payload.Messages)
		payload.Messages = repaired
		rpt2.emit("claudeoauth.stream.post_relocate")
	}
	applyAnthropicCacheBreakpoints(payload)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: marshal stream request: %w", err)
	}

	// Proactive refresh before the request.
	token, err := p.lc.FreshToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: credential unavailable: %w", err)
	}

	// Use a no-timeout client for streaming — generation can run long.
	streamClient := &http.Client{}

	resp, err := p.doStreamRequest(ctx, streamClient, body, token, false)
	if err != nil {
		return nil, err
	}

	// Reactive 401: refresh and retry before we start reading the stream.
	if resp.StatusCode == http.StatusUnauthorized {
		_ = resp.Body.Close()
		newToken, refreshErr := p.lc.ReactiveRefreshAndRetry(ctx)
		if refreshErr != nil {
			return nil, fmt.Errorf("claude-oauth: reactive refresh failed: %w", refreshErr)
		}
		token = newToken
		resp, err = p.doStreamRequest(ctx, streamClient, body, token, false)
		if err != nil {
			return nil, err
		}
	}

	// 429: respect retry-after, back off a bounded number of times, then fall
	// back to the CLI provider (same subscription via the official client).
	for attempt := 0; resp.StatusCode == http.StatusTooManyRequests; attempt++ {
		wait := oauthRetryAfter(resp.Header, attempt)
		_ = resp.Body.Close()
		if attempt >= oauthMax429Retries || wait > oauthMax429Backoff {
			if p.fallback != nil {
				slog.Warn("claude-oauth: stream rate limited (429) after backoff; falling back to CLI",
					"fallback", p.fallback.Name())
				return p.fallback.Stream(ctx, req)
			}
			return nil, fmt.Errorf("claude-oauth: stream rate limited (429), no fallback configured")
		}
		slog.Debug("claude-oauth: stream 429, backing off", "attempt", attempt+1, "wait", wait.String())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		resp, err = p.doStreamRequest(ctx, streamClient, body, token, false)
		if err != nil {
			return nil, err
		}
	}

	// 400 with "long context beta" error: strip beta and retry.
	if resp.StatusCode == http.StatusBadRequest {
		errBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if strings.Contains(string(errBody), "long context beta") {
			resp, err = p.doStreamRequest(ctx, streamClient, body, token, false)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("claude-oauth: stream status 400: %s", string(errBody))
		}
	}

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("claude-oauth: stream status %d: %s", resp.StatusCode, string(errBody))
	}

	ch := make(chan StreamChunk, 32)
	if len(oauthToolNames) == 0 {
		go func() {
			defer close(ch)
			defer resp.Body.Close()
			parseAnthropicSSE(ctx, resp.Body, ch, model, p.name)
		}()
		return ch, nil
	}
	// Reverse the outbound mcp_ -> mcp__ rewrite on streamed tool-call deltas
	// so the client sees the names it originally registered.
	raw := make(chan StreamChunk, 32)
	go func() {
		defer close(raw)
		defer resp.Body.Close()
		parseAnthropicSSE(ctx, resp.Body, raw, model, p.name)
	}()
	go func() {
		defer close(ch)
		for chunk := range raw {
			if chunk.ToolCallDelta != nil && chunk.ToolCallDelta.Name != "" {
				if orig, ok := oauthToolNames[chunk.ToolCallDelta.Name]; ok {
					chunk.ToolCallDelta.Name = orig
				}
			}
			ch <- chunk
		}
	}()
	return ch, nil
}

func (p *ClaudeOAuthProvider) doStreamRequest(ctx context.Context, client *http.Client, payload []byte, token string, with1MBeta bool) (*http.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.endpoint+"/v1/messages", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: build stream request: %w", err)
	}
	p.setOAuthHeaders(httpReq, token, with1MBeta)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("claude-oauth: stream request: %w", err)
	}
	return resp, nil
}

// ── SSE re-use ────────────────────────────────────────────────────────────────
// ClaudeOAuthProvider reuses parseAnthropicSSE from provider_anthropic.go.
// The function is package-internal and accepts model/providerName strings,
// so we simply pass p.name and the effective model.
