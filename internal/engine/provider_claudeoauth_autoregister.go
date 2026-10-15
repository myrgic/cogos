// provider_claudeoauth_autoregister.go — discovery-not-declaration
// auto-registration of the node-local "claude-oauth" provider.
//
// Operator ruling: every node's kernel should automatically discover and
// register a Claude OAuth provider from the node's own existing Claude Code
// credentials. No providers.yaml entry, no token paste — the kernel notices
// the credential and wires up the provider itself (kill-per-interface-token-
// paste ruling, task 60 lineage).
//
// Precedence (declaration wins over discovery):
//  1. An explicit "claude-oauth" entry in providers.yaml (or providers.local.yaml)
//     is handled entirely by BuildRouter's normal providers.Providers loop and
//     is registered/skipped there via IsEnabled(); this file's auto-registration
//     never runs for that name once such an entry exists.
//  2. providers.claude_oauth.auto: false disables auto-registration outright.
//  3. Otherwise, probe the local credential sources (macOS keychain,
//     CLAUDE_CODE_OAUTH_TOKEN env var, ~/.claude/.credentials.json or
//     %USERPROFILE%\.claude\.credentials.json / $CLAUDE_CONFIG_DIR). If a
//     credential resolves, register "claude-oauth".
//
// Re-checked, not one-shot: maybeAutoRegisterClaudeOAuth is called once from
// BuildRouter (boot) AND from SimpleRouter's background availability
// maintainer ticker (see router.go Start/probeAll), so a node notices a
// freshly-created credential — e.g. the operator's first `claude /login` on
// this machine — without a kernel restart. The check is cheap (an in-memory
// map lookup guard plus, only when not yet registered, one credential-source
// probe) so running it every availTTL tick (10s default) is negligible.
package engine

import "log/slog"

// claudeOAuthAutoProviderName is the provider name used for both the manual
// providers.yaml entry and the auto-registered instance, so an explicit entry
// always shadows discovery under the same identity.
const claudeOAuthAutoProviderName = "claude-oauth"

// claudeOAuthAutoDefaultModel is the model advertised by an auto-registered
// claude-oauth provider when providers.yaml carries no explicit entry (and
// therefore no explicit model). It matches the canonical "sonnet" / "foreground"
// alias target in resolve.go so an auto-registered node behaves like an
// explicitly-configured one for the common frontier-routing paths; requests
// that carry their own ModelOverride (the normal path — see effectiveModel)
// ignore this value entirely.
const claudeOAuthAutoDefaultModel = "claude-sonnet-5"

// claudeOAuthAutoCredentialProbe constructs the CredentialSource used to
// decide whether a discoverable credential exists. It is a package-level var
// (not a direct newClaudeCodeCredentialSource() call inline below) so tests
// can substitute a stub source and stay hermetic: newClaudeCodeCredentialSource
// unconditionally tries the real macOS keychain first on darwin (see
// claudeCodeCredentialSource.Resolve), which would make auto-registration
// tests depend on whatever Claude Code credentials happen to be logged into
// the machine running `go test` — exactly the non-determinism the rest of
// this codebase's tests avoid by constructing claudeCodeCredentialSource{}
// literals directly instead of going through the constructor.
var claudeOAuthAutoCredentialProbe = func() CredentialSource {
	return newClaudeCodeCredentialSource()
}

// maybeAutoRegisterClaudeOAuth probes for a node-local Claude Code credential
// and registers a "claude-oauth" provider on router if one is found and no
// higher-precedence signal (explicit config, already-registered) says
// otherwise. Safe to call repeatedly — it is idempotent and cheap once a
// provider is registered (an already-registered check short-circuits before
// any credential probe).
//
// procMgr may be nil; makeProvider lazily creates one for the claude-oauth
// case's CLI fallback (see router.go makeProvider, case "claude-oauth").
func maybeAutoRegisterClaudeOAuth(router *SimpleRouter, pcfg ProvidersConfig, procMgr *ProcessManager) {
	if router == nil {
		return
	}

	// 1. Declaration wins over discovery. If providers.yaml (or the local
	// overlay) already names claude-oauth explicitly, BuildRouter's normal
	// loop owns it entirely — including honouring `enabled: false`. Do not
	// second-guess an explicit disable by auto-registering anyway.
	if _, explicit := pcfg.Providers[claudeOAuthAutoProviderName]; explicit {
		return
	}

	// 2. Idempotent: nothing to do if a previous boot or a previous tick
	// already auto-registered it.
	if _, already := router.ProviderForName(claudeOAuthAutoProviderName); already {
		return
	}

	// 3. Explicit opt-out.
	if !pcfg.ClaudeOAuth.IsAutoEnabled() {
		slog.Info("router: claude-oauth auto-registration skipped",
			"reason", "disabled (providers.yaml claude_oauth.auto: false)")
		return
	}

	// 4. Probe the credential sources. A cheap, local, non-network check:
	// Resolve() reads the keychain (2s bounded exec)/env/file only — no HTTP.
	src := claudeOAuthAutoCredentialProbe()
	if _, err := src.Resolve(); err != nil {
		slog.Debug("router: claude-oauth auto-registration skipped",
			"reason", "no local Claude Code credential found", "err", err)
		return
	}

	p, err := makeProvider(claudeOAuthAutoProviderName, ProviderConfig{
		Type:     claudeOAuthAutoProviderName,
		Model:    claudeOAuthAutoDefaultModel,
		CacheTTL: pcfg.ClaudeOAuth.CacheTTL,
	}, procMgr)
	if err != nil {
		slog.Warn("router: claude-oauth auto-registration failed", "err", err)
		return
	}
	router.RegisterProvider(p)
	slog.Info("router: claude-oauth auto-registered",
		"reason", "local Claude Code credential discovered", "model", claudeOAuthAutoDefaultModel)
}
