// serve_oauth.go — the kernel as an OAuth 2.1 Authorization Server for the
// stage's MCP connector (Phase 3). This file is the plumbing slice (K2): the
// resolved oauth: settings, the discovery document, and route registration.
// The authorization endpoint and the consent approve/deny routes are K3
// (serve_oauth_authorize.go). /token, introspect and the grant list/revoke
// arrive in K4; until then their routes answer a clear
// 501 not_implemented rather than 404, so the route table, the grant-gate
// classification and the manifest are already the final shape.
//
// The OAuth store (serve_oauth_store.go) is built at boot and survives restart
// via RebuildOAuthStoreFromLedger. It is deliberately separate from
// IdentityGrantRegistry: an OAuth token must never satisfy VerifyAny on the
// :6932 gate.
//
// Public vs internal. The public paths (/.well-known/oauth-authorization-server,
// /oauth/*) authenticate inside their own handlers and are exempt from the
// grant gate (isGrantExemptRequest). The internal paths (/v1/oauth/*) sit
// behind the gate, are scoped (oauth:consent, oauth:introspect, admin) and are
// never mounted on the public stage_ui listener.
//
// The whole surface is opt-in (oauth.enabled in kernel.yaml): a kernel with no
// oauth: block registers none of it.
package engine

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// Route constants shared by the registrar, the gate exemption and (K5) the
// stage_ui listener's public-path predicate.
const (
	oauthWellKnownASPath     = "/.well-known/oauth-authorization-server"
	oauthAuthorizePath       = "/oauth/authorize"
	oauthAuthorizeStatusPath = "/oauth/authorize/status/"
	oauthTokenPath           = "/oauth/token"
	oauthPublicPrefix        = "/oauth/"
	oauthInternalPrefix      = "/v1/oauth"
)

const (
	// ScopeOAuthConsent authorizes approving or denying a pending OAuth
	// authorization request. Held only by the stage's studio grant: the stage
	// decides "paired device", the kernel records it.
	ScopeOAuthConsent = "oauth:consent"

	// ScopeOAuthIntrospect authorizes POST /v1/oauth/introspect (RFC 7662),
	// the stage resource server's token check.
	ScopeOAuthIntrospect = "oauth:introspect"
)

// OAuthConfig is the resolved `oauth:` block of kernel.yaml. The zero value is
// "off".
type OAuthConfig struct {
	// Enabled turns the whole AS surface on. Default off.
	Enabled bool
	// Issuer is the public base URL of the AS, e.g. https://stage.myrgic.com.
	// No trailing slash. When empty it is derived from stage_ui.public_host
	// (https), else from the kernel's own loopback address.
	Issuer string
	// Resource is the RFC 8707 resource indicator the AS issues tokens for
	// (the stage's /mcp URL). Empty means Issuer + "/mcp".
	Resource string
	// AllowedClientHosts bounds the CIMD fetch (K3). Empty means ["claude.ai"].
	AllowedClientHosts []string
	// RedirectURIs is the exact-match allowlist of redirect URIs (K3). Empty
	// means ["https://claude.ai/api/mcp/auth_callback"].
	RedirectURIs []string
	// StageNotifyURL is where the kernel POSTs a consent request so the stage
	// can raise a pending item (K3). Empty means http://127.0.0.1:7872/local/oauth-consent.
	StageNotifyURL string
}

// oauthConfigSection is the `oauth:` block as written in kernel.yaml.
type oauthConfigSection struct {
	Enabled            bool     `yaml:"enabled"`
	Issuer             string   `yaml:"issuer"`
	Resource           string   `yaml:"resource"`
	AllowedClientHosts []string `yaml:"allowed_client_hosts"`
	RedirectURIs       []string `yaml:"redirect_uris"`
	StageNotifyURL     string   `yaml:"stage_notify_url"`
}

// applyOAuthSection overlays a kernel.yaml oauth block onto cfg.
func applyOAuthSection(cfg *Config, s *oauthConfigSection) {
	if s == nil {
		return
	}
	cfg.OAuth.Enabled = s.Enabled
	if s.Issuer != "" {
		cfg.OAuth.Issuer = s.Issuer
	}
	if s.Resource != "" {
		cfg.OAuth.Resource = s.Resource
	}
	if len(s.AllowedClientHosts) > 0 {
		cfg.OAuth.AllowedClientHosts = s.AllowedClientHosts
	}
	if len(s.RedirectURIs) > 0 {
		cfg.OAuth.RedirectURIs = s.RedirectURIs
	}
	if s.StageNotifyURL != "" {
		cfg.OAuth.StageNotifyURL = s.StageNotifyURL
	}
}

// oauthSettings is OAuthConfig with every default filled in.
type oauthSettings struct {
	Issuer             string
	Resource           string
	AllowedClientHosts []string
	RedirectURIs       []string
	StageNotifyURL     string
}

// oauthSettings resolves the effective settings from the config.
func (s *Server) oauthSettings() oauthSettings {
	var c OAuthConfig
	if s.cfg != nil {
		c = s.cfg.OAuth
	}
	out := oauthSettings{
		Issuer:             strings.TrimRight(strings.TrimSpace(c.Issuer), "/"),
		Resource:           strings.TrimRight(strings.TrimSpace(c.Resource), "/"),
		AllowedClientHosts: c.AllowedClientHosts,
		RedirectURIs:       c.RedirectURIs,
		StageNotifyURL:     c.StageNotifyURL,
	}
	if out.Issuer == "" {
		switch {
		case s.cfg != nil && s.cfg.StageUI.PublicHost != "":
			out.Issuer = "https://" + s.cfg.StageUI.PublicHost
		case s.cfg != nil:
			host := s.cfg.BindAddr
			if host == "" {
				host = "127.0.0.1"
			}
			out.Issuer = "http://" + host + ":" + strconv.Itoa(s.cfg.Port)
		}
	}
	if out.Resource == "" {
		out.Resource = out.Issuer + "/mcp"
	}
	if len(out.AllowedClientHosts) == 0 {
		out.AllowedClientHosts = []string{"claude.ai"}
	}
	if len(out.RedirectURIs) == 0 {
		out.RedirectURIs = []string{"https://claude.ai/api/mcp/auth_callback"}
	}
	if out.StageNotifyURL == "" {
		out.StageNotifyURL = "http://127.0.0.1:7872/local/oauth-consent"
	}
	return out
}

// oauthASMetadata builds the RFC 8414 authorization-server metadata document.
// It is computed from config only; no request state is involved.
func (s *Server) oauthASMetadata() map[string]any {
	st := s.oauthSettings()
	return map[string]any{
		"issuer":                   st.Issuer,
		"authorization_endpoint":   st.Issuer + oauthAuthorizePath,
		"token_endpoint":           st.Issuer + oauthTokenPath,
		"response_types_supported": []string{"code"},
		"grant_types_supported":    []string{"authorization_code", "refresh_token"},
		// S256 only: "plain" is not advertised and is refused at /authorize.
		"code_challenge_methods_supported": []string{"S256"},
		// Public clients only; the client authenticates by PKCE, not a secret.
		"token_endpoint_auth_methods_supported": []string{"none"},
		// draft-ietf-oauth-client-id-metadata-document: client_id is an https
		// URL whose document the AS fetches (SSRF-guarded, K3).
		"client_id_metadata_document_supported": true,
		"scopes_supported":                      []string{OAuthScopeMCPConnector},
	}
}

// registerOAuthRoutes registers the OAuth AS routes on mux. No-op unless
// oauth.enabled. The metadata route is live; the rest are 501 placeholders
// until K3 (/authorize, approve/deny) and K4 (/token, introspect, grants).
func (s *Server) registerOAuthRoutes(mux *http.ServeMux) {
	if s.cfg == nil || !s.cfg.OAuth.Enabled {
		return
	}
	s.route(mux, "GET "+oauthWellKnownASPath, s.handleOAuthASMetadata)

	// Public, self-authenticating (K3/K4).
	s.route(mux, "GET "+oauthAuthorizePath, s.handleOAuthAuthorize)                    // K3
	s.route(mux, "GET "+oauthAuthorizeStatusPath+"{id}", s.handleOAuthAuthorizeStatus) // K3
	s.route(mux, "POST "+oauthTokenPath, s.handleOAuthToken)                           // K4

	// Internal, grant-gated, never on the public listener (K3/K4).
	s.route(mux, "POST "+oauthInternalPrefix+"/requests/{id}/approve", s.handleOAuthRequestApprove) // K3
	s.route(mux, "POST "+oauthInternalPrefix+"/requests/{id}/deny", s.handleOAuthRequestDeny)       // K3
	s.route(mux, "POST "+oauthInternalPrefix+"/introspect", s.handleOAuthIntrospect)                // K4
	s.route(mux, "GET "+oauthInternalPrefix+"/grants", s.handleOAuthGrantList)                      // K4
	s.route(mux, "POST "+oauthInternalPrefix+"/grants/{id}/revoke", s.handleOAuthGrantRevoke)       // K4
}

// handleOAuthASMetadata serves GET /.well-known/oauth-authorization-server.
// Not cacheable (the issuer is config and a stale doc would mislead a client),
// and readable cross-origin so browser tools (MCP Inspector) can discover.
func (s *Server) handleOAuthASMetadata(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(s.oauthASMetadata())
}

// handleOAuthNotImplemented is the placeholder for routes a later slice fills.
func (s *Server) handleOAuthNotImplemented(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSONError(w, http.StatusNotImplemented, "not_implemented",
		"this OAuth endpoint is registered but not implemented yet")
}

// isOAuthPublicPath reports whether path is one of the public AS paths that
// authenticate inside their own handlers: the discovery document and /oauth/*.
// /v1/oauth/* is NOT public.
func isOAuthPublicPath(path string) bool {
	return path == oauthWellKnownASPath || strings.HasPrefix(path, oauthPublicPrefix)
}

// isOAuthInternalPath reports whether path is under /v1/oauth.
func isOAuthInternalPath(path string) bool {
	return path == oauthInternalPrefix || strings.HasPrefix(path, oauthInternalPrefix+"/")
}

// oauthRequiredScope maps an internal OAuth route onto its scope.
func oauthRequiredScope(path string) string {
	switch {
	case strings.HasPrefix(path, oauthInternalPrefix+"/requests/"):
		return ScopeOAuthConsent
	case path == oauthInternalPrefix+"/introspect":
		return ScopeOAuthIntrospect
	default:
		return ScopeAdmin
	}
}
