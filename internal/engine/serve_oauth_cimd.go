// serve_oauth_cimd.go — fetching and validating an OAuth client's Client ID
// Metadata Document (draft-ietf-oauth-client-id-metadata-document) for the
// kernel's /oauth/authorize (Phase 3 OAuth, slice K3).
//
// With CIMD the client_id IS an https URL and the AS fetches the document the
// URL names. That is a server-side fetch of a URL a stranger chose, so every
// step here is an SSRF control:
//
//   - the host must be on the operator's allowlist (oauth.allowed_client_hosts,
//     default claude.ai) — the allowlist is the safety, the fetch is the
//     CIMD-compliance and a second check on redirect_uri;
//   - https only, default port only, no userinfo, no fragment;
//   - the host is resolved here and EVERY address must be public. The dial
//     then goes to a vetted address, not back through DNS, so a rebinding
//     answer cannot swap in a private one between check and use;
//   - redirects are never followed (a 3xx is a failure), so an allowlisted
//     host cannot bounce the AS onto an internal address;
//   - 5 s timeout, 64 KiB body cap, application/json only;
//   - the answer is cached for 5 minutes, bounded in size.
//
// The fetcher takes its DNS lookup and final dial as fields so tests can
// exercise the real validation path against an httptest server; production
// uses the system resolver and a plain net.Dialer.
package engine

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	oauthCIMDTimeout   = 5 * time.Second
	oauthCIMDMaxBody   = 64 << 10
	oauthCIMDCacheTTL  = 5 * time.Minute
	oauthCIMDCacheSize = 64
)

// ErrOAuthClientMetadata wraps every CIMD fetch/validation failure. /authorize
// maps it to invalid_client without leaking which check tripped.
var ErrOAuthClientMetadata = errors.New("oauth client metadata rejected")

// oauthClientMetadata is the validated subset of a CIMD document.
type oauthClientMetadata struct {
	ClientID     string
	ClientName   string
	RedirectURIs []string
	fetched      time.Time
}

func (m *oauthClientMetadata) hasRedirect(uri string) bool {
	for _, u := range m.RedirectURIs {
		if u == uri {
			return true
		}
	}
	return false
}

// oauthClientMetadataFetcher fetches and caches CIMD documents.
type oauthClientMetadataFetcher struct {
	// lookup resolves a host to addresses. nil means net.DefaultResolver.
	lookup func(ctx context.Context, host string) ([]net.IP, error)
	// dial opens the connection to a vetted ip:port. nil means a net.Dialer.
	dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// tlsConfig overrides the TLS client config (tests trust an httptest CA).
	tlsConfig *tls.Config
	now       func() time.Time

	mu    sync.Mutex
	cache map[string]*oauthClientMetadata
}

func newOAuthClientMetadataFetcher() *oauthClientMetadataFetcher {
	return &oauthClientMetadataFetcher{
		now:   func() time.Time { return time.Now().UTC() },
		cache: make(map[string]*oauthClientMetadata),
	}
}

// oauthPublicIP reports whether ip is a globally routable unicast address.
// Everything an SSRF would aim at — loopback, RFC 1918, link-local (cloud
// metadata 169.254.169.254), CGNAT, ULA, multicast, reserved — is refused.
func oauthPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsMulticast() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() {
		return false
	}
	for _, n := range oauthReservedNets {
		if n.Contains(ip) {
			return false
		}
	}
	return true
}

var oauthReservedNets = func() []*net.IPNet {
	var out []*net.IPNet
	for _, c := range []string{
		"0.0.0.0/8",       // "this network"
		"100.64.0.0/10",   // CGNAT
		"192.0.0.0/24",    // IETF protocol assignments
		"192.0.2.0/24",    // TEST-NET-1
		"198.18.0.0/15",   // benchmarking
		"198.51.100.0/24", // TEST-NET-2
		"203.0.113.0/24",  // TEST-NET-3
		"240.0.0.0/4",     // reserved + broadcast
		"64:ff9b::/96",    // NAT64: can embed a private v4
		"2001:db8::/32",   // documentation
		"100::/64",        // discard-only
	} {
		_, n, err := net.ParseCIDR(c)
		if err == nil {
			out = append(out, n)
		}
	}
	return out
}()

// validateClientIDURL checks the shape of a CIMD client_id and its host
// against the allowlist. It does no I/O.
func validateClientIDURL(clientID string, allowedHosts []string) (*url.URL, error) {
	u, err := url.Parse(clientID)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("%w: client_id must be an https URL", ErrOAuthClientMetadata)
	}
	if u.User != nil || u.Fragment != "" || u.Path == "" || u.Path == "/" {
		return nil, fmt.Errorf("%w: client_id must be an https URL with a path, no userinfo, no fragment", ErrOAuthClientMetadata)
	}
	if p := u.Port(); p != "" && p != "443" {
		return nil, fmt.Errorf("%w: client_id port not allowed", ErrOAuthClientMetadata)
	}
	host := strings.ToLower(u.Hostname())
	ok := false
	for _, h := range allowedHosts {
		if strings.EqualFold(strings.TrimSpace(h), host) {
			ok = true
			break
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w: client_id host %q is not on the allowlist", ErrOAuthClientMetadata, host)
	}
	return u, nil
}

// Fetch returns the validated metadata for clientID. The cache is keyed by
// the exact client_id URL and honours the allowlist on every call (a host
// removed from the allowlist stops resolving at once).
func (f *oauthClientMetadataFetcher) Fetch(ctx context.Context, clientID string, allowedHosts []string) (*oauthClientMetadata, error) {
	u, err := validateClientIDURL(clientID, allowedHosts)
	if err != nil {
		return nil, err
	}
	now := f.now()
	f.mu.Lock()
	if m, ok := f.cache[clientID]; ok && now.Sub(m.fetched) < oauthCIMDCacheTTL {
		f.mu.Unlock()
		return m, nil
	}
	f.mu.Unlock()

	body, err := f.get(ctx, u)
	if err != nil {
		return nil, err
	}
	m, err := parseClientMetadata(clientID, body)
	if err != nil {
		return nil, err
	}
	m.fetched = now
	f.mu.Lock()
	if len(f.cache) >= oauthCIMDCacheSize {
		for k, v := range f.cache { // evict anything stale, else one arbitrary entry
			if now.Sub(v.fetched) >= oauthCIMDCacheTTL {
				delete(f.cache, k)
			}
		}
		for k := range f.cache {
			if len(f.cache) < oauthCIMDCacheSize {
				break
			}
			delete(f.cache, k)
		}
	}
	f.cache[clientID] = m
	f.mu.Unlock()
	return m, nil
}

// get performs the guarded GET and returns the capped body.
func (f *oauthClientMetadataFetcher) get(ctx context.Context, u *url.URL) ([]byte, error) {
	host := u.Hostname()
	ips, err := f.resolve(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve %s: %v", ErrOAuthClientMetadata, host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("%w: %s did not resolve", ErrOAuthClientMetadata, host)
	}
	for _, ip := range ips {
		if !oauthPublicIP(ip) {
			return nil, fmt.Errorf("%w: %s resolves to a non-public address", ErrOAuthClientMetadata, host)
		}
	}

	dialer := &net.Dialer{Timeout: oauthCIMDTimeout}
	dial := f.dial
	if dial == nil {
		dial = dialer.DialContext
	}
	tlsCfg := f.tlsConfig
	if tlsCfg == nil {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	tlsCfg = tlsCfg.Clone()
	tlsCfg.ServerName = host // verify against the name, dial the vetted IP
	tr := &http.Transport{
		Proxy: nil, // never via an environment proxy: it would see an internal hop
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var last error
			for _, ip := range ips { // dial the vetted addresses, not DNS again
				c, err := dial(ctx, network, net.JoinHostPort(ip.String(), "443"))
				if err == nil {
					return c, nil
				}
				last = err
			}
			return nil, last
		},
		TLSClientConfig:       tlsCfg,
		DisableKeepAlives:     true,
		ResponseHeaderTimeout: oauthCIMDTimeout,
		TLSHandshakeTimeout:   oauthCIMDTimeout,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{
		Transport:     tr,
		Timeout:       oauthCIMDTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	ctx, cancel := context.WithTimeout(ctx, oauthCIMDTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOAuthClientMetadata, err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "cogos-oauth-as/1")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: fetch: %v", ErrOAuthClientMetadata, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: metadata document answered %d (redirects are not followed)", ErrOAuthClientMetadata, resp.StatusCode)
	}
	mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mt != "application/json" && !strings.HasSuffix(mt, "+json") {
		return nil, fmt.Errorf("%w: metadata document content-type %q is not JSON", ErrOAuthClientMetadata, mt)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, oauthCIMDMaxBody+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrOAuthClientMetadata, err)
	}
	if len(body) > oauthCIMDMaxBody {
		return nil, fmt.Errorf("%w: metadata document exceeds %d bytes", ErrOAuthClientMetadata, oauthCIMDMaxBody)
	}
	return body, nil
}

func (f *oauthClientMetadataFetcher) resolve(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil { // an IP literal is checked as-is
		return []net.IP{ip}, nil
	}
	if f.lookup != nil {
		return f.lookup(ctx, host)
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	out := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.IP)
	}
	return out, nil
}

// parseClientMetadata validates the document against the client_id it was
// fetched from.
func parseClientMetadata(clientID string, body []byte) (*oauthClientMetadata, error) {
	var doc struct {
		ClientID                string   `json:"client_id"`
		ClientName              string   `json:"client_name"`
		RedirectURIs            []string `json:"redirect_uris"`
		TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("%w: metadata document is not valid JSON", ErrOAuthClientMetadata)
	}
	if doc.ClientID != clientID {
		return nil, fmt.Errorf("%w: document client_id does not match the URL it was fetched from", ErrOAuthClientMetadata)
	}
	if doc.TokenEndpointAuthMethod != "none" {
		return nil, fmt.Errorf("%w: token_endpoint_auth_method must be none (public client)", ErrOAuthClientMetadata)
	}
	if len(doc.RedirectURIs) == 0 || len(doc.RedirectURIs) > 16 {
		return nil, fmt.Errorf("%w: redirect_uris must hold 1 to 16 entries", ErrOAuthClientMetadata)
	}
	return &oauthClientMetadata{
		ClientID:     clientID,
		ClientName:   sanitizeOAuthClientName(doc.ClientName),
		RedirectURIs: doc.RedirectURIs,
	}, nil
}

// sanitizeOAuthClientName keeps a client-supplied display name safe to show
// on the authorize page and in the stage's consent card: printable, short.
func sanitizeOAuthClientName(s string) string {
	var b strings.Builder
	n := 0
	for _, r := range s {
		if r < 0x20 || r == 0x7f || r == '<' || r == '>' {
			continue
		}
		b.WriteRune(r)
		if n++; n >= 64 {
			break
		}
	}
	return strings.TrimSpace(b.String())
}
