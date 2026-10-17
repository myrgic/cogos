// managed_session_classes.go — runtime classes for kernel-managed sessions
// (rooms RFC §5.2). A runtime class is a kernel-config-declared bundle:
// program argv, Hermes profile, an allowlisted environment, a cwd policy and a
// toolset label. The KERNEL owns it; a create request only NAMES a class.
//
//	# .cog/config/kernel.yaml
//	managedSessions:
//	  vars: {hermes_py: /opt/hermes/bin/python, stage: /srv/stage, profile_home: /srv/profiles/cog}
//	  runtimeClasses:
//	    chat:
//	      argv: ["{hermes_py}", "{stage}/acp_guest.py"]
//	      profile: cog
//	      toolset: stage-guest
//	      env: {inherit: [PATH, HOME, LANG], drop: [HERMES_YOLO_MODE, "STAGE_COLLAB_*"], set: {HERMES_HOME: "{profile_home}"}}
//
// A request that names no class gets today's behavior unchanged (the driver's
// Command/Env, the request's profile): the built-in default class. A request
// that names a class may not also carry profile: the class owns it. The
// request's mcp_servers are refused too unless the class's mcpServers policy
// is "request", which lets the class accept them; a rejected field is visible
// where a silent override is not.
//
// Authorization: whoever may create a class is decided by the HTTP layer from
// the caller's grant (see managedSessionAuthority); the driver receives the
// decision as HermesACPOpts.AllowClass and applies it once the effective class
// (named, or inherited from a fork parent) is known.
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ManagedSessionsConfig is the `managedSessions:` section of kernel.yaml.
type ManagedSessionsConfig struct {
	// Vars are substituted for {name} in argv, env.set values, cwd and profile.
	// Per-session vars ({profile}) are added at resolution time.
	Vars           map[string]string       `yaml:"vars,omitempty"`
	RuntimeClasses map[string]RuntimeClass `yaml:"runtimeClasses,omitempty"`
}

// RuntimeClass is one declared class.
type RuntimeClass struct {
	Argv    []string `yaml:"argv"`
	Profile string   `yaml:"profile,omitempty"`
	Env     struct {
		// Inherit is the allowlist of names (exact, or trailing-* prefix)
		// copied from the kernel's base environment. Empty = inherit nothing.
		Inherit []string `yaml:"inherit,omitempty"`
		// Drop removes names (exact or trailing-*) after Inherit.
		Drop []string `yaml:"drop,omitempty"`
		// Set adds fixed variables last; values may use {vars}.
		Set map[string]string `yaml:"set,omitempty"`
	} `yaml:"env"`
	// Cwd with CwdPolicy: "request" (default) takes the request's cwd and
	// falls back to Cwd; "fixed" always uses Cwd and refuses a different one.
	Cwd       string `yaml:"cwd,omitempty"`
	CwdPolicy string `yaml:"cwdPolicy,omitempty"`
	// McpServers is the class's MCP-server policy:
	//   "none" (default) — no MCP servers; a request may not supply any.
	//   "configured"     — the kernel's own servers for this class (today:
	//                      none); a request may not supply any.
	//   "request"        — the class ACCEPTS the mcp_servers the create
	//                      request carries, so a kernel-launched seat (e.g.
	//                      the stage's per-room voice server) can be handed
	//                      its tools per session. The word matches CwdPolicy,
	//                      which already means "take the request's value";
	//                      the resolved set is reported in the receipt.
	// Only "request" lets request-supplied servers reach the session.
	McpServers string `yaml:"mcpServers,omitempty"`
	// Toolset is the label of the toolset the program pins (receipt/audit
	// metadata; the program, not the kernel, enforces it).
	Toolset string `yaml:"toolset,omitempty"`
}

// ResolvedRuntimeClass is a class bound to concrete values for one process.
type ResolvedRuntimeClass struct {
	Name    string
	Argv    []string
	Env     []string
	Profile string
	Cwd     string // "" = request supplied none and the class has none
	Toolset string
	// McpServers is the class's mcpServers policy ("", "none", "configured"
	// or "request"); "request" means the create request's servers are used.
	McpServers string
}

// RuntimeClassReceipt records what a session was actually started with, so a
// later comparison against the registry can show drift (RFC §5.2).
type RuntimeClassReceipt struct {
	Class         string `json:"class"`
	ArgvSHA256    string `json:"argv_sha256"`
	Profile       string `json:"profile,omitempty"`
	Toolset       string `json:"toolset,omitempty"`
	WrapperSHA256 string `json:"wrapper_sha256,omitempty"` // content hash of the first regular-file argv element after argv[0]
	// McpServers is the server list the session was actually given (only
	// non-empty when the class's policy is "request"), so a session's tools
	// are auditable from its receipt.
	McpServers []json.RawMessage `json:"mcp_servers,omitempty"`
}

// RuntimeClassMcpRequest is the mcpServers policy under which a class accepts
// the MCP servers its create request carries (RuntimeClass.McpServers).
const RuntimeClassMcpRequest = "request"

// Errors for class handling. Unknown/invalid map to 400, forbidden to 403.
var ErrUnknownRuntimeClass = fmt.Errorf("%w: unknown runtime class", ErrInvalidManagedSession)

var (
	classNameRe       = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	classPlaceholders = regexp.MustCompile(`\{([a-zA-Z0-9_]+)\}`)
)

// RuntimeClassRegistry is the validated, immutable set of declared classes.
type RuntimeClassRegistry struct {
	vars    map[string]string
	classes map[string]RuntimeClass
}

// NewRuntimeClassRegistry validates cfg. A nil cfg yields an empty registry
// (every named class is unknown). Any invalid class fails the whole config:
// a half-loaded registry would let a typo silently remove a restriction.
func NewRuntimeClassRegistry(cfg *ManagedSessionsConfig) (*RuntimeClassRegistry, error) {
	r := &RuntimeClassRegistry{vars: map[string]string{}, classes: map[string]RuntimeClass{}}
	if cfg == nil {
		return r, nil
	}
	for k, v := range cfg.Vars {
		r.vars[k] = v
	}
	var errs []error
	for name, c := range cfg.RuntimeClasses {
		if err := r.validate(name, c); err != nil {
			errs = append(errs, fmt.Errorf("runtime class %q: %w", name, err))
			continue
		}
		r.classes[name] = c
	}
	if len(errs) > 0 {
		return &RuntimeClassRegistry{vars: map[string]string{}, classes: map[string]RuntimeClass{}}, errors.Join(errs...)
	}
	return r, nil
}

func (r *RuntimeClassRegistry) validate(name string, c RuntimeClass) error {
	if !classNameRe.MatchString(name) {
		return fmt.Errorf("name must match %s", classNameRe)
	}
	if len(c.Argv) == 0 || c.Argv[0] == "" {
		return errors.New("argv is required")
	}
	switch c.CwdPolicy {
	case "", "request":
	case "fixed":
		if c.Cwd == "" {
			return errors.New("cwdPolicy fixed requires cwd")
		}
	default:
		return fmt.Errorf("cwdPolicy %q (want request|fixed)", c.CwdPolicy)
	}
	switch c.McpServers {
	case "", "none", "configured", RuntimeClassMcpRequest:
	default:
		return fmt.Errorf("mcpServers %q (want none|configured|request)", c.McpServers)
	}
	// Every placeholder must resolve against vars (+ the per-session {profile}).
	var texts []string
	texts = append(texts, c.Argv...)
	texts = append(texts, c.Cwd, c.Profile)
	for _, v := range c.Env.Set {
		texts = append(texts, v)
	}
	for _, t := range texts {
		for _, m := range classPlaceholders.FindAllStringSubmatch(t, -1) {
			if _, ok := r.vars[m[1]]; !ok && m[1] != "profile" {
				return fmt.Errorf("placeholder {%s} is not declared in managedSessions.vars", m[1])
			}
		}
	}
	return nil
}

// Has reports whether name is declared.
func (r *RuntimeClassRegistry) Has(name string) bool {
	if r == nil {
		return false
	}
	_, ok := r.classes[name]
	return ok
}

// Names lists declared classes, sorted.
func (r *RuntimeClassRegistry) Names() []string {
	if r == nil {
		return nil
	}
	out := make([]string, 0, len(r.classes))
	for n := range r.classes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (r *RuntimeClassRegistry) expand(s, profile string) string {
	return classPlaceholders.ReplaceAllStringFunc(s, func(m string) string {
		key := m[1 : len(m)-1]
		if key == "profile" {
			return profile
		}
		return r.vars[key]
	})
}

// Resolve binds a class to concrete argv/env/profile/cwd. baseEnv is the
// kernel's environment (KEY=VALUE) that Inherit draws from. reqCwd is the
// request's cwd ("" if none). The request's profile is not an input: a class
// owns its profile.
func (r *RuntimeClassRegistry) Resolve(name string, baseEnv []string, reqCwd string) (*ResolvedRuntimeClass, error) {
	if r == nil || !r.Has(name) {
		return nil, fmt.Errorf("%w %q (declared: %s)", ErrUnknownRuntimeClass, name, strings.Join(r.Names(), ", "))
	}
	c := r.classes[name]
	profile := r.expand(c.Profile, "")
	out := &ResolvedRuntimeClass{Name: name, Profile: profile, Toolset: c.Toolset, McpServers: c.McpServers}
	for _, a := range c.Argv {
		out.Argv = append(out.Argv, r.expand(a, profile))
	}
	classCwd := r.expand(c.Cwd, profile)
	switch {
	case c.CwdPolicy == "fixed":
		if reqCwd != "" && reqCwd != classCwd {
			return nil, fmt.Errorf("%w: runtime class %q pins cwd; the request may not set it", ErrInvalidManagedSession, name)
		}
		out.Cwd = classCwd
	case reqCwd != "":
		out.Cwd = reqCwd
	default:
		out.Cwd = classCwd
	}
	out.Env = buildClassEnv(baseEnv, c, func(v string) string { return r.expand(v, profile) })
	return out, nil
}

// matchEnvName: exact, or prefix when pattern ends in "*".
func matchEnvName(pattern, name string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == name
}

// buildClassEnv is the allowlist: inherit, then drop, then set. Nothing in
// baseEnv reaches the child unless Inherit names it.
func buildClassEnv(baseEnv []string, c RuntimeClass, expand func(string) string) []string {
	var env []string
	for _, kv := range baseEnv {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		inherited := false
		for _, p := range c.Env.Inherit {
			if matchEnvName(p, k) {
				inherited = true
				break
			}
		}
		if !inherited {
			continue
		}
		dropped := false
		for _, p := range c.Env.Drop {
			if matchEnvName(p, k) {
				dropped = true
				break
			}
		}
		if !dropped {
			// Env.Set wins over an inherited value of the same name: never
			// emit two entries for one key (which one a child sees would
			// depend on its libc).
			if _, set := c.Env.Set[k]; set {
				continue
			}
			env = append(env, kv)
		}
	}
	keys := make([]string, 0, len(c.Env.Set))
	for k := range c.Env.Set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		env = append(env, k+"="+expand(c.Env.Set[k]))
	}
	return env
}

// Receipt computes the resolution receipt for a resolved class.
func (rc *ResolvedRuntimeClass) Receipt() *RuntimeClassReceipt {
	h := sha256.Sum256([]byte(strings.Join(rc.Argv, "\x00")))
	rec := &RuntimeClassReceipt{Class: rc.Name, ArgvSHA256: hex.EncodeToString(h[:]), Profile: rc.Profile, Toolset: rc.Toolset}
	for _, a := range rc.Argv[1:] {
		if !filepath.IsAbs(a) {
			continue
		}
		if st, err := os.Stat(a); err != nil || !st.Mode().IsRegular() {
			continue
		}
		if data, err := os.ReadFile(a); err == nil {
			w := sha256.Sum256(data)
			rec.WrapperSHA256 = hex.EncodeToString(w[:])
			break
		}
	}
	return rec
}
