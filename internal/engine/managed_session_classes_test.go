package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/myrgic/cogos/internal/acprpc/acpfake"
)

// classTestConfig declares two classes whose "program" is the test binary run
// as the fake ACP agent, wrapped in sh so the child records what it was given
// (its argv marker and its full environment) into a file the test reads back.
// That makes argv/env selection observable without a real Hermes.
func classTestConfig(t *testing.T) (*ManagedSessionsConfig, string) {
	t.Helper()
	out := t.TempDir()
	script := `echo "class=$1" > "$OUT/$1.argv"; env > "$OUT/$1.env"; exec "$0" -test.run='^$'`
	mk := func(name, profile, toolset string) RuntimeClass {
		var c RuntimeClass
		c.Argv = []string{"/bin/sh", "-c", script, os.Args[0], name}
		// $0 is the test binary, $1 the class name (sh -c script arg0 arg1...).
		c.Profile = profile
		c.Toolset = toolset
		c.Env.Inherit = []string{"PATH", "KEEP_*", acpfake.EnvVar}
		c.Env.Drop = []string{"KEEP_DROPPED"}
		c.Env.Set = map[string]string{"OUT": out, "HERMES_HOME": "{profile_home}/" + name, "CLASS_PROFILE": "{profile}"}
		return c
	}
	return &ManagedSessionsConfig{
		Vars: map[string]string{"profile_home": "/profiles"},
		RuntimeClasses: map[string]RuntimeClass{
			"chat":   mk("chat", "guest", "stage-guest"),
			"collab": mk("collab", "collab", "stage-collab"),
		},
	}, out
}

func classDriver(t *testing.T) (*HermesACPDriver, string) {
	t.Helper()
	return classDriverAt(t, t.TempDir())
}

// classDriverAt builds a class driver over recordDir; two drivers sharing one
// recordDir model a kernel restart (fresh in-memory map, same durable records).
func classDriverAt(t *testing.T, recordDir string) (*HermesACPDriver, string) {
	t.Helper()
	cfg, out := classTestConfig(t)
	reg, err := NewRuntimeClassRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := fakeHermesDriver(t, func(c *HermesACPDriverConfig) {
		c.Classes = reg
		c.RecordDir = recordDir
		c.Env = append(os.Environ(), acpfake.EnvVar+"=1", "KEEP_ME=yes", "KEEP_DROPPED=no", "SECRET_TOKEN=leak", "HERMES_YOLO_MODE=1")
	})
	return d, out
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRuntimeClass_ResolvesArgvEnvProfilePerClass(t *testing.T) {
	d, out := classDriver(t)
	hashes := map[string]bool{}
	for _, name := range []string{"chat", "collab"} {
		s, err := d.Create(context.Background(), HermesACPOpts{Class: name, Cwd: t.TempDir()})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		info := s.Info()
		if info.Receipt != nil {
			hashes[info.Receipt.ArgvSHA256] = true
		}
		if info.Class != name || info.Profile != map[string]string{"chat": "guest", "collab": "collab"}[name] {
			t.Fatalf("%s: info class=%q profile=%q", name, info.Class, info.Profile)
		}
		if info.Receipt == nil || info.Receipt.Class != name || len(info.Receipt.ArgvSHA256) != 64 || info.Receipt.Toolset == "" {
			t.Fatalf("%s: receipt = %+v", name, info.Receipt)
		}
		if got := readFile(t, filepath.Join(out, name+".argv")); strings.TrimSpace(got) != "class="+name {
			t.Fatalf("%s: argv marker = %q", name, got)
		}
		env := readFile(t, filepath.Join(out, name+".env"))
		for _, want := range []string{"KEEP_ME=yes", "HERMES_HOME=/profiles/" + name, "CLASS_PROFILE=" + info.Profile} {
			if !strings.Contains(env, want+"\n") {
				t.Errorf("%s: env missing %q", name, want)
			}
		}
		// Allowlist: not inherited, dropped, and the YOLO flag never survives.
		for _, bad := range []string{"SECRET_TOKEN=", "KEEP_DROPPED=", "HERMES_YOLO_MODE="} {
			if strings.Contains(env, bad) {
				t.Errorf("%s: env leaked %q", name, bad)
			}
		}
	}
	// Different classes, different argv receipts.
	if len(hashes) != 2 {
		t.Fatalf("expected distinct argv hashes per class, got %d", len(hashes))
	}
}

func TestRuntimeClass_UnknownClassRefused(t *testing.T) {
	d, _ := classDriver(t)
	_, err := d.Create(context.Background(), HermesACPOpts{Class: "nope"})
	if !errors.Is(err, ErrUnknownRuntimeClass) {
		t.Fatalf("err = %v, want ErrUnknownRuntimeClass", err)
	}
	if status, _ := managedSessionStatus(err); status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
	// A driver with no registry at all refuses every named class too.
	d2 := fakeHermesDriver(t, nil)
	if _, err := d2.Create(context.Background(), HermesACPOpts{Class: "chat"}); !errors.Is(err, ErrUnknownRuntimeClass) {
		t.Fatalf("no-registry err = %v", err)
	}
}

func TestRuntimeClass_RequestMayNotOverrideClassFields(t *testing.T) {
	d, _ := classDriver(t)
	for name, opts := range map[string]HermesACPOpts{
		"profile": {Class: "chat", Profile: "cog"},
		"mcp":     {Class: "chat", McpServers: nil},
	} {
		if name == "mcp" {
			opts.McpServers = append(opts.McpServers, []byte(`{"name":"x"}`))
		}
		if _, err := d.Create(context.Background(), opts); !errors.Is(err, ErrInvalidManagedSession) {
			t.Errorf("%s: err = %v, want invalid", name, err)
		}
	}
}

func TestRuntimeClass_AllowClassRefusal(t *testing.T) {
	d, _ := classDriver(t)
	only := func(c string) bool { return c == "chat" }
	if _, err := d.Create(context.Background(), HermesACPOpts{Class: "collab", AllowClass: only}); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("collab with chat-only authority: err = %v, want forbidden", err)
	}
	// Class-less (default class) is also refused to a class-limited caller.
	if _, err := d.Create(context.Background(), HermesACPOpts{AllowClass: only}); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("default class with chat-only authority: err = %v, want forbidden", err)
	}
	s, err := d.Create(context.Background(), HermesACPOpts{Class: "chat", Cwd: t.TempDir(), AllowClass: only})
	if err != nil {
		t.Fatal(err)
	}
	// Fork inherits the class; a fork cannot change it, and authority is checked.
	if _, err := d.Create(context.Background(), HermesACPOpts{ForkOf: s.ID(), Class: "collab"}); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("fork changing class: err = %v", err)
	}
	f, err := d.Create(context.Background(), HermesACPOpts{ForkOf: s.ID(), AllowClass: only})
	if err != nil || f.Info().Class != "chat" {
		t.Fatalf("fork: %v %+v", err, f)
	}
	if _, err := d.Create(context.Background(), HermesACPOpts{ForkOf: s.ID(), AllowClass: func(string) bool { return false }}); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("fork without authority: err = %v", err)
	}
	// Attaching a live session of a class the caller may not use is refused.
	if _, err := d.Create(context.Background(), HermesACPOpts{Load: s.ID(), AllowClass: func(c string) bool { return c == "collab" }}); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("attach without authority: err = %v", err)
	}
}

func TestRuntimeClass_DefaultClassBackwardCompatible(t *testing.T) {
	// No class: the driver's own Command/Env, the request's profile, no
	// class on the record, no receipt. Registry presence changes nothing.
	d, _ := classDriver(t)
	var gotProfile string
	d.cfg.Command = func(p string) []string { gotProfile = p; return []string{os.Args[0], "-test.run=^$"} }
	s, err := d.Create(context.Background(), HermesACPOpts{Profile: "cog", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	info := s.Info()
	if gotProfile != "cog" || info.Profile != "cog" || info.Class != "" || info.Receipt != nil {
		t.Fatalf("default path changed: profile=%q info=%+v", gotProfile, info)
	}
}

func TestRuntimeClassRegistry_Validation(t *testing.T) {
	bad := map[string]RuntimeClass{
		"BadName":  {Argv: []string{"x"}},
		"no-argv":  {},
		"badcwd":   {Argv: []string{"x"}, CwdPolicy: "weird"},
		"fixednil": {Argv: []string{"x"}, CwdPolicy: "fixed"},
		"badmcp":   {Argv: []string{"x"}, McpServers: "all"},
		"badvar":   {Argv: []string{"{undeclared}"}},
	}
	for name, c := range bad {
		if _, err := NewRuntimeClassRegistry(&ManagedSessionsConfig{RuntimeClasses: map[string]RuntimeClass{name: c}}); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
	// One bad class empties the registry (fail closed), good siblings included.
	reg, err := NewRuntimeClassRegistry(&ManagedSessionsConfig{RuntimeClasses: map[string]RuntimeClass{"ok": {Argv: []string{"x"}}, "no-argv": {}}})
	if err == nil || reg.Has("ok") {
		t.Fatalf("partial registry survived: err=%v has=%v", err, reg.Has("ok"))
	}
}

func TestRuntimeClass_CwdPolicy(t *testing.T) {
	reg, err := NewRuntimeClassRegistry(&ManagedSessionsConfig{RuntimeClasses: map[string]RuntimeClass{
		"fixed": {Argv: []string{"x"}, Cwd: "/srv/fixed", CwdPolicy: "fixed"},
		"req":   {Argv: []string{"x"}, Cwd: "/srv/default"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if rc, err := reg.Resolve("fixed", nil, ""); err != nil || rc.Cwd != "/srv/fixed" {
		t.Fatalf("fixed: %v %+v", err, rc)
	}
	if _, err := reg.Resolve("fixed", nil, "/elsewhere"); !errors.Is(err, ErrInvalidManagedSession) {
		t.Fatalf("fixed with other cwd: %v", err)
	}
	if rc, _ := reg.Resolve("req", nil, "/mine"); rc.Cwd != "/mine" {
		t.Fatalf("request cwd = %q", rc.Cwd)
	}
	if rc, _ := reg.Resolve("req", nil, ""); rc.Cwd != "/srv/default" {
		t.Fatalf("fallback cwd = %q", rc.Cwd)
	}
}

// ---- HTTP: scope authority through the real grant gate ----

func classServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv := newGrantAuthTestServer(t)
	d, _ := classDriver(t)
	srv.managedSessions = d
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

func TestRuntimeClass_HTTPScopeAuthority(t *testing.T) {
	srv, ts := classServer(t)
	cwd := t.TempDir()
	chatOnly := mintScopedTestGrant(t, srv, "guest-surface", ScopeManagedSessionsClassPrefix+"chat")
	owner := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeManagedSessions)
	star := mintScopedTestGrant(t, srv, "class-star", ScopeManagedSessionsClassPrefix+"*")
	node := mintScopedTestGrant(t, srv, "node-like", nodeRootScope)

	create := func(tok string, body map[string]any) (int, ManagedSessionInfo) {
		var info ManagedSessionInfo
		body["cwd"] = cwd
		return g0Do(t, ts, tok, "POST", "/v1/managed-sessions", body, &info), info
	}
	// Scoped surface: its listed class only.
	code, chat := create(chatOnly, map[string]any{"class": "chat"})
	if code != http.StatusCreated || chat.Class != "chat" {
		t.Fatalf("chat-scoped create chat = %d %+v", code, chat)
	}
	if code, _ := create(chatOnly, map[string]any{"class": "collab"}); code != http.StatusForbidden {
		t.Fatalf("chat-scoped create collab = %d, want 403", code)
	}
	if code, _ := create(chatOnly, map[string]any{}); code != http.StatusForbidden {
		t.Fatalf("chat-scoped create default class = %d, want 403", code)
	}
	// Unknown class: 400 for an authorized caller, 403 (no registry leak) for a scoped one.
	if code, _ := create(owner, map[string]any{"class": "nope"}); code != http.StatusBadRequest {
		t.Fatalf("owner unknown class = %d, want 400", code)
	}
	if code, _ := create(chatOnly, map[string]any{"class": "nope"}); code != http.StatusForbidden {
		t.Fatalf("scoped unknown class = %d, want 403", code)
	}
	// Owner (bare scope), wildcard, and node-root may create any class.
	for name, tok := range map[string]string{"owner": owner, "star": star, "node": node} {
		for _, c := range []string{"chat", "collab"} {
			if code, info := create(tok, map[string]any{"class": c}); code != http.StatusCreated || info.Class != c {
				t.Errorf("%s create %s = %d %+v", name, c, code, info)
			}
		}
	}
	// A scoped grant cannot drive or delete a session of another class.
	_, ownerSess := create(owner, map[string]any{"class": "collab"})
	for _, path := range []string{"/prompt", "/cancel", "/resume"} {
		if code := g0Do(t, ts, chatOnly, "POST", "/v1/managed-sessions/"+ownerSess.SessionID+path, map[string]any{"prompt": "hi"}, nil); code != http.StatusForbidden {
			t.Errorf("chat-scoped %s on collab session = %d, want 403", path, code)
		}
	}
	if code := g0Do(t, ts, chatOnly, "DELETE", "/v1/managed-sessions/"+ownerSess.SessionID, nil, nil); code != http.StatusForbidden {
		t.Errorf("chat-scoped delete on collab session = %d, want 403", code)
	}
	if srv.managedSessions.Get(ownerSess.SessionID) == nil {
		t.Fatal("forbidden delete removed the session")
	}
	// ...but drives its own class.
	if code := g0Do(t, ts, chatOnly, "POST", "/v1/managed-sessions/"+chat.SessionID+"/cancel", map[string]any{}, nil); code != http.StatusOK {
		t.Errorf("chat-scoped cancel on chat session = %d, want 200", code)
	}
}

func TestRuntimeClass_HTTPListFilteredByClassAuthority(t *testing.T) {
	srv, ts := classServer(t)
	cwd := t.TempDir()
	chatOnly := mintScopedTestGrant(t, srv, "guest-surface", ScopeManagedSessionsClassPrefix+"chat")
	chatCollab := mintScopedTestGrant(t, srv, "two-class", ScopeManagedSessionsClassPrefix+"chat", ScopeManagedSessionsClassPrefix+"collab")
	owner := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeManagedSessions)
	star := mintScopedTestGrant(t, srv, "class-star", ScopeManagedSessionsClassPrefix+"*")
	node := mintScopedTestGrant(t, srv, "node-like", nodeRootScope)

	// One session per class, including the default class (class "").
	for _, c := range []string{"chat", "collab", ""} {
		body := map[string]any{"cwd": cwd}
		if c != "" {
			body["class"] = c
		}
		if code := g0Do(t, ts, owner, "POST", "/v1/managed-sessions", body, nil); code != http.StatusCreated {
			t.Fatalf("owner create class %q = %d", c, code)
		}
	}
	// GET reads bypass grantAuthMiddleware today (isGrantExemptRequest), so no
	// grant reaches the handler over HTTP. Call the handler with the verified
	// grant in context, as the middleware does for gated routes.
	classes := func(tok string) string {
		grant, ok := srv.identityGrants.VerifyAny(tok)
		if !ok {
			t.Fatal("grant did not verify")
		}
		req := httptest.NewRequest("GET", "/v1/managed-sessions", nil).WithContext(contextWithGrant(context.Background(), grant))
		rec := httptest.NewRecorder()
		srv.handleManagedSessionList(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("list = %d", rec.Code)
		}
		var out struct {
			Sessions []ManagedSessionInfo `json:"sessions"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, s := range out.Sessions {
			got = append(got, s.Class)
		}
		sort.Strings(got)
		return strings.Join(got, ",")
	}
	// Unrestricted callers see everything, default class included.
	for name, tok := range map[string]string{"owner": owner, "star": star, "node": node} {
		if got := classes(tok); got != ",chat,collab" {
			t.Errorf("%s list classes = %q, want all", name, got)
		}
	}
	// Class-scoped callers see only permitted classes, never the default class.
	if got := classes(chatOnly); got != "chat" {
		t.Errorf("chat-scoped list classes = %q, want chat", got)
	}
	if got := classes(chatCollab); got != "chat,collab" {
		t.Errorf("two-class list classes = %q, want chat,collab", got)
	}
}

func TestRuntimeClass_HTTPDefaultPathUnchangedForUnscopedGrants(t *testing.T) {
	srv, ts := classServer(t)
	srv.managedSessions = fakeHermesDriver(t, nil) // today's driver, no registry
	ts.Config.Handler = srv.Handler()
	// Legacy stage-studio (write) and bare managed-sessions grants create with no class.
	for name, tok := range map[string]string{
		"stage-studio": mintScopedTestGrant(t, srv, "stage-studio", ScopeWrite),
		"bare":         mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeManagedSessions),
	} {
		var info ManagedSessionInfo
		if code := g0Do(t, ts, tok, "POST", "/v1/managed-sessions", map[string]any{"profile": "cog", "cwd": t.TempDir()}, &info); code != http.StatusCreated || info.Class != "" || info.Profile != "cog" {
			t.Errorf("%s default create = %d %+v", name, code, info)
		}
	}
}

func TestManagedSessionScope_ClassScopeMappingStillGatesOtherScopes(t *testing.T) {
	// A class scope opens the managed-sessions gate only, nothing else.
	g := &IdentityGrant{Scope: []string{ScopeManagedSessionsClassPrefix + "chat"}}
	if !classScopedManagedSessions(g, ScopeManagedSessions) || classScopedManagedSessions(g, ScopeWrite) || classScopedManagedSessions(g, ScopeAdmin) {
		t.Fatal("class scope must satisfy managed-sessions and nothing else")
	}
	if classScopedManagedSessions(&IdentityGrant{Scope: []string{ScopeManagedSessionsClassPrefix}}, ScopeManagedSessions) {
		t.Fatal("empty class name must not count as a scope")
	}
}

func TestRuntimeClass_LoadsFromKernelYAML(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".cog", "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	yml := `managedSessions:
  vars: {hermes_py: /opt/py, stage: /srv/stage, profile_home: /srv/p}
  runtimeClasses:
    chat:
      argv: ["{hermes_py}", "{stage}/acp_guest.py"]
      profile: cog
      toolset: stage-guest
      mcpServers: none
      env: {inherit: [PATH, HOME], drop: [HERMES_YOLO_MODE, "STAGE_COLLAB_*"], set: {HERMES_HOME: "{profile_home}"}}
`
	if err := os.WriteFile(filepath.Join(root, ".cog", "config", "kernel.yaml"), []byte(yml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := NewRuntimeClassRegistry(cfg.ManagedSessions)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := reg.Resolve("chat", []string{"PATH=/bin", "SECRET=x", "STAGE_COLLAB_NAME=v"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(rc.Argv, " ") != "/opt/py /srv/stage/acp_guest.py" || rc.Profile != "cog" {
		t.Fatalf("resolved %+v", rc)
	}
	if strings.Join(rc.Env, ",") != "PATH=/bin,HERMES_HOME=/srv/p" {
		t.Fatalf("env = %v", rc.Env)
	}
}

// Resuming a registered-but-dead (Crashed) classed session keeps its class,
// and the authority check runs against that class, not the empty default.
func TestRuntimeClass_ResumeOfCrashedKeepsClassAndChecksAuthority(t *testing.T) {
	d, out := classDriver(t) // CrashWindow = 1h: an early crash is terminal
	old, err := d.Create(context.Background(), HermesACPOpts{Class: "chat", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = old.Prompt(json.RawMessage(`"crash now"`))
	waitState(t, old, StateCrashed)
	if err := os.Remove(filepath.Join(out, "chat.argv")); err != nil {
		t.Fatal(err)
	}

	// A caller scoped to another class is refused; the crashed entry is untouched.
	if _, err := d.Create(context.Background(), HermesACPOpts{Load: old.ID(), AllowClass: func(c string) bool { return c == "collab" }}); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("unauthorized resume: err = %v, want forbidden", err)
	}
	if d.Get(old.ID()) != old || old.State() != StateCrashed {
		t.Fatal("refused resume must not replace or detach the crashed session")
	}
	// A resume may not change class.
	if _, err := d.Create(context.Background(), HermesACPOpts{Load: old.ID(), Class: "collab"}); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("resume changing class: err = %v, want forbidden", err)
	}

	// The chat-scoped caller resumes with no class in the request; class is kept.
	fresh, err := d.Create(context.Background(), HermesACPOpts{Load: old.ID(), AllowClass: func(c string) bool { return c == "chat" }})
	if err != nil {
		t.Fatalf("authorized resume: %v", err)
	}
	if got := fresh.Info().Class; got != "chat" {
		t.Fatalf("resumed class = %q, want chat", got)
	}
	// The process was spawned from the class's pinned argv, not the default command.
	if _, err := os.Stat(filepath.Join(out, "chat.argv")); err != nil {
		t.Fatalf("resumed process did not run the chat class argv: %v", err)
	}
}

// A follower sharing an in-flight load is checked against the class the load
// resolved to, exactly like every other attach path.
func TestRuntimeClass_LoadFollowerChecksAuthority(t *testing.T) {
	d, _ := classDriver(t)
	s, err := d.Create(context.Background(), HermesACPOpts{Class: "chat", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	const id = "inflight-1"
	claim := &loadClaim{done: make(chan struct{})}
	d.mu.Lock()
	d.loads[id] = claim
	d.mu.Unlock()

	type res struct {
		sess *HermesACPSession
		err  error
	}
	follow := func(allow func(string) bool) chan res {
		ch := make(chan res, 1)
		go func() {
			sess, err := d.Create(context.Background(), HermesACPOpts{Load: id, AllowClass: allow})
			ch <- res{sess, err}
		}()
		return ch
	}
	bad := follow(func(c string) bool { return c == "collab" })
	good := follow(func(c string) bool { return c == "chat" })
	claim.sess = s
	close(claim.done)

	if r := <-bad; !errors.Is(r.err, ErrManagedSessionForbidden) || r.sess != nil {
		t.Fatalf("unauthorized follower: sess=%v err=%v, want forbidden", r.sess, r.err)
	}
	if r := <-good; r.err != nil || r.sess != s {
		t.Fatalf("authorized follower: sess=%v err=%v", r.sess, r.err)
	}
}

// coldDriver creates a chat-class session on one driver, then returns a second
// driver over the same records (a restart) plus the session id.
func coldDriver(t *testing.T) (*HermesACPDriver, string, string) {
	t.Helper()
	rec := t.TempDir()
	d1, _ := classDriverAt(t, rec)
	s, err := d1.Create(context.Background(), HermesACPOpts{Class: "chat", Cwd: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id := s.ID()
	d1.Close() // kernel shutdown: the durable record must survive it
	d2, out := classDriverAt(t, rec)
	if d2.Get(id) != nil {
		t.Fatal("restarted driver must not hold the session in memory")
	}
	return d2, out, id
}

// Cold load: a caller scoped to another class cannot adopt a classed session
// just because the driver forgot it.
func TestRuntimeClass_ColdResumeByOtherClassScopeRefused(t *testing.T) {
	d, _, id := coldDriver(t)
	for _, class := range []string{"", "collab"} {
		_, err := d.Create(context.Background(), HermesACPOpts{Load: id, Class: class, AllowClass: func(c string) bool { return c == "collab" }})
		if !errors.Is(err, ErrManagedSessionForbidden) {
			t.Fatalf("class %q: cold resume by collab-scoped caller: err = %v, want forbidden", class, err)
		}
	}
	if d.Get(id) != nil {
		t.Fatal("refused cold resume must not register a session")
	}
}

// Cold load: a class that differs from the record is refused. A session with
// no record is the legacy default class, so naming a class on it is a mismatch.
func TestRuntimeClass_ColdResumeMismatchedClassRefused(t *testing.T) {
	d, _, id := coldDriver(t)
	_, err := d.Create(context.Background(), HermesACPOpts{Load: id, Class: "collab"})
	if !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("mismatched class: err = %v, want forbidden", err)
	}
	// A legacy session (no record) is the default class: naming a class is a mismatch.
	_, err = d.Create(context.Background(), HermesACPOpts{Load: "unrecorded-1", Class: "chat"})
	if !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("class on unrecorded session: err = %v, want forbidden", err)
	}
}

// Cold load: an authorized resume keeps the recorded class and its argv/env,
// whether the request omits the class or restates it; never the default env.
func TestRuntimeClass_ColdResumeAuthorizedKeepsRecordedClass(t *testing.T) {
	for _, supplied := range []string{"", "chat"} {
		d, out, id := coldDriver(t)
		for _, f := range []string{"chat.argv", "chat.env"} {
			if err := os.Remove(filepath.Join(out, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
		s, err := d.Create(context.Background(), HermesACPOpts{Load: id, Class: supplied, AllowClass: func(c string) bool { return c == "chat" }})
		if err != nil {
			t.Fatalf("supplied %q: authorized cold resume: %v", supplied, err)
		}
		if got := s.Info().Class; got != "chat" {
			t.Fatalf("supplied %q: class = %q, want chat", supplied, got)
		}
		if _, err := os.Stat(filepath.Join(out, "chat.argv")); err != nil {
			t.Fatalf("supplied %q: resumed process did not run the chat class argv: %v", supplied, err)
		}
		if got := readFile(t, filepath.Join(out, "chat.env")); strings.Contains(got, "SECRET_TOKEN") {
			t.Fatalf("supplied %q: chat env leaked: %s", supplied, got)
		}
		d.Close()
	}
}

// Delete forgets the record (the id is then a legacy default session).
func TestRuntimeClass_DeleteRemovesRecord(t *testing.T) {
	d, _, id := coldDriver(t)
	if err := d.Delete(id); err != nil {
		t.Fatal(err)
	}
	if rec, err := d.loadRecord(id); err != nil || rec.Class != "" {
		t.Fatalf("record after delete = %+v, %v", rec, err)
	}
}

// Cold delete: a caller not authorized for the recorded class cannot remove
// the record (which would downgrade the session to the default class), and a
// default-scoped cold resume is still refused afterwards.
func TestRuntimeClass_ColdDeleteByUnauthorizedCallerRefused(t *testing.T) {
	d, _, id := coldDriver(t)
	for name, allow := range map[string]func(string) bool{
		"default-only": func(c string) bool { return c == "" },
		"other-class":  func(c string) bool { return c == "collab" },
	} {
		if err := d.DeleteAuthorized(id, allow); !errors.Is(err, ErrManagedSessionForbidden) {
			t.Fatalf("%s: cold delete err = %v, want forbidden", name, err)
		}
		if rec, err := d.loadRecord(id); err != nil || rec.Class != "chat" {
			t.Fatalf("%s: record after refused delete = %+v, %v", name, rec, err)
		}
	}
	_, err := d.Create(context.Background(), HermesACPOpts{Load: id, AllowClass: func(c string) bool { return c == "" }})
	if !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("default-scoped cold resume after refused delete: err = %v, want forbidden", err)
	}
}

// Cold delete fails closed on an unreadable record.
func TestRuntimeClass_ColdDeleteCorruptRecordRefused(t *testing.T) {
	d, _, id := coldDriver(t)
	path := d.managedSessionRecordPath(id)
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteAuthorized(id, func(string) bool { return true }); !errors.Is(err, ErrManagedSessionForbidden) {
		t.Fatalf("corrupt record delete err = %v, want forbidden", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("corrupt record must survive a refused delete: %v", err)
	}
}

// Cold delete by an authorized caller removes the record.
func TestRuntimeClass_ColdDeleteAuthorizedRemovesRecord(t *testing.T) {
	d, _, id := coldDriver(t)
	if err := d.DeleteAuthorized(id, func(c string) bool { return c == "chat" }); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(d.managedSessionRecordPath(id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after authorized delete: %v", err)
	}
}

// Over HTTP: the same bypass through the real grant gate.
func TestRuntimeClass_HTTPColdDeleteGated(t *testing.T) {
	srv, ts := classServer(t)
	d := srv.managedSessions
	owner := mintActorTestGrant(t, srv, "discord-chaz", "chaz", ScopeManagedSessions)
	collab := mintScopedTestGrant(t, srv, "collab-surface", ScopeManagedSessionsClassPrefix+"collab")
	var info ManagedSessionInfo
	if code := g0Do(t, ts, owner, "POST", "/v1/managed-sessions", map[string]any{"class": "chat", "cwd": t.TempDir()}, &info); code != http.StatusCreated {
		t.Fatalf("create = %d", code)
	}
	d.drop(info.SessionID) // forget it in memory, keep the record: now cold
	if code := g0Do(t, ts, collab, "DELETE", "/v1/managed-sessions/"+info.SessionID, nil, nil); code != http.StatusForbidden {
		t.Fatalf("collab cold delete = %d, want 403", code)
	}
	if rec, err := d.loadRecord(info.SessionID); err != nil || rec.Class != "chat" {
		t.Fatalf("record after refused cold delete = %+v, %v", rec, err)
	}
	if code := g0Do(t, ts, owner, "DELETE", "/v1/managed-sessions/"+info.SessionID, nil, nil); code != http.StatusNoContent {
		t.Fatalf("owner cold delete = %d, want 204", code)
	}
}
