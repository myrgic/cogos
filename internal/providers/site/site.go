// Package site provides the Reconcilable provider for static-site deployments.
//
// This package makes SiteProvider and GHPagesStrategy available to the daemon
// binary (cmd/cogos) which cannot import the workspace-root package main.
//
// The types and logic mirror site_*.go in the workspace root. The workspace-root
// copies remain canonical for the cog CLI; this package is the importable
// counterpart used by cmd/cogos/providers_wire.go.
//
// Registration: importing this package (even as a blank import) triggers init(),
// which registers "site" with pkg/reconcile. GHPagesStrategy is self-registered
// in the same init() chain.
package site

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
	"gopkg.in/yaml.v3"
)

// artifactSHAFile is the deploy-embedded manifest recording the content hash of
// the artifact a target was built from. FetchLive reads it back to compare live
// against a freshly-built artifact; Deploy writes it before staging.
const artifactSHAFile = ".artifact-sha"

// artifactHash computes a deterministic sha256 over every file in distDir,
// hashing the sorted sequence of (relative-path, file-bytes) so the result is
// independent of filesystem walk order. Directories and symlinks are ignored;
// only regular file contents contribute. Returns lowercase hex.
//
// Each entry is length-prefixed (path length, path, content length, content) so
// that no two distinct trees can collide by concatenation ambiguity.
func artifactHash(distDir string) (string, error) {
	var rels []string
	err := filepath.Walk(distDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(distDir, path)
		if err != nil {
			return err
		}
		rels = append(rels, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("artifactHash: walk %s: %w", distDir, err)
	}
	sort.Strings(rels)

	h := sha256.New()
	var lenBuf [8]byte
	for _, rel := range rels {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(rel)))
		h.Write(lenBuf[:])
		h.Write([]byte(rel))

		data, err := os.ReadFile(filepath.Join(distDir, filepath.FromSlash(rel)))
		if err != nil {
			return "", fmt.Errorf("artifactHash: read %s: %w", rel, err)
		}
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(data)))
		h.Write(lenBuf[:])
		h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func init() {
	sp := newSiteProvider()
	reconcile.RegisterProvider("site", sp)
	sp.RegisterStrategy("gh-pages", &ghPagesStrategy{})
}

// ─── SiteCRD types ───────────────────────────────────────────────────────────

var knownStrategies = map[string]bool{
	"gh-pages":          true,
	"gitlab-pages":      true,
	"s3":                true,
	"k8s-ingress":       true,
	"self-hosted-rsync": true,
}

// ErrInvalidSiteCRD is returned by Validate when the CRD is inconsistent.
var ErrInvalidSiteCRD = errors.New("invalid SiteCRD")

// ErrUnsupportedStrategy is returned when the requested strategy is not registered.
var ErrUnsupportedStrategy = errors.New("unsupported deploy strategy")

type siteCRD struct {
	APIVersion string       `yaml:"apiVersion" json:"apiVersion"`
	Kind       string       `yaml:"kind"       json:"kind"`
	Metadata   siteMetadata `yaml:"metadata"   json:"metadata"`
	Spec       siteSpec     `yaml:"spec"       json:"spec"`
}

type siteMetadata struct {
	Name string `yaml:"name" json:"name"`
}

type siteSpec struct {
	Domain     string     `yaml:"domain"      json:"domain"`
	Canonical  bool       `yaml:"canonical"   json:"canonical"`
	Source     sourceSpec `yaml:"source"      json:"source"`
	Build      buildSpec  `yaml:"build"       json:"build"`
	HTTPS      httpsSpec  `yaml:"https"       json:"https"`
	Deploy     deploySpec `yaml:"deploy"      json:"deploy"`
	RedirectTo *string    `yaml:"redirect_to,omitempty" json:"redirect_to,omitempty"`
}

type sourceSpec struct {
	Path string `yaml:"path" json:"path"`
}
type buildSpec struct {
	Command string `yaml:"command" json:"command"`
	Dist    string `yaml:"dist"    json:"dist"`
}
type httpsSpec struct {
	Required bool `yaml:"required" json:"required"`
}
type deploySpec struct {
	Strategy string         `yaml:"strategy" json:"strategy"`
	Target   map[string]any `yaml:"target"   json:"target"`
}

func (s siteCRD) Validate() error {
	if s.Metadata.Name == "" {
		return fmt.Errorf("%w: metadata.name is required", ErrInvalidSiteCRD)
	}
	if s.Spec.Domain == "" {
		return fmt.Errorf("%w: spec.domain is required", ErrInvalidSiteCRD)
	}
	if s.Spec.Canonical && s.Spec.RedirectTo != nil {
		return fmt.Errorf("%w: canonical site %q must not set redirect_to", ErrInvalidSiteCRD, s.Metadata.Name)
	}
	if s.Spec.Deploy.Strategy == "" {
		return fmt.Errorf("%w: spec.deploy.strategy is required", ErrInvalidSiteCRD)
	}
	if !knownStrategies[s.Spec.Deploy.Strategy] {
		return fmt.Errorf("%w: unknown deploy strategy %q", ErrInvalidSiteCRD, s.Spec.Deploy.Strategy)
	}
	return nil
}

// ─── DeployStrategy ──────────────────────────────────────────────────────────

type liveSiteState struct {
	ArtifactSHA        string         `json:"artifact_sha"`
	CNAMEContent       string         `json:"cname_content"`
	ResolvedFromTarget bool           `json:"resolved_from_target"`
	Metadata           map[string]any `json:"metadata,omitempty"`
}

type deployStrategy interface {
	FetchLive(ctx context.Context, app siteCRD) (liveSiteState, error)
	Deploy(ctx context.Context, app siteCRD, artifactDir string) error
}

// ─── SiteProvider ────────────────────────────────────────────────────────────

type siteProvider struct {
	mu         sync.Mutex
	root       string
	strategies map[string]deployStrategy

	// buildHashFn builds an app and returns the content hash of its dist tree.
	// Injectable so tests can drive ComputePlan's drift logic without running a
	// real build; defaults to (*siteProvider).buildAndHash.
	buildHashFn func(ctx context.Context, appDir, name string) (string, error)

	// lastLogErr throttles repeated per-resource log lines the same way
	// internal/engine/reconcile_daemon.go's warnPhaseFailureThrottled does
	// (issue #494, cog-review PR #496 third pass). Several call sites in
	// this file log a per-CRD warning/notice on every reconcile cycle with
	// no way for the daemon's own phase-level throttle to see them:
	// FetchLive swallows per-CRD strategy-lookup/live-fetch errors into
	// per-resource state and always returns (live, nil); loadCRDs swallows
	// validation errors into a log line and still returns the CRD;
	// applyAction's delete no-op path isn't gated on ApplyFailed at all.
	// A persistently failing/no-op resource would otherwise repeat the
	// identical line every ~30s tick forever — the exact bug class this PR
	// fixes for reconcile_daemon.go's own phases and actions, reproduced
	// here via call sites this PR's earlier passes left untouched. See
	// logThrottled.
	lastLogErrMu sync.Mutex
	lastLogErr   map[string]string
}

// logThrottled logs msg (via slog, with args as structured attributes) at
// level the first time key's associated text is seen, or whenever that text
// changes from what was last recorded for key, and at Debug for an exact
// repeat. text is typically an error's .Error() string, but for a
// non-error repeating notice (e.g. the delete no-op path) any stable string
// works — the mechanism only cares whether it changed.
func (s *siteProvider) logThrottled(key, text string, level slog.Level, msg string, args ...any) {
	s.lastLogErrMu.Lock()
	if s.lastLogErr == nil {
		s.lastLogErr = make(map[string]string)
	}
	prev, seen := s.lastLogErr[key]
	changed := !seen || prev != text
	s.lastLogErr[key] = text
	s.lastLogErrMu.Unlock()

	if changed {
		slog.Log(context.Background(), level, msg, args...)
		return
	}
	slog.Debug(msg, args...)
}

func newSiteProvider() *siteProvider {
	sp := &siteProvider{strategies: map[string]deployStrategy{}}
	sp.buildHashFn = sp.buildAndHash
	return sp
}

func (s *siteProvider) RegisterStrategy(name string, strat deployStrategy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.strategies[name] = strat
}

func (s *siteProvider) lookupStrategy(name string) (deployStrategy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	strat, ok := s.strategies[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedStrategy, name)
	}
	return strat, nil
}

func (s *siteProvider) Type() string { return "site" }

func (s *siteProvider) Health() reconcile.ResourceStatus {
	return reconcile.NewResourceStatus(reconcile.SyncStatusUnknown, reconcile.HealthProgressing)
}

func (s *siteProvider) LoadConfig(root string) (any, error) {
	s.mu.Lock()
	s.root = root
	s.mu.Unlock()
	crds, err := s.loadCRDs(root)
	if err != nil {
		return nil, err
	}
	if crds == nil {
		crds = []siteCRD{}
	}
	return crds, nil
}

func (s *siteProvider) FetchLive(ctx context.Context, config any) (any, error) {
	crds, ok := config.([]siteCRD)
	if !ok {
		return nil, fmt.Errorf("site provider: FetchLive: unexpected config type %T", config)
	}
	live := make(map[string]liveSiteState, len(crds))
	for _, crd := range crds {
		strat, err := s.lookupStrategy(crd.Spec.Deploy.Strategy)
		if err != nil {
			// Throttled (cog-review, PR #496 third pass): a CRD naming an
			// unregistered strategy fails identically every ~30s tick
			// forever, and this loop always returns (live, nil) overall —
			// one broken CRD must not fail FetchLive for every other CRD
			// (ADR-095 §2 per-resource isolation) — so reconcile_daemon.go's
			// own phase-level throttle around the FetchLive call never sees
			// this failure at all. Keyed per CRD name, independent of the
			// live-fetch throttle key below.
			s.logThrottled("fetchlive-strategy:"+crd.Metadata.Name, err.Error(), slog.LevelWarn,
				"site: FetchLive strategy lookup failed", "name", crd.Metadata.Name, "err", err)
			live[crd.Metadata.Name] = liveSiteState{}
			continue
		}
		state, err := strat.FetchLive(ctx, crd)
		if err != nil {
			// Throttled for the same reason as the strategy-lookup failure
			// above — a persistently unreachable deploy target repeats this
			// line every tick otherwise.
			s.logThrottled("fetchlive:"+crd.Metadata.Name, err.Error(), slog.LevelWarn,
				"site: FetchLive failed", "name", crd.Metadata.Name, "err", err)
			live[crd.Metadata.Name] = liveSiteState{}
			continue
		}
		live[crd.Metadata.Name] = state
	}
	return live, nil
}

func (s *siteProvider) ComputePlan(config any, live any, state *reconcile.State) (*reconcile.Plan, error) {
	crds, ok := config.([]siteCRD)
	if !ok {
		return nil, fmt.Errorf("site provider: ComputePlan: unexpected config type %T", config)
	}
	liveMap, ok := live.(map[string]liveSiteState)
	if !ok {
		return nil, fmt.Errorf("site provider: ComputePlan: unexpected live type %T", live)
	}

	s.mu.Lock()
	root := s.root
	s.mu.Unlock()

	plan := &reconcile.Plan{
		ResourceType: "site",
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		ConfigPath:   root + "/apps",
	}

	declared := make(map[string]bool, len(crds))
	for _, crd := range crds {
		name := crd.Metadata.Name
		declared[name] = true
		appDir := root + "/apps/" + name

		liveState := liveMap[name]
		details := map[string]any{
			"domain":   crd.Spec.Domain,
			"strategy": crd.Spec.Deploy.Strategy,
			"target":   crd.Spec.Deploy.Target,
			"app_dir":  appDir,
		}

		if _, err := s.lookupStrategy(crd.Spec.Deploy.Strategy); err != nil {
			plan.Warnings = append(plan.Warnings,
				fmt.Sprintf("%s: strategy %q not registered — deploy will fail", name, crd.Spec.Deploy.Strategy))
		}

		if !liveState.ResolvedFromTarget {
			plan.Actions = append(plan.Actions, reconcile.Action{
				Action:       reconcile.ActionCreate,
				ResourceType: "site",
				Name:         name,
				Details:      details,
			})
			plan.Summary.Creates++
			continue
		}

		// Drift detection: build the artifact and hash its content, then compare
		// against the hash recorded at the live target (.artifact-sha). A build
		// or hash failure is surfaced as an update (safer to attempt a deploy
		// than to silently skip a possibly-stale target).
		//
		// NOTE: this builds the app to hash it, duplicating the build ApplyPlan
		// performs when the update is applied. Acceptable for now — plan and
		// apply run the same build.sh, so the extra build is cheap and keeps the
		// diff honest (an empty stub check could never detect content changes).
		localSHA, buildErr := s.buildHashFn(context.Background(), appDir, name)
		if buildErr != nil {
			plan.Warnings = append(plan.Warnings,
				fmt.Sprintf("%s: drift build failed, assuming update: %v", name, buildErr))
			plan.Actions = append(plan.Actions, reconcile.Action{
				Action:       reconcile.ActionUpdate,
				ResourceType: "site",
				Name:         name,
				Details:      details,
			})
			plan.Summary.Updates++
			continue
		}
		details["artifact_sha"] = localSHA

		if liveState.ArtifactSHA == "" || liveState.ArtifactSHA != localSHA {
			// No recorded content hash at the target, or the content changed.
			plan.Actions = append(plan.Actions, reconcile.Action{
				Action:       reconcile.ActionUpdate,
				ResourceType: "site",
				Name:         name,
				Details:      details,
			})
			plan.Summary.Updates++
			continue
		}

		plan.Actions = append(plan.Actions, reconcile.Action{
			Action:       reconcile.ActionSkip,
			ResourceType: "site",
			Name:         name,
			Details:      details,
		})
		plan.Summary.Skipped++
	}

	if state != nil {
		for _, res := range state.Resources {
			if !declared[res.Name] {
				plan.Actions = append(plan.Actions, reconcile.Action{
					Action:       reconcile.ActionDelete,
					ResourceType: "site",
					Name:         res.Name,
					Details:      map[string]any{"reason": "no matching site CRD"},
				})
				plan.Summary.Deletes++
			}
		}
	}

	return plan, nil
}

func (s *siteProvider) ApplyPlan(ctx context.Context, plan *reconcile.Plan) ([]reconcile.Result, error) {
	var results []reconcile.Result
	for _, action := range plan.Actions {
		if action.Action == reconcile.ActionSkip {
			continue
		}
		results = append(results, s.applyAction(ctx, action))
	}
	return results, nil
}

func (s *siteProvider) applyAction(ctx context.Context, action reconcile.Action) reconcile.Result {
	base := reconcile.Result{
		Phase:  "site",
		Action: string(action.Action),
		Name:   action.Name,
	}
	if action.Action == reconcile.ActionDelete {
		// Throttled (cog-review, PR #496 third pass): teardown is manual in
		// v0.0.1, so ComputePlan keeps re-emitting this ActionDelete every
		// ~30s cycle until an operator does that teardown — the common
		// case, not the exception. Unlike the ApplyFailed-gated throttling
		// elsewhere in this PR, this path always reports ApplySucceeded, so
		// it needed its own throttle rather than reusing
		// warnActionFailureThrottled. First occurrence (or any future CRD
		// change under the same name) logs at Info; an exact repeat logs at
		// Debug.
		s.logThrottled("delete-noop:"+action.Name, "no-op", slog.LevelInfo,
			"site: delete no-op (manual teardown required)", "name", action.Name)
		base.Status = reconcile.ApplySucceeded
		return base
	}
	appDir, _ := action.Details["app_dir"].(string)
	if appDir == "" {
		base.Status = reconcile.ApplyFailed
		base.Error = "no app_dir in action details"
		return base
	}
	out, err := siteBuild(ctx, appDir, action.Name)
	if err != nil {
		base.Status = reconcile.ApplyFailed
		base.Error = fmt.Sprintf("build: %v", err)
		return base
	}
	// Unlike buildAndHash's drift-check build (demoted to Debug — see its
	// call site), this is a genuine deploy actually being applied: a rare,
	// operator-relevant event, not per-cycle noise. Its build output stays
	// visible at Info by default (cog-review, PR #496 first pass: the
	// original fix demoted both callers uniformly, which would have hidden
	// a real deploy's build output too).
	slog.Info("site: deploy build succeeded", "name", action.Name, "output", string(out))
	stratName, _ := action.Details["strategy"].(string)
	strat, err := s.lookupStrategy(stratName)
	if err != nil {
		base.Status = reconcile.ApplyFailed
		base.Error = fmt.Sprintf("strategy lookup: %v", err)
		return base
	}
	s.mu.Lock()
	root := s.root
	s.mu.Unlock()
	crds, err := s.loadCRDs(root)
	if err != nil {
		base.Status = reconcile.ApplyFailed
		base.Error = fmt.Sprintf("reload config: %v", err)
		return base
	}
	crd, found := crdByName(crds, action.Name)
	if !found {
		base.Status = reconcile.ApplyFailed
		base.Error = fmt.Sprintf("CRD not found after reload: %s", action.Name)
		return base
	}
	if err := strat.Deploy(ctx, crd, appDir+"/dist"); err != nil {
		base.Status = reconcile.ApplyFailed
		base.Error = fmt.Sprintf("deploy: %v", err)
		return base
	}
	base.Status = reconcile.ApplySucceeded
	base.CreatedID = "site:" + action.Name
	return base
}

func (s *siteProvider) BuildState(config any, live any, existing *reconcile.State) (*reconcile.State, error) {
	liveMap, ok := live.(map[string]liveSiteState)
	if !ok {
		return nil, fmt.Errorf("site provider: BuildState: unexpected live type %T", live)
	}
	state := &reconcile.State{
		Version:      1,
		ResourceType: "site",
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	if existing != nil {
		state.Lineage = existing.Lineage
		state.Serial = existing.Serial + 1
	} else {
		state.Lineage = "site-" + time.Now().UTC().Format("20060102T150405Z")
	}
	for name, liveState := range liveMap {
		state.Resources = append(state.Resources, reconcile.Resource{
			Address:       "site." + name,
			Type:          "site",
			Mode:          reconcile.ModeManaged,
			ExternalID:    liveState.ArtifactSHA,
			Name:          name,
			LastRefreshed: time.Now().UTC().Format(time.RFC3339),
			Attributes: map[string]any{
				"artifact_sha":         liveState.ArtifactSHA,
				"cname_content":        liveState.CNAMEContent,
				"resolved_from_target": liveState.ResolvedFromTarget,
			},
		})
	}
	return state, nil
}

// ─── Config helpers ──────────────────────────────────────────────────────────

// loadCRDs is a method (not a free function) specifically so its validation
// warning can go through s.logThrottled (cog-review, PR #496 third pass): a
// persistently invalid site.yaml (typo'd strategy, missing domain, etc.)
// otherwise logs the identical warning on every LoadConfig call — every
// reconcile tick — forever, and since the error is swallowed here rather
// than returned, reconcile_daemon.go's phase-level throttle around
// LoadConfig never sees it either.
func (s *siteProvider) loadCRDs(root string) ([]siteCRD, error) {
	pattern := filepath.Join(root, "apps", "*", "site.yaml")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("site provider: glob %q: %w", pattern, err)
	}
	var crds []siteCRD
	for _, yamlPath := range matches {
		data, err := os.ReadFile(yamlPath)
		if err != nil {
			return nil, fmt.Errorf("site provider: read %s: %w", yamlPath, err)
		}
		var crd siteCRD
		if err := yaml.Unmarshal(data, &crd); err != nil {
			return nil, fmt.Errorf("site provider: parse %s: %w", yamlPath, err)
		}
		if err := crd.Validate(); err != nil {
			s.logThrottled("validate:"+yamlPath, err.Error(), slog.LevelWarn,
				"site: CRD validation failed", "path", yamlPath, "err", err)
		}
		crds = append(crds, crd)
	}
	return crds, nil
}

func crdByName(crds []siteCRD, name string) (siteCRD, bool) {
	for _, c := range crds {
		if c.Metadata.Name == name {
			return c, true
		}
	}
	return siteCRD{}, false
}

// siteBuild runs `bash build.sh` in appDir and returns its combined
// stdout+stderr output on success. On failure the output is folded into the
// returned error (unchanged from before issue #494's log-noise fix).
//
// Deliberately does NOT log the success-path output itself — see the two
// call sites below, buildAndHash and (*siteProvider).applyAction, which log
// it at different levels because they have very different noise profiles:
// buildAndHash runs on every reconcile cycle (default 30s) for every
// deployed site purely to hash the result for drift detection (see its doc
// comment), while applyAction only runs when a create/update action is
// actually being deployed — a rare, operator-relevant event. A single
// logging policy inside siteBuild can't serve both correctly (cog-review, PR
// #496 first pass): demoting to Debug unconditionally would also hide a
// genuine deploy's build output by default, which was never the noise this
// issue was about.
func siteBuild(ctx context.Context, appDir, name string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "bash", "build.sh")
	cmd.Dir = appDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("build.sh for %s: %w\n%s", name, err, out)
	}
	return out, nil
}

// buildAndHash runs the app's build (the same build.sh path ApplyPlan uses) and
// returns the content hash of the produced dist directory. The build writes to
// appDir/dist in place — build.sh hard-codes that path and relies on the app's
// location for its ../../packages copies, so there is no separate temp build dir
// to redirect to or clean up; the dist tree is the artifact ApplyPlan deploys.
func (s *siteProvider) buildAndHash(ctx context.Context, appDir, name string) (string, error) {
	out, err := siteBuild(ctx, appDir, name)
	if err != nil {
		return "", err
	}
	// Issue #494 (unrelated observation): this drift-detection hash runs on
	// every reconcile cycle for every deployed site, whether or not anything
	// changed. Logging the full combined stdout+stderr of `bash build.sh` on
	// every one of those cycles, via the stdlib log package (no level,
	// always writes to os.Stderr), was the single largest contributor to
	// ~/.cog/var/logs/serve.log's observed 488 MB — almost entirely macOS's
	// "MallocStackLogging: can't turn off malloc stack logging because it
	// was not enabled." emitted by the forked build subprocess to its own
	// stderr and captured verbatim by CombinedOutput. A successful
	// drift-check build's full output is a debugging aid, not routine
	// operational signal here, so it moves to slog.Debug (suppressed by
	// default; opt in with COG_LOG_DEBUG=1, see log_capture.go). This is
	// distinct from applyAction's real-deploy build below, which stays
	// visible at Info.
	slog.Debug("site: drift-check build succeeded", "name", name, "output", string(out))
	return artifactHash(filepath.Join(appDir, "dist"))
}

// ─── GHPagesStrategy ─────────────────────────────────────────────────────────

type ghPagesStrategy struct{ mu sync.Mutex }

func (g *ghPagesStrategy) resolveRepo(app siteCRD) (string, error) {
	raw, ok := app.Spec.Deploy.Target["repo"]
	if !ok {
		return "", fmt.Errorf("gh-pages: deploy.target.repo is required")
	}
	repo, ok := raw.(string)
	if !ok || repo == "" {
		return "", fmt.Errorf("gh-pages: deploy.target.repo must be a non-empty string")
	}
	return repo, nil
}

func ghAPI(ctx context.Context, path string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gh", "api", path)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("gh api %s: %w: %s", path, err, stderr.String())
	}
	return stdout.Bytes(), nil
}

func (g *ghPagesStrategy) FetchLive(ctx context.Context, app siteCRD) (liveSiteState, error) {
	repo, err := g.resolveRepo(app)
	if err != nil {
		return liveSiteState{}, err
	}
	state := liveSiteState{ResolvedFromTarget: true, Metadata: make(map[string]any)}

	// The main ref establishes whether the target is deployed at all. Its commit
	// SHA is not content-comparable (it changes on every force-push regardless of
	// artifact content), so it is recorded only as metadata; ArtifactSHA comes
	// from the deploy-embedded .artifact-sha manifest below.
	refData, err := ghAPI(ctx, fmt.Sprintf("/repos/%s/git/refs/heads/main", repo))
	if err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "Not Found") {
			state.ResolvedFromTarget = false
			return state, nil
		}
		return liveSiteState{}, fmt.Errorf("FetchLive: fetch main ref: %w", err)
	}
	var refResp struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := json.Unmarshal(refData, &refResp); err != nil {
		return liveSiteState{}, fmt.Errorf("FetchLive: parse ref response: %w", err)
	}
	state.Metadata["commit_sha"] = refResp.Object.SHA

	cnameData, err := ghAPI(ctx, fmt.Sprintf("/repos/%s/contents/CNAME", repo))
	if err == nil {
		var cnameResp struct {
			Content  string `json:"content"`
			Encoding string `json:"encoding"`
		}
		if err := json.Unmarshal(cnameData, &cnameResp); err == nil && cnameResp.Encoding == "base64" {
			decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(cnameResp.Content, "\n", ""))
			if err == nil {
				state.CNAMEContent = strings.TrimSpace(string(decoded))
			}
		}
	}

	// Fetch the deploy-embedded artifact content hash (optional — 404 is normal
	// for a target deployed before .artifact-sha was introduced; an empty
	// ArtifactSHA then forces an update on the next reconcile).
	shaData, err := ghAPI(ctx, fmt.Sprintf("/repos/%s/contents/%s", repo, artifactSHAFile))
	if err == nil {
		var shaResp struct {
			Content  string `json:"content"`
			Encoding string `json:"encoding"`
		}
		if err := json.Unmarshal(shaData, &shaResp); err == nil && shaResp.Encoding == "base64" {
			decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(shaResp.Content, "\n", ""))
			if err == nil {
				state.ArtifactSHA = strings.TrimSpace(string(decoded))
			}
		}
	}

	state.Metadata["repo"] = repo
	return state, nil
}

func (g *ghPagesStrategy) Deploy(ctx context.Context, app siteCRD, artifactDir string) error {
	repo, err := g.resolveRepo(app)
	if err != nil {
		return err
	}
	sourceSHA := os.Getenv("__SOURCE_SHA")
	if sourceSHA == "" {
		sourceSHA = "unknown"
	}
	if len(sourceSHA) > 12 {
		sourceSHA = sourceSHA[:12]
	}
	tmpDir, err := os.MkdirTemp("", "cogos-ghpages-*")
	if err != nil {
		return fmt.Errorf("Deploy: create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	if err := gitRun(ctx, tmpDir, "init"); err != nil {
		return fmt.Errorf("Deploy: git init: %w", err)
	}
	if err := copyDir(artifactDir, tmpDir); err != nil {
		return fmt.Errorf("Deploy: copy artifact: %w", err)
	}
	// Record the content hash of the artifact this deploy ships, so a later
	// FetchLive can detect drift. Hash the source artifactDir (before CNAME and
	// .artifact-sha are added to the deploy tree) so it matches the hash Diff
	// computes over a freshly-built dist.
	sha, err := artifactHash(artifactDir)
	if err != nil {
		return fmt.Errorf("Deploy: hash artifact: %w", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, artifactSHAFile), []byte(sha), 0644); err != nil {
		return fmt.Errorf("Deploy: write %s: %w", artifactSHAFile, err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "CNAME"), []byte(app.Spec.Domain), 0644); err != nil {
		return fmt.Errorf("Deploy: write CNAME: %w", err)
	}
	if err := gitRun(ctx, tmpDir, "add", "."); err != nil {
		return fmt.Errorf("Deploy: git add: %w", err)
	}
	// Public-release gate on the GENERATED artifact, not the source.
	//
	// This is the only point in the pipeline that can catch a build-time leak.
	// `siteBuild` runs `bash build.sh` on the operator's machine, so anything
	// the build embeds from its environment — $HOME, `pwd`, tool output, env
	// vars — lands in the artifact having never existed in the `sites` repo.
	// A gate on `sites` cannot see it, and CI on the deploy target cannot stop
	// it: Pages publishes from `main` the instant this force-push lands, so a
	// check there is post-hoc by construction.
	//
	// This is the mod3 #146 shape exactly — a leak in a generated form that no
	// reviewer reads as a path — and the "machine-managed, therefore exempt"
	// assumption is wrong the same way `cog plan upstream` was: the exemption
	// covered the one path that emits content no human reviews.
	if err := gateArtifact(ctx, tmpDir); err != nil {
		return fmt.Errorf("Deploy: public release gate: %w", err)
	}
	msg := fmt.Sprintf("deploy: %s @ %s", app.Spec.Domain, sourceSHA)
	if err := gitRun(ctx, tmpDir, "-c", "user.email=cog@myrgic.io",
		"-c", "user.name=CogOS", "commit", "-m", msg); err != nil {
		return fmt.Errorf("Deploy: git commit: %w", err)
	}
	remote := fmt.Sprintf("https://github.com/%s.git", repo)
	if err := gitRun(ctx, tmpDir, "remote", "add", "origin", remote); err != nil {
		return fmt.Errorf("Deploy: git remote add: %w", err)
	}
	if err := gitRun(ctx, tmpDir, "push", "--force", "origin", "HEAD:main"); err != nil {
		return fmt.Errorf("Deploy: git push: %w", err)
	}
	return nil
}

// gateArtifact runs the public-release content gate over a built deploy
// artifact immediately before it is force-pushed to a public repository.
//
// FAILS CLOSED. Every failure mode — guard missing, python missing, config
// missing, unreadable, non-zero exit — aborts the deploy. The whole point of
// the incident that produced this gate (a .cogpublic that declared guards
// nothing executed for four and a half months) is that a check which cannot
// run must never be mistaken for a check that passed. A skipped gate here
// publishes to the open internet with no second chance: the target is a Pages
// repo, so `main` is live the moment the push lands.
//
// The gate scans the artifact's working tree rather than git-tracked files,
// because at this point the artifact is a fresh `git init` + `git add .` and
// every file in it is about to become public.
func gateArtifact(ctx context.Context, artifactDir string) error {
	root, err := repoRootForGuard()
	if err != nil {
		return fmt.Errorf("locate guard: %w", err)
	}
	guard := filepath.Join(root, "scripts", "cogpublic-guard.py")
	cfg := filepath.Join(root, ".cogpublic")
	for _, p := range []string{guard, cfg} {
		if _, statErr := os.Stat(p); statErr != nil {
			return fmt.Errorf("required file missing (%s): %w", p, statErr)
		}
	}

	// The artifact has no .cogpublic of its own; supply the kernel repo's
	// policy by copying it in, scanning, then removing it so the published
	// tree is unchanged.
	stagedCfg := filepath.Join(artifactDir, ".cogpublic")
	policy, err := os.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("read policy: %w", err)
	}
	if err := os.WriteFile(stagedCfg, policy, 0o644); err != nil {
		return fmt.Errorf("stage policy: %w", err)
	}
	defer os.Remove(stagedCfg)

	// Self-test first: a ruleset that stopped parsing must fail loudly rather
	// than scan against zero patterns and report clean.
	//
	// --tree, not the default HEAD scan: the artifact is `git init` + `git add`
	// with nothing committed, so `git ls-files` is empty and a HEAD scan would
	// examine zero files. Caught by this package's own positive control.
	for _, args := range [][]string{
		{guard, "--root", artifactDir, "--self-test"},
		{guard, "--root", artifactDir, "--tree"},
	} {
		cmd := exec.CommandContext(ctx, "python3", args...)
		cmd.Dir = artifactDir
		out, runErr := cmd.CombinedOutput()
		if runErr != nil {
			return fmt.Errorf("BLOCKED — refusing to publish %s:\n%s",
				artifactDir, strings.TrimSpace(string(out)))
		}
	}
	return nil
}

// repoRootForGuard resolves the cogos checkout holding the guard and policy.
// COGOS_REPO_ROOT wins when set (containers, CI); otherwise walk up from this
// source file's own directory.
func repoRootForGuard() (string, error) {
	if v := os.Getenv("COGOS_REPO_ROOT"); v != "" {
		return v, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, ".cogpublic")); statErr == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no .cogpublic found walking up from working directory; " +
				"set COGOS_REPO_ROOT to the cogos checkout")
		}
		dir = parent
	}
}

func gitRun(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return nil
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		return copyFile(path, target, info.Mode())
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
