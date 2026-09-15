// projection_reconciler.go
// ProjectionReconciler generates and maintains projection documents
// over the lineage observatory cogdoc corpus.
//
// The lineage observatory lives at .cog/mem/semantic/lineage/ and contains:
//   - nodes/    — one cogdoc per lineage node (threads + antecedents)
//   - projections/ — generated outputs (bibliography, lineage chain, etc.)
//   - SCHEMA.md — canonical field + edge type reference
//
// The ProjectionReconciler implements pkg/reconcile.Reconcilable. It:
//   1. Loads node cogdocs from the nodes/ directory as "config"
//   2. Computes what projections differ from current state
//   3. Writes the six canonical projection files
//   4. Watches the nodes/ directory for changes and triggers reconciliation
//
// Six theoretical-lineage projection instances served by ProjectionReconciler
// (per ADR-094), all reading the same nodes/ corpus:
//   - pedagogical-descent  — curriculum from Tier 1 antecedents to Tier 3/4 claims
//   - bibliography         — all Tier 1 nodes, citable references
//   - lineage-chain        — grounds/extends/supersedes traversal
//   - convergence-map      — convergence edge cross-reference
//   - open-questions       — open-gap edges + Tier 3/4 nodes
//   - outreach-status      — public_exposure_risk audit + demotion templates
//
// A SEVENTH projection — decision-lineage — is registered alongside these by
// RegisterProjectionProviders, but it is served by a SEPARATE reconciler
// (DecisionLineageReconciler, in decision_lineage_reconciler.go) because it
// reads a different corpus: the architecture's own decision records
// (architecture/{adrs,rfcs} + decision-insights), not the theoretical nodes/
// corpus. It is the decision-lineage sibling to convergence-map.
//
// ADR-094 §3: ProjectionReconciler is a substrate primitive.
// ADR-092 §4: Implements the seven-method Reconcilable contract.
// ADR-091 §5: Ledger-first rule — every reconcile cycle emits a ledger event.

package engine

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/myrgic/cogos/pkg/substrate/reconcile"
	"gopkg.in/yaml.v3"
)

// ─── State machine phases ─────────────────────────────────────────────────────

// ProjectionPhase represents the ProjectionReconciler's lifecycle state.
type ProjectionPhase string

const (
	// ProjectionStarting is the initial phase before the first reconcile.
	ProjectionStarting ProjectionPhase = "Starting"
	// ProjectionLive means the reconciler is running and projections are current.
	ProjectionLive ProjectionPhase = "Live"
	// ProjectionStalled means reconcile is running but no changes are detected
	// (synced with no outstanding work).
	ProjectionStalled ProjectionPhase = "Stalled"
	// ProjectionDetached means the watch source (nodes/ dir) is unreachable.
	ProjectionDetached ProjectionPhase = "Detached"
	// ProjectionCrashed means the reconciler encountered an unrecoverable error.
	ProjectionCrashed ProjectionPhase = "Crashed"
)

// ─── Node frontmatter ─────────────────────────────────────────────────────────

// LineageNodeRef is a typed edge from one lineage node to another.
type LineageNodeRef struct {
	Rel  string `yaml:"rel"`
	URI  string `yaml:"uri"`
	Note string `yaml:"note,omitempty"`
}

// LineageNodeFrontmatter is the parsed frontmatter of a lineage node cogdoc.
type LineageNodeFrontmatter struct {
	ID                 string           `yaml:"id"`
	Kind               string           `yaml:"kind"`
	Tier               int              `yaml:"tier"`
	Title              string           `yaml:"title"`
	PublicExposureRisk string           `yaml:"public_exposure_risk"`
	DemotionTemplate   string           `yaml:"demotion_template,omitempty"`
	CorpusDepth        string           `yaml:"corpus_depth,omitempty"`
	Refs               []LineageNodeRef `yaml:"refs,omitempty"`
	Corrected          string           `yaml:"corrected,omitempty"`
	CorrectionNote     string           `yaml:"correction_note,omitempty"`
	Created            string           `yaml:"created,omitempty"`
	Updated            string           `yaml:"updated,omitempty"`
}

// LineageNode is a parsed lineage node cogdoc with its source path.
type LineageNode struct {
	Path        string
	Frontmatter LineageNodeFrontmatter
	Body        string // content after frontmatter
}

// ─── Projection config ────────────────────────────────────────────────────────

// ProjectionKind identifies which of the six projection types to generate.
type ProjectionKind string

const (
	ProjectionPedagogicalDescent ProjectionKind = "pedagogical-descent"
	ProjectionBibliography       ProjectionKind = "bibliography"
	ProjectionLineageChain       ProjectionKind = "lineage-chain"
	ProjectionConvergenceMap     ProjectionKind = "convergence-map"
	ProjectionOpenQuestions      ProjectionKind = "open-questions"
	ProjectionOutreachStatus     ProjectionKind = "outreach-status"

	// ProjectionDecisionLineage projects the decision manifold — the
	// gravity/inertia field over the corpus's own architectural decisions
	// (ADRs, RFCs, ratified decision-insights). It is the missing sibling to
	// ProjectionConvergenceMap: convergence-map projects the *theoretical
	// antecedent* lineage; decision-lineage projects the *decision* lineage.
	//
	// Unlike the six theoretical-lineage kinds (which read the same nodes/
	// corpus through ProjectionReconciler), decision-lineage reads a different
	// corpus (the decision records) and is served by DecisionLineageReconciler.
	// It is registered alongside the others via RegisterProjectionProviders.
	ProjectionDecisionLineage ProjectionKind = "decision-lineage"
)

// AllProjectionKinds lists the six theoretical-lineage projections served by
// ProjectionReconciler, in generation order. It deliberately does NOT include
// ProjectionDecisionLineage: that seventh kind is served by the separate
// DecisionLineageReconciler (it reads a different corpus) and is registered on
// its own in RegisterProjectionProviders. This slice is the set of kinds the
// nodes/-corpus reconciler and its watcher iterate.
var AllProjectionKinds = []ProjectionKind{
	ProjectionPedagogicalDescent,
	ProjectionBibliography,
	ProjectionLineageChain,
	ProjectionConvergenceMap,
	ProjectionOpenQuestions,
	ProjectionOutreachStatus,
}

// ProjectionConfig is the "declared config" the reconciler loads from disk.
// It encodes the node directory path and which projection kinds are enabled.
type ProjectionConfig struct {
	NodesDir      string
	ProjectionDir string
	Kinds         []ProjectionKind
}

// ─── ProjectionReconciler ─────────────────────────────────────────────────────

// ProjectionReconciler implements reconcile.Reconcilable for the lineage
// observatory projection system. It reads lineage node cogdocs, computes
// projection diffs, and writes the six canonical projection files.
//
// Each instance manages one projection kind. The observatory registers six
// instances — one per AllProjectionKinds entry — via reconcile.RegisterProvider
// at init time. D5 (registration) calls RegisterProjectionProviders().
type ProjectionReconciler struct {
	mu     sync.Mutex
	kind   ProjectionKind
	phase  ProjectionPhase
	health reconcile.ResourceStatus

	// watcher is the fsnotify watcher for the nodes/ directory.
	// Non-nil when the watch loop is running.
	watcher *fsnotify.Watcher
}

// NewProjectionReconciler creates a ProjectionReconciler for the given kind.
func NewProjectionReconciler(kind ProjectionKind) *ProjectionReconciler {
	return &ProjectionReconciler{
		kind:  kind,
		phase: ProjectionStarting,
		health: reconcile.NewResourceStatus(
			reconcile.SyncStatusUnknown,
			reconcile.HealthProgressing,
		),
	}
}

// ─── Reconcilable interface ───────────────────────────────────────────────────

// Type returns the provider type identifier.
// Format: "lineage-projection-<kind>"
func (r *ProjectionReconciler) Type() string {
	return "lineage-projection-" + string(r.kind)
}

// LoadConfig discovers the nodes/ directory from the workspace root and
// returns a ProjectionConfig. This is a read-only disk operation.
//
// When the lineage nodes directory does not yet exist (fresh install, or
// workspace pre-dating the lineage observatory), LoadConfig logs at DEBUG
// and returns nil so the reconcile cycle exits cleanly with no drift rather
// than WARNing on every tick. The directory is created by `cogos init` once
// the user opts into the lineage corpus.
func (r *ProjectionReconciler) LoadConfig(root string) (any, error) {
	lineageBase := filepath.Join(root, ".cog", "mem", "semantic", "lineage")
	nodesDir := filepath.Join(lineageBase, "nodes")
	projDir := filepath.Join(lineageBase, "projections")

	// Verify nodes directory exists; skip quietly if absent.
	if _, err := os.Stat(nodesDir); err != nil {
		if os.IsNotExist(err) {
			slog.Debug("lineage-projection: nodes dir absent, skipping",
				"kind", r.kind, "dir", nodesDir)
			return nil, nil
		}
		return nil, fmt.Errorf("stat nodes dir: %w", err)
	}
	if err := os.MkdirAll(projDir, 0755); err != nil {
		return nil, fmt.Errorf("create projections dir: %w", err)
	}

	return &ProjectionConfig{
		NodesDir:      nodesDir,
		ProjectionDir: projDir,
		Kinds:         []ProjectionKind{r.kind},
	}, nil
}

// FetchLive reads all .cog.md files from the nodes/ directory and parses
// their frontmatter. This is a read-only observation of the world.
func (r *ProjectionReconciler) FetchLive(ctx context.Context, config any) (any, error) {
	// nil config means nodes dir absent — nothing to observe.
	if config == nil {
		return []LineageNode{}, nil
	}
	cfg, ok := config.(*ProjectionConfig)
	if !ok {
		return nil, fmt.Errorf("projection reconciler: unexpected config type %T", config)
	}

	entries, err := os.ReadDir(cfg.NodesDir)
	if err != nil {
		r.mu.Lock()
		r.phase = ProjectionDetached
		r.mu.Unlock()
		return nil, fmt.Errorf("reading nodes dir: %w", err)
	}

	var nodes []LineageNode
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".cog.md") {
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		nodePath := filepath.Join(cfg.NodesDir, name)
		node, err := parseLineageNode(nodePath)
		if err != nil {
			log.Printf("[projection-reconciler] skipping %s: %v", name, err)
			continue
		}
		nodes = append(nodes, *node)
	}

	// Sort by tier ascending, then ID for deterministic output.
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Frontmatter.Tier != nodes[j].Frontmatter.Tier {
			return nodes[i].Frontmatter.Tier < nodes[j].Frontmatter.Tier
		}
		return nodes[i].Frontmatter.ID < nodes[j].Frontmatter.ID
	})

	return nodes, nil
}

// ComputePlan compares the current projection file content against what
// would be generated from the live nodes. Returns a Plan with one action
// per projection file that differs or is missing.
//
// This is a pure function: deterministic given (config, live, state).
func (r *ProjectionReconciler) ComputePlan(config any, live any, state *reconcile.State) (*reconcile.Plan, error) {
	// nil config means nodes dir absent — nothing to project.
	if config == nil {
		return &reconcile.Plan{ResourceType: r.Type()}, nil
	}
	cfg := config.(*ProjectionConfig)
	nodes := live.([]LineageNode)

	projected := generateProjection(r.kind, nodes)
	projPath := filepath.Join(cfg.ProjectionDir, string(r.kind)+".md")

	plan := &reconcile.Plan{
		ResourceType: r.Type(),
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		ConfigPath:   cfg.NodesDir,
	}

	// Check current file
	existing, err := os.ReadFile(projPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("reading projection %s: %w", projPath, err)
	}

	if err != nil || !equalIgnoringTimestamp(existing, []byte(projected)) {
		action := reconcile.ActionCreate
		if err == nil {
			action = reconcile.ActionUpdate
		}
		plan.Actions = append(plan.Actions, reconcile.Action{
			Action:       action,
			ResourceType: r.Type(),
			Name:         string(r.kind),
			Details: map[string]any{
				"path":       projPath,
				"node_count": len(nodes),
				"kind":       string(r.kind),
				// Carry the rendered projection so ApplyPlan writes it directly
				// instead of re-reading + re-parsing the whole nodes corpus.
				"content": projected,
			},
		})
		if action == reconcile.ActionCreate {
			plan.Summary.Creates++
		} else {
			plan.Summary.Updates++
		}
	} else {
		plan.Actions = append(plan.Actions, reconcile.Action{
			Action:       reconcile.ActionSkip,
			ResourceType: r.Type(),
			Name:         string(r.kind),
			Details:      map[string]any{"reason": "content unchanged"},
		})
		plan.Summary.Skipped++
	}

	return plan, nil
}

// ApplyPlan writes the projection file(s) described in the plan.
// Idempotent: writing the same content twice produces the same result.
func (r *ProjectionReconciler) ApplyPlan(ctx context.Context, plan *reconcile.Plan) ([]reconcile.Result, error) {
	var results []reconcile.Result

	for _, action := range plan.Actions {
		if action.Action == reconcile.ActionSkip {
			results = append(results, reconcile.Result{
				Phase:  "apply",
				Action: string(action.Action),
				Name:   action.Name,
				Status: reconcile.ApplySkipped,
			})
			continue
		}

		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}

		projPath, _ := action.Details["path"].(string)
		if projPath == "" {
			results = append(results, reconcile.Result{
				Phase:  "apply",
				Action: string(action.Action),
				Name:   action.Name,
				Status: reconcile.ApplyFailed,
				Error:  "missing path in action details",
			})
			continue
		}

		// Use the projection ComputePlan already rendered this cycle. Only fall
		// back to re-reading + re-parsing the whole nodes corpus (the redundant
		// O(corpus) work FetchLive/ComputePlan already did) if a caller built
		// the plan without content — e.g. a test or a hand-built plan.
		content, _ := action.Details["content"].(string)
		if content == "" {
			nodesDir := filepath.Dir(filepath.Dir(projPath)) // projections/../ = lineage/
			nodesDir = filepath.Join(nodesDir, "nodes")

			entries, derr := os.ReadDir(nodesDir)
			if derr != nil {
				results = append(results, reconcile.Result{
					Phase:  "apply",
					Action: string(action.Action),
					Name:   action.Name,
					Status: reconcile.ApplyFailed,
					Error:  fmt.Sprintf("reload nodes: %v", derr),
				})
				continue
			}

			var nodes []LineageNode
			for _, entry := range entries {
				if !strings.HasSuffix(entry.Name(), ".cog.md") {
					continue
				}
				node, perr := parseLineageNode(filepath.Join(nodesDir, entry.Name()))
				if perr == nil {
					nodes = append(nodes, *node)
				}
			}
			sort.Slice(nodes, func(i, j int) bool {
				if nodes[i].Frontmatter.Tier != nodes[j].Frontmatter.Tier {
					return nodes[i].Frontmatter.Tier < nodes[j].Frontmatter.Tier
				}
				return nodes[i].Frontmatter.ID < nodes[j].Frontmatter.ID
			})

			content = generateProjection(r.kind, nodes)
		}

		// Atomic write: tmp + rename.
		tmp := projPath + ".tmp"
		if err := os.WriteFile(tmp, []byte(content), 0644); err != nil {
			results = append(results, reconcile.Result{
				Phase:  "apply",
				Action: string(action.Action),
				Name:   action.Name,
				Status: reconcile.ApplyFailed,
				Error:  fmt.Sprintf("write tmp: %v", err),
			})
			continue
		}
		if err := os.Rename(tmp, projPath); err != nil {
			os.Remove(tmp)
			results = append(results, reconcile.Result{
				Phase:  "apply",
				Action: string(action.Action),
				Name:   action.Name,
				Status: reconcile.ApplyFailed,
				Error:  fmt.Sprintf("rename: %v", err),
			})
			continue
		}

		r.mu.Lock()
		r.phase = ProjectionLive
		r.health = reconcile.NewResourceStatus(reconcile.SyncStatusSynced, reconcile.HealthHealthy)
		r.mu.Unlock()

		results = append(results, reconcile.Result{
			Phase:  "apply",
			Action: string(action.Action),
			Name:   action.Name,
			Status: reconcile.ApplySucceeded,
		})

		nodeCount, _ := action.Details["node_count"].(int)
		log.Printf("[projection-reconciler] wrote %s (%d nodes)", projPath, nodeCount)
	}

	return results, nil
}

// BuildState constructs the reconciler state from live data.
// Pure function — no side effects.
func (r *ProjectionReconciler) BuildState(config any, live any, existing *reconcile.State) (*reconcile.State, error) {
	// nil config means nodes dir absent — return empty state.
	if config == nil {
		return reconcile.NewState(r.Type()), nil
	}
	nodes := live.([]LineageNode)
	cfg := config.(*ProjectionConfig)

	state := reconcile.NewState(r.Type())
	if existing != nil {
		state.Lineage = existing.Lineage
		state.Serial = existing.Serial
	}

	for _, node := range nodes {
		state.Resources = append(state.Resources, reconcile.Resource{
			Address:    "lineage-node." + node.Frontmatter.ID,
			Type:       "lineage-node",
			Mode:       reconcile.ModeManaged,
			Name:       node.Frontmatter.ID,
			ExternalID: node.Path,
			Attributes: map[string]any{
				"tier":                 node.Frontmatter.Tier,
				"public_exposure_risk": node.Frontmatter.PublicExposureRisk,
				"corpus_depth":         node.Frontmatter.CorpusDepth,
				"ref_count":            len(node.Frontmatter.Refs),
			},
			LastRefreshed: time.Now().UTC().Format(time.RFC3339),
		})
	}

	// Add the projection file itself as a tracked resource.
	projPath := filepath.Join(cfg.ProjectionDir, string(r.kind)+".md")
	state.Resources = append(state.Resources, reconcile.Resource{
		Address:    "lineage-projection." + string(r.kind),
		Type:       "lineage-projection",
		Mode:       reconcile.ModeManaged,
		Name:       string(r.kind),
		ExternalID: projPath,
		Attributes: map[string]any{
			"kind": string(r.kind),
		},
		LastRefreshed: time.Now().UTC().Format(time.RFC3339),
	})

	return state, nil
}

// Health returns the current three-axis status of this reconciler.
func (r *ProjectionReconciler) Health() reconcile.ResourceStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.health
}

// ─── Watch mechanism (D4) ─────────────────────────────────────────────────────

// ProjectionWatcher watches the lineage nodes/ directory using fsnotify
// and triggers reconciliation on any write event.
//
// This implements ADR-094 §4 (watch/trigger mechanism). On each WRITE or CREATE
// event in the nodes/ directory, it calls the provided trigger function after
// a 500ms debounce. If fsnotify is unavailable, falls back to polling at
// pollInterval.
type ProjectionWatcher struct {
	NodesDir     string
	Trigger      func(ctx context.Context) error
	PollInterval time.Duration

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	watcher *fsnotify.Watcher
}

// NewProjectionWatcher creates a watcher for the given nodes directory.
// trigger is called after each debounced file-system event.
// pollInterval is the fallback polling interval when fsnotify is unavailable;
// 0 uses the default of 5 seconds.
func NewProjectionWatcher(nodesDir string, trigger func(ctx context.Context) error, pollInterval time.Duration) *ProjectionWatcher {
	if pollInterval == 0 {
		pollInterval = 5 * time.Second
	}
	return &ProjectionWatcher{
		NodesDir:     nodesDir,
		Trigger:      trigger,
		PollInterval: pollInterval,
	}
}

// Start begins watching the nodes/ directory. Non-blocking: the watch loop
// runs in a goroutine. Returns an error if already running or if the
// directory does not exist.
func (w *ProjectionWatcher) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		return fmt.Errorf("projection watcher already running")
	}
	if _, err := os.Stat(w.NodesDir); err != nil {
		return fmt.Errorf("nodes dir stat: %w", err)
	}

	w.stopCh = make(chan struct{})
	w.running = true

	// Try fsnotify; fall back to polling on failure.
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("[projection-watcher] fsnotify unavailable (%v), using polling fallback", err)
		fsWatcher = nil
	} else if err := fsWatcher.Add(w.NodesDir); err != nil {
		log.Printf("[projection-watcher] cannot watch %s (%v), using polling fallback", w.NodesDir, err)
		fsWatcher.Close()
		fsWatcher = nil
	}
	w.watcher = fsWatcher

	if fsWatcher != nil {
		go w.runFsnotify(ctx, fsWatcher)
	} else {
		go w.runPolling(ctx)
	}

	return nil
}

// Stop halts the watch loop and releases resources.
func (w *ProjectionWatcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running {
		return
	}
	close(w.stopCh)
	w.running = false
	if w.watcher != nil {
		w.watcher.Close()
		w.watcher = nil
	}
}

// runFsnotify runs the event-driven watch loop.
func (w *ProjectionWatcher) runFsnotify(ctx context.Context, fsWatcher *fsnotify.Watcher) {
	// Debounce: coalesce rapid writes within a 500ms window.
	const debounce = 500 * time.Millisecond
	var debounceTimer *time.Timer
	var timerMu sync.Mutex

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case event, ok := <-fsWatcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Remove) {
				timerMu.Lock()
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.AfterFunc(debounce, func() {
					if err := w.Trigger(ctx); err != nil {
						log.Printf("[projection-watcher] trigger error: %v", err)
					}
				})
				timerMu.Unlock()
			}
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return
			}
			log.Printf("[projection-watcher] fsnotify error: %v", err)
		}
	}
}

// runPolling runs the fallback polling loop.
func (w *ProjectionWatcher) runPolling(ctx context.Context) {
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()

	// Track last-seen modification time of the nodes directory.
	lastMod := time.Time{}

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(w.NodesDir)
			if err != nil {
				log.Printf("[projection-watcher] poll stat error: %v", err)
				continue
			}
			if info.ModTime().After(lastMod) {
				lastMod = info.ModTime()
				if err := w.Trigger(ctx); err != nil {
					log.Printf("[projection-watcher] trigger error: %v", err)
				}
			}
		}
	}
}

// ─── Registration (D5 entry point) ───────────────────────────────────────────

// RegisterProjectionProviders registers all six ProjectionReconciler instances
// with the global reconcile registry. Called from init() in the registration
// file (projection_reconciler_register.go).
//
// This is the D5 entry point. Each instance is keyed by its Type() string,
// e.g., "lineage-projection-bibliography".
//
// Uses UpsertProvider (not RegisterProvider) so it is safe to call multiple
// times — e.g., after a reconcile.ResetProviders() call in tests.
func RegisterProjectionProviders() {
	for _, kind := range AllProjectionKinds {
		reconcile.UpsertProvider(
			"lineage-projection-"+string(kind),
			NewProjectionReconciler(kind),
		)
	}

	// The decision-lineage projection is a sibling kind served by a distinct
	// reconciler (it reads the decision corpus, not the theoretical nodes/
	// corpus). It registers under the same projection-kind namespace so the
	// CLI reconcile path and observability treat it uniformly.
	reconcile.UpsertProvider(
		"lineage-projection-"+string(ProjectionDecisionLineage),
		NewDecisionLineageReconciler(),
	)
}

// ─── Projection generators ────────────────────────────────────────────────────

// generateProjection renders the content of the given projection kind
// from the provided sorted nodes slice.
func generateProjection(kind ProjectionKind, nodes []LineageNode) string {
	switch kind {
	case ProjectionPedagogicalDescent:
		return generatePedagogicalDescent(nodes)
	case ProjectionBibliography:
		return generateBibliography(nodes)
	case ProjectionLineageChain:
		return generateLineageChain(nodes)
	case ProjectionConvergenceMap:
		return generateConvergenceMap(nodes)
	case ProjectionOpenQuestions:
		return generateOpenQuestions(nodes)
	case ProjectionOutreachStatus:
		return generateOutreachStatus(nodes)
	default:
		return fmt.Sprintf("# Unknown projection kind: %s\n", kind)
	}
}

func generatePedagogicalDescent(nodes []LineageNode) string {
	var b strings.Builder
	b.WriteString("# Pedagogical Descent\n\n")
	b.WriteString("_Generated by ProjectionReconciler — do not edit by hand._\n\n")
	b.WriteString(fmt.Sprintf("_Last generated: %s_\n\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString("Ordered curriculum from established antecedents (Tier 1) to working hypotheses (Tier 3/4).\n\n")

	tiers := map[int][]LineageNode{}
	for _, n := range nodes {
		tiers[n.Frontmatter.Tier] = append(tiers[n.Frontmatter.Tier], n)
	}

	tierNames := map[int]string{
		1: "Tier 1: Established Science (citable antecedents)",
		2: "Tier 2: Structural Parallels (well-grounded; not peer-reviewed as stated)",
		3: "Tier 3: Working Hypotheses (operator-developed; requires validation)",
		4: "Tier 4: Speculative / Universal Claims (requires explicit demotion before publication)",
	}

	for _, tier := range []int{1, 2, 3, 4} {
		tierNodes := tiers[tier]
		if len(tierNodes) == 0 {
			continue
		}
		b.WriteString("## " + tierNames[tier] + "\n\n")
		for _, n := range tierNodes {
			b.WriteString(fmt.Sprintf("### %s\n\n", n.Frontmatter.Title))
			if n.Frontmatter.DemotionTemplate != "" {
				b.WriteString("> **Public framing:** " + n.Frontmatter.DemotionTemplate + "\n\n")
			}
		}
	}
	return b.String()
}

func generateBibliography(nodes []LineageNode) string {
	var b strings.Builder
	b.WriteString("# Bibliography\n\n")
	b.WriteString("_Generated by ProjectionReconciler — do not edit by hand._\n\n")
	b.WriteString(fmt.Sprintf("_Last generated: %s_\n\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString("All Tier 1 nodes with citable external references.\n\n")

	for _, n := range nodes {
		if n.Frontmatter.Tier != 1 {
			continue
		}
		b.WriteString(fmt.Sprintf("- **%s** (`%s`)\n", n.Frontmatter.Title, n.Frontmatter.ID))
	}
	if b.Len() == 0 {
		b.WriteString("_(No Tier 1 nodes yet)_\n")
	}
	return b.String()
}

func generateLineageChain(nodes []LineageNode) string {
	var b strings.Builder
	b.WriteString("# Lineage Chain\n\n")
	b.WriteString("_Generated by ProjectionReconciler — do not edit by hand._\n\n")
	b.WriteString(fmt.Sprintf("_Last generated: %s_\n\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString("Traversal of grounds → extends → supersedes edges across all nodes.\n\n")

	relTypes := []string{"grounds", "extends", "supersedes"}
	for _, n := range nodes {
		relevant := filterRefs(n.Frontmatter.Refs, relTypes)
		if len(relevant) == 0 {
			continue
		}
		b.WriteString(fmt.Sprintf("### %s (`%s`, Tier %d)\n\n", n.Frontmatter.Title, n.Frontmatter.ID, n.Frontmatter.Tier))
		for _, ref := range relevant {
			note := ""
			if ref.Note != "" {
				note = " — " + ref.Note
			}
			b.WriteString(fmt.Sprintf("- `%s` → `%s`%s\n", ref.Rel, ref.URI, note))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func generateConvergenceMap(nodes []LineageNode) string {
	var b strings.Builder
	b.WriteString("# Convergence Map\n\n")
	b.WriteString("_Generated by ProjectionReconciler — do not edit by hand._\n\n")
	b.WriteString(fmt.Sprintf("_Last generated: %s_\n\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString("All convergence edges: nodes that arrive at the same structural form independently.\n\n")

	for _, n := range nodes {
		convRefs := filterRefs(n.Frontmatter.Refs, []string{"convergence"})
		if len(convRefs) == 0 {
			continue
		}
		b.WriteString(fmt.Sprintf("### %s\n\n", n.Frontmatter.Title))
		for _, ref := range convRefs {
			note := ""
			if ref.Note != "" {
				note = ": " + ref.Note
			}
			b.WriteString(fmt.Sprintf("- Converges with `%s`%s\n", ref.URI, note))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func generateOpenQuestions(nodes []LineageNode) string {
	var b strings.Builder
	b.WriteString("# Open Questions\n\n")
	b.WriteString("_Generated by ProjectionReconciler — do not edit by hand._\n\n")
	b.WriteString(fmt.Sprintf("_Last generated: %s_\n\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString("Open-gap edges and Tier 3/4 nodes pending promotion.\n\n")

	// Open-gap edges.
	b.WriteString("## Open-Gap Edges\n\n")
	gapCount := 0
	for _, n := range nodes {
		gapRefs := filterRefs(n.Frontmatter.Refs, []string{"open-gap"})
		if len(gapRefs) == 0 {
			continue
		}
		for _, ref := range gapRefs {
			note := ""
			if ref.Note != "" {
				note = " — " + ref.Note
			}
			b.WriteString(fmt.Sprintf("- **%s** → `%s`%s\n", n.Frontmatter.Title, ref.URI, note))
			gapCount++
		}
	}
	if gapCount == 0 {
		b.WriteString("_(No open-gap edges yet)_\n")
	}

	// Tier 3/4 nodes awaiting validation.
	b.WriteString("\n## Tier 3/4 Nodes Awaiting Promotion\n\n")
	speculativeCount := 0
	for _, n := range nodes {
		if n.Frontmatter.Tier < 3 {
			continue
		}
		b.WriteString(fmt.Sprintf("- **%s** (Tier %d, exposure: %s)\n",
			n.Frontmatter.Title, n.Frontmatter.Tier, n.Frontmatter.PublicExposureRisk))
		if n.Frontmatter.DemotionTemplate != "" {
			b.WriteString(fmt.Sprintf("  > Demotion: %s\n", n.Frontmatter.DemotionTemplate))
		}
		speculativeCount++
	}
	if speculativeCount == 0 {
		b.WriteString("_(No Tier 3/4 nodes yet)_\n")
	}

	return b.String()
}

func generateOutreachStatus(nodes []LineageNode) string {
	var b strings.Builder
	b.WriteString("# Outreach Status\n\n")
	b.WriteString("_Generated by ProjectionReconciler — do not edit by hand._\n\n")
	b.WriteString(fmt.Sprintf("_Last generated: %s_\n\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString("Public exposure audit: nodes with demotion templates for external communication.\n\n")

	for _, risk := range []string{"high", "medium"} {
		var matching []LineageNode
		for _, n := range nodes {
			if n.Frontmatter.PublicExposureRisk == risk {
				matching = append(matching, n)
			}
		}
		if len(matching) == 0 {
			continue
		}
		b.WriteString(fmt.Sprintf("## Risk: %s\n\n", strings.ToUpper(risk[:1])+risk[1:]))
		for _, n := range matching {
			b.WriteString(fmt.Sprintf("### %s (Tier %d)\n\n", n.Frontmatter.Title, n.Frontmatter.Tier))
			if n.Frontmatter.DemotionTemplate != "" {
				b.WriteString("> **Public framing:** " + n.Frontmatter.DemotionTemplate + "\n\n")
			} else {
				b.WriteString("> _(No demotion template defined — required for Tier 3 nodes)_\n\n")
			}
		}
	}
	return b.String()
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// filterRefs returns refs whose Rel field is in the allowed set.
func filterRefs(refs []LineageNodeRef, allowed []string) []LineageNodeRef {
	set := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		set[a] = true
	}
	var out []LineageNodeRef
	for _, ref := range refs {
		if set[ref.Rel] {
			out = append(out, ref)
		}
	}
	return out
}

// parseLineageNode reads and parses a cogdoc file, extracting YAML frontmatter.
// The file is expected to have the form:
//
//	---
//	<yaml frontmatter>
//	---
//	<body>
func parseLineageNode(path string) (*LineageNode, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	content := string(data)
	if !strings.HasPrefix(content, "---") {
		return nil, fmt.Errorf("%s: no frontmatter delimiter", path)
	}

	scanner := bufio.NewScanner(strings.NewReader(content))
	scanner.Scan() // skip opening ---

	var fmLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "---" {
			break
		}
		fmLines = append(fmLines, line)
	}

	var fm LineageNodeFrontmatter
	if err := yaml.Unmarshal([]byte(strings.Join(fmLines, "\n")), &fm); err != nil {
		return nil, fmt.Errorf("parse frontmatter in %s: %w", path, err)
	}

	// Remaining lines are the body.
	var bodyLines []string
	for scanner.Scan() {
		bodyLines = append(bodyLines, scanner.Text())
	}

	return &LineageNode{
		Path:        path,
		Frontmatter: fm,
		Body:        strings.Join(bodyLines, "\n"),
	}, nil
}
