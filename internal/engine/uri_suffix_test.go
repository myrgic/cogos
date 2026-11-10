package engine

import (
	"os"
	"path/filepath"
	"testing"
)

// memSampleResolvable is a frozen sample of cog://mem/... URIs as written in
// insights/architecture cogdocs, paired with the on-disk file (relative to
// .cog/mem/) each one denotes.  The links are written with and without
// .cog.md, and some with a bare .md; all must resolve as written.
// memSampleDead are sample links with no file behind them at all; the
// resolver must not invent a path that exists for them.
var memSampleResolvable = []struct{ uri, onDisk string }{
	{"cog://mem/semantic/insights/eclipse-rocm-windows-status", "semantic/insights/eclipse-rocm-windows-status.cog.md"},
	{"cog://mem/semantic/architecture/identity-trust-module", "semantic/architecture/identity-trust-module.cog.md"},
	{"cog://mem/semantic/insights/multi-head-observer-anticipation.cog.md", "semantic/insights/multi-head-observer-anticipation.cog.md"},
	{"cog://mem/reflective/2026-06-05-consciousness-as-vestibular-sense-of-temporal-self-alignment", "reflective/2026-06-05-consciousness-as-vestibular-sense-of-temporal-self-alignment.cog.md"},
	{"cog://mem/semantic/research/convergent-foveation-survey-expansion", "semantic/research/convergent-foveation-survey-expansion.cog.md"},
	{"cog://mem/semantic/architecture/2026-08-02-myrgic-frame-doctrine", "semantic/architecture/2026-08-02-myrgic-frame-doctrine.cog.md"},
	{"cog://mem/semantic/insights/insight-cog-as-fundamental-force", "semantic/insights/insight-cog-as-fundamental-force.md"},
	{"cog://mem/procedural/guides/two-session-bus-coordination", "procedural/guides/two-session-bus-coordination.cog.md"},
	{"cog://mem/semantic/research/2026-06-14-eta-tower-drive-to-ground-verdict", "semantic/research/2026-06-14-eta-tower-drive-to-ground-verdict.cog.md"},
	{"cog://mem/semantic/architecture/workspace-blob-crystal-furnace-synthesis", "semantic/architecture/workspace-blob-crystal-furnace-synthesis.cog.md"},
	{"cog://mem/semantic/insights/springdrift-trm-eval-alignment", "semantic/insights/springdrift-trm-eval-alignment.cog.md"},
	{"cog://mem/semantic/insights/quarters-over-sprints", "semantic/insights/quarters-over-sprints.cog.md"},
	{"cog://mem/reflective/2026-05-21-stigmergy-reflection", "reflective/2026-05-21-stigmergy-reflection.cog.md"},
	{"cog://mem/semantic/insights/2026-09-16-genesis-and-diffs-the-ledger-form-of-the-self", "semantic/insights/2026-09-16-genesis-and-diffs-the-ledger-form-of-the-self.cog.md"},
	{"cog://mem/semantic/research/link-reviews/2026-05-11-engaged/synthesis/19-noon-paper-digest.cog.md", "semantic/research/link-reviews/2026-05-11-engaged/synthesis/19-noon-paper-digest.cog.md"},
	{"cog://mem/semantic/insights/second-order-cybernetic-foundation-novelty-verdict", "semantic/insights/second-order-cybernetic-foundation-novelty-verdict.cog.md"},
	{"cog://mem/procedural/plans/v3-implementation-roadmap", "procedural/plans/v3-implementation-roadmap.cog.md"},
	{"cog://mem/semantic/research/partition-function-theorem-2026-03-11", "semantic/research/partition-function-theorem-2026-03-11.cog.md"},
	{"cog://mem/semantic/architecture/cogos-openclaw-boundary", "semantic/architecture/cogos-openclaw-boundary.cog.md"},
	{"cog://mem/semantic/design-seeds/moe-axes-for-cpu-resident-organ", "semantic/design-seeds/moe-axes-for-cpu-resident-organ.cog.md"},
	{"cog://mem/working/scorecard-v2-2026-11-05", "working/scorecard-v2-2026-11-05.md"},
	{"cog://mem/working/hermes-overflow/hermes-evicted-memory-20260525T221426Z-e8a3ff2cfe53.cog.md", "working/hermes-overflow/hermes-evicted-memory-20260525T221426Z-e8a3ff2cfe53.cog.md"},
	{"cog://mem/semantic/terms/skills-as-leaves", "semantic/terms/skills-as-leaves.cog.md"},
	{"cog://mem/semantic/insights/neuromodulatory-architecture-convergence", "semantic/insights/neuromodulatory-architecture-convergence.cog.md"},
	{"cog://mem/semantic/design-seeds/fractal-liveness-pointer-SEALED", "semantic/design-seeds/fractal-liveness-pointer-SEALED.md"},
	{"cog://mem/identities/cog/architecture/ln2-cascade-unification", "identities/cog/architecture/ln2-cascade-unification.cog.md"},
	{"cog://mem/semantic/research/link-reviews/2026-10-21-engaged/03-microsoft-research-escapement", "semantic/research/link-reviews/2026-10-21-engaged/03-microsoft-research-escapement.cog.md"},
	{"cog://mem/semantic/architecture/corpus-checkpoint-2026-04-14", "semantic/architecture/corpus-checkpoint-2026-04-14.md"},
	{"cog://mem/semantic/insights/communion-synthesis-2026-01-09", "semantic/insights/communion-synthesis-2026-01-09.cog.md"},
	{"cog://mem/semantic/insights/rag-is-pull-mediation-is-push", "semantic/insights/rag-is-pull-mediation-is-push.cog.md"},
	{"cog://mem/semantic/architecture/foveated-context-engine-and-architecture-state", "semantic/architecture/foveated-context-engine-and-architecture-state.cog.md"},
	{"cog://mem/semantic/architecture/cogos-as-cognitive-operating-system.cog.md", "semantic/architecture/cogos-as-cognitive-operating-system.cog.md"},
	{"cog://mem/semantic/insights/coupling-does-not-predict-restructuring-cost", "semantic/insights/coupling-does-not-predict-restructuring-cost.cog.md"},
	{"cog://mem/semantic/research/local-voice-conversation-research-prospect", "semantic/research/local-voice-conversation-research-prospect.cog.md"},
	{"cog://mem/semantic/architecture/plans/cogos-cloud-node/cogos-cloud-node-plan", "semantic/architecture/plans/cogos-cloud-node/cogos-cloud-node-plan.cog.md"},
	{"cog://mem/semantic/architecture/block-sync-protocol", "semantic/architecture/block-sync-protocol.cog.md"},
	{"cog://mem/working/stage-kernel-taxonomy-audit-2026-11-05", "working/stage-kernel-taxonomy-audit-2026-11-05.md"},
	{"cog://mem/semantic/architecture/recursive-node-convergence", "semantic/architecture/recursive-node-convergence.cog.md"},
	{"cog://mem/semantic/insights/knot-language-is-wrong-for-the-doc-graph", "semantic/insights/knot-language-is-wrong-for-the-doc-graph.cog.md"},
	{"cog://mem/semantic/architecture/agent-experience-control-surface", "semantic/architecture/agent-experience-control-surface.cog.md"},
	{"cog://mem/semantic/research/gauge-group-emergence-2026-03-12", "semantic/research/gauge-group-emergence-2026-03-12.cog.md"},
	{"cog://mem/semantic/research/myrgic-research-program-orientation.cog.md", "semantic/research/myrgic-research-program-orientation.cog.md"},
	{"cog://mem/working/2026-11-03-r15-ops-log", "working/2026-11-03-r15-ops-log.cog.md"},
	{"cog://mem/semantic/architecture/ucp-secrets", "semantic/architecture/ucp-secrets.cog.md"},
	{"cog://mem/semantic/projects/portfolio/overnight-runs/research-superwhisper-integration.md", "semantic/projects/portfolio/overnight-runs/research-superwhisper-integration.md"},
	{"cog://mem/semantic/research/consciousness/research-project/domains/temporal/DOMAIN-STATUS-REPORT", "semantic/research/consciousness/research-project/domains/temporal/DOMAIN-STATUS-REPORT.cog.md"},
	{"cog://mem/semantic/insights/self-observing-retrieval-loop", "semantic/insights/self-observing-retrieval-loop.cog.md"},
}

var memSampleDead = []string{
	"cog://mem/insights/claude-eigenform-continuity.md",
	"cog://mem/user/project_cogos_foveal_and_peripheral",
	"cog://mem/feedback/operator-as-reconcilable-already-running",
	"cog://mem/semantic/",
	"cog://mem/semantic/agents/exec-assistant/shared/",
	"cog://mem/procedural/prompts/link-discovery-system.md",
	"cog://mem/user/project_cogos_meta_substrate_framing",
	"cog://mem/path",
	"cog://mem/semantic/research/eigen-error-catastrophe-2026-03-14",
	"cog://mem/semantic/research/transcripts/rovelli-loop-qg-relational-time.mp3",
	"cog://mem/user/project_cogos_block_mesh_architecture",
	"cog://mem/procedural/prompts/strict.md",
	"cog://mem/procedural/prompts/aggressive-linker.md",
}

// TestResolveURIMemSuffixSample resolves every sampled cog://mem link as
// written and asserts it lands on the real file.  COG_TEST_REAL_WS may point
// at a live workspace root to run the same sample against the real corpus.
func TestResolveURIMemSuffixSample(t *testing.T) {
	root := os.Getenv("COG_TEST_REAL_WS")
	if root == "" {
		root = t.TempDir()
		for _, c := range memSampleResolvable {
			p := filepath.Join(root, ".cog", "mem", c.onDisk)
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	ok := 0
	for _, c := range memSampleResolvable {
		res, err := ResolveURI(root, c.uri)
		if err != nil {
			t.Errorf("%s: %v", c.uri, err)
			continue
		}
		want := filepath.Join(root, ".cog", "mem", c.onDisk)
		if res.Path != want {
			t.Errorf("%s: got %s want %s", c.uri, res.Path, want)
			continue
		}
		if _, err := os.Stat(res.Path); err != nil {
			t.Errorf("%s: resolved path missing: %v", c.uri, err)
			continue
		}
		ok++
	}
	t.Logf("resolved as written: %d/%d", ok, len(memSampleResolvable))

	for _, u := range memSampleDead {
		res, err := ResolveURI(root, u)
		if err == nil {
			if st, serr := os.Stat(res.Path); serr == nil && !st.IsDir() {
				t.Errorf("dead link %s resolved to existing file %s", u, res.Path)
			}
		}
	}
}

// TestResolveURIMemSuffixPrecedence pins the variant rules: the as-written
// file wins; absent files keep the as-written path (writers target exactly
// what was named); bare .md maps to .cog.md; other projections are untouched.
func TestResolveURIMemSuffixPrecedence(t *testing.T) {
	root := t.TempDir()
	mem := filepath.Join(root, ".cog", "mem")
	if err := os.MkdirAll(mem, 0o755); err != nil {
		t.Fatal(err)
	}
	touch := func(n string) {
		if err := os.WriteFile(filepath.Join(mem, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	touch("a.cog.md")
	touch("b.md")
	touch("b.cog.md")
	touch("c")
	cases := []struct{ uri, want string }{
		{"cog:mem/a", "a.cog.md"},
		{"cog://mem/a", "a.cog.md"},
		{"cog://mem/a.md", "a.cog.md"},
		{"cog://mem/a.cog.md", "a.cog.md"},
		{"cog://mem/a.cog.md#sec", "a.cog.md"},
		{"cog://mem/b", "b.cog.md"},      // .cog.md preferred over .md
		{"cog://mem/b.md", "b.md"},       // as written exists -> wins
		{"cog://mem/c", "c"},             // as written exists -> wins
		{"cog://mem/new-doc", "new-doc"}, // nothing exists -> as written (write target)
	}
	for _, c := range cases {
		res, err := ResolveURI(root, c.uri)
		if err != nil {
			t.Errorf("%s: %v", c.uri, err)
			continue
		}
		if want := filepath.Join(mem, c.want); res.Path != want {
			t.Errorf("%s: got %s want %s", c.uri, res.Path, want)
		}
	}
}
