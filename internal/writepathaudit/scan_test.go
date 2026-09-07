package writepathaudit

// scan_test.go — golden-diff CI gate over the live inventory.
//
// TestInventory_MatchesGolden regenerates the inventory by scanning the
// repository fresh on every run and diffs it against the committed golden
// files. A new write path (or a removed one) changes the diff and fails the
// test with a message telling the developer to either undo the write or
// re-declare the golden with -update. This is the two-plane RFC's section
// 6.4 lint made real: the declared set is generated, not hand-authored — a
// human only ever approves a diff.
//
// To regenerate after a deliberate change:
//
//	go test ./internal/writepathaudit/ -run TestInventory_MatchesGolden -update
//
// -update writes both testdata/inventory.golden.json and
// testdata/inventory.golden.md and then passes; it is the ONLY code path in
// this package that writes anything, and it only ever writes inside
// testdata/.

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "regenerate the golden write-path inventory")

// repoRoot locates the repository root from this test file's own location,
// the same technique internal/engine/namespace_sync_test.go uses: walk up
// until a go.work file is found. go.work (not go.mod) is the marker here
// because Scan needs to walk every module in the workspace, not just the
// root module.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("cannot locate repo root (go.work) walking up from %s", file)
	return ""
}

func goldenPaths(t *testing.T) (jsonPath, mdPath string) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Join(filepath.Dir(file), "testdata")
	return filepath.Join(dir, "inventory.golden.json"), filepath.Join(dir, "inventory.golden.md")
}

func marshalReport(t *testing.T, r *Report) []byte {
	t.Helper()
	out, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	return append(out, '\n')
}

// diffAgainstGolden serializes report exactly the way the golden files are
// serialized (marshalReport for JSON, RenderMarkdown for markdown) and
// reports whether each differs from the committed golden. This is the ONE
// comparison the golden-diff gate runs — TestInventory_MatchesGolden (the
// drift check that runs on every CI build) and TestGoldenGate_
// DetectsInjectedCogWrite (the mutation test proving the gate fails
// closed) both call this same function rather than each maintaining its
// own copy of the comparison. That sharing is the point: a mutation test
// built against an independently-reimplemented comparison can prove
// nothing about whether the REAL gate's comparison fails closed — it
// would keep passing even if TestInventory_MatchesGolden's own comparison
// were silently neutered, since the two never share code. Routing both
// through diffAgainstGolden is what makes the mutation test's proof
// actually apply to the gate it claims to guard: no neuter of this shared
// comparison leaves BOTH tests green. That is weaker than "any bug here
// breaks both identically" — a blind neuter that always returns (false,
// false), say, still fails TestGoldenGate_DetectsInjectedCogWrite (its own
// injected write no longer shows as a diff) while TestInventory_
// MatchesGolden keeps passing (no drift ever gets reported either) — but
// it is the property this test actually needs: at least one of the two
// always catches a broken comparison.
func diffAgainstGolden(t *testing.T, report *Report) (jsonDiffers, mdDiffers bool) {
	t.Helper()
	jsonPath, mdPath := goldenPaths(t)
	wantJSON, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read golden json (run TestInventory_MatchesGolden -update first): %v", err)
	}
	wantMD, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("read golden md (run TestInventory_MatchesGolden -update first): %v", err)
	}
	gotJSON := marshalReport(t, report)
	gotMD := RenderMarkdown(report)
	return string(gotJSON) != string(wantJSON), gotMD != string(wantMD)
}

func TestInventory_MatchesGolden(t *testing.T) {
	root := repoRoot(t)
	report, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan(%s): %v", root, err)
	}
	if report.Summary.Total == 0 {
		t.Fatalf("scan found zero write sites — this almost certainly means the scan is broken, not that the repo stopped writing to disk")
	}

	jsonPath, mdPath := goldenPaths(t)

	if *update {
		gotJSON := marshalReport(t, report)
		gotMD := RenderMarkdown(report)
		if err := os.WriteFile(jsonPath, gotJSON, 0o644); err != nil {
			t.Fatalf("write golden json: %v", err)
		}
		if err := os.WriteFile(mdPath, []byte(gotMD), 0o644); err != nil {
			t.Fatalf("write golden md: %v", err)
		}
		t.Logf("golden inventory regenerated: %d sites (cog=%d home=%d elsewhere=%d unanchored=%d dynamic=%d)",
			report.Summary.Total, report.Summary.Cog, report.Summary.Home, report.Summary.Elsewhere, report.Summary.Unanchored, report.Summary.Dynamic)
		return
	}

	jsonDiffers, mdDiffers := diffAgainstGolden(t, report)
	if jsonDiffers {
		t.Errorf("write-path inventory (JSON) has drifted from testdata/inventory.golden.json.\n" +
			"A write path was added, removed, or changed. Either:\n" +
			"  1. undo the write-path change, or\n" +
			"  2. re-declare the golden: go test ./internal/writepathaudit/ -run TestInventory_MatchesGolden -update\n" +
			"then review the diff to testdata/inventory.golden.json before committing it.")
	}
	if mdDiffers {
		t.Errorf("write-path inventory (markdown) has drifted from testdata/inventory.golden.md — regenerate with -update (see JSON error above for the procedure)")
	}
}

// TestGoldenGate_DetectsInjectedCogWrite is the negative-path mutation
// check the PR description claims exists but that no test in this package
// actually implemented: proof that the golden-diff gate itself fails
// closed. TestInventory_MatchesGolden only ever exercises the drift
// comparison against the CURRENT, unmodified repo — it can pass forever
// even if the comparison it performs were silently neutered (an always-
// take-the--update-branch bug, or Scan returning stale/cached data), and
// nothing else in the suite would notice.
//
// This test takes a full report that is DIFFERENT from the committed
// golden by exactly one injected write site under .cog/, serializes it
// exactly the way TestInventory_MatchesGolden serializes a real scan, and
// asserts that the resulting JSON and markdown are NOT byte-identical to
// the committed golden — i.e. that the comparison the gate depends on
// actually notices an unreviewed .cog/ writer. It never calls -update and
// never writes to testdata/; the injected site lives only in an in-memory
// copy of the report.
func TestGoldenGate_DetectsInjectedCogWrite(t *testing.T) {
	root := repoRoot(t)
	report, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan(%s): %v", root, err)
	}

	// Precondition: the UNMODIFIED live scan must already match the
	// committed golden. Otherwise this test would "pass" for the wrong
	// reason — ANY report at all would differ from an already-stale
	// golden, telling us nothing about whether the gate detects a real,
	// isolated mutation. TestInventory_MatchesGolden enforces this same
	// invariant (through this same diffAgainstGolden call); asserting it
	// again here keeps this test meaningful on its own even if run in
	// isolation.
	if jsonDiffers, mdDiffers := diffAgainstGolden(t, report); jsonDiffers || mdDiffers {
		t.Fatalf("precondition failed: the live scan does not match the committed golden BEFORE injecting anything — golden is stale, run -update first")
	}

	// Inject a synthetic .cog/ write site the real repo does not have —
	// the exact defect class this gate exists to catch: a new writer
	// landing under .cog/ without ever going through review.
	mutated := *report
	mutated.Sites = append(append([]Site{}, report.Sites...), Site{
		File:      "internal/testkernel/injected_mutation_probe.go",
		Line:      1,
		Column:    1,
		Primitive: "os.WriteFile",
		Func:      "injectedMutationProbe",
		Pattern:   "<WorkspaceRoot>/.cog/injected-mutation-probe/site.json",
		Category:  "cog",
		Resolved:  true,
		Subsystem: "internal:testkernel",
		Raw:       "path",
	})
	mutated.Summary.Total++
	mutated.Summary.Cog++
	byPrimitive := make(map[string]int, len(report.Summary.ByPrimitive)+1)
	for k, v := range report.Summary.ByPrimitive {
		byPrimitive[k] = v
	}
	byPrimitive["os.WriteFile"]++
	mutated.Summary.ByPrimitive = byPrimitive

	jsonDiffers, mdDiffers := diffAgainstGolden(t, &mutated)
	if !jsonDiffers {
		t.Error("golden gate did not notice an injected .cog/ write site — mutated JSON is byte-identical to the committed golden")
	}
	if !mdDiffers {
		t.Error("golden gate did not notice an injected .cog/ write site — mutated markdown is byte-identical to the committed golden")
	}
}

// ─── Round-3 adversarial gate: named regression fixtures ───────────────────
//
// TestRound3GateFindings_Fixed pins, by name, every writer the round-3
// adversarial gate found either ABSENT ENTIRELY or AFFIRMATIVELY
// MISCLASSIFIED (stamped "elsewhere" for a genuinely unknown destination).
// Each case quotes the gate's own finding and asserts the ACTUAL outcome
// after the honest-binning + field/param-origin-chase fix:
//
//   - fully resolved to "cog" — the callable-origin index closed the hop
//     (a struct field or parameter traced back to a literal), or
//   - "unanchored" with its real file:line — the chase hit a genuine,
//     declared dead end (a cross-package call this tool intentionally does
//     not follow, or call sites that disagree and are correctly not
//     merged). This is the honesty margin working as designed, NOT a
//     remaining defect: the gate's own acceptance predicate accepts either
//     outcome ("appears... under a .cog/ path... or, where genuinely
//     dynamic, in the honesty margin with its file:line").
//
// A site never being "elsewhere" for a destination this tool could not
// positively confirm is the one invariant every case here checks first.
func TestRound3GateFindings_Fixed(t *testing.T) {
	root := repoRoot(t)
	report, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan(%s): %v", root, err)
	}

	find := func(file string, line int) (Site, bool) {
		for _, s := range report.Sites {
			if s.File == file && s.Line == line {
				return s, true
			}
		}
		return Site{}, false
	}
	// findAny reports the first site matching file + a pattern substring,
	// for cases where chasing a wrapper resolves to a DIFFERENT call's
	// line than the one the hand-written verdict named (e.g. a Primitive
	// added by this fix, or a passthrough resolved at its real primitive
	// rather than the wrapper's own call expression).
	findAny := func(file, patternContains string) (Site, bool) {
		for _, s := range report.Sites {
			if s.File == file && strings.Contains(s.Pattern, patternContains) {
				return s, true
			}
		}
		return Site{}, false
	}

	cases := []struct {
		name  string
		gate  string // the gate's own words, for traceability
		check func(t *testing.T)
	}{
		{
			name: "blobstore.go bs.root — chased through NewBlobStore's constructor to <WorkspaceRoot>/.cog/blobs",
			gate: `internal/engine/blobstore.go:66,72,273,290 — ".cog/blobs/**" binned 'elsewhere' as {bs.root}`,
			check: func(t *testing.T) {
				s, ok := find("internal/engine/blobstore.go", 66)
				if !ok {
					t.Fatal("blobstore.go:66 (os.MkdirAll ensureDirs) not found in inventory")
				}
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/blobs") {
					t.Errorf("blobstore.go:66 = category %q pattern %q, want cog / .cog/blobs", s.Category, s.Pattern)
				}
				if got, _ := find("internal/engine/blobstore.go", 290); got.Category != "cog" {
					t.Errorf("blobstore.go:290 (manifest.jsonl) = category %q, want cog", got.Category)
				}
			},
		},
		{
			name: "bep_provider.go p.watchDir — chased through NewBEPProvider's constructor AND its boot.go call site to <WorkspaceRoot>/.cog/bin/agents/definitions",
			gate: `internal/engine/bep_provider.go:111,456 — ".cog/bin/agents/definitions/" (BEP-replicated, peer-writable) binned 'elsewhere' as {p.watchDir}`,
			check: func(t *testing.T) {
				s, ok := find("internal/engine/bep_provider.go", 111)
				if !ok {
					t.Fatal("bep_provider.go:111 not found in inventory")
				}
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/bin/agents/definitions") {
					t.Errorf("bep_provider.go:111 = category %q pattern %q, want cog / .cog/bin/agents/definitions", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "serve_attention.go l.path — resolved directly (workspaceRoot is anchor-named in the SAME constructor, no chase needed) to <WorkspaceRoot>/.cog/run/attention.jsonl",
			gate: `internal/engine/serve_attention.go:53 — the ".cog/run/attention.jsonl" append itself binned 'elsewhere' as {l.path}`,
			check: func(t *testing.T) {
				s, ok := find("internal/engine/serve_attention.go", 53)
				if !ok {
					t.Fatal("serve_attention.go:53 not found in inventory")
				}
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/run/attention.jsonl") {
					t.Errorf("serve_attention.go:53 = category %q pattern %q, want cog / .cog/run/attention.jsonl", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "rotating_writer.go w.path — pure passthrough field, chased through log_capture.go's call site to a genuinely branch-dependent path (round-7 re-pin: unanchored, never elsewhere)",
			gate: `internal/engine/log_capture.go:48 — ".cog/run/kernel.log.jsonl"; real writes at internal/engine/rotating_writer.go:47/50/89/137/141 binned 'elsewhere' as {path}/{w.path}`,
			check: func(t *testing.T) {
				// ROUND 7 re-pin. The chase still reaches log_capture.go's
				// `path := cfg.KernelLogPath; if path == "" { path =
				// DefaultKernelLogPath(cfg.WorkspaceRoot) }` — but it no
				// longer takes the `if` branch's value as the site's answer.
				// The two branches disagree in category (an operator-supplied
				// cfg.KernelLogPath has no chaseable origin and can name any
				// file on disk → unanchored; the fallback is
				// <WorkspaceRoot>/.cog/run/kernel.log.jsonl → cog), and a
				// disagreement is exactly what the category-agreement rule
				// refuses to resolve by picking one side (see
				// resolveCondRebindPair). The gate's own finding — these rows
				// stamped "elsewhere" — stays closed: unanchored is the
				// honesty margin, not a claim about being outside .cog/.
				for _, line := range []int{50, 89, 141} {
					s, ok := find("internal/engine/rotating_writer.go", line)
					if !ok {
						t.Fatalf("rotating_writer.go:%d not found in inventory", line)
					}
					if s.Category == "elsewhere" || s.Category == "home" {
						t.Errorf("rotating_writer.go:%d = category %q pattern %q — the gate's own finding: a confident non-.cog category taken from one branch is never acceptable here", line, s.Category, s.Pattern)
					}
					// The disagreement travels out-of-band (degenerateRoot),
					// never as marker text: what the pattern shows is the
					// variable's ORDINARY opaque placeholder, exactly as any
					// unresolvable name renders. Asserting on the placeholder
					// rather than on a reserved string is the point — there is
					// no reserved string any more.
					if s.Category != "unanchored" || !strings.Contains(s.Pattern, "{path}") {
						t.Errorf("rotating_writer.go:%d = category %q pattern %q, want unanchored / {path} (cfg.KernelLogPath vs the DefaultKernelLogPath fallback disagree in category)", line, s.Category, s.Pattern)
					}
				}
				// log_capture.go itself has no os/io primitive of its own
				// (it only computes the path and hands it to
				// newRotatingWriter) — that is correctly zero rows, not a
				// gap, now that the REAL writer resolves fully.
			},
		},
		{
			name: "proprioceptive.go logPath — chased through NewProprioceptiveLogger's call sites to <WorkspaceRoot>/.cog/run/proprioceptive.jsonl",
			gate: `internal/engine/proprioceptive.go:54,72 — ".cog/run/proprioceptive.jsonl" binned 'elsewhere'; '.cog' never appears in any row`,
			check: func(t *testing.T) {
				s, ok := find("internal/engine/proprioceptive.go", 72)
				if !ok {
					t.Fatal("proprioceptive.go:72 not found in inventory")
				}
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/run/proprioceptive.jsonl") {
					t.Errorf("proprioceptive.go:72 = category %q pattern %q, want cog / .cog/run/proprioceptive.jsonl", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "mcp_stubs.go / sdk/constellation/db.go — sql.Open(sqlite3) now a recognized primitive; the two direct-anchor call sites resolve fully to .cog/.state/constellation.db",
			gate: `.cog/.state/constellation.db — mcp_stubs.go opens it via sql.Open("sqlite3", ...); sdk/constellation/store_guard.go:201 likewise. sqlite writes through CGO, invisible to an os-primitive scanner`,
			check: func(t *testing.T) {
				s, ok := find("sdk/constellation/db.go", 42)
				if !ok {
					t.Fatal("sdk/constellation/db.go:42 (the real read-write sql.Open) not found in inventory")
				}
				if s.Primitive != "sql.Open(sqlite3)" || s.Category != "cog" || !strings.Contains(s.Pattern, "constellation.db") {
					t.Errorf("db.go:42 = primitive %q category %q pattern %q, want sql.Open(sqlite3) / cog / constellation.db", s.Primitive, s.Category, s.Pattern)
				}
				if s, ok := find("internal/engine/mcp_stubs.go", 108); !ok || s.Category != "cog" {
					t.Errorf("mcp_stubs.go:108 (workspaceRoot-anchored sql.Open) = %+v, want category cog", s)
				}
				// store_guard.go:201 and mcp_stubs.go:210 pass an
				// already-computed dbPath THROUGH one or more further
				// hops this tool cannot positively confirm agree with
				// each other (one chain runs through a cross-package
				// call it intentionally does not follow) — correctly
				// UNANCHORED, never "elsewhere", never merged with the
				// resolved sites above by guessing.
				if s, ok := find("sdk/constellation/store_guard.go", 201); !ok || s.Category == "elsewhere" {
					t.Errorf("store_guard.go:201 = %+v, must never be 'elsewhere' for an unconfirmed sqlite DSN", s)
				}
			},
		},
		{
			name: "serve_blocks.go os.CreateTemp — the PUT /v1/blocks/{hash} ingress is no longer absent from the inventory",
			gate: `internal/engine/serve_blocks.go — the PUT /v1/blocks/{hash} machine-plane ingress. Its only write primitive is os.CreateTemp at :184, which is not in the tool's recognized set. File is wholly absent`,
			check: func(t *testing.T) {
				s, ok := find("internal/engine/serve_blocks.go", 184)
				if !ok {
					t.Fatal("serve_blocks.go:184 (os.CreateTemp) still absent from the inventory")
				}
				if s.Primitive != "os.CreateTemp" {
					t.Errorf("serve_blocks.go:184 primitive = %q, want os.CreateTemp", s.Primitive)
				}
				// It streams into os.TempDir() (dir arg is ""), NOT under
				// .cog/ — "elsewhere" is the honest, positively-resolved
				// answer here (root "" is a concrete, known-non-opaque
				// root), not an over-claim.
				if s.Category != "elsewhere" {
					t.Errorf("serve_blocks.go:184 category = %q, want elsewhere (root is the literal \"\" = os.TempDir)", s.Category)
				}
				if s, ok := find("internal/engine/serve_blocks.go", 195); !ok || s.Primitive != "io.Copy" {
					t.Errorf("serve_blocks.go:195 (io.Copy into the CreateTemp'd file) = %+v, want io.Copy present", s)
				}
			},
		},
		{
			name: "internal/conversations — .cog/state/conversations is no longer absent",
			gate: `internal/conversations/provider.go — ".cog/state/conversations/"; no os primitive, file absent`,
			check: func(t *testing.T) {
				// The real primitive lives in index.go, not provider.go
				// (provider.go itself has none, same "wrapper vs
				// primitive" shape as everywhere else in this tool) — but
				// the bucket itself is no longer invisible.
				s, ok := findAny("internal/conversations/index.go", ".cog/state/conversations")
				if !ok {
					t.Fatal(".cog/state/conversations not found anywhere in internal/conversations/index.go")
				}
				if s.Category != "cog" {
					t.Errorf("conversations index.go %s:%d = category %q, want cog", s.File, s.Line, s.Category)
				}
			},
		},
		{
			name: "boot_node_root_grant.go vault path — fully resolved to cog now that callChase excludes nodeRootVaultPath's own error-guard branch, to <Home>/.cog/vault/node-root-grant",
			gate: `internal/engine/boot_node_root_grant.go:275,279,282 — "~/.cog/vault/node-root-grant" (0600 raw token) binned 'elsewhere' as {path}`,
			check: func(t *testing.T) {
				// Round-3 found this dead-ending to UNANCHORED, because
				// nodeRootVaultPath's own `return "", err` guard branch was
				// (wrongly) treated as a second, disagreeing candidate
				// against its real `return filepath.Join(home, ".cog",
				// "vault", ...), nil` branch. With that guard branch
				// correctly excluded (callChase's isErrorGuardReturn), the
				// function has exactly ONE real candidate and resolves in
				// full — to <Home>/.cog/vault/node-root-grant, a genuine
				// .cog/ path (home-rooted rather than workspace-rooted,
				// but .cog/ all the same per hasCogSegment's definition).
				s, ok := find("internal/engine/boot_node_root_grant.go", 292)
				if !ok {
					t.Fatal("boot_node_root_grant.go:292 not found in inventory")
				}
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/vault/node-root-grant") {
					t.Errorf("boot_node_root_grant.go:292 = category %q pattern %q, want cog / .cog/vault/node-root-grant", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "pkg/substrate/bep/tls.go CertDir — dead-ends to UNANCHORED (GenerateBEPCert is only ever called through a cross-package qualified selector, a declared blind spot), never 'elsewhere'",
			gate: `pkg/substrate/bep/tls.go:61,124,143 ... "~/.cog/etc" CertDir / node identity, binned 'elsewhere'`,
			check: func(t *testing.T) {
				s, ok := find("pkg/substrate/bep/tls.go", 61)
				if !ok {
					t.Fatal("pkg/substrate/bep/tls.go:61 not found in inventory")
				}
				if s.Category == "elsewhere" {
					t.Errorf("tls.go:61 = elsewhere, must never over-claim a non-.cog destination for the BEP cert dir")
				}
				if s.Category != "unanchored" {
					t.Errorf("tls.go:61 = category %q, want unanchored (cross-package call site, not chased by design)", s.Category)
				}
			},
		},
		{
			name: "node_identity.go persistNodeID — dead-ends to UNANCHORED (call-site chase does not reach a positively-known root), never 'elsewhere'",
			gate: `internal/engine/node_identity.go:321,326,335 — node identity, binned 'elsewhere'`,
			check: func(t *testing.T) {
				s, ok := find("internal/engine/node_identity.go", 321)
				if !ok {
					t.Fatal("node_identity.go:321 not found in inventory")
				}
				if s.Category == "elsewhere" {
					t.Errorf("node_identity.go:321 = elsewhere, must never over-claim a non-.cog destination for the machine node-id dir")
				}
			},
		},
		{
			name: "pkg/substrate/bep/index.go PersistIndex — dead-ends to UNANCHORED (PersistIndex is only ever called cross-package as bep.PersistIndex(...), a declared blind spot), never 'elsewhere'",
			gate: `internal/engine/bep_engine.go:200 — ".cog/.state/bep"; real write at pkg/substrate/bep/index.go:335/352/355 binned 'elsewhere' as {stateDir}`,
			check: func(t *testing.T) {
				s, ok := find("pkg/substrate/bep/index.go", 335)
				if !ok {
					t.Fatal("pkg/substrate/bep/index.go:335 not found in inventory")
				}
				if s.Category == "elsewhere" {
					t.Errorf("index.go:335 = elsewhere, must never over-claim a non-.cog destination for the BEP sync-state dir")
				}
				if s.Category != "unanchored" {
					t.Errorf("index.go:335 = category %q, want unanchored (PersistIndex is called only via the cross-package selector bep.PersistIndex(...))", s.Category)
				}
			},
		},
		{
			name: "projection_compiler.go writeCompilerState — dead-ends to UNANCHORED (cfg.StatePath is a config value with no traced origin), never 'elsewhere'",
			gate: `internal/engine/projection_compiler.go — ".cog/state/projection-compiler.json" binned 'elsewhere'`,
			check: func(t *testing.T) {
				s, ok := find("internal/engine/projection_compiler.go", 181)
				if !ok {
					t.Fatal("projection_compiler.go:181 not found in inventory")
				}
				if s.Category == "elsewhere" {
					t.Errorf("projection_compiler.go:181 = elsewhere, must never over-claim a non-.cog destination for the projection-compiler state file")
				}
			},
		},
		{
			name: "identity-grants / worktree-reconciler / mcp-client ledger buckets — now individually emitted as bucket-specific rows, in addition to the shared AppendEvent primitive site",
			gate: `serve_identity_grants.go has no os primitive; the string "identity-grants" appears nowhere in the inventory ... .cog/ledger/worktree-reconciler/ (worktree_spawn.go, FilesystemLedgerWriter) and .cog/ledger/mcp-client/ (mcp_stubs.go:641). Neither bucket name is in the inventory`,
			check: func(t *testing.T) {
				// This fixture's premise changed (see appendEventBucketSites
				// in audit.go, added for the round-2 gate's predicate about
				// the ledger-bucket literal names): the tool now chases the
				// bucket argument at every AppendEvent-reaching call site —
				// a bare call, or (serve_identity_grants.go's case) a call
				// through the "appendEvent: AppendEvent" struct-field seam
				// — and emits one Primitive="engine.AppendEvent(bucket)"
				// row per call site whose bucket resolves to a positive
				// literal. Each of the three named buckets must now appear
				// as its own row.
				wantBucket := func(file string, line int, bucket string) {
					t.Helper()
					s, ok := find(file, line)
					if !ok {
						t.Errorf("%s:%d not found in inventory (want the %q ledger bucket row)", file, line, bucket)
						return
					}
					if s.Primitive != "engine.AppendEvent(bucket)" || s.Category != "cog" {
						t.Errorf("%s:%d = primitive %q category %q, want engine.AppendEvent(bucket) / cog", file, line, s.Primitive, s.Category)
					}
					want := ".cog/ledger/" + bucket + "/events.jsonl"
					if !strings.Contains(s.Pattern, want) {
						t.Errorf("%s:%d pattern %q does not contain %q", file, line, s.Pattern, want)
					}
				}
				wantBucket("internal/engine/serve_identity_grants.go", 725, "identity-grants")
				wantBucket("internal/engine/serve_identity_grants.go", 782, "identity-grants")
				wantBucket("internal/engine/serve_identity_grants.go", 825, "identity-grants")
				wantBucket("internal/engine/worktree_spawn.go", 273, "worktree-reconciler")
				wantBucket("internal/engine/mcp_stubs.go", 643, "mcp-client")

				// The shared AppendEvent primitive site itself (ledger.go's
				// one real os.OpenFile call) is unaffected by this — it
				// still reports its own generic, sanitize-hole row.
				ledger, ok := find("internal/engine/ledger.go", 192)
				if !ok || ledger.Category != "cog" || !strings.Contains(ledger.Pattern, ".cog/ledger/") {
					t.Fatalf("internal/engine/ledger.go:192 (the shared AppendEvent primitive all buckets funnel through) = %+v, want a cog-classified .cog/ledger/ site", ledger)
				}
			},
		},
		{
			name: "internal/coherence/coherence.go:190 is a READ, not a write — correction to the hand-discovered validation set, not a tool defect",
			gate: `the verdicts cite internal/coherence/coherence.go:190 as writing ".cog/run/coherence/canonical-hash". Source shows :190 is inside gitCanonicalHash and is an os.ReadFile. No non-test writer of that file exists in this tree`,
			check: func(t *testing.T) {
				if _, ok := find("internal/coherence/coherence.go", 190); ok {
					t.Errorf("coherence.go:190 appears as a write site — the gate itself confirmed this line is os.ReadFile, a read; it must NEVER be listed as a writer fixture (this is the one correction TO the validation set, not a finding about the tool)")
				}
				// The file's one real write primitive (an unrelated,
				// immediately-removed git-index scratch file used to
				// compute a tree hash without touching the real index —
				// nothing to do with canonical-hash) is still correctly
				// present, proving the file itself IS scanned.
				if _, ok := find("internal/coherence/coherence.go", 156); !ok {
					t.Errorf("coherence.go:156 (the unrelated os.CreateTemp git-index scratch file) missing — coherence.go may not be getting scanned at all")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.check)
	}
}

// ─── Round-4 adversarial gate: the nine false-elsewhere rows ───────────────
//
// TestRound4GateFindings_NeverElsewhere pins, by name, every one of the
// round-4 gate's nine AFFIRMATIVELY FALSE "elsewhere" rows — real .cog/
// writers that a prior repair had stamped resolved:true, category:
// "elsewhere" — plus the separate manifest.go mis-bin (a genuine non-.cog
// writer that landed in the wrong HONEST bin). Each case quotes the gate's
// own finding and asserts the row now lands in "cog" or "unanchored" (per
// the gate's own acceptance bar: "each must land cog or unanchored"),
// NEVER "elsewhere" again. The three root causes these rows exercise:
//
//   - callChase dropping/collapsing a callee's return branches instead of
//     excluding only its own error-guard branch (containedJoin,
//     resolveMemoryDocPath, RunDir, and — one hop further —
//     memoryProjector.setMemory via p.kernel.MemoryDir());
//   - isOpaqueRoot treating a WorkspaceRoot-rooted pattern with an opaque
//     TAIL segment as positively resolved (init.go's two rows);
//   - collectLocalDefs folding a compound assignment as a rebind, and two
//     sibling for-loops' same-named loop-local variable flattening into
//     one shared definition (sdk/cogos.go, init.go).
func TestRound4GateFindings_NeverElsewhere(t *testing.T) {
	root := repoRoot(t)
	report, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan(%s): %v", root, err)
	}
	find := func(file string, line int) (Site, bool) {
		for _, s := range report.Sites {
			if s.File == file && s.Line == line {
				return s, true
			}
		}
		return Site{}, false
	}

	// wantNeverElsewhere is the single invariant every case here checks:
	// under-claiming (cog or unanchored) is always acceptable, but
	// "elsewhere" for a destination this tool has not positively confirmed
	// is outside .cog/ is the one failure mode round 2 through round 4 all
	// converged on as the worst.
	wantNeverElsewhere := func(t *testing.T, file string, line int) Site {
		t.Helper()
		s, ok := find(file, line)
		if !ok {
			t.Fatalf("%s:%d not found in inventory", file, line)
		}
		if s.Category == "elsewhere" {
			t.Errorf("%s:%d = elsewhere (pattern %q) — must never over-claim a non-.cog destination", file, line, s.Pattern)
		}
		if s.Category != "cog" && s.Category != "unanchored" {
			t.Errorf("%s:%d = category %q, want cog or unanchored", file, line, s.Category)
		}
		return s
	}

	cases := []struct {
		name  string
		gate  string
		check func(t *testing.T)
	}{
		{
			name: "daemon_detach_unix.go:43 (detachProcess's os.OpenFile) — REGRESSION GUARD: logPath has a TempDir outer def AND a workspace/.cog rebind inside a range over args; a use site outside the loop cannot know which fired, so any resolved category is over-claiming (round-3 gate's sole predicate-2 failure; fixed by poisonLoopRebinds)",
			gate: `internal/engine/daemon_detach_unix.go:43 (os.OpenFile) ... stamped resolved:true, category:"elsewhere", pattern "<TempDir>/cogos-daemon.log" ... the :35 reassignment sits inside a for i, a := range args body ... "elsewhere" with resolved:true is the one answer that is affirmatively wrong.`,
			check: func(t *testing.T) {
				s := wantNeverElsewhere(t, "internal/engine/daemon_detach_unix.go", 43)
				if s.Category != "unanchored" && s.Category != "cog" {
					t.Errorf("daemon_detach_unix.go:43 = category %q, want unanchored (branch disagreement) or cog (workspace branch)", s.Category)
				}
			},
		},
		{
			name: "mcp_server.go:1600 (WriteCogDoc's os.MkdirAll) — REGRESSION GUARD: this exact site was the round-4 gate's headline finding (stamped elsewhere under a MORE specific, MORE false heading) and must never relapse",
			gate: `internal/engine/mcp_server.go:1600 (os.MkdirAll, pattern "dirname()") ... Both stamped resolved:true, category:"elsewhere". REGRESSION: the prior gate had these in the DYNAMIC honesty margin.`,
			check: func(t *testing.T) {
				s := wantNeverElsewhere(t, "internal/engine/mcp_server.go", 1600)
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/mem") {
					t.Errorf("mcp_server.go:1600 = category %q pattern %q, want cog / .cog/mem (containedJoin's filepath.Clean(full) branch, not its own \"\", err guard)", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "mcp_server.go:1652 (WriteCogDoc's os.WriteFile) — REGRESSION GUARD, same call chain as :1600",
			gate: `internal/engine/mcp_server.go:1652 (os.WriteFile, pattern "") ... Both stamped resolved:true, category:"elsewhere".`,
			check: func(t *testing.T) {
				s := wantNeverElsewhere(t, "internal/engine/mcp_server.go", 1652)
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/mem") {
					t.Errorf("mcp_server.go:1652 = category %q pattern %q, want cog / .cog/mem", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "memory_sections.go:353 (atomicWriteMemoryFile's os.CreateTemp) — resolveMemoryDocPath's filepath.Clean(candidate) branch, not its own escape/absolute-path guards",
			gate: `internal/engine/memory_sections.go:353 (os.CreateTemp, "dirname()") ... Stamped "elsewhere".`,
			check: func(t *testing.T) {
				// ROUND 7 re-pin. This row read "cog" from round 4 until the
				// category-agreement rule landed (see
				// poisonConditionalAndLoopRebinds/resolveCondRebindPair): it
				// was reading resolveMemoryDocPath's LAST `candidate = ...`
				// branch alone, under last-assignment-wins. That function
				// assigns `candidate` in four mutually exclusive branches, and
				// they do NOT all land under .cog/mem — the default case's
				// first branch is `wsPath := filepath.Join(cogRoot, path)`,
				// workspace-relative on purpose (its own comment names
				// `.cog/ontology/crystal.cog.md` as the case it exists for),
				// which classifies unanchored while the other three are
				// .cog/mem-rooted. A genuine branch disagreement, so the
				// honest bin is the unanchored margin. The gate's own
				// complaint — a false "elsewhere" — stays closed either way,
				// which is what wantNeverElsewhere asserts.
				s := wantNeverElsewhere(t, "internal/engine/memory_sections.go", 353)
				if s.Category != "unanchored" || !strings.Contains(s.Pattern, "{candidate}") {
					t.Errorf("memory_sections.go:353 = category %q pattern %q, want unanchored / {candidate} (resolveMemoryDocPath's workspace-relative branch disagrees in category with its .cog/mem branches; the disagreement is carried out-of-band, so the pattern shows the variable's ordinary opaque placeholder)", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "memory_sections.go:375 (atomicWriteMemoryFile's os.Rename)",
			gate: `internal/engine/memory_sections.go:375 (os.Rename, "") ... Stamped "elsewhere".`,
			check: func(t *testing.T) {
				// Same re-pin as :353 above, same call chain.
				s := wantNeverElsewhere(t, "internal/engine/memory_sections.go", 375)
				if s.Category != "unanchored" || !strings.Contains(s.Pattern, "{candidate}") {
					t.Errorf("memory_sections.go:375 = category %q pattern %q, want unanchored / {candidate}", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "sdk/cogos.go:414 (setMemory's os.MkdirAll) — memPath's compound assignment (+=) no longer destroys the MemoryDir() join, and p.kernel.MemoryDir() now chases through methodChase",
			gate: `sdk/cogos.go:414 (os.MkdirAll, "dirname(.md)"), :469 ..., :478 ... — memoryProjector.setMemory / writeAtomicFile write under p.kernel.MemoryDir() = <root>/.cog/mem. The memPath += ".md" compound assignment destroyed the join. All three stamped "elsewhere".`,
			check: func(t *testing.T) {
				s := wantNeverElsewhere(t, "sdk/cogos.go", 414)
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/mem") {
					t.Errorf("sdk/cogos.go:414 = category %q pattern %q, want cog / .cog/mem", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "sdk/cogos.go:469 (deleteMemory's os.Stat path — actually a read; the write is :478's os.Rename, checked separately, but :469 must still never be elsewhere if present)",
			gate: `sdk/cogos.go:469 (os.MkdirAll, "dirname(.md)")`,
			check: func(t *testing.T) {
				wantNeverElsewhere(t, "sdk/cogos.go", 469)
			},
		},
		{
			name: "sdk/cogos.go:478 (writeAtomicFile's os.Rename)",
			gate: `sdk/cogos.go:478 (os.Rename, ".md") ... All three stamped "elsewhere".`,
			check: func(t *testing.T) {
				s := wantNeverElsewhere(t, "sdk/cogos.go", 478)
				if s.Category != "cog" || !strings.Contains(s.Pattern, ".cog/mem") {
					t.Errorf("sdk/cogos.go:478 = category %q pattern %q, want cog / .cog/mem", s.Category, s.Pattern)
				}
			},
		},
		{
			name: "init.go:68 (cogos init's directory-scaffolding os.MkdirAll) — the dir-over-initDirs loop's OWN loop variable, not the SECOND loop's f.target (round-4's mis-attribution finding), and a WorkspaceRoot-rooted opaque tail is no longer treated as positively resolved",
			gate: `internal/engine/init.go:68 (os.MkdirAll) ... pattern "<WorkspaceRoot>/{f.target}" ... Also mis-attributed: :68's loop var is dir over initDirs, but the flat local-def map gave it the later target := filepath.Join(workspaceRoot, f.target) assignment.`,
			check: func(t *testing.T) {
				s := wantNeverElsewhere(t, "internal/engine/init.go", 68)
				if s.Category != "unanchored" {
					t.Errorf("init.go:68 = category %q, want unanchored (the tail segment is a range-loop variable this tool has no data-flow into, not a positively-known non-.cog destination)", s.Category)
				}
				if strings.Contains(s.Pattern, "f.target") {
					t.Errorf("init.go:68 pattern %q still shows the SECOND loop's f.target — the loop-scoping fix did not take", s.Pattern)
				}
			},
		},
		{
			name: "init.go:91 (cogos init's config-file os.WriteFile) — same WorkspaceRoot+opaque-tail rule as :68, from the SECOND loop's own f.target this time",
			gate: `internal/engine/init.go:91 (os.WriteFile), pattern "<WorkspaceRoot>/{f.target}" ... Every element of initFiles ... is under .cog/. Stamped "elsewhere".`,
			check: func(t *testing.T) {
				s := wantNeverElsewhere(t, "internal/engine/init.go", 91)
				if s.Category != "unanchored" {
					t.Errorf("init.go:91 = category %q, want unanchored", s.Category)
				}
			},
		},
		{
			name: "manifest.go:103 (experiment.RunDir's os.OpenFile) — not a .cog writer, but the elsewhere verdict rested on RunDir's own \"\", err guard branch; the honest bin is home",
			gate: `internal/testkernel/experiment/manifest.go:103 (os.OpenFile, "/observations.jsonl") — not a .cog writer, but the "elsewhere" verdict rests on bogus evidence (RunDir's return "", err guard). Its real root is $COGOS_WORKSPACE_ROOT or <Home>/workspaces/first-instruments-runs, so the honest bin is "home", not "elsewhere".`,
			check: func(t *testing.T) {
				s, ok := find("internal/testkernel/experiment/manifest.go", 103)
				if !ok {
					t.Fatal("manifest.go:103 not found in inventory")
				}
				if s.Category == "elsewhere" {
					t.Errorf("manifest.go:103 = elsewhere — must never over-claim; RunDir's error-guard branch is not a real, disagreeing candidate")
				}
				if s.Category != "home" && s.Category != "unanchored" {
					t.Errorf("manifest.go:103 = category %q, want home (or, failing full resolution, unanchored) — never elsewhere", s.Category)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, tc.check)
	}
}

// TestManifestGoRunsRootEnvBranch_NoLongerOverclaimsHome is the round-5
// gate's own regression: internal/testkernel/experiment/manifest.go's
// RunsRoot conditionally rebinds `root` (`root := os.Getenv(...); if root ==
// "" { home, _ := os.UserHomeDir(); root = filepath.Join(home,
// "workspaces") }`) and both of its call sites (manifest.go:41 and :103)
// used to resolve on the env-var-UNSET branch's join alone, stamping a
// confident "home" for a root that is genuinely branch-dependent — an
// unresolvable os.Getenv() on one path, a resolvable <Home>/workspaces on
// the other. TestRound4GateFindings_NeverElsewhere already accepted "home"
// OR "unanchored" at :103 (never demanding the fix); this test pins the
// actual post-fix behavior down precisely, at BOTH call sites, so a future
// change that reintroduces the over-claim fails here even if a broader
// "home or unanchored" check would not have caught it.
func TestManifestGoRunsRootEnvBranch_NoLongerOverclaimsHome(t *testing.T) {
	root := repoRoot(t)
	report, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan(%s): %v", root, err)
	}
	find := func(line int) (Site, bool) {
		for _, s := range report.Sites {
			if s.File == "internal/testkernel/experiment/manifest.go" && s.Line == line {
				return s, true
			}
		}
		return Site{}, false
	}
	for _, line := range []int{41, 103} {
		s, ok := find(line)
		if !ok {
			t.Fatalf("manifest.go:%d not found in inventory", line)
		}
		if s.Category == "home" {
			t.Errorf("manifest.go:%d = home (pattern %q) — RunsRoot's `root` is only \"home\" on the env-var-UNSET branch; the env-var-SET branch is an unresolvable os.Getenv(), so this must never confidently claim home again", line, s.Pattern)
		}
		if s.Category != "unanchored" {
			t.Errorf("manifest.go:%d = category %q, want unanchored", line, s.Category)
		}
		if strings.Contains(s.Pattern, "<Home>") {
			t.Errorf("manifest.go:%d pattern %q still shows the env-var-unset branch's <Home> — the conditional rebind was not poisoned", line, s.Pattern)
		}
	}
}

// TestAppendEventBucketRows_NeverDoubleCount checks that a call site whose
// bucket argument is genuinely dynamic (not a positive literal) does NOT
// get a synthetic bucket row — the ledger.go primitive site already covers
// it, and adding a second row would double-count the same underlying
// write. Spot-checked against turn_storage.go, whose sessionID is a
// per-turn struct field with no traced literal origin.
func TestAppendEventBucketRows_NeverDoubleCount(t *testing.T) {
	root := repoRoot(t)
	report, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan(%s): %v", root, err)
	}
	for _, s := range report.Sites {
		if s.Primitive == "engine.AppendEvent(bucket)" && s.File == "internal/engine/turn_storage.go" {
			t.Errorf("turn_storage.go unexpectedly got a bucket row (%d): %+v — its sessionID is turn.SessionID, a genuinely dynamic per-turn value with no literal origin", s.Line, s)
		}
	}
}

// TestSubprocessAppendix_Declared checks that the subprocess-writer
// appendix (item 5: declared out of scope, never silently dropped) is
// populated with the named exec.Command/exec.CommandContext sites, with
// cmd.Dir resolved where the source sets it.
func TestSubprocessAppendix_Declared(t *testing.T) {
	root := repoRoot(t)
	report, err := Scan(root)
	if err != nil {
		t.Fatalf("Scan(%s): %v", root, err)
	}
	if len(report.Subprocess) == 0 {
		t.Fatal("Report.Subprocess is empty — the subprocess appendix must enumerate exec.Command/exec.CommandContext sites, never silently drop them")
	}

	// findCall locates a subprocess site by file + a substring of the rendered
	// call, rather than by hardcoded line number.
	//
	// Line-keyed assertions fail on any edit ABOVE the call, which is a false
	// alarm that looks identical to the real thing this appendix guards
	// (a subprocess site disappearing). That is exactly what happened: #587
	// inserted one line into worktree_reconciler.go, the `git worktree list`
	// call moved 831->832 relative to its old position, and CI on main went
	// red with "not found in subprocess appendix" — indistinguishable, at a
	// glance, from someone deleting a tracked subprocess. main stayed red
	// across three subsequent merges.
	//
	// The call text is the stable identity here; the line number is an
	// accident of layout.
	findCall := func(file, callContains string) (SubprocessSite, bool) {
		for _, s := range report.Subprocess {
			if s.File == file && strings.Contains(s.Raw, callContains) {
				return s, true
			}
		}
		return SubprocessSite{}, false
	}

	wantDirCall := func(t *testing.T, file, callContains, dirContains string) {
		t.Helper()
		s, ok := findCall(file, callContains)
		if !ok {
			t.Fatalf("%s: no subprocess site whose call contains %q — "+
				"either the call was removed or the scanner stopped seeing it",
				file, callContains)
		}
		if !s.DirKnown {
			t.Errorf("%s:%d DirKnown=false, want a resolved cmd.Dir", file, s.Line)
			return
		}
		if !strings.Contains(s.Dir, dirContains) {
			t.Errorf("%s:%d Dir=%q, want it to contain %q", file, s.Line, s.Dir, dirContains)
		}
	}

	wantCall := func(t *testing.T, file, callContains, why string) {
		t.Helper()
		if _, ok := findCall(file, callContains); !ok {
			t.Errorf("%s: no subprocess site whose call contains %q (%s)",
				file, callContains, why)
		}
	}

	// NOTE: the line-keyed `find`/`wantDir` helpers were removed here in favour
	// of findCall/wantDirCall above. Every assertion in this test now keys on
	// the rendered call text, so inserting a line above a tracked subprocess
	// no longer turns CI red with a message that reads like a deleted site.

	// transition_hooks.go's `sh -c <h.Shell>` runs with cmd.Dir set to the
	// hook's own workspace parameter — an operator-configured shell
	// hook getting the workspace root as its cwd, exactly the §5.1a
	// config-overwrite shape the appendix exists to surface.
	wantCall(t, "internal/engine/transition_hooks.go", `"sh"`,
		"the sh -c transition-hook subprocess")
	// worktree_spawn.go / worktree_reconciler.go: `git worktree add|remove`
	// with cmd.Dir = repoRoot — a real filesystem mutation (a whole
	// worktree directory tree, or its recursive removal) this tool's
	// primitive scanner cannot see because it crosses a process boundary.
	wantDirCall(t, "internal/engine/worktree_spawn.go", `"git", args...`, "repoRoot")
	wantDirCall(t, "internal/engine/worktree_reconciler.go", `"list", "--porcelain"`, "repoRoot")
	// mcp_architecture.go / projection_compiler.go: python3 script
	// subprocesses with no cmd.Dir set at all (inherits the process's own
	// cwd) — DirKnown must be false, not a guessed value.
	if s, ok := findCall("internal/engine/mcp_architecture.go", `"python3"`); !ok {
		t.Error("mcp_architecture.go: python3 architecture-tool subprocess missing from the appendix")
	} else if s.DirKnown {
		t.Errorf("mcp_architecture.go:%d DirKnown=true (Dir=%q), want false — no cmd.Dir is set in source", s.Line, s.Dir)
	}
	wantCall(t, "internal/engine/projection_compiler.go", `"python3"`,
		"the python3 cogblock-parse subprocess")
	// site.go: `bash build.sh` with cmd.Dir = appDir.
	wantDirCall(t, "internal/providers/site/site.go", `"build.sh"`, "appDir")

	// The rendered markdown must carry the appendix and its scope-boundary
	// statement — declared, not silent.
	md := RenderMarkdown(report)
	if !strings.Contains(md, "Subprocess writers (declared out of scope for v1 — uncounted)") {
		t.Error("rendered markdown is missing the 'Subprocess writers' appendix heading")
	}
}

// TestNoInBandDisagreementMarker is the round-8 grep, made a test. The whole
// premise of the branch-disagreement repair is that a disagreement travels
// out-of-band as degenerateRoot; if any reserved marker string could reach a
// pattern, the leak that motivated the repair is back. Asserted over BOTH the
// live scan and the committed golden bytes, since a marker that reached only
// one of the two would be its own kind of drift.
//
// The retired marker is reconstructed at runtime rather than spelled, so this
// test cannot pass merely because this file contains the literal.
func TestNoInBandDisagreementMarker(t *testing.T) {
	retired := "{" + "branch-" + "disagreement" + "}"

	report, err := Scan(repoRoot(t))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, s := range report.Sites {
		if strings.Contains(s.Pattern, retired) {
			t.Errorf("%s:%d pattern %q carries the retired in-band marker", s.File, s.Line, s.Pattern)
		}
	}

	jsonPath, mdPath := goldenPaths(t)
	for _, path := range []string{jsonPath, mdPath} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if strings.Contains(string(b), retired) {
			t.Errorf("%s still contains the retired in-band marker", filepath.Base(path))
		}
	}
}
