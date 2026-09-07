// cli_doctor.go — "cogos doctor" subcommand.
//
// Usage:
//
//	cogos doctor [--workspace PATH] [--json] [--stale-days N] [--deep]
//	             [--context-budget-kb N] [--scan-dir DIR ...]
//	             [--config FILE ...] [--skip-network]
//	             [--lint [--severity-min warn|fail]]
//
// doctor compares the local install and workspace against what a healthy
// install should look like. It exists because every failure mode it checks
// for fails *silently*: a stale binary still prints a version string, an
// empty search result still exits 0, a dead SQLite store still answers
// queries. See myrgic/cogos#568 for the incident that motivated it — a
// working session spent diagnosing a phantom FTS bug that was actually a
// 79-day-old binary shadowing the real one on PATH. myrgic/cogos#571 adds
// the lint exit contract, --deep store corruption checking, *.corrupt-*
// enumeration, and the context-construction check group below.
//
// # Output contract
//
// Every check reports exactly one of:
//
//	OK       - checked, and the property holds.
//	WARN     - checked, holds with a caveat (drift, staleness, an unindexed
//	           tree) that a human should look at but that is not itself
//	           evidence of a broken system.
//	FAIL     - checked, and the property does NOT hold. A FAIL means the
//	           system is misbehaving, not merely untidy (e.g. the negative-
//	           control search returned zero hits for a term known to be
//	           indexed).
//	UNKNOWN  - the check could not be performed (missing file, unreadable
//	           database, unreachable network, no daemon running). UNKNOWN is
//	           never reported as OK — a check that cannot run has learned
//	           nothing, and silently defaulting to "OK" is exactly the
//	           failure class this command exists to close off.
//
// # Exit contract
//
// Two postures, chosen deliberately per #571 rather than replacing one with
// the other:
//
//	cogos doctor              (advisory, DEFAULT): exit 0 unless at least one
//	                           check reports FAIL, in which case exit 1. This
//	                           is the exact contract #570 shipped and documented
//	                           first; kept unchanged by default so scripts and
//	                           habits built against `cog doctor && echo ok`
//	                           keep working. WARN/UNKNOWN never affect this
//	                           exit code.
//
//	cogos doctor --lint        (gate, OPT-IN): exit 0 = no finding at or above
//	  [--severity-min warn|fail]  --severity-min (default "warn"); exit 1 = at
//	                           least one finding meets it; exit 2 = the
//	                           command failed before a report was produced at
//	                           all — never conflated with "produced a report
//	                           full of findings". UNKNOWN always counts as
//	                           meeting the warn threshold (an observation
//	                           doctor could not perform is never a
//	                           lintable-clean state); at the fail threshold
//	                           only FAIL counts, since UNKNOWN is explicitly
//	                           not evidence of breakage.
//
// Bad flags exit 2 UNCONDITIONALLY, in every invocation, with or without
// --lint — not a --lint-specific behavior. This isn't new in #571: fs is a
// flag.ExitOnError FlagSet, and the stdlib's Parse calls os.Exit(2) itself
// on any parse error before ever returning one (ErrHelp — "-h" — is the one
// exception, exiting 0); this has been true since #570. What IS --lint-
// specific pre-report failure is everything downstream of a successful
// parse: an unresolvable workspace, or an invalid --severity-min value.
//
// The issue text describes the *advisory* posture itself changing ("exit 0
// once a report is produced, regardless of findings"). That would break the
// documented #570 contract for anyone already gating CI/cron on the bare
// `cog doctor` exit code, so this ships the reconciliation instead: the old
// any-FAIL contract stays the default, and the new threshold-gate contract
// is what --lint opts into. `--lint` is the correct command for "advisory
// run for a human" vs "gate for CI/cron" the issue is actually asking to
// distinguish; a caller that wants FAIL-only gating uses `--lint
// --severity-min fail`, which is exactly today's any-FAIL contract made
// explicit and requested on purpose rather than inherited by default.
//
// cli_*.go files may import sdk/constellation; see the package-boundary note
// in cli_reindex.go.
package engine

import (
	"context"
	"database/sql"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

// DoctorStatus is the verdict of a single doctor check.
type DoctorStatus string

const (
	StatusOK      DoctorStatus = "OK"
	StatusWarn    DoctorStatus = "WARN"
	StatusFail    DoctorStatus = "FAIL"
	StatusUnknown DoctorStatus = "UNKNOWN"
)

// DoctorCheck is one reported assertion within a group.
type DoctorCheck struct {
	Name   string       `json:"name"`
	Status DoctorStatus `json:"status"`
	Detail string       `json:"detail"`
}

// DoctorGroup is a named cluster of related checks (install integrity,
// config coherence, index health, store liveness).
type DoctorGroup struct {
	Name   string        `json:"name"`
	Checks []DoctorCheck `json:"checks"`
}

// DoctorReport is the full result of a `cogos doctor` run.
type DoctorReport struct {
	Workspace   string        `json:"workspace"`
	GeneratedAt time.Time     `json:"generated_at"`
	Groups      []DoctorGroup `json:"groups"`
}

// ExitCode implements the default ADVISORY output contract: non-zero iff any
// check FAILed. This is the #570 contract, unchanged by #571 — see the
// package doc comment's "Exit contract" section for why the lint-threshold
// contract lives in --lint / LintFindings instead of replacing this.
func (r *DoctorReport) ExitCode() int {
	if r.hasStatus(StatusFail) {
		return 1
	}
	return 0
}

// LintFindings reports whether at least one check in r meets or exceeds min
// (StatusWarn or StatusFail — the two --severity-min values `cogos doctor
// --lint` accepts). At the warn threshold, WARN, UNKNOWN, and FAIL all
// count: an observation doctor could not perform (UNKNOWN) is never a
// lintable-clean state. At the fail threshold, only FAIL counts — UNKNOWN is
// explicitly not evidence the system is broken, so it must not gate a
// fail-only check.
func (r *DoctorReport) LintFindings(min DoctorStatus) bool {
	for _, g := range r.Groups {
		for _, c := range g.Checks {
			if lintMeetsThreshold(c.Status, min) {
				return true
			}
		}
	}
	return false
}

func lintMeetsThreshold(status, min DoctorStatus) bool {
	if min == StatusFail {
		return status == StatusFail
	}
	// Default / warn threshold.
	return status == StatusWarn || status == StatusUnknown || status == StatusFail
}

func (r *DoctorReport) hasStatus(s DoctorStatus) bool {
	for _, g := range r.Groups {
		for _, c := range g.Checks {
			if c.Status == s {
				return true
			}
		}
	}
	return false
}

func (r *DoctorReport) addGroup(name string) *DoctorGroup {
	r.Groups = append(r.Groups, DoctorGroup{Name: name})
	return &r.Groups[len(r.Groups)-1]
}

func (g *DoctorGroup) add(name string, status DoctorStatus, detail string) {
	g.Checks = append(g.Checks, DoctorCheck{Name: name, Status: status, Detail: detail})
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// DoctorOptions configures a doctor run. Zero value is a sane default.
type DoctorOptions struct {
	// ExtraScanDirs are additional directories to search for stray
	// cogos/cog binaries, beyond the built-in defaults ($PATH entries,
	// ~/.cog*, ~/go/bin, the workspace root and its .cog dir).
	ExtraScanDirs []string
	// ExtraConfigFiles are additional config files to check for coherence,
	// beyond the built-in defaults (~/.claude/settings*.json, discovered
	// .mcp.json files, ~/.hermes/profiles/*/config.yaml).
	ExtraConfigFiles []string
	// StaleDays is the age threshold, in days, beyond which a SQLite
	// store's last write flags it DEAD (WARN) rather than merely queryable.
	StaleDays int
	// SkipNetwork disables the published-tag lookup (install integrity
	// check "version vs published tag"), which otherwise makes one HTTP
	// call to GitHub. Useful offline; the check reports UNKNOWN instead of
	// silently passing.
	SkipNetwork bool
	// ReleaseRepo is the "owner/repo" GitHub slug used for the
	// published-tag check. Defaults to "myrgic/cogos".
	ReleaseRepo string
	// Deep runs PRAGMA quick_check against every discovered SQLite store
	// (store liveness group). Off by default: quick_check is a full page
	// scan, and the constellation.db this checks against on a working
	// substrate runs ~978MB — not free, hence opt-in.
	Deep bool
	// DeepTimeout bounds each store's quick_check (per #571: "bounded: use a
	// context timeout ~30s per store and report UNKNOWN on timeout, never
	// OK"). Defaults to 30s; exposed on DoctorOptions rather than hardcoded
	// so tests can drive the timeout path deterministically without a real
	// 30s wait.
	DeepTimeout time.Duration
	// ContextBudgetKB is the WARN threshold, in KB, for a single
	// always-loaded context file (context-construction group). Defaults to
	// 64.
	ContextBudgetKB int
}

func (o *DoctorOptions) normalize() {
	if o.StaleDays <= 0 {
		o.StaleDays = 30
	}
	if o.ReleaseRepo == "" {
		o.ReleaseRepo = "myrgic/cogos"
	}
	if o.DeepTimeout <= 0 {
		o.DeepTimeout = 30 * time.Second
	}
	if o.ContextBudgetKB <= 0 {
		o.ContextBudgetKB = 64
	}
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

func runDoctorCmd(args []string, defaultWorkspace string) {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root path (auto-detected from cwd if empty)")
	jsonOut := fs.Bool("json", false, "Output the report as JSON")
	staleDays := fs.Int("stale-days", 30, "Days since last write before a SQLite store is flagged DEAD")
	skipNetwork := fs.Bool("skip-network", false, "Skip the published-tag network lookup (report UNKNOWN instead)")
	releaseRepo := fs.String("release-repo", "myrgic/cogos", "owner/repo GitHub slug used for the published-tag check")
	deep := fs.Bool("deep", false, "Run PRAGMA quick_check against every discovered SQLite store (FAIL on corruption). Off by default: a full page scan is not free against a large store.")
	contextBudgetKB := fs.Int("context-budget-kb", 64, "WARN threshold, in KB, for a single always-loaded context file")
	lint := fs.Bool("lint", false, "Lint mode: exit by --severity-min threshold instead of the default any-FAIL contract (see the full exit contract above)")
	severityMin := fs.String("severity-min", "warn", "Lint threshold with --lint: \"warn\" or \"fail\". UNKNOWN always meets \"warn\"; only FAIL meets \"fail\".")
	var scanDirs stringListFlag
	fs.Var(&scanDirs, "scan-dir", "Extra directory to scan for stray cogos/cog binaries (repeatable)")
	var configFiles stringListFlag
	fs.Var(&configFiles, "config", "Extra config file to check for coherence (repeatable)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cogos doctor [flags]\n\n")
		fmt.Fprintf(os.Stderr, "Compares the local install and workspace against what a healthy install\n")
		fmt.Fprintf(os.Stderr, "should look like: install integrity, config coherence, index health,\n")
		fmt.Fprintf(os.Stderr, "SQLite store liveness, and context-construction hygiene.\n\n")
		fmt.Fprintf(os.Stderr, "Output contract:\n")
		fmt.Fprintf(os.Stderr, "  OK       checked, property holds\n")
		fmt.Fprintf(os.Stderr, "  WARN     checked, holds with a caveat worth a human's attention\n")
		fmt.Fprintf(os.Stderr, "  FAIL     checked, property does NOT hold\n")
		fmt.Fprintf(os.Stderr, "  UNKNOWN  could not be checked (never reported as OK)\n\n")
		fmt.Fprintf(os.Stderr, "Exit contract (two postures, see the cli_doctor.go package doc for full rationale):\n")
		fmt.Fprintf(os.Stderr, "  Bad flags always exit 2, with or without --lint (the stdlib flag package's own\n")
		fmt.Fprintf(os.Stderr, "  behavior, unchanged since #570 -- not itself part of either posture below).\n")
		fmt.Fprintf(os.Stderr, "  cogos doctor              (advisory, DEFAULT) exit 0 unless a check reports FAIL,\n")
		fmt.Fprintf(os.Stderr, "                            then 1. WARN/UNKNOWN never affect this exit code. This\n")
		fmt.Fprintf(os.Stderr, "                            is the exact #570 contract, unchanged by default.\n")
		fmt.Fprintf(os.Stderr, "  cogos doctor --lint       (gate, opt-in) exit 0 = no finding at/above\n")
		fmt.Fprintf(os.Stderr, "    [--severity-min X]      --severity-min (default warn); exit 1 = at least one\n")
		fmt.Fprintf(os.Stderr, "                            finding meets it; exit 2 = a resolvable-workspace or\n")
		fmt.Fprintf(os.Stderr, "                            --severity-min failure before a report was produced.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		fs.PrintDefaults()
	}

	// fs is flag.ExitOnError: Parse calls os.Exit(2) itself on any parse
	// error (ErrHelp/"-h" is the sole exception, exiting 0) before this line
	// could ever observe a non-nil err -- see the flag package source. Bad
	// flags have therefore always exited 2 unconditionally, regardless of
	// --lint, since #570; this check is defensive, not reachable in
	// practice, and deliberately does NOT branch on *lint the way the
	// downstream pre-report-failure checks below do.
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	minStatus := StatusWarn
	if *lint {
		switch strings.ToLower(strings.TrimSpace(*severityMin)) {
		case "", "warn":
			minStatus = StatusWarn
		case "fail":
			minStatus = StatusFail
		default:
			fmt.Fprintf(os.Stderr, "error: --severity-min must be \"warn\" or \"fail\", got %q\n", *severityMin)
			os.Exit(2)
		}
	}

	root := *workspace
	if root == "" {
		var err error
		root, err = reconcileResolveWorkspace()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: resolve workspace: %v\n", err)
			if *lint {
				os.Exit(2) // pre-report failure under --lint's three-posture contract
			}
			os.Exit(1)
		}
	}

	opts := DoctorOptions{
		ExtraScanDirs:    []string(scanDirs),
		ExtraConfigFiles: []string(configFiles),
		StaleDays:        *staleDays,
		SkipNetwork:      *skipNetwork,
		ReleaseRepo:      *releaseRepo,
		Deep:             *deep,
		ContextBudgetKB:  *contextBudgetKB,
	}

	report := RunDoctor(root, opts)

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
	} else {
		printDoctorReport(os.Stdout, report)
	}

	if *lint {
		// This status line is diagnostic, not part of the report: it always
		// goes to stderr, never stdout, regardless of --json. Writing it to
		// stdout after enc.Encode(report) above would append a plain-text
		// line onto the encoded JSON stream on the SAME fd, corrupting it
		// for exactly the machine-consumption (CI/cron, piped through jq)
		// use case --lint exists to serve.
		if report.LintFindings(minStatus) {
			fmt.Fprintf(os.Stderr, "lint: severity-min=%s, finding(s) at/above threshold, exit 1\n", minStatus)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "lint: severity-min=%s, no finding at/above threshold, exit 0\n", minStatus)
		os.Exit(0)
	}
	os.Exit(report.ExitCode())
}

// stringListFlag implements flag.Value for a repeatable string flag.
type stringListFlag []string

func (s *stringListFlag) String() string { return strings.Join(*s, ",") }
func (s *stringListFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func printDoctorReport(w *os.File, r *DoctorReport) {
	fmt.Fprintf(w, "cogos doctor — workspace %s\n\n", r.Workspace)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	counts := map[DoctorStatus]int{}
	for _, g := range r.Groups {
		fmt.Fprintf(tw, "-- %s --\t\t\n", g.Name)
		for _, c := range g.Checks {
			counts[c.Status]++
			fmt.Fprintf(tw, "  [%s]\t%s\t%s\n", c.Status, c.Name, doctorFirstLine(c.Detail))
			for _, extra := range doctorExtraLines(c.Detail) {
				fmt.Fprintf(tw, "\t\t  %s\n", extra)
			}
		}
	}
	tw.Flush()
	fmt.Fprintf(w, "\n%d OK, %d WARN, %d FAIL, %d UNKNOWN\n",
		counts[StatusOK], counts[StatusWarn], counts[StatusFail], counts[StatusUnknown])
	if counts[StatusFail] > 0 {
		fmt.Fprintf(w, "exit 1 (FAIL present)\n")
	}
}

func doctorFirstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func doctorExtraLines(s string) []string {
	i := strings.IndexByte(s, '\n')
	if i < 0 {
		return nil
	}
	return strings.Split(s[i+1:], "\n")
}

// ---------------------------------------------------------------------------
// RunDoctor — orchestration (no os.Exit; testable directly)
// ---------------------------------------------------------------------------

// RunDoctor runs every check group against the workspace at root and returns
// the assembled report. It never calls os.Exit and never panics on a missing
// or unreadable workspace — a nonexistent root simply drives every group's
// checks to UNKNOWN or FAIL as appropriate, per the output contract.
func RunDoctor(root string, opts DoctorOptions) *DoctorReport {
	opts.normalize()

	report := &DoctorReport{
		Workspace:   root,
		GeneratedAt: time.Now().UTC(),
	}

	doctorInstallIntegrity(report, root, opts)
	doctorConfigCoherence(report, root, opts)
	doctorIndexHealth(report, root, opts)
	doctorStoreLiveness(report, root, opts)
	doctorContextConstruction(report, root, opts)
	doctorCredentialHygiene(report, root, opts)

	return report
}

// ---------------------------------------------------------------------------
// Group 1: Install integrity
// ---------------------------------------------------------------------------

func doctorInstallIntegrity(report *DoctorReport, root string, opts DoctorOptions) {
	g := report.addGroup("install integrity")

	// -- PATH resolution -----------------------------------------------
	pathBins := map[string]string{} // name -> resolved path
	for _, name := range []string{"cogos", "cog"} {
		p, err := lookPathAll(name)
		if err != nil || len(p) == 0 {
			g.add("PATH resolution: "+name, StatusWarn, name+" not found on PATH")
			continue
		}
		pathBins[name] = p[0]
		detail := fmt.Sprintf("%s -> %s", name, p[0])
		if len(p) > 1 {
			detail += fmt.Sprintf("\nshadowed by %d more PATH entries: %s", len(p)-1, strings.Join(p[1:], ", "))
		}
		g.add("PATH resolution: "+name, StatusOK, detail)
	}

	// -- Version + build info of the resolved cogos ---------------------
	var resolvedInfo *buildinfo.BuildInfo
	if p, ok := pathBins["cogos"]; ok {
		bi, err := buildinfo.ReadFile(p)
		if err != nil {
			g.add("resolved binary build info", StatusUnknown, fmt.Sprintf("could not read build info from %s: %v", p, err))
		} else {
			resolvedInfo = bi
			g.add("resolved binary build info", StatusOK, fmt.Sprintf(
				"%s\ngo=%s module=%s version=%s",
				p, bi.GoVersion, bi.Main.Path, bi.Main.Version))
		}
	} else {
		g.add("resolved binary build info", StatusUnknown, "no cogos resolved on PATH to inspect")
	}

	// -- Version vs published tag ---------------------------------------
	doctorVersionVsPublished(g, resolvedInfo, opts)

	// -- Enumerate every cogos/cog binary on disk, dev-artifact flagged --
	doctorBinarySprawl(g, root, opts)
}

// lookPathAll resolves name against every directory on PATH, returning every
// match (not just the first) so shadowing can be reported. Mirrors
// exec.LookPath's executable-bit check but does not stop at the first hit.
func lookPathAll(name string) ([]string, error) {
	pathEnv := os.Getenv("PATH")
	if pathEnv == "" {
		return nil, fmt.Errorf("PATH is empty")
	}
	var matches []string
	for _, dir := range filepath.SplitList(pathEnv) {
		if dir == "" {
			dir = "."
		}
		cand := filepath.Join(dir, name)
		info, err := os.Stat(cand)
		if err != nil || info.IsDir() {
			continue
		}
		if runtime.GOOS != "windows" && info.Mode()&0111 == 0 {
			continue // not executable
		}
		matches = append(matches, cand)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("%s: not found on PATH", name)
	}
	return matches, nil
}

// ghTagRelease is the subset of the GitHub release JSON doctor needs.
type ghTagRelease struct {
	TagName string `json:"tag_name"`
}

func doctorVersionVsPublished(g *DoctorGroup, bi *buildinfo.BuildInfo, opts DoctorOptions) {
	const name = "version vs published tag"
	if opts.SkipNetwork {
		g.add(name, StatusUnknown, "network check skipped (--skip-network)")
		return
	}
	if bi == nil {
		g.add(name, StatusUnknown, "no resolved binary build info to compare")
		return
	}
	running := bi.Main.Version
	if running == "" || running == "(devel)" {
		g.add(name, StatusWarn, fmt.Sprintf("resolved binary reports version %q — a local `go build` without -ldflags, not a tagged release", running))
		return
	}

	url := "https://api.github.com/repos/" + opts.ReleaseRepo + "/releases/latest"
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		g.add(name, StatusUnknown, fmt.Sprintf("build request: %v", err))
		return
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := client.Do(req)
	if err != nil {
		g.add(name, StatusUnknown, fmt.Sprintf("GitHub unreachable: %v", err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		g.add(name, StatusUnknown, fmt.Sprintf("GitHub returned status %d", resp.StatusCode))
		return
	}
	var rel ghTagRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		g.add(name, StatusUnknown, fmt.Sprintf("decode GitHub response: %v", err))
		return
	}
	if rel.TagName == "" {
		g.add(name, StatusUnknown, "GitHub response had no tag_name")
		return
	}

	if running == rel.TagName || "v"+strings.TrimPrefix(running, "v") == rel.TagName {
		g.add(name, StatusOK, fmt.Sprintf("running %s matches latest published tag %s", running, rel.TagName))
		return
	}
	g.add(name, StatusWarn, fmt.Sprintf("running %s, latest published tag is %s", running, rel.TagName))
}

// binInfo is one located cogos/cog binary on disk.
type binInfo struct {
	Path    string
	Size    int64
	Age     time.Duration
	ModTime time.Time
	DevOnly bool // in-repo build artifact, not a real install location
}

// cogosBinaryNameRe matches "cogos" or "cog" themselves plus the naming
// variants a manual install/rollback workflow actually produces on this
// class of tooling: "cogos.prev", "cogos.prev2", "cogos-build", "cog.bak",
// etc. Anchored so it does not match unrelated files that merely start with
// "cog" (e.g. "cogfield", "cogn8-notes.md").
var cogosBinaryNameRe = regexp.MustCompile(`^cog(?:os)?(?:[.\-_].*)?$`)

func doctorBinarySprawl(g *DoctorGroup, root string, opts DoctorOptions) {
	home, _ := os.UserHomeDir()

	dirs := map[string]bool{} // dedupe
	addDir := func(d string) {
		if d != "" {
			dirs[d] = true
		}
	}
	for _, d := range filepath.SplitList(os.Getenv("PATH")) {
		addDir(d)
	}
	if home != "" {
		addDir(filepath.Join(home, ".cog"))
		addDir(filepath.Join(home, ".cog", "bin"))
		addDir(filepath.Join(home, ".cog", "flight-ops"))
		addDir(filepath.Join(home, "go", "bin"))
	}
	if exe, err := os.Executable(); err == nil {
		addDir(filepath.Dir(exe))
	}
	// The doctor target workspace itself: a workspace-local wrapper script
	// (e.g. scripts/cog) commonly resolves a binary from the workspace root
	// or its .cog dir, and a build sitting there silently shadows whatever
	// the wrapper *thinks* it is running (cogos#568 finding 1).
	if root != "" {
		addDir(root)
		addDir(filepath.Join(root, ".cog"))
	}
	for _, d := range opts.ExtraScanDirs {
		addDir(d)
	}

	var found []binInfo
	for dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !cogosBinaryNameRe.MatchString(e.Name()) {
				continue
			}
			p := filepath.Join(dir, e.Name())
			info, err := e.Info()
			if err != nil {
				continue
			}
			if runtime.GOOS != "windows" && info.Mode()&0111 == 0 {
				continue // not executable — a stray config/log file, not a binary
			}
			found = append(found, binInfo{
				Path:    p,
				Size:    info.Size(),
				Age:     time.Since(info.ModTime()),
				ModTime: info.ModTime(),
				DevOnly: looksLikeDevArtifact(p, opts),
			})
		}
	}

	if len(found) == 0 {
		g.add("binary sprawl", StatusUnknown, "no scan directories yielded a stat'able cogos/cog binary")
		return
	}

	sort.Slice(found, func(i, j int) bool { return found[i].Path < found[j].Path })

	var lines []string
	for _, b := range found {
		tag := ""
		if b.DevOnly {
			tag = " [dev-only, in-repo build artifact]"
		}
		lines = append(lines, fmt.Sprintf("%s  %.1fMB  age=%s%s",
			b.Path, float64(b.Size)/1e6, roundDays(b.Age), tag))
	}
	detail := fmt.Sprintf("%d binaries found:\n%s", len(found), strings.Join(lines, "\n"))

	status := StatusOK
	nonDev := 0
	for _, b := range found {
		if !b.DevOnly {
			nonDev++
		}
	}
	if nonDev > 1 {
		status = StatusWarn
	}
	g.add("binary sprawl", status, detail)
}

// looksLikeDevArtifact flags a binary path that sits inside a git worktree of
// the cogos repo itself (a `go build` output next to go.mod) rather than a
// real install location. It walks upward from the binary looking for a
// sibling go.mod that declares the cogos module — cheap, and correct
// regardless of where the repo happens to be checked out.
func looksLikeDevArtifact(binPath string, opts DoctorOptions) bool {
	dir := filepath.Dir(binPath)
	for i := 0; i < 4; i++ { // repo root is at most a few levels up from a build artifact
		gomod := filepath.Join(dir, "go.mod")
		if data, err := os.ReadFile(gomod); err == nil {
			if strings.Contains(string(data), "module github.com/myrgic/cogos") {
				return true
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return false
}

func roundDays(d time.Duration) string {
	days := d.Hours() / 24
	if days < 1 {
		return fmt.Sprintf("%.0fh", d.Hours())
	}
	return fmt.Sprintf("%.0fd", days)
}

// ---------------------------------------------------------------------------
// Group 2: Config coherence
// ---------------------------------------------------------------------------

// pathLikeRe extracts filesystem-path-shaped substrings from a larger string
// (e.g. the path embedded in a `Bash(find /Users/x/y ...)` permission
// string). It is deliberately anchored to conventional absolute-path roots
// (~/, /Users/, /home/, /opt/, /usr/, /var/, /tmp/, /etc/) rather than any
// "/segment/segment" shape — Hermes/MCP configs are full of superficially
// path-shaped strings that are not paths at all (HuggingFace model IDs like
// "lmstudio-eclipse/google/gemma-4-e4b", API base URLs like
// "https://api.anthropic.com/v1", docker image refs, MCP route strings like
// "/v1/synthesize"), and an unanchored pattern flags all of them as
// "nonexistent path" false positives.
var pathLikeRe = regexp.MustCompile(`~/[A-Za-z0-9_.\-]+(?:/[A-Za-z0-9_.\-]+)*|/(?:Users|home|opt|usr|var|tmp|etc)/[A-Za-z0-9_.\-]+(?:/[A-Za-z0-9_.\-]+)*`)
var mcpServerNameRe = regexp.MustCompile(`mcp__([A-Za-z0-9_-]+)__`)

func doctorConfigCoherence(report *DoctorReport, root string, opts DoctorOptions) {
	g := report.addGroup("config coherence")
	home, _ := os.UserHomeDir()

	var files []string
	if home != "" {
		files = append(files,
			filepath.Join(home, ".claude", "settings.json"),
			filepath.Join(home, ".claude", "settings.local.json"),
		)
		files = append(files, globQuiet(filepath.Join(home, ".claude", "*.mcp.json"))...)
		files = append(files, globQuiet(filepath.Join(home, ".claude", "**", ".mcp.json"))...)
		files = append(files, globQuiet(filepath.Join(home, ".hermes", "profiles", "*", "config.yaml"))...)
	}
	files = append(files, filepath.Join(root, ".mcp.json"))
	files = append(files, opts.ExtraConfigFiles...)

	type foundConfig struct {
		path string
		data any
	}
	var loaded []foundConfig
	checkedAny := false
	for _, f := range dedupeExisting(files) {
		checkedAny = true
		data, err := loadConfigFile(f)
		if err != nil {
			g.add("parse "+f, StatusWarn, fmt.Sprintf("could not parse: %v", err))
			continue
		}
		loaded = append(loaded, foundConfig{path: f, data: data})
	}

	if !checkedAny {
		g.add("config discovery", StatusUnknown, "no MCP/Claude/Hermes config files found at the known locations")
		return
	}
	g.add("config discovery", StatusOK, fmt.Sprintf("%d config file(s) found", len(loaded)))

	// -- Nonexistent path references -------------------------------------
	var badRefs []string
	serverNames := map[string]bool{}
	cogosCommands := map[string]bool{} // distinct "command" values whose basename is cogos/cog
	for _, fc := range loaded {
		raw, _ := json.Marshal(fc.data) // normalize YAML→map through JSON for uniform string-walking
		var generic any
		if err := json.Unmarshal(raw, &generic); err == nil {
			walkStrings(generic, func(s string) {
				for _, m := range mcpServerNameRe.FindAllStringSubmatch(s, -1) {
					serverNames[m[1]] = true
				}
				for _, m := range pathLikeRe.FindAllString(s, -1) {
					p := expandHome(m, os.Getenv("HOME"))
					if looksLikeRealPath(p) {
						if _, err := os.Stat(p); err != nil {
							badRefs = append(badRefs, fmt.Sprintf("%s: %s (in %s)", "nonexistent path", p, fc.path))
						}
					}
				}
			})
			// Structured "command" field walk: precise binary-path extraction
			// (as opposed to the freeform path-substring scan above), so a
			// docker image tag or model ID that happens to be path-shaped
			// never gets counted as a binary reference.
			walkCommandFields(generic, func(cmd string) {
				base := filepath.Base(cmd)
				if base == "cogos" || base == "cog" {
					cogosCommands[cmd] = true
				}
			})
		}
	}
	badRefs = dedupeStrings(badRefs)
	if len(badRefs) == 0 {
		g.add("nonexistent path references", StatusOK, "no config path reference points at a missing directory")
	} else {
		sort.Strings(badRefs)
		g.add("nonexistent path references", StatusWarn, strings.Join(badRefs, "\n"))
	}

	// -- MCP configs agree on one binary -----------------------------------
	if len(cogosCommands) == 0 {
		g.add("MCP configs point at one binary", StatusUnknown, "no MCP config declares a cogos/cog \"command\" field to compare")
	} else if len(cogosCommands) == 1 {
		g.add("MCP configs point at one binary", StatusOK, "all reference(s) resolve to the same binary: "+doctorSortedBoolKeys(cogosCommands)[0])
	} else {
		g.add("MCP configs point at one binary", StatusWarn, "distinct binaries referenced:\n"+strings.Join(doctorSortedBoolKeys(cogosCommands), "\n"))
	}

	// -- MCP server-name generation drift ---------------------------------
	groups := groupByPrefix(mapKeys(serverNames))
	var driftLines []string
	for _, grp := range groups {
		if len(grp) > 1 {
			sort.Strings(grp)
			driftLines = append(driftLines, strings.Join(grp, ", "))
		}
	}
	if len(driftLines) == 0 {
		g.add("MCP server-name generations", StatusOK, "no server-name family has more than one generation referenced")
	} else {
		g.add("MCP server-name generations", StatusWarn, "multiple generations referenced:\n"+strings.Join(driftLines, "\n"))
	}
}

// walkCommandFields visits every object in a JSON-shaped any value that has
// a string "command" key (the MCP server-config shape: {"command": "...",
// "args": [...]}) and calls fn with that command string.
func walkCommandFields(v any, fn func(string)) {
	switch t := v.(type) {
	case map[string]any:
		if cmd, ok := t["command"].(string); ok {
			fn(cmd)
		}
		for _, e := range t {
			walkCommandFields(e, fn)
		}
	case []any:
		for _, e := range t {
			walkCommandFields(e, fn)
		}
	}
}

func doctorSortedBoolKeys(m map[string]bool) []string {
	out := mapKeys(m)
	sort.Strings(out)
	return out
}

// looksLikeRealPath filters pathLikeRe matches down to plausible filesystem
// paths: at least two path segments and not obviously a URL fragment.
func looksLikeRealPath(p string) bool {
	if strings.Contains(p, "://") {
		return false
	}
	segs := strings.Split(strings.Trim(p, "/"), "/")
	return len(segs) >= 2
}

func expandHome(p, home string) string {
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	if p == "~" {
		return home
	}
	if strings.HasPrefix(p, "~/") && home != "" {
		return filepath.Join(home, p[2:])
	}
	return p
}

// groupByPrefix groups names where the shortest name in a cluster is a
// literal prefix of the others (e.g. "cogos" is a prefix of "cogos-http" and
// "cogos-v3"), which is exactly the naming-generation drift pattern the
// config-coherence check looks for. Names that share no such relation stay
// in their own singleton group.
func groupByPrefix(names []string) [][]string {
	sort.Strings(names) // shortest-first within equal length is not guaranteed,
	// but sorting lexicographically clusters common-prefix names adjacently
	// enough for this heuristic; exhaustive pairwise check below is authoritative.
	used := make([]bool, len(names))
	var groups [][]string
	for i, base := range names {
		if used[i] {
			continue
		}
		group := []string{base}
		used[i] = true
		for j := i + 1; j < len(names); j++ {
			if used[j] {
				continue
			}
			if strings.HasPrefix(names[j], base+"-") || strings.HasPrefix(base, names[j]+"-") {
				group = append(group, names[j])
				used[j] = true
			}
		}
		groups = append(groups, group)
	}
	return groups
}

func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func dedupeExisting(paths []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range paths {
		if p == "" || seen[p] {
			continue
		}
		if _, err := os.Stat(p); err != nil {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

func globQuiet(pattern string) []string {
	// filepath.Glob does not support "**"; expand it into a bounded manual
	// walk (depth <= 4) when present, otherwise defer to filepath.Glob.
	if strings.Contains(pattern, "**") {
		parts := strings.SplitN(pattern, "**", 2)
		base := strings.TrimSuffix(parts[0], string(filepath.Separator))
		suffix := strings.TrimPrefix(parts[1], string(filepath.Separator))
		var out []string
		_ = filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				depth := strings.Count(strings.TrimPrefix(p, base), string(filepath.Separator))
				if depth > 4 {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(p, suffix) {
				out = append(out, p)
			}
			return nil
		})
		return out
	}
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil
	}
	return matches
}

func loadConfigFile(path string) (any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out any
	if strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml") {
		if err := yaml.Unmarshal(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// walkStrings visits every string leaf in a JSON-shaped any value (as
// produced by encoding/json or converted from YAML).
func walkStrings(v any, fn func(string)) {
	switch t := v.(type) {
	case string:
		fn(t)
	case []any:
		for _, e := range t {
			walkStrings(e, fn)
		}
	case map[string]any:
		for k, e := range t {
			fn(k)
			walkStrings(e, fn)
		}
	}
}

// ---------------------------------------------------------------------------
// Group 3: Index health
// ---------------------------------------------------------------------------

func doctorIndexHealth(report *DoctorReport, root string, opts DoctorOptions) {
	g := report.addGroup("index health")

	dbPath := filepath.Join(root, ".cog", ".state", "constellation.db")
	if _, err := os.Stat(dbPath); err != nil {
		g.add("constellation.db present", StatusUnknown, fmt.Sprintf("not found at %s: %v", dbPath, err))
		g.add("documents vs files on disk", StatusUnknown, "no database to compare against")
		g.add("index freshness", StatusUnknown, "no database to compare against")
		g.add("negative control", StatusUnknown, "no database to query")
		return
	}
	g.add("constellation.db present", StatusOK, dbPath)

	db, err := sql.Open("sqlite3", dbPath+"?mode=ro&_journal_mode=WAL&_busy_timeout=3000")
	if err != nil {
		g.add("constellation.db open", StatusUnknown, fmt.Sprintf("%v", err))
		g.add("documents vs files on disk", StatusUnknown, "database unreadable")
		g.add("index freshness", StatusUnknown, "database unreadable")
		g.add("negative control", StatusUnknown, "database unreadable")
		return
	}
	defer db.Close()

	doctorDocsVsFiles(g, db, root)
	doctorIndexFreshness(g, db, root)
	doctorNegativeControl(g, db, dbPath, root)
}

func doctorDocsVsFiles(g *DoctorGroup, db *sql.DB, root string) {
	// documents.path in the constellation DB is stored only after
	// filepath.EvalSymlinks resolution (walkRoots in
	// sdk/constellation/indexer.go resolves cogRoot the same way, with the
	// same fallback-to-unresolved on error). Building the LIKE prefix below
	// from the raw, unresolved root would never match a real row on any
	// workspace whose path traverses a symlink (macOS /tmp -> /private/tmp,
	// /var/folders temp dirs, a symlinked home/workspace mount), silently
	// reporting every populated .cog subtree as false UNINDEXED. Mirror the
	// indexer's own resolution exactly.
	cogDir := filepath.Join(root, ".cog")
	if resolved, err := filepath.EvalSymlinks(cogDir); err == nil {
		cogDir = resolved
	}
	entries, err := os.ReadDir(cogDir)
	if err != nil {
		g.add("documents vs files on disk", StatusUnknown, fmt.Sprintf("read %s: %v", cogDir, err))
		return
	}

	var lines []string
	unindexed := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sub := e.Name()
		subPath := filepath.Join(cogDir, sub)
		fileCount := countMarkdownFiles(subPath)
		if fileCount == 0 {
			continue // nothing on disk in this tree; no signal either way
		}
		rowCount, err := countDocsUnderPrefix(db, subPath)
		if err != nil {
			continue
		}
		status := "ok"
		if rowCount == 0 {
			unindexed++
			status = "UNINDEXED"
		} else if rowCount < fileCount/2 {
			status = "partial"
		}
		lines = append(lines, fmt.Sprintf(".cog/%s: %d files on disk, %d indexed [%s]", sub, fileCount, rowCount, status))
	}

	sort.Strings(lines)
	detail := strings.Join(lines, "\n")
	if detail == "" {
		g.add("documents vs files on disk", StatusUnknown, "no markdown-bearing .cog subdirectories found")
		return
	}
	if unindexed > 0 {
		g.add("documents vs files on disk", StatusWarn, detail)
	} else {
		g.add("documents vs files on disk", StatusOK, detail)
	}
}

// countMarkdownFiles counts *.cog.md files under dir -- the exact suffix
// sdk/constellation/indexer.go:129 requires for a file to be indexable at
// all. Counting plain *.md (any file ending in the substring ".md", which
// ".cog.md" itself also matches) would count files the indexer never looks
// at -- an ordinary README.md sitting in a .cog subdirectory, for example --
// and doctorDocsVsFiles would then report that subtree as false
// UNINDEXED/partial for content that was never eligible for indexing.
func countMarkdownFiles(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(p, ".cog.md") {
			n++
		}
		return nil
	})
	return n
}

func countDocsUnderPrefix(db *sql.DB, prefix string) (int, error) {
	var n int
	// Escape LIKE metacharacters in the literal prefix and require the
	// wildcard to start after a path separator, not immediately after the
	// prefix text. Without both of these, a bare `prefix+"%"` pattern (a) lets
	// any literal `_`/`%` in the real path act as an unintended wildcard, and
	// (b) matches sibling subtrees that merely share a name prefix -- e.g.
	// ".cog/adr" would also match every row under ".cog/adr-legacy" -- which
	// silently merges two subtrees' document counts. Same hazard the project
	// already called out and avoided once in sdk/constellation/indexer.go's
	// removed prefix-DELETE.
	//
	// The ESCAPE character below is deliberately '!', not the conventional
	// '\': on Windows, filepath.Separator IS '\', so declaring '\' as the
	// escape char would make the trailing separator+wildcard this pattern
	// appends collide with it -- 'prefix\%' would parse as an ESCAPE'd
	// literal '%' rather than separator-then-wildcard, making this query
	// return 0 for every subtree on Windows regardless of index health. '!'
	// is neither path separator this codebase runs on.
	pattern := escapeLikePattern(prefix) + string(filepath.Separator) + "%"
	err := db.QueryRow(`SELECT COUNT(*) FROM documents WHERE path LIKE ? ESCAPE '!'`, pattern).Scan(&n)
	return n, err
}

// escapeLikePattern escapes SQL LIKE metacharacters (%, _, and the escape
// character itself) in a literal string so it can be used as a LIKE prefix
// without a literal underscore or percent sign in a real path acting as an
// unintended wildcard. Pair with `ESCAPE '!'` in the query -- see
// countDocsUnderPrefix for why '!' rather than the conventional '\'.
func escapeLikePattern(s string) string {
	r := strings.NewReplacer(`!`, `!!`, `%`, `!%`, `_`, `!_`)
	return r.Replace(s)
}

func doctorIndexFreshness(g *DoctorGroup, db *sql.DB, root string) {
	var newestIndexed sql.NullString
	if err := db.QueryRow(`SELECT MAX(indexed_at) FROM documents`).Scan(&newestIndexed); err != nil || !newestIndexed.Valid {
		g.add("index freshness", StatusUnknown, "could not read newest indexed_at from documents table")
		return
	}
	indexedAt, err := time.Parse(time.RFC3339, newestIndexed.String)
	if err != nil {
		g.add("index freshness", StatusUnknown, fmt.Sprintf("unparseable indexed_at %q: %v", newestIndexed.String, err))
		return
	}

	// Scan the whole .cog/ tree, not just .cog/mem: IndexWorkspace's base walk
	// root (walkRoots in sdk/constellation/indexer.go) covers all of .cog/
	// (.cog/adr, .cog/hooks, etc, not only .cog/mem), so a freshness check
	// scoped to .cog/mem alone can report OK/fresh while a genuinely stale
	// edit sits in some other indexed subtree -- exactly the silent-false-OK
	// shape this command exists to rule out. This still does not cover
	// cogdocs.yaml requiredPaths declared outside .cog/ (an unconfirmed,
	// config-dependent case a reviewer flagged separately).
	cogDir := filepath.Join(root, ".cog")
	var newestMtime time.Time
	found := false
	_ = filepath.WalkDir(cogDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == ".state" {
				return fs.SkipDir // matches IndexWorkspace's own .state skip
			}
			return nil
		}
		// Only *.cog.md is ever indexed (indexer.go:129); an ordinary
		// non-cogdoc *.md file's mtime is not a signal the index should be
		// judged against -- editing a README shouldn't produce a false
		// freshness-gap WARN for content the indexer never looks at.
		if !strings.HasSuffix(p, ".cog.md") {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil {
			return nil
		}
		if info.ModTime().After(newestMtime) {
			newestMtime = info.ModTime()
			found = true
		}
		return nil
	})
	if !found {
		g.add("index freshness", StatusUnknown, fmt.Sprintf("no markdown files found under %s", cogDir))
		return
	}

	gap := newestMtime.Sub(indexedAt)
	detail := fmt.Sprintf("newest indexed_at=%s, newest file mtime=%s, gap=%s",
		indexedAt.Format(time.RFC3339), newestMtime.Format(time.RFC3339), gap.Round(time.Minute))
	if gap > time.Hour {
		g.add("index freshness", StatusWarn, detail)
	} else {
		g.add("index freshness", StatusOK, detail)
	}
}

// doctorNegativeControl is the single most important check: it derives a
// sentinel phrase from a document already known to be indexed, then runs
// that phrase through the REAL search path (searchMemoryFTS — the same
// function the MCP search tool calls). A live, healthy index MUST return at
// least one hit for a term drawn from its own contents; if it does not, the
// index is broken, not empty, and this check reports FAIL rather than
// letting an empty result read as "no matches" the way a caller normally
// would.
func doctorNegativeControl(g *DoctorGroup, db *sql.DB, dbPath, root string) {
	const name = "negative control (sentinel query)"

	rows, err := db.Query(`
		SELECT title FROM documents
		WHERE (status IS NULL OR status != 'deprecated') AND title != ''
		ORDER BY indexed_at DESC LIMIT 200
	`)
	if err != nil {
		g.add(name, StatusUnknown, fmt.Sprintf("could not sample documents: %v", err))
		return
	}
	defer rows.Close()

	sentinel := ""
	for rows.Next() {
		var title string
		if err := rows.Scan(&title); err != nil {
			continue
		}
		if w := longestWord(title); w != "" {
			sentinel = w
			break
		}
	}
	_ = rows.Close()

	if sentinel == "" {
		g.add(name, StatusUnknown, "could not derive a sentinel term from any indexed document title (empty or unqueryable table)")
		return
	}

	result, err := searchMemoryFTS(dbPath, root, sentinel, 5, "")
	if err != nil {
		g.add(name, StatusFail, fmt.Sprintf("BROKEN: sentinel %q derived from an indexed title, but searchMemoryFTS errored: %v", sentinel, err))
		return
	}
	count, _ := result["count"].(int)
	if count < 1 {
		g.add(name, StatusFail, fmt.Sprintf("BROKEN: sentinel %q was drawn from an indexed document title but the real search path (searchMemoryFTS) returned 0 hits — treat ALL empty search results from this index as untrustworthy until this is fixed", sentinel))
		return
	}
	g.add(name, StatusOK, fmt.Sprintf("sentinel %q (drawn from an indexed title) returned %d hit(s) through searchMemoryFTS", sentinel, count))
}

// longestWord returns the longest alphabetic word (>=6 chars) in s, lower-
// cased, as a distinctive-enough sentinel term. Empty if none qualifies.
func longestWord(s string) string {
	best := ""
	for _, w := range strings.FieldsFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z')
	}) {
		if len(w) >= 6 && len(w) > len(best) {
			best = w
		}
	}
	return strings.ToLower(best)
}

// ---------------------------------------------------------------------------
// Group 4: Store liveness
// ---------------------------------------------------------------------------

func doctorStoreLiveness(report *DoctorReport, root string, opts DoctorOptions) {
	g := report.addGroup("store liveness")

	cogDir := filepath.Join(root, ".cog")
	if _, err := os.Stat(cogDir); err != nil {
		g.add("store discovery", StatusUnknown, fmt.Sprintf("%s: %v", cogDir, err))
		return
	}

	doctorCorruptFiles(g, cogDir)

	var dbFiles []string
	_ = filepath.WalkDir(cogDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(p, ".db") {
			dbFiles = append(dbFiles, p)
		}
		return nil
	})

	if len(dbFiles) == 0 {
		g.add("store discovery", StatusUnknown, "no *.db files found under "+cogDir)
		return
	}
	sort.Strings(dbFiles)
	g.add("store discovery", StatusOK, fmt.Sprintf("%d SQLite store(s) found", len(dbFiles)))

	for _, path := range dbFiles {
		doctorOneStore(g, path, opts.StaleDays, opts.Deep, opts.DeepTimeout)
	}
}

// corruptFileMarker is the naming convention "<name>.corrupt-<timestamp>"
// that #571 item 2 specifies for a corruption-safe reindex-replace: rename,
// never unlink, a store that fails its integrity check, so corrupt data
// stays evidence instead of becoming garbage. Matched as a plain substring
// (mirroring the shell glob `*.corrupt-*` the issue specifies) rather than a
// stricter regexp, so any file carrying this marker anywhere in its name is
// surfaced regardless of the exact timestamp format used to produce it.
//
// The producer is `preserveCorruptStore`/`renameAside` in
// sdk/constellation/store_guard.go, invoked as constellation.PreserveCorruptStore
// from this package's runReindex (cli_reindex.go) -- shipped in #572, which
// landed on main after this branch's own merge-base, so it predates this
// check reaching main even though the two PRs were developed concurrently.
// `cogos reindex` against a corrupted constellation.db therefore already
// produces a *.corrupt-* file today: this check is load-bearing on the very
// first real corruption a user hits, not speculative or "ahead of its
// producer."
const corruptFileMarker = ".corrupt-"

// doctorCorruptFiles enumerates *.corrupt-* files under cogDir so a store
// preserved by this naming convention doesn't rot silently — the same
// anti-sprawl philosophy as the binary-sprawl check, surfacing exactly what
// `cogos reindex`'s corruption-safe rename-aside path (see
// corruptFileMarker's doc comment above) already leaves behind today. This
// runs unconditionally (not gated behind --deep): finding a corpse that
// already exists on disk costs one WalkDir, unlike producing a fresh one via
// quick_check.
func doctorCorruptFiles(g *DoctorGroup, cogDir string) {
	var found []string
	var walkErrs []string
	walkErr := filepath.WalkDir(cogDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// A path under cogDir could not be visited (permission denied,
			// vanished mid-walk, etc). Record it and keep walking the rest of
			// the tree so a single bad subdirectory doesn't blind the whole
			// check -- but this walk can no longer certify "no *.corrupt-*
			// files anywhere under cogDir", only "none found among the paths
			// we could reach". Never let that partial result present as OK.
			walkErrs = append(walkErrs, fmt.Sprintf("%s: %v", p, err))
			return nil
		}
		if !d.IsDir() && strings.Contains(d.Name(), corruptFileMarker) {
			found = append(found, p)
		}
		return nil
	})
	if walkErr != nil {
		// WalkDir itself only surfaces a top-level error here if the
		// callback returns non-nil, which it never does above; guarded
		// anyway so a future change to this callback can't silently regress
		// back to swallowing it.
		walkErrs = append(walkErrs, fmt.Sprintf("%s: %v", cogDir, walkErr))
	}

	if len(walkErrs) > 0 {
		sort.Strings(walkErrs)
		detail := fmt.Sprintf(
			"could not fully walk %s -- %d path(s) unreadable, so the absence of *.corrupt-* files under them is unverified (never reported as OK):\n%s",
			cogDir, len(walkErrs), strings.Join(walkErrs, "\n"))
		if len(found) > 0 {
			sort.Strings(found)
			detail = fmt.Sprintf("%s\nfound within the reachable portion of the tree, despite the incomplete walk:\n%s", detail, strings.Join(found, "\n"))
		}
		g.add("preserved corrupt stores", StatusUnknown, detail)
		return
	}

	if len(found) == 0 {
		g.add("preserved corrupt stores", StatusOK, "no *.corrupt-* files found under "+cogDir)
		return
	}
	sort.Strings(found)
	g.add("preserved corrupt stores", StatusWarn, fmt.Sprintf(
		"%d file(s) matching the *.corrupt-* preserved-corpse naming convention found (corrupt data is evidence, not garbage — do not delete without inspecting):\n%s",
		len(found), strings.Join(found, "\n")))
}

func doctorOneStore(g *DoctorGroup, path string, staleDays int, deep bool, deepTimeout time.Duration) {
	rel := path
	info, statErr := os.Stat(path)
	if statErr != nil {
		g.add("store: "+rel, StatusUnknown, statErr.Error())
		return
	}
	age := time.Since(info.ModTime())

	db, err := sql.Open("sqlite3", path+"?mode=ro&_busy_timeout=3000")
	if err != nil {
		g.add("store: "+rel, StatusUnknown, fmt.Sprintf("open: %v", err))
		return
	}
	defer db.Close()

	if deep {
		doctorQuickCheck(g, db, path, deepTimeout)
	}

	rowCount, tableErr := sumRowCounts(db)

	detail := fmt.Sprintf("last-write age=%s (mtime=%s)", roundDays(age), info.ModTime().Format(time.RFC3339))
	if tableErr == nil {
		detail = fmt.Sprintf("%s, ~%d rows across user tables", detail, rowCount)
	} else {
		detail = fmt.Sprintf("%s, row count unavailable: %v", detail, tableErr)
	}

	switch {
	case age > time.Duration(staleDays)*24*time.Hour:
		g.add("store: "+rel, StatusWarn, "DEAD (stale beyond "+fmt.Sprintf("%d", staleDays)+"d): "+detail)
	case tableErr != nil:
		// Row count could not be established (corrupt file, permission
		// denied, etc.). The last-write age above is a genuine measurement
		// (os.Stat, no db open required), but liveness itself is unverified
		// -- report UNKNOWN rather than falling through to OK, per the
		// never-report-unverified-as-OK contract this command advertises.
		g.add("store: "+rel, StatusUnknown, detail)
	case rowCount == 0:
		g.add("store: "+rel, StatusWarn, "empty store: "+detail)
	default:
		g.add("store: "+rel, StatusOK, detail)
	}
}

// doctorQuickCheck runs `PRAGMA quick_check` against an already-opened,
// read-only store handle, bounded by timeout. Per #571 item 2: corruption is
// a FAIL (the store is genuinely broken, not merely untidy); a timeout is
// UNKNOWN, never OK — quick_check not finishing in time is not evidence the
// store is healthy, only that this run couldn't verify it. A generic query
// error (permission, already-corrupt-enough-to-not-open) is likewise
// UNKNOWN, matching the row-count path's own never-OK-when-unverified rule
// just above it in doctorOneStore.
func doctorQuickCheck(g *DoctorGroup, db *sql.DB, path string, timeout time.Duration) {
	name := "quick check: " + path
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var result string
	err := db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&result)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			g.add(name, StatusUnknown, fmt.Sprintf("PRAGMA quick_check timed out after %s (never treated as OK)", timeout))
			return
		}
		g.add(name, StatusUnknown, fmt.Sprintf("PRAGMA quick_check: %v", err))
		return
	}
	if result != "ok" {
		g.add(name, StatusFail, fmt.Sprintf("PRAGMA quick_check reports corruption: %s", result))
		return
	}
	g.add(name, StatusOK, "PRAGMA quick_check: ok")
}

// sumRowCounts sums row counts across every non-internal, non-FTS-shadow
// table in the database. FTS5 virtual tables spawn shadow tables
// (`<name>_data`, `_idx`, `_docsize`, `_config`) that would double-count
// against the logical row total, so they are skipped.
func sumRowCounts(db *sql.DB) (int64, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		if strings.HasSuffix(name, "_data") || strings.HasSuffix(name, "_idx") ||
			strings.HasSuffix(name, "_docsize") || strings.HasSuffix(name, "_config") ||
			strings.HasSuffix(name, "_content") {
			continue
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(tables) == 0 {
		return 0, fmt.Errorf("no user tables")
	}

	var total int64
	for _, t := range tables {
		var n int64
		// Table names come from sqlite_master, not user input, but are
		// still interpolated into SQL text (COUNT(*) queries can't be
		// parameterized on table name) — quote defensively.
		q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, strings.ReplaceAll(t, `"`, `""`))
		if err := db.QueryRow(q).Scan(&n); err != nil {
			continue
		}
		total += n
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// Group 5: Context construction (myrgic/cogos#571 item 3)
//
// Claude Code's `/doctor` audits the *context economics* of an installation:
// unused extensions vs their context cost, oversized always-loaded memory
// files, duplicate config. The same drift class exists on a CogOS seat and
// nothing observed it before this group. Every check here is observation
// only — nothing in this group writes to any config file.
// ---------------------------------------------------------------------------

func doctorContextConstruction(report *DoctorReport, root string, opts DoctorOptions) {
	g := report.addGroup("context construction")
	home, homeErr := os.UserHomeDir()
	if homeErr != nil || home == "" {
		g.add("context construction", StatusUnknown, fmt.Sprintf("could not resolve home directory: %v", homeErr))
		return
	}

	doctorDuplicateToolsets(g, home)
	doctorDuplicatePermissions(g, home)
	doctorContextBudget(g, home, opts.ContextBudgetKB)
	doctorDeadHooks(g, home)
}

// -- 5a: duplicate toolset registrations across every config scope ---------

// mcpRegistration is one MCP server name as registered in one config scope,
// normalized to the target it actually mounts.
type mcpRegistration struct {
	Name   string
	Scope  string
	Target string
}

// mcpEntryTarget normalizes an MCP server definition (the {"command":...,
// "args":[...]} / {"type":"http","url":...} shape shared by Claude Code,
// Claude Desktop, and Hermes config) to the single string that identifies
// what it actually mounts: the URL for an http/sse server, or command+args
// joined for a stdio server — with one deliberate exception. A definition
// shaped like `command: npx, args: [mcp-remote, <url>]` (the wrapper Claude
// Desktop uses to mount an http MCP server through a stdio bridge) targets
// the URL argument alone, not the npx binary or the rest of its args: two
// such entries pointing at different URLs are NOT the same toolset even
// though "npx" is identical, and — the concrete case this check exists to
// catch (#571 live evidence) — an entry using this wrapper and an entry
// using a plain {"url": ...} registration for the SAME address ARE the same
// toolset even though neither their "command" nor their shape matches
// literally.
//
// Command+args (rather than the bare command alone) matters for any generic
// package-runner command — "uvx", "npx", "python3" and similar are
// legitimately shared by many UNRELATED MCP servers; using the bare command
// as the target would flag every uvx-launched server as a duplicate of
// every other one. `uvx blender-mcp` and `uvx some-other-tool` share a
// command and must NOT collide; two registrations that share command AND
// every arg (the real live-evidence "blender" case this check was
// validated against, registered identically in two scopes) legitimately
// should.
func mcpEntryTarget(def map[string]any) (string, bool) {
	if url, ok := def["url"].(string); ok && url != "" {
		return url, true
	}
	cmd, _ := def["command"].(string)
	if cmd == "" {
		return "", false
	}
	var args []string
	if raw, ok := def["args"].([]any); ok {
		for _, a := range raw {
			if s, ok := a.(string); ok {
				args = append(args, s)
			}
		}
	}
	if base := filepath.Base(cmd); base == "npx" || base == "npx.cmd" {
		// Only the documented `npx mcp-remote <url>` bridge shape targets its
		// URL argument -- the package name must be "mcp-remote" literally.
		// Matching ANY URL-shaped argument for ANY npx-launched server would
		// collide two unrelated packages that merely happen to take a
		// URL-shaped flag (e.g. --api-base, --callback) of their own, which
		// is exactly the generic-launcher false-positive this function's own
		// doc comment says the command+args design exists to avoid.
		//
		// The package name is not always args[0]: npx accepts its own flags
		// first, most commonly "-y"/"--yes" to skip the install-confirmation
		// prompt (`npx -y mcp-remote <url>`), so skip any leading
		// "-"-prefixed args to find it.
		pkgIdx := -1
		for i, a := range args {
			if strings.HasPrefix(a, "-") {
				continue
			}
			pkgIdx = i
			break
		}
		if pkgIdx >= 0 && args[pkgIdx] == "mcp-remote" {
			for _, s := range args[pkgIdx+1:] {
				if strings.Contains(s, "://") {
					return s, true
				}
			}
		}
	}
	if len(args) > 0 {
		return cmd + " " + strings.Join(args, " "), true
	}
	return cmd, true
}

// redactMCPTarget strips whatever part of an mcpEntryTarget result could
// carry a credential before it is ever grouped or displayed by
// doctorDuplicateToolsets. An http/sse MCP registration's URL routinely
// carries an auth token in its query string (?token=..., ?key=...) or
// userinfo (https://user:pass@host/...) -- doctor's report is built to be
// pasted/logged/shared for diagnosis, so printing that raw would leak it.
// Non-URL (stdio command+args) targets pass through unchanged; see
// doctorDuplicateToolsets' doc comment for why that half is not in scope
// here. Query string and fragment are stripped via plain string slicing
// BEFORE any url.Parse attempt, so a malformed URL that fails to parse
// still gets its query stripped rather than being returned raw.
func redactMCPTarget(target string) string {
	if !strings.Contains(target, "://") {
		return target
	}
	s := target
	if i := strings.IndexAny(s, "?#"); i >= 0 {
		s = s[:i]
	}

	// Strip any userinfo (user:pass@host) via plain string slicing BEFORE
	// attempting url.Parse, and independently of whether Parse below
	// succeeds. A malformed percent-escape inside the credential itself
	// (e.g. "https://user:p%2@host/mcp") makes net/url's own userinfo
	// unescaper return an error, so a Parse-then-clear-User approach loses
	// the credential silently on exactly the URLs where redaction matters
	// most -- Parse failing must never mean the credential survives into
	// the string this function returns.
	if schemeEnd := strings.Index(s, "://"); schemeEnd >= 0 {
		authorityStart := schemeEnd + len("://")
		authority := s[authorityStart:]
		if slash := strings.IndexByte(authority, '/'); slash >= 0 {
			authority = authority[:slash]
		}
		if at := strings.LastIndexByte(authority, '@'); at >= 0 {
			s = s[:authorityStart] + authority[at+1:] + s[authorityStart+len(authority):]
		}
	}

	if u, err := neturl.Parse(s); err == nil {
		u.User = nil
		return u.String()
	}
	return s
}

// mcpProjectScopeRe matches the scope label collectMCPRegistrations gives a
// ~/.claude.json PER-PROJECT mcpServers block ("~/.claude.json (project:
// <path>)"), as opposed to every other scope this doctor reads (user-level
// ~/.claude.json, ~/.mcp.json, settings*.json, Hermes profiles, Claude
// Desktop, managed-settings.json) -- all of which are always active
// regardless of which project (if any) a session is in.
var mcpProjectScopeRe = regexp.MustCompile(`^~/\.claude\.json \(project: (.+)\)$`)

// mcpRegistrationProject reports the project path a scope names, if it
// names one at all.
func mcpRegistrationProject(scope string) (project string, isProjectScoped bool) {
	m := mcpProjectScopeRe.FindStringSubmatch(scope)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// mcpRegistrationsGenuinelyCoLoad reports whether at least two of the given
// same-target registrations could actually be loaded into the SAME running
// session together -- the premise doctorDuplicateToolsets' "double context
// cost per session" WARN message asserts. Only one ~/.claude.json project's
// mcpServers block ever applies to a given session (whichever project that
// session is rooted in); every other scope this doctor reads is always
// active alongside it. So:
//   - two registrations in DIFFERENT projects, and nowhere else, never
//     co-load -- not a real collision, regardless of how many distinct
//     projects register the same target this way.
//   - a registration in ANY project co-loads with every non-project-scoped
//     registration (that project's session always has the global scopes
//     active too) -- a real collision.
//   - two non-project-scoped registrations always co-load with each other
//     -- a real collision.
//   - two registrations that share the SAME project (two differently-named
//     entries in one project's mcpServers block both resolving to this
//     target) co-load within that one project's own sessions -- a real
//     collision.
func mcpRegistrationsGenuinelyCoLoad(regs []mcpRegistration) bool {
	projectCounts := map[string]int{}
	nonProjectScoped := 0
	for _, r := range regs {
		if proj, ok := mcpRegistrationProject(r.Scope); ok {
			projectCounts[proj]++
		} else {
			nonProjectScoped++
		}
	}
	if nonProjectScoped >= 2 {
		return true
	}
	if nonProjectScoped >= 1 && len(projectCounts) >= 1 {
		return true
	}
	for _, n := range projectCounts {
		if n >= 2 {
			return true
		}
	}
	return false
}

// toAnyMap coerces a decoded JSON or YAML value to map[string]any. YAML
// decoded via gopkg.in/yaml.v3 into `any` already produces map[string]any
// for mapping nodes (unlike yaml.v2's map[interface{}]interface{}), so this
// is a plain type assertion, not a conversion — kept as a named helper so
// every registration-scope walker below reads the same way.
func toAnyMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

// collectMCPServersFrom walks one already-decoded "mcpServers"-shaped object
// (name -> definition) and appends a registration per name with a
// resolvable target, tagged with scope.
func collectMCPServersFrom(obj any, scope string, out *[]mcpRegistration) {
	ms, ok := toAnyMap(obj)
	if !ok {
		return
	}
	names := make([]string, 0, len(ms))
	for name := range ms {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic scan order; map[target] grouping below re-sorts anyway
	for _, name := range names {
		def, ok := toAnyMap(ms[name])
		if !ok {
			continue
		}
		if target, ok := mcpEntryTarget(def); ok {
			*out = append(*out, mcpRegistration{Name: name, Scope: scope, Target: target})
		}
	}
}

// collectMCPRegistrations gathers every MCP server registration this doctor
// can read, across every scope named in #571 item 3a, plus the scope hunt
// the issue asked for explicitly (the "browserOS" twin): ~/.claude.json
// top-level AND per-project, ~/.mcp.json, ~/.claude/settings*.json (if they
// carry mcpServers), ~/.hermes/profiles/*/config.yaml, Claude Desktop's own
// config (found during the hunt — see doc comment above doctorDuplicateToolsets),
// and the enterprise managed-settings.json path (present on none of the
// machines this shipped against, but checked per the issue). unreadable
// notes a scope that exists but could not be parsed, distinct from a scope
// that simply is not present on this machine (silently skipped, per the
// "missing = not listed" convention this whole command uses).
func collectMCPRegistrations(home string) (regs []mcpRegistration, unreadable []string) {
	noteBad := func(scope, path string, err error) {
		unreadable = append(unreadable, fmt.Sprintf("%s (%s): %v", scope, path, err))
	}

	// ~/.claude.json: top-level mcpServers + every projects.<path>.mcpServers.
	claudeJSON := filepath.Join(home, ".claude.json")
	if data, err := os.ReadFile(claudeJSON); err == nil {
		var top map[string]any
		if err := json.Unmarshal(data, &top); err != nil {
			noteBad("~/.claude.json", claudeJSON, err)
		} else {
			collectMCPServersFrom(top["mcpServers"], "~/.claude.json (user)", &regs)
			if projects, ok := toAnyMap(top["projects"]); ok {
				projPaths := make([]string, 0, len(projects))
				for p := range projects {
					projPaths = append(projPaths, p)
				}
				sort.Strings(projPaths)
				for _, p := range projPaths {
					if proj, ok := toAnyMap(projects[p]); ok {
						collectMCPServersFrom(proj["mcpServers"], "~/.claude.json (project: "+p+")", &regs)
					}
				}
			}
		}
	}

	// ~/.mcp.json
	mcpJSON := filepath.Join(home, ".mcp.json")
	if data, err := os.ReadFile(mcpJSON); err == nil {
		var top map[string]any
		if err := json.Unmarshal(data, &top); err != nil {
			noteBad("~/.mcp.json", mcpJSON, err)
		} else {
			collectMCPServersFrom(top["mcpServers"], "~/.mcp.json", &regs)
		}
	}

	// ~/.claude/settings.json + settings.local.json, if they carry mcpServers.
	for _, sf := range []struct{ path, label string }{
		{filepath.Join(home, ".claude", "settings.json"), "~/.claude/settings.json"},
		{filepath.Join(home, ".claude", "settings.local.json"), "~/.claude/settings.local.json"},
	} {
		data, err := os.ReadFile(sf.path)
		if err != nil {
			continue // not present -- not an error, just nothing to add
		}
		var top map[string]any
		if err := json.Unmarshal(data, &top); err != nil {
			noteBad(sf.label, sf.path, err)
			continue
		}
		collectMCPServersFrom(top["mcpServers"], sf.label, &regs)
	}

	// ~/.hermes/profiles/*/config.yaml: mcp_servers (Hermes' own key name).
	for _, cfgPath := range globQuiet(filepath.Join(home, ".hermes", "profiles", "*", "config.yaml")) {
		data, err := os.ReadFile(cfgPath)
		if err != nil {
			noteBad("hermes profile", cfgPath, err)
			continue
		}
		var doc map[string]any
		if err := yaml.Unmarshal(data, &doc); err != nil {
			noteBad("hermes profile", cfgPath, err)
			continue
		}
		profile := filepath.Base(filepath.Dir(cfgPath))
		collectMCPServersFrom(doc["mcp_servers"], "~/.hermes/profiles/"+profile+"/config.yaml", &regs)
	}

	// Claude Desktop's own config. Not one of the scopes #571 named up
	// front; found by the hunt the issue asked for when the twin wasn't in
	// ~/.claude/plugins or managed-settings.json (see doctorDuplicateToolsets
	// doc comment and the PR body for the hunt result).
	if desktopCfg := claudeDesktopConfigPath(home); desktopCfg != "" {
		if data, err := os.ReadFile(desktopCfg); err == nil {
			var top map[string]any
			if err := json.Unmarshal(data, &top); err != nil {
				noteBad("Claude Desktop config", desktopCfg, err)
			} else {
				collectMCPServersFrom(top["mcpServers"], "Claude Desktop config", &regs)
			}
		}
	}

	// Enterprise managed-settings.json (org policy scope) -- checked per the
	// issue's explicit hunt list; not present on a personal-machine install,
	// so this scope contributes nothing here but is not skipped in code.
	if managedPath := claudeCodeManagedSettingsPath(); managedPath != "" {
		if data, err := os.ReadFile(managedPath); err == nil {
			var top map[string]any
			if err := json.Unmarshal(data, &top); err != nil {
				noteBad("managed-settings.json", managedPath, err)
			} else {
				collectMCPServersFrom(top["mcpServers"], "managed-settings.json (org policy)", &regs)
			}
		}
	}

	return regs, unreadable
}

// claudeDesktopConfigPath returns Claude Desktop's config path for the
// current OS, per Anthropic's documented install layout. Falls back to the
// macOS path (this codebase's primary target, and this check's own
// development/verification platform) for any GOOS this doctor does not
// specifically know, rather than returning empty and silently skipping the
// scope everywhere unrecognized. Thin wrapper over
// claudeDesktopConfigPathForGOOS so tests can exercise all three branches
// deterministically regardless of the host running the test.
func claudeDesktopConfigPath(home string) string {
	return claudeDesktopConfigPathForGOOS(home, runtime.GOOS)
}

func claudeDesktopConfigPathForGOOS(home, goos string) string {
	switch goos {
	case "windows":
		appData := os.Getenv("APPDATA")
		if appData == "" {
			return "" // no reliable fallback: %APPDATA% absent means we cannot locate this scope at all
		}
		return filepath.Join(appData, "Claude", "claude_desktop_config.json")
	case "linux":
		return filepath.Join(home, ".config", "Claude", "claude_desktop_config.json")
	default: // darwin and anything else
		return filepath.Join(home, "Library", "Application Support", "Claude", "claude_desktop_config.json")
	}
}

// claudeCodeManagedSettingsPath returns Claude Code's enterprise
// managed-settings.json path for the current OS, per Anthropic's documented
// managed-policy locations. Unlike claudeDesktopConfigPath this is a
// machine-wide (not per-user) path with no $HOME component. Thin wrapper
// over claudeCodeManagedSettingsPathForGOOS, same test-determinism reason as
// claudeDesktopConfigPath above.
func claudeCodeManagedSettingsPath() string {
	return claudeCodeManagedSettingsPathForGOOS(runtime.GOOS)
}

func claudeCodeManagedSettingsPathForGOOS(goos string) string {
	switch goos {
	case "windows":
		programData := os.Getenv("ProgramData")
		if programData == "" {
			programData = `C:\ProgramData`
		}
		return filepath.Join(programData, "ClaudeCode", "managed-settings.json")
	case "linux":
		return filepath.Join(string(filepath.Separator), "etc", "claude-code", "managed-settings.json")
	default: // darwin and anything else
		return filepath.Join(string(filepath.Separator), "Library", "Application Support", "ClaudeCode", "managed-settings.json")
	}
}

// doctorDuplicateToolsets WARNs when the SAME target (URL or resolved
// command, per mcpEntryTarget) is registered under 2+ names or 2+ scopes --
// the harder case a single-file inspection or the existing "MCP server-name
// generations" check (doctorConfigCoherence, string-prefix drift on names
// within one merged config load) cannot see, because it never normalizes
// past the literal name or looks across every scope Claude Code / Claude
// Desktop / Hermes each maintain independently.
//
// Live evidence this check was built against (darkstar, 2026-08-21): the
// target http://127.0.0.1:9000/mcp is mounted THREE separate ways --
// "browseros" in ~/.claude.json's user scope (type:http, direct url),
// "browseros" again in ~/.hermes/profiles/darkstar/config.yaml's mcp_servers
// (same name, different scope), and "browserOS" in Claude Desktop's
// claude_desktop_config.json via the `npx mcp-remote <url>` stdio-bridge
// shape mcpEntryTarget normalizes through. The twin the issue asked doctor
// to hunt for was not in ~/.claude/plugins (no matches) or
// /Library/Application Support/ClaudeCode/managed-settings.json (does not
// exist on this machine) -- it was in Claude Desktop's own config, a scope
// outside the issue's original list, found by exhausting "any other config
// the harness reads" and confirmed by this session's own deferred-tool
// listing carrying both `mcp__browserOS__*` and `mcp__browseros__*`.
func doctorDuplicateToolsets(g *DoctorGroup, home string) {
	regs, unreadable := collectMCPRegistrations(home)
	if len(unreadable) > 0 {
		sort.Strings(unreadable)
		g.add("duplicate toolset scopes (unreadable)", StatusUnknown, strings.Join(unreadable, "\n"))
	}
	if len(regs) == 0 {
		g.add("duplicate toolset registrations", StatusUnknown, "no MCP server registration found in any scanned scope")
		return
	}

	// Group AND display by the REDACTED target, never the raw one: an http
	// MCP registration's URL routinely carries an auth token in its query
	// string (?token=..., ?key=...), and this report is built to be shared
	// -- pasted into an issue, logged from `--lint` in CI/cron, piped
	// through --json -- exactly the #568 narrative this PR itself cites.
	// Printing the raw target would leak that token into whatever the
	// report lands in. Grouping on the redacted form is also more CORRECT,
	// not just safer: two registrations of the same server that happen to
	// embed different per-registration tokens in their query string are
	// still the same double-mounted toolset and must still collide.
	//
	// Stdio command+args targets (mcpEntryTarget's non-URL branch) are
	// displayed as-is -- args in the registrations this check was built
	// against are package names and flags (uvx blender-mcp, npx -y
	// mcp-markdown-viewer), not secrets; secrets in a stdio MCP server's
	// config live in its "env" map, which this check never reads or
	// displays at all.
	byTarget := map[string][]mcpRegistration{}
	for _, r := range regs {
		key := redactMCPTarget(r.Target)
		byTarget[key] = append(byTarget[key], r)
	}
	var targets []string
	for t := range byTarget {
		targets = append(targets, t)
	}
	sort.Strings(targets)

	var dupLines []string
	for _, t := range targets {
		// Dedup identical (name, scope) pairs (a glob or double scan could
		// otherwise double-count the same registration).
		seen := map[string]bool{}
		var uniq []mcpRegistration
		for _, r := range byTarget[t] {
			key := r.Name + "|" + r.Scope
			if seen[key] {
				continue
			}
			seen[key] = true
			uniq = append(uniq, r)
		}
		if len(uniq) < 2 {
			continue
		}
		if !mcpRegistrationsGenuinelyCoLoad(uniq) {
			// Every registration at this target is confined to a DIFFERENT
			// ~/.claude.json project scope (or a single project scope with
			// no other registration anywhere), so no single session ever
			// loads more than one of them together -- see
			// mcpRegistrationsGenuinelyCoLoad's doc comment. Not a
			// duplicate this check should warn about.
			continue
		}
		var parts []string
		for _, r := range uniq {
			parts = append(parts, fmt.Sprintf("%q in %s", r.Name, r.Scope))
		}
		dupLines = append(dupLines, fmt.Sprintf("%s:\n    %s", t, strings.Join(parts, "\n    ")))
	}

	if len(dupLines) == 0 {
		g.add("duplicate toolset registrations", StatusOK, fmt.Sprintf(
			"%d distinct MCP target(s) across %d scanned scope-registration(s); none double-mounted", len(targets), len(regs)))
		return
	}
	g.add("duplicate toolset registrations", StatusWarn, fmt.Sprintf(
		"%d target(s) registered under 2+ names/scopes (double context cost per session):\n%s",
		len(dupLines), strings.Join(dupLines, "\n")))
}

// -- 5b: duplicate permission entries across settings.json / settings.local.json --

// loadPermissionEntries reads the allow/deny/ask lists from a Claude Code
// settings file. A missing file returns an empty map and a nil error (not
// having a settings.local.json is normal, not a defect); a present-but-
// unparseable file returns an error the caller reports as UNKNOWN.
func loadPermissionEntries(path string) (map[string][]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string][]string{}, nil
		}
		return nil, err
	}
	var top map[string]any
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, err
	}
	perms, ok := toAnyMap(top["permissions"])
	if !ok {
		return map[string][]string{}, nil
	}
	out := map[string][]string{}
	for _, list := range []string{"allow", "deny", "ask"} {
		raw, ok := perms[list].([]any)
		if !ok {
			continue
		}
		for _, e := range raw {
			if s, ok := e.(string); ok {
				out[list] = append(out[list], s)
			}
		}
	}
	return out, nil
}

// doctorDuplicatePermissions WARNs on exact-string permission entries
// present in both settings.json and settings.local.json's allow/deny/ask
// lists -- #568's sweep found the same mcp__cogos-v3__... line duplicated
// across both; nothing before this check ever re-verified that finding
// stayed fixed.
func doctorDuplicatePermissions(g *DoctorGroup, home string) {
	sjPath := filepath.Join(home, ".claude", "settings.json")
	slPath := filepath.Join(home, ".claude", "settings.local.json")
	sj, sjErr := loadPermissionEntries(sjPath)
	sl, slErr := loadPermissionEntries(slPath)
	if sjErr != nil || slErr != nil {
		var msgs []string
		if sjErr != nil {
			msgs = append(msgs, fmt.Sprintf("%s: %v", sjPath, sjErr))
		}
		if slErr != nil {
			msgs = append(msgs, fmt.Sprintf("%s: %v", slPath, slErr))
		}
		g.add("duplicate permission entries", StatusUnknown, strings.Join(msgs, "\n"))
		return
	}

	var dupes []string
	for _, list := range []string{"allow", "deny", "ask"} {
		slSet := map[string]bool{}
		for _, s := range sl[list] {
			slSet[s] = true
		}
		for _, s := range sj[list] {
			if slSet[s] {
				dupes = append(dupes, fmt.Sprintf("%s: %s", list, s))
			}
		}
	}
	dupes = dedupeStrings(dupes)
	if len(dupes) == 0 {
		g.add("duplicate permission entries", StatusOK, "no exact-string permission entry appears in both settings.json and settings.local.json")
		return
	}
	sort.Strings(dupes)
	g.add("duplicate permission entries", StatusWarn, fmt.Sprintf(
		"%d entry(ies) duplicated across settings.json and settings.local.json:\n%s", len(dupes), strings.Join(dupes, "\n")))
}

// -- 5c: always-loaded file budgets -----------------------------------------

// doctorContextBudget lists the size of every always-loaded context file
// this doctor knows about: ~/.claude/CLAUDE.md, every
// ~/.claude/projects/*/memory/MEMORY.md, and CLAUDE.md at the root (and
// .claude/ subdir) of every project ~/.claude.json has a "projects" entry
// for. A file over thresholdKB is called out; a missing file is simply not
// listed (absence is not a defect here); a present-but-unreadable file is
// UNKNOWN, never silently skipped the same way a missing one is.
func doctorContextBudget(g *DoctorGroup, home string, thresholdKB int) {
	var candidates []string
	candidates = append(candidates, filepath.Join(home, ".claude", "CLAUDE.md"))
	candidates = append(candidates, globQuiet(filepath.Join(home, ".claude", "projects", "*", "memory", "MEMORY.md"))...)

	if data, err := os.ReadFile(filepath.Join(home, ".claude.json")); err == nil {
		var top map[string]any
		if json.Unmarshal(data, &top) == nil {
			if projects, ok := toAnyMap(top["projects"]); ok {
				for p := range projects {
					candidates = append(candidates,
						filepath.Join(p, "CLAUDE.md"),
						filepath.Join(p, ".claude", "CLAUDE.md"))
				}
			}
		}
	}

	candidates = dedupeStrings(candidates)
	sort.Strings(candidates)

	var lines []string
	var unknowns []string
	overBudget := 0
	listed := 0
	var totalBytes int64
	for _, f := range candidates {
		info, err := os.Stat(f)
		if err != nil {
			if os.IsNotExist(err) {
				continue // not always-loaded on this machine -- not listed, not a defect
			}
			unknowns = append(unknowns, fmt.Sprintf("%s: %v", f, err))
			continue
		}
		listed++
		totalBytes += info.Size()
		kb := float64(info.Size()) / 1024
		tag := ""
		if kb > float64(thresholdKB) {
			overBudget++
			tag = fmt.Sprintf(" [OVER %dKB BUDGET]", thresholdKB)
		}
		lines = append(lines, fmt.Sprintf("%s: %.1fKB%s", f, kb, tag))
	}

	if len(unknowns) > 0 {
		sort.Strings(unknowns)
		g.add("always-loaded file budgets (unreadable)", StatusUnknown, strings.Join(unknowns, "\n"))
	}
	if listed == 0 {
		g.add("always-loaded file budgets", StatusUnknown, "no always-loaded context file found at any known location")
		return
	}

	detail := fmt.Sprintf("threshold=%dKB; %d file(s), %.1fKB total:\n%s",
		thresholdKB, listed, float64(totalBytes)/1024, strings.Join(lines, "\n"))
	if overBudget > 0 {
		g.add("always-loaded file budgets", StatusWarn, detail)
	} else {
		g.add("always-loaded file budgets", StatusOK, detail)
	}
}

// -- 5d: dead hook commands ---------------------------------------------------

// doctorDeadHooks WARNs, naming the hook event and the missing path, when a
// hook command configured in settings.json / settings.local.json references
// an executable/script path that no longer exists on disk. Hook commands in
// this codebase's own settings are compound shell-like strings (hookrun.py
// wrapping a target script, e.g. `python3 "<hookrun.py>" <label> python3
// "<target.py>" <event>`), so this reuses pathLikeRe -- the same anchored
// path-substring extractor doctorConfigCoherence already validated against
// false positives from URLs/model IDs -- rather than a naive first-token
// split, so every absolute path embedded in the command gets checked, not
// just the first.
func doctorDeadHooks(g *DoctorGroup, home string) {
	files := []struct{ path, label string }{
		{filepath.Join(home, ".claude", "settings.json"), "settings.json"},
		{filepath.Join(home, ".claude", "settings.local.json"), "settings.local.json"},
	}

	var dead []string
	var unknowns []string
	checkedAny := false
	for _, f := range files {
		data, err := os.ReadFile(f.path)
		if err != nil {
			continue // not present -- nothing to check, not a defect
		}
		checkedAny = true
		var top map[string]any
		if err := json.Unmarshal(data, &top); err != nil {
			unknowns = append(unknowns, fmt.Sprintf("%s: %v", f.path, err))
			continue
		}
		hooksField, ok := toAnyMap(top["hooks"])
		if !ok {
			continue
		}
		events := make([]string, 0, len(hooksField))
		for ev := range hooksField {
			events = append(events, ev)
		}
		sort.Strings(events)
		for _, event := range events {
			matchers, ok := hooksField[event].([]any)
			if !ok {
				continue
			}
			for _, rm := range matchers {
				mm, ok := toAnyMap(rm)
				if !ok {
					continue
				}
				hooksList, ok := mm["hooks"].([]any)
				if !ok {
					continue
				}
				for _, rh := range hooksList {
					hh, ok := toAnyMap(rh)
					if !ok {
						continue
					}
					cmd, _ := hh["command"].(string)
					if cmd == "" {
						continue
					}
					for _, m := range pathLikeRe.FindAllString(cmd, -1) {
						p := expandHome(m, home)
						if !looksLikeRealPath(p) {
							continue
						}
						if _, err := os.Stat(p); err != nil {
							dead = append(dead, fmt.Sprintf("%s (%s): %s", event, f.label, p))
						}
					}
				}
			}
		}
	}

	if len(unknowns) > 0 {
		sort.Strings(unknowns)
		g.add("dead hook commands (unreadable)", StatusUnknown, strings.Join(unknowns, "\n"))
	}
	if !checkedAny {
		g.add("dead hook commands", StatusUnknown, "no settings.json/settings.local.json found to check")
		return
	}
	dead = dedupeStrings(dead)
	if len(dead) == 0 {
		g.add("dead hook commands", StatusOK, "every path referenced by a configured hook command exists on disk")
		return
	}
	sort.Strings(dead)
	g.add("dead hook commands", StatusWarn, fmt.Sprintf(
		"%d hook-referenced path(s) do not exist on disk:\n%s", len(dead), strings.Join(dead, "\n")))
}
