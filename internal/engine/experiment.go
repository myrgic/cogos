// experiment.go — autoresearch experiment runner
//
// An experiment is a CogDoc (YAML frontmatter + markdown body) that specifies:
//   - Which benchmark prompts file to use
//   - Model, budget, and method
//   - Optional comparison against a baseline run
//
// Usage:
//
//	cogos experiment run <path-to-experiment.md>
//
// The runner:
//  1. Loads the experiment config from YAML frontmatter
//  2. Loads the benchmark prompts
//  3. Runs the benchmark suite
//  4. Saves results as a new experiment log CogDoc
//  5. If a previous run exists, computes and prints the recall/precision delta
//  6. Flags regressions (recall drop > threshold)
package engine

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ExperimentConfig is the YAML frontmatter of an experiment CogDoc.
type ExperimentConfig struct {
	Type    string `yaml:"type"`
	Title   string `yaml:"title"`
	Created string `yaml:"created"`
	Run     struct {
		PromptsFile         string  `yaml:"prompts_file"`         // path to benchmark_prompts.json
		Model               string  `yaml:"model"`                // e.g. "qwen3.5:9b"
		Budget              int     `yaml:"budget"`               // token budget (0 = default 4096)
		Method              string  `yaml:"method"`               // e.g. "keyword-match"
		RegressionThreshold float64 `yaml:"regression_threshold"` // recall drop that triggers flag (default 0.1)
		BaselineRun         string  `yaml:"baseline_run"`         // path to previous result CogDoc for comparison
	} `yaml:"run"`
}

// ExperimentDelta is the change in aggregate metrics vs a baseline.
type ExperimentDelta struct {
	RecallDelta    float64
	PrecisionDelta float64
	IsRegression   bool
}

// loadExperimentConfig reads an experiment CogDoc and parses the YAML frontmatter.
func loadExperimentConfig(path string) (*ExperimentConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	content := string(data)
	skipBytes := 0
	switch {
	case strings.HasPrefix(content, "---\n"):
		skipBytes = 4
	case strings.HasPrefix(content, "---\r\n"):
		skipBytes = 5
	default:
		return nil, fmt.Errorf("no YAML frontmatter found in %s", path)
	}

	rest := content[skipBytes:]
	yamlBlock, _, found := strings.Cut(rest, "\n---")
	if !found {
		return nil, fmt.Errorf("unterminated frontmatter in %s", path)
	}

	var cfg ExperimentConfig
	if err := yaml.Unmarshal([]byte(yamlBlock), &cfg); err != nil {
		return nil, fmt.Errorf("parse frontmatter %s: %w", path, err)
	}
	return &cfg, nil
}

// RunExperiment loads and executes an experiment document.
// workspaceRoot is used to resolve relative prompt file paths.
func RunExperiment(ctx context.Context, experimentPath, workspaceRoot string, process *Process, router Router) error {
	slog.Info("experiment: loading", "path", experimentPath)

	cfg, err := loadExperimentConfig(experimentPath)
	if err != nil {
		return fmt.Errorf("load experiment: %w", err)
	}

	if cfg.Run.PromptsFile == "" {
		cfg.Run.PromptsFile = filepath.Join(workspaceRoot,
			"apps", "cogos", "testdata", "benchmark_prompts.json")
	} else if !filepath.IsAbs(cfg.Run.PromptsFile) {
		cfg.Run.PromptsFile = filepath.Join(workspaceRoot, cfg.Run.PromptsFile)
	}

	prompts, err := LoadPrompts(cfg.Run.PromptsFile)
	if err != nil {
		return fmt.Errorf("load prompts: %w", err)
	}

	threshold := cfg.Run.RegressionThreshold
	if threshold == 0 {
		threshold = 0.1
	}
	method := cfg.Run.Method
	if method == "" {
		method = "keyword-match"
	}

	// Run benchmark.
	suite := NewBenchmarkSuite(process, router, cfg.Run.Model, cfg.Run.Budget)

	slog.Info("experiment: running benchmark",
		"title", cfg.Title,
		"prompts", len(prompts),
		"model", cfg.Run.Model,
		"method", method,
	)

	results := suite.Run(ctx, prompts)
	PrintSummary(results)

	// Save results.
	if err := SaveResults(workspaceRoot, results, cfg.Run.Model, method); err != nil {
		slog.Warn("experiment: save results failed", "err", err)
	}

	// Compare to baseline if specified.
	if cfg.Run.BaselineRun != "" {
		baselinePath := cfg.Run.BaselineRun
		if !filepath.IsAbs(baselinePath) {
			baselinePath = filepath.Join(workspaceRoot, baselinePath)
		}
		delta, err := computeDelta(results, baselinePath)
		if err != nil {
			slog.Warn("experiment: baseline comparison failed", "err", err)
		} else {
			printDelta(delta, threshold)
			if delta.IsRegression {
				return fmt.Errorf("REGRESSION: recall dropped by %.2f (threshold %.2f)",
					-delta.RecallDelta, threshold)
			}
		}
	}

	return nil
}

// computeDelta parses a previous SaveResults file and computes metric deltas.
// The baseline file is expected to contain "Avg Recall:" and "Avg Precision:" lines.
func computeDelta(current []BenchmarkResult, baselinePath string) (*ExperimentDelta, error) {
	data, err := os.ReadFile(baselinePath)
	if err != nil {
		return nil, fmt.Errorf("read baseline %s: %w", baselinePath, err)
	}

	// Parse "**Avg Recall:** 0.80  **Avg Precision:** 0.60  **N:** 5" from the markdown.
	var baseRecall, basePrec float64
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "**Avg Recall:**") {
			fmt.Sscanf(strings.ReplaceAll(line, "**", ""), "Avg Recall: %f  Avg Precision: %f",
				&baseRecall, &basePrec)
			break
		}
	}

	var sumRecall, sumPrec float64
	for _, r := range current {
		sumRecall += r.Recall
		sumPrec += r.Precision
	}
	n := float64(len(current))
	curRecall := sumRecall / n
	curPrec := sumPrec / n

	delta := &ExperimentDelta{
		RecallDelta:    curRecall - baseRecall,
		PrecisionDelta: curPrec - basePrec,
	}
	// Flag regression when recall drops beyond the threshold.
	delta.IsRegression = delta.RecallDelta < -0.1 // caller overrides with cfg threshold
	return delta, nil
}

// printDelta logs the metric delta vs baseline.
func printDelta(d *ExperimentDelta, threshold float64) {
	sign := func(v float64) string {
		if v >= 0 {
			return fmt.Sprintf("+%.3f", v)
		}
		return fmt.Sprintf("%.3f", v)
	}
	fmt.Printf("Delta vs baseline  recall=%s  precision=%s\n",
		sign(d.RecallDelta), sign(d.PrecisionDelta))
	if d.RecallDelta < -threshold {
		fmt.Printf("⚠ REGRESSION: recall dropped %.3f (threshold %.2f)\n",
			-d.RecallDelta, threshold)
	}
}

// writeExperimentTemplate writes a starter experiment CogDoc to path.
func writeExperimentTemplate(path string) error {
	ts := time.Now().UTC().Format(time.RFC3339)
	content := fmt.Sprintf(`---
type: experiment
title: "Foveated Context Experiment"
created: "%s"
run:
  prompts_file: ""  # default: apps/cogos/testdata/benchmark_prompts.json
  model: "qwen3.5:9b"
  budget: 4096
  method: "keyword-match"
  regression_threshold: 0.10
  baseline_run: ""  # path to a previous bench-*.md for comparison
---

# Foveated Context Experiment

Describe the hypothesis or change being evaluated here.
`, ts)
	return os.WriteFile(path, []byte(content), 0644)
}

// runExperimentCmd is the entry point for the "experiment" subcommand.
func runExperimentCmd(args []string, workspaceRoot string, defaultPort int) {
	fs := flag.NewFlagSet("experiment", flag.ExitOnError)
	workspace := fs.String("workspace", workspaceRoot, "Workspace root path (auto-detected if empty)")
	port := fs.Int("port", defaultPort, "Daemon port (for health check)")
	_ = fs.Parse(args)

	if *port == 0 {
		*port = 6931
	}

	subArgs := fs.Args()
	if len(subArgs) == 0 {
		fmt.Fprintln(os.Stderr, "usage: cogos experiment <run|new> [args...]")
		os.Exit(1)
	}

	switch subArgs[0] {
	case "run":
		if len(subArgs) < 2 {
			fmt.Fprintln(os.Stderr, "usage: cogos experiment run <path-to-experiment.md>")
			os.Exit(1)
		}
		runExperimentRun(subArgs[1], *workspace)

	case "new":
		out := "experiment.md"
		if len(subArgs) >= 2 {
			out = subArgs[1]
		}
		if err := writeExperimentTemplate(out); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Created experiment template: %s\n", out)

	default:
		fmt.Fprintf(os.Stderr, "unknown experiment subcommand: %s\n", subArgs[0])
		os.Exit(1)
	}
}

// runExperimentRun executes a single experiment document.
func runExperimentRun(experimentPath, workspaceRoot string) {
	level := slog.LevelInfo
	if os.Getenv("COG_LOG_DEBUG") != "" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	cfg, err := LoadConfig(workspaceRoot, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load config: %v\n", err)
		os.Exit(1)
	}
	// Upgrade stderr-only logger to also fan into the kernel slog JSONL sink
	// (Agent U's kernel-slog-api). See log_capture.go.
	upgradeLoggerWithFileSink(cfg)

	nucleus, err := LoadNucleus(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: load nucleus: %v\n", err)
		os.Exit(1)
	}

	process := NewProcess(cfg, nucleus)

	// Build index eagerly (don't wait for consolidation tick).
	if idx, err := BuildIndex(cfg.WorkspaceRoot); err != nil {
		slog.Warn("experiment: index build failed", "err", err)
	} else {
		process.indexMu.Lock()
		process.index = idx
		process.indexMu.Unlock()
	}

	// Attempt to update the attentional field.
	if err := process.field.Update(); err != nil {
		slog.Warn("experiment: field update failed", "err", err)
	}

	router, err := BuildRouter(cfg)
	if err != nil {
		slog.Warn("experiment: router unavailable; assembly-only mode", "err", err)
	}

	if err := RunExperiment(context.Background(), experimentPath, cfg.WorkspaceRoot, process, router); err != nil {
		fmt.Fprintf(os.Stderr, "experiment failed: %v\n", err)
		os.Exit(1)
	}
}
