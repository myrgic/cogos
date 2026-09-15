// coherence.go — CogOS v3 coherence validation
//
// Simplified from apps/cogos/validation.go (v2.4.0).
// Provides the 4-layer validation stack as callable functions.
// The continuous process runs this on a cadence; it is not session-triggered.
//
// Layers:
//  1. Schema    — frontmatter structure valid
//  2. Invariants — system invariants hold (nucleus loaded, workspace intact)
//  3. Policy    — kernel boundary not violated
//  4. Consistency — cross-artifact coherence
package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ValidationResult is the outcome of a single validation check.
type ValidationResult struct {
	Pass       bool        `json:"pass"`
	Layer      string      `json:"layer"`
	Diagnostic *Diagnostic `json:"diagnostic,omitempty"`
	Timestamp  string      `json:"timestamp"`
}

// Diagnostic carries the details of a validation failure.
type Diagnostic struct {
	Rule       string `json:"rule"`
	Expected   string `json:"expected"`
	Actual     string `json:"actual"`
	Suggestion string `json:"suggestion"`
	Severity   string `json:"severity"` // "error", "warning", "info"
}

// CoherenceReport aggregates all validation results from a single pass.
type CoherenceReport struct {
	Pass      bool               `json:"pass"`
	Results   []ValidationResult `json:"results"`
	Timestamp string             `json:"timestamp"`
}

// RunCoherence executes the 4-layer validation stack and returns a report.
// An optional *CogDocIndex enables Layer 4 dead-reference detection; without
// it Layer 4 passes trivially (maintaining backward compatibility).
func RunCoherence(cfg *Config, nucleus *Nucleus, idxArgs ...*CogDocIndex) *CoherenceReport {
	var idx *CogDocIndex
	if len(idxArgs) > 0 {
		idx = idxArgs[0]
	}

	report := &CoherenceReport{
		Timestamp: nowISO(),
		Pass:      true,
	}

	layers := []func(*Config, *Nucleus) ValidationResult{
		validateSchema,
		validateInvariants,
		validatePolicy,
		func(cfg *Config, nucleus *Nucleus) ValidationResult {
			return validateConsistency(cfg, nucleus, idx)
		},
	}

	for _, fn := range layers {
		result := fn(cfg, nucleus)
		report.Results = append(report.Results, result)
		if !result.Pass {
			report.Pass = false
		}
	}

	return report
}

// validateSchema (Layer 1): workspace structure is intact.
func validateSchema(cfg *Config, _ *Nucleus) ValidationResult {
	ts := time.Now().UTC().Format(time.RFC3339)
	required := []string{
		filepath.Join(cfg.CogDir, "config", "identity.yaml"),
		filepath.Join(cfg.CogDir, "mem"),
	}
	for _, path := range required {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return ValidationResult{
				Pass:      false,
				Layer:     "schema",
				Timestamp: ts,
				Diagnostic: &Diagnostic{
					Rule:       "schema.required_path",
					Expected:   fmt.Sprintf("exists: %s", path),
					Actual:     "not found",
					Suggestion: "Ensure the workspace is properly initialized (cog init)",
					Severity:   "error",
				},
			}
		}
	}
	return ValidationResult{Pass: true, Layer: "schema", Timestamp: ts}
}

// validateInvariants (Layer 2): nucleus is loaded and valid.
func validateInvariants(_ *Config, nucleus *Nucleus) ValidationResult {
	ts := time.Now().UTC().Format(time.RFC3339)
	if nucleus == nil {
		return ValidationResult{
			Pass:      false,
			Layer:     "invariants",
			Timestamp: ts,
			Diagnostic: &Diagnostic{
				Rule:       "I1.nucleus_loaded",
				Expected:   "nucleus != nil",
				Actual:     "nil",
				Suggestion: "Nucleus failed to load at startup; check identity config",
				Severity:   "error",
			},
		}
	}
	if nucleus.Name == "" {
		return ValidationResult{
			Pass:      false,
			Layer:     "invariants",
			Timestamp: ts,
			Diagnostic: &Diagnostic{
				Rule:       "I2.nucleus_name",
				Expected:   "nucleus.Name != empty",
				Actual:     "empty",
				Suggestion: "Check identity card frontmatter has a 'name' field",
				Severity:   "error",
			},
		}
	}
	return ValidationResult{Pass: true, Layer: "invariants", Timestamp: ts}
}

// validatePolicy (Layer 3): kernel is operating within policy boundaries.
// Stub for stage 1 — always passes. Policy enforcement added in later stages.
func validatePolicy(_ *Config, _ *Nucleus) ValidationResult {
	return ValidationResult{
		Pass:      true,
		Layer:     "policy",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

// validateConsistency (Layer 4): cross-artifact coherence.
// When idx is non-nil, it validates that every explicit cog:// reference
// declared in frontmatter refs: blocks resolves to an existing file.
// With a nil idx the check is skipped (always passes).
func validateConsistency(cfg *Config, _ *Nucleus, idx *CogDocIndex) ValidationResult {
	ts := time.Now().UTC().Format(time.RFC3339)

	if idx == nil || cfg == nil {
		return ValidationResult{Pass: true, Layer: "consistency", Timestamp: ts}
	}

	// Collect dead explicit references (frontmatter refs: only — not inline,
	// which may be aspirational or point to files created later).
	var dead []string
	for _, doc := range idx.ByURI {
		for _, ref := range doc.Refs {
			res, err := ResolveURI(cfg.WorkspaceRoot, ref.URI)
			if err != nil {
				// Unknown type or resolution error — flag as dead.
				dead = append(dead, ref.URI)
				continue
			}
			if _, statErr := os.Stat(res.Path); os.IsNotExist(statErr) {
				dead = append(dead, ref.URI)
			}
		}
	}

	if len(dead) == 0 {
		return ValidationResult{Pass: true, Layer: "consistency", Timestamp: ts}
	}

	// Report up to 3 dead refs in the diagnostic to keep it readable.
	sample := dead
	if len(sample) > 3 {
		sample = sample[:3]
	}
	return ValidationResult{
		Pass:      false,
		Layer:     "consistency",
		Timestamp: ts,
		Diagnostic: &Diagnostic{
			Rule:     "C1.dead_references",
			Expected: "all explicit cog:// refs resolve to existing files",
			Actual:   fmt.Sprintf("%d dead ref(s): %v", len(dead), sample),
			Suggestion: "Remove or fix broken refs; " +
				"run `cog coherence check` for the full list",
			Severity: "warning",
		},
	}
}
