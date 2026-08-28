package acp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestFrameCensus is the L1 frame-catalogue spike's verification tool. It
// reads every golden_*.ndjson fixture in testdata/ and tabulates frame
// `type` (and `subtype` where present) so the questions this spike exists
// to answer can be read straight off `go test -run TestFrameCensus -v`:
//
//   - does rate_limit_event appear?
//   - what frames only show up with --include-partial-messages /
//     --include-hook-events?
//   - anything absent from, or new relative to, the ADR-093 §10 May
//     catalogue (system.{init,hook_started,hook_response}, stream_event,
//     assistant, result, rate_limit_event)?
//
// Skips (does not fail) when no golden fixtures are present yet — see
// testdata/README.md for why they may be pending a live `claude` capture.
func TestFrameCensus(t *testing.T) {
	files, err := filepath.Glob("testdata/golden_*.ndjson")
	if err != nil {
		t.Fatalf("glob testdata: %v", err)
	}
	if len(files) == 0 {
		t.Skip("no golden_*.ndjson fixtures in testdata/ yet — pending live claude capture, see testdata/README.md")
	}
	sort.Strings(files)

	type frameKey struct{ typ, subtype string }

	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}

		counts := map[frameKey]int{}
		var unknownTypes []string

		lineNo := 0
		for _, line := range splitNDJSONLines(data) {
			lineNo++
			if len(line) == 0 {
				continue
			}
			var probe struct {
				Type    string `json:"type"`
				Subtype string `json:"subtype"`
			}
			if err := json.Unmarshal(line, &probe); err != nil {
				t.Errorf("%s:%d: malformed JSON: %v", f, lineNo, err)
				continue
			}
			counts[frameKey{probe.Type, probe.Subtype}]++

			if ev, perr := ParseLine(line); perr == nil && ev.Unknown != nil && ev.Unknown.Type != "" {
				known := map[string]bool{
					string(EventSystem): true, string(EventAssistant): true,
					string(EventUser): true, string(EventResult): true,
					string(EventStream): true,
				}
				if !known[ev.Unknown.Type] {
					unknownTypes = append(unknownTypes, ev.Unknown.Type)
				}
			}
		}

		keys := make([]frameKey, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].typ != keys[j].typ {
				return keys[i].typ < keys[j].typ
			}
			return keys[i].subtype < keys[j].subtype
		})

		t.Logf("=== %s ===", filepath.Base(f))
		for _, k := range keys {
			label := k.typ
			if k.subtype != "" {
				label += "/" + k.subtype
			}
			t.Logf("  %-40s %d", label, counts[k])
		}
		if len(unknownTypes) > 0 {
			t.Logf("  frame types NOT in the acp.EventType set: %v", unknownTypes)
		}
	}
}

func splitNDJSONLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}
