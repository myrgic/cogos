package engine

import (
	"strings"
	"testing"
)

// TestRecordTurnRollsUpTokenSpendMetric drives RecordTurn with a known
// usage and asserts the rendered /metrics text carries the rolled-up
// cogos_tokens_spent_total series with the expected per-kind values.
// Uses a unique provider/model label so the process-global accumulator
// does not collide with other tests.
func TestRecordTurnRollsUpTokenSpendMetric(t *testing.T) {
	const provider = "test-spend-provider"
	const model = "test-spend-model"

	p, _, sessionID := newTestProcessForTurns(t)
	turn := &TurnRecord{
		TurnID:    "tokenspend-turn-1",
		TurnIndex: 1,
		SessionID: sessionID,
		Prompt:    "hi",
		Response:  "hello",
		Provider:  provider,
		Model:     model,
		Usage:     newTurnUsage(24, 452, 14944, 278906),
	}
	if err := p.RecordTurn(turn); err != nil {
		t.Fatalf("RecordTurn: %v", err)
	}

	out := renderTokenSpendMetrics()
	for _, want := range []string{
		"# TYPE cogos_tokens_spent_total counter",
		`cogos_tokens_spent_total{provider="test-spend-provider",model="test-spend-model",kind="input"} 24`,
		`cogos_tokens_spent_total{provider="test-spend-provider",model="test-spend-model",kind="output"} 452`,
		`cogos_tokens_spent_total{provider="test-spend-provider",model="test-spend-model",kind="cache_read"} 14944`,
		`cogos_tokens_spent_total{provider="test-spend-provider",model="test-spend-model",kind="cache_write"} 278906`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered token-spend output missing %q\n--- got ---\n%s", want, out)
		}
	}

	// Counters accumulate across turns: a second turn adds, not replaces.
	turn2 := *turn
	turn2.TurnID = "tokenspend-turn-2"
	turn2.TurnIndex = 2
	turn2.Usage = newTurnUsage(1, 2, 3, 4)
	if err := p.RecordTurn(&turn2); err != nil {
		t.Fatalf("RecordTurn turn2: %v", err)
	}
	out = renderTokenSpendMetrics()
	if !strings.Contains(out, `cogos_tokens_spent_total{provider="test-spend-provider",model="test-spend-model",kind="input"} 25`) {
		t.Errorf("expected accumulated input=25 after two turns\n--- got ---\n%s", out)
	}
}

// TestRecordTurnZeroUsageAddsNoSeries verifies an errored turn with no
// provider usage does not fabricate a token-spend series.
func TestRecordTurnZeroUsageAddsNoSeries(t *testing.T) {
	const provider = "test-zero-provider"
	p, _, sessionID := newTestProcessForTurns(t)
	turn := &TurnRecord{
		TurnID:    "tokenspend-zero-1",
		TurnIndex: 1,
		SessionID: sessionID,
		Prompt:    "hi",
		Provider:  provider,
		Model:     "m",
		Status:    "error",
		Error:     "boom",
	}
	if err := p.RecordTurn(turn); err != nil {
		t.Fatalf("RecordTurn: %v", err)
	}
	if strings.Contains(renderTokenSpendMetrics(), provider) {
		t.Errorf("zero-usage errored turn must not create a series for provider %q", provider)
	}
}
