package room

import (
	"sort"
	"testing"
)

// R4-3: orderLess compared HLC strings lexically. The grammar allows an
// unpadded counter, so "/9" sorted after "/10". Display order must compare
// the counter numerically.
func TestBoardR4_R4_3(t *testing.T) {
	const ts = "2026-10-16T10:00:00.000Z"
	mk := func(counter string, log string) *cand {
		return &cand{e: &Entry{HLC: ts + "/" + counter, Log: log, Seq: 1}}
	}
	cases := [][2]string{{"9", "10"}, {"99", "100"}, {"1", "2"}}
	for _, c := range cases {
		lo, hi := mk(c[0], "a"), mk(c[1], "a")
		if !orderLess(lo, hi) {
			t.Fatalf("counter %s should order before %s", c[0], c[1])
		}
		if orderLess(hi, lo) {
			t.Fatalf("counter %s should not order before %s", c[1], c[0])
		}
	}
	xs := []*cand{mk("100", "a"), mk("9", "a"), mk("10", "a"), mk("99", "a")}
	sort.Slice(xs, func(i, j int) bool { return orderLess(xs[i], xs[j]) })
	want := []string{"9", "10", "99", "100"}
	for i, x := range xs {
		if got := x.e.HLC[len(ts)+1:]; got != want[i] {
			t.Fatalf("position %d: got counter %s want %s", i, got, want[i])
		}
	}
}
