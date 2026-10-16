package room

import "testing"

// R4-6: PickWinner returned the first Default:true claim in input order, so
// two kernels holding the same claim set with two defaults could pick
// different winners. The result must not depend on input order.
func TestBoardR4_R4_6(t *testing.T) {
	prio := []string{"cog@node-a", "sage@node-b"}
	zed := Claim{Member: "zed@node-b", Default: true}
	cog := Claim{Member: "cog@node-a", Default: true}

	w1, ok1 := PickWinner([]Claim{zed, cog}, prio)
	w2, ok2 := PickWinner([]Claim{cog, zed}, prio)
	if !ok1 || !ok2 {
		t.Fatalf("expected a winner for both orders, got ok1=%v ok2=%v", ok1, ok2)
	}
	if w1 != w2 {
		t.Fatalf("order dependence: [zed,cog] gave %s, [cog,zed] gave %s", w1, w2)
	}
	if w1 != "cog@node-a" {
		t.Fatalf("expected priority winner cog@node-a, got %s", w1)
	}
}
