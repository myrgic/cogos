package discord

import (
	"strconv"
	"testing"
)

// A role carrying bits Discord added after discordPermBits was written
// (47 USE_CLYDE_AI, 48 SET_VOICE_CHANNEL_STATUS, 51 PIN_MESSAGES, 52
// BYPASS_SLOWMODE) and a bit nobody knows yet (60) must survive
// bits -> names -> bits unchanged, or a fresh snapshot plans to strip them.
func TestPermBitsRoundTripIsLossless(t *testing.T) {
	for _, v := range []uint64{0, 8864258447638527, 2248473465835073, 1 << 60, 1<<60 | 1<<3} {
		s := strconv.FormatUint(v, 10)
		if got := permNamesToBits(permBitsToNames(s)); got != v {
			t.Errorf("%d: round trip gave %d", v, got)
		}
		if d := diffRolePermissions(permBitsToNames(s), s); d != "" {
			t.Errorf("%d: snapshot then diff reports %q", v, d)
		}
	}
}

// A live overwrite with allow=0 deny=0 is a no-op; convertPermOverwrites
// omits it, so the diff must not plan to remove it.
func TestZeroOverwriteIsNotARemoval(t *testing.T) {
	live := []DiscordPermOverwrite{
		{ID: "1", Type: 0, Allow: "0", Deny: "0"},
		{ID: "2", Type: 1, Allow: "0", Deny: "0"},
		{ID: "3", Type: 0, Allow: "67584", Deny: "0"},
	}
	desired := convertPermOverwrites(live, map[string]string{"3": "claw-colony"})
	if d := diffPermOverwrites(desired, live, map[string]string{"claw-colony": "3"}); len(d) != 0 {
		t.Fatalf("expected no diff after snapshot, got %+v", d)
	}
	// A real removal is still reported.
	if d := diffPermOverwrites(nil, live, nil); len(d) != 1 || d[0].TargetID != "3" {
		t.Fatalf("expected exactly the non-zero overwrite removed, got %+v", d)
	}
}
