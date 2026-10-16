package room

import (
	"math"
	"strings"
	"testing"
)

// RFC 8785 Appendix B number serialization vectors (IEEE-754 bits -> text).
func TestJCSNumberVectors(t *testing.T) {
	vectors := []struct {
		bits uint64
		want string
	}{
		{0x0000000000000000, "0"},
		{0x8000000000000000, "0"},
		{0x0000000000000001, "5e-324"},
		{0x8000000000000001, "-5e-324"},
		{0x7fefffffffffffff, "1.7976931348623157e+308"},
		{0xffefffffffffffff, "-1.7976931348623157e+308"},
		{0x4340000000000000, "9007199254740992"},
		{0xc340000000000000, "-9007199254740992"},
		{0x4430000000000000, "295147905179352830000"},
		{0x44b52d02c7e14af5, "9.999999999999997e+22"},
		{0x44b52d02c7e14af6, "1e+23"},
		{0x44b52d02c7e14af7, "1.0000000000000001e+23"},
		{0x444b1ae4d6e2ef4e, "999999999999999700000"},
		{0x444b1ae4d6e2ef4f, "999999999999999900000"},
		{0x444b1ae4d6e2ef50, "1e+21"},
		{0x3eb0c6f7a0b5ed8c, "9.999999999999997e-7"},
		{0x3eb0c6f7a0b5ed8d, "0.000001"},
		{0x41b3de4355555553, "333333333.3333332"},
		{0x41b3de4355555554, "333333333.33333325"},
		{0x41b3de4355555555, "333333333.3333333"},
		{0x41b3de4355555556, "333333333.3333334"},
		{0x41b3de4355555557, "333333333.33333343"},
		{0xbecbf647612f3696, "-0.0000033333333333333333"},
		{0x43143ff3c1cb0959, "1424953923781206.2"},
	}
	for _, v := range vectors {
		got, err := FormatNumber(math.Float64frombits(v.bits))
		if err != nil || got != v.want {
			t.Errorf("bits %016x: got %q (%v), want %q", v.bits, got, err, v.want)
		}
	}
	for _, bits := range []uint64{0x7fffffffffffffff, 0x7ff0000000000000, 0xfff0000000000000} {
		if _, err := FormatNumber(math.Float64frombits(bits)); err == nil {
			t.Errorf("bits %016x: non-finite number must be an error", bits)
		}
	}
}

// RFC 8785 section 3.2.4 sample.
func TestJCSAppendixAExample(t *testing.T) {
	in := `{"numbers":[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001],` +
		`"string":"\u20ac$\u000F\u000aA'\u0042\u0022\u005c\\\"\/","literals":[null,true,false]}`
	want := `{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],` +
		"\"string\":\"\u20ac$\\u000f\\nA'B\\\"\\\\\\\\\\\"/\"}"
	got, err := CanonicalizeJSON([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// RFC 8785 section 3.2.3: keys sort by UTF-16 code units, not UTF-8 bytes.
func TestJCSKeySortUTF16(t *testing.T) {
	in := "{\"\\u20ac\":\"Euro Sign\",\"\\r\":\"Carriage Return\",\"\\ufb33\":\"Hebrew Letter Dalet With Dagesh\"," +
		"\"1\":\"One\",\"\\ud83d\\ude00\":\"Emoji: Grinning Face\",\"\\u0080\":\"Control\",\"\\u00f6\":\"Latin Small Letter O With Diaeresis\"}"
	got, err := CanonicalizeJSON([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	order := []string{"\r", "1", "\u0080", "\u00f6", "\u20ac", "\U0001F600", "\ufb33"}
	last := -1
	s := string(got)
	for _, k := range order {
		enc := strings.Trim(mustCanon(t, k), "")
		i := strings.Index(s, enc+":")
		if i < 0 || i < last {
			t.Fatalf("key %q out of order in %s", k, s)
		}
		last = i
	}
}

func mustCanon(t *testing.T, s string) string {
	t.Helper()
	b, err := Canonicalize(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Strings are not HTML-escaped and U+2028 stays literal: the two places the
// ledger's encoding/json based canonicalJSON departs from RFC 8785.
func TestJCSStringEscapes(t *testing.T) {
	got := mustCanon(t, "<a&b>\u2028\u007f")
	if got != "\"<a&b>\u2028\u007f\"" {
		t.Fatalf("got %q", got)
	}
}

func TestJCSRejects(t *testing.T) {
	for _, in := range []string{`{"a":1,"a":2}`, `{"a":1} x`, `[1,]`, ``, `{"a":1e999}`} {
		if _, err := CanonicalizeJSON([]byte(in)); err == nil {
			t.Errorf("%q: want error", in)
		}
	}
}
