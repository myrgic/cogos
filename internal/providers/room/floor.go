package room

import (
	"sort"
	"strings"
	"unicode"
)

// Floor arbitration (RFC section 6.9.4). What lives here is the part that is
// a function of entries and the spec: the receiver-side checks on a
// floor.claim and the deterministic winner rule. The kernel `decide` route
// that produces p_me, the 3 s wait and the session plumbing are P4-11a.

// NameHit reports whether text contains the member's name as a whole word,
// case-insensitively. The name is the part of the member id before "@".
// (The RFC says receivers recompute name_hit "from L's text and the member
// names" without fixing the matcher; this is the P4-1 definition.)
func NameHit(text, memberID string) bool {
	name, _, _ := strings.Cut(memberID, "@")
	name = strings.ToLower(name)
	if name == "" {
		return false
	}
	lt := strings.ToLower(text)
	for from := 0; ; {
		i := strings.Index(lt[from:], name)
		if i < 0 {
			return false
		}
		i += from
		end := i + len(name)
		before := i == 0 || !isWordRune(lastRune(lt[:i]))
		after := end == len(lt) || !isWordRune(firstRune(lt[end:]))
		if before && after {
			return true
		}
		from = i + 1
	}
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }

func lastRune(s string) rune {
	r := []rune(s)
	return r[len(r)-1]
}

func firstRune(s string) rune {
	for _, r := range s {
		return r
	}
	return 0
}

// orderKey is the display order: hlc, then log, then seq. It is used only
// where the RFC says "last" over a causal past; it never decides validity.
func orderLess(a, b *cand) bool {
	if a.e.HLC != b.e.HLC {
		return a.e.HLC < b.e.HLC
	}
	if a.e.Log != b.e.Log {
		return a.e.Log < b.e.Log
	}
	return a.e.Seq < b.e.Seq
}

// defaultClaimant is a pure function of L's causal past and the spec: the
// agent member that last replied to L's author in this room; else the first
// agent in spec.floor.priority with a floor.claim or a line in the last 50
// entries of L's causal past; else the first agent in priority; else the
// lowest agent member id.
func (c *ctx) defaultClaimant(l *cand, s *Spec) string {
	var past []*cand
	for _, y := range c.byID {
		if y != l && c.inPast(l, y) {
			past = append(past, y)
		}
	}
	sort.Slice(past, func(a, b int) bool { return orderLess(past[a], past[b]) })

	isAgent := func(id string) bool {
		m, ok := s.Members[id]
		return ok && m.Type == TypeAgent
	}
	// 1. the last agent line replying to a line by L's author.
	for i := len(past) - 1; i >= 0; i-- {
		y := past[i]
		if y.e.Kind != KindLine || y.e.Actor.Agent == "" || !isAgent(y.e.Actor.Agent) {
			continue
		}
		rid, _ := y.e.Body["in_reply_to"].(string)
		if t := c.byID[rid]; t != nil && t.e.Kind == KindLine && t.e.Actor == l.e.Actor {
			return y.e.Actor.Agent
		}
	}
	// 2. the first in priority with a claim or a line among the last 50.
	tail := past
	if len(tail) > 50 {
		tail = tail[len(tail)-50:]
	}
	seen := map[string]bool{}
	for _, y := range tail {
		if (y.e.Kind == KindFloorClaim || y.e.Kind == KindLine) && y.e.Actor.Agent != "" {
			seen[y.e.Actor.Agent] = true
		}
	}
	for _, id := range s.Floor.Priority {
		if isAgent(id) && seen[id] {
			return id
		}
	}
	// 3. the first in priority.
	for _, id := range s.Floor.Priority {
		if isAgent(id) {
			return id
		}
	}
	// 4. the lowest member id.
	if a := s.AgentMembers(); len(a) > 0 {
		return a[0]
	}
	return ""
}

// checkClaim is what every receiver checks on a floor.claim.
func (c *ctx) checkClaim(x *cand, s *Spec) res {
	e := x.e
	lid, ok := str(e, "line")
	if !ok {
		return rej(CodeBodyBad)
	}
	def, ok1 := e.Body["default"].(bool)
	fb, ok2 := e.Body["fallback"].(bool)
	nh, ok3 := e.Body["name_hit"].(bool)
	if !ok1 || !ok2 || !ok3 {
		return rej(CodeBodyBad)
	}
	if v, has := e.Body["p_me"]; has {
		f, isNum := v.(float64)
		if !isNum || f < 0 || f > 1 {
			return rej(CodeBodyBad)
		}
		if fb {
			return rej(CodeClaimFallbackPMe)
		}
	}
	if v, has := e.Body["active_s"]; has {
		if f, isNum := v.(float64); !isNum || f < 0 {
			return rej(CodeBodyBad)
		}
	}
	l := c.byID[lid]
	if l == nil || l.e.Kind != KindLine || !c.inPast(x, l) {
		return rej(CodeClaimLineUnknown)
	}
	if lr := c.status[l.id]; !lr.ok() {
		return rej(CodeClaimLineUnknown)
	}
	if to, _ := l.e.Body["to"].([]any); len(to) > 0 {
		return rej(CodeClaimNotArbitrated)
	}
	text, _ := l.e.Body["text"].(string)
	if nh != NameHit(text, e.Actor.Agent) {
		return rej(CodeClaimNameHitWrong)
	}
	if def != (c.defaultClaimant(l, s) == e.Actor.Agent) || (def && fb) {
		return rej(CodeClaimDefaultWrong)
	}
	for _, r := range c.replies[lid] {
		if c.status[r.id].ok() && c.inPast(x, r) {
			return rej(CodeClaimAfterReply)
		}
	}
	return acc()
}

// Claim is a floor claim as the winner rule sees it.
type Claim struct {
	Member   string
	PMe      *float64 // nil: absent, counts 0
	NameHit  bool
	Default  bool
	Fallback bool
}

// PickWinner applies the deterministic rule (RFC section 6.9.4) to a set of
// valid claims, so two kernels with the same set agree:
//  1. name_hit wins outright; among several, only that subset goes on.
//  2. Higher raw p_me wins: with m the highest, the claims with p_me > m-0.05
//     go on. An absent p_me counts as 0.
//  3. The claim with default: true. If none, non-fallback claims beat fallback.
//  4. The first in spec.floor.priority; members not listed rank after those
//     that are, by lower member id.
func PickWinner(claims []Claim, priority []string) (string, bool) {
	set := append([]Claim(nil), claims...)
	if len(set) == 0 {
		return "", false
	}
	var named []Claim
	for _, cl := range set {
		if cl.NameHit {
			named = append(named, cl)
		}
	}
	if len(named) > 0 {
		set = named
	}
	pme := func(cl Claim) float64 {
		if cl.PMe == nil {
			return 0
		}
		return *cl.PMe
	}
	m := pme(set[0])
	for _, cl := range set {
		if pme(cl) > m {
			m = pme(cl)
		}
	}
	var tie []Claim
	for _, cl := range set {
		if pme(cl) > m-0.05 {
			tie = append(tie, cl)
		}
	}
	set = tie
	for _, cl := range set {
		if cl.Default {
			return cl.Member, true
		}
	}
	var firm []Claim
	for _, cl := range set {
		if !cl.Fallback {
			firm = append(firm, cl)
		}
	}
	if len(firm) > 0 {
		set = firm
	}
	rank := func(id string) int {
		for i, p := range priority {
			if p == id {
				return i
			}
		}
		return len(priority)
	}
	sort.Slice(set, func(a, b int) bool {
		ra, rb := rank(set[a].Member), rank(set[b].Member)
		if ra != rb {
			return ra < rb
		}
		return set[a].Member < set[b].Member
	})
	return set[0].Member, true
}
