package room

// effectFields are the item fields a reconciler reads (RFC section 6.9.3,
// "Effect-bearing item fields"). This set is the one home of the list.
var effectFields = map[string]bool{
	"section": true, "state": true, "brief": true, "text": true, "owner": true,
	"executor": true, "workflow": true, "model": true, "worktree": true,
}

// IsEffectField reports whether setting field changes what a reconciler does.
func IsEffectField(field string) bool { return effectFields[field] }

// evidenceBodies are the three self-verifying bodies of identity.evidence
// (identity A.4.2). P4-1 checks the shape only; the proofs are slice B7.
var evidenceBodies = []string{"equivocation", "post_end", "headers"}

// admit is Layer 3: a pure function of the entry, the spec chain and the
// entries this replica holds. Every replica evaluates it independently.
func (c *ctx) admit(x *cand) res {
	e := x.e

	// The spec the entry is written under.
	s := c.specByID[e.Spec]
	if s == nil {
		// A cited spec held in this call is held, never rejected (RFC
		// section 6.9.1 held spec; P4-1 round 4 R4-5).
		if r, decided := c.final[e.Spec]; decided && r.status == StatusHeld {
			return held(CodeSpecMissing)
		}
		if _, known := c.specGen[e.Spec]; known {
			return rej(CodeSpecLost)
		}
		if y := c.byID[e.Spec]; y != nil && (y.e.Kind == KindSpec || y.e.Kind == KindGenesis) {
			return rej(CodeSpecLost)
		}
		return held(CodeSpecMissing)
	}
	// Citing an older spec than one the author had seen buys nothing.
	// Only accepted chain specs count: a rejected spec entry stays in the log
	// and must not make everything that cites it stale.
	for _, t := range c.chain {
		if y := c.byID[t.ID]; y != nil && t.Gen > s.Gen && y != x && c.inPast(x, y) {
			return rej(CodeSpecStale)
		}
	}
	// A node dropped from spec.nodes is closed above its cutoff whatever spec
	// the entry cites (RFC section 6.9.1, node cutoffs).
	for _, t := range c.chain {
		if co, dropped := t.NodeCutoffs[e.Log]; dropped && e.Seq > co {
			return rej(CodeNodeRemoved)
		}
	}

	// Who is acting, and from which node.
	var (
		member Member
		before bool
	)
	if a := e.Actor; a.Node != "" {
		if !(e.Kind == KindItemSet && e.Body["field"] == "executor") && e.Kind != KindIdentityEvidence {
			return rej(CodeNotAMember)
		}
		dev, ok := s.Nodes[a.Node]
		if !ok {
			return rej(CodeNotAMember)
		}
		if dev != e.Log {
			if e.Kind == KindItemSet {
				return rej(CodeExecNotDispatch)
			}
			return rej(CodeActorNotHomed)
		}
		if e.Kind == KindItemSet && a.Node != s.Dispatch.Node {
			return rej(CodeExecNotDispatch)
		}
		if e.Attested != nil {
			return rej(CodeAttestedBad)
		}
	} else {
		m, ok := s.Members[a.MemberID()]
		if !ok || (a.Person != "" && m.Type != TypePerson) || (a.Agent != "" && m.Type != TypeAgent) {
			return rej(CodeNotAMember)
		}
		member = m
		if !c.signerHomed(s, a, m, e.Log) {
			if !c.nodeListed(s, e.Log) {
				return rej(CodeLogNotInNodes)
			}
			return rej(CodeActorNotHomed)
		}
		// A removal pins cutoffs; later writes on a cutoff log are refused
		// even if the author cites the old spec and omits the removal.
		// A re-add under the same member id lifts the cutoff only for entries
		// citing a spec at least as new as the re-adding one (RFC section
		// 6.9.1, member re-add).
		for _, t := range c.chain {
			if co, removed := t.Cutoffs[a.MemberID()]; removed {
				// A log absent from the cutoff map has no cutoff; it is not cutoff 0.
				if cut, pinned := co[e.Log]; pinned && e.Seq > cut {
					if !c.readdedBy(t, a.MemberID(), s) {
						return rej(CodeAfterRemoval)
					}
					// Above the cutoff and lifted by a re-add: new work, not
					// before the removal.
					continue
				}
				before = true
			}
		}
	}

	// Size: the entry, and the log's prefix, accepted or rejected.
	if x.size > s.EntryBodyLimit() || x.cum > s.LogLimit() {
		return rej(CodeQuotaExceeded)
	}

	// Surface attestation never speaks for an owner or an agent.
	if e.Attested != nil {
		if e.Attested.Subject != e.Actor.MemberID() {
			return rej(CodeAttestedBad)
		}
		if member.Role == RoleOwner || member.Type == TypeAgent {
			return rej(CodeOwnerViaSurface)
		}
	}

	// Heads. v1: the effective heads set is empty and no entry carries one.
	// A kind in the set needs an identity chain this slice does not verify
	// (B7); it fails closed.
	hasHead := len(e.Head) > 0 || len(e.Proof) > 0
	if e.Actor.Node != "" {
		if hasHead {
			return rej(CodeHeadUnexpected)
		}
	} else {
		in := contains(c.effectiveHeads(), string(e.Kind))
		switch {
		case in && !hasHead:
			return rej(CodeHeadMissing)
		case in:
			return rej(CodeHeadUnsupported)
		case hasHead:
			return rej(CodeHeadUnexpected)
		}
	}

	r := c.admitKind(x, s, member)
	if r.ok() {
		r.before = before
	}
	return r
}

// readdedBy reports whether the member removed by spec rm was re-added by a
// later chain spec whose gen the cited spec s has reached.
func (c *ctx) readdedBy(rm *Spec, member string, s *Spec) bool {
	for _, t := range c.chain {
		if t.Gen > rm.Gen {
			if _, in := t.Members[member]; in {
				return s.Gen >= t.Gen
			}
		}
	}
	return false
}

func (c *ctx) effectiveHeads() []string {
	var out []string
	for _, t := range c.chain {
		for _, k := range t.Identity.Heads {
			if !contains(out, k) {
				out = append(out, k)
			}
		}
	}
	return out
}

func (c *ctx) nodeListed(s *Spec, log string) bool {
	_, ok := s.NodeName(log)
	return ok
}

// signerHomed: the signing DeviceID is spec.nodes[n] for a node n in
// homes(actor) (people) or is the agent member's node.
func (c *ctx) signerHomed(s *Spec, a Actor, m Member, log string) bool {
	if a.Agent != "" {
		return s.Nodes[m.Node] == log
	}
	for _, h := range m.Homes {
		if s.Nodes[h] == log {
			return true
		}
	}
	return false
}

// item resolves the accepted item.create an entry's body names, if it is in
// the entry's causal past.
func (c *ctx) item(x *cand, id string) *cand {
	it := c.creates[id]
	if it == nil || it == x || !c.inPast(x, it) {
		return nil
	}
	return it
}

func str(e *Entry, k string) (string, bool) {
	v, ok := e.Body[k].(string)
	return v, ok && v != ""
}

// admitKind is the kind-specific half of Layer 3: the role table and the
// body checks.
func (c *ctx) admitKind(x *cand, s *Spec, m Member) res {
	e := x.e
	role := m.Role
	node := e.Actor.Node != ""

	switch e.Kind {
	case KindGenesis:
		return rej(CodeKindNotAllowed)

	case KindLine:
		if _, ok := e.Body["text"].(string); !ok {
			return rej(CodeBodyBad)
		}
		if !stringList(e.Body["to"]) {
			return rej(CodeBodyBad)
		}
		return acc()

	case KindMark:
		return acc()

	case KindItemCreate:
		bid, ok := str(e, "id")
		if !ok {
			return rej(CodeBodyBad)
		}
		own, err := ItemID(e.Log, e.Seq)
		if err != nil {
			return rej(CodeItemIDBad)
		}
		if bid != own {
			if y := c.ownIDs[bid]; y != nil && y != x && y.e.Kind == KindItemCreate {
				return rej(CodeItemIDClash)
			}
			return rej(CodeItemIDBad)
		}
		// Two creates can hold one id only by a short-form collision. Of the
		// creates that pass every other check the lowest entry id wins;
		// c.idLosers is computed before the main pass, so this is a function
		// of the set and not of arrival order.
		if c.idLosers[x.id] {
			return rej(CodeItemIDClash)
		}
		return acc()

	case KindItemAppend:
		if _, ok := str(e, "field"); !ok {
			return rej(CodeBodyBad)
		}
		id, ok := str(e, "item")
		if !ok {
			return rej(CodeBodyBad)
		}
		if c.item(x, id) == nil {
			return rej(CodeItemUnknown)
		}
		return acc()

	case KindItemSet:
		field, ok := str(e, "field")
		id, ok2 := str(e, "item")
		if _, hasV := e.Body["value"]; !ok || !ok2 || !hasV {
			return rej(CodeBodyBad)
		}
		if field == "author" {
			return rej(CodeAuthorNotSettable)
		}
		it := c.item(x, id)
		if it == nil {
			return rej(CodeItemUnknown)
		}
		if !effectFields[field] || node {
			return acc()
		}
		switch role {
		case RoleOwner:
			return acc()
		case RoleCollaborator, RoleAgent:
			if it.e.Actor == e.Actor {
				return acc()
			}
		}
		return rej(CodeEffectFieldDenied)

	case KindItemArchive:
		id, ok := str(e, "item")
		if !ok {
			return rej(CodeBodyBad)
		}
		it := c.item(x, id)
		if it == nil {
			return rej(CodeItemUnknown)
		}
		switch role {
		case RoleOwner:
		case RoleCollaborator, RoleAgent:
			if it.e.Actor != e.Actor {
				return rej(CodeRoleDenied)
			}
		default:
			return rej(CodeRoleDenied)
		}
		switch e.Body["closure"] {
		case "done":
		case "killed":
			if _, ok := str(e, "reason"); !ok {
				return rej(CodeClosureMissing)
			}
		case "handed-off":
			_, o1 := str(e, "owner")
			_, o2 := str(e, "next")
			if !o1 || !o2 {
				return rej(CodeClosureMissing)
			}
		default:
			return rej(CodeClosureMissing)
		}
		return acc()

	case KindFloorClaim:
		if role != RoleAgent {
			return rej(CodeRoleDenied)
		}
		return c.checkClaim(x, s)

	case KindApprovalAnswer:
		if role != RoleOwner {
			return rej(CodeRoleDenied)
		}
		if _, ok := str(e, "request"); !ok {
			return rej(CodeBodyBad)
		}
		if _, ok := str(e, "session"); !ok {
			return rej(CodeBodyBad)
		}
		return acc()

	case KindWorkRequest:
		switch role {
		case RoleOwner, RoleCollaborator:
			return acc()
		case RoleAgent:
			if m.Class == "full" || m.Class == "collab" {
				return acc()
			}
		}
		return rej(CodeRoleDenied)

	case KindPipelineRequest:
		p, ok := str(e, "pipeline")
		if !ok {
			return rej(CodeBodyBad)
		}
		switch role {
		case RoleOwner, RoleCollaborator:
		case RoleAgent:
			if m.Class != "collab" {
				return rej(CodeRoleDenied)
			}
		default:
			return rej(CodeRoleDenied)
		}
		if !contains(s.Scope.Pipelines, p) {
			return rej(CodePipelineUnlisted)
		}
		return acc()

	case KindSpec:
		if role != RoleOwner {
			return rej(CodeRoleDenied)
		}
		ns, err := SpecFromEntry(e)
		if err != nil {
			return rej(CodeSpecBad)
		}
		if ns.Gen != s.Gen+1 {
			return rej(CodeSpecGenBad)
		}
		if ns.Validate(s, c.chain...) != nil {
			return rej(CodeSpecBad)
		}
		if cited := c.byID[e.Spec]; cited == nil || !c.inPast(x, cited) {
			return rej(CodeSpecDepsMissing)
		}
		return acc()

	case KindIdentityEvidence:
		if !node || len(c.effectiveHeads()) == 0 {
			return rej(CodeKindNotAllowed)
		}
		n := 0
		for _, k := range evidenceBodies {
			if _, ok := e.Body[k]; ok {
				n++
			}
		}
		if n != 1 || len(e.Body) != 1 {
			return rej(CodeEvidenceBad)
		}
		return acc()
	}
	return rej(CodeKindNotAllowed)
}

func stringList(v any) bool {
	if v == nil {
		return true
	}
	l, ok := v.([]any)
	if !ok {
		return false
	}
	for _, x := range l {
		if _, ok := x.(string); !ok {
			return false
		}
	}
	return true
}
