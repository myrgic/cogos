package room

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"sort"

	"github.com/myrgic/cogos/pkg/substrate/bep"
)

// Status is the outcome of evaluating one entry against the set a replica holds.
type Status int

const (
	// StatusAccepted: the entry passed every layer and folds into the board.
	StatusAccepted Status = iota
	// StatusHeld: the entry waits for something it cites (a seq gap, a dep,
	// a spec, a certificate). It is neither accepted nor rejected yet.
	StatusHeld
	// StatusRejected: a function of the entry and the replicated spec chain.
	StatusRejected
	// StatusDuplicate: an entry with the same id as one already evaluated
	// (whatever its signature bytes).
	StatusDuplicate
)

func (s Status) String() string {
	return [...]string{"accepted", "held", "rejected", "duplicate"}[s]
}

// Reject and hold codes. Codes the RFC names keep the RFC spelling.
const (
	// Wire: the entry is not stored at all (it is not part of any log).
	CodeMalformed     = "entry_malformed"
	CodeLogBad        = "log_bad"
	CodeLogNonCanon   = "log_non_canonical"
	CodeLogCertWrong  = "log_cert_mismatch"
	CodeSigBad        = "sig_bad"
	CodeSigNotLowS    = "sig_not_low_s"
	CodeRoomMismatch  = "room_mismatch"
	CodeCertMissing   = "cert_missing" // held
	CodeGenesisAbsent = "genesis_missing"

	// Layer 2: log integrity.
	CodePrevBad      = "prev_bad"
	CodeDepsRegress  = "deps_regress"
	CodeDepsBad      = "deps_bad"
	CodeEquivocation = "equivocation"
	CodeSeqGap       = "seq_gap"      // held
	CodeDepsUnheld   = "deps_unheld"  // held
	CodeSpecMissing  = "spec_missing" // held

	// Layer 3: the membrane.
	CodeGenesisBad         = "genesis_bad"
	CodeGenesisOwnerUnbond = "genesis_owner_unbound"
	CodeGenesisRejected    = "genesis_rejected"
	CodeSpecStale          = "spec_stale"
	CodeSpecLost           = "spec_lost"
	CodeSpecConflict       = "spec_conflict"
	CodeSpecBad            = "spec_bad"
	CodeSpecGenBad         = "spec_gen_bad"
	CodeSpecDepsMissing    = "spec_deps_missing"
	CodeNotAMember         = "not_a_member"
	CodeActorNotHomed      = "actor_not_homed"
	CodeLogNotInNodes      = "log_not_in_nodes"
	CodeExecNotDispatch    = "exec_not_dispatch"
	CodeAfterRemoval       = "after_removal"
	CodeNodeRemoved        = "node_removed"
	CodeQuotaExceeded      = "quota_exceeded"
	CodeAttestedBad        = "attested_subject_bad"
	CodeOwnerViaSurface    = "owner_via_surface"
	CodeHeadMissing        = "head_missing"
	CodeHeadUnexpected     = "head_unexpected"
	CodeHeadUnsupported    = "head_unsupported"
	CodeKindNotAllowed     = "kind_not_allowed"
	CodeRoleDenied         = "role_denied"
	CodeEffectFieldDenied  = "effect_field_denied"
	CodeBodyBad            = "body_bad"
	CodeItemIDBad          = "item_id_bad"
	CodeItemIDClash        = "item_id_clash"
	CodeAuthorNotSettable  = "author_not_settable"
	CodeItemUnknown        = "item_unknown"
	CodeClosureMissing     = "closure_missing"
	CodePipelineUnlisted   = "pipeline_unlisted"
	CodeEvidenceBad        = "evidence_bad"

	CodeClaimNameHitWrong  = "claim_name_hit_wrong"
	CodeClaimDefaultWrong  = "claim_default_wrong"
	CodeClaimAfterReply    = "claim_after_reply"
	CodeClaimFallbackPMe   = "claim_fallback_p_me"
	CodeClaimLineUnknown   = "claim_line_unknown"
	CodeClaimNotArbitrated = "claim_not_arbitrated"
)

// Verdict is the evaluation of one input entry. Verdicts are index-aligned
// with the entries passed to Evaluate.
type Verdict struct {
	ID     string
	Log    string
	Seq    uint64
	Kind   Kind
	Status Status
	Code   string
	// BeforeRemoval marks an accepted entry at or below a removal's cutoff:
	// shown, but never acted on (the acting kernel checks the current spec).
	BeforeRemoval bool
}

// Event is a kernel event Evaluate asks its caller to emit. Room and Actor
// are the R10c fields every board event carries.
type Event struct {
	Type  string
	Room  string
	Actor string
	Data  map[string]any
}

// Event types.
const (
	EventEntryRejected   = "room.entry_rejected"
	EventLogEquivocation = "room.log_equivocation"
)

// Params are the replica-local inputs of an evaluation.
type Params struct {
	// Room is the genesis id this replica accepted. Which genesis a replica
	// accepts is a local, owner-visible decision; nothing inside a room's
	// validity reads it.
	Room string
	// Certs are the DER certificates delivered with the entries, one per log.
	// A key is used only if the certificate's hash is the log's DeviceID.
	Certs [][]byte
	// Binding decides whether the genesis' authoring node is bound to its
	// owner. nil binds no one: every genesis is refused (fails closed).
	Binding Binding
}

// Outcome is the result of evaluating a set of entries for one room.
type Outcome struct {
	Room     string
	Verdicts []Verdict // index-aligned with the input entries
	// Chain is the accepted spec chain, gen 1 first.
	Chain  []*Spec
	Events []Event
	// ItemAuthors maps item id -> actor string of its accepted item.create.
	ItemAuthors map[string]string

	byID map[string]int
}

// Verdict returns the primary verdict for an entry id.
func (o *Outcome) Verdict(id string) (Verdict, bool) {
	i, ok := o.byID[id]
	if !ok {
		return Verdict{}, false
	}
	return o.Verdicts[i], true
}

// Accepted returns the ids of accepted entries, sorted.
func (o *Outcome) Accepted() []string {
	var out []string
	for id, i := range o.byID {
		if o.Verdicts[i].Status == StatusAccepted {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

type cand struct {
	idx  int
	e    *Entry
	id   string
	size int64
	cum  int64 // bytes of this log's prefix up to and including this entry
}

type res struct {
	status Status
	code   string
	before bool
}

func acc() res                      { return res{status: StatusAccepted} }
func rej(code string) res           { return res{status: StatusRejected, code: code} }
func held(code string) res          { return res{status: StatusHeld, code: code} }
func (r res) ok() bool              { return r.status == StatusAccepted }
func (r res) withBefore(b bool) res { r.before = b; return r }

// accepted reports whether x has a stored verdict and it is accepted. The
// zero Status is StatusAccepted, so a missing verdict (an entry rejected at
// Layer 2, or one not yet reached) must never be read through c.status[...]
// directly: use this.
func (c *ctx) accepted(x *cand) bool {
	if x == nil {
		return false
	}
	r, ok := c.status[x.id]
	return ok && r.ok()
}

// ctx is the read-only view admit() runs against: the replicated spec chain
// plus the entries this replica holds.
type ctx struct {
	room     string
	byID     map[string]*cand
	byLogSeq map[string]map[uint64][]*cand
	genesis  *cand
	vecs     map[string]map[string]uint64
	chain    []*Spec
	specByID map[string]*Spec
	specGen  map[string]int
	status   map[string]res
	creates  map[string]*cand // item id -> accepted item.create
	ownIDs   map[string]*cand // <log short>-<seq> -> entry
	replies  map[string][]*cand
	devices  map[string]bool // canonical DeviceIDs of the logs held
	idLosers map[string]bool // item.create entry ids that lose an item id to a lower entry id
	// final is Evaluate's verdict table so far. admit reads it only to tell a
	// cited spec that is held in this call (held) from one rejected (spec_lost).
	final map[string]res
}

// Evaluate evaluates a SET of entries for one room. The result is a pure
// function of the set (never of arrival order): Layer 2 (signature and log
// integrity) for every entry, the spec chain, then Layer 3 (admit) for every
// entry against the spec it cites.
func Evaluate(p Params, entries []*Entry) *Outcome {
	o := &Outcome{
		Room:        p.Room,
		Verdicts:    make([]Verdict, len(entries)),
		ItemAuthors: map[string]string{},
		byID:        map[string]int{},
	}
	c := &ctx{
		room:     p.Room,
		byID:     map[string]*cand{},
		byLogSeq: map[string]map[uint64][]*cand{},
		vecs:     map[string]map[string]uint64{},
		specByID: map[string]*Spec{},
		specGen:  map[string]int{},
		status:   map[string]res{},
		creates:  map[string]*cand{},
		ownIDs:   map[string]*cand{},
		replies:  map[string][]*cand{},
		devices:  map[string]bool{},
		idLosers: map[string]bool{},
	}
	keys := keyRing(p.Certs)

	setVerdict := func(i int, id string, e *Entry, r res) {
		o.Verdicts[i] = Verdict{ID: id, Log: e.Log, Seq: e.Seq, Kind: e.Kind,
			Status: r.status, Code: r.code, BeforeRemoval: r.before}
	}

	// ---- Wire phase: shape, canonical log, certificate, signature. -----
	type pre struct {
		i  int
		e  *Entry
		id string
	}
	var order []pre
	for i, e := range entries {
		if e == nil {
			o.Verdicts[i] = Verdict{Status: StatusRejected, Code: CodeMalformed}
			continue
		}
		e = e.normalized()
		id, err := e.ID()
		if err != nil || e.validateShape() != nil {
			setVerdict(i, id, e, rej(CodeMalformed))
			if id != "" {
				if _, seen := o.byID[id]; !seen {
					o.byID[id] = i
				}
			}
			continue
		}
		order = append(order, pre{i, e, id})
	}
	// Choose the primary of a set of same-id entries independently of input
	// order: sort by (id, sig).
	sort.SliceStable(order, func(a, b int) bool {
		if order[a].id != order[b].id {
			return order[a].id < order[b].id
		}
		return order[a].e.Sig < order[b].e.Sig
	})
	var cands []*cand
	for _, pr := range order {
		e, id := pr.e, pr.id
		r := wireCheck(e, keys, p.Room, id)
		if !r.ok() {
			setVerdict(pr.i, id, e, r)
			continue
		}
		if _, dup := c.byID[id]; dup {
			setVerdict(pr.i, id, e, res{status: StatusDuplicate})
			continue
		}
		canon, _ := e.Canonical()
		x := &cand{idx: pr.i, e: e, id: id, size: int64(len(canon))}
		c.byID[id] = x
		cands = append(cands, x)
		m := c.byLogSeq[e.Log]
		if m == nil {
			m = map[uint64][]*cand{}
			c.byLogSeq[e.Log] = m
		}
		m[e.Seq] = append(m[e.Seq], x)
		c.devices[e.Log] = true
		o.byID[id] = pr.i
		if e.Kind == KindGenesis && id == p.Room {
			c.genesis = x
		}
	}
	sort.Slice(cands, func(a, b int) bool {
		ea, eb := cands[a].e, cands[b].e
		if ea.Log != eb.Log {
			return ea.Log < eb.Log
		}
		if ea.Seq != eb.Seq {
			return ea.Seq < eb.Seq
		}
		return cands[a].id < cands[b].id
	})
	// Prefix sizes per log, accepted or rejected.
	{
		var log string
		var sum int64
		for _, x := range cands {
			if x.e.Log != log {
				log, sum = x.e.Log, 0
			}
			sum += x.size
			x.cum = sum
		}
	}

	final := map[string]res{} // id -> verdict decided so far
	c.final = final
	// ---- Equivocation (Layer 2.3), a function of the set. ---------------
	equivFrom := map[string]uint64{}
	for log, m := range c.byLogSeq {
		var seqs []uint64
		for s, cs := range m {
			if len(cs) > 1 {
				seqs = append(seqs, s)
			}
		}
		if len(seqs) == 0 {
			continue
		}
		sort.Slice(seqs, func(a, b int) bool { return seqs[a] < seqs[b] })
		equivFrom[log] = seqs[0]
		var ids []string
		for _, x := range m[seqs[0]] {
			ids = append(ids, x.id)
		}
		sort.Strings(ids)
		o.Events = append(o.Events, Event{
			Type: EventLogEquivocation, Room: p.Room, Actor: "node:" + log,
			Data: map[string]any{"log": log, "seq": seqs[0], "ids": ids},
		})
	}
	for _, x := range cands {
		if from, ok := equivFrom[x.e.Log]; ok && x.e.Seq >= from {
			final[x.id] = rej(CodeEquivocation)
		}
	}

	// ---- Log integrity (Layer 2.2): prev chain, deps, regress. ----------
	resolved := map[string]bool{}
	heldWhy := map[string]string{}
	for changed := true; changed; {
		changed = false
		for _, x := range cands {
			if resolved[x.id] {
				continue
			}
			if _, done := final[x.id]; done {
				resolved[x.id] = true
				changed = true
				continue
			}
			r, ok := c.resolve(x, resolved, final)
			if !ok {
				heldWhy[x.id] = r.code
				continue
			}
			delete(heldWhy, x.id)
			resolved[x.id] = true
			changed = true
			if !r.ok() {
				final[x.id] = r
			}
		}
	}
	for _, x := range cands {
		if !resolved[x.id] {
			final[x.id] = held(heldWhy[x.id])
		}
	}

	// ---- Genesis and the spec chain. ------------------------------------
	genesisOK := false
	if c.genesis != nil && final[c.genesis.id].status != StatusRejected && resolved[c.genesis.id] {
		gr, gs := c.checkGenesis(c.genesis, p.Binding)
		if gr.ok() {
			genesisOK = true
			c.chain = []*Spec{gs}
			c.specByID[c.genesis.id] = gs
			c.specGen[c.genesis.id] = 1
			c.status[c.genesis.id] = acc()
		}
		final[c.genesis.id] = gr
	}
	if c.genesis == nil {
		// Without a genesis nothing resolves past seq 1; entries stay held.
		for _, x := range cands {
			if r, ok := final[x.id]; !ok || r.status == StatusHeld {
				final[x.id] = held(CodeGenesisAbsent)
			}
		}
	} else if !genesisOK {
		for _, x := range cands {
			if x != c.genesis {
				if r := final[x.id]; r.status != StatusRejected {
					final[x.id] = rej(CodeGenesisRejected)
				}
			}
		}
	}

	if genesisOK {
		// Spec candidates: resolved `spec` entries.
		byGen := map[int][]*cand{}
		for _, x := range cands {
			if x.e.Kind != KindSpec {
				continue
			}
			if _, has := final[x.id]; has {
				continue // rejected or held at Layer 2
			}
			sp, err := SpecFromEntry(x.e)
			if err != nil {
				final[x.id] = rej(CodeSpecBad)
				continue
			}
			c.specGen[x.id] = sp.Gen
			byGen[sp.Gen] = append(byGen[sp.Gen], x)
		}
		for g := 2; ; g++ {
			var passing []*cand
			for _, x := range byGen[g] {
				r := c.admit(x)
				if r.ok() {
					passing = append(passing, x)
				} else {
					final[x.id] = r
				}
			}
			if len(passing) == 0 {
				break
			}
			// Same-gen conflict: a head-carrying entry beats a headless one,
			// then the lower entry id wins. The others are rejected.
			sort.Slice(passing, func(a, b int) bool {
				ha, hb := len(passing[a].e.Head) > 0, len(passing[b].e.Head) > 0
				if ha != hb {
					return ha
				}
				return passing[a].id < passing[b].id
			})
			w := passing[0]
			sp, _ := SpecFromEntry(w.e)
			c.chain = append(c.chain, sp)
			c.specByID[w.id] = sp
			c.status[w.id] = acc()
			final[w.id] = acc()
			for _, l := range passing[1:] {
				final[l.id] = rej(CodeSpecConflict)
			}
		}
		// Spec entries the chain never reached.
		// Lowest gen first, so a cited spec has its verdict before the entry
		// that cites it (P4-1 round 5 R5-1). Ties keep cands order.
		var unreached []*cand
		for _, x := range cands {
			if x.e.Kind != KindSpec {
				continue
			}
			if _, has := final[x.id]; has {
				continue
			}
			unreached = append(unreached, x)
		}
		sort.SliceStable(unreached, func(a, b int) bool {
			return c.specGen[unreached[a].id] < c.specGen[unreached[b].id]
		})
		for _, x := range unreached {
			final[x.id] = c.admit(x)
		}

		// ---- Layer 3 for everything else, in causal order. --------------
		for _, x := range cands {
			if resolved[x.id] {
				c.index(x)
			}
		}
		var rest []*cand
		for _, x := range cands {
			if x.e.Kind == KindSpec || x.e.Kind == KindGenesis || !resolved[x.id] {
				continue
			}
			if r, has := final[x.id]; has && r.status != StatusAccepted {
				continue
			}
			rest = append(rest, x)
		}
		c.markIDClashes(rest)
		rank := map[string]uint64{}
		for _, x := range rest {
			var s uint64
			for _, v := range c.vec(x) {
				s += v
			}
			rank[x.id] = s
		}
		sort.SliceStable(rest, func(a, b int) bool {
			if rank[rest[a].id] != rank[rest[b].id] {
				return rank[rest[a].id] < rank[rest[b].id]
			}
			ea, eb := rest[a].e, rest[b].e
			if ea.Log != eb.Log {
				return ea.Log < eb.Log
			}
			return ea.Seq < eb.Seq
		})
		for _, x := range rest {
			r := c.admit(x)
			c.status[x.id] = r
			final[x.id] = r
			if r.ok() && x.e.Kind == KindItemCreate {
				if id, _ := x.e.Body["id"].(string); id != "" {
					c.creates[id] = x
					o.ItemAuthors[id] = x.e.Actor.String()
				}
			}
		}
	}

	// ---- Assemble. ------------------------------------------------------
	for _, x := range cands {
		r, ok := final[x.id]
		if !ok {
			r = held(CodeSpecMissing)
		}
		setVerdict(x.idx, x.id, x.e, r)
	}
	o.Chain = c.chain
	for i, v := range o.Verdicts {
		if v.Status == StatusRejected && i >= 0 {
			actor := ""
			var e *Entry
			if v.ID != "" {
				e = entries[i]
				if e != nil {
					actor = e.Actor.String()
				}
			}
			o.Events = append(o.Events, Event{
				Type: EventEntryRejected, Room: p.Room, Actor: actor,
				Data: map[string]any{"id": v.ID, "log": v.Log, "seq": v.Seq, "kind": string(v.Kind), "code": v.Code},
			})
		}
	}
	return o
}

// keyRing maps a DeviceID to the public key of the certificate that hashes to
// it. A certificate that does not carry a P-256 key is dropped.
func keyRing(ders [][]byte) map[bep.DeviceID]*ecdsa.PublicKey {
	out := map[bep.DeviceID]*ecdsa.PublicKey{}
	for _, der := range ders {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			continue
		}
		pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
		if !ok || pub.Curve != elliptic.P256() {
			continue
		}
		out[bep.DeviceIDFromCert(cert)] = pub
	}
	return out
}

// wireCheck is Layer 2 item 1: the log is the canonical spelling of a
// DeviceID whose certificate was delivered, and sig verifies as low-S.
func wireCheck(e *Entry, keys map[bep.DeviceID]*ecdsa.PublicKey, room, id string) res {
	devID, canon, err := CanonicalLog(e.Log)
	if err != nil {
		return rej(CodeLogBad)
	}
	if canon != e.Log {
		return rej(CodeLogNonCanon)
	}
	if e.Kind == KindGenesis {
		if id != room {
			return rej(CodeRoomMismatch)
		}
	} else if e.Room != room {
		return rej(CodeRoomMismatch)
	}
	pub, ok := keys[devID]
	if !ok {
		return held(CodeCertMissing)
	}
	switch err := e.VerifySig(pub); err {
	case nil:
		return acc()
	case ErrHighS:
		return rej(CodeSigNotLowS)
	default:
		return rej(CodeSigBad)
	}
}

// resolve decides whether x's predecessor and deps are in hand. ok is false
// when x must wait; otherwise the res is accepted or a Layer 2 rejection.
func (c *ctx) resolve(x *cand, resolved map[string]bool, final map[string]res) (res, bool) {
	e := x.e
	var pred *cand
	switch {
	case e.Kind == KindGenesis:
		// prev is null by shape.
	case e.Seq == 1:
		if c.genesis == nil || !resolved[c.genesis.id] {
			return held(CodeGenesisAbsent), false
		}
		if *e.Prev != c.room {
			return rej(CodePrevBad), true
		}
		pred = c.genesis
	default:
		cs := c.byLogSeq[e.Log][e.Seq-1]
		for _, p := range cs {
			if p.id == *e.Prev {
				pred = p
			}
		}
		if pred == nil {
			if len(cs) > 0 {
				return rej(CodePrevBad), true
			}
			return held(CodeSeqGap), false
		}
		if !resolved[pred.id] {
			return held(CodeSeqGap), false
		}
		// A rejected predecessor rejects its successors on the same log
		// (R4-1): the chain past a Layer-2 rejection is not admitted.
		if final[pred.id].status == StatusRejected {
			return rej(CodePrevBad), true
		}
	}
	for log, d := range e.Deps {
		if _, canon, err := CanonicalLog(log); err != nil || canon != log {
			return rej(CodeDepsBad), true
		}
		if log == e.Log && d.Seq >= e.Seq {
			return rej(CodeDepsBad), true
		}
		cs := c.byLogSeq[log][d.Seq]
		var match *cand
		for _, y := range cs {
			if y.id == d.ID {
				match = y
			}
		}
		switch {
		case match != nil:
			if !resolved[match.id] {
				return held(CodeDepsUnheld), false
			}
		case len(cs) > 0:
			return rej(CodeDepsBad), true
		default:
			return held(CodeDepsUnheld), false
		}
	}
	if pred != nil && pred.e.Kind != KindGenesis {
		for log, pd := range pred.e.Deps {
			if d, ok := e.Deps[log]; !ok || d.Seq < pd.Seq {
				return rej(CodeDepsRegress), true
			}
		}
	}
	return acc(), true
}

// vec is x's causal past as a vector: for each log, the highest seq in x's
// past. An entry y is in the past of x iff vec(x)[y.log] >= y.seq (logs are
// hash chains, so a head implies its whole prefix).
func (c *ctx) vec(x *cand) map[string]uint64 {
	if v, ok := c.vecs[x.id]; ok {
		return v
	}
	v := map[string]uint64{}
	c.vecs[x.id] = v // guards against cycles (impossible for hashes) and recursion
	merge := func(from map[string]uint64) {
		for l, s := range from {
			if s > v[l] {
				v[l] = s
			}
		}
	}
	e := x.e
	if e.Kind != KindGenesis {
		v[e.Log] = e.Seq - 1
		if e.Prev != nil {
			if pred := c.byID[*e.Prev]; pred != nil && pred != x {
				merge(c.vec(pred))
				if pred.e.Seq > v[pred.e.Log] {
					v[pred.e.Log] = pred.e.Seq
				}
			}
		}
		if c.genesis != nil && x != c.genesis {
			merge(c.vec(c.genesis))
			if v[c.genesis.e.Log] < 1 {
				v[c.genesis.e.Log] = 1
			}
		}
	}
	for log, d := range e.Deps {
		if y := c.byID[d.ID]; y != nil && y != x {
			merge(c.vec(y))
		}
		if d.Seq > v[log] {
			v[log] = d.Seq
		}
	}
	return v
}

// inPast reports whether y is in x's causal past.
func (c *ctx) inPast(x, y *cand) bool {
	return c.vec(x)[y.e.Log] >= y.e.Seq
}

// index records the short ids of an entry so item.create checks can see
// whether an id belongs to another entry.
func (c *ctx) index(x *cand) {
	if id, err := ItemID(x.e.Log, x.e.Seq); err == nil {
		c.ownIDs[id] = x
	}
	if x.e.Kind == KindLine {
		if r, _ := x.e.Body["in_reply_to"].(string); r != "" {
			c.replies[r] = append(c.replies[r], x)
		}
	}
}

// checkGenesis is the root of a room's authority.
func (c *ctx) checkGenesis(x *cand, b Binding) (res, *Spec) {
	sp, err := SpecFromEntry(x.e)
	if err != nil || sp.Validate(nil) != nil {
		return rej(CodeGenesisBad), nil
	}
	if x.e.Actor.Person == "" || x.e.Actor.Person != sp.Owner {
		return rej(CodeGenesisBad), nil
	}
	if len(x.e.Head) > 0 || len(x.e.Proof) > 0 {
		return rej(CodeHeadUnexpected), nil
	}
	if b == nil || !b.NodeBoundTo(sp.Owner, x.e.Log) {
		return rej(CodeGenesisOwnerUnbond), nil
	}
	owner := sp.Members[sp.Owner]
	homed := false
	for _, h := range owner.Homes {
		if sp.Nodes[h] == x.e.Log {
			homed = true
		}
	}
	if !homed {
		return rej(CodeGenesisOwnerUnbond), nil
	}
	return acc(), sp
}

// markIDClashes finds item.create entries that would hold an item id another
// create also holds. Each is judged on everything but the clash (admit with
// idLosers still empty); among those that pass, the lowest entry id keeps the
// id and the others lose it. Creates do not depend on each other for any other
// check, so this is independent of arrival and causal order.
func (c *ctx) markIDClashes(rest []*cand) {
	byItem := map[string][]*cand{}
	for _, x := range rest {
		if x.e.Kind != KindItemCreate {
			continue
		}
		id, _ := x.e.Body["id"].(string)
		if id == "" || !c.admit(x).ok() {
			continue
		}
		byItem[id] = append(byItem[id], x)
	}
	for _, xs := range byItem {
		if len(xs) < 2 {
			continue
		}
		sort.Slice(xs, func(a, b int) bool { return xs[a].id < xs[b].id })
		for _, l := range xs[1:] {
			c.idLosers[l.id] = true
		}
	}
}
