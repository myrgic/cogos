package room

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"

	"github.com/myrgic/cogos/pkg/substrate/bep"
)

// Domain is the fixed string that separates the use of a node's BEP device
// key for room entries from its other uses (NodeRecords, the TLS handshake).
const Domain = "cogos.room.entry.v1\n"

// MaxSafeInt is the largest integer a JCS number carries exactly.
const MaxSafeInt = 1<<53 - 1

// Kind is the type of a board entry (RFC section 6.9.1).
type Kind string

const (
	KindLine             Kind = "line"
	KindItemCreate       Kind = "item.create"
	KindItemSet          Kind = "item.set"
	KindItemAppend       Kind = "item.append"
	KindItemArchive      Kind = "item.archive"
	KindMark             Kind = "mark"
	KindFloorClaim       Kind = "floor.claim"
	KindApprovalAnswer   Kind = "approval.answer"
	KindWorkRequest      Kind = "work.request"
	KindPipelineRequest  Kind = "pipeline.request"
	KindSpec             Kind = "spec"
	KindGenesis          Kind = "genesis"
	KindIdentityEvidence Kind = "identity.evidence"
)

var allKinds = map[Kind]bool{
	KindLine: true, KindItemCreate: true, KindItemSet: true, KindItemAppend: true,
	KindItemArchive: true, KindMark: true, KindFloorClaim: true, KindApprovalAnswer: true,
	KindWorkRequest: true, KindPipelineRequest: true, KindSpec: true, KindGenesis: true,
	KindIdentityEvidence: true,
}

// Actor is a member id and nothing else: {person}, {agent: "name@node"} or,
// for the two kernel-authored kinds only, {node} (RFC section 6.9.1). Exactly
// one field is set. This is the R10c actor field.
type Actor struct {
	Person string `json:"person,omitempty"`
	Agent  string `json:"agent,omitempty"`
	Node   string `json:"node,omitempty"`
}

// Valid reports whether exactly one field is set.
func (a Actor) Valid() bool {
	n := 0
	for _, s := range []string{a.Person, a.Agent, a.Node} {
		if s != "" {
			n++
		}
	}
	return n == 1
}

// MemberID is the person id or agent id; "" for a {node} actor.
func (a Actor) MemberID() string {
	if a.Person != "" {
		return a.Person
	}
	return a.Agent
}

// String is the form used in kernel events (R10c): person:vroku,
// agent:cog@node-b, node:node-b.
func (a Actor) String() string {
	switch {
	case a.Person != "":
		return "person:" + a.Person
	case a.Agent != "":
		return "agent:" + a.Agent
	case a.Node != "":
		return "node:" + a.Node
	}
	return ""
}

// Dep names the head of one log the author had accepted: its seq and entry id.
type Dep struct {
	Seq uint64 `json:"seq"`
	ID  string `json:"id"`
}

// Attested is present only when a multi-person surface attested the speaker.
type Attested struct {
	Subject string `json:"subject"`
	Surface string `json:"surface"`
	By      string `json:"by,omitempty"`
}

// Entry is one board entry. A room's board is the union of per-node
// append-only logs of entries.
type Entry struct {
	Room     string          `json:"room,omitempty"` // genesis id; absent only on the genesis entry itself
	Log      string          `json:"log"`            // FormatDeviceID of the authoring node
	Seq      uint64          `json:"seq"`
	Prev     *string         `json:"prev"` // null only on the genesis
	Deps     map[string]Dep  `json:"deps"`
	Spec     string          `json:"spec,omitempty"` // entry id of the spec entry in force; absent on the genesis
	Actor    Actor           `json:"actor"`
	Attested *Attested       `json:"attested,omitempty"`
	Kind     Kind            `json:"kind"`
	Body     map[string]any  `json:"body"`
	HLC      string          `json:"hlc"`
	Head     json.RawMessage `json:"head,omitempty"`  // reserved: identity heads (slice B7)
	Proof    json.RawMessage `json:"proof,omitempty"` // reserved: identity heads (slice B7)
	Sig      string          `json:"sig,omitempty"`   // 128 lowercase hex: r || s, low-S
}

var (
	hex64Re = regexp.MustCompile(`^[0-9a-f]{64}$`)
	hlcRe   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z/\d+$`)
	sigRe   = regexp.MustCompile(`^[0-9a-f]{128}$`)
)

// ErrMalformed is returned for entries that fail the structural check.
var ErrMalformed = errors.New("entry malformed")

// ParseEntry decodes one wire entry. Unknown fields and duplicate keys are
// rejected, so one byte string cannot mean two entries.
func ParseEntry(raw []byte) (*Entry, error) {
	v, err := parseStrict(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := exactKeys(v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := exactValues(v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := exactNumbers(v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var e Entry
	if err := dec.Decode(&e); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if err := e.validateShape(); err != nil {
		return nil, err
	}
	return &e, nil
}

// exactNumbers keeps every number on the wire exact in the float64 that
// decoding produces. Without it, two byte strings that differ only in a body
// integer above 2^53 (or in digits a float64 cannot hold) collapse to one
// value and hash to one id. A number is accepted only when its literal is the
// shortest round-trip form of its float64, and integers stay within 2^53-1.
func exactNumbers(v any) error {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return fmt.Errorf("number %s: %v", x, err)
		}
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return fmt.Errorf("number %s is not finite", x)
		}
		if !strings.ContainsAny(string(x), ".eE") && math.Abs(f) > MaxSafeInt {
			return fmt.Errorf("integer %s is above 2^53-1", x)
		}
		s, err := FormatNumber(f)
		if err != nil || s != string(x) {
			return fmt.Errorf("number %s does not round-trip exactly (canonical %s)", x, s)
		}
	case map[string]any:
		for _, m := range x {
			if err := exactNumbers(m); err != nil {
				return err
			}
		}
	case []any:
		for _, m := range x {
			if err := exactNumbers(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// exactKeys rejects any key of the entry, its actor, its attested block or a
// deps value that is not spelled exactly like a known field. encoding/json
// matches struct fields case-insensitively and merges repeats, so without this
// "KIND" and "kind" could carry two meanings for one byte string. Body keys are
// free-form map keys (case-sensitive already); head and proof are opaque.
func exactKeys(v any) error {
	top, ok := v.(map[string]any)
	if !ok {
		return errors.New("entry is not an object")
	}
	check := func(where string, m map[string]any, known ...string) error {
		for k := range m {
			if !contains(known, k) {
				return fmt.Errorf("%s: unknown or mis-cased key %q", where, k)
			}
		}
		return nil
	}
	if err := check("entry", top, "room", "log", "seq", "prev", "deps", "spec", "actor",
		"attested", "kind", "body", "hlc", "head", "proof", "sig"); err != nil {
		return err
	}
	if m, ok := top["actor"].(map[string]any); ok {
		if err := check("actor", m, "person", "agent", "node"); err != nil {
			return err
		}
	}
	if m, ok := top["attested"].(map[string]any); ok {
		if err := check("attested", m, "subject", "surface", "by"); err != nil {
			return err
		}
	}
	if deps, ok := top["deps"].(map[string]any); ok {
		for _, d := range deps {
			if m, ok := d.(map[string]any); ok {
				if err := check("deps value", m, "seq", "id"); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// exactValues rejects null or empty-string values for known fields. omitempty
// drops a nil pointer, an empty string and an empty value alike, and ID() runs
// over the re-marshaled struct, so without this "attested":null,
// "actor":{"agent":""} and "room":"" would hash to the same id as the clean
// form. prev is the one nullable field (null on the genesis). body is
// free-form, head and proof are opaque, and deps is normalized by Canonical.
func exactValues(v any) error {
	top, ok := v.(map[string]any)
	if !ok {
		return errors.New("entry is not an object")
	}
	nonEmpty := func(where string, m map[string]any, keys ...string) error {
		for _, k := range keys {
			x, present := m[k]
			if !present {
				continue
			}
			if s, isStr := x.(string); !isStr || s == "" {
				return fmt.Errorf("%s: %q must be a non-empty string", where, k)
			}
		}
		return nil
	}
	for _, k := range []string{"actor", "attested", "deps"} {
		if x, present := top[k]; present && x == nil {
			return fmt.Errorf("entry: %q must not be null", k)
		}
	}
	if err := nonEmpty("entry", top, "room", "log", "spec", "kind", "hlc", "sig"); err != nil {
		return err
	}
	if x := top["prev"]; x != nil {
		if err := nonEmpty("entry", top, "prev"); err != nil {
			return err
		}
	}
	if m, ok := top["actor"].(map[string]any); ok {
		if err := nonEmpty("actor", m, "person", "agent", "node"); err != nil {
			return err
		}
	}
	if m, ok := top["attested"].(map[string]any); ok {
		if err := nonEmpty("attested", m, "subject", "surface", "by"); err != nil {
			return err
		}
	}
	return nil
}

// validateShape is the structural check every replica runs before anything
// else. It reads no spec.
func (e *Entry) validateShape() error {
	bad := func(f string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(f, a...))
	}
	if !allKinds[e.Kind] {
		return bad("unknown kind %q", e.Kind)
	}
	if e.Seq < 1 || e.Seq > MaxSafeInt {
		return bad("seq out of range")
	}
	if !e.Actor.Valid() {
		return bad("actor must name exactly one of person, agent, node")
	}
	if e.Log == "" {
		return bad("log missing")
	}
	if !hlcRe.MatchString(e.HLC) {
		return bad("hlc")
	}
	if e.Body == nil {
		return bad("body missing")
	}
	if e.Kind == KindGenesis {
		if e.Room != "" || e.Spec != "" {
			return bad("genesis carries neither room nor spec")
		}
		if e.Prev != nil {
			return bad("genesis prev must be null")
		}
		if e.Seq != 1 {
			return bad("genesis seq must be 1")
		}
	} else {
		if !hex64Re.MatchString(e.Room) {
			return bad("room")
		}
		if !hex64Re.MatchString(e.Spec) {
			return bad("spec")
		}
		if e.Prev == nil || !hex64Re.MatchString(*e.Prev) {
			return bad("prev")
		}
	}
	for log, d := range e.Deps {
		if log == "" || !hex64Re.MatchString(d.ID) || d.Seq < 1 || d.Seq > MaxSafeInt {
			return bad("deps entry for %q", log)
		}
	}
	if e.Attested != nil && (e.Attested.Subject == "" || e.Attested.Surface == "") {
		return bad("attested")
	}
	if e.Sig != "" && !sigRe.MatchString(e.Sig) {
		return bad("sig must be 128 lowercase hex")
	}
	return nil
}

// Canonical returns JCS(entry without sig), the bytes the id and the
// signature cover.
func (e *Entry) Canonical() ([]byte, error) {
	c := *e
	c.Sig = ""
	if c.Deps == nil {
		c.Deps = map[string]Dep{}
	}
	if c.Body == nil {
		c.Body = map[string]any{}
	}
	return Canonicalize(&c)
}

// ID is SHA-256(D || JCS(entry without sig)) as lowercase hex. The signature
// is excluded, so a malleated twin of an entry has the same id.
func (e *Entry) ID() (string, error) {
	c, err := e.Canonical()
	if err != nil {
		return "", err
	}
	return idOf(c), nil
}

func idOf(canonical []byte) string {
	h := sha256.New()
	h.Write([]byte(Domain))
	h.Write(canonical)
	return hex.EncodeToString(h.Sum(nil))
}

// Signature errors.
var (
	ErrSigFormat = errors.New("signature not 128 lowercase hex")
	ErrHighS     = errors.New("signature is not low-S")
	ErrBadSig    = errors.New("signature does not verify")
)

var (
	curveN     = elliptic.P256().Params().N
	curveHalfN = new(big.Int).Rsh(curveN, 1)
)

// Sign sets e.Sig: a low-S ECDSA P-256 signature, r || s as 128 lowercase
// hex, over D || JCS(entry without sig).
func (e *Entry) Sign(priv *ecdsa.PrivateKey) error {
	c, err := e.Canonical()
	if err != nil {
		return err
	}
	digest := sha256.Sum256(append([]byte(Domain), c...))
	der, err := ecdsa.SignASN1(rand.Reader, priv, digest[:])
	if err != nil {
		return err
	}
	var rs struct{ R, S *big.Int }
	if _, err := asn1.Unmarshal(der, &rs); err != nil {
		return err
	}
	if rs.S.Cmp(curveHalfN) > 0 {
		rs.S = new(big.Int).Sub(curveN, rs.S)
	}
	var buf [64]byte
	rs.R.FillBytes(buf[:32])
	rs.S.FillBytes(buf[32:])
	e.Sig = hex.EncodeToString(buf[:])
	return nil
}

// VerifySig checks e.Sig against pub. A high-S signature is refused even if
// it verifies (ErrHighS): ECDSA is malleable and Go does not enforce low-S.
func (e *Entry) VerifySig(pub *ecdsa.PublicKey) error {
	if !sigRe.MatchString(e.Sig) {
		return ErrSigFormat
	}
	raw, _ := hex.DecodeString(e.Sig)
	r := new(big.Int).SetBytes(raw[:32])
	s := new(big.Int).SetBytes(raw[32:])
	if r.Sign() == 0 || s.Sign() == 0 || r.Cmp(curveN) >= 0 || s.Cmp(curveN) >= 0 {
		return ErrBadSig
	}
	c, err := e.Canonical()
	if err != nil {
		return err
	}
	digest := sha256.Sum256(append([]byte(Domain), c...))
	der, err := asn1.Marshal(struct{ R, S *big.Int }{r, s})
	if err != nil {
		return err
	}
	if !ecdsa.VerifyASN1(pub, digest[:], der) {
		return ErrBadSig
	}
	if s.Cmp(curveHalfN) > 0 {
		return ErrHighS
	}
	return nil
}

// CanonicalLog parses a log string leniently and returns the DeviceID and its
// canonical spelling. The entry is canonical only if the spelling matches
// byte for byte (bep.ParseDeviceID tolerates dashes, case and wrong Luhn
// characters, so one key has many spellings).
func CanonicalLog(s string) (bep.DeviceID, string, error) {
	id, err := bep.ParseDeviceID(s)
	if err != nil {
		return id, "", err
	}
	return id, bep.FormatDeviceID(id), nil
}

// logShortBytes is how many leading DeviceID bytes the short form carries.
// A variable only so tests can shrink it to build real collisions.
var logShortBytes = 6

// LogShort is the short form of a log used in item ids: the first six bytes
// of the DeviceID as lowercase hex. (The RFC says "canonical log short"
// without defining it; this is the P4-1 definition.)
func LogShort(log string) (string, error) {
	id, canon, err := CanonicalLog(log)
	if err != nil {
		return "", err
	}
	if canon != log {
		return "", fmt.Errorf("log not canonical")
	}
	return hex.EncodeToString(id[:logShortBytes]), nil
}

// ItemID is the id an item.create entry must carry: <log short>-<seq>.
func ItemID(log string, seq uint64) (string, error) {
	sh, err := LogShort(log)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d", sh, seq), nil
}

// normalized returns a copy whose body has passed through JSON, so that
// admission sees the same Go types (float64, []any, map[string]any) whether
// the entry came off the wire or was built in memory.
func (e *Entry) normalized() *Entry {
	c := *e
	raw, err := json.Marshal(e.Body)
	if err != nil {
		c.Body = nil
		return &c
	}
	var b map[string]any
	if err := json.Unmarshal(raw, &b); err != nil {
		c.Body = nil
		return &c
	}
	c.Body = b
	return &c
}
