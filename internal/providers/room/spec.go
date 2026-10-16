package room

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Member types and roles.
const (
	TypePerson = "person"
	TypeAgent  = "agent"

	RoleOwner        = "owner"
	RoleCollaborator = "collaborator"
	RoleGuest        = "guest"
	RoleAgent        = "agent"
)

// Default quotas used when a spec declares none.
const (
	DefaultEntryBodyBytes int64 = 64 << 10
	DefaultLogBytes       int64 = 64 << 20
)

// Member is one entry of spec.members.
type Member struct {
	Type    string   `json:"type"`              // person | agent
	Role    string   `json:"role"`              // owner | collaborator | guest | agent
	Homes   []string `json:"homes,omitempty"`   // people: node names
	Node    string   `json:"node,omitempty"`    // agents: the node the agent lives on
	Class   string   `json:"class,omitempty"`   // agents: runtime class (full, collab, ...)
	Discord string   `json:"discord,omitempty"` // surface mapping
}

// Spec is the replicated room spec, carried by the genesis (gen 1) and by
// `spec` entries (gen 2, 3, ...). Only the fields admission reads are modelled.
type Spec struct {
	Name     string            `json:"name,omitempty"` // display label, genesis only
	Gen      int               `json:"gen"`
	Owner    string            `json:"owner"`
	Nodes    map[string]string `json:"nodes"` // node name -> canonical DeviceID
	Members  map[string]Member `json:"members"`
	Dispatch struct {
		Node string `json:"node,omitempty"`
	} `json:"dispatch"`
	Quota struct {
		EntryBodyBytes int64 `json:"entryBodyBytes,omitempty"`
		LogBytes       int64 `json:"logBytes,omitempty"`
	} `json:"quota"`
	Floor struct {
		Priority []string `json:"priority,omitempty"`
	} `json:"floor"`
	Scope struct {
		Pipelines []string `json:"pipelines,omitempty"`
	} `json:"scope"`
	Identity struct {
		Heads []string `json:"heads,omitempty"`
	} `json:"identity"`
	// Cutoffs pins, for each member a spec removes, the head the owner had
	// seen on every log in Nodes at removal time: member -> log -> seq.
	Cutoffs map[string]map[string]uint64 `json:"cutoffs,omitempty"`
	// NodeCutoffs pins, for each node a spec drops from Nodes, the last seq
	// of its log that still counts: DeviceID -> seq (RFC section 6.9.1).
	NodeCutoffs map[string]uint64 `json:"node_cutoffs,omitempty"`

	// ID is the id of the entry that carries this spec. Set by SpecFromEntry.
	ID string `json:"-"`
}

// SpecFromEntry decodes the spec out of a genesis or spec entry body.
func SpecFromEntry(e *Entry) (*Spec, error) {
	if e.Kind != KindGenesis && e.Kind != KindSpec {
		return nil, fmt.Errorf("%s is not a spec carrier", e.Kind)
	}
	if err := specKeysExact(e.Body); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(e.Body)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var s Spec
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	id, err := e.ID()
	if err != nil {
		return nil, err
	}
	s.ID = id
	return &s, nil
}

// specKeysExact rejects any key of a spec or genesis body that is not spelled
// exactly like a field the RFC defines, at every object level the body has.
// encoding/json matches struct fields case-insensitively (and folds Unicode,
// e.g. long s and the Kelvin sign) and merges repeated map keys, so without
// this "Members" next to "members" would mean two specs to two readers.
// nodes, cutoffs and node_cutoffs are maps keyed by data, not by field name.
func specKeysExact(body map[string]any) error {
	check := func(where string, v any, known ...string) error {
		m, ok := v.(map[string]any)
		if !ok {
			return nil // a wrong type is the decoder's error
		}
		for k := range m {
			if !contains(known, k) {
				return fmt.Errorf("%s: unknown or mis-cased key %q", where, k)
			}
		}
		return nil
	}
	if err := check("spec", body, "name", "gen", "owner", "nodes", "members", "dispatch",
		"quota", "floor", "scope", "identity", "cutoffs", "node_cutoffs"); err != nil {
		return err
	}
	if ms, ok := body["members"].(map[string]any); ok {
		for id, m := range ms {
			if err := check("member "+id, m, "type", "role", "homes", "node", "class", "discord"); err != nil {
				return err
			}
		}
	}
	for _, f := range []struct {
		name  string
		known []string
	}{
		{"dispatch", []string{"node"}},
		{"quota", []string{"entryBodyBytes", "logBytes"}},
		{"floor", []string{"priority"}},
		{"scope", []string{"pipelines"}},
		{"identity", []string{"heads"}},
	} {
		if err := check(f.name, body[f.name], f.known...); err != nil {
			return err
		}
	}
	return nil
}

// Validate is the structural check on a spec. prev is the spec it amends (nil
// for the genesis). history is every spec already in the chain (oldest first,
// prev included): a node cutoff is permanent, so none of their node_cutoffs
// DeviceIDs may be listed again (a node that comes back joins under a new
// DeviceID, RFC section 6.9.1).
func (s *Spec) Validate(prev *Spec, history ...*Spec) error {
	if prev == nil && s.Gen != 1 {
		return fmt.Errorf("genesis spec must be gen 1")
	}
	if prev != nil && s.Gen != prev.Gen+1 {
		return fmt.Errorf("gen %d does not follow %d", s.Gen, prev.Gen)
	}
	own, ok := s.Members[s.Owner]
	if s.Owner == "" || !ok || own.Role != RoleOwner || own.Type != TypePerson {
		return fmt.Errorf("owner must be a person member with role owner")
	}
	if prev != nil && prev.Owner != s.Owner {
		return fmt.Errorf("owner cannot change in place")
	}
	for name, dev := range s.Nodes {
		if _, canon, err := CanonicalLog(dev); err != nil || canon != dev {
			return fmt.Errorf("node %q: device id not canonical", name)
		}
	}
	for id, m := range s.Members {
		switch m.Type {
		case TypePerson:
			if m.Role != RoleOwner && m.Role != RoleCollaborator && m.Role != RoleGuest {
				return fmt.Errorf("member %q: bad role %q", id, m.Role)
			}
			// A room has exactly one owner: the one spec.owner names
			// (cog-review on 824b74b).
			if m.Role == RoleOwner && id != s.Owner {
				return fmt.Errorf("member %q: only spec.owner %q may hold the owner role", id, s.Owner)
			}
			if len(m.Homes) == 0 || m.Node != "" {
				return fmt.Errorf("member %q: a person has homes and no node", id)
			}
			for _, h := range m.Homes {
				if _, ok := s.Nodes[h]; !ok {
					return fmt.Errorf("member %q: home %q not in nodes", id, h)
				}
			}
		case TypeAgent:
			if m.Role != RoleAgent {
				return fmt.Errorf("member %q: an agent has role agent", id)
			}
			name, node, ok := strings.Cut(id, "@")
			if !ok || name == "" || node != m.Node {
				return fmt.Errorf("member %q: agent id must be name@node", id)
			}
			if _, ok := s.Nodes[m.Node]; !ok {
				return fmt.Errorf("member %q: node %q not in nodes", id, m.Node)
			}
			if len(m.Homes) != 0 {
				return fmt.Errorf("member %q: an agent has a node, not homes", id)
			}
		default:
			return fmt.Errorf("member %q: bad type %q", id, m.Type)
		}
	}
	if d := s.Dispatch.Node; d != "" {
		if _, ok := s.Nodes[d]; !ok {
			return fmt.Errorf("dispatch node %q not in nodes", d)
		}
	}
	if s.Quota.EntryBodyBytes < 0 || s.Quota.LogBytes < 0 {
		return fmt.Errorf("negative quota")
	}
	if len(s.Identity.Heads) > 0 && !contains(s.Identity.Heads, string(KindSpec)) {
		return fmt.Errorf("a non-empty identity.heads must list spec")
	}
	for _, h := range append(append([]*Spec(nil), history...), prev) {
		if h == nil {
			continue
		}
		for dev := range h.NodeCutoffs {
			if containsValue(s.Nodes, dev) {
				return fmt.Errorf("node %q was cut off at gen %d and cannot rejoin (new DeviceID)", dev, h.Gen)
			}
		}
	}
	if prev == nil && (len(s.Cutoffs) > 0 || len(s.NodeCutoffs) > 0) {
		return fmt.Errorf("a genesis carries no cutoffs or node_cutoffs")
	}
	if prev != nil {
		// A cutoffs key names a member the previous spec has and this one removes.
		for id := range s.Cutoffs {
			_, was := prev.Members[id]
			_, still := s.Members[id]
			if !was || still {
				return fmt.Errorf("cutoffs: %q is not a member this spec removes", id)
			}
		}
		// A removal pins cutoffs on every log in the previous spec's nodes.
		for id := range prev.Members {
			if _, still := s.Members[id]; still {
				continue
			}
			c, ok := s.Cutoffs[id]
			if !ok {
				return fmt.Errorf("removed member %q has no cutoffs", id)
			}
			for _, dev := range prev.Nodes {
				if _, ok := c[dev]; !ok {
					return fmt.Errorf("removed member %q: no cutoff for log %s", id, dev)
				}
			}
		}
		// A dropped node pins the last seq of its log that still counts.
		for name, dev := range prev.Nodes {
			still := false
			for _, d := range s.Nodes {
				if d == dev {
					still = true
				}
			}
			if still {
				continue
			}
			if _, ok := s.NodeCutoffs[dev]; !ok {
				return fmt.Errorf("dropped node %q has no node_cutoffs entry", name)
			}
		}
		for dev := range s.NodeCutoffs {
			if _, canon, err := CanonicalLog(dev); err != nil || canon != dev {
				return fmt.Errorf("node_cutoffs: %q is not a canonical device id", dev)
			}
			// Only a node the previous spec listed and this one drops.
			if !containsValue(prev.Nodes, dev) {
				return fmt.Errorf("node_cutoffs: %q is not a node of the previous spec", dev)
			}
			if containsValue(s.Nodes, dev) {
				return fmt.Errorf("node_cutoffs: %q is still listed in nodes", dev)
			}
		}
		// The effective heads set never shrinks in place.
		for _, k := range prev.Identity.Heads {
			if !contains(s.Identity.Heads, k) {
				return fmt.Errorf("identity.heads cannot shrink (new room instead)")
			}
		}
	}
	return nil
}

// EntryBodyLimit is the per-entry size cap, with the default applied.
func (s *Spec) EntryBodyLimit() int64 {
	if s.Quota.EntryBodyBytes > 0 {
		return s.Quota.EntryBodyBytes
	}
	return DefaultEntryBodyBytes
}

// LogLimit is the per-log byte cap, with the default applied.
func (s *Spec) LogLimit() int64 {
	if s.Quota.LogBytes > 0 {
		return s.Quota.LogBytes
	}
	return DefaultLogBytes
}

// NodeName returns the name a DeviceID has in spec.nodes.
func (s *Spec) NodeName(device string) (string, bool) {
	for n, d := range s.Nodes {
		if d == device {
			return n, true
		}
	}
	return "", false
}

// Homes is homes(R): the union of every member's homes or node plus the
// owner's homes, as node names, sorted. A peer outside it never receives the
// room (RFC section 6.9.1, "Who receives a room").
func (s *Spec) Homes() []string {
	set := map[string]bool{}
	for _, m := range s.Members {
		for _, h := range m.Homes {
			set[h] = true
		}
		if m.Node != "" {
			set[m.Node] = true
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// HomeDevices is Homes as canonical DeviceIDs, sorted.
func (s *Spec) HomeDevices() []string {
	var out []string
	for _, n := range s.Homes() {
		if d, ok := s.Nodes[n]; ok {
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

// AgentMembers returns the agent member ids, sorted.
func (s *Spec) AgentMembers() []string {
	var out []string
	for id, m := range s.Members {
		if m.Type == TypeAgent {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func containsValue(m map[string]string, v string) bool {
	for _, x := range m {
		if x == v {
			return true
		}
	}
	return false
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// Binding says whether a peer is bound to a person at enrolment (the
// `person` field of the enrolment record, RFC section 6.9.3 Genesis). The
// real record does not exist yet (a B2 prerequisite); P4-1 runs against this
// interface and a stub. A peer with no person field binds to no one.
type Binding interface {
	NodeBoundTo(person, deviceID string) bool
}

// StubBinding is the test and P4-1 binding: person -> DeviceIDs.
type StubBinding map[string][]string

// NodeBoundTo implements Binding.
func (b StubBinding) NodeBoundTo(person, deviceID string) bool {
	return contains(b[person], deviceID)
}

// LabelClash reports whether two geneses are different rooms with the same
// display name. Neither invalidates the other (RFC section 6.9.3 Genesis);
// the owner is shown board.label_clash.
func LabelClash(a, b *Entry) bool {
	if a.Kind != KindGenesis || b.Kind != KindGenesis {
		return false
	}
	ia, _ := a.ID()
	ib, _ := b.ID()
	na, _ := a.Body["name"].(string)
	nb, _ := b.Body["name"].(string)
	return ia != ib && na != "" && na == nb
}
