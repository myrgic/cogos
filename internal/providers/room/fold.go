package room

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// Board fold (RFC rooms-as-declared-resources section 6.9.2, slice B3,
// stage-into-kernel slice P4-2). The fold is a pure function of the SET of
// accepted item.* entries: entries are applied in a causal order that is
// itself a function of the set (causal vector sum, then entry id), and no
// clock or arrival order decides anything.

// ConflictMark names why a field or item shows more than one truth (6.9.2).
type ConflictMark string

const (
	// MarkNone: a single value, or no value.
	MarkNone ConflictMark = ""
	// MarkFieldConflict: two sets of one field, neither in the other's deps.
	MarkFieldConflict ConflictMark = "field_conflict"
	// MarkConcurrentEdit: an archive concurrent with a set.
	MarkConcurrentEdit ConflictMark = "concurrent_edit"
)

// FieldValue is one value of a multi-value register with its writer.
type FieldValue struct {
	Value  any
	Author string // Actor.String() of the writing entry
	Entry  string // entry id of the item.set
	log    string
	seq    uint64
}

// Field is a multi-value register: the maximal (causally unsuperseded) sets
// of one field. More than one value means Mark is MarkFieldConflict.
type Field struct {
	Values []FieldValue
	Mark   ConflictMark
}

// Appended is one element of a list field (comments, checklist).
type Appended struct {
	Value  any
	Author string
	Entry  string
	HLC    string
	Log    string
	Seq    uint64
}

// Closure is the body.closure of an accepted item.archive (6.9.2).
type Closure struct {
	Kind   string // done | killed | handed-off
	Reason string
	Owner  string
	Next   string
}

// Item is the fold of every accepted item.* entry naming one item id.
type Item struct {
	ID      string
	Author  string // derived: actor of the accepted item.create, immutable
	Fields  map[string]Field
	Lists   map[string][]Appended
	History []string // entry ids of every accepted item.* entry, causal order
	// Archived is true once an accepted item.archive exists.
	Archived bool
	Closure  *Closure
	// Mark is MarkConcurrentEdit when an archive was concurrent with a set.
	Mark ConflictMark

	sets     []vecRef // every applied item.set, for concurrency against an archive
	archives []vecRef // every applied item.archive
}

// vecRef is an applied entry with its causal vector.
type vecRef struct {
	e   *Entry
	vec map[string]uint64
}

// Held is an item.* entry kept out of the fold until its deps arrive
// (forward-dated set, 6.9.2 and Layer 2 rule 2).
type Held struct {
	Entry string
	Code  string
}

// Board is the fold result for one room.
type Board struct {
	Room  string
	Items map[string]*Item
	Held  []Held
}

// entryLogSeq keys an entry by its position in its log.
type entryLogSeq struct {
	log string
	seq uint64
}

// causalVectors computes, for each accepted entry, its causal vector: for
// every log, the highest seq in the entry's causal past (itself included).
// The past is the entry's own log prefix plus the transitive past of every
// head it cites in deps. A head not held here still counts at its cited seq.
func causalVectors(idx map[entryLogSeq]*Entry, ids map[*Entry]string) map[*Entry]map[string]uint64 {
	memo := map[*Entry]map[string]uint64{}
	var vec func(e *Entry) map[string]uint64
	vec = func(e *Entry) map[string]uint64 {
		if v, ok := memo[e]; ok {
			return v
		}
		v := map[string]uint64{e.Log: e.Seq}
		memo[e] = v // an entry cannot cite itself (hash chain); guards a cycle regardless
		merge := func(o map[string]uint64) {
			for l, s := range o {
				if s > v[l] {
					v[l] = s
				}
			}
		}
		if e.Seq > 1 {
			if p := idx[entryLogSeq{e.Log, e.Seq - 1}]; p != nil {
				merge(vec(p))
			}
		}
		for l, d := range e.Deps {
			if d.Seq > v[l] {
				v[l] = d.Seq
			}
			if p := idx[entryLogSeq{l, d.Seq}]; p != nil && p != e {
				merge(vec(p))
			}
		}
		return v
	}
	for e := range ids {
		vec(e)
	}
	return memo
}

func vecSum(v map[string]uint64) uint64 {
	var n uint64
	for _, s := range v {
		n += s
	}
	return n
}

// FoldBoard folds the accepted item.* entries of an evaluated room into a
// Board. Section 6.9.2: the fold is a pure function of the set of accepted
// entries in causal order (deps); the same set gives the same Board in any
// delivery order. Only entries with StatusAccepted in out are folded.
func FoldBoard(out *Outcome, entries []*Entry) *Board {
	b := &Board{Items: map[string]*Item{}}
	if out == nil {
		return b
	}
	b.Room = out.Room

	ids := map[*Entry]string{} // accepted entries -> id
	idx := map[entryLogSeq]*Entry{}
	// Verdicts are looked up by entry id, not by position, so the order and
	// duplication of the slice passed in cannot change the fold.
	seen := map[string]bool{}
	for _, e := range entries {
		if e == nil {
			continue
		}
		id := entryID(e)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		v, ok := out.Verdict(id)
		if !ok {
			continue
		}
		switch v.Status {
		case StatusAccepted:
			ids[e] = id
			idx[entryLogSeq{e.Log, e.Seq}] = e
		case StatusHeld:
			if isItemKind(e.Kind) {
				b.Held = append(b.Held, Held{Entry: id, Code: v.Code})
			}
		}
	}
	sort.Slice(b.Held, func(i, j int) bool {
		if b.Held[i].Entry != b.Held[j].Entry {
			return b.Held[i].Entry < b.Held[j].Entry
		}
		return b.Held[i].Code < b.Held[j].Code
	})

	vecs := causalVectors(idx, ids)
	var items []*Entry
	for e := range ids {
		if isItemKind(e.Kind) {
			items = append(items, e)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		si, sj := vecSum(vecs[items[i]]), vecSum(vecs[items[j]])
		if si != sj {
			return si < sj
		}
		return ids[items[i]] < ids[items[j]]
	})

	for _, e := range items {
		id, _ := e.Body["id"].(string)
		if e.Kind != KindItemCreate {
			id, _ = e.Body["item"].(string)
		}
		if id == "" {
			continue
		}
		it := b.Items[id]
		if e.Kind == KindItemCreate {
			if it != nil {
				continue // a clash loser is rejected upstream; keep the first
			}
			it = &Item{ID: id, Author: e.Actor.String(),
				Fields: map[string]Field{}, Lists: map[string][]Appended{}}
			b.Items[id] = it
			it.History = append(it.History, ids[e])
			for k, val := range e.Body {
				if k == "id" || k == "author" {
					continue
				}
				it.Fields[k] = Field{Values: []FieldValue{{
					Value: val, Author: e.Actor.String(), Entry: ids[e], log: e.Log, seq: e.Seq}}}
			}
			continue
		}
		if it == nil {
			continue
		}
		it.History = append(it.History, ids[e])
		switch e.Kind {
		case KindItemSet:
			applySet(it, e, ids[e], vecs[e])
		case KindItemAppend:
			applyAppend(it, e, ids[e])
		case KindItemArchive:
			applyArchive(it, e, vecs[e])
		}
	}
	return b
}

func isItemKind(k Kind) bool {
	return k == KindItemCreate || k == KindItemSet || k == KindItemAppend || k == KindItemArchive
}

// entryID is the entry's id; "" if it cannot be computed.
func entryID(e *Entry) string {
	id, err := e.ID()
	if err != nil {
		return ""
	}
	return id
}

// FoldHash returns a stable hash of the Board, so two replicas can compare
// folds (gate B3: equal accepted sets must give equal fold hashes). Section
// 6.9.2; the encoding is canonical (JCS) and independent of map order.
func FoldHash(b *Board) (string, error) {
	items := map[string]any{}
	for id, it := range b.Items {
		fields := map[string]any{}
		for name, f := range it.Fields {
			vals := make([]any, 0, len(f.Values))
			for _, v := range f.Values {
				vals = append(vals, map[string]any{"value": v.Value, "author": v.Author, "entry": v.Entry})
			}
			fields[name] = map[string]any{"values": vals, "mark": string(f.Mark)}
		}
		lists := map[string]any{}
		for name, l := range it.Lists {
			ls := make([]any, 0, len(l))
			for _, a := range l {
				ls = append(ls, map[string]any{"value": a.Value, "author": a.Author, "entry": a.Entry,
					"hlc": a.HLC, "log": a.Log, "seq": a.Seq})
			}
			lists[name] = ls
		}
		hist := make([]any, 0, len(it.History))
		for _, h := range it.History {
			hist = append(hist, h)
		}
		m := map[string]any{"author": it.Author, "fields": fields, "lists": lists,
			"history": hist, "archived": it.Archived, "mark": string(it.Mark)}
		if it.Closure != nil {
			m["closure"] = map[string]any{"kind": it.Closure.Kind, "reason": it.Closure.Reason,
				"owner": it.Closure.Owner, "next": it.Closure.Next}
		}
		items[id] = m
	}
	held := make([]any, 0, len(b.Held))
	for _, h := range b.Held {
		held = append(held, map[string]any{"entry": h.Entry, "code": h.Code})
	}
	raw, err := Canonicalize(map[string]any{"room": b.Room, "items": items, "held": held})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// ApplySet folds one accepted item.set into an item. Section 6.9.2: two sets
// of one field neither in the other's deps are both kept (multi-value
// register) with MarkFieldConflict; a set whose deps cover every current
// value replaces them and clears the mark. A set naming "author" is never
// folded (author_not_settable, enforced at admission).
//
// Callers apply entries in causal order (FoldBoard does); vec is the entry's
// causal vector, including itself.
func ApplySet(it *Item, e *Entry, vec map[string]uint64) {
	applySet(it, e, entryID(e), vec)
}

func applySet(it *Item, e *Entry, id string, vec map[string]uint64) {
	field, _ := e.Body["field"].(string)
	if field == "" || field == "author" {
		return
	}
	if it.Fields == nil {
		it.Fields = map[string]Field{}
	}
	f := it.Fields[field]
	kept := f.Values[:0:0]
	for _, v := range f.Values {
		if vec[v.log] >= v.seq {
			continue // superseded: the new set has seen it
		}
		kept = append(kept, v)
	}
	kept = append(kept, FieldValue{Value: e.Body["value"], Author: e.Actor.String(),
		Entry: id, log: e.Log, seq: e.Seq})
	sort.Slice(kept, func(i, j int) bool { return kept[i].Entry < kept[j].Entry })
	f.Values = kept
	f.Mark = MarkNone
	if len(kept) > 1 {
		f.Mark = MarkFieldConflict
	}
	it.Fields[field] = f

	ref := vecRef{e: e, vec: vec}
	it.sets = append(it.sets, ref)
	for _, a := range it.archives {
		if Concurrent(e, a.e, vec, a.vec) {
			it.Mark = MarkConcurrentEdit
		}
	}
}

// ApplyAppend folds one accepted item.append into an item. Section 6.9.2:
// concurrent appends are all kept, ordered by (hlc, log, seq).
func ApplyAppend(it *Item, e *Entry) {
	applyAppend(it, e, entryID(e))
}

func applyAppend(it *Item, e *Entry, id string) {
	field, _ := e.Body["field"].(string)
	if field == "" {
		return
	}
	if it.Lists == nil {
		it.Lists = map[string][]Appended{}
	}
	l := append(it.Lists[field], Appended{Value: e.Body["value"], Author: e.Actor.String(),
		Entry: id, HLC: e.HLC, Log: e.Log, Seq: e.Seq})
	sort.SliceStable(l, func(i, j int) bool {
		if c := compareHLC(l[i].HLC, l[j].HLC); c != 0 {
			return c < 0
		}
		if l[i].Log != l[j].Log {
			return l[i].Log < l[j].Log
		}
		return l[i].Seq < l[j].Seq
	})
	it.Lists[field] = l
}

// compareHLC orders "2026-10-15T18:00:01.000Z/7" by its time, then its
// counter as a number (a plain string compare would put /10 before /9).
func compareHLC(a, b string) int {
	ta, ca, _ := strings.Cut(a, "/")
	tb, cb, _ := strings.Cut(b, "/")
	if c := strings.Compare(ta, tb); c != 0 {
		return c
	}
	na, _ := strconv.ParseUint(ca, 10, 64)
	nb, _ := strconv.ParseUint(cb, 10, 64)
	switch {
	case na < nb:
		return -1
	case na > nb:
		return 1
	}
	return 0
}

// ApplyArchive folds one accepted item.archive into an item. Section 6.9.2:
// the item is archived with its closure; an archive concurrent with a set
// carries MarkConcurrentEdit and the set stays in History.
func ApplyArchive(it *Item, e *Entry, vec map[string]uint64) {
	it.History = appendUnique(it.History, entryID(e))
	applyArchive(it, e, vec)
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func applyArchive(it *Item, e *Entry, vec map[string]uint64) {
	if !it.Archived {
		it.Archived = true
		str := func(k string) string { s, _ := e.Body[k].(string); return s }
		it.Closure = &Closure{Kind: str("closure"), Reason: str("reason"), Owner: str("owner"), Next: str("next")}
	}
	ref := vecRef{e: e, vec: vec}
	it.archives = append(it.archives, ref)
	for _, s := range it.sets {
		if Concurrent(s.e, e, s.vec, vec) {
			it.Mark = MarkConcurrentEdit
		}
	}
}

// Concurrent reports whether neither entry is in the other's causal past,
// given each entry's deps vector (log -> seq). Section 6.9.2: no clock
// decides anything; causality is deps only.
func Concurrent(a, b *Entry, vecA, vecB map[string]uint64) bool {
	if a.Log == b.Log {
		return false // one log is a total order
	}
	aBeforeB := vecB[a.Log] >= a.Seq
	bBeforeA := vecA[b.Log] >= b.Seq
	return !aBeforeB && !bBeforeA
}

// CreateWinner picks the surviving item.create among creates sharing one id.
// Section 6.9.2: the one with the lower entry id is the item; the other is
// rejected item_id_clash.
func CreateWinner(a, b *Entry) *Entry {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if entryID(b) < entryID(a) {
		return b
	}
	return a
}

// ConflictedState reports whether the item's state field holds more than one
// value, which the board RFC's reconciler reads as stalled (6.9.2 state move
// row).
func ConflictedState(it *Item) bool {
	if it == nil {
		return false
	}
	return len(it.Fields["state"].Values) > 1
}

// HoldForward reports whether an item.set cites (log, seq, id) deps that this
// replica does not hold yet, and returns the missing entry ids. Layer 2 rule
// 2 and gate B3: a forward-dated set is held and never overrides a later
// concurrent set. have is keyed by entry id.
func HoldForward(e *Entry, have map[string]*Entry) (missing []string, held bool) {
	for _, d := range e.Deps {
		if d.ID == "" {
			continue
		}
		if have[d.ID] == nil {
			missing = append(missing, d.ID)
		}
	}
	sort.Strings(missing)
	return missing, len(missing) > 0
}
