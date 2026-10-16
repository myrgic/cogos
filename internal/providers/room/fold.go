package room

// Board fold (RFC rooms-as-declared-resources section 6.9.2, slice B3,
// stage-into-kernel slice P4-2). This file is a skeleton: the types and
// signatures are fixed here, every body is a stub. The fold is a pure
// function of the SET of accepted item.* entries; no clock, no arrival order.

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

// FoldBoard folds the accepted item.* entries of an evaluated room into a
// Board. Section 6.9.2: the fold is a pure function of the set of accepted
// entries in causal order (deps); the same set gives the same Board in any
// delivery order. Only entries with StatusAccepted in out are folded.
func FoldBoard(out *Outcome, entries []*Entry) *Board {
	panic("unimplemented")
}

// FoldHash returns a stable hash of the Board, so two replicas can compare
// folds (gate B3: equal accepted sets must give equal fold hashes). Section
// 6.9.2; the encoding is canonical (JCS) and independent of map order.
func FoldHash(b *Board) (string, error) {
	panic("unimplemented")
}

// ApplySet folds one accepted item.set into an item. Section 6.9.2: two sets
// of one field neither in the other's deps are both kept (multi-value
// register) with MarkFieldConflict; a set whose deps cover every current
// value replaces them and clears the mark. A set naming "author" is never
// folded (author_not_settable, enforced at admission).
func ApplySet(it *Item, e *Entry, vec map[string]uint64) {
	panic("unimplemented")
}

// ApplyAppend folds one accepted item.append into an item. Section 6.9.2:
// concurrent appends are all kept, ordered by (hlc, log, seq).
func ApplyAppend(it *Item, e *Entry) {
	panic("unimplemented")
}

// ApplyArchive folds one accepted item.archive into an item. Section 6.9.2:
// the item is archived with its closure; an archive concurrent with a set
// carries MarkConcurrentEdit and the set stays in History.
func ApplyArchive(it *Item, e *Entry, vec map[string]uint64) {
	panic("unimplemented")
}

// Concurrent reports whether neither entry is in the other's causal past,
// given each entry's deps vector (log -> seq). Section 6.9.2: no clock
// decides anything; causality is deps only.
func Concurrent(a, b *Entry, vecA, vecB map[string]uint64) bool {
	panic("unimplemented")
}

// CreateWinner picks the surviving item.create among creates sharing one id.
// Section 6.9.2: the one with the lower entry id is the item; the other is
// rejected item_id_clash.
func CreateWinner(a, b *Entry) *Entry {
	panic("unimplemented")
}

// ConflictedState reports whether the item's state field holds more than one
// value, which the board RFC's reconciler reads as stalled (6.9.2 state move
// row).
func ConflictedState(it *Item) bool {
	panic("unimplemented")
}

// HoldForward reports whether an item.set cites (log, seq, id) deps that this
// replica does not hold yet, and returns the missing entry ids. Layer 2 rule
// 2 and gate B3: a forward-dated set is held and never overrides a later
// concurrent set.
func HoldForward(e *Entry, have map[string]*Entry) (missing []string, held bool) {
	panic("unimplemented")
}
