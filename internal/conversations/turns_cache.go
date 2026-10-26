// turns_cache.go — bounded LRU for per-session turns (issue #716).
//
// Turns are loaded lazily from their per-session projection files and kept in
// this cache, bounded by both entry count and estimated bytes, so resident
// memory no longer scales with the total size of the turns corpus.
//
// Locking: turnsCache has its OWN mutex and is a LEAF lock. Callers may hold
// idx.mu while calling into the cache, but the cache must never acquire
// idx.mu. Generations guard against a slow disk read overwriting a fresher
// writer's value: a loader captures gen(sid) before reading the file and
// stores with putIfGen, which is rejected if any put/invalidate/clear ran in
// between.

package conversations

import (
	"container/list"
	"sync"
)

// turnsCacheMaxEntries and turnsCacheMaxBytes bound the cache. Package vars so
// tests can shrink them; production code never assigns to them.
var turnsCacheMaxEntries = 1024
var turnsCacheMaxBytes int64 = 64 << 20 // 64 MiB

// turnsCache is an LRU of session_id → []Turn. The zero value is not usable;
// construct with newTurnsCache.
type turnsCache struct {
	mu    sync.Mutex
	ll    *list.List // front = most recently used; values are *turnsCacheEntry
	items map[string]*list.Element
	bytes int64

	// seq is a monotonic counter; every generation bump takes a fresh value so
	// generations are unique across sessions and across clear() calls.
	seq uint64
	// gens holds the generation of each session touched by put/invalidate
	// since the last clear. Sessions absent from gens report epoch.
	gens  map[string]uint64
	epoch uint64
}

type turnsCacheEntry struct {
	sid   string
	turns []Turn
	size  int64
}

func newTurnsCache() *turnsCache {
	return &turnsCache{
		ll:    list.New(),
		items: make(map[string]*list.Element),
		gens:  make(map[string]uint64),
	}
}

// turnsEstimateBytes is a rough, deterministic size estimate for one session's
// turns: the string payload of every turn plus a fixed per-turn overhead.
func turnsEstimateBytes(turns []Turn) int64 {
	var n int64
	for i := range turns {
		t := &turns[i]
		n += int64(len(t.Text) + len(t.UUID) + len(t.SessionID) + len(t.ParentUUID) +
			len(t.Component) + len(t.OntologyVersion) + len(t.MappingVersion) + len(t.Role))
		n += 160
	}
	return n
}

// genLocked returns sid's current generation. Caller holds c.mu.
func (c *turnsCache) genLocked(sid string) uint64 {
	if g, ok := c.gens[sid]; ok {
		return g
	}
	return c.epoch
}

// gen returns the current generation for sid.
func (c *turnsCache) gen(sid string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.genLocked(sid)
}

// get returns the cached turns for sid and marks it most recently used.
func (c *turnsCache) get(sid string) ([]Turn, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[sid]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*turnsCacheEntry).turns, true
}

// put stores turns for sid unconditionally (writer path). It bumps sid's
// generation so any in-flight putIfGen for sid is rejected.
func (c *turnsCache) put(sid string, turns []Turn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	c.gens[sid] = c.seq
	c.storeLocked(sid, turns)
}

// putIfGen stores turns for sid only if sid's generation still equals g.
// It is the loader path: g must have been captured before the disk read.
func (c *turnsCache) putIfGen(sid string, turns []Turn, g uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.genLocked(sid) != g {
		return
	}
	c.storeLocked(sid, turns)
}

// invalidate bumps sid's generation and removes its entry.
func (c *turnsCache) invalidate(sid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	c.gens[sid] = c.seq
	c.removeLocked(sid)
}

// clear empties the cache and bumps the global epoch, so every outstanding
// putIfGen captured before the clear is rejected.
func (c *turnsCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	c.epoch = c.seq
	c.gens = make(map[string]uint64)
	c.ll.Init()
	c.items = make(map[string]*list.Element)
	c.bytes = 0
}

// stats reports the current entry count and estimated bytes (for tests).
func (c *turnsCache) stats() (entries int, bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len(), c.bytes
}

// storeLocked inserts or replaces sid's entry, then evicts LRU entries until
// both bounds hold. An entry larger than turnsCacheMaxBytes/4 is not cached;
// any stale entry for sid is removed in that case. Caller holds c.mu.
func (c *turnsCache) storeLocked(sid string, turns []Turn) {
	c.removeLocked(sid)
	size := turnsEstimateBytes(turns)
	if size > turnsCacheMaxBytes/4 {
		return
	}
	el := c.ll.PushFront(&turnsCacheEntry{sid: sid, turns: turns, size: size})
	c.items[sid] = el
	c.bytes += size
	for c.ll.Len() > turnsCacheMaxEntries || c.bytes > turnsCacheMaxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.removeLocked(back.Value.(*turnsCacheEntry).sid)
	}
}

// removeLocked drops sid's entry if present. Caller holds c.mu.
func (c *turnsCache) removeLocked(sid string) {
	el, ok := c.items[sid]
	if !ok {
		return
	}
	e := el.Value.(*turnsCacheEntry)
	c.ll.Remove(el)
	delete(c.items, sid)
	c.bytes -= e.size
}
