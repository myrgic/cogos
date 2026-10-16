package room

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestBoardFoldProperty is the RFC rooms-as-declared-resources B3 gate for
// the board fold (section 6.9.2): random concurrent item.create, item.set,
// item.append and item.archive sequences over two logs, each folded under
// several arrival orders. Equal accepted sets must give equal fold hashes.
func TestBoardFoldProperty(t *testing.T) {
	fields := []string{"title", "state"}
	for seed := int64(1); seed <= 5; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			w := newWorld(t, nil)
			w.app("node-b")
			nodes := []string{"node-a", "node-b"}
			actors := map[string]Actor{"node-a": chaz, "node-b": vroku}

			_, base := w.createItem("node-a", chaz, nil)
			w.sync()
			known := map[string][]string{"node-a": {base}, "node-b": {base}}

			// 200 random operations. Each node only references items it has
			// seen (its own creates, or creates it synced), so every op is
			// causally placed by the node that wrote it.
			for i := 0; i < 200; i++ {
				node := nodes[rng.Intn(len(nodes))]
				actor := actors[node]
				item := known[node][rng.Intn(len(known[node]))]
				switch r := rng.Intn(100); {
				case r < 10:
					_, id := w.createItem(node, actor, nil)
					known[node] = append(known[node], id)
				case r < 50:
					w.mk(node, KindItemSet, actor, map[string]any{
						"item": item, "field": fields[rng.Intn(len(fields))], "value": fmt.Sprintf("v%d", i),
					})
				case r < 80:
					w.mk(node, KindItemAppend, actor, map[string]any{
						"item": item, "field": "comments", "value": i,
					})
				case r < 90:
					w.mk(node, KindItemArchive, actor, map[string]any{
						"item": item, "closure": "done",
					})
				default:
					w.sync()
					merged := map[string]bool{}
					var union []string
					for _, n := range nodes {
						for _, id := range known[n] {
							if !merged[id] {
								merged[id] = true
								union = append(union, id)
							}
						}
					}
					for _, n := range nodes {
						known[n] = append([]string(nil), union...)
					}
				}
			}

			var want string
			for k := 0; k < 8; k++ {
				perm := append([]*Entry(nil), w.all...)
				if k > 0 {
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				}
				out := Evaluate(w.params(), perm)
				if k == 0 && len(out.Accepted()) < 50 {
					t.Fatalf("only %d accepted entries: the property is not exercised", len(out.Accepted()))
				}
				h, err := FoldHash(FoldBoard(out, perm))
				if err != nil {
					t.Fatal(err)
				}
				if k == 0 {
					want = h
					continue
				}
				if h != want {
					t.Fatalf("arrival order %d folds to %s, want %s", k, h, want)
				}
			}
		})
	}
}
