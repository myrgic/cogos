// Package room is the board entry format, signatures and admission for
// rooms-as-boards (RFC rooms-as-declared-resources section 6.9, slice B1,
// stage-into-kernel slice P4-1).
//
// Everything here is a pure function of its inputs: no network, no ledger
// writes, no routes, no file or clock access. The package defines
//
//   - the wire entry (Entry) and its RFC 8785 canonical form (jcs.go),
//   - the entry id and the low-S ECDSA P-256 signature over
//     "cogos.room.entry.v1\n" || JCS(entry without sig) (entry.go),
//   - the replicated room spec (Spec, spec.go),
//   - Layer 2 (signature and log integrity) and Layer 3 (the membrane,
//     admit) evaluated over a SET of entries (evaluate.go, admit.go), so the
//     outcome never depends on arrival order,
//   - the floor-claim checks and the deterministic winner rule (floor.go).
//
// A later slice (P4-3) stores entries and serves routes; P5 W1 (the run
// board) builds on the entry format defined here. Fields named in the
// kernel events this package describes carry room and actor (campaign R10c).
package room
