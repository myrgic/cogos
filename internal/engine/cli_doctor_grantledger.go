// cli_doctor_grantledger.go: classify a presented grant against this kernel's
// identity-grant ledger (<workspace>/.cog/ledger/identity-grants/events.jsonl)
// by fingerprint only.
//
// The ledger stores sha256(token) as integrity_hash (never the token), which
// is exactly what IdentityGrantRegistry.VerifyAny compares. Doctor hashes the
// presented value in memory and asks three questions: is the hash a LIVE
// grant here, was it ever ISSUED here (so it is merely stale), or was it never
// issued by this kernel (another node's grant). Only the first 10 hex chars of
// the hash are ever shown.
package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type grantClassKind int

const (
	grantLedgerMissing grantClassKind = iota // no ledger to compare against
	grantLive                                // live grant on this kernel
	grantStale                               // issued here, no longer live
	grantForeign                             // never issued by this kernel
)

type grantClass struct {
	Kind    grantClassKind
	Surface string
	Hash10  string
}

// describe renders the classification as one pointer-style clause.
func (c grantClass) describe() string {
	fp := "sha256:" + c.Hash10
	switch c.Kind {
	case grantLive:
		return fmt.Sprintf("grant %s was issued by this kernel (surface %s) and is live", fp, c.Surface)
	case grantStale:
		return fmt.Sprintf("grant %s was issued by this kernel (surface %s) but is no longer live (superseded, revoked or expired)", fp, c.Surface)
	case grantForeign:
		return fmt.Sprintf("grant %s was NOT issued by this kernel: likely another node's grant", fp)
	}
	return fmt.Sprintf("grant %s could not be compared (no grant ledger)", fp)
}

type grantLedgerView struct {
	path    string
	exists  bool
	issued  map[string]string // integrity hash -> surface, every hash ever issued here
	live    map[string]string // integrity hash -> surface, live now
	scanned int
}

func grantLedgerPath(root string) string {
	return filepath.Join(root, ".cog", "ledger", identityGrantsLedgerSession, "events.jsonl")
}

// loadGrantLedger replays the grant ledger with the same event semantics as
// RebuildIdentityGrantRegistryFromLedger and additionally remembers every
// integrity hash ever issued, so stale can be told apart from foreign.
func loadGrantLedger(root string) *grantLedgerView {
	v := &grantLedgerView{path: grantLedgerPath(root), issued: map[string]string{}, live: map[string]string{}}
	f, err := os.Open(v.path)
	if err != nil {
		return v
	}
	defer f.Close()
	v.exists = true

	reg := NewIdentityGrantRegistry()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var env EventEnvelope
		if json.Unmarshal([]byte(line), &env) != nil {
			continue
		}
		v.scanned++
		switch env.HashedPayload.Type {
		case "identity.grant.issued", "identity.grant.superseded":
			h, _ := env.HashedPayload.Data["integrity_hash"].(string)
			s, _ := env.HashedPayload.Data["surface"].(string)
			if h != "" {
				v.issued[h] = s
			}
		}
		applyIdentityGrantLedgerEvent(reg, &env)
	}
	for _, g := range reg.Snapshot() {
		v.live[g.IntegrityHash] = g.Surface
	}
	return v
}

func (v *grantLedgerView) classify(token string) grantClass {
	h := sha256Hex(token)
	c := grantClass{Hash10: grantFingerprint(token)}
	if !v.exists {
		c.Kind = grantLedgerMissing
		return c
	}
	if s, ok := v.live[h]; ok {
		c.Kind, c.Surface = grantLive, s
	} else if s, ok := v.issued[h]; ok {
		c.Kind, c.Surface = grantStale, s
	} else {
		c.Kind = grantForeign
	}
	return c
}
