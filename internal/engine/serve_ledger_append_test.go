package engine

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func postLedgerAppend(t *testing.T, srv *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/ledger/append", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.handleLedgerAppend(w, req)
	return w
}

func getLedgerEntry(t *testing.T, srv *Server, bucket, hash string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/ledger/entry?bucket="+bucket+"&hash="+hash, nil)
	w := httptest.NewRecorder()
	srv.handleLedgerEntry(w, req)
	return w
}

func TestLedgerAppendReadBackByteIdentical(t *testing.T) {
	srv := newTestServer(t)
	t.Cleanup(resetLedgerCacheForTest)

	content := "room transcript é\n\"quoted\" \\ line two\n\ttabbed"
	body, _ := json.Marshal(map[string]any{
		"room_id": "room-1", "source_uri": "cog://rooms/room-1",
		"content": content, "metadata": map[string]any{"k": "v"},
		"observed_at": "2026-11-18T10:00:00Z",
	})
	w := postLedgerAppend(t, srv, string(body))
	if w.Code != http.StatusOK {
		t.Fatalf("append status = %d: %s", w.Code, w.Body.String())
	}
	var ar LedgerAppendResult
	if err := json.Unmarshal(w.Body.Bytes(), &ar); err != nil {
		t.Fatal(err)
	}
	if ar.Bucket != DefaultLedgerAppendBucket || ar.EventHash == "" || ar.Seq != 1 {
		t.Fatalf("unexpected append result: %+v", ar)
	}

	g := getLedgerEntry(t, srv, "", ar.EventHash)
	if g.Code != http.StatusOK {
		t.Fatalf("read-back status = %d: %s", g.Code, g.Body.String())
	}
	var er LedgerEntryResult
	if err := json.Unmarshal(g.Body.Bytes(), &er); err != nil {
		t.Fatal(err)
	}
	if er.Content == nil || !bytes.Equal([]byte(*er.Content), []byte(content)) {
		t.Fatalf("content not byte-identical: got %v want %q", er.Content, content)
	}
	if !er.Verified || !er.EventHashValid || !er.ContentHashValid {
		t.Fatalf("expected verified entry: %+v", er)
	}
	if er.RoomID != "room-1" || er.ObservedAt != "2026-11-18T10:00:00Z" || er.ContentHash != ar.ContentHash {
		t.Errorf("envelope fields lost: %+v", er)
	}

	// The chain itself must also verify via the existing ledger query.
	res, err := QueryLedger(srv.cfg.WorkspaceRoot, LedgerQuery{SessionID: DefaultLedgerAppendBucket, VerifyChain: true})
	if err != nil || !res.Verification.Valid {
		t.Fatalf("chain verify: err=%v res=%+v", err, res)
	}
}

func TestLedgerAppendNamedBucketLandsInThatBucket(t *testing.T) {
	srv := newTestServer(t)
	t.Cleanup(resetLedgerCacheForTest)
	root := srv.cfg.WorkspaceRoot

	w := postLedgerAppend(t, srv, `{"bucket":"bucket-a","room_id":"r","content":"alpha"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var ar LedgerAppendResult
	_ = json.Unmarshal(w.Body.Bytes(), &ar)

	a := filepath.Join(root, ".cog", "ledger", "bucket-a", "events.jsonl")
	if b, err := os.ReadFile(a); err != nil || !strings.Contains(string(b), ar.EventHash) {
		t.Fatalf("bucket-a file missing event: err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".cog", "ledger", DefaultLedgerAppendBucket, "events.jsonl")); !os.IsNotExist(err) {
		t.Errorf("default bucket file must not exist (err=%v)", err)
	}
	if g := getLedgerEntry(t, srv, "bucket-a", ar.EventHash); g.Code != http.StatusOK {
		t.Errorf("read from right bucket: %d", g.Code)
	}
	if g := getLedgerEntry(t, srv, "bucket-b", ar.EventHash); g.Code != http.StatusNotFound {
		t.Errorf("read from wrong bucket: %d want 404", g.Code)
	}
}

func TestLedgerAppendRejectsBadRequests(t *testing.T) {
	srv := newTestServer(t)
	t.Cleanup(resetLedgerCacheForTest)
	for name, body := range map[string]string{
		"reserved bucket":   `{"bucket":"rooms","room_id":"r","content":"x"}`,
		"traversal bucket":  `{"bucket":"../x","room_id":"r","content":"x"}`,
		"no content":        `{"room_id":"r"}`,
		"no source or room": `{"content":"x"}`,
		"hash mismatch":     `{"room_id":"r","content":"x","content_hash":"deadbeef"}`,
		"bad observed_at":   `{"room_id":"r","content":"x","observed_at":"yesterday"}`,
		"bad json":          `{`,
	} {
		if w := postLedgerAppend(t, srv, body); w.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d want 400 (%s)", name, w.Code, w.Body.String())
		}
	}
}

// A tampered file must be reported as unverified, never silently accepted.
func TestLedgerReadBackDetectsCorruption(t *testing.T) {
	for name, tamper := range map[string]func(string) string{
		// Content edited, hashes left alone.
		"content": func(s string) string { return strings.Replace(s, "original", "forgedxx", 1) },
		// Content AND stored content_hash rewritten consistently: only the
		// event (chain) hash catches it.
		"content+content_hash": func(s string) string {
			old, _ := ledgerContentHash("original")
			neu, _ := ledgerContentHash("forgedxx")
			return strings.Replace(strings.Replace(s, "original", "forgedxx", 1), old, neu, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv := newTestServer(t)
			t.Cleanup(resetLedgerCacheForTest)
			w := postLedgerAppend(t, srv, `{"room_id":"r","content":"original"}`)
			var ar LedgerAppendResult
			_ = json.Unmarshal(w.Body.Bytes(), &ar)

			path := filepath.Join(srv.cfg.WorkspaceRoot, ".cog", "ledger", DefaultLedgerAppendBucket, "events.jsonl")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(tamper(string(raw))), 0o644); err != nil {
				t.Fatal(err)
			}

			g := getLedgerEntry(t, srv, "", ar.EventHash)
			if g.Code != http.StatusOK {
				t.Fatalf("status %d: %s", g.Code, g.Body.String())
			}
			var er LedgerEntryResult
			_ = json.Unmarshal(g.Body.Bytes(), &er)
			if er.Verified || len(er.Errors) == 0 {
				t.Fatalf("corruption not detected: %+v", er)
			}
			if er.EventHashValid {
				t.Errorf("event hash should not validate after tamper")
			}
		})
	}
}
