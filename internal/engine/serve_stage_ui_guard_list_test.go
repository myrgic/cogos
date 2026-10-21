package engine

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GET /local/dev/guard must list the kernel's own lockouts (P3b S5): under
// keys=enforce the kernel counts key misses itself, but the Stage's listing
// only knows its own table (guest_escape_probe row 8 saw 429 yet "lockout
// listed False").

const guardListStageBody = `{"locked": ["198.51.100.1"], "misses": {"198.51.100.1": 21, "203.0.113.7": 2}, "limit": 20}`

func guardListStage() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == guardListPath {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, guardListStageBody)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
}

type guardListDoc struct {
	Locked []string       `json:"locked"`
	Misses map[string]int `json:"misses"`
	Limit  int            `json:"limit"`
}

// guardListReq is the owner's request: loopback peer, loopback Host, no tunnel header.
func guardListReq(h http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, guardListPath, nil)
	req.RemoteAddr = guardPeer
	req.Host = "localhost"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func guardListGet(t *testing.T, h http.Handler) (*httptest.ResponseRecorder, guardListDoc) {
	t.Helper()
	rec := guardListReq(h)
	var d guardListDoc
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
			t.Fatalf("bad json %q: %v", rec.Body.String(), err)
		}
	}
	return rec, d
}

func guardListMissPast(h http.Handler, ip string) {
	for i := 0; i <= stageGuardLimit+1; i++ {
		shadowDo(h, http.MethodGet, "/r/"+skBAD+"/x", ip, false)
	}
}

func TestStageUIGuardList_EnforceListsKernelLockout(t *testing.T) {
	up := guardListStage()
	defer up.Close()
	h, g := shadowHandler(t, StageUIKeysEnforce, up.URL, nil)
	const ip = "203.0.113.50"
	guardListMissPast(h, ip)
	if !g.isLocked(ip) {
		t.Fatal("precondition: kernel should hold a lockout for the guesser")
	}
	rec, d := guardListGet(t, h)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	got := map[string]bool{}
	for _, x := range d.Locked {
		if got[x] {
			t.Fatalf("duplicate %s in %v", x, d.Locked)
		}
		got[x] = true
	}
	if !got[ip] || !got["198.51.100.1"] {
		t.Fatalf("want kernel and Stage lockouts, got %v", d.Locked)
	}
	if d.Misses[ip] < stageGuardLimit+1 || d.Misses["203.0.113.7"] != 2 || d.Limit != 20 {
		t.Fatalf("misses/limit not merged: %+v", d)
	}
}

func TestStageUIGuardList_UpstreamDownAnswersFromKernel(t *testing.T) {
	up := guardListStage()
	h, _ := shadowHandler(t, StageUIKeysEnforce, up.URL, nil)
	const ip = "203.0.113.51"
	guardListMissPast(h, ip)
	up.Close()
	rec, d := guardListGet(t, h)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if len(d.Locked) != 1 || d.Locked[0] != ip || d.Limit != stageGuardLimit {
		t.Fatalf("want only kernel lockout, got %+v", d)
	}
}

func TestStageUIGuardList_NoKernelLockoutPassesThrough(t *testing.T) {
	for _, mode := range []string{StageUIKeysOff, StageUIKeysShadow, StageUIKeysEnforce} {
		up := guardListStage()
		h, _ := shadowHandler(t, mode, up.URL, nil)
		rec := guardListReq(h)
		if rec.Code != http.StatusOK || rec.Body.String() != guardListStageBody {
			t.Fatalf("%s: want byte-identical passthrough, got %d %q", mode, rec.Code, rec.Body.String())
		}
		up.Close()
	}
}

func TestStageUIGuardList_SubThresholdMissesMerged(t *testing.T) {
	up := guardListStage()
	defer up.Close()
	h, _ := shadowHandler(t, StageUIKeysEnforce, up.URL, nil)
	const ip = "203.0.113.99"
	for i := 0; i < 15; i++ {
		shadowDo(h, http.MethodGet, "/r/"+skBAD+"/x", ip, false)
	}
	rec, d := guardListGet(t, h)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if d.Misses[ip] != 15 {
		t.Fatalf("want kernel's 15 sub-threshold misses for %s, got %+v", ip, d)
	}
	if d.Misses["203.0.113.7"] != 2 || len(d.Locked) != 1 || d.Locked[0] != "198.51.100.1" {
		t.Fatalf("Stage's own entries lost: %+v", d)
	}
}

func TestStageUIGuardList_GzipClientStillSeesKernelLockout(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != guardListPath {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			zw := gzip.NewWriter(w)
			zw.Write([]byte(guardListStageBody))
			zw.Close()
			return
		}
		fmt.Fprint(w, guardListStageBody)
	}))
	defer up.Close()
	h, g := shadowHandler(t, StageUIKeysEnforce, up.URL, nil)
	const ip = "203.0.113.52"
	guardListMissPast(h, ip)
	if !g.isLocked(ip) {
		t.Fatal("precondition: kernel lockout")
	}
	req := httptest.NewRequest(http.MethodGet, guardListPath, nil)
	req.RemoteAddr = guardPeer
	req.Host = "localhost"
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var d guardListDoc
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatalf("bad json %q: %v", rec.Body.String(), err)
	}
	found := false
	for _, x := range d.Locked {
		found = found || x == ip
	}
	if !found {
		t.Fatalf("gzip client missed kernel lockout: %+v", d)
	}
}
