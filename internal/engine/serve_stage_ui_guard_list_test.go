package engine

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
