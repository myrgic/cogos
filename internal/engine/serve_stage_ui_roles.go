// serve_stage_ui_roles.go — the Stage UI listener's role gate (P3b slice S6).
//
// Under stage_ui.keys=enforce, after S5 has settled that a key is live, the
// listener also answers the owner-only refusals itself: a key whose share link
// carries a role other than "owner" never reaches the Stage on a route the
// Stage reserves for the owner. The answers are the Stage's own, byte for
// byte, so the 404 stays an oracle that cannot tell a guest key from an
// unknown one (front-door vectors section 6.10).
//
// The route table is read from stage.py, not invented here:
//
//	/r/<key>/ and /k/<key>/ (owner_key_only, stage.py:110-125, 1915-1935)
//	    monengine, monengine/*, voicetest/voices, voicetest/synth, deck,
//	    deck/<f>, canvases, canvas/<cid>, surface, oracles, oracles/*, oscine,
//	    oscine/*, studio, studio/*, ear, device/request
//	      -> 404 text/plain "not found"          (O01)
//	    ear/ws, oscine/bridge as a websocket
//	      -> bare 403 (the pre-accept close 4404) (O02)
//	    collab, GET or POST (collab_api, stage.py:243-253)
//	      -> 403 {"error":"owner only"}           (O04)
//	    cog, POST (cog_api, stage.py:268-269)
//	      -> 403 {"error":"only Chaz can do that"} (O06)
//	/h/<home key>/api/<what> (home_api, HOME_OWNER_ONLY, stage.py:889-908)
//	    devices, session, continue, app-rooms, app-room, boards, GET or POST,
//	    and sessions on any method but GET
//	      -> 404 {"error":"unknown"}              (O09)
//	    GET sessions stays with the Stage: it answers {"sessions": []} quietly
//	    (O10), and the guest probe counts that empty body as a refusal (row 5a).
//
// What stays in Python: the socket message gates (O15-O18, until O8 in P4),
// GET /cog with the owner data removed (O05), voice (O08), everything a guest
// is allowed to do, and any path or method this table does not name exactly
// (a route the Stage answers 405 or redirects itself).
//
// The role is the link's: keys_sync.py writes people.py's role into the link
// when it mirrors the key. A role refusal is an answer to a known key, so it
// never counts toward the lockout (the Stage's key_known says the key is real).
//
// off and shadow never reach this file's gate.
package engine

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

const stageRoleOwner = "owner"

// stageOwnerRouteExact are the owner_key_only routes with a fixed tail.
var stageOwnerRouteExact = map[string]bool{
	"monengine": true, "voicetest/voices": true, "voicetest/synth": true, "deck": true,
	"canvases": true, "surface": true, "oracles": true, "oscine": true, "studio": true,
	"ear": true, "device/request": true,
}

// stageOwnerRoutePrefix are the {path:path} routes: the prefix, then anything
// (including nothing).
var stageOwnerRoutePrefix = []string{"monengine/", "oracles/", "oscine/", "studio/"}

// stageOwnerSocketTail are the websocket-only owner routes. Over plain HTTP the
// Stage has no route for them (ear/ws) or covers them by a prefix above.
var stageOwnerSocketTail = map[string]bool{"ear/ws": true, "oscine/bridge": true}

// stageHomeOwnerOnly is HOME_OWNER_ONLY in stage.py.
var stageHomeOwnerOnly = map[string]bool{
	"devices": true, "sessions": true, "session": true, "continue": true,
	"app-rooms": true, "app-room": true, "boards": true,
}

// stageOwnerHTTPRoute reports whether the tail after /r/<key>/ is an
// owner_key_only HTTP route, matched the way starlette matches it.
func stageOwnerHTTPRoute(tail string) bool {
	if stageOwnerRouteExact[tail] {
		return true
	}
	for _, pre := range stageOwnerRoutePrefix {
		if strings.HasPrefix(tail, pre) {
			return true
		}
	}
	// deck/{f} and canvas/{cid}: exactly one non-empty segment.
	for _, pre := range []string{"deck/", "canvas/"} {
		if strings.HasPrefix(tail, pre) {
			seg := tail[len(pre):]
			return seg != "" && !strings.Contains(seg, "/")
		}
	}
	return false
}

// stageRoleAnswer is a refusal the listener writes itself.
type stageRoleAnswer struct {
	status int
	ws     bool   // bare 403 on the wire
	json   string // body, when JSON
	text   bool   // body is the starlette plain "not found"
	why    string // log tag
}

// roleRefusal decides whether the role gate answers a request itself. nil
// means proxy. Only a live "share" link whose role is not owner is refused;
// anything else (no store, unknown key, owner) is left to S5 and the Stage.
func (k *keyShadow) roleRefusal(method, p string, ws bool) *stageRoleAnswer {
	class, seg, pair, ok := keyedClass(p)
	if !ok || pair || seg == "" {
		return nil
	}
	st := k.store()
	if st == nil {
		return nil
	}
	l, found := st.Resolve(seg)
	if !found || l.Kind != ShareLinkKindShare || l.Role == stageRoleOwner {
		return nil
	}
	parts := strings.SplitN(p, "/", 4) // "", class, key, tail
	tail := ""
	if len(parts) == 4 {
		tail = parts[3]
	}

	if class == "h" {
		// Only a home key reads the home API as itself; a room key on /h/ is
		// the Stage's own 404.
		if !strings.HasPrefix(l.Surface, stageHomeSurfacePrefix) || ws {
			return nil
		}
		what, isAPI := strings.CutPrefix(tail, "api/")
		if !isAPI || what == "" || strings.Contains(what, "/") || !stageHomeOwnerOnly[what] {
			return nil
		}
		if method != http.MethodGet && method != http.MethodPost {
			return nil
		}
		if what == "sessions" && method == http.MethodGet {
			return nil
		}
		return &stageRoleAnswer{status: http.StatusNotFound, json: `{"error":"unknown"}`, why: "home/" + what}
	}

	// /r/ and /k/ carry room keys. A home key here was already answered by S5.
	if strings.HasPrefix(l.Surface, stageHomeSurfacePrefix) {
		return nil
	}
	if ws {
		if stageOwnerSocketTail[tail] || stageOwnerHTTPRoute(tail) {
			return &stageRoleAnswer{status: http.StatusForbidden, ws: true, why: tail}
		}
		return nil
	}
	switch {
	case stageOwnerHTTPRoute(tail):
		return &stageRoleAnswer{status: http.StatusNotFound, text: true, why: tail}
	case tail == "collab" && (method == http.MethodGet || method == http.MethodPost):
		return &stageRoleAnswer{status: http.StatusForbidden, json: `{"error":"owner only"}`, why: "collab"}
	case tail == "cog" && method == http.MethodPost:
		return &stageRoleAnswer{status: http.StatusForbidden, json: `{"error":"only Chaz can do that"}`, why: "cog"}
	}
	return nil
}

// writeStageRoleRefusal writes the answer and logs it the way the Stage logs
// guest-refused (path with the key elided, no key, no hash).
func writeStageRoleRefusal(w http.ResponseWriter, a *stageRoleAnswer, path string) {
	slog.Warn("stage-ui: role-refused", "path", stageGuardLogPath(path), "what", a.why, "code", a.status)
	switch {
	case a.ws:
		w.WriteHeader(a.status)
	case a.text:
		writeStageKeyRefusal(w, false)
	default:
		w.Header().Set("Content-Type", "application/json")
		if a.why == "collab" {
			w.Header().Set("Cache-Control", "no-store")
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(a.json)))
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.json))
	}
}
