// serve_inference.go — GET /v1/inference: one read-only join over what the
// kernel already knows about its inference substrate.
//
// Motivation (cog://mem/reflective/2026-09-04-declared-not-wired-ledger):
// four of six declared-vs-executed gaps hit in one day were answerable from
// state the kernel already held — the lms-model-state reconciler's cached
// live rows, the dispatch resolver's fallback note, the in-flight request
// table, and the two disagreeing views of provider health — but that state
// lived in serve.log and a free-text last_reason instead of on a route a
// caller could check before acting.
//
// This handler adds no state, no goroutine, and no network I/O of its own:
//
//	engines — LMSModelStateProvider cached rows (populated by the reconcile
//	          daemon's FetchLive; Health() is O(1) by design)
//	pins    — recordPinResolution table (typed "declared" | "fallback:<cause>")
//	queue   — inflightRequests (#432 retry-dedup table)
//	health  — per provider: router liveness (Provider.Available) beside the
//	          reconciler's ResourceStatus, with disagreement=true when the
//	          two views do not agree. This is ledger row 6: /v1/providers
//	          said available:true while the self-heal loop logged Degraded.
//
// The only call that may touch the network is Provider.Available, which
// /v1/providers already makes on every request; it is bounded by the
// request context.
package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// InferenceEngine is one model row on one backend as last observed by the
// lms-model-state reconciler.
type InferenceEngine struct {
	Provider            string    `json:"provider"`
	Node                string    `json:"node"`
	Model               string    `json:"model"`
	State               string    `json:"state"` // loaded | loading
	Target              bool      `json:"target"`
	LoadedContextLength *int      `json:"loaded_context_length,omitempty"`
	MaxContextLength    int       `json:"max_context_length,omitempty"`
	Parallel            *int      `json:"parallel,omitempty"`
	ObservedAt          time.Time `json:"observed_at"`
}

// InferenceQueueEntry is one request currently registered in-flight.
type InferenceQueueEntry struct {
	RequestID string `json:"request_id"`
}

// InferenceHealth joins the router's liveness view with the reconciler's
// state view for one provider. Disagreement is the ledger's row 6.
type InferenceHealth struct {
	Provider        string `json:"provider"`
	Liveness        *bool  `json:"liveness,omitempty"` // nil ⇒ not registered in the router
	ReconcilerType  string `json:"reconciler_type,omitempty"`
	ReconcilerSync  string `json:"reconciler_sync,omitempty"`
	ReconcilerState string `json:"reconciler_state,omitempty"`
	ReconcilerOp    string `json:"reconciler_operation,omitempty"`
	Message         string `json:"message,omitempty"`
	Disagreement    bool   `json:"disagreement"`
}

// InferenceView is the GET /v1/inference response body.
type InferenceView struct {
	Engines   []InferenceEngine     `json:"engines"`
	Pins      []PinResolution       `json:"pins"`
	Queue     []InferenceQueueEntry `json:"queue"`
	Health    []InferenceHealth     `json:"health"`
	Timestamp time.Time             `json:"timestamp"`
}

// healthDisagrees reports whether a liveness ping and a reconciler status
// tell different stories. Suspended and Progressing are neither agreement
// nor disagreement: the reconciler is deliberately not asserting a verdict
// (opt-out, unreachable, or converging), so nothing is contradicted.
func healthDisagrees(live bool, st reconcile.ResourceStatus) bool {
	switch st.Health {
	case reconcile.HealthSuspended, reconcile.HealthProgressing:
		return false
	case reconcile.HealthHealthy:
		return !live || st.Sync == reconcile.SyncStatusOutOfSync
	case reconcile.HealthDegraded, reconcile.HealthMissing:
		return live
	}
	return live && st.Sync == reconcile.SyncStatusOutOfSync
}

// reconcilerProviderName strips the "<type>/" prefix from a registry key
// (e.g. "lms-model-state/lmstudio-eclipse" → "lmstudio-eclipse"). Keys
// without a slash are returned unchanged.
func reconcilerProviderName(key string) string {
	if i := strings.IndexByte(key, '/'); i >= 0 {
		return key[i+1:]
	}
	return key
}

// buildInferenceView assembles the response from the router and the
// reconcile registry. listReconcilers/getReconciler are injectable so tests
// can run against an isolated provider set rather than the global registry.
func buildInferenceView(
	ctx context.Context,
	router Router,
	listReconcilers func() []string,
	getReconciler func(string) (reconcile.Reconcilable, error),
) InferenceView {
	view := InferenceView{
		Engines:   []InferenceEngine{},
		Pins:      snapshotPinResolutions(),
		Queue:     []InferenceQueueEntry{},
		Health:    []InferenceHealth{},
		Timestamp: time.Now().UTC(),
	}

	// Liveness per router-registered provider.
	liveness := map[string]bool{}
	var routerNames []string
	if sr, ok := router.(*SimpleRouter); ok && sr != nil {
		sr.mu.RLock()
		providers := append([]Provider(nil), sr.providers...)
		sr.mu.RUnlock()
		for _, p := range providers {
			liveness[p.Name()] = p.Available(ctx)
			routerNames = append(routerNames, p.Name())
		}
	}

	// Reconciler views, keyed by provider name.
	recon := map[string]reconcile.ResourceStatus{}
	for _, key := range listReconcilers() {
		if !strings.HasPrefix(key, lmsModelStateType+"/") {
			continue
		}
		p, err := getReconciler(key)
		if err != nil {
			continue
		}
		recon[reconcilerProviderName(key)] = p.Health()
		if msp, ok := p.(*LMSModelStateProvider); ok {
			view.Engines = append(view.Engines, msp.engineRows()...)
		}
	}

	// Union of names, sorted for a stable body.
	seen := map[string]struct{}{}
	names := make([]string, 0, len(liveness)+len(recon))
	for _, n := range routerNames {
		if _, dup := seen[n]; !dup {
			seen[n] = struct{}{}
			names = append(names, n)
		}
	}
	for n := range recon {
		if _, dup := seen[n]; !dup {
			seen[n] = struct{}{}
			names = append(names, n)
		}
	}
	sort.Strings(names)

	for _, n := range names {
		h := InferenceHealth{Provider: n}
		live, hasLive := liveness[n]
		if hasLive {
			v := live
			h.Liveness = &v
		}
		if st, ok := recon[n]; ok {
			h.ReconcilerType = lmsModelStateType
			h.ReconcilerSync = string(st.Sync)
			h.ReconcilerState = string(st.Health)
			h.ReconcilerOp = string(st.Operation)
			h.Message = st.Message
			if hasLive {
				h.Disagreement = healthDisagrees(live, st)
			}
		}
		view.Health = append(view.Health, h)
	}

	sort.Slice(view.Engines, func(i, j int) bool {
		if view.Engines[i].Provider != view.Engines[j].Provider {
			return view.Engines[i].Provider < view.Engines[j].Provider
		}
		return view.Engines[i].Model < view.Engines[j].Model
	})

	inflightRequests.Range(func(k, _ any) bool {
		if id, ok := k.(string); ok {
			view.Queue = append(view.Queue, InferenceQueueEntry{RequestID: id})
		}
		return true
	})
	sort.Slice(view.Queue, func(i, j int) bool { return view.Queue[i].RequestID < view.Queue[j].RequestID })

	return view
}

// engineRows projects the provider's cached live rows into InferenceEngine
// entries. Reads only the cache; never probes.
func (p *LMSModelStateProvider) engineRows() []InferenceEngine {
	p.mu.RLock()
	rows := p.lastLive
	probed := p.lastProbed
	target := p.target.Model
	p.mu.RUnlock()

	out := make([]InferenceEngine, 0, len(rows))
	for _, r := range rows {
		if r.State != "loaded" && r.State != "loading" {
			continue
		}
		out = append(out, InferenceEngine{
			Provider:            p.name,
			Node:                p.host,
			Model:               r.ID,
			State:               r.State,
			Target:              target != "" && modelIDMatch(r.ID, target),
			LoadedContextLength: r.LoadedContextLength,
			MaxContextLength:    r.MaxContextLength,
			Parallel:            r.Parallel,
			ObservedAt:          probed.UTC(),
		})
	}
	return out
}

// handleInference serves GET /v1/inference.
func (s *Server) handleInference(w http.ResponseWriter, r *http.Request) {
	view := buildInferenceView(r.Context(), s.router, reconcile.ListProviders, reconcile.GetProvider)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(view)
}
