package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/myrgic/cogos/pkg/substrate/reconcile"
)

// inferenceStubProvider is a minimal engine.Provider whose liveness is
// fixed by the test.
type inferenceStubProvider struct {
	name  string
	model string
	live  bool
}

func (p *inferenceStubProvider) Complete(context.Context, *CompletionRequest) (*CompletionResponse, error) {
	return nil, nil
}
func (p *inferenceStubProvider) Stream(context.Context, *CompletionRequest) (<-chan StreamChunk, error) {
	return nil, nil
}
func (p *inferenceStubProvider) Name() string                   { return p.name }
func (p *inferenceStubProvider) Model() string                  { return p.model }
func (p *inferenceStubProvider) Available(context.Context) bool { return p.live }
func (p *inferenceStubProvider) Capabilities() ProviderCapabilities {
	return ProviderCapabilities{IsLocal: true}
}
func (p *inferenceStubProvider) Ping(context.Context) (time.Duration, error) { return 0, nil }

func isolatedReconcilers(m map[string]reconcile.Reconcilable) (func() []string, func(string) (reconcile.Reconcilable, error)) {
	list := func() []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	get := func(k string) (reconcile.Reconcilable, error) {
		p, ok := m[k]
		if !ok {
			return nil, fmt.Errorf("provider %q not found", k)
		}
		return p, nil
	}
	return list, get
}

func newTestRouterWith(ps ...Provider) *SimpleRouter {
	r := NewSimpleRouter(RoutingConfig{})
	for _, p := range ps {
		r.RegisterProvider(p)
	}
	return r
}

// Ledger row 6: /v1/providers said available:true while the reconciler
// said Degraded. The join must flag that as disagreement.
func TestInferenceView_HealthDisagreement(t *testing.T) {
	resetPinResolutionsForTest()
	router := newTestRouterWith(&inferenceStubProvider{name: "lmstudio-eclipse", model: "m", live: true})
	list, get := isolatedReconcilers(map[string]reconcile.Reconcilable{
		lmsModelStateType + "/lmstudio-eclipse": &stubProvider{
			name: "lmstudio-eclipse",
			status: reconcile.ResourceStatus{
				Sync:      reconcile.SyncStatusOutOfSync,
				Health:    reconcile.HealthDegraded,
				Operation: reconcile.OperationIdle,
				Message:   "target not loaded",
			},
		},
	})

	view := buildInferenceView(context.Background(), router, list, get)
	if len(view.Health) != 1 {
		t.Fatalf("health rows = %d, want 1: %+v", len(view.Health), view.Health)
	}
	h := view.Health[0]
	if h.Liveness == nil || !*h.Liveness {
		t.Fatalf("liveness = %v, want true", h.Liveness)
	}
	if h.ReconcilerState != string(reconcile.HealthDegraded) {
		t.Fatalf("reconciler_state = %q", h.ReconcilerState)
	}
	if !h.Disagreement {
		t.Fatal("expected disagreement=true when liveness=true but reconciler=Degraded")
	}
}

func TestHealthDisagrees_Matrix(t *testing.T) {
	cases := []struct {
		live   bool
		health reconcile.HealthStatus
		sync   reconcile.SyncStatus
		want   bool
	}{
		{true, reconcile.HealthHealthy, reconcile.SyncStatusSynced, false},
		{false, reconcile.HealthHealthy, reconcile.SyncStatusSynced, true},
		{true, reconcile.HealthHealthy, reconcile.SyncStatusOutOfSync, true},
		{true, reconcile.HealthDegraded, reconcile.SyncStatusOutOfSync, true},
		{false, reconcile.HealthDegraded, reconcile.SyncStatusOutOfSync, false},
		{true, reconcile.HealthMissing, reconcile.SyncStatusUnknown, true},
		{true, reconcile.HealthSuspended, reconcile.SyncStatusUnknown, false},
		{false, reconcile.HealthSuspended, reconcile.SyncStatusUnknown, false},
		{true, reconcile.HealthProgressing, reconcile.SyncStatusUnknown, false},
	}
	for _, c := range cases {
		got := healthDisagrees(c.live, reconcile.ResourceStatus{Health: c.health, Sync: c.sync})
		if got != c.want {
			t.Errorf("live=%v health=%s sync=%s: got %v want %v", c.live, c.health, c.sync, got, c.want)
		}
	}
}

// Ledger row 5: "configured local model X not loaded, using Y" was only a
// free-text note. It must now surface as a typed fallback reason.
func TestInferenceView_PinReasonTyped(t *testing.T) {
	resetPinResolutionsForTest()
	t.Cleanup(resetPinResolutionsForTest)

	_, note := func() (string, string) {
		m, _, n := resolveDispatchLocalModel([]string{"ornith-1.5-35b"}, "gemma4:e4b", DispatchModelE4B)
		return m, n
	}()
	recordPinResolution("dispatch", "gemma4:e4b", "ornith-1.5-35b", note)
	recordPinResolution("assess", "ornith-1.5-35b", "ornith-1.5-35b", "")

	view := buildInferenceView(context.Background(), nil, func() []string { return nil }, reconcile.GetProvider)
	if len(view.Pins) != 2 {
		t.Fatalf("pins = %d, want 2: %+v", len(view.Pins), view.Pins)
	}
	// Sorted by site: assess, dispatch.
	if view.Pins[0].Site != "assess" || view.Pins[0].Reason != PinReasonDeclared {
		t.Fatalf("assess pin = %+v", view.Pins[0])
	}
	d := view.Pins[1]
	if d.Site != "dispatch" || d.Reason != "fallback:not-loaded" {
		t.Fatalf("dispatch pin = %+v, want reason fallback:not-loaded", d)
	}
	if d.Requested != "gemma4:e4b" || d.Resolved != "ornith-1.5-35b" {
		t.Fatalf("dispatch requested/resolved = %q/%q", d.Requested, d.Resolved)
	}
	if d.Detail == "" {
		t.Fatal("original resolver note must be preserved as detail")
	}
}

func TestClassifyPinNote(t *testing.T) {
	cases := map[string]string{
		"": PinReasonDeclared,
		`configured local model "a" not loaded, using "b"`:   "fallback:not-loaded",
		"26b route unavailable, using preferred local model": "fallback:route-unavailable",
		"26b route unavailable: no local models are loaded":  "fallback:none-loaded",
		"no local models are loaded":                         "fallback:none-loaded",
		"something new":                                      "fallback:other",
	}
	for note, want := range cases {
		if got := classifyPinNote(note); got != want {
			t.Errorf("%q: got %q want %q", note, got, want)
		}
	}
}

// Ledger row 4: the in-flight table existed but was not on any route.
func TestInferenceView_QueueReflectsInflight(t *testing.T) {
	const id = "inference-view-queue-test"
	if !beginInflightInference(id) {
		t.Fatal("begin should succeed")
	}
	view := buildInferenceView(context.Background(), nil, func() []string { return nil }, reconcile.GetProvider)
	found := false
	for _, q := range view.Queue {
		if q.RequestID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("queue %+v does not contain %q", view.Queue, id)
	}

	endInflightInference(id)
	view = buildInferenceView(context.Background(), nil, func() []string { return nil }, reconcile.GetProvider)
	for _, q := range view.Queue {
		if q.RequestID == id {
			t.Fatal("queue still contains released request")
		}
	}
}

// Ledger rows 1–3: the reconciler's cached rows carry what is loaded where
// (with context and parallel) but were only reachable through serve.log.
func TestInferenceView_EnginesFromReconcilerCache(t *testing.T) {
	ctxLen := 65536
	par := 4
	msp := &LMSModelStateProvider{
		name:       "lmstudio-eclipse",
		host:       "192.168.10.191",
		target:     lmsModelStateConfig{Manage: true, Model: "ornith-1.5-35b"},
		lastProbed: time.Now(),
		lastLive: []lmsModelRow{
			{ID: "ornith-1.5-35b-a3b-mlx", State: "loaded", LoadedContextLength: &ctxLen, MaxContextLength: 262144, Parallel: &par},
			{ID: "gemma4:e4b", State: "not-loaded"},
			{ID: "other", State: "loading"},
		},
	}
	list, get := isolatedReconcilers(map[string]reconcile.Reconcilable{
		lmsModelStateType + "/lmstudio-eclipse": msp,
	})
	view := buildInferenceView(context.Background(), nil, list, get)
	if len(view.Engines) != 2 {
		t.Fatalf("engines = %d, want 2 (not-loaded rows excluded): %+v", len(view.Engines), view.Engines)
	}
	e := view.Engines[0]
	if e.Model != "ornith-1.5-35b-a3b-mlx" || e.State != "loaded" || !e.Target {
		t.Fatalf("engine[0] = %+v", e)
	}
	if e.Node != "192.168.10.191" || e.LoadedContextLength == nil || *e.LoadedContextLength != ctxLen || e.Parallel == nil || *e.Parallel != par {
		t.Fatalf("engine[0] fields = %+v", e)
	}
	if view.Engines[1].State != "loading" || view.Engines[1].Target {
		t.Fatalf("engine[1] = %+v", view.Engines[1])
	}
	// Reconciler-only provider (not in router) has no liveness, no disagreement.
	if len(view.Health) != 1 || view.Health[0].Liveness != nil || view.Health[0].Disagreement {
		t.Fatalf("health = %+v", view.Health)
	}
}

func TestHandleInference_HTTP(t *testing.T) {
	resetPinResolutionsForTest()
	s := &Server{router: newTestRouterWith(&inferenceStubProvider{name: "p", model: "m", live: true})}
	rec := httptest.NewRecorder()
	s.handleInference(rec, httptest.NewRequest(http.MethodGet, "/v1/inference", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body InferenceView
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v\n%s", err, rec.Body.String())
	}
	if body.Engines == nil || body.Pins == nil || body.Queue == nil || body.Health == nil {
		t.Fatalf("all sections must be present (empty arrays, not null): %s", rec.Body.String())
	}
	if body.Timestamp.IsZero() {
		t.Fatal("timestamp missing")
	}
}
