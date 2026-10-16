// serve_services_test.go — HTTP handler tests for GET /v1/services.
//
// Covers:
//
//	GET /v1/services         — list all declared services (list, count, fields)
//	GET /v1/services/{name}  — single-service projection
//	kind defaulting           — omitted kind defaults to "managed"
//	controllable              — false on observed and external services
//	404                       — unknown service name
//	nil manifest              — empty response when process has no manifest
package engine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// newServicesTestServer returns an HTTP handler backed by a Server wired with
// the supplied manifest. health is optional; pass nil to skip health wiring.
func newServicesTestServer(t *testing.T, manifest *NodeManifest, health *NodeHealth) http.Handler {
	t.Helper()
	root := t.TempDir()
	cfg := makeConfig(t, root)
	nucleus := makeNucleus("Test", "tester")
	proc := NewProcess(cfg, nucleus)

	// Inject the test manifest and health directly so we don't depend on
	// disk layout or the heartbeat ticker.
	proc.nodeManifest = manifest
	if health != nil {
		proc.nodeHealth = health
	}

	srv := NewServer(cfg, nucleus, proc)
	t.Cleanup(func() {
		if b := proc.Broker(); b != nil {
			_ = b.Close()
		}
	})
	return srv.Handler()
}

// testManifest builds a small NodeManifest for use in multiple test cases.
func testManifest() *NodeManifest {
	return &NodeManifest{
		APIVersion: "cog.os/v1",
		Kind:       "NodeManifest",
		Services: map[string]ServiceDef{
			"kernel": {
				Port:      6931,
				Command:   "cogos serve",
				Health:    "/health",
				Restart:   "always",
				Launchd:   "com.cogos.kernel",
				DependsOn: []string{},
			},
			"mod3": {
				Port:      7860,
				Command:   "{{venv}}/bin/python server.py",
				Venv:      "apps/tts-mcp/.venv",
				Health:    "/health",
				Restart:   "on-failure",
				DependsOn: []string{},
			},
			"ollama": {
				Kind:      ServiceKindObserved,
				Port:      11434,
				Health:    "/api/tags",
				DependsOn: []string{},
			},
			"gateway": {
				Kind:      ServiceKindExternal,
				Port:      18789,
				Command:   "example-gateway serve",
				Health:    "/health",
				Restart:   "always",
				DependsOn: []string{"kernel"},
			},
		},
	}
}

// TestServicesList_Count verifies the list endpoint returns all declared services.
func TestServicesList_Count(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, testManifest(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	var resp struct {
		Services []serviceView `json:"services"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Services) != 4 {
		t.Errorf("len(services) = %d; want 4", len(resp.Services))
	}
}

// TestServicesList_SortedByName confirms the list is returned in sorted order.
func TestServicesList_SortedByName(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, testManifest(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services", nil)
	handler.ServeHTTP(rec, req)

	var resp struct {
		Services []serviceView `json:"services"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&resp)

	names := make([]string, len(resp.Services))
	for i, s := range resp.Services {
		names[i] = s.Name
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] > names[i] {
			t.Errorf("services not sorted: %q before %q", names[i-1], names[i])
		}
	}
}

// TestServicesGet_Single verifies the single-service endpoint returns the
// correct service and 200 status.
func TestServicesGet_Single(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, testManifest(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services/kernel", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	var view serviceView
	if err := json.NewDecoder(rec.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if view.Name != "kernel" {
		t.Errorf("name = %q; want %q", view.Name, "kernel")
	}
	if view.Port != 6931 {
		t.Errorf("port = %d; want 6931", view.Port)
	}
}

// TestServicesGet_NotFound verifies 404 for an unknown service name.
func TestServicesGet_NotFound(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, testManifest(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services/nonexistent", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404", rec.Code)
	}
}

// TestServicesKindDefaulting verifies that a service with no explicit kind
// returns "managed" and controllable=true.
func TestServicesKindDefaulting(t *testing.T) {
	t.Parallel()
	// "kernel" and "mod3" have no explicit kind in testManifest().
	handler := newServicesTestServer(t, testManifest(), nil)

	for _, name := range []string{"kernel", "mod3"} {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/services/"+name, nil)
			handler.ServeHTTP(rec, req)

			var view serviceView
			if err := json.NewDecoder(rec.Body).Decode(&view); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if view.Kind != ServiceKindManaged {
				t.Errorf("kind = %q; want %q", view.Kind, ServiceKindManaged)
			}
			if !view.Controllable {
				t.Errorf("controllable = false; want true for managed service %q", name)
			}
		})
	}
}

// TestServicesControllableFalseOnObserved verifies controllable=false for
// "observed" kind services.
func TestServicesControllableFalseOnObserved(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, testManifest(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services/ollama", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	var view serviceView
	if err := json.NewDecoder(rec.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if view.Kind != ServiceKindObserved {
		t.Errorf("kind = %q; want %q", view.Kind, ServiceKindObserved)
	}
	if view.Controllable {
		t.Errorf("controllable = true; want false for observed service")
	}
	if view.Supervisor != "observer" {
		t.Errorf("supervisor = %q; want %q", view.Supervisor, "observer")
	}
}

// TestServicesControllableFalseOnExternal verifies controllable=false for
// "external" kind services.
func TestServicesControllableFalseOnExternal(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, testManifest(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services/gateway", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}

	var view serviceView
	if err := json.NewDecoder(rec.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if view.Kind != ServiceKindExternal {
		t.Errorf("kind = %q; want %q", view.Kind, ServiceKindExternal)
	}
	if view.Controllable {
		t.Errorf("controllable = true; want false for external service")
	}
}

// TestServicesList_NilManifest verifies the list endpoint returns an empty
// services array (not an error) when no manifest is loaded.
func TestServicesList_NilManifest(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, nil, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}

	var resp struct {
		Services []serviceView `json:"services"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Services) != 0 {
		t.Errorf("len(services) = %d; want 0 for nil manifest", len(resp.Services))
	}
}

// TestServicesHealthProjection verifies that health probe data is surfaced
// in the response when available.
func TestServicesHealthProjection(t *testing.T) {
	t.Parallel()

	nh := NewNodeHealth()
	// Manually inject a health snapshot for "mod3".
	nh.mu.Lock()
	nh.services["mod3"] = ServiceHealth{
		Port:   7860,
		Status: "healthy",
	}
	nh.mu.Unlock()

	handler := newServicesTestServer(t, testManifest(), nh)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services/mod3", nil)
	handler.ServeHTTP(rec, req)

	var view serviceView
	if err := json.NewDecoder(rec.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !view.Running {
		t.Errorf("running = false; want true when health status is 'healthy'")
	}
	if view.Health == nil {
		t.Fatal("health = nil; want non-nil when probe data is available")
	}
	if view.Health.Status != "healthy" {
		t.Errorf("health.status = %q; want %q", view.Health.Status, "healthy")
	}
	if view.Health.Endpoint != "/health" {
		t.Errorf("health.endpoint = %q; want %q", view.Health.Endpoint, "/health")
	}
}

// TestServicesLaunchdSupervisor verifies supervisor="launchctl" when
// the service has a launchd label.
func TestServicesLaunchdSupervisor(t *testing.T) {
	t.Parallel()
	handler := newServicesTestServer(t, testManifest(), nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/services/kernel", nil)
	handler.ServeHTTP(rec, req)

	var view serviceView
	_ = json.NewDecoder(rec.Body).Decode(&view)

	if view.Supervisor != "launchctl" {
		t.Errorf("supervisor = %q; want %q", view.Supervisor, "launchctl")
	}
	if view.SupervisorLabel != "com.cogos.kernel" {
		t.Errorf("supervisor_label = %q; want %q", view.SupervisorLabel, "com.cogos.kernel")
	}
}
