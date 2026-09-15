// serve_services_mutations_test.go — Tests for Phase 2 service mutation endpoints.
//
// Covers the operational-semantics spec from the pinned comment on issue #100:
//
//  1. Gate — 403 when EnableServiceControl=false (default).
//  2. Not-found — 404 for unknown service name.
//  3. Not-controllable — 409 for kind=observed and kind=external services.
//  4. Idempotent start — already-running returns 200 + status (not 409).
//  5. Idempotent stop — already-stopped returns 200 + status.
//  6. Stop returns Stopping=true in status.
//  7. Enable/Disable idempotency — succeed even when double-called.
//  8. Response shape — success/action/service_name fields always present.
//  9. Nil manifest — mutation endpoints return 404 (manifest not loaded).
//
// 10. Status method — returns live snapshot without side effects.
// 11. ObserverSupervisor mutations — always ErrNotControllable.
// 12. supervisorFor routing — managed+controller → controller; observed → observer.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ─── Stub supervisor for testing ─────────────────────────────────────────────

// stubSupervisor is a configurable ServiceSupervisor for use in tests.
// Each verb records whether it was called; callers can set stubbed return values.
type stubSupervisor struct {
	mu sync.Mutex

	startCalled   bool
	stopCalled    bool
	restartCalled bool
	enableCalled  bool
	disableCalled bool
	statusCalled  bool

	// Return values; zero values mean success with a minimal status.
	startErr   error
	stopErr    error
	restartErr error
	enableErr  error
	disableErr error
	statusErr  error

	startStatus   *ServiceStatus
	stopStatus    *ServiceStatus
	restartStatus *ServiceStatus
	enableStatus  *ServiceStatus
	disableStatus *ServiceStatus
	statusResult  *ServiceStatus
}

func newStubSupervisor() *stubSupervisor { return &stubSupervisor{} }

func defaultStatus(running bool) *ServiceStatus {
	return &ServiceStatus{
		Running:           running,
		LaunchdRegistered: running,
		At:                time.Now().UTC(),
	}
}

func (s *stubSupervisor) Start(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startCalled = true
	if s.startErr != nil {
		return s.startStatus, s.startErr
	}
	if s.startStatus != nil {
		return s.startStatus, nil
	}
	return defaultStatus(true), nil
}

func (s *stubSupervisor) Stop(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopCalled = true
	if s.stopErr != nil {
		return s.stopStatus, s.stopErr
	}
	if s.stopStatus != nil {
		return s.stopStatus, nil
	}
	st := defaultStatus(false)
	st.Stopping = true
	return st, nil
}

func (s *stubSupervisor) Restart(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.restartCalled = true
	if s.restartErr != nil {
		return s.restartStatus, s.restartErr
	}
	if s.restartStatus != nil {
		return s.restartStatus, nil
	}
	return defaultStatus(true), nil
}

func (s *stubSupervisor) Enable(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enableCalled = true
	if s.enableErr != nil {
		return s.enableStatus, s.enableErr
	}
	if s.enableStatus != nil {
		return s.enableStatus, nil
	}
	return defaultStatus(true), nil
}

func (s *stubSupervisor) Disable(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disableCalled = true
	if s.disableErr != nil {
		return s.disableStatus, s.disableErr
	}
	if s.disableStatus != nil {
		return s.disableStatus, nil
	}
	return defaultStatus(false), nil
}

func (s *stubSupervisor) Status(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCalled = true
	if s.statusErr != nil {
		return s.statusResult, s.statusErr
	}
	if s.statusResult != nil {
		return s.statusResult, nil
	}
	return defaultStatus(true), nil
}

// ─── Test server helper ───────────────────────────────────────────────────────

// newMutationTestServer returns an HTTP handler backed by a Server wired with
// the supplied manifest and supervisor. enableServiceControl gates mutations.
func newMutationTestServer(
	t *testing.T,
	manifest *NodeManifest,
	sup ServiceSupervisor,
	enableServiceControl bool,
) http.Handler {
	t.Helper()
	root := t.TempDir()
	cfg := makeConfig(t, root)
	cfg.EnableServiceControl = enableServiceControl
	nucleus := makeNucleus("Test", "tester")
	proc := NewProcess(cfg, nucleus)
	proc.nodeManifest = manifest

	srv := NewServer(cfg, nucleus, proc)
	if sup != nil {
		srv.SetServiceSupervisor(sup)
	}
	t.Cleanup(func() {
		if b := proc.Broker(); b != nil {
			_ = b.Close()
		}
	})
	return srv.Handler()
}

// postAction is a helper that issues POST /v1/services/{name}/{action}.
func postAction(handler http.Handler, name, action string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/services/"+name+"/"+action, nil)
	handler.ServeHTTP(rec, req)
	return rec
}

// decodeMutationResp decodes a serviceMutationResponse from a recorder.
func decodeMutationResp(t *testing.T, rec *httptest.ResponseRecorder) serviceMutationResponse {
	t.Helper()
	var resp serviceMutationResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode mutation response: %v; body=%q", err, rec.Body.String())
	}
	return resp
}

// ─── Gate tests ──────────────────────────────────────────────────────────────

// TestServiceMutation_GateDisabled verifies all mutation endpoints return 403
// when EnableServiceControl is false (the default).
func TestServiceMutation_GateDisabled(t *testing.T) {
	t.Parallel()
	handler := newMutationTestServer(t, testManifest(), newStubSupervisor(), false)

	for _, action := range []string{"start", "stop", "restart", "enable", "disable"} {
		action := action
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := postAction(handler, "kernel", action)
			if rec.Code != http.StatusForbidden {
				t.Errorf("action=%q: status=%d; want 403", action, rec.Code)
			}
			resp := decodeMutationResp(t, rec)
			if resp.Error == "" {
				t.Errorf("action=%q: error field empty; want non-empty", action)
			}
			if !strings.Contains(resp.Error, "disabled") {
				t.Errorf("action=%q: error=%q; want 'disabled'", action, resp.Error)
			}
		})
	}
}

// ─── Not-found tests ─────────────────────────────────────────────────────────

// TestServiceMutation_NotFound verifies 404 for an unknown service name.
func TestServiceMutation_NotFound(t *testing.T) {
	t.Parallel()
	handler := newMutationTestServer(t, testManifest(), newStubSupervisor(), true)

	for _, action := range []string{"start", "stop", "restart", "enable", "disable"} {
		action := action
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := postAction(handler, "nonexistent", action)
			if rec.Code != http.StatusNotFound {
				t.Errorf("action=%q: status=%d; want 404", action, rec.Code)
			}
		})
	}
}

// TestServiceMutation_NilManifest verifies 404 when no manifest is loaded.
func TestServiceMutation_NilManifest(t *testing.T) {
	t.Parallel()
	handler := newMutationTestServer(t, nil, newStubSupervisor(), true)

	rec := postAction(handler, "kernel", "start")
	if rec.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404 for nil manifest", rec.Code)
	}
}

// ─── Not-controllable tests ───────────────────────────────────────────────────

// TestServiceMutation_ObservedReturns409 verifies that mutations on
// kind=observed services return 409 regardless of the supervisor.
func TestServiceMutation_ObservedReturns409(t *testing.T) {
	t.Parallel()
	// Use a stub that would succeed for managed services — the router should
	// still select ObserverSupervisor for "ollama" (kind=observed).
	handler := newMutationTestServer(t, testManifest(), newStubSupervisor(), true)

	for _, action := range []string{"start", "stop", "restart", "enable", "disable"} {
		action := action
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := postAction(handler, "ollama", action)
			if rec.Code != http.StatusConflict {
				t.Errorf("action=%q: status=%d; want 409 for observed service", action, rec.Code)
			}
			resp := decodeMutationResp(t, rec)
			if resp.Success {
				t.Errorf("action=%q: success=true; want false for observed service", action)
			}
		})
	}
}

// TestServiceMutation_ExternalReturns409 verifies 409 for kind=external.
func TestServiceMutation_ExternalReturns409(t *testing.T) {
	t.Parallel()
	handler := newMutationTestServer(t, testManifest(), newStubSupervisor(), true)

	rec := postAction(handler, "gateway", "start")
	if rec.Code != http.StatusConflict {
		t.Errorf("status=%d; want 409 for external service", rec.Code)
	}
}

// ─── Happy-path tests ─────────────────────────────────────────────────────────

// TestServiceMutation_Start_Success verifies a successful start returns 200
// with success=true and a status object.
func TestServiceMutation_Start_Success(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	stub.startStatus = &ServiceStatus{
		Running:           true,
		LaunchdRegistered: true,
		PID:               12345,
		At:                time.Now().UTC(),
	}
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "start")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	resp := decodeMutationResp(t, rec)
	if !resp.Success {
		t.Errorf("success=false; want true")
	}
	if resp.Action != "start" {
		t.Errorf("action=%q; want %q", resp.Action, "start")
	}
	if resp.ServiceName != "kernel" {
		t.Errorf("service_name=%q; want %q", resp.ServiceName, "kernel")
	}
	if resp.Status == nil {
		t.Fatal("status=nil; want non-nil")
	}
	if !resp.Status.Running {
		t.Errorf("status.running=false; want true")
	}
	if resp.Status.PID != 12345 {
		t.Errorf("status.pid=%d; want 12345", resp.Status.PID)
	}
	if !stub.startCalled {
		t.Errorf("stub.startCalled=false; want true")
	}
}

// TestServiceMutation_Stop_ReturnsStopping verifies that stop returns
// Stopping=true in the status object.
func TestServiceMutation_Stop_ReturnsStopping(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	// Stop returns Stopping=true by default in stubSupervisor.
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "stop")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	resp := decodeMutationResp(t, rec)
	if !resp.Success {
		t.Errorf("success=false; want true")
	}
	if resp.Status == nil {
		t.Fatal("status=nil; want non-nil")
	}
	if !resp.Status.Stopping {
		t.Errorf("status.stopping=false; want true")
	}
}

// TestServiceMutation_Restart_Success verifies restart returns 200.
func TestServiceMutation_Restart_Success(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "restart")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	resp := decodeMutationResp(t, rec)
	if !resp.Success {
		t.Errorf("success=false; want true")
	}
	if !stub.restartCalled {
		t.Errorf("stub.restartCalled=false; want true")
	}
}

// TestServiceMutation_Enable_Success verifies enable returns 200.
func TestServiceMutation_Enable_Success(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "enable")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	resp := decodeMutationResp(t, rec)
	if !resp.Success {
		t.Errorf("success=false; want true")
	}
	if !stub.enableCalled {
		t.Errorf("stub.enableCalled=false; want true")
	}
}

// TestServiceMutation_Disable_Success verifies disable returns 200.
func TestServiceMutation_Disable_Success(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "disable")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200; body=%q", rec.Code, rec.Body.String())
	}

	resp := decodeMutationResp(t, rec)
	if !resp.Success {
		t.Errorf("success=false; want true")
	}
	if !stub.disableCalled {
		t.Errorf("stub.disableCalled=false; want true")
	}
}

// ─── Idempotency tests ────────────────────────────────────────────────────────

// TestServiceMutation_Start_IdempotentWhenAlreadyRunning verifies that
// starting an already-running service returns 200, not 409.
func TestServiceMutation_Start_IdempotentWhenAlreadyRunning(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	// Simulate: service is already running; Start returns running status.
	stub.startStatus = &ServiceStatus{
		Running:           true,
		LaunchdRegistered: true,
		At:                time.Now().UTC(),
	}
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "start")
	if rec.Code != http.StatusOK {
		t.Errorf("start on running service: status=%d; want 200 (idempotent)", rec.Code)
	}

	resp := decodeMutationResp(t, rec)
	if !resp.Success {
		t.Errorf("success=false; want true for idempotent start")
	}
}

// TestServiceMutation_Stop_IdempotentWhenAlreadyStopped verifies that
// stopping an already-stopped service returns 200, not an error.
func TestServiceMutation_Stop_IdempotentWhenAlreadyStopped(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	stub.stopStatus = &ServiceStatus{
		Running:           false,
		LaunchdRegistered: false,
		Stopping:          false,
		At:                time.Now().UTC(),
	}
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "stop")
	if rec.Code != http.StatusOK {
		t.Errorf("stop on stopped service: status=%d; want 200 (idempotent)", rec.Code)
	}
}

// TestServiceMutation_Enable_Idempotent verifies that enabling a service
// twice returns 200 both times.
func TestServiceMutation_Enable_Idempotent(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	handler := newMutationTestServer(t, testManifest(), stub, true)

	for i := 0; i < 2; i++ {
		rec := postAction(handler, "kernel", "enable")
		if rec.Code != http.StatusOK {
			t.Errorf("enable call %d: status=%d; want 200", i+1, rec.Code)
		}
	}
}

// TestServiceMutation_Disable_Idempotent verifies that disabling a service
// twice returns 200 both times.
func TestServiceMutation_Disable_Idempotent(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	handler := newMutationTestServer(t, testManifest(), stub, true)

	for i := 0; i < 2; i++ {
		rec := postAction(handler, "kernel", "disable")
		if rec.Code != http.StatusOK {
			t.Errorf("disable call %d: status=%d; want 200", i+1, rec.Code)
		}
	}
}

// ─── Error shape tests ────────────────────────────────────────────────────────

// TestServiceMutation_ErrorShape verifies that on error the response always
// includes success=false, error string, and launchctl_exit_code.
func TestServiceMutation_ErrorShape(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	stub.startErr = errors.New("launchd transient failure")
	stub.startStatus = &ServiceStatus{
		Running:           false,
		LaunchctlExitCode: 125,
		At:                time.Now().UTC(),
	}
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "start")
	if rec.Code == http.StatusOK {
		t.Errorf("status=200; want non-200 on error")
	}

	resp := decodeMutationResp(t, rec)
	if resp.Success {
		t.Errorf("success=true; want false on error")
	}
	if resp.Error == "" {
		t.Errorf("error field empty; want non-empty")
	}
	if resp.LaunchctlExitCode != 125 {
		t.Errorf("launchctl_exit_code=%d; want 125", resp.LaunchctlExitCode)
	}
}

// ─── Spec item 4: 503 for transient launchd errors ───────────────────────────

// TestServiceMutation_TransientLaunchd503 verifies that when the supervisor
// returns an error wrapping ErrLaunchctlTransient (launchctl exit 125),
// dispatchMutation maps it to HTTP 503 per the operational-semantics spec
// (pinned comment on issue #100, spec item 4).
func TestServiceMutation_TransientLaunchd503(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	// Wrap the sentinel the same way LaunchctlController does: %w chain so that
	// errors.Is traversal finds ErrLaunchctlTransient.
	stub.startErr = fmt.Errorf("%w: launchctl kickstart exited 125", ErrLaunchctlTransient)
	stub.startStatus = &ServiceStatus{
		Running:           false,
		LaunchctlExitCode: 125,
		At:                time.Now().UTC(),
	}
	handler := newMutationTestServer(t, testManifest(), stub, true)

	rec := postAction(handler, "kernel", "start")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status=%d; want 503 for ErrLaunchctlTransient", rec.Code)
	}

	resp := decodeMutationResp(t, rec)
	if resp.Success {
		t.Errorf("success=true; want false for transient error")
	}
	if resp.LaunchctlExitCode != 125 {
		t.Errorf("launchctl_exit_code=%d; want 125", resp.LaunchctlExitCode)
	}
}

// ─── ObserverSupervisor unit tests ───────────────────────────────────────────

// TestObserverSupervisor_MutationsReturnNotControllable verifies every mutation
// verb on ObserverSupervisor returns ErrNotControllable.
func TestObserverSupervisor_MutationsReturnNotControllable(t *testing.T) {
	t.Parallel()
	obs := &ObserverSupervisor{}
	ctx := context.Background()
	def := ServiceDef{}

	_, err := obs.Start(ctx, "test", def)
	if !errors.Is(err, ErrNotControllable) {
		t.Errorf("Start: err=%v; want ErrNotControllable", err)
	}
	_, err = obs.Stop(ctx, "test", def)
	if !errors.Is(err, ErrNotControllable) {
		t.Errorf("Stop: err=%v; want ErrNotControllable", err)
	}
	_, err = obs.Restart(ctx, "test", def)
	if !errors.Is(err, ErrNotControllable) {
		t.Errorf("Restart: err=%v; want ErrNotControllable", err)
	}
	_, err = obs.Enable(ctx, "test", def)
	if !errors.Is(err, ErrNotControllable) {
		t.Errorf("Enable: err=%v; want ErrNotControllable", err)
	}
	_, err = obs.Disable(ctx, "test", def)
	if !errors.Is(err, ErrNotControllable) {
		t.Errorf("Disable: err=%v; want ErrNotControllable", err)
	}
}

// TestObserverSupervisor_StatusReturnsSnapshot verifies Status returns a
// valid snapshot with running=false without error.
func TestObserverSupervisor_StatusReturnsSnapshot(t *testing.T) {
	t.Parallel()
	obs := &ObserverSupervisor{}
	st, err := obs.Status(context.Background(), "test", ServiceDef{})
	if err != nil {
		t.Fatalf("Status: unexpected error: %v", err)
	}
	if st == nil {
		t.Fatal("Status: returned nil")
	}
	if st.Running {
		t.Errorf("Status.Running=true; want false for ObserverSupervisor")
	}
	if st.LaunchdRegistered {
		t.Errorf("Status.LaunchdRegistered=true; want false for ObserverSupervisor")
	}
}

// ─── supervisorFor routing tests ─────────────────────────────────────────────

// TestSupervisorFor_ManagedUsesController verifies that supervisorFor returns
// the provided controller for kind=managed services.
func TestSupervisorFor_ManagedUsesController(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	def := ServiceDef{Kind: ServiceKindManaged}
	sup := supervisorFor(def, stub)
	if sup != stub {
		t.Errorf("supervisorFor managed: returned %T; want *stubSupervisor", sup)
	}
}

// TestSupervisorFor_ManagedNilControllerFallsBack verifies that when no
// controller is provided, managed services fall back to ObserverSupervisor.
func TestSupervisorFor_ManagedNilControllerFallsBack(t *testing.T) {
	t.Parallel()
	def := ServiceDef{Kind: ServiceKindManaged}
	sup := supervisorFor(def, nil)
	if _, ok := sup.(*ObserverSupervisor); !ok {
		t.Errorf("supervisorFor managed+nil: returned %T; want *ObserverSupervisor", sup)
	}
}

// TestSupervisorFor_ObservedUsesObserver verifies that supervisorFor returns
// ObserverSupervisor for kind=observed regardless of controller.
func TestSupervisorFor_ObservedUsesObserver(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	def := ServiceDef{Kind: ServiceKindObserved}
	sup := supervisorFor(def, stub)
	if _, ok := sup.(*ObserverSupervisor); !ok {
		t.Errorf("supervisorFor observed: returned %T; want *ObserverSupervisor", sup)
	}
}

// TestSupervisorFor_ExternalUsesObserver verifies kind=external routes to
// ObserverSupervisor.
func TestSupervisorFor_ExternalUsesObserver(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	def := ServiceDef{Kind: ServiceKindExternal}
	sup := supervisorFor(def, stub)
	if _, ok := sup.(*ObserverSupervisor); !ok {
		t.Errorf("supervisorFor external: returned %T; want *ObserverSupervisor", sup)
	}
}

// TestSupervisorFor_DefaultKindUsesController verifies that a service with no
// explicit kind (defaults to "managed") uses the provided controller.
func TestSupervisorFor_DefaultKindUsesController(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	def := ServiceDef{} // Kind="" → EffectiveKind()="managed"
	sup := supervisorFor(def, stub)
	if sup != stub {
		t.Errorf("supervisorFor default kind: returned %T; want *stubSupervisor", sup)
	}
}

// ─── Response-shape invariants ────────────────────────────────────────────────

// TestServiceMutation_ResponseShapeInvariants verifies that action and
// service_name fields are always populated in mutation responses.
func TestServiceMutation_ResponseShapeInvariants(t *testing.T) {
	t.Parallel()
	stub := newStubSupervisor()
	handler := newMutationTestServer(t, testManifest(), stub, true)

	for _, action := range []string{"start", "stop", "restart", "enable", "disable"} {
		action := action
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := postAction(handler, "kernel", action)
			resp := decodeMutationResp(t, rec)

			if resp.Action != action {
				t.Errorf("action=%q; want %q", resp.Action, action)
			}
			if resp.ServiceName != "kernel" {
				t.Errorf("service_name=%q; want %q", resp.ServiceName, "kernel")
			}
		})
	}
}
