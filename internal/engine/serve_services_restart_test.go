package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// ctxCapturingSupervisor records the context Restart was called with so the
// test can check it outlives the HTTP request.
type ctxCapturingSupervisor struct {
	*stubSupervisor
	mu sync.Mutex
	// midRestart runs inside Restart, standing in for "the old process
	// exits and the caller's connection drops".
	midRestart  func()
	errAfter    error
	hadDeadline bool
	called      bool
}

func (c *ctxCapturingSupervisor) Restart(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error) {
	if c.midRestart != nil {
		c.midRestart()
	}
	c.mu.Lock()
	c.called = true
	c.errAfter = ctx.Err()
	_, c.hadDeadline = ctx.Deadline()
	c.mu.Unlock()
	return c.stubSupervisor.Restart(ctx, name, def)
}

// TestServiceMutation_Restart_DetachedFromRequest pins the self-restart
// contract: the caller may be the service being restarted, so its request
// context is cancelled the moment the old process exits. The restart must
// run on a context that survives that cancellation.
func TestServiceMutation_Restart_DetachedFromRequest(t *testing.T) {
	t.Parallel()
	sup := &ctxCapturingSupervisor{stubSupervisor: newStubSupervisor()}
	handler := newMutationTestServer(t, testManifest(), sup, true)

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup.midRestart = cancel // the caller goes away mid-restart

	req := httptest.NewRequest(http.MethodPost, "/v1/services/kernel/restart", nil).WithContext(reqCtx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	sup.mu.Lock()
	defer sup.mu.Unlock()
	if !sup.called {
		t.Fatal("Restart was not called")
	}
	if reqCtx.Err() == nil {
		t.Fatal("test bug: request context was not cancelled")
	}
	if sup.errAfter != nil {
		t.Fatalf("restart context was cancelled with the request (%v); it must be detached", sup.errAfter)
	}
	if !sup.hadDeadline {
		t.Error("restart context has no deadline; a detached restart must still be bounded")
	}
}

// TestRequiredScope_ServiceMutationIsAdmin pins service control to the admin
// scope: restarting a gateway is not a plain write.
func TestRequiredScope_ServiceMutationIsAdmin(t *testing.T) {
	t.Parallel()
	for _, action := range []string{"start", "stop", "restart", "enable", "disable"} {
		r := httptest.NewRequest(http.MethodPost, "/v1/services/hermes-cog/"+action, nil)
		if got := requiredScopeForRequest(r); got != ScopeAdmin {
			t.Errorf("POST %s: scope=%q; want %q", action, got, ScopeAdmin)
		}
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/services/hermes-cog", nil)
	if got := requiredScopeForRequest(r); got == ScopeAdmin {
		t.Errorf("GET /v1/services/{name} must not require admin")
	}
}

// TestServiceMutation_Restart_Async returns 202 before the restart runs and
// still performs it — the self-restart path.
func TestServiceMutation_Restart_Async(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	done := make(chan struct{})
	sup := &ctxCapturingSupervisor{stubSupervisor: newStubSupervisor()}
	sup.midRestart = func() { <-release; close(done) }
	handler := newMutationTestServer(t, testManifest(), sup, true)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/services/kernel/restart?wait=false", nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d; want 202; body=%q", rec.Code, rec.Body.String())
	}
	close(release) // restart was blocked; the response came back anyway
	<-done
	sup.mu.Lock()
	defer sup.mu.Unlock()
	if !sup.called || sup.errAfter != nil {
		t.Fatalf("background restart: called=%v ctxErr=%v", sup.called, sup.errAfter)
	}
}

// TestServiceMutation_Restart_Async_GateAndKind keeps the 403/409 contract on
// the async path.
func TestServiceMutation_Restart_Async_GateAndKind(t *testing.T) {
	t.Parallel()
	off := newMutationTestServer(t, testManifest(), newStubSupervisor(), false)
	rec := httptest.NewRecorder()
	off.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/services/kernel/restart?wait=false", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("gate off: status=%d; want 403", rec.Code)
	}
	on := newMutationTestServer(t, testManifest(), newStubSupervisor(), true)
	for name := range testManifest().Services {
		def := testManifest().Services[name]
		if def.Kind.EffectiveKind() == ServiceKindManaged {
			continue
		}
		rec := httptest.NewRecorder()
		on.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/services/"+name+"/restart?wait=false", nil))
		if rec.Code != http.StatusConflict {
			t.Errorf("%s (kind=%s): status=%d; want 409", name, def.Kind.EffectiveKind(), rec.Code)
		}
	}
}
