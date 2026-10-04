// service_supervisor.go — ServiceSupervisor interface and shared types.
//
// ProcessSupervisor (in pkg/modality) is the subprocess lifecycle manager for
// modality modules. This is a distinct concept: ServiceSupervisor is the
// control-plane abstraction for kernel-declared services, routing mutations
// (start/stop/restart/enable/disable) through kind-aware implementations.
//
// Two implementations ship with Phase 2:
//
//   - LaunchctlController (service_supervisor_launchctl.go): macOS-only.
//     Wraps launchctl load/unload/kickstart/stop for kind=managed services
//     that declare a launchd label.
//   - ObserverSupervisor: read-only stub that returns ErrNotControllable for
//     every mutation. Used for kind=observed and kind=external services.
//
// Phase 3 (#101) will add DirectSupervisor (dev-mode) and SystemdSupervisor
// (Linux); the interface is designed to accommodate them without changes.
package engine

import (
	"context"
	"errors"
	"time"
)

// ErrNotControllable is returned by supervisors that do not support lifecycle
// mutations (e.g. ObserverSupervisor for kind=observed|external services).
var ErrNotControllable = errors.New("service is not controllable: kind=observed or kind=external")

// ErrServiceNotFound is returned when the named service does not exist in the
// manifest. Distinct from ErrNotControllable so callers can map to 404 vs 409.
var ErrServiceNotFound = errors.New("service not found in manifest")

// ErrLaunchctlTransient is returned when launchctl exits with code 125,
// indicating a transient launchd error ("service failed"). Maps to HTTP 503.
// Wrapping with %w preserves the original error message for diagnostics.
var ErrLaunchctlTransient = errors.New("transient launchd error (launchctl exit 125)")

// ServiceStatus is the live state snapshot returned by ServiceSupervisor.Status.
// All fields are launchd-authoritative; /health is a bonus signal for Healthy.
type ServiceStatus struct {
	// Running indicates the process is alive (launchd PID != 0).
	Running bool `json:"running"`
	// LaunchdRegistered indicates the job is registered with launchd
	// (loaded), even if the process is not currently alive.
	LaunchdRegistered bool `json:"launchd_registered"`
	// PID is the live process identifier. Zero when not running.
	PID int `json:"pid,omitempty"`
	// ExitCode is the most recent exit code reported by launchd.
	// Zero when the process has not exited or is still running.
	ExitCode int `json:"exit_code,omitempty"`
	// Stopping indicates a graceful shutdown is in progress (SIGTERM sent,
	// waiting for process to exit before SIGKILL).
	Stopping bool `json:"stopping,omitempty"`
	// Healthy reflects the last /health probe result. Nil when no probe has
	// been run or when the supervisor does not support health checking.
	Healthy *bool `json:"healthy,omitempty"`
	// At is the wall-clock time when this snapshot was captured.
	At time.Time `json:"at"`
	// LaunchctlExitCode is the raw exit code from the launchctl subprocess.
	// Included for diagnostics; zero on success.
	LaunchctlExitCode int `json:"launchctl_exit_code,omitempty"`
	// PreviousPID is the PID observed before a restart (restart only).
	PreviousPID int `json:"previous_pid,omitempty"`
	// Restarted is true when a restart was confirmed by observing a live PID
	// different from PreviousPID (restart only).
	Restarted bool `json:"restarted,omitempty"`
}

// ServiceSupervisor is the control-plane interface for a single service kind.
// The HTTP handlers select the correct implementation based on ServiceDef.Kind.
//
// All methods accept context.Context as the first argument so callers can
// propagate deadlines (e.g. from the HTTP request context).
//
// Idempotency contract (from the pinned operational-semantics spec):
//
//   - Start on an already-running service returns 200 + current status (not 409).
//   - Stop on an already-stopped service returns 200 + current status (not 409).
//   - Enable/Disable are idempotent (unload of a missing job is harmless).
//   - Restart serialises concurrent calls: the second concurrent call waits for
//     the first to complete and then returns the same status.
type ServiceSupervisor interface {
	// Start ensures the service is running. Idempotent: already-running → 200.
	// If the service is registered in launchd but not running, it kickstarts it.
	// If the service is not registered, it loads the plist first.
	Start(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error)

	// Stop sends SIGTERM and waits up to 5 s, then SIGKILL. Returns immediately
	// with Stopping=true; the caller can poll Status for confirmation.
	Stop(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error)

	// Restart restarts the service and reports whether a new process was
	// observed. Must complete even if the caller's connection drops (the
	// caller may be the service being restarted). Per-service mutex
	// serialises concurrent restart requests.
	Restart(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error)

	// Enable marks the service as load-at-boot. Idempotent.
	Enable(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error)

	// Disable unloads the service from launchd and marks it disabled. Idempotent.
	Disable(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error)

	// Status returns the current live state snapshot without side effects.
	Status(ctx context.Context, name string, def ServiceDef) (*ServiceStatus, error)
}

// ObserverSupervisor is the read-only ServiceSupervisor for kind=observed and
// kind=external services. Every mutation returns ErrNotControllable; Status
// returns a minimal snapshot (running=false, launchd_registered=false).
type ObserverSupervisor struct{}

var _ ServiceSupervisor = (*ObserverSupervisor)(nil)

func (o *ObserverSupervisor) Start(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	return nil, ErrNotControllable
}

func (o *ObserverSupervisor) Stop(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	return nil, ErrNotControllable
}

func (o *ObserverSupervisor) Restart(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	return nil, ErrNotControllable
}

func (o *ObserverSupervisor) Enable(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	return nil, ErrNotControllable
}

func (o *ObserverSupervisor) Disable(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	return nil, ErrNotControllable
}

func (o *ObserverSupervisor) Status(_ context.Context, _ string, _ ServiceDef) (*ServiceStatus, error) {
	return &ServiceStatus{
		Running:           false,
		LaunchdRegistered: false,
		At:                time.Now().UTC(),
	}, nil
}

// supervisorFor returns the correct ServiceSupervisor implementation for the
// given service definition, consulting the node-level controller if managed.
// controller may be nil (on non-darwin platforms or when launchd is absent);
// in that case managed services use the ObserverSupervisor fallback.
func supervisorFor(def ServiceDef, controller ServiceSupervisor) ServiceSupervisor {
	kind := def.Kind.EffectiveKind()
	if kind == ServiceKindManaged && controller != nil {
		return controller
	}
	return &ObserverSupervisor{}
}
