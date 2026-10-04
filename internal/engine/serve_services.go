// serve_services.go — /v1/services API (read-only Phase 1 + mutations Phase 2).
//
// Routes:
//
//	GET  /v1/services                    — list all declared services with live health
//	GET  /v1/services/{name}             — single service projection
//	POST /v1/services/{name}/start       — start a managed service
//	POST /v1/services/{name}/stop        — stop a managed service
//	POST /v1/services/{name}/restart     — restart a managed service
//	POST /v1/services/{name}/enable      — enable a managed service (boot persistence)
//	POST /v1/services/{name}/disable     — disable a managed service (remove boot persistence)
//
// Design:
//   - GET routes project from *NodeManifest (static declaration) and *NodeHealth
//     (last probe snapshot). Both are held on *Process.
//   - Mutation routes are gated by Config.EnableServiceControl (default false).
//     Each mutation calls s.serviceSupervisor; if nil, defaults to ObserverSupervisor.
//   - "kind" defaults to "managed" when omitted from the manifest.
//   - "controllable" is false for observed and external services.
//   - Mutations on observed/external services return 409 with ErrNotControllable.
//
// Registered via s.route() so the routes auto-appear in GET /v1/manifest.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"
)

// serviceView is the JSON projection returned by /v1/services.
type serviceView struct {
	Name            string          `json:"name"`
	Kind            ServiceKind     `json:"kind"`
	Port            int             `json:"port"`
	Supervisor      string          `json:"supervisor"`
	SupervisorLabel string          `json:"supervisor_label,omitempty"`
	Controllable    bool            `json:"controllable"`
	Running         bool            `json:"running"`
	PID             *int            `json:"pid"`
	ExitCode        int             `json:"exit_code"`
	Health          *serviceHealthV `json:"health,omitempty"`
	DependsOn       []string        `json:"depends_on"`
	Command         string          `json:"command,omitempty"`
	RestartPolicy   string          `json:"restart_policy,omitempty"`
}

// serviceHealthV is the health sub-object embedded in serviceView.
type serviceHealthV struct {
	Status   string    `json:"status"`
	Endpoint string    `json:"endpoint"`
	ProbedAt time.Time `json:"probed_at"`
}

// registerServiceRoutes wires GET /v1/services and GET /v1/services/{name}.
func (s *Server) registerServiceRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/services", s.handleServicesList)
	s.route(mux, "GET /v1/services/{name}", s.handleServicesGet)
}

// registerServiceMutationRoutes wires the Phase 2 mutation endpoints.
// All are gated by Config.EnableServiceControl.
func (s *Server) registerServiceMutationRoutes(mux *http.ServeMux) {
	s.route(mux, "POST /v1/services/{name}/start", s.handleServiceStart)
	s.route(mux, "POST /v1/services/{name}/stop", s.handleServiceStop)
	s.route(mux, "POST /v1/services/{name}/restart", s.handleServiceRestart)
	s.route(mux, "POST /v1/services/{name}/enable", s.handleServiceEnable)
	s.route(mux, "POST /v1/services/{name}/disable", s.handleServiceDisable)
}

// handleServicesList returns all declared services sorted by name.
//
//	GET /v1/services
//	200 → { services: [...] }
func (s *Server) handleServicesList(w http.ResponseWriter, r *http.Request) {
	manifest := s.process.NodeManifest()
	if manifest == nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"services": []serviceView{},
		})
		return
	}

	healthSnap := s.process.NodeHealth().Snapshot()

	names := make([]string, 0, len(manifest.Services))
	for name := range manifest.Services {
		names = append(names, name)
	}
	sort.Strings(names)

	views := make([]serviceView, 0, len(names))
	for _, name := range names {
		def := manifest.Services[name]
		views = append(views, projectService(name, def, healthSnap[name]))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"services": views,
	})
}

// handleServicesGet returns a single service by name.
//
//	GET /v1/services/{name}
//	200 → serviceView
//	404 → { error: "..." }
func (s *Server) handleServicesGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	manifest := s.process.NodeManifest()
	if manifest == nil {
		http.Error(w, `{"error":"manifest not loaded"}`, http.StatusNotFound)
		return
	}

	def, ok := manifest.Services[name]
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found: " + name})
		return
	}

	healthSnap := s.process.NodeHealth().Snapshot()
	view := projectService(name, def, healthSnap[name])

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(view)
}

// projectService converts a ServiceDef + live ServiceHealth into a serviceView.
// All fields not yet tracked in the manifest (pid, exit_code) are defaulted;
// Phase 2 will wire live process state.
func projectService(name string, def ServiceDef, h ServiceHealth) serviceView {
	kind := def.Kind.EffectiveKind()
	controllable := kind == ServiceKindManaged

	// Supervisor classification: launchctl if a launchd label is present;
	// "observer" for observed/external (kernel doesn't supervise them);
	// "direct" for managed services launched without launchd.
	var supervisor string
	switch kind {
	case ServiceKindObserved, ServiceKindExternal:
		supervisor = "observer"
	default:
		if def.Launchd != "" {
			supervisor = "launchctl"
		} else {
			supervisor = "direct"
		}
	}

	// running is inferred from the last health probe. A zero-value
	// ServiceHealth (no probe yet) reports status "" which is treated as not
	// running. Phase 2 will wire live pid/exit_code from the supervisor.
	running := h.Status == "healthy" || h.Status == "degraded"

	var healthV *serviceHealthV
	if h.Status != "" {
		healthV = &serviceHealthV{
			Status:   h.Status,
			Endpoint: def.Health,
			ProbedAt: h.At,
		}
	}

	dependsOn := def.DependsOn
	if dependsOn == nil {
		dependsOn = []string{}
	}

	// Resolve command template placeholder: {{venv}} → def.Venv path.
	cmd := def.Command
	if def.Venv != "" {
		cmd = strings.ReplaceAll(cmd, "{{venv}}", def.Venv)
	}

	return serviceView{
		Name:            name,
		Kind:            kind,
		Port:            def.Port,
		Supervisor:      supervisor,
		SupervisorLabel: def.Launchd,
		Controllable:    controllable,
		Running:         running,
		PID:             nil, // Phase 2: live process state
		ExitCode:        0,   // Phase 2: live process state
		Health:          healthV,
		DependsOn:       dependsOn,
		Command:         cmd,
		RestartPolicy:   def.Restart,
	}
}

// ─── Phase 2: Mutation endpoints ─────────────────────────────────────────────

// serviceMutationResponse is the JSON envelope returned by all mutation endpoints.
type serviceMutationResponse struct {
	Success           bool           `json:"success"`
	Action            string         `json:"action"`
	ServiceName       string         `json:"service_name"`
	Status            *ServiceStatus `json:"status,omitempty"`
	Error             string         `json:"error,omitempty"`
	LaunchctlExitCode int            `json:"launchctl_exit_code,omitempty"`
}

// supervisorFromServer returns the configured ServiceSupervisor, or an
// ObserverSupervisor if none is wired. This provides a safe fallback so the
// server always has a supervisor to call.
func (s *Server) supervisorFromServer() ServiceSupervisor {
	if s.serviceSupervisor != nil {
		return s.serviceSupervisor
	}
	return &ObserverSupervisor{}
}

// requireServiceControl checks the gate and returns a 403 if disabled.
// Returns true if the handler should continue, false if it has already written
// an error response.
func (s *Server) requireServiceControl(w http.ResponseWriter) bool {
	if s.cfg == nil || !s.cfg.EnableServiceControl {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "disabled",
			"detail": "service control via HTTP is disabled; set enable_service_control: true in kernel.yaml",
		})
		return false
	}
	return true
}

// lookupServiceForMutation resolves the service name from the manifest and
// returns the ServiceDef. Writes 404 and returns false if not found.
func (s *Server) lookupServiceForMutation(w http.ResponseWriter, name string) (ServiceDef, bool) {
	manifest := s.process.NodeManifest()
	if manifest == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "manifest not loaded"})
		return ServiceDef{}, false
	}
	def, ok := manifest.Services[name]
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service not found: " + name})
		return ServiceDef{}, false
	}
	return def, true
}

// writeMutationResponse writes the standard mutation JSON envelope.
func writeMutationResponse(w http.ResponseWriter, httpStatus int, resp serviceMutationResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(resp)
}

// dispatchMutation is the shared core for all mutation handlers. It:
//  1. Gates on EnableServiceControl.
//  2. Resolves the service from the manifest.
//  3. Routes to the correct supervisor via supervisorFor().
//  4. Calls mutationFn.
//  5. Maps errors to HTTP status codes per the spec.
func (s *Server) dispatchMutation(
	w http.ResponseWriter, r *http.Request,
	action string,
	mutationFn func(sup ServiceSupervisor, name string, def ServiceDef) (*ServiceStatus, error),
) {
	if !s.requireServiceControl(w) {
		return
	}

	name := r.PathValue("name")
	def, ok := s.lookupServiceForMutation(w, name)
	if !ok {
		return
	}

	controller := s.supervisorFromServer()
	sup := supervisorFor(def, controller)

	st, err := mutationFn(sup, name, def)

	exitCode := 0
	if st != nil {
		exitCode = st.LaunchctlExitCode
	}

	if err != nil {
		httpStatus := http.StatusInternalServerError
		errMsg := err.Error()

		switch {
		case errors.Is(err, ErrNotControllable):
			// 409: service exists but is not controllable (kind=observed|external).
			httpStatus = http.StatusConflict
		case errors.Is(err, ErrServiceNotFound):
			// 404: should have been caught above, but guard defensively.
			httpStatus = http.StatusNotFound
		case errors.Is(err, ErrLaunchctlTransient):
			// 503: transient launchd error (launchctl exit 125 / "service failed").
			// The caller may retry; this is not a permanent failure state.
			httpStatus = http.StatusServiceUnavailable
		case errors.Is(err, r.Context().Err()):
			// Request cancelled.
			httpStatus = http.StatusBadRequest
		}

		writeMutationResponse(w, httpStatus, serviceMutationResponse{
			Success:           false,
			Action:            action,
			ServiceName:       name,
			Status:            st,
			Error:             errMsg,
			LaunchctlExitCode: exitCode,
		})
		return
	}

	writeMutationResponse(w, http.StatusOK, serviceMutationResponse{
		Success:           true,
		Action:            action,
		ServiceName:       name,
		Status:            st,
		LaunchctlExitCode: exitCode,
	})
}

// handleServiceStart — POST /v1/services/{name}/start
//
//	200 → { success: true, action: "start", service_name: "...", status: {...} }
//	403 → service control is disabled
//	404 → service not found
//	409 → service is not controllable (kind=observed|external)
func (s *Server) handleServiceStart(w http.ResponseWriter, r *http.Request) {
	s.dispatchMutation(w, r, "start", func(sup ServiceSupervisor, name string, def ServiceDef) (*ServiceStatus, error) {
		return sup.Start(r.Context(), name, def)
	})
}

// handleServiceStop — POST /v1/services/{name}/stop
func (s *Server) handleServiceStop(w http.ResponseWriter, r *http.Request) {
	s.dispatchMutation(w, r, "stop", func(sup ServiceSupervisor, name string, def ServiceDef) (*ServiceStatus, error) {
		return sup.Stop(r.Context(), name, def)
	})
}

// handleServiceRestart — POST /v1/services/{name}/restart[?wait=false]
//
// The restart runs on a context detached from the request: the caller may be
// the service being restarted (an agent gateway restarting itself), and its
// connection drops as soon as the old process exits. Cancelling the restart
// at that moment would leave the service down. The response reports
// previous_pid and restarted=true once a new PID is observed.
//
// ?wait=false returns 202 immediately and restarts in the background. A
// caller restarting ITSELF must use it: a gateway drains in-flight work on
// SIGTERM, and the in-flight work includes the very call that is waiting on
// this response — waiting would deadlock until the drain timeout.
func (s *Server) handleServiceRestart(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("wait") == "false" {
		s.handleServiceRestartAsync(w, r)
		return
	}
	s.dispatchMutation(w, r, "restart", func(sup ServiceSupervisor, name string, def ServiceDef) (*ServiceStatus, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), serviceRestartBudget)
		defer cancel()
		return sup.Restart(ctx, name, def)
	})
}

// handleServiceRestartAsync accepts a restart and runs it in the background.
// Same gate, lookup, and supervisor routing as dispatchMutation; a
// non-controllable service still gets 409 synchronously.
func (s *Server) handleServiceRestartAsync(w http.ResponseWriter, r *http.Request) {
	if !s.requireServiceControl(w) {
		return
	}
	name := r.PathValue("name")
	def, ok := s.lookupServiceForMutation(w, name)
	if !ok {
		return
	}
	sup := supervisorFor(def, s.supervisorFromServer())
	before, err := sup.Status(r.Context(), name, def)
	if err != nil && errors.Is(err, ErrNotControllable) {
		writeMutationResponse(w, http.StatusConflict, serviceMutationResponse{
			Success: false, Action: "restart", ServiceName: name, Error: err.Error(),
		})
		return
	}
	if def.Kind.EffectiveKind() != ServiceKindManaged {
		writeMutationResponse(w, http.StatusConflict, serviceMutationResponse{
			Success: false, Action: "restart", ServiceName: name, Error: ErrNotControllable.Error(),
		})
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), serviceRestartBudget)
		defer cancel()
		st, rerr := sup.Restart(ctx, name, def)
		attrs := []any{"service", name, "err", rerr}
		if st != nil {
			attrs = append(attrs, "previous_pid", st.PreviousPID, "pid", st.PID, "restarted", st.Restarted)
		}
		slog.Info("services: async restart finished", attrs...)
	}()

	writeMutationResponse(w, http.StatusAccepted, serviceMutationResponse{
		Success: true, Action: "restart", ServiceName: name, Status: before,
	})
}

// serviceRestartBudget bounds a detached restart (kickstart + PID confirm).
var serviceRestartBudget = 60 * time.Second

// handleServiceEnable — POST /v1/services/{name}/enable
func (s *Server) handleServiceEnable(w http.ResponseWriter, r *http.Request) {
	s.dispatchMutation(w, r, "enable", func(sup ServiceSupervisor, name string, def ServiceDef) (*ServiceStatus, error) {
		return sup.Enable(r.Context(), name, def)
	})
}

// handleServiceDisable — POST /v1/services/{name}/disable
func (s *Server) handleServiceDisable(w http.ResponseWriter, r *http.Request) {
	s.dispatchMutation(w, r, "disable", func(sup ServiceSupervisor, name string, def ServiceDef) (*ServiceStatus, error) {
		return sup.Disable(r.Context(), name, def)
	})
}
