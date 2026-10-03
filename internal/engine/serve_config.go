// serve_config.go — HTTP surface for the Config Mutation API (Agent O design).
//
//	GET   /v1/config             — read effective + raw + defaults
//	PATCH /v1/config             — RFC 7396 merge-patch (validated, atomic, backed up)
//	POST  /v1/config/rollback    — restore from a .bak-<timestamp> file
//
// All three routes are gated by Config.EnableConfigMutation (default false),
// parity with EnableSkillExec (serve_skills.go) / EnableServiceControl
// (serve_services.go): any local process on the same host could otherwise
// read or rewrite kernel.yaml over loopback. Set enable_config_mutation:
// true in kernel.yaml to opt in.
//
// All handlers return JSON on success and on structured-error paths.
package engine

import (
	"encoding/json"
	"errors"
	"net/http"
)

// registerConfigRoutes mounts the config read/write/rollback endpoints.
func (s *Server) registerConfigRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/config", s.handleConfigGet)
	s.route(mux, "PATCH /v1/config", s.handleConfigPatch)
	s.route(mux, "POST /v1/config/rollback", s.handleConfigRollback)
}

// requireConfigMutation checks the gate and writes a 403 if disabled.
// Returns true if the handler should continue, false if it has already
// written an error response. Mirrors requireServiceControl in
// serve_services.go.
func (s *Server) requireConfigMutation(w http.ResponseWriter) bool {
	if s.cfg == nil || !s.cfg.EnableConfigMutation {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "disabled",
			"detail": "config access via HTTP is disabled; set enable_config_mutation: true in kernel.yaml",
		})
		return false
	}
	return true
}

// handleConfigGet returns the effective kernel config. Query params:
//
//	include_raw_yaml=1   — also return raw kernel.yaml bytes
//	include_defaults=1   — also return hardcoded defaults
func (s *Server) handleConfigGet(w http.ResponseWriter, r *http.Request) {
	if !s.requireConfigMutation(w) {
		return
	}
	q := r.URL.Query()
	includeRaw := truthyQuery(q.Get("include_raw_yaml"))
	includeDefaults := truthyQuery(q.Get("include_defaults"))
	snapshot, err := ReadConfigSnapshot(s.cfg.WorkspaceRoot, includeRaw, includeDefaults)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		// Parse error is non-fatal — return what we can with a field tag.
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"effective_config": snapshot.EffectiveConfig,
			"path":             snapshot.Path,
			"exists":           snapshot.Exists,
			"raw_yaml":         snapshot.RawYAML,
			"defaults":         snapshot.Defaults,
			"parse_error":      err.Error(),
		})
		return
	}
	_ = json.NewEncoder(w).Encode(snapshot)
}

// configPatchRequest is the HTTP body shape for PATCH /v1/config.
type configPatchRequest struct {
	Patch  json.RawMessage `json:"patch"`
	Scope  string          `json:"scope,omitempty"`
	DryRun bool            `json:"dry_run,omitempty"`
}

func (s *Server) handleConfigPatch(w http.ResponseWriter, r *http.Request) {
	if !s.requireConfigMutation(w) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	body, err := readLimitedBody(w, r, maxConfigRequestBodyBytes)
	if err != nil {
		w.WriteHeader(configBodyErrStatus(err))
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "read body: " + err.Error()})
		return
	}
	var req configPatchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "parse body: " + err.Error()})
		return
	}

	patch, err := DecodePatchBody(req.Patch)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "parse patch: " + err.Error()})
		return
	}

	result, werr := WriteConfigPatch(s.cfg.WorkspaceRoot, patch, WriteConfigOptions{
		Scope:  req.Scope,
		DryRun: req.DryRun,
	})
	if werr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": werr.Error()})
		return
	}

	status := http.StatusOK
	if len(result.Violations) > 0 {
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}

func (s *Server) handleConfigRollback(w http.ResponseWriter, r *http.Request) {
	if !s.requireConfigMutation(w) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	var body rollbackConfigInput
	if r.ContentLength > 0 {
		data, err := readLimitedBody(w, r, maxConfigRequestBodyBytes)
		if err != nil {
			w.WriteHeader(configBodyErrStatus(err))
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "read body: " + err.Error()})
			return
		}
		if len(data) > 0 {
			if err := json.Unmarshal(data, &body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "parse body: " + err.Error()})
				return
			}
		}
	}

	result, err := RollbackConfig(s.cfg.WorkspaceRoot, RollbackOptions{
		Backup:   body.Backup,
		ListOnly: body.ListOnly,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	status := http.StatusOK
	if result.Error != "" {
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}

// truthyQuery accepts "1", "true", "yes" (case-insensitive).
func truthyQuery(v string) bool {
	switch v {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes":
		return true
	}
	return false
}

// configBodyErrStatus maps a readLimitedBody error to an HTTP status:
// 413 for an oversized body, 400 otherwise.
func configBodyErrStatus(err error) int {
	if errors.Is(err, errRequestBodyTooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}
