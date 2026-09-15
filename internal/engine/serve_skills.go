// serve_skills.go — HTTP handlers for skill discovery and execution (issues #96, #97).
//
// Routes:
//   GET  /v1/skills              — list available skills (JSON array)
//   POST /v1/skills/{name}/exec  — invoke a skill by name

package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/myrgic/cogos/pkg/skills"
)

// ─── Re-exported types ────────────────────────────────────────────────────────

// SkillRecord is the JSON object returned by GET /v1/skills.
// Aliased from pkg/skills so the engine package surface is unchanged.
type SkillRecord = skills.Record

// SkillExecRequest is the POST body for /v1/skills/{name}/exec.
type SkillExecRequest struct {
	Input   string `json:"input"`             // raw JSON to pass via stdin
	Timeout string `json:"timeout,omitempty"` // duration string, e.g. "30s"
}

// SkillExecResponse is the response from /v1/skills/{name}/exec.
// Aliased from pkg/skills so the HTTP handler uses the same type.
type SkillExecResponse = skills.ExecResult

// ─── Route registration ──────────────────────────────────────────────────────────

func (s *Server) registerSkillRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/skills", s.handleSkillList)
	s.route(mux, "POST /v1/skills/{name}/exec", s.handleSkillExec)
}

// ─── Discovery helpers ───────────────────────────────────────────────────────────

// skillDirs returns directories to scan, in priority order (workspace overrides user).
func (s *Server) skillDirs() []string {
	var dirs []string

	home, err := os.UserHomeDir()
	if err == nil {
		dirs = append(dirs, filepath.Join(home, ".claude", "skills"))
	}

	if s.cfg != nil && s.cfg.WorkspaceRoot != "" {
		dirs = append(dirs, filepath.Join(s.cfg.WorkspaceRoot, ".claude", "skills"))
	}

	return dirs
}

// discoverSkills returns all skills from the configured directories.
func (s *Server) discoverSkills() ([]SkillRecord, error) {
	return skills.Discover(s.skillDirs())
}

// findSkill looks up a skill by name.
func (s *Server) findSkill(name string) (*SkillRecord, error) {
	return skills.FindByName(name, s.skillDirs())
}

// ─── Handlers ────────────────────────────────────────────────────────────────────

func (s *Server) handleSkillList(w http.ResponseWriter, r *http.Request) {
	skillList, err := s.discoverSkills()
	if err != nil {
		http.Error(w, `{"error":"discovery_failed","detail":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(skillList)
}

func (s *Server) handleSkillExec(w http.ResponseWriter, r *http.Request) {
	if s.cfg == nil || !s.cfg.EnableSkillExec {
		http.Error(w, `{"error":"disabled","detail":"skill exec via HTTP is disabled; set enable_skill_exec: true in kernel.yaml"}`, http.StatusForbidden)
		return
	}

	name := r.PathValue("name")

	skill, err := s.findSkill(name)
	if err != nil {
		// Same response whether not-found or role-gated (don't leak existence).
		http.Error(w, `{"error":"not_found","detail":"skill not found or not available"}`, http.StatusNotFound)
		return
	}

	var req SkillExecRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid_json"}`, http.StatusBadRequest)
			return
		}
	}

	resp, err := s.execSkill(r.Context(), skill, req.Input, req.Timeout)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "not_executable") || strings.Contains(err.Error(), "no_canonical_entry_point") {
			status = http.StatusUnprocessableEntity
		}
		errJSON, _ := json.Marshal(map[string]string{"error": err.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(errJSON)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}

// ─── Exec ─────────────────────────────────────────────────────────────────────────

// execSkill delegates to pkg/skills.Exec and then emits a skill.exec bus event.
func (s *Server) execSkill(ctx context.Context, skill *SkillRecord, inputJSON, timeoutStr string) (*SkillExecResponse, error) {
	wsRoot := ""
	if s.cfg != nil {
		wsRoot = s.cfg.WorkspaceRoot
	}

	result, err := skills.Exec(ctx, skill, inputJSON, timeoutStr, wsRoot)
	if err != nil {
		return nil, err
	}

	// Emit skill.exec bus event (best-effort).
	s.emitSkillExecEvent(skill, result)

	return result, nil
}

// emitSkillExecEvent emits a skill.exec event to the bus. Non-fatal.
func (s *Server) emitSkillExecEvent(skill *SkillRecord, resp *SkillExecResponse) {
	if s.busSessions == nil {
		return
	}
	payload := map[string]interface{}{
		"name":        skill.Name,
		"version":     skill.Version,
		"exit_code":   resp.ExitCode,
		"duration_ms": resp.DurationMS,
	}
	if resp.Error != "" {
		payload["error"] = resp.Error
	}
	_, _ = s.busSessions.AppendEvent("skill", "skill.exec", "kernel:skill:exec", payload)
}
