// serve_claude_code.go — /v1/claude-code/* API
//
// Routes:
//
//	GET  /v1/claude-code/projects                       — list project dirs under ~/.claude/projects/
//	GET  /v1/claude-code/projects/{project}/sessions    — list .jsonl session files with metadata
//	POST /v1/claude-code/spawn                          — spawn or resume Claude Code with
//	                                                       channel-client MCP injection
//
// Design:
//   - Filesystem reads are cheap (os.ReadDir + first-line JSONL scan).
//   - Spawn writes a temp .mcp.json, then calls ClaudeCodeProvider.SpawnBackground.
//   - Process info is returned immediately; caller polls /v1/claude-code/processes/{id}
//     or observes seat attachment on the mod3 dashboard to confirm the session is live.
//   - ProjectsDir defaults to ~/.claude/projects/ but honours the CLAUDE_PROJECTS_DIR
//     env-var for testing and non-standard installations.
//
// Registered via s.registerClaudeCodeRoutes(mux) which is called from NewServer.
package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

// claudeProjectsDir returns the directory that holds Claude Code project dirs.
// Each subdirectory is one project; its .jsonl files are sessions.
func claudeProjectsDir() string {
	if v := os.Getenv("CLAUDE_PROJECTS_DIR"); v != "" {
		return v
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".claude", "projects")
}

// validClaudeProjectName reports whether p is safe to use as a single path
// component under claudeProjectsDir(): no parent refs and no separators, so it
// cannot escape the projects directory. Shared by the sessions and spawn
// handlers so both reject traversal identically.
func validClaudeProjectName(p string) bool {
	return !strings.Contains(p, "..") && !strings.ContainsRune(p, '/')
}

// ── route registration ───────────────────────────────────────────────────────

// registerClaudeCodeRoutes wires the three /v1/claude-code/* routes onto mux.
func (s *Server) registerClaudeCodeRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/claude-code/projects", s.handleClaudeCodeProjects)
	s.route(mux, "GET /v1/claude-code/projects/{project}/sessions", s.handleClaudeCodeSessions)
	s.route(mux, "POST /v1/claude-code/spawn", s.handleClaudeCodeSpawn)
}

// ── GET /v1/claude-code/projects ─────────────────────────────────────────────

// claudeProjectView is the JSON shape for a single project entry.
type claudeProjectView struct {
	Name         string    `json:"name"`
	LastActivity time.Time `json:"last_activity"`
	SessionCount int       `json:"session_count"`
}

// handleClaudeCodeProjects lists all project directories under ~/.claude/projects/.
//
//	200 → { projects: [...] }
func (s *Server) handleClaudeCodeProjects(w http.ResponseWriter, r *http.Request) {
	dir := claudeProjectsDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSONResp(w, http.StatusOK, map[string]any{"projects": []claudeProjectView{}})
			return
		}
		slog.Warn("claude-code: readdir projects", "err", err)
		writeJSONError(w, http.StatusInternalServerError, "fs_error", "cannot read projects directory")
		return
	}

	var projects []claudeProjectView
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		// Count .jsonl files and find most-recent mtime.
		subEntries, err := os.ReadDir(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var count int
		lastAct := info.ModTime()
		for _, se := range subEntries {
			if se.IsDir() || !strings.HasSuffix(se.Name(), ".jsonl") {
				continue
			}
			count++
			if si, err := se.Info(); err == nil && si.ModTime().After(lastAct) {
				lastAct = si.ModTime()
			}
		}
		projects = append(projects, claudeProjectView{
			Name:         e.Name(),
			LastActivity: lastAct.UTC(),
			SessionCount: count,
		})
	}
	if projects == nil {
		projects = []claudeProjectView{}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"projects": projects})
}

// ── GET /v1/claude-code/projects/{project}/sessions ──────────────────────────

// claudeSessionView is the JSON shape for a single session entry.
type claudeSessionView struct {
	SessionID          string    `json:"session_id"`
	LastModified       time.Time `json:"last_modified"`
	TurnCount          int       `json:"turn_count"`
	TotalMessages      int       `json:"total_messages"`
	FirstPromptSummary string    `json:"first_prompt_summary"` // ≤120 chars
}

// handleClaudeCodeSessions lists .jsonl session files for a project with metadata.
//
//	200 → { project, sessions: [...] }
func (s *Server) handleClaudeCodeSessions(w http.ResponseWriter, r *http.Request) {
	project := r.PathValue("project")
	if project == "" {
		writeJSONError(w, http.StatusBadRequest, "missing_param", "project required")
		return
	}
	// Sanitize: no path traversal.
	if !validClaudeProjectName(project) {
		writeJSONError(w, http.StatusBadRequest, "invalid_param", "invalid project name")
		return
	}

	projDir := filepath.Join(claudeProjectsDir(), project)
	entries, err := os.ReadDir(projDir)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSONError(w, http.StatusNotFound, "not_found", "project not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "fs_error", "cannot read project directory")
		return
	}

	var sessions []claudeSessionView
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		sessionID := strings.TrimSuffix(e.Name(), ".jsonl")
		path := filepath.Join(projDir, e.Name())
		summary, turns, total := scanSessionFile(path)
		sessions = append(sessions, claudeSessionView{
			SessionID:          sessionID,
			LastModified:       info.ModTime().UTC(),
			TurnCount:          turns,
			TotalMessages:      total,
			FirstPromptSummary: summary,
		})
	}
	if sessions == nil {
		sessions = []claudeSessionView{}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{
		"project":  project,
		"sessions": sessions,
	})
}

// scanSessionFile reads a Claude Code .jsonl file and returns:
//   - first user prompt truncated to 120 chars
//   - number of "user" turns (turn_count)
//   - total message count
//
// Claude Code JSONL uses a wrapper envelope:
//
//	{"type":"user","message":{"role":"user","content":"..."},...}
//	{"type":"assistant","message":{"role":"assistant","content":[...]},...}
//
// Reads only enough lines to extract the summary; bails early once found.
func scanSessionFile(path string) (summary string, turns, total int) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256*1024), 256*1024)

	var firstPrompt string
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		total++

		// Claude Code wraps every record in an envelope with a top-level "type"
		// field. The actual conversation message lives in the "message" field.
		var envelope struct {
			Type             string `json:"type"` // "user", "assistant", "system", etc.
			IsCompactSummary bool   `json:"isCompactSummary"`
			Message          struct {
				Role    string `json:"role"`
				Content any    `json:"content"` // string or []map
			} `json:"message"`
			// Some records (queue-operation, pr-link, etc.) have no "message".
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			continue
		}

		// Count user turns: top-level type=="user" with a message payload.
		if envelope.Type != "user" || envelope.Message.Role == "" {
			continue
		}
		// Skip compact-summary placeholders and UI-injected meta messages.
		if envelope.IsCompactSummary || isMeta(envelope.Message.Content) {
			continue
		}
		turns++
		if firstPrompt == "" {
			firstPrompt = extractPromptText(envelope.Message.Content)
		}
	}
	summary = truncateToChars(firstPrompt, 120)
	return summary, turns, total
}

// isMeta returns true for UI-injected wrapper messages that should not count
// as real user turns (e.g. local-command-caveat, compact summaries).
func isMeta(content any) bool {
	s, ok := content.(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(s, "<local-command-caveat>") ||
		strings.HasPrefix(s, "<command-name>") ||
		strings.HasPrefix(s, "<local-command-stdout>")
}

// extractPromptText pulls plain text from a content field that may be a
// plain string or an OpenAI-style array of content blocks.
func extractPromptText(content any) string {
	switch v := content.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if t, _ := m["type"].(string); t == "text" {
				if s, _ := m["text"].(string); s != "" {
					return strings.TrimSpace(s)
				}
			}
		}
	}
	return ""
}

// truncateToChars truncates s to maxChars unicode code points, appending "…"
// if truncation occurs. Collapses whitespace runs first.
func truncateToChars(s string, maxChars int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) <= maxChars {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxChars]) + "…"
}

// ── POST /v1/claude-code/spawn ───────────────────────────────────────────────

// claudeCodeSpawnRequest is the JSON body for the spawn endpoint.
type claudeCodeSpawnRequest struct {
	// Project is the project name (directory under ~/.claude/projects/).
	// Used only to derive WorkDir context; not required.
	Project string `json:"project,omitempty"`

	// SessionID, when non-empty, resumes an existing Claude Code session.
	SessionID string `json:"session_id,omitempty"`

	// DangerouslyLoadDevelopmentChannels enables the dev-channel MCP config.
	DangerouslyLoadDevelopmentChannels bool `json:"dangerously_load_development_channels,omitempty"`
}

// claudeCodeSpawnResponse is returned on a successful spawn.
type claudeCodeSpawnResponse struct {
	ProcessID string `json:"process_id"` // opaque ID from ProcessManager
	SessionID string `json:"session_id,omitempty"`
	Project   string `json:"project,omitempty"`
	Status    string `json:"status"` // "spawned"
	// SpawnedAt is the wall-clock time the subprocess was started.
	SpawnedAt time.Time `json:"spawned_at"`
}

// handleClaudeCodeSpawn spawns or resumes a Claude Code session with
// channel-client MCP injection via a generated .mcp.json temp file.
//
//	201 → { process_id, session_id, project, status:"spawned", spawned_at }
func (s *Server) handleClaudeCodeSpawn(w http.ResponseWriter, r *http.Request) {
	var req claudeCodeSpawnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return
	}
	// Sanitize: req.Project becomes the spawned process's working directory
	// (filepath.Join(claudeProjectsDir(), req.Project)); reject traversal so the
	// CWD can't be steered outside ~/.claude/projects. Matches the guard in
	// handleClaudeCodeSessions. Empty Project is allowed (no WorkDir derived).
	if req.Project != "" && !validClaudeProjectName(req.Project) {
		writeJSONError(w, http.StatusBadRequest, "invalid_param", "invalid project name")
		return
	}

	// Build the channel-client .mcp.json temp file so the spawned Claude Code
	// process registers a seat with the mod3 dashboard.
	mcpPath, err := writeTempMCPConfig(req.SessionID, s.cfg.Mod3URL)
	if err != nil {
		slog.Error("claude-code: write temp mcp config", "err", err)
		writeJSONError(w, http.StatusInternalServerError, "spawn_error", "cannot write MCP config: "+err.Error())
		return
	}

	// Construct a minimal system prompt indicating this is a dashboard-spawned session.
	systemPrompt := "You were spawned from the Mod³ dashboard as an ACP-client session."
	if req.SessionID != "" {
		systemPrompt += fmt.Sprintf(" Resuming session %s.", req.SessionID)
	}

	// Instantiate a ClaudeCodeProvider for the one-off spawn.
	pm := NewProcessManager(ProcessManagerConfig{})
	provider := NewClaudeCodeProvider("dashboard-spawn", ProviderConfig{
		Type:    "claude-code",
		Model:   "sonnet",
		Timeout: 0, // background — uses cancel-only lifecycle
	}, pm)

	opts := BackgroundTaskOpts{
		Prompt:          "You have been resumed from the Mod³ dashboard. Await further input via the channel client.",
		Model:           "sonnet",
		MCPConfig:       mcpPath,
		AllowedTools:    nil, // unrestricted — operator initiated
		Source:          "dashboard",
		CallbackChannel: "",
		Identity:        req.Project,
		MaxBudgetUSD:    0,
		Timeout:         0, // background; no hard timeout
		SystemPrompt:    systemPrompt,
	}

	// Derive working directory from project name.
	if req.Project != "" {
		projDir := filepath.Join(claudeProjectsDir(), req.Project)
		if info, err := os.Stat(projDir); err == nil && info.IsDir() {
			opts.WorkDir = projDir
		}
	}

	now := time.Now().UTC()
	procID, err := provider.SpawnBackground(opts)
	if err != nil {
		slog.Error("claude-code: spawn background", "err", err)
		// Clean up temp file on spawn failure.
		_ = os.Remove(mcpPath)
		writeJSONError(w, http.StatusInternalServerError, "spawn_error", "failed to start Claude Code: "+err.Error())
		return
	}

	slog.Info("claude-code: spawned background session",
		"proc_id", procID,
		"session_id", req.SessionID,
		"project", req.Project,
		"mcp_config", mcpPath,
	)

	writeJSONResp(w, http.StatusCreated, claudeCodeSpawnResponse{
		ProcessID: procID,
		SessionID: req.SessionID,
		Project:   req.Project,
		Status:    "spawned",
		SpawnedAt: now,
	})
}

// writeTempMCPConfig writes a .mcp.json file that injects the channel-client
// into the spawned Claude Code process. The file is written to the OS temp dir
// and the path is returned. Callers own the lifecycle of this file; it is safe
// to remove it once the subprocess has started (Claude Code reads it on init).
func writeTempMCPConfig(sessionID, mod3URL string) (string, error) {
	serverURL := mod3URL
	if serverURL == "" {
		serverURL = "http://localhost:7860"
	}

	// channel_client.py is co-located with the mod3 package. Find it via the
	// known canonical location on this node.
	channelClientPath := findChannelClientPath()

	args := []string{"clients/channel_client.py", "--server", serverURL}
	if sessionID != "" {
		args = append(args, "--session", sessionID)
	}

	type mcpServerEntry struct {
		Type    string   `json:"type"`
		Command string   `json:"command"`
		Args    []string `json:"args"`
	}
	type mcpConfig struct {
		MCPServers map[string]mcpServerEntry `json:"mcpServers"`
	}

	// Resolve the python3 binary path.
	pythonBin := "python3"

	// If we found the channel client, use its absolute path as the first arg.
	if channelClientPath != "" {
		args[0] = channelClientPath
	}

	cfg := mcpConfig{
		MCPServers: map[string]mcpServerEntry{
			"mod3-channel": {
				Type:    "stdio",
				Command: pythonBin,
				Args:    args,
			},
		},
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal mcp config: %w", err)
	}

	f, err := os.CreateTemp("", "cogos-mcp-*.json")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("write mcp config: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("close mcp config: %w", err)
	}
	return f.Name(), nil
}

// findChannelClientPath returns the absolute path to channel_client.py.
// Checks the canonical mod3 install location; falls back to "clients/channel_client.py"
// (relative to wherever python3 resolves at runtime) if not found.
func findChannelClientPath() string {
	home, _ := os.UserHomeDir()
	candidates := []string{
		filepath.Join(home, "workspaces", "myrgic", "mod3", "clients", "channel_client.py"),
		"/opt/mod3/clients/channel_client.py",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}
