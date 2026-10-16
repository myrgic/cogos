// serve_runs.go - /v1/runs routes for the kernel run board (W1 skeleton).
// Every handler answers 501 until the W1 body slice lands.
package engine

import (
	"encoding/json"
	"net/http"
)

// runsRoutePrefix is the route family guarded by ScopeRunsWrite for writes
// and carved out of the blanket GET exemption (reads need a live grant).
const runsRoutePrefix = "/v1/runs"

func (s *Server) registerRunRoutes(mux *http.ServeMux) {
	s.route(mux, "GET /v1/runs", s.handleRunList)
	s.route(mux, "GET /v1/runs/events", s.handleRunEvents)
	s.route(mux, "GET /v1/runs/{id}", s.handleRunGet)
	s.route(mux, "POST /v1/runs", s.handleRunOpen)
	s.route(mux, "POST /v1/runs/{id}/end", s.handleRunEnd)
	s.route(mux, "POST /v1/runs/{id}/delivered", s.handleRunDelivered)
}

func runsNotImplemented(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "run board: not implemented"})
}

// handleRunList serves GET /v1/runs?room=&executor=&state=.
func (s *Server) handleRunList(w http.ResponseWriter, r *http.Request) { runsNotImplemented(w) }

// handleRunGet serves GET /v1/runs/{id}.
func (s *Server) handleRunGet(w http.ResponseWriter, r *http.Request) { runsNotImplemented(w) }

// handleRunOpen serves POST /v1/runs (executor manual only in W1).
func (s *Server) handleRunOpen(w http.ResponseWriter, r *http.Request) { runsNotImplemented(w) }

// handleRunEnd serves POST /v1/runs/{id}/end.
func (s *Server) handleRunEnd(w http.ResponseWriter, r *http.Request) { runsNotImplemented(w) }

// handleRunDelivered serves POST /v1/runs/{id}/delivered.
func (s *Server) handleRunDelivered(w http.ResponseWriter, r *http.Request) { runsNotImplemented(w) }

// handleRunEvents serves GET /v1/runs/events (WebSocket, since=).
func (s *Server) handleRunEvents(w http.ResponseWriter, r *http.Request) { runsNotImplemented(w) }
