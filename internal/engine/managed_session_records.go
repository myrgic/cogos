// managed_session_records.go — durable per-session authority record for
// kernel-managed sessions. The in-memory registry dies with the kernel, but
// the agent's own on-disk session (what session/load reopens) outlives it, so
// the runtime class a session was created under must outlive it too: a cold
// resume reads the class from here instead of trusting the request.
//
// Legacy rule: a session with no record, or a record with an empty class, is
// the built-in default class. A cold resume of such a session may not name a
// class (it would be a different class than recorded) and runs the default
// environment, which is what it was created under.
//
// The record also keeps the session's resume shape: the profile and cwd it was
// created with and the last mode and model a client set. The idle reaper
// (managed_session_reaper.go) frees a session's process, not its conversation,
// and a cold resume restores what the reap let go of: a request that names no
// profile runs the recorded one (not whatever the agent's active profile is
// today), and the recorded mode and model are put back after session/load, as
// a crash-restart already does (reapplySettings).
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"crypto/sha256"
	"encoding/hex"
)

// managedSessionRecord is the persisted authority state of one session.
type managedSessionRecord struct {
	SessionID string    `json:"session_id"`
	Class     string    `json:"class,omitempty"` // "" = the built-in default class
	CreatedAt time.Time `json:"created_at"`
	// Resume shape (see the file comment). Never secrets: mcp_servers are not
	// kept (they can carry tokens); the resuming client sends its own.
	Profile string `json:"profile,omitempty"`
	Cwd     string `json:"cwd,omitempty"`
	Mode    string `json:"mode,omitempty"`
	Model   string `json:"model,omitempty"`
}

// managedSessionRecordPath addresses a record by a hash of the id: the id is
// caller-supplied on resume, so it never becomes a path component itself.
func (d *HermesACPDriver) managedSessionRecordPath(id string) string {
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(d.cfg.RecordDir, hex.EncodeToString(sum[:])+".json")
}

// loadRecord returns the durable record for id; a missing record (or an
// unconfigured RecordDir) reads as the legacy default class. A record that
// exists but cannot be read or parsed is an error: fail closed, never default.
func (d *HermesACPDriver) loadRecord(id string) (managedSessionRecord, error) {
	rec := managedSessionRecord{SessionID: id}
	if d.cfg.RecordDir == "" {
		return rec, nil
	}
	b, err := os.ReadFile(d.managedSessionRecordPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return rec, nil
	}
	if err != nil {
		return rec, fmt.Errorf("managed session record %s: %w", id, err)
	}
	if err := json.Unmarshal(b, &rec); err != nil || rec.SessionID != id {
		return managedSessionRecord{}, fmt.Errorf("managed session record %s: unreadable or mismatched", id)
	}
	return rec, nil
}

// saveRecord persists the record. Best-effort callers must not ignore the
// error for classed sessions: a classed session without a record would
// cold-resume as the default class.
func (d *HermesACPDriver) saveRecord(rec managedSessionRecord) error {
	if d.cfg.RecordDir == "" {
		return nil
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return atomicWriteConfigFile(d.managedSessionRecordPath(rec.SessionID), b)
}

func (d *HermesACPDriver) removeRecord(id string) {
	if d.cfg.RecordDir != "" {
		_ = os.Remove(d.managedSessionRecordPath(id))
	}
}

// sessionRecord is the record a session's current state persists as.
func (s *HermesACPSession) sessionRecord() managedSessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return managedSessionRecord{SessionID: s.id, Class: s.class, CreatedAt: s.createdAt,
		Profile: s.profile, Cwd: s.cwd, Mode: s.mode, Model: s.model}
}

// persistSettings rewrites a live session's record after a client changed its
// mode or model, so a resume after a reap comes back on them. Best effort: the
// change itself already landed on the agent, and a stale record only costs a
// resume its settings (the client can set them again). recMu orders the
// rewrites of one session, each taken from the fields as they are now.
func (d *HermesACPDriver) persistSettings(s *HermesACPSession) {
	if d.cfg.RecordDir == "" {
		return
	}
	s.recMu.Lock()
	defer s.recMu.Unlock()
	if s.State() != StateLive { // deleted meanwhile: never resurrect a removed record
		return
	}
	if err := d.saveRecord(s.sessionRecord()); err != nil {
		log.Printf("managed-sessions: persist settings for %s: %v", s.id, err)
	}
}
