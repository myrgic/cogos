// managed_session_reaper.go — ADR-093 §4 session abandonment for the
// hermes-acp driver: the kernel's backstop for session end-of-life.
//
// Before this, nothing ended a managed session unless a client asked
// (cog apps/stage/docs/briefs/session-eol.md): on 2026-10-24 darkstar carried
// 62 live sessions, ~300 agent processes and ~11 GB RSS, most of them finished
// work threads and room fronts idle for days, because the client that opened
// them never sent DELETE and the kernel had no idle_timeout.
//
// The rule is "the client releases what it owns; the kernel reaps what nobody
// owns". Reap detaches a session when either
//
//   - it has had no activity for IdleTimeout (default 1 h; ADR-093 §4), or
//   - it holds a lease (HermesACPOpts.LeaseTTLSecs) that expired unrenewed.
//
// Activity is the later of the last client call on the session (prompt,
// forward, cancel, mode, model, permission answer, resume, a new event
// subscription) and the newest event on its ring (agent output, permission
// asks, turn start/end). An events stream that is merely open is not activity
// (a client may hold one for its whole life), and neither is a lease renewal:
// renewing says "I still own it", not "I am using it", and the idle bound
// applies with or without a lease. A lease can end a session earlier than
// IdleTimeout, never later.
//
// Never reaped: a session with a turn in flight, a forwarded prompt pending or
// a parked permission request (a person may be about to answer; the permission
// timeout bounds that wait), or one in state starting (a crash-restart is in
// flight). They are reaped at the first tick after they stop being busy.
//
// A reap is drop, not Delete: the session leaves the registry, its process
// stops once no session runs in it, its ring gets a final state event
// {state: detached, error: "reaped: ..."} and closes, and its durable record
// stays, so POST /v1/managed-sessions/{id}/resume (session/load) brings the
// conversation back in a new process with its recorded profile, cwd, mode and
// model. Reaping frees the process, not the conversation.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Reaper defaults (kernel.yaml managedSessions.idleTimeout / reapInterval).
const (
	DefaultManagedSessionIdleTimeout  = time.Hour
	DefaultManagedSessionReapInterval = time.Minute
	// maxRecentReaps bounds the reap log the summary reports.
	maxRecentReaps = 32
	// reapConcurrency bounds how many sessions one tick detaches at once.
	reapConcurrency = 8
)

// Reap reasons (ReapRecord.Reason, and the detached state event's error).
const (
	ReapReasonIdle         = "idle"
	ReapReasonLeaseExpired = "lease expired"
)

// ReapRecord is one session the reaper detached.
type ReapRecord struct {
	SessionID string    `json:"session_id"`
	Reason    string    `json:"reason"`
	IdleSecs  int64     `json:"idle_secs"`
	Owner     string    `json:"owner,omitempty"`
	Room      string    `json:"room,omitempty"`
	Class     string    `json:"class,omitempty"`
	State     string    `json:"state"` // the state it was reaped from (live, crashed)
	At        time.Time `json:"at"`
}

// reapLog keeps the count and the most recent reaps for the summary.
type reapLog struct {
	mu     sync.Mutex
	total  int64
	recent []ReapRecord // newest last, at most maxRecentReaps
}

func (l *reapLog) add(rs []ReapRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total += int64(len(rs))
	l.recent = append(l.recent, rs...)
	if n := len(l.recent) - maxRecentReaps; n > 0 {
		l.recent = append([]ReapRecord(nil), l.recent[n:]...)
	}
}

func (l *reapLog) snapshot() (int64, []ReapRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.total, append([]ReapRecord(nil), l.recent...)
}

// reapVerdict decides, under s.mu, whether the reaper takes s at now, and if
// so marks it reaped in the same critical section: Prompt registers a turn
// under s.mu too, so a session is either busy (and kept) or reaped (and every
// later call on it refuses), never both. Returns "" to keep it.
func (s *HermesACPSession) reapVerdict(now time.Time, idleTimeout time.Duration) (reason string, idle time.Duration, from ManagedSessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	from = s.state
	if s.reaped != "" || s.state == StateStarting || s.state == StateDetached {
		return "", 0, from
	}
	if s.turn != nil || s.fwdPending > 0 || len(s.perms) > 0 {
		return "", 0, from
	}
	idle = now.Sub(s.lastActivityLocked())
	switch {
	case s.leaseTTL > 0 && now.After(s.leaseExpires):
		reason = ReapReasonLeaseExpired
	case idleTimeout > 0 && idle >= idleTimeout:
		reason = ReapReasonIdle
	default:
		return "", 0, from
	}
	s.reaped = reason
	return reason, idle, from
}

// Reap detaches every session the policy above takes at now and returns what
// it took. Safe to call at any time and concurrently with every other driver
// call; the serving loop calls it every ReapInterval (RunReaper).
func (d *HermesACPDriver) Reap(now time.Time) []ReapRecord {
	type victim struct {
		s      *HermesACPSession
		reason string
		idle   time.Duration
		state  ManagedSessionState
	}
	var victims []victim
	d.mu.Lock()
	for id, s := range d.sessions {
		reason, idle, st := s.reapVerdict(now, d.cfg.IdleTimeout)
		if reason == "" {
			continue
		}
		// Out of the registry in the same critical section that decided it,
		// so a resume arriving now finds no resident session and loads it
		// fresh rather than attaching to the one being detached.
		delete(d.sessions, id)
		victims = append(victims, victim{s, reason, idle, st})
	}
	d.mu.Unlock()

	out := make([]ReapRecord, len(victims))
	// Detach concurrently, a few at a time: stopping a process waits for it
	// to exit (up to 3 s before a kill), and the first tick after a deploy
	// can find dozens of sessions to let go.
	var wg sync.WaitGroup
	sem := make(chan struct{}, reapConcurrency)
	for i, v := range victims {
		s := v.s
		s.mu.Lock()
		out[i] = ReapRecord{SessionID: s.id, Reason: v.reason, IdleSecs: int64(v.idle / time.Second),
			Owner: s.owner, Room: s.room, Class: s.class, State: v.state.String(), At: now.UTC()}
		s.mu.Unlock()
		why := fmt.Sprintf("reaped: %s; resume to continue", v.reason)
		if v.reason == ReapReasonIdle {
			why = fmt.Sprintf("reaped: idle %s (idle_timeout %s); resume to continue", v.idle.Round(time.Second), d.cfg.IdleTimeout)
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(s *HermesACPSession, why string) {
			defer func() { <-sem; wg.Done() }()
			d.detachWith(s, errors.New(why))
		}(s, why)
		slog.Info("managed-sessions: reaped", "session", out[i].SessionID, "reason", out[i].Reason, "idle_s", out[i].IdleSecs,
			"owner", out[i].Owner, "state", out[i].State)
	}
	wg.Wait()
	if len(out) > 0 {
		d.reaps.add(out)
	}
	return out
}

// RunReaper calls Reap every ReapInterval until ctx is done, and the retention
// sweep at most once an hour. The serving lifetime owns it
// (Server.startBackground); a driver that is only constructed (tests) never
// reaps on its own.
func (d *HermesACPDriver) RunReaper(ctx context.Context) {
	t := time.NewTicker(d.cfg.ReapInterval)
	defer t.Stop()
	var swept time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			d.Reap(now)
			if d.cfg.Retention > 0 && now.Sub(swept) >= retentionSweepEvery {
				swept = now
				d.SweepRetention(now)
			}
		}
	}
}

// retentionSweepEvery spaces the retention sweeps.
const retentionSweepEvery = time.Hour

// SweepRetention removes the per-session files of sessions this driver does
// not hold whose last write is older than Retention: journals (JournalDir,
// <session>.jsonl) and durable records (RecordDir). A resident session's files
// are never touched, whatever their age. Removing a record means a later cold
// resume of that session runs the default class with the request's profile;
// the agent's own conversation store is not touched. Returns the paths
// removed. Off (nothing removed) while Retention is 0.
func (d *HermesACPDriver) SweepRetention(now time.Time) []string {
	if d.cfg.Retention <= 0 {
		return nil
	}
	cut := now.Add(-d.cfg.Retention)
	resident := map[string]bool{}
	for _, s := range d.snapshotSessions() {
		resident[journalFileName(s.id)+".jsonl"] = true
		resident[filepath.Base(d.managedSessionRecordPath(s.id))] = true
	}
	var removed []string
	sweep := func(dir, suffix string) {
		if dir == "" {
			return
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, suffix) || resident[name] {
				continue
			}
			info, err := e.Info()
			if err != nil || info.ModTime().After(cut) {
				continue
			}
			p := filepath.Join(dir, name)
			if err := os.Remove(p); err == nil {
				removed = append(removed, p)
			}
		}
	}
	sweep(d.cfg.JournalDir, ".jsonl")
	sweep(d.cfg.RecordDir, ".json")
	if len(removed) > 0 {
		slog.Info("managed-sessions: retention removed files of sessions not held", "count", len(removed),
			"retention", d.cfg.Retention.String())
	}
	return removed
}

// ReaperSettings reports the effective idle timeout and reap interval.
func (d *HermesACPDriver) ReaperSettings() (idleTimeout, reapInterval time.Duration) {
	return d.cfg.IdleTimeout, d.cfg.ReapInterval
}
