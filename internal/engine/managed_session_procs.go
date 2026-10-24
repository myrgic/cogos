// managed_session_procs.go — what each managed session costs, for
// GET /v1/managed-sessions[/{id}], GET /v1/managed-sessions/summary and the
// /health managed_sessions block (cog apps/stage/docs/briefs/session-eol.md:
// the 2026-10-24 leak went unseen for days because the list carried ids and
// states only).
//
// A session's cost is its agent process's whole tree: the agent plus the MCP
// servers it spawned (on darkstar a Hermes session tree was ~6 processes,
// ~146 MB). One `ps -A -o pid=,ppid=,rss=` snapshot serves a whole request and
// is reused for procStatsTTL, so polling the list or /health does not fork a
// ps per session or per call. Where ps is unavailable (windows) or fails, the
// process fields are omitted and the summary says why; nothing else depends on
// them.
package engine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	procStatsTTL     = 5 * time.Second
	procStatsTimeout = 3 * time.Second
)

// procRow is one line of the process table.
type procRow struct {
	PID, PPID int
	RSSKB     int64
}

// psSnapshot reads the process table (procTreeCache.snap overrides it).
func psSnapshot(ctx context.Context) ([]procRow, error) {
	if runtime.GOOS == "windows" {
		return nil, errors.New("process stats unsupported on windows")
	}
	out, err := exec.CommandContext(ctx, "ps", "-A", "-o", "pid=,ppid=,rss=").Output()
	if err != nil {
		return nil, err
	}
	return parsePSRows(out), nil
}

// parsePSRows parses `ps -o pid=,ppid=,rss=` output (rss in KiB on darwin and
// linux). Lines that do not parse are skipped.
func parsePSRows(out []byte) []procRow {
	var rows []procRow
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 3 {
			continue
		}
		pid, err1 := strconv.Atoi(f[0])
		ppid, err2 := strconv.Atoi(f[1])
		rss, err3 := strconv.ParseInt(f[2], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		rows = append(rows, procRow{PID: pid, PPID: ppid, RSSKB: rss})
	}
	return rows
}

// procTree is one snapshot of the process table, indexed for tree walks.
type procTree struct {
	children map[int][]int
	rssKB    map[int]int64
}

func newProcTree(rows []procRow) *procTree {
	t := &procTree{children: make(map[int][]int, len(rows)), rssKB: make(map[int]int64, len(rows))}
	for _, r := range rows {
		t.rssKB[r.PID] = r.RSSKB
		if r.PPID != r.PID {
			t.children[r.PPID] = append(t.children[r.PPID], r.PID)
		}
	}
	return t
}

// stats walks the tree rooted at pid: its process count and total RSS in
// bytes. (0, 0) when pid is not in the snapshot (it exited).
func (t *procTree) stats(pid int) (count int, rssBytes int64) {
	if t == nil || pid <= 0 {
		return 0, 0
	}
	if _, ok := t.rssKB[pid]; !ok {
		return 0, 0
	}
	seen := map[int]bool{}
	stack := []int{pid}
	for len(stack) > 0 {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[p] {
			continue
		}
		seen[p] = true
		count++
		rssBytes += t.rssKB[p] * 1024
		stack = append(stack, t.children[p]...)
	}
	return count, rssBytes
}

// procTreeCache holds the latest snapshot for procStatsTTL.
type procTreeCache struct {
	mu   sync.Mutex
	at   time.Time
	tree *procTree
	err  error
	// snap reads the process table; nil = psSnapshot. A test seam per driver,
	// so no test swaps a package global under another's feet.
	snap func(context.Context) ([]procRow, error)
}

func (c *procTreeCache) get(ctx context.Context) (*procTree, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.at.IsZero() && time.Since(c.at) < procStatsTTL {
		return c.tree, c.err
	}
	pctx, cancel := context.WithTimeout(ctx, procStatsTimeout)
	defer cancel()
	snap := c.snap
	if snap == nil {
		snap = psSnapshot
	}
	rows, err := snap(pctx)
	c.at, c.err = time.Now(), err
	if err != nil {
		c.tree = nil
	} else {
		c.tree = newProcTree(rows)
	}
	return c.tree, c.err
}

// livePID is the agent process's pid while it runs, 0 once its connection has
// ended (the pid may already belong to another process) or before it started.
func (p *hermesProcess) livePID() (pid, sessions int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sessions = len(p.sessions)
	if p.cmd == nil || p.cmd.Process == nil || p.conn == nil {
		return 0, sessions
	}
	select {
	case <-p.conn.Done():
		return 0, sessions
	default:
	}
	return p.cmd.Process.Pid, sessions
}

// withProcessStats fills the process fields of each info from one snapshot.
// sessions and infos are parallel.
func (d *HermesACPDriver) withProcessStats(ctx context.Context, sessions []*HermesACPSession, infos []ManagedSessionInfo) error {
	if len(sessions) == 0 {
		return nil // nothing to measure: no ps
	}
	tree, err := d.procs.get(ctx)
	for i, s := range sessions {
		if s.proc == nil {
			continue
		}
		pid, shared := s.proc.livePID()
		if pid == 0 {
			continue
		}
		infos[i].PID, infos[i].SharedProcess = pid, shared
		if tree != nil {
			infos[i].ProcCount, infos[i].RSSBytes = tree.stats(pid)
		}
	}
	return err
}

// ListDetailed is List with the process fields filled (the HTTP list).
func (d *HermesACPDriver) ListDetailed(ctx context.Context) []ManagedSessionInfo {
	sessions := d.snapshotSessions()
	infos := make([]ManagedSessionInfo, len(sessions))
	for i, s := range sessions {
		infos[i] = s.Info()
	}
	_ = d.withProcessStats(ctx, sessions, infos)
	return infos
}

// InfoDetailed is s.Info() with the process fields filled (the HTTP get).
func (d *HermesACPDriver) InfoDetailed(ctx context.Context, s *HermesACPSession) ManagedSessionInfo {
	infos := []ManagedSessionInfo{s.Info()}
	_ = d.withProcessStats(ctx, []*HermesACPSession{s}, infos)
	return infos[0]
}

func (d *HermesACPDriver) snapshotSessions() []*HermesACPSession {
	d.mu.Lock()
	defer d.mu.Unlock()
	all := make([]*HermesACPSession, 0, len(d.sessions))
	for _, s := range d.sessions {
		all = append(all, s)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].createdAt.Before(all[j].createdAt) })
	return all
}

// ManagedSessionSummary is GET /v1/managed-sessions/summary: what the driver
// holds and what it costs, plus what the reaper has let go of.
type ManagedSessionSummary struct {
	Count              int            `json:"count"`
	ByState            map[string]int `json:"by_state"`
	ByOwner            map[string]int `json:"by_owner"`
	ByClass            map[string]int `json:"by_class"`
	TurnsRunning       int            `json:"turns_running"`
	PendingPermissions int            `json:"pending_permissions"`
	// Processes counts distinct agent processes (forks share one); ProcCount
	// and RSSBytes total their whole trees, each process counted once.
	Processes       int          `json:"processes"`
	ProcCount       int          `json:"proc_count"`
	RSSBytes        int64        `json:"rss_bytes"`
	ProcStatsError  string       `json:"proc_stats_error,omitempty"`
	OldestIdleSecs  int64        `json:"oldest_idle_secs"`
	IdlePastTimeout int          `json:"idle_past_timeout"` // idle beyond idle_timeout but kept (busy) or not yet ticked
	IdleTimeoutSecs int64        `json:"idle_timeout_secs"` // <= 0: idle reaping off
	ReapIntervalSec int64        `json:"reap_interval_secs"`
	ReapedTotal     int64        `json:"reaped_total"`
	RecentReaps     []ReapRecord `json:"recent_reaps"`
}

// Summary builds the summary from one session snapshot and one ps snapshot.
func (d *HermesACPDriver) Summary(ctx context.Context) ManagedSessionSummary {
	sessions := d.snapshotSessions()
	infos := make([]ManagedSessionInfo, len(sessions))
	for i, s := range sessions {
		infos[i] = s.Info()
	}
	statsErr := d.withProcessStats(ctx, sessions, infos)
	sum := ManagedSessionSummary{
		Count: len(infos), ByState: map[string]int{}, ByOwner: map[string]int{}, ByClass: map[string]int{},
		IdleTimeoutSecs: int64(d.cfg.IdleTimeout / time.Second), ReapIntervalSec: int64(d.cfg.ReapInterval / time.Second),
	}
	if statsErr != nil {
		sum.ProcStatsError = statsErr.Error()
	}
	seen := map[int]bool{}
	for _, info := range infos {
		sum.ByState[info.State]++
		owner := info.Owner
		if owner == "" {
			owner = "(none)"
		}
		sum.ByOwner[owner]++
		class := info.Class
		if class == "" {
			class = "(default)"
		}
		sum.ByClass[class]++
		if info.TurnRunning {
			sum.TurnsRunning++
		}
		sum.PendingPermissions += len(info.PendingPermissions)
		if info.IdleSecs > sum.OldestIdleSecs {
			sum.OldestIdleSecs = info.IdleSecs
		}
		if d.cfg.IdleTimeout > 0 && time.Duration(info.IdleSecs)*time.Second >= d.cfg.IdleTimeout {
			sum.IdlePastTimeout++
		}
		if info.PID > 0 && !seen[info.PID] {
			seen[info.PID] = true
			sum.Processes++
			sum.ProcCount += info.ProcCount
			sum.RSSBytes += info.RSSBytes
		}
	}
	sum.ReapedTotal, sum.RecentReaps = d.reaps.snapshot()
	if sum.RecentReaps == nil {
		sum.RecentReaps = []ReapRecord{}
	}
	return sum
}

// HealthBlock is the compact managed_sessions block on GET /health.
func (d *HermesACPDriver) HealthBlock(ctx context.Context) map[string]any {
	s := d.Summary(ctx)
	out := map[string]any{
		"count":             s.Count,
		"by_state":          s.ByState,
		"turns_running":     s.TurnsRunning,
		"processes":         s.Processes,
		"proc_count":        s.ProcCount,
		"rss_bytes":         s.RSSBytes,
		"oldest_idle_secs":  s.OldestIdleSecs,
		"idle_past_timeout": s.IdlePastTimeout,
		"idle_timeout_secs": s.IdleTimeoutSecs,
		"reaped_total":      s.ReapedTotal,
	}
	if s.ProcStatsError != "" {
		out["proc_stats_error"] = s.ProcStatsError
	}
	return out
}
