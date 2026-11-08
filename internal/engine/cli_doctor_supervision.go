// cli_doctor_supervision.go: detect declared services that share one systemd
// unit (Linux), the "supervision trap".
//
// Observed 2026-11-08: the stage process was started from inside
// cogos-kernel.service's cgroup, so `systemctl restart cogos-kernel` killed the
// stage too. For every manifest-declared service with a port, doctor finds the
// listener pid, reads its cgroup unit, and WARNs when two declared services
// resolve to the same unit. Linux/systemd only; UNKNOWN elsewhere.
package engine

import (
	"fmt"
	"sort"
	"strings"
)

func doctorServiceSupervision(g *DoctorGroup, root string, opts DoctorOptions) {
	const name = "service supervision"
	h := opts.doctorHostOrDefault()
	if h.goos != "linux" {
		g.add(name, StatusUnknown, fmt.Sprintf("systemd cgroup inspection is Linux only (this host: %s)", h.goos))
		return
	}
	mpath := DefaultManifestPath(root)
	m, err := LoadManifest(mpath)
	if err != nil {
		g.add(name, StatusUnknown, fmt.Sprintf("no node manifest to enumerate declared services: %v", err))
		return
	}

	type svcUnit struct {
		svc  string
		pid  int
		unit string
	}
	var found []svcUnit
	checked := 0
	var names []string
	for n, s := range m.Services {
		if s.Port > 0 && s.Kind.EffectiveKind() != ServiceKindExternal {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	allNames := make([]string, 0, len(m.Services))
	for n := range m.Services {
		allNames = append(allNames, n)
	}
	sort.Strings(allNames)
	for _, n := range names {
		pids := procListenerPIDs(h.procRoot, m.Services[n].Port)
		if len(pids) == 0 {
			continue
		}
		checked++
		if u := procCgroupUnit(h.procRoot, pids[0]); u != "" {
			found = append(found, svcUnit{n, pids[0], u})
		}
	}
	if checked == 0 {
		g.add(name, StatusUnknown, fmt.Sprintf("none of the %d declared service port(s) in %s has a visible listener", len(names), mpath))
		return
	}

	byUnit := map[string][]svcUnit{}
	var units []string
	for _, f := range found {
		if _, ok := byUnit[f.unit]; !ok {
			units = append(units, f.unit)
		}
		byUnit[f.unit] = append(byUnit[f.unit], f)
	}
	sort.Strings(units)
	warned := false
	for _, u := range units {
		group := byUnit[u]
		if len(group) < 2 {
			continue
		}
		warned = true
		var parts []string
		for _, f := range group {
			parts = append(parts, fmt.Sprintf("%s (pid %d)", f.svc, f.pid))
		}
		g.add(name+": "+u, StatusWarn, fmt.Sprintf(
			"declared services %s all run inside systemd unit %s; restarting or stopping that unit kills every one of them. "+
				"Fix: run each under its own unit (e.g. systemd-run --user --unit=cogos-<service> ... or a dedicated .service), then restart. Detail: /proc/%d/cgroup, %s",
			strings.Join(parts, ", "), u, group[0].pid, mpath))
	}
	// A kernel running as another user (or root) has unreadable fds, so its
	// listener is invisible and the pairwise test above cannot fire. A service
	// found inside the unit conventionally named for ANOTHER declared service
	// (cogos-<name>.service) is the same trap.
	for _, f := range found {
		for _, other := range allNames {
			if other == f.svc || f.unit != "cogos-"+other+".service" {
				continue
			}
			if len(byUnit[f.unit]) >= 2 {
				continue // already reported pairwise
			}
			warned = true
			g.add(name+": "+f.unit, StatusWarn, fmt.Sprintf(
				"declared service %s (pid %d) runs inside %s, the unit conventionally supervising declared service %s (its own listener is not visible to this user); restarting that unit kills %s. "+
					"Fix: run %s under its own unit (e.g. systemd-run --user --unit=cogos-%s ...), then restart. Detail: /proc/%d/cgroup, %s",
				f.svc, f.pid, f.unit, other, f.svc, f.svc, f.svc, f.pid, mpath))
		}
	}
	if !warned {
		g.add(name, StatusOK, fmt.Sprintf("%d declared service listener(s) inspected (%d in a systemd service unit); none share a unit", checked, len(found)))
	}
}
