// cli_doctor_hermesenv.go: Hermes grant provenance and the inherited-env trap.
//
// Three checks over every ~/.hermes/profiles/*/config.yaml that fills an
// api_key from ${env:VAR}:
//
//  1. grant provenance (offline): resolve VAR the way Hermes does and compare
//     sha256(grant) with this kernel's grant ledger. A grant this kernel never
//     issued is another node's; one it issued but no longer honors is stale.
//  2. override_existing: Hermes' command secret source defaults to
//     override_existing=false (agent/secret_sources/command.py), so a parent
//     process already holding an old VAR silently wins over the vault helper.
//  3. running processes (Linux, /proc, same-user only): any process whose
//     startup environment carries VAR with a grant that is not live here.
//
// Output names variable, file, pid and sha256 prefix only; never a value.
package engine

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

const defaultHermesGrantVar = "COGOS_HERMES_GRANT"

func doctorHermesGrantHygiene(g *DoctorGroup, root string, opts DoctorOptions) {
	h := opts.doctorHostOrDefault()
	profiles := loadHermesProfiles(h.home)
	cache := opts.hermesCache()
	ledger := loadGrantLedger(root)

	vars := map[string]bool{}
	checked := 0
	for _, p := range profiles {
		refs := p.envRefs()
		if len(refs) == 0 {
			continue
		}
		// Only the grant (providers.cogos.api_key) is classified against the
		// grant ledger; other providers' keys are never in it.
		for _, v := range p.grantVars() {
			vars[v] = true
		}
		checked++

		// (2) override_existing trap.
		if p.SecretsEnabled && strings.TrimSpace(p.SecretsCommand) != "" && !p.OverrideExisting {
			var names []string
			for _, r := range refs {
				names = append(names, "${env:"+r[1]+"}")
			}
			g.add("hermes inherited env: "+p.Name, StatusWarn, fmt.Sprintf(
				"%s fills %s via secrets.command without override_existing: true; Hermes then lets a parent process's already-set variable win over the vault helper, so an old inherited grant is used silently. "+
					"Fix: set secrets.command.override_existing: true in %s. Detail: hermes agent/secret_sources/command.py (override_existing defaults to false)",
				p.label(), strings.Join(names, ", "), p.Path))
		}

		// (1) grant provenance, only meaningful when the profile targets a local kernel.
		cogos, ok := p.Providers["cogos"]
		if !ok || cogos.APIKey == "" {
			continue
		}
		if host, _ := splitHostPort(cogos.BaseURL); host != "" && !isLocalHost(host) {
			g.add("hermes grant provenance: "+p.Name, StatusUnknown, fmt.Sprintf(
				"providers.cogos.base_url in %s targets %s, not this node's kernel, so this kernel's ledger cannot classify its grant", p.label(), host))
			continue
		}
		val, how, unresolved, note, shadowed := cache.resolveX(p, cogos.APIKey, false)
		if len(unresolved) > 0 {
			g.add("hermes grant provenance: "+p.Name, StatusUnknown, fmt.Sprintf(
				"providers.cogos.api_key %s in %s not resolvable (%s not in process env%s); cannot classify",
				cogos.APIKey, p.label(), strings.Join(unresolved, ", "), helperClause(note)))
			continue
		}
		if how == "literal" {
			how = "literal api_key"
		}
		if how == "process env" && shadowed {
			// The verdict would depend on the caller's shell, not the profile.
			// A service (systemd/launchd) does not inherit it, so judge the
			// helper's value for the profile and warn about the shell apart.
			if hv, _, hun, _, _ := cache.resolveX(p, cogos.APIKey, true); len(hun) == 0 {
				g.add("hermes shell env: "+p.Name, StatusWarn, fmt.Sprintf(
					"this shell exports %s (sha256:%s) which differs from the secrets.command helper's value; a Hermes started from here would use the shell's. "+
						"Fix: unset %s in this shell (a service-managed Hermes does not inherit it). Detail: %s",
					firstVarOf(p), grantFingerprint(val), firstVarOf(p), p.label()))
				val, how = hv, "secrets.command helper (shell env ignored)"
			}
		}
		c := ledger.classify(val)
		switch c.Kind {
		case grantLive:
			g.add("hermes grant provenance: "+p.Name, StatusOK, fmt.Sprintf("%s; resolved via %s from %s", c.describe(), how, p.label()))
		case grantLedgerMissing:
			g.add("hermes grant provenance: "+p.Name, StatusUnknown, fmt.Sprintf("%s; expected at %s", c.describe(), ledger.path))
		default:
			g.add("hermes grant provenance: "+p.Name, StatusFail, fmt.Sprintf(
				"%s; resolved via %s from %s. Fix: make the profile's secrets.command helper print this node's grant (~/.cog/vault/node-root-grant) as %s and set override_existing: true. Detail: %s",
				c.describe(), how, p.label(), firstVarOf(p), ledger.path))
		}
	}
	if checked == 0 {
		g.add("hermes inherited env", StatusUnknown, "no Hermes profile with an ${env:VAR} api_key found under ~/.hermes/profiles")
	}
	if len(vars) == 0 {
		vars[defaultHermesGrantVar] = true
	}
	doctorStaleInheritedGrants(g, ledger, vars, h)
}

// grantVars lists the env vars providers.cogos.api_key references, defaulting
// to COGOS_HERMES_GRANT when the profile does not reference any.
func (p hermesProfile) grantVars() []string {
	var out []string
	if c, ok := p.Providers["cogos"]; ok {
		for _, m := range hermesEnvRefRE.FindAllStringSubmatch(c.APIKey, -1) {
			out = append(out, m[1])
		}
	}
	return out
}

func firstVarOf(p hermesProfile) string {
	if v := p.grantVars(); len(v) > 0 {
		return v[0]
	}
	return defaultHermesGrantVar
}

// doctorStaleInheritedGrants scans same-user processes' startup environments
// for the given variables and WARNs on grants that are not live on this kernel.
func doctorStaleInheritedGrants(g *DoctorGroup, ledger *grantLedgerView, vars map[string]bool, h doctorHost) {
	const name = "hermes inherited env: running processes"
	if h.goos != "linux" {
		g.add(name, StatusUnknown, fmt.Sprintf("/proc environment inspection is Linux only (this host: %s)", h.goos))
		return
	}
	if !ledger.exists {
		g.add(name, StatusUnknown, fmt.Sprintf("no grant ledger at %s to compare process grants against", ledger.path))
		return
	}
	var names []string
	for v := range vars {
		names = append(names, v)
	}
	sort.Strings(names)

	self := os.Getpid()
	scanned, holders, stale := 0, 0, 0
	var offenders []string
	for _, pid := range procPIDs(h.procRoot) {
		if uid, ok := procUID(h.procRoot, pid); !ok || uid != h.uid {
			continue
		}
		env, err := procEnviron(h.procRoot, pid)
		if err != nil {
			continue
		}
		scanned++
		for _, v := range names {
			val := strings.TrimSpace(env[v])
			if val == "" {
				continue
			}
			holders++
			c := ledger.classify(val)
			if c.Kind == grantLive {
				continue
			}
			stale++
			who := procComm(h.procRoot, pid)
			if pid == self {
				who += ", this doctor process"
			}
			kind := "NOT issued by this kernel"
			if c.Kind == grantStale {
				kind = "issued here but no longer live"
			}
			offenders = append(offenders, fmt.Sprintf("pid %d (%s) %s sha256:%s %s", pid, who, v, c.Hash10, kind))
		}
	}
	if stale > 0 {
		shown := offenders
		if len(shown) > 10 {
			shown = append(append([]string{}, shown[:10]...), fmt.Sprintf("... %d more", len(shown)-10))
		}
		g.add(name, StatusWarn, fmt.Sprintf(
			"%d process(es) hold a startup grant that is not live on this kernel: %s. "+
				"Fix: restart those processes from a clean environment (env -u %s ...) so the vault helper's value is used. Detail: /proc/<pid>/environ, %s",
			stale, strings.Join(shown, "; "), strings.Join(names, " -u "), ledger.path))
		return
	}
	g.add(name, StatusOK, fmt.Sprintf("%d same-user process(es) scanned; %d carry %s and every such grant is live on this kernel",
		scanned, holders, strings.Join(names, "/")))
}
