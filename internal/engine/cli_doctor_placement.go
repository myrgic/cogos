// cli_doctor_placement.go: WARN when a configured endpoint depends on a node
// that placement does not declare always_on (a laptop).
//
// Observed 2026-11-08: cog-node's stage.env pointed STAGE_TTS/STAGE_UPSTREAM at
// Darkstar (a laptop) and providers.yaml named an lmstudio-darkstar endpoint.
// Declared intent lives in <workspace>/.cog/config/placement.yaml (always_on);
// node addresses live in .cog/conf/registry/machines/*.yaml (address/hostname),
// bound to nodes by .cog/config/node/nodes/*.node.yaml, and in
// .cog/config/cluster.yaml peers. Matching is static (address, hostname, or
// first DNS label == node id); doctor does no DNS.
package engine

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type endpointRef struct {
	setting string // human pointer: file + key
	host    string
}

type nodeAddrIndex struct {
	byAddr map[string]string // lowercased address/hostname -> node id
	nodes  map[string]bool   // known node ids
}

func (ix *nodeAddrIndex) add(addr, node string) {
	addr = strings.ToLower(strings.TrimSpace(addr))
	if addr == "" || node == "" {
		return
	}
	ix.byAddr[addr] = node
	ix.nodes[node] = true
}

// nodeFor maps a host to a known node id, or "".
func (ix *nodeAddrIndex) nodeFor(host string) string {
	host = strings.ToLower(host)
	if n, ok := ix.byAddr[host]; ok {
		return n
	}
	if net.ParseIP(host) == nil {
		first := host
		if i := strings.Index(host, "."); i > 0 {
			first = host[:i]
		}
		if ix.nodes[first] {
			return first
		}
		if n, ok := ix.byAddr[first]; ok {
			return n
		}
	}
	return ""
}

func loadNodeAddrIndex(root string) *nodeAddrIndex {
	ix := &nodeAddrIndex{byAddr: map[string]string{}, nodes: map[string]bool{}}
	cog := filepath.Join(root, ".cog")

	// node id -> machine id (defaults to the same id)
	machineNode := map[string]string{}
	for _, f := range globQuiet(filepath.Join(cog, "config", "node", "nodes", "*.node.yaml")) {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var d struct {
			Metadata struct {
				ID string `yaml:"id"`
			} `yaml:"metadata"`
			Binds struct {
				Machine string `yaml:"machine"`
			} `yaml:"binds"`
		}
		if yaml.Unmarshal(data, &d) != nil || d.Metadata.ID == "" {
			continue
		}
		ix.nodes[d.Metadata.ID] = true
		m := d.Binds.Machine
		if m == "" {
			m = d.Metadata.ID
		}
		machineNode[m] = d.Metadata.ID
	}
	for _, f := range globQuiet(filepath.Join(cog, "conf", "registry", "machines", "*.yaml")) {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var d struct {
			Hostname string `yaml:"hostname"`
			Address  string `yaml:"address"`
		}
		if yaml.Unmarshal(data, &d) != nil {
			continue
		}
		mid := strings.TrimSuffix(filepath.Base(f), ".yaml")
		node := machineNode[mid]
		if node == "" {
			node = mid
		}
		ix.add(d.Address, node)
		ix.add(d.Hostname, node)
	}
	if data, err := os.ReadFile(filepath.Join(cog, "config", "cluster.yaml")); err == nil {
		var c struct {
			Peers []struct {
				Name    string `yaml:"name"`
				Address string `yaml:"address"`
			} `yaml:"peers"`
		}
		if yaml.Unmarshal(data, &c) == nil {
			for _, p := range c.Peers {
				host, _, err := net.SplitHostPort(p.Address)
				if err != nil {
					host = p.Address
				}
				node := ""
				for id := range ix.nodes {
					if p.Name == id || strings.HasPrefix(p.Name, id+"-") {
						node = id
					}
				}
				if node == "" {
					node = p.Name
				}
				if _, ok := ix.byAddr[strings.ToLower(host)]; !ok {
					ix.add(host, node)
				}
			}
		}
	}
	return ix
}

var hostPortValueRE = regexp.MustCompile(`^(?:[a-zA-Z][a-zA-Z0-9+.-]*://)?([A-Za-z0-9._-]+)(?::[0-9]+)?(?:/.*)?$`)

// hostOfSetting extracts a host from a URL or host[:port] value, "" if the
// value does not look like one.
func hostOfSetting(v string) string {
	v = strings.TrimSpace(strings.Trim(v, `"'`))
	if v == "" || strings.ContainsAny(v, " \t") {
		return ""
	}
	if strings.Contains(v, "://") {
		if h, _ := splitHostPort(v); h != "" {
			return h
		}
		return ""
	}
	m := hostPortValueRE.FindStringSubmatch(v)
	if m == nil || !strings.Contains(v, ":") && net.ParseIP(m[1]) == nil {
		return "" // bare words are not endpoints
	}
	return m[1]
}

// stageEnvFiles lists stage.env files under the stage state dir(s).
func stageEnvFiles(home string) []string {
	var files []string
	if st := os.Getenv("STAGE_STATE"); st != "" {
		files = append(files, filepath.Join(st, "stage.env"))
	}
	if home != "" {
		files = append(files, globQuiet(filepath.Join(home, ".hermes", "profiles", "*", "lanes", "runs", "stage", "stage.env"))...)
	}
	return dedupeStrings(files)
}

func doctorPlacementDependencies(g *DoctorGroup, root string, opts DoctorOptions) {
	const name = "placement: always_on dependencies"
	pf := filepath.Join(root, ".cog", "config", "placement.yaml")
	data, err := os.ReadFile(pf)
	if err != nil {
		g.add(name, StatusUnknown, fmt.Sprintf("no placement declaration to compare against: %v", err))
		return
	}
	var pl struct {
		AlwaysOn []string `yaml:"always_on"`
	}
	if err := yaml.Unmarshal(data, &pl); err != nil || len(pl.AlwaysOn) == 0 {
		g.add(name, StatusUnknown, fmt.Sprintf("%s declares no always_on nodes (parse err: %v)", pf, err))
		return
	}
	alwaysOn := map[string]bool{}
	for _, n := range pl.AlwaysOn {
		alwaysOn[n] = true
	}
	ix := loadNodeAddrIndex(root)
	home := opts.doctorHostOrDefault().home

	var refs []endpointRef
	// providers*.yaml
	if cfg, cerr := LoadConfig(root, 0); cerr == nil {
		if pcfg, perr := loadProvidersConfig(cfg); perr == nil {
			var pn []string
			for n := range pcfg.Providers {
				pn = append(pn, n)
			}
			sort.Strings(pn)
			for _, n := range pn {
				pc := pcfg.Providers[n]
				if !pc.IsEnabled() || pc.Endpoint == "" || cliProviderTypes[pc.Type] {
					continue
				}
				if h := hostOfSetting(pc.Endpoint); h != "" {
					refs = append(refs, endpointRef{fmt.Sprintf("providers.%s.endpoint (.cog/config/providers*.yaml)", n), h})
				}
			}
		}
	}
	// stage.env-style files
	for _, f := range stageEnvFiles(home) {
		raw, rerr := os.ReadFile(f)
		if rerr != nil {
			continue
		}
		kv := parseDotenvMap(string(raw))
		var keys []string
		for k := range kv {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if h := hostOfSetting(kv[k]); h != "" {
				refs = append(refs, endpointRef{fmt.Sprintf("%s in %s", k, f), h})
			}
		}
	}
	// Hermes profile base_urls
	for _, p := range loadHermesProfiles(home) {
		var pn []string
		for n := range p.Providers {
			pn = append(pn, n)
		}
		sort.Strings(pn)
		for _, n := range pn {
			if h := hostOfSetting(p.Providers[n].BaseURL); h != "" {
				refs = append(refs, endpointRef{fmt.Sprintf("providers.%s.base_url in %s", n, p.label()), h})
			}
		}
	}

	scanned, flagged := 0, 0
	for _, r := range refs {
		if isLocalHost(r.host) {
			continue
		}
		scanned++
		node := ix.nodeFor(r.host)
		if node == "" || alwaysOn[node] {
			continue
		}
		flagged++
		g.add(name+": "+r.setting, StatusWarn, fmt.Sprintf(
			"host %s is node %q, which placement does not list as always_on (%s); this node stops working when %s sleeps or goes offline. "+
				"Fix: point the setting at an always_on node's address (or move the service there). Detail: %s",
			r.host, node, strings.Join(pl.AlwaysOn, ", "), node, pf))
	}
	if flagged == 0 {
		g.add(name, StatusOK, fmt.Sprintf("%d non-loopback endpoint setting(s) scanned (providers*.yaml, stage.env, Hermes base_urls); none resolve to a node outside always_on [%s]",
			scanned, strings.Join(pl.AlwaysOn, ", ")))
	}
}
