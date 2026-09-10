package engine

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// runNodeCmd dispatches `cogos node <subcommand>`.
func runNodeCmd(args []string, defaultWorkspace string) {
	// Pre-parse --workspace flag before subcommand dispatch.
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	workspace := fs.String("workspace", defaultWorkspace, "Workspace root")
	_ = fs.Parse(args)
	remaining := fs.Args()

	if len(remaining) == 0 {
		printNodeHelp()
		return
	}
	switch remaining[0] {
	case "status":
		runNodeStatus(remaining[1:], *workspace)
	case "sync":
		runNodeSync(remaining[1:], *workspace)
	case "check":
		runNodeCheck(remaining[1:], *workspace)
	case "help", "-h", "--help":
		printNodeHelp()
	default:
		fmt.Fprintf(os.Stderr, "unknown node command: %s\n", remaining[0])
		printNodeHelp()
		os.Exit(1)
	}
}

func printNodeHelp() {
	fmt.Fprintln(os.Stderr, `cog node — node control plane

COMMANDS:
    status          Show health, port, and PID for all services
    sync            Dry-run: show what consumer files would change
    sync --apply    Propagate port values to all consumer files
    check           Detect port conflicts`)
}

// ── status ───────────────────────────────────────────���──────────────────────

func runNodeStatus(args []string, workspace string) {
	ws, m := loadNodeManifest(workspace)

	names := sortedServiceNames(m)

	isTTY := fileIsTerminal(os.Stdout)

	fmt.Printf("%-14s %6s  %-10s %7s  %s\n", "SERVICE", "PORT", "HEALTH", "PID", "UPTIME")
	fmt.Println("------------------------------------------------------")

	healthy := 0
	for _, name := range names {
		svc := m.Services[name]
		health, pid, uptime := probeService(svc)
		if health == "healthy" {
			healthy++
		}

		healthStr := health
		if isTTY {
			switch health {
			case "healthy":
				healthStr = "\033[32m" + health + "\033[0m"
			case "degraded":
				healthStr = "\033[33m" + health + "\033[0m"
			case "unknown":
				// Dim, not red: we have no evidence, which is not the same
				// as evidence of failure.
				healthStr = "\033[90m" + health + "\033[0m"
			default:
				healthStr = "\033[31m" + health + "\033[0m"
			}
		}

		fmt.Printf("%-14s %6d  %-21s %7s  %s\n", name, svc.Port, healthStr, pid, uptime)
	}

	fmt.Println()
	fmt.Printf("Services: %d/%d healthy\n", healthy, len(names))

	_ = ws // used only for manifest loading
}

func probeService(svc ServiceDef) (health, pid, uptime string) {
	health = "down"
	pid = "-"
	uptime = "-"

	// Same contract as NodeHealth.Probe: a service we cannot speak HTTP to is
	// "unknown", never "down". See probeable() in node_probe.go.
	if !probeable(svc) {
		return "unknown", pid, uptime
	}

	url := fmt.Sprintf("http://localhost:%d%s", svc.Port, svc.Health)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			health = "healthy"
		} else {
			health = "degraded"
		}
	}

	foundPID := findPIDOnPort(svc.Port)
	if foundPID != "" {
		pid = foundPID
		uptime = processUptime(foundPID)
	}
	return
}

func findPIDOnPort(port int) string {
	out, err := exec.Command("lsof", "-ti", fmt.Sprintf(":%d", port)).Output()
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) > 0 && lines[0] != "" {
		return lines[0]
	}
	return ""
}

func processUptime(pid string) string {
	out, err := exec.Command("ps", "-o", "etime=", "-p", pid).Output()
	if err != nil {
		return "-"
	}
	return strings.TrimSpace(string(out))
}

// ── check ──────────────────────────────────���────────────────────────────���───

func runNodeCheck(args []string, workspace string) {
	_, m := loadNodeManifest(workspace)
	names := sortedServiceNames(m)

	fmt.Println("Checking port conflicts...")

	// Detect duplicate ports within the manifest.
	portOwners := map[int][]string{}
	for _, name := range names {
		p := m.Services[name].Port
		portOwners[p] = append(portOwners[p], name)
	}

	conflicts := 0
	for _, name := range names {
		svc := m.Services[name]
		pid := findPIDOnPort(svc.Port)
		if pid == "" {
			fmt.Printf("  %5d  %-14s  (unused)\n", svc.Port, name)
		} else {
			procName := processName(pid)
			fmt.Printf("  %5d  %-14s  %s (pid %s)\n", svc.Port, name, procName, pid)
		}
	}

	for port, owners := range portOwners {
		if len(owners) > 1 {
			fmt.Printf("\nCONFLICT: port %d claimed by multiple services: %s\n", port, strings.Join(owners, ", "))
			conflicts++
		}
	}

	if conflicts == 0 {
		fmt.Println("\nNo conflicts detected.")
	} else {
		os.Exit(1)
	}
}

func processName(pid string) string {
	out, err := exec.Command("ps", "-o", "comm=", "-p", pid).Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// ── sync ───────────────────────────────────────────────────────────────────���

func runNodeSync(args []string, workspace string) {
	fs := flag.NewFlagSet("node sync", flag.ExitOnError)
	apply := fs.Bool("apply", false, "Apply changes (default: dry-run)")
	_ = fs.Parse(args)

	ws, m := loadNodeManifest(workspace)
	names := sortedServiceNames(m)

	var updated, current, errors int

	for _, name := range names {
		svc := m.Services[name]
		if len(svc.Consumers) == 0 {
			continue
		}

		fmt.Printf("%s (port %d):\n", name, svc.Port)

		for _, c := range svc.Consumers {
			resolved := resolvePath(c.Path, ws)
			if _, err := os.Stat(resolved); err != nil {
				fmt.Printf("  [SKIP] %s — file not found\n", c.Path)
				continue
			}

			var result syncResult
			switch c.Type {
			case "json":
				result = syncJSON(resolved, c, svc.Port, *apply)
			case "sed":
				result = syncSed(resolved, c, svc.Port, *apply)
			case "plist":
				result = syncPlist(resolved, svc.Port, *apply)
			default:
				fmt.Printf("  [SKIP] %s — unknown type %q\n", c.Path, c.Type)
				continue
			}

			switch result.state {
			case syncOK:
				fmt.Printf("  [ok] %s\n", c.Path)
				current++
			case syncDrift:
				if *apply {
					fmt.Printf("  [FIXED] %s — %s\n", c.Path, result.detail)
					updated++
				} else {
					fmt.Printf("  [DRIFT] %s — %s\n", c.Path, result.detail)
					updated++
				}
			case syncError:
				fmt.Printf("  [ERROR] %s — %s\n", c.Path, result.detail)
				errors++
			}
		}
	}

	fmt.Println()
	if *apply {
		fmt.Printf("Sync complete: %d updated, %d already current, %d errors\n", updated, current, errors)
	} else {
		fmt.Printf("Dry run: %d would change, %d already current, %d errors\n", updated, current, errors)
		if updated > 0 {
			fmt.Println("Run with --apply to write changes.")
		}
	}
}

type syncState int

const (
	syncOK syncState = iota
	syncDrift
	syncError
)

type syncResult struct {
	state  syncState
	detail string
}

// syncJSON handles consumer entries of type "json".
// Uses a simple top-level-aware JSON path (e.g. ".mcpServers.cogos.url")
// rather than pulling in a full jsonpath library.
func syncJSON(path string, c ConsumerEntry, port int, apply bool) syncResult {
	data, err := os.ReadFile(path)
	if err != nil {
		return syncResult{syncError, err.Error()}
	}

	desired := interpolatePort(c.Template, port)

	// Parse the JSON into a generic structure.
	var root any
	if err := json.Unmarshal(data, &root); err != nil {
		return syncResult{syncError, fmt.Sprintf("parse: %v", err)}
	}

	// Navigate the path and read the current value.
	keys := parseJSONPath(c.JSONPath)
	currentVal, err := jsonGet(root, keys)
	if err != nil {
		return syncResult{syncError, fmt.Sprintf("read %s: %v", c.JSONPath, err)}
	}

	// Compare current value to desired.
	currentStr := fmt.Sprintf("%v", currentVal)
	if currentStr == desired {
		return syncResult{syncOK, ""}
	}

	if !apply {
		return syncResult{syncDrift, fmt.Sprintf("%s -> %s", currentStr, desired)}
	}

	// Determine the replacement value type.
	var newVal any
	if n, err := strconv.Atoi(desired); err == nil {
		newVal = n
	} else {
		newVal = desired
	}

	updated, err := jsonSet(root, keys, newVal)
	if err != nil {
		return syncResult{syncError, fmt.Sprintf("set %s: %v", c.JSONPath, err)}
	}

	out, err := json.MarshalIndent(updated, "", "  ")
	if err != nil {
		return syncResult{syncError, fmt.Sprintf("marshal: %v", err)}
	}
	// Preserve trailing newline.
	out = append(out, '\n')

	if err := os.WriteFile(path, out, 0644); err != nil {
		return syncResult{syncError, fmt.Sprintf("write: %v", err)}
	}
	return syncResult{syncDrift, fmt.Sprintf("%s -> %s", currentStr, desired)}
}

// syncSed handles consumer entries of type "sed".
func syncSed(path string, c ConsumerEntry, port int, apply bool) syncResult {
	data, err := os.ReadFile(path)
	if err != nil {
		return syncResult{syncError, err.Error()}
	}

	content := string(data)
	replacement := interpolatePort(c.Replace, port)

	// If the file already contains the replacement string, it's in sync.
	if strings.Contains(content, replacement) {
		return syncResult{syncOK, ""}
	}

	// Check if the match pattern exists (i.e., there's drift to fix).
	if !strings.Contains(content, c.Match) {
		// Neither match nor replacement found — nothing to do.
		return syncResult{syncOK, ""}
	}

	count := strings.Count(content, c.Match)
	if !apply {
		return syncResult{syncDrift, fmt.Sprintf("%d occurrences of %q", count, c.Match)}
	}

	updated := strings.ReplaceAll(content, c.Match, replacement)
	if err := os.WriteFile(path, []byte(updated), 0644); err != nil {
		return syncResult{syncError, fmt.Sprintf("write: %v", err)}
	}
	return syncResult{syncDrift, fmt.Sprintf("replaced %d occurrences", count)}
}

// syncPlist handles consumer entries of type "plist".
// Finds the --port argument in ProgramArguments and updates the value after it.
func syncPlist(path string, port int, apply bool) syncResult {
	// Read plist as XML text and find the port value.
	data, err := os.ReadFile(path)
	if err != nil {
		return syncResult{syncError, err.Error()}
	}

	content := string(data)
	portStr := strconv.Itoa(port)

	// Find the pattern: <string>--port</string>\n\t\t<string>NNNN</string>
	// and check/update the port value.
	marker := "<string>--port</string>"
	idx := strings.Index(content, marker)
	if idx < 0 {
		return syncResult{syncError, "--port not found in ProgramArguments"}
	}

	// Find the next <string>...</string> after the marker.
	after := content[idx+len(marker):]
	openTag := "<string>"
	closeTag := "</string>"

	openIdx := strings.Index(after, openTag)
	if openIdx < 0 {
		return syncResult{syncError, "no value after --port"}
	}
	closeIdx := strings.Index(after[openIdx+len(openTag):], closeTag)
	if closeIdx < 0 {
		return syncResult{syncError, "malformed plist after --port"}
	}

	currentPort := after[openIdx+len(openTag) : openIdx+len(openTag)+closeIdx]
	if currentPort == portStr {
		return syncResult{syncOK, ""}
	}

	if !apply {
		return syncResult{syncDrift, fmt.Sprintf("port %s -> %s", currentPort, portStr)}
	}

	// Replace just this occurrence.
	oldFragment := marker + after[:openIdx+len(openTag)+closeIdx] + closeTag
	newFragment := marker + after[:openIdx+len(openTag)] + portStr + closeTag
	updated := strings.Replace(content, oldFragment, newFragment, 1)

	if err := os.WriteFile(path, []byte(updated), 0644); err != nil {
		return syncResult{syncError, fmt.Sprintf("write: %v", err)}
	}
	return syncResult{syncDrift, fmt.Sprintf("port %s -> %s", currentPort, portStr)}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func interpolatePort(template string, port int) string {
	return strings.ReplaceAll(template, "{{port}}", strconv.Itoa(port))
}

func resolvePath(p, workspaceRoot string) string {
	if strings.HasPrefix(p, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, p[2:])
	}
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(workspaceRoot, p)
}

func sortedServiceNames(m *NodeManifest) []string {
	names := make([]string, 0, len(m.Services))
	for k := range m.Services {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func loadNodeManifest(workspace string) (string, *NodeManifest) {
	ws, err := resolveManifestWorkspace(workspace)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	m, err := LoadManifest(DefaultManifestPath(ws))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	return ws, m
}

func fileIsTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// parseJSONPath splits ".foo.bar.baz" into ["foo", "bar", "baz"].
// Also handles array indices like ".args[3]" → ["args", "3"].
func parseJSONPath(jp string) []string {
	jp = strings.TrimPrefix(jp, ".")
	var keys []string
	for _, part := range strings.Split(jp, ".") {
		if idx := strings.Index(part, "["); idx >= 0 {
			keys = append(keys, part[:idx])
			inner := strings.TrimSuffix(part[idx+1:], "]")
			keys = append(keys, inner)
		} else {
			keys = append(keys, part)
		}
	}
	return keys
}

// jsonGet navigates a parsed JSON value by key path.
func jsonGet(root any, keys []string) (any, error) {
	current := root
	for _, key := range keys {
		switch v := current.(type) {
		case map[string]any:
			val, ok := v[key]
			if !ok {
				return nil, fmt.Errorf("key %q not found", key)
			}
			current = val
		case []any:
			idx, err := strconv.Atoi(key)
			if err != nil {
				return nil, fmt.Errorf("non-numeric index %q for array", key)
			}
			if idx < 0 || idx >= len(v) {
				return nil, fmt.Errorf("index %d out of range (len %d)", idx, len(v))
			}
			current = v[idx]
		default:
			return nil, fmt.Errorf("cannot index into %T with key %q", current, key)
		}
	}
	return current, nil
}

// jsonSet navigates a parsed JSON value and sets the leaf.
func jsonSet(root any, keys []string, value any) (any, error) {
	if len(keys) == 0 {
		return value, nil
	}

	key := keys[0]
	rest := keys[1:]

	switch v := root.(type) {
	case map[string]any:
		child, ok := v[key]
		if !ok {
			return nil, fmt.Errorf("key %q not found", key)
		}
		updated, err := jsonSet(child, rest, value)
		if err != nil {
			return nil, err
		}
		v[key] = updated
		return v, nil
	case []any:
		idx, err := strconv.Atoi(key)
		if err != nil {
			return nil, fmt.Errorf("non-numeric index %q for array", key)
		}
		if idx < 0 || idx >= len(v) {
			return nil, fmt.Errorf("index %d out of range (len %d)", idx, len(v))
		}
		updated, err := jsonSet(v[idx], rest, value)
		if err != nil {
			return nil, err
		}
		v[idx] = updated
		return v, nil
	default:
		return nil, fmt.Errorf("cannot index into %T", root)
	}
}
