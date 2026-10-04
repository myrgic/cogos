package engine

// podman_runtime.go — Podman backend for the ADR-065 ContainerRuntime.
//
// The kernel drives the installed `podman` binary directly. There is no
// Docker daemon, no Docker-compatible socket, and no Podman REST client: the
// CLI is the stable contract, and it is the same CLI on Linux (native,
// rootless), macOS and Windows (where Podman runs a `podman machine` VM).
//
// On hosts that need a machine (macOS, Windows), the runtime owns the machine
// the kernel was pointed at: EnsureReady starts it if it is stopped. Which
// machine is a declaration (COG_PODMAN_MACHINE), not a guess, because a
// machine's mounts are its isolation boundary: the stock default machine
// mounts the whole user home read-write into the VM.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
)

// Environment knobs for runtime selection.
const (
	// envContainerRuntime selects the backend: "podman", "nerdctl", or an
	// absolute path to either binary. Empty means auto-detect (podman first).
	envContainerRuntime = "COG_CONTAINER_RUNTIME"
	// envPodmanBin overrides the podman binary path.
	envPodmanBin = "COG_PODMAN"
	// envPodmanMachine names the podman machine (and its connection) the
	// kernel owns on macOS/Windows. Empty means podman's default connection.
	envPodmanMachine = "COG_PODMAN_MACHINE"
)

// PodmanRuntime implements ContainerRuntime with the podman CLI.
type PodmanRuntime struct {
	bin     string
	machine string // podman machine / connection name; "" = podman default
	// needsMachine is true on hosts where containers run inside a VM.
	needsMachine bool
	// run executes the binary; swapped in tests.
	run func(args ...string) ([]byte, error)
}

// NewPodmanRuntime locates the podman binary. It does not touch the machine;
// call EnsureReady before the first container operation.
func NewPodmanRuntime() (*PodmanRuntime, error) {
	bin, err := findBinary(envPodmanBin, "podman", []string{
		"/opt/homebrew/bin/podman",
		"/usr/local/bin/podman",
		"/opt/podman/bin/podman",
		"/usr/bin/podman",
	})
	if err != nil {
		return nil, fmt.Errorf("podman not found; install podman or set %s: %w", envPodmanBin, err)
	}
	return newPodmanRuntimeWithBin(bin, os.Getenv(envPodmanMachine), runtime.GOOS != "linux"), nil
}

func newPodmanRuntimeWithBin(bin, machine string, needsMachine bool) *PodmanRuntime {
	p := &PodmanRuntime{bin: bin, machine: machine, needsMachine: needsMachine}
	p.run = func(args ...string) ([]byte, error) {
		return exec.Command(p.bin, args...).CombinedOutput()
	}
	return p
}

// Bin returns the resolved podman binary path.
func (p *PodmanRuntime) Bin() string { return p.bin }

// global prefixes every container command with the declared connection, so a
// kernel pointed at machine X never silently drives podman's default machine.
func (p *PodmanRuntime) global(args ...string) []string {
	if p.machine == "" {
		return args
	}
	return append([]string{"--connection", p.machine}, args...)
}

// MachineState reports the declared machine's state ("running", "stopped",
// ...). On Linux, where no machine is needed, it returns "native".
func (p *PodmanRuntime) MachineState() (string, error) {
	if !p.needsMachine {
		return "native", nil
	}
	args := []string{"machine", "inspect"}
	if p.machine != "" {
		args = append(args, p.machine)
	}
	out, err := p.run(args...)
	if err != nil {
		return "", fmt.Errorf("podman machine inspect: %w: %s", err, strings.TrimSpace(string(out)))
	}
	var machines []struct {
		Name  string `json:"Name"`
		State string `json:"State"`
	}
	if err := json.Unmarshal(out, &machines); err != nil {
		return "", fmt.Errorf("parse podman machine inspect: %w", err)
	}
	if len(machines) == 0 {
		return "", errors.New("podman machine inspect: no machine")
	}
	return strings.ToLower(machines[0].State), nil
}

// EnsureReady starts the declared machine if it is not running. It is a
// no-op on Linux.
func (p *PodmanRuntime) EnsureReady() error {
	state, err := p.MachineState()
	if err != nil {
		return err
	}
	if state == "native" || state == "running" {
		return nil
	}
	args := []string{"machine", "start"}
	if p.machine != "" {
		args = append(args, p.machine)
	}
	out, err := p.run(args...)
	if err != nil {
		// A concurrent start races to "already running"; treat as ready.
		if strings.Contains(strings.ToLower(string(out)), "already running") {
			return nil
		}
		return fmt.Errorf("podman machine start: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (p *PodmanRuntime) Start(image string, config ContainerConfig) (string, error) {
	if err := p.EnsureReady(); err != nil {
		return "", err
	}
	args := []string{"run", "-d", "--replace", "--name", config.Name}
	if config.RestartPolicy != "" {
		args = append(args, "--restart", config.RestartPolicy)
	}
	if config.Port != 0 {
		args = append(args, "-p", fmt.Sprintf("%d:%d", config.Port, config.Port))
	}
	if config.WorkspaceRoot != "" {
		args = append(args,
			"-v", fmt.Sprintf("%s:%s", config.WorkspaceRoot, config.WorkspaceRoot),
			"-w", config.WorkspaceRoot,
		)
	}
	for _, k := range sortedStringKeys(config.Env) {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, config.Env[k]))
	}
	args = append(args, image)
	args = append(args, config.Command...)

	out, err := p.run(p.global(args...)...)
	if err != nil {
		return "", fmt.Errorf("podman run: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return lastLine(out), nil
}

func (p *PodmanRuntime) Stop(containerID string) error {
	out, err := p.run(p.global("stop", containerID)...)
	if err != nil {
		return fmt.Errorf("podman stop %s: %w: %s", containerID, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (p *PodmanRuntime) Status(containerID string) (ContainerStatus, error) {
	out, err := p.run(p.global("container", "inspect", containerID)...)
	if err != nil {
		if isNoSuchContainer(out) {
			return ContainerStatus{}, nil
		}
		return ContainerStatus{}, fmt.Errorf("podman inspect %s: %w: %s", containerID, err, strings.TrimSpace(string(out)))
	}
	var inspect []struct {
		State struct {
			Running bool   `json:"Running"`
			Status  string `json:"Status"`
		} `json:"State"`
	}
	if err := json.Unmarshal(out, &inspect); err != nil {
		return ContainerStatus{}, fmt.Errorf("parse podman inspect output: %w", err)
	}
	if len(inspect) == 0 {
		return ContainerStatus{}, nil
	}
	return ContainerStatus{
		Exists:  true,
		Running: inspect[0].State.Running,
		Status:  inspect[0].State.Status,
	}, nil
}

func (p *PodmanRuntime) Logs(containerID string, follow bool) (io.ReadCloser, error) {
	args := []string{"logs"}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, containerID)
	cmd := exec.Command(p.bin, p.global(args...)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &commandReadCloser{ReadCloser: stdout, wait: cmd.Wait, stderr: &stderr}, nil
}

func (p *PodmanRuntime) Exec(containerID string, command []string) ([]byte, error) {
	args := append([]string{"exec", containerID}, command...)
	out, err := p.run(p.global(args...)...)
	if err != nil {
		return nil, fmt.Errorf("podman exec %s: %w: %s", containerID, err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func (p *PodmanRuntime) Pull(image string) error {
	if err := p.EnsureReady(); err != nil {
		return err
	}
	out, err := p.run(p.global("pull", "-q", image)...)
	if err != nil {
		return fmt.Errorf("podman pull %s: %w: %s", image, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// NewContainerRuntime selects the host's container backend.
//
// COG_CONTAINER_RUNTIME=podman|nerdctl|<path> forces a backend. Otherwise
// podman wins when installed, and nerdctl (the ADR-065 phase-1 backend) is the
// fallback.
func NewContainerRuntime() (ContainerRuntime, error) {
	choice := strings.TrimSpace(os.Getenv(envContainerRuntime))
	switch {
	case choice == "":
		if p, err := NewPodmanRuntime(); err == nil {
			return p, nil
		}
		if n, err := NewNerdctlRuntime(); err == nil {
			return n, nil
		}
		return nil, fmt.Errorf("no container runtime found: install podman (or set %s)", envContainerRuntime)
	case choice == "podman" || strings.HasSuffix(choice, "/podman"):
		if choice != "podman" {
			return newPodmanRuntimeWithBin(choice, os.Getenv(envPodmanMachine), runtime.GOOS != "linux"), nil
		}
		return NewPodmanRuntime()
	case choice == "nerdctl" || strings.HasSuffix(choice, "/nerdctl"):
		if choice != "nerdctl" {
			return &NerdctlRuntime{bin: choice}, nil
		}
		return NewNerdctlRuntime()
	default:
		return nil, fmt.Errorf("%s=%q: want podman, nerdctl, or a path to one", envContainerRuntime, choice)
	}
}

// findBinary resolves env override → PATH → known install locations.
func findBinary(envVar, name string, fallbacks []string) (string, error) {
	if bin := os.Getenv(envVar); bin != "" {
		return bin, nil
	}
	if bin, err := exec.LookPath(name); err == nil {
		return bin, nil
	}
	for _, f := range fallbacks {
		if st, err := os.Stat(f); err == nil && !st.IsDir() {
			return f, nil
		}
	}
	return "", fmt.Errorf("%s not on PATH or in %v", name, fallbacks)
}

func isNoSuchContainer(out []byte) bool {
	text := strings.ToLower(string(out))
	return strings.Contains(text, "no such container") ||
		strings.Contains(text, "no such object") ||
		strings.Contains(text, "not found")
}

// lastLine returns the final non-empty line: `podman run -d` may print pull
// progress before the container id.
func lastLine(out []byte) string {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
