package engine

import (
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

// recorder fakes the podman binary: it records argv and replies by verb.
type recorder struct {
	calls   [][]string
	replies map[string]reply // keyed by the first non-global arg(s), e.g. "machine inspect"
}

type reply struct {
	out string
	err error
}

func (r *recorder) run(args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string(nil), args...))
	a := args
	if len(a) >= 2 && a[0] == "--connection" {
		a = a[2:]
	}
	for _, n := range []int{2, 1} {
		if len(a) >= n {
			if rep, ok := r.replies[strings.Join(a[:n], " ")]; ok {
				return []byte(rep.out), rep.err
			}
		}
	}
	return nil, nil
}

func fakePodman(machine string, needsMachine bool, replies map[string]reply) (*PodmanRuntime, *recorder) {
	rec := &recorder{replies: replies}
	p := newPodmanRuntimeWithBin("/fake/podman", machine, needsMachine)
	p.run = rec.run
	return p, rec
}

func TestPodmanStartBuildsArgvWithConnectionAndSortedEnv(t *testing.T) {
	p, rec := fakePodman("cogos-node", true, map[string]reply{
		"machine inspect": {out: `[{"Name":"cogos-node","State":"running"}]`},
		"run":             {out: "Trying to pull...\nabc123\n"},
	})
	id, err := p.Start("ghcr.io/myrgic/cogos:dev", ContainerConfig{
		Name: "cog-ws", WorkspaceRoot: "/ws", Port: 6931,
		RestartPolicy: "unless-stopped",
		Env:           map[string]string{"B": "2", "A": "1"},
		Command:       []string{"serve"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "abc123" {
		t.Fatalf("id = %q, want last line of output", id)
	}
	want := []string{"--connection", "cogos-node", "run", "-d", "--replace", "--name", "cog-ws",
		"--restart", "unless-stopped", "-p", "6931:6931", "-v", "/ws:/ws", "-w", "/ws",
		"-e", "A=1", "-e", "B=2", "ghcr.io/myrgic/cogos:dev", "serve"}
	got := rec.calls[len(rec.calls)-1]
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv\n got %q\nwant %q", got, want)
	}
	// machine inspect must NOT carry --connection (it is not a remote command)
	if rec.calls[0][0] != "machine" {
		t.Fatalf("first call should be machine inspect, got %q", rec.calls[0])
	}
}

func TestPodmanEnsureReadyStartsStoppedMachine(t *testing.T) {
	p, rec := fakePodman("cogos-node", true, map[string]reply{
		"machine inspect": {out: `[{"Name":"cogos-node","State":"stopped"}]`},
	})
	if err := p.EnsureReady(); err != nil {
		t.Fatal(err)
	}
	if got := rec.calls[1]; !reflect.DeepEqual(got, []string{"machine", "start", "cogos-node"}) {
		t.Fatalf("expected machine start, got %q", got)
	}
}

func TestPodmanEnsureReadyToleratesAlreadyRunningRace(t *testing.T) {
	p, _ := fakePodman("", true, map[string]reply{
		"machine inspect": {out: `[{"Name":"podman-machine-default","State":"stopped"}]`},
		"machine start":   {out: "Error: machine already running", err: errors.New("exit 125")},
	})
	if err := p.EnsureReady(); err != nil {
		t.Fatalf("already-running race should be ready, got %v", err)
	}
}

func TestPodmanLinuxNeedsNoMachine(t *testing.T) {
	p, rec := fakePodman("", false, nil)
	if err := p.EnsureReady(); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("linux EnsureReady must not shell out, got %q", rec.calls)
	}
}

func TestPodmanStatusMissingContainerIsNotAnError(t *testing.T) {
	p, _ := fakePodman("", false, map[string]reply{
		"container inspect": {out: "Error: no such container nope", err: errors.New("exit 125")},
	})
	st, err := p.Status("nope")
	if err != nil || st.Exists {
		t.Fatalf("got %+v, %v; want not-exists, nil", st, err)
	}
}

func TestPodmanStatusParsesInspect(t *testing.T) {
	p, _ := fakePodman("", false, map[string]reply{
		"container inspect": {out: `[{"State":{"Running":true,"Status":"running"}}]`},
	})
	st, err := p.Status("x")
	if err != nil || !st.Exists || !st.Running || st.Status != "running" {
		t.Fatalf("got %+v, %v", st, err)
	}
}

func TestNewContainerRuntimeSelection(t *testing.T) {
	t.Setenv(envContainerRuntime, "/some/where/podman")
	rt, err := NewContainerRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := rt.(*PodmanRuntime); !ok || p.Bin() != "/some/where/podman" {
		t.Fatalf("want PodmanRuntime at path, got %T %+v", rt, rt)
	}
	t.Setenv(envContainerRuntime, "/some/where/nerdctl")
	if rt, err = NewContainerRuntime(); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.(*NerdctlRuntime); !ok {
		t.Fatalf("want NerdctlRuntime, got %T", rt)
	}
	t.Setenv(envContainerRuntime, "docker")
	if _, err = NewContainerRuntime(); err == nil {
		t.Fatal("docker must be rejected: no daemon-backed runtime")
	}
}

// TestPodmanRealBinary drives the installed podman end to end. It runs only
// when COG_PODMAN_IT=1, because on macOS it boots a VM.
func TestPodmanRealBinary(t *testing.T) {
	if os.Getenv("COG_PODMAN_IT") != "1" {
		t.Skip("set COG_PODMAN_IT=1 to run against the installed podman")
	}
	if _, err := exec.LookPath("podman"); err != nil {
		t.Skip("podman not installed")
	}
	p, err := NewPodmanRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.EnsureReady(); err != nil {
		t.Fatalf("EnsureReady: %v", err)
	}
	const img = "docker.io/library/alpine:3.20"
	if err := p.Pull(img); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	name := "cogos-podman-it"
	id, err := p.Start(img, ContainerConfig{Name: name, Command: []string{"sleep", "60"},
		Env: map[string]string{"COG_IT": "1"}})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _, _ = p.run(p.global("rm", "-f", name)...) })
	if id == "" {
		t.Fatal("empty container id")
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		st, err := p.Status(name)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if st.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("container never running: %+v", st)
		}
		time.Sleep(300 * time.Millisecond)
	}
	out, err := p.Exec(name, []string{"sh", "-c", "echo $COG_IT"})
	if err != nil || strings.TrimSpace(string(out)) != "1" {
		t.Fatalf("Exec: %q %v", out, err)
	}
	if err := p.Stop(name); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	st, _ := p.Status(name)
	if st.Running {
		t.Fatalf("still running after Stop: %+v", st)
	}
	if st, _ := p.Status("cogos-definitely-absent"); st.Exists {
		t.Fatal("absent container reported as existing")
	}
}
