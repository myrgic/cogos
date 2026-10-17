//go:build windows

// stage_ui_supervisor_windows.go — Windows fallbacks for the Stage UI
// supervisor.
//
// Windows has neither sessions nor SIGTERM. There is no clean way to make a
// child survive its parent's death without a Job Object with
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE deliberately omitted, so the supervisor's
// survival guarantee is Unix-only for now; the direct child is still started,
// tracked and restarted, and the graceful/forceful taskkill pair below mirrors
// daemon_stop_windows.go.

package engine

import (
	"os"
	"os/exec"
	"strconv"
)

// supervisorDetach is a no-op on Windows: a detached session needs
// CreateProcess with DETACHED_PROCESS, which the supervisor does not yet set
// up. The child is still started and reaped normally.
func supervisorDetach(cmd *exec.Cmd) {}

// processAlive reports whether pid names a live process, via tasklist (see
// windowsPIDAlive). If tasklist itself fails it errs towards "alive".
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return windowsPIDAlive(pid)
}

// terminateProcess asks pid to close (taskkill without /F).
func terminateProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	return exec.Command("taskkill", "/PID", strconv.Itoa(pid)).Run()
}

// killProcess ends pid forcefully (taskkill /F), falling back to the process
// handle if taskkill is unavailable.
func killProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := exec.Command("taskkill", "/F", "/PID", strconv.Itoa(pid)).Run(); err == nil {
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Kill()
}
