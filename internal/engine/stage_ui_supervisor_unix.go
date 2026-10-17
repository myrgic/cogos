//go:build unix

// stage_ui_supervisor_unix.go — Unix process helpers for the Stage UI
// supervisor.
//
// The supervisor starts the upstream in its own SESSION (Setsid), not merely
// its own process group: the kernel is not the Stage's parent in any sense the
// Stage can observe, so a kernel restart, a terminal hang-up or a signal to
// the kernel's process group never reaches the Stage. That is what makes the
// handover from launchd (or from an operator's shell) a no-op: whoever was
// keeping the Stage up can stop, because the kernel now is.

package engine

import (
	"errors"
	"os/exec"
	"syscall"
)

// supervisorDetach puts the child in a new session, detached from the kernel's
// controlling terminal and process group.
func supervisorDetach(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setsid = true
}

// processAlive reports whether pid names a live process. Signal 0 performs the
// permission and existence checks without delivering anything; EPERM means the
// process exists but belongs to someone else, which still counts as alive.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// terminateProcess asks pid to exit (SIGTERM).
func terminateProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	return syscall.Kill(pid, syscall.SIGTERM)
}

// killProcess ends pid unconditionally (SIGKILL).
func killProcess(pid int) error {
	if pid <= 0 {
		return nil
	}
	return syscall.Kill(pid, syscall.SIGKILL)
}
