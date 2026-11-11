//go:build !windows

package engine

import (
	"os/exec"
	"syscall"
)

// bindHelperLifetime puts the helper in its own process group and makes the
// context cancel kill the whole group, like Hermes (start_new_session=True,
// group kill). Without it a grandchild that inherits the stdout pipe keeps
// cmd.Run blocked long after the timeout.
func bindHelperLifetime(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
