//go:build windows

package engine

import "os/exec"

// bindHelperLifetime is a no-op: secrets.command needs /bin/sh and is POSIX only.
func bindHelperLifetime(cmd *exec.Cmd) {}
