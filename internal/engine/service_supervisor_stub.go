//go:build !darwin

// service_supervisor_stub.go — no-op stub for non-darwin platforms.
//
// On non-macOS platforms there is no launchctl(1). LaunchctlController is
// present as a type alias for ObserverSupervisor so the rest of the package
// compiles cleanly; every mutation returns ErrNotControllable.
//
// Phase 3 (#101) will add a SystemdSupervisor for Linux and a DirectSupervisor
// for dev-mode process management. Until then, non-darwin platforms are in
// observer-only mode for all services.
package engine

import (
	"os"
	"path/filepath"
)

// LaunchctlController is a no-op alias for ObserverSupervisor on non-darwin
// platforms. It compiles cleanly but returns ErrNotControllable for all
// mutations.
type LaunchctlController = ObserverSupervisor

// NewLaunchctlController returns an ObserverSupervisor on non-darwin platforms.
func NewLaunchctlController() *LaunchctlController {
	return &ObserverSupervisor{}
}

// homeDir returns the current user's home directory on non-darwin platforms.
// MLXSupervisedProvider uses this to build the plist path; on Linux/other
// platforms the plist path is meaningless (launchd is macOS-only) but the
// provider must still construct cleanly so tests and cross-platform builds work.
func homeDir() (string, error) {
	return os.UserHomeDir()
}

// plistPathForLabel mirrors the darwin helper so platform-neutral callers
// (restartPreflight) compile everywhere. Off darwin the observer supervisor
// short-circuits restartPreflight before this path is ever consulted.
func plistPathForLabel(label string) string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents", label+".plist")
}
