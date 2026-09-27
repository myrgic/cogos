// registry.go
// Provider registry for the reconciliation framework.
// Maps resource type names to Reconcilable implementations.
//
// Usage:
//
//	RegisterProvider("discord", &DiscordProvider{})
//	provider, err := GetProvider("discord")

package reconcile

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var (
	providersMu sync.RWMutex
	providers   = make(map[string]Reconcilable)
)

// ValidateInstanceName reports whether name is safe to use as a registry key.
//
// This is a filesystem-safety gate, not a style rule: StatePath joins the
// registry key into `<root>/.cog/config/<name>/.state.json`, so the key is a
// path component. An unvalidated key with an absolute path in it materialises
// a nested directory tree inside the workspace config dir, and a key
// containing ".." escapes the workspace entirely.
//
// This is not hypothetical. WorktreeReconciler.Type() returned
// "worktree-reconciler:" + repoRoot, which produced a real
// `.cog/config/worktree-reconciler:/Users/.../cog/.state.json` tree in a live
// workspace. Validating at the registry boundary closes the class rather than
// the instance, and does it before any operator- or peer-supplied name can
// reach a path join.
//
// A key is one or two segments separated by "/" (the type, and an optional
// instance discriminator — e.g. "lms-model-state/lmstudio-remote"). Each
// segment must be a portable filename: non-empty, not "." or "..", and free
// of path separators, ':' and other characters that are illegal on Windows.
// ':' is rejected even though it is legal on Unix because this substrate runs
// on Windows nodes too, and a state path that cannot be created on one node is
// a portability defect wherever it is minted.
func ValidateInstanceName(name string) error {
	if name == "" {
		return fmt.Errorf("reconcile: provider name is empty")
	}
	if filepath.IsAbs(name) || strings.HasPrefix(name, "/") || strings.HasPrefix(name, `\`) {
		return fmt.Errorf("reconcile: provider name %q is an absolute path", name)
	}
	segs := strings.Split(name, "/")
	if len(segs) > 2 {
		return fmt.Errorf("reconcile: provider name %q has %d segments, want 1 or 2 (type[/instance])", name, len(segs))
	}
	for _, seg := range segs {
		switch seg {
		case "":
			return fmt.Errorf("reconcile: provider name %q has an empty segment", name)
		case ".", "..":
			return fmt.Errorf("reconcile: provider name %q contains a %q segment", name, seg)
		}
		if strings.ContainsAny(seg, `\:*?"<>|`) {
			return fmt.Errorf("reconcile: provider name %q segment %q contains a character that is not portable in a filename", name, seg)
		}
		if strings.ContainsRune(seg, 0) {
			return fmt.Errorf("reconcile: provider name %q segment %q contains a NUL byte", name, seg)
		}
	}
	// Belt and braces: whatever the segment rules let through must still be a
	// path that stays inside its parent.
	if !filepath.IsLocal(filepath.FromSlash(name)) {
		return fmt.Errorf("reconcile: provider name %q does not resolve to a location inside the config directory", name)
	}
	return nil
}

// RegisterProvider adds a reconciliation provider to the global registry.
// Panics if a provider with the same name is already registered, or if the
// name is not a valid registry key. Both are programming errors on the
// boot-time registration path, and panicking there surfaces them in
// development rather than materialising a malformed state path in a live
// workspace.
func RegisterProvider(name string, provider Reconcilable) {
	if err := ValidateInstanceName(name); err != nil {
		panic(err.Error())
	}
	providersMu.Lock()
	defer providersMu.Unlock()
	if _, exists := providers[name]; exists {
		panic(fmt.Sprintf("reconcile: provider %q already registered", name))
	}
	providers[name] = provider
}

// GetProvider returns the provider for the given resource type.
func GetProvider(name string) (Reconcilable, error) {
	providersMu.RLock()
	defer providersMu.RUnlock()
	p, ok := providers[name]
	if !ok {
		// Build the name list inline rather than calling ListProviders(): that
		// would take a second RLock, and sync.RWMutex is not reentrant — once a
		// writer (config-reload RegisterProvider/UpsertProvider) is blocked
		// waiting, Go's RWMutex stops admitting new readers, so the re-RLock
		// would deadlock this goroutine permanently.
		names := make([]string, 0, len(providers))
		for n := range providers {
			names = append(names, n)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("unknown resource type: %s (registered: %v)", name, names)
	}
	return p, nil
}

// ListProviders returns sorted names of all registered providers.
func ListProviders() []string {
	providersMu.RLock()
	defer providersMu.RUnlock()
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// HasProvider returns true if a provider is registered for the given name.
func HasProvider(name string) bool {
	providersMu.RLock()
	defer providersMu.RUnlock()
	_, ok := providers[name]
	return ok
}

// UpsertProvider adds or replaces a reconciliation provider in the global
// registry. Unlike RegisterProvider, this does not panic on duplicate names —
// the new provider replaces the existing one. Used by BuildRouter to register
// MLXSupervisedProvider instances without conflicting with daemon-side stubs.
// An invalid name is refused here rather than panicking, because unlike
// RegisterProvider this path runs at runtime with names partly derived from
// operator config (e.g. "lms-model-state/<provider-name>" in providers.yaml).
// A malformed entry in a config file should cost that one provider, loudly,
// not the whole daemon.
func UpsertProvider(name string, provider Reconcilable) {
	if err := ValidateInstanceName(name); err != nil {
		slog.Error("reconcile: refusing to register provider with unsafe name",
			"name", name, "err", err)
		return
	}
	providersMu.Lock()
	defer providersMu.Unlock()
	providers[name] = provider
}

// UnregisterProvider removes a provider from the global registry, returning
// true if one was present. It is the missing half of UpsertProvider: without
// it a provider whose declaration or config entry has been removed keeps being
// swept by the reconcile daemon — and keeps ACTUATING against a target its
// operator has already retracted — until the kernel restarts.
func UnregisterProvider(name string) bool {
	providersMu.Lock()
	defer providersMu.Unlock()
	if _, ok := providers[name]; !ok {
		return false
	}
	delete(providers, name)
	return true
}

// ResetProviders clears the registry (for testing only).
func ResetProviders() {
	providersMu.Lock()
	defer providersMu.Unlock()
	providers = make(map[string]Reconcilable)
}
