package testkernel_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/myrgic/cogos/internal/testkernel"
)

// TestBoot_DoesNotTouchRealHomeVault is the guard for the 2026-09-24
// incident: a testkernel.Boot call minted a node-root identity grant into
// the operator's REAL ~/.cog/vault/node-root-grant, desyncing it from the
// live kernel and 401'ing every local kernel write until restart. Boot now
// isolates HOME (see testkernel.go's Boot doc comment); this test fails if
// that isolation ever regresses, by snapshotting the real vault file (under
// the real, pre-override HOME) before a Boot+Stop cycle and asserting it is
// byte-for-byte and mtime-identical afterward.
func TestBoot_DoesNotTouchRealHomeVault(t *testing.T) {
	realHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("os.UserHomeDir: %v", err)
	}
	vaultPath := filepath.Join(realHome, ".cog", "vault", "node-root-grant")

	before, beforeErr := snapshotVaultFile(vaultPath)
	if beforeErr != nil && !os.IsNotExist(beforeErr) {
		t.Fatalf("stat real vault file before Boot: %v", beforeErr)
	}

	// Run the actual Boot+Stop cycle in a subtest so its t.Setenv-scoped HOME
	// override (testkernel.Boot) is restored to the real HOME the instant
	// this subtest ends, before we re-snapshot below.
	t.Run("boot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		k, err := testkernel.Boot(ctx, t)
		if err != nil {
			t.Fatalf("testkernel.Boot: %v", err)
		}
		if err := k.Stop(); err != nil {
			t.Errorf("testkernel.Stop: %v", err)
		}
	})

	after, afterErr := snapshotVaultFile(vaultPath)
	if afterErr != nil && !os.IsNotExist(afterErr) {
		t.Fatalf("stat real vault file after Boot: %v", afterErr)
	}

	realVaultExistedBefore := beforeErr == nil
	realVaultExistsAfter := afterErr == nil

	if realVaultExistedBefore != realVaultExistsAfter {
		t.Fatalf("guard: real node-root vault %s existence changed across a test Boot (existed before=%v, exists after=%v) — HOME isolation in testkernel.Boot has regressed",
			vaultPath, realVaultExistedBefore, realVaultExistsAfter)
	}
	if realVaultExistedBefore && before != after {
		t.Fatalf("guard: real node-root vault %s changed during a test Boot (mtime/hash differ: before=%s after=%s) — a test-booted kernel wrote to the operator's real vault instead of an isolated HOME",
			vaultPath, before, after)
	}
}

// snapshotVaultFile returns a string combining the file's mtime and content
// hash, for cheap before/after comparison. Returns an error (possibly
// os.IsNotExist) if the file cannot be read.
func snapshotVaultFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("mtime=%d sha256=%s", info.ModTime().UnixNano(), hex.EncodeToString(h.Sum(nil))), nil
}
