package discord

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeAuth(t *testing.T, root, body string) {
	t.Helper()
	dir := filepath.Join(root, ".cog", "config", "discord")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "auth.yaml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeScript(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestResolveTokenCommandRelativeToRoot(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "")
	root := t.TempDir()
	writeScript(t, root, "scripts/tok", `echo "  from-command  "`)
	writeAuth(t, root, "token: literal\ntoken_command: [scripts/tok, discord/cog]\n")
	got, err := resolveToken(root, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "from-command" {
		t.Fatalf("got %q, want token_command output (trimmed) to win over literal", got)
	}
}

func TestResolveTokenOrderFlagEnvThenAuth(t *testing.T) {
	root := t.TempDir()
	writeScript(t, root, "scripts/tok", `echo from-command`)
	writeAuth(t, root, "token_command: [scripts/tok]\n")
	t.Setenv("DISCORD_BOT_TOKEN", "from-env")
	if got, _ := resolveToken(root, "from-flag"); got != "from-flag" {
		t.Fatalf("flag: got %q", got)
	}
	if got, _ := resolveToken(root, ""); got != "from-env" {
		t.Fatalf("env: got %q", got)
	}
}

func TestResolveTokenLiteralStillWorks(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "")
	root := t.TempDir()
	writeAuth(t, root, "token: literal\n")
	if got, err := resolveToken(root, ""); err != nil || got != "literal" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestResolveTokenCommandFailureDoesNotLeakStdout(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "")
	root := t.TempDir()
	writeScript(t, root, "scripts/tok", `echo SECRETVALUE; echo vault locked >&2; exit 3`)
	writeAuth(t, root, "token_command: [scripts/tok]\n")
	_, err := resolveToken(root, "")
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "SECRETVALUE") {
		t.Fatalf("error leaks stdout: %v", err)
	}
	if !strings.Contains(err.Error(), "vault locked") {
		t.Fatalf("error should carry stderr: %v", err)
	}
}

func TestResolveTokenCommandEmptyAndMalformed(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "")
	for name, body := range map[string]string{"empty": `true`, "multiline": `printf 'a\nb\n'`} {
		root := t.TempDir()
		writeScript(t, root, "scripts/tok", body)
		writeAuth(t, root, "token_command: [scripts/tok]\n")
		if _, err := resolveToken(root, ""); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestProviderFetchLiveResolvesViaWorkspaceRoot(t *testing.T) {
	t.Setenv("DISCORD_BOT_TOKEN", "")
	root := t.TempDir()
	writeScript(t, root, "scripts/tok", `echo from-command`)
	writeAuth(t, root, "token_command: [scripts/tok]\n")
	p := &DiscordProvider{WorkspaceRoot: root}
	if err := p.ensureToken(); err != nil {
		t.Fatal(err)
	}
	if p.Token != "from-command" {
		t.Fatalf("got %q", p.Token)
	}
}
