# CogOS Kernel Build System
# github.com/myrgic/cogos
#
# Multi-platform binaries can be built for distribution.
#
# Usage:
#   make          - Build for current platform (creates 'cog' binary)
#   make all      - Build for all platforms (cog-{os}-{arch})
#   make test     - Run tests
#   make clean    - Remove build artifacts
#   make install  - Install to ~/.cog/bin/cogos
#   make image    - Build production OCI image
#   make e2e      - Run e2e test in a container
#   make e2e-local - Run e2e test locally

# VERSION is derived from git so a local build never claims to be a release it
# is not. `git describe --tags --always --dirty` yields e.g.
# v0.16.22-2-g127b651-dirty: descended from v0.16.22, two commits past it, with
# uncommitted changes.
# Overridable (?=) so packagers and any caller that builds through make can pin
# an exact value. Note the release workflow does NOT go through make — it calls
# `go build` with its own ldflags (release.yml:95) — so releases are unaffected
# by this default either way.
#
# The `dev-` prefix is load-bearing, not cosmetic — DO NOT REMOVE IT. A bare
# `git describe` string like `v0.16.22-6-g4f0acf1-dirty` is VALID semver whose
# suffix is a PRERELEASE, and prereleases sort BEFORE their base tag under
# golang.org/x/mod/semver — see internal/providers/selfupdate/version_test.go:35-36.
# So a bare describe string reads as OLDER than the release it descends from:
# GATE D (provider.go:272) does not fire because the version parses, and GATE F
# (provider.go:282) does not fire because
# versionAfter("v0.16.22", "v0.16.22-6-g4f0acf1-dirty") is TRUE.
# Self-update would then overwrite the dev binary with the plain release — the
# exact clobber this PR exists to prevent. Prefixing with `dev-` makes
# normVersion() return "" so GATE D fires and self-update stays inert, while the
# full describe string is retained for traceability. This is pinned by
# internal/providers/selfupdate/devbuild_version_test.go.
# Falls back to the honest "dev" (matching internal/engine/cli.go:32's default)
# when git is unavailable, e.g. a source tarball with no .git.
VERSION ?= $(shell d=$$(git describe --tags --always --dirty 2>/dev/null) && echo "dev-$$d" || echo dev)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
# BUILD_TAGS is defined BEFORE LDFLAGS on purpose: LDFLAGS injects it into the
# binary (engine.BuildTags) so `/health` can report what the build claimed it
# compiled in. Make's := is immediate-expansion, so the order is load-bearing —
# defining BUILD_TAGS after LDFLAGS would inject an empty string.
# The injected value is a CLAIM only. /health pairs it with a runtime probe
# (CREATE VIRTUAL TABLE ... USING fts5) and reports build_tags.fts5 from the
# probe, so an ldflags/module mismatch surfaces instead of being believed.
# See internal/engine/build_tags.go and ledger row L01.
BUILD_TAGS := fts5
LDFLAGS := -s -w -X github.com/myrgic/cogos/internal/engine.BuildTime=$(BUILD_TIME) -X github.com/myrgic/cogos/internal/engine.Version=$(VERSION) -X github.com/myrgic/cogos/internal/engine.BuildTags=$(BUILD_TAGS)
BINARY := cog
GO := go

IMAGE      := ghcr.io/myrgic/cogos
TAG        := dev
PORT       := 6931
WORKSPACE  ?= $(shell git rev-parse --show-toplevel 2>/dev/null || echo $$HOME/cog-workspace)

# Detect current platform
GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

# Build targets
PLATFORMS := darwin-arm64 darwin-amd64 linux-amd64 linux-arm64 android-arm64 windows-amd64 windows-arm64

.PHONY: all build clean test test-coverage test-integration bench install check-not-running test-install-guard test-prepush-hook image run e2e e2e-local $(PLATFORMS) $(BINARY)

# Default: build for current platform
build: $(BINARY)

$(BINARY):
	$(GO) build -tags "$(BUILD_TAGS)" -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/cogos

# Build for all platforms
all: $(PLATFORMS)

darwin-arm64:
	GOOS=darwin GOARCH=arm64 $(GO) build -tags "$(BUILD_TAGS)" -ldflags="$(LDFLAGS)" -o $(BINARY)-darwin-arm64 ./cmd/cogos

darwin-amd64:
	GOOS=darwin GOARCH=amd64 $(GO) build -tags "$(BUILD_TAGS)" -ldflags="$(LDFLAGS)" -o $(BINARY)-darwin-amd64 ./cmd/cogos

linux-amd64:
	GOOS=linux GOARCH=amd64 $(GO) build -tags "$(BUILD_TAGS)" -ldflags="$(LDFLAGS)" -o $(BINARY)-linux-amd64 ./cmd/cogos

linux-arm64:
	GOOS=linux GOARCH=arm64 $(GO) build -tags "$(BUILD_TAGS)" -ldflags="$(LDFLAGS)" -o $(BINARY)-linux-arm64 ./cmd/cogos

# Android requires PIE (position-independent executables)
android-arm64:
	GOOS=android GOARCH=arm64 $(GO) build -tags "$(BUILD_TAGS)" -buildmode=pie -ldflags="$(LDFLAGS)" -o $(BINARY)-android-arm64 ./cmd/cogos

# Windows cross-compile: matches .github/workflows/release.yml exactly.
# CGO_ENABLED=0 avoids tree-sitter's CGO path (same reason as the default
# build target). .exe suffix is required to execute on Windows.
windows-amd64:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -ldflags="$(LDFLAGS)" -o $(BINARY)-windows-amd64.exe ./cmd/cogos/

windows-arm64:
	CGO_ENABLED=0 GOOS=windows GOARCH=arm64 $(GO) build -ldflags="$(LDFLAGS)" -o $(BINARY)-windows-arm64.exe ./cmd/cogos/

INSTALL_DIR := $(HOME)/.cog/bin
INSTALL_TARGET := $(INSTALL_DIR)/cogos

# Refuse to clobber a binary a live cogos daemon is executing. Delegates to
# the ONE shared implementation at scripts/lib/refuse-if-running.sh -- see
# that file's header for why this is not (again) a hand-copied check.
# Set ALLOW_RUNNING_INSTALL=1 to override.
check-not-running:
	@scripts/lib/refuse-if-running.sh "$(INSTALL_TARGET)"

# Functional regression suite for the guard above. Also wired into CI's lint
# job; this target is the local entry point so the suite is discoverable
# without reading ci.yml.
test-install-guard:
	@scripts/test-refuse-if-running.sh

# Install to ~/.cog/bin/cogos (atomic: build, verify, checksum, move).
# check-not-running runs first so the backup/cp/mv below never fire against
# a binary a running kernel is executing.
install: build check-not-running
	@echo "=== Installing to $(INSTALL_TARGET) ==="
	@./$(BINARY) version > /dev/null 2>&1 || (echo "ERROR: built binary fails version check" && exit 1)
	@mkdir -p "$(INSTALL_DIR)"
	@if [ -f "$(INSTALL_TARGET)" ]; then \
		cp "$(INSTALL_TARGET)" "$(INSTALL_TARGET).bak"; \
		echo "  Backed up existing binary to $(INSTALL_TARGET).bak"; \
	fi
	@cp $(BINARY) "$(INSTALL_TARGET).tmp"
	@chmod +x "$(INSTALL_TARGET).tmp"
	@mv "$(INSTALL_TARGET).tmp" "$(INSTALL_TARGET)"
	@NEW_SHA=$$(shasum -a 256 "$(INSTALL_TARGET)" | cut -d' ' -f1); \
		echo "  Installed cogos $(VERSION) ($(GOOS)/$(GOARCH))"; \
		echo "  SHA-256: $$NEW_SHA"

# Run tests
test: build
	@echo "=== Unit Tests ==="
	$(GO) test -tags "$(BUILD_TAGS)" -count=1 ./...
	@echo ""
	@echo "=== Smoke Tests ==="
	@echo "=== Version Test ==="
	./$(BINARY) version
	@echo ""
	@echo "=== Health Check ==="
	./$(BINARY) health || true
	@echo ""
	@echo "=== All tests passed ==="

test-coverage:
	$(GO) test -tags "$(BUILD_TAGS)" -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

test-integration:
	$(GO) test -tags "integration,$(BUILD_TAGS)" -race -count=1 -timeout 30s ./...

bench: build
	./$(BINARY) bench --workspace $(WORKSPACE) --no-inference

# Docker targets
image:
	docker build \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(IMAGE):$(TAG) \
		.

run: image
	docker run --rm \
		-v $(WORKSPACE):$(WORKSPACE) \
		-p $(PORT):$(PORT) \
		$(IMAGE):$(TAG) \
		serve --workspace $(WORKSPACE) --port $(PORT)

e2e:
	docker build -f Dockerfile.e2e -t cogos-e2e-test .
	docker run --rm cogos-e2e-test

e2e-local: build
	COGOS_BIN=./$(BINARY) ./scripts/e2e-test.sh

# Compare with Python version
compare: build
	@echo "=== Go Version ==="
	./$(BINARY) coherence check
	@echo ""
	@echo "=== Python Version ==="
	python3 hooks/coherence.py check || echo "(Python coherence not available)"

# Clean build artifacts
clean:
	rm -f $(BINARY) $(BINARY)-*
	rm -f *.tmp.*
	go clean ./...

# Development helpers
fmt:
	gofmt -s -w *.go

vet:
	$(GO) vet ./...

tidy:
	go mod tidy

# Install the tracked git hooks by pointing core.hooksPath at .githooks/.
#
# Deliberately NOT a copy into .git/hooks: a copied hook drifts from its source
# the moment either changes, and .git/hooks is unversioned so the drift is
# invisible. Pointing at the tracked directory means there is exactly one
# implementation and `git pull` updates the guard.
#
# Until this existed cogos had no real git hooks at all, while carrying the
# heavier CI suite — a shell-hardening violation could only be discovered from
# a red PR. The hook runs the same scripts CI runs; see .githooks/pre-push.
.PHONY: hooks hooks-verify
hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/*
	@echo "hooks: core.hooksPath -> .githooks"
	@echo "  pre-push: shell hardening + doctor (gate: fail)"

# Report whether the hooks are actually active. A guard that is merely present
# is not a guard that runs.
hooks-verify:
	@if [ "$$(git config core.hooksPath)" = ".githooks" ]; then \
		echo "hooks: ACTIVE (core.hooksPath=.githooks)"; \
		test -x .githooks/pre-push && echo "  pre-push: executable" || { echo "  pre-push: NOT EXECUTABLE — run 'make hooks'" >&2; exit 1; }; \
	else \
		echo "hooks: NOT INSTALLED (core.hooksPath='$$(git config core.hooksPath)') — run 'make hooks'" >&2; \
		exit 1; \
	fi

# Negative control for the pre-push hook: proves it actually REFUSES a real
# shell-hardening violation, rather than merely being installed. Same role
# test-install-guard plays for refuse-if-running.sh, and wired the same two
# ways (this target + a CI step) for the same reason: a guard whose refusal
# path nothing executes regresses silently. Depends on `hooks` because the
# control exits 2 rather than reporting a meaningless pass when the hook is
# not installed.
test-prepush-hook: hooks
	@sh scripts/test-prepush-hook.sh

lint: vet
	@echo "=== Checking for bare exec.Command ==="
	@if grep -n 'exec\.Command(' *.go | grep -v '_test\.go' | grep -v 'CommandContext' | grep -v '// bare-ok' > /dev/null 2>&1; then \
		echo "ERROR: bare exec.Command found (use CommandContext with timeout):"; \
		grep -n 'exec\.Command(' *.go | grep -v '_test\.go' | grep -v 'CommandContext' | grep -v '// bare-ok'; \
		exit 1; \
	else \
		echo "  All exec.Command calls use CommandContext"; \
	fi
	@if grep -rn 'exec\.Command(' sdk/ | grep -v '_test\.go' | grep -v 'CommandContext' | grep -v '// bare-ok' > /dev/null 2>&1; then \
		echo "ERROR: bare exec.Command found in sdk/:"; \
		grep -rn 'exec\.Command(' sdk/ | grep -v '_test\.go' | grep -v 'CommandContext' | grep -v '// bare-ok'; \
		exit 1; \
	else \
		echo "  SDK: All exec.Command calls use CommandContext"; \
	fi

# Show binary info
info: build
	@echo "Binary: $(BINARY)"
	@echo "Size: $(shell ls -lh $(BINARY) | awk '{print $$5}')"
	@echo "Version: $(VERSION)"
	@echo "Build: $(BUILD_TIME)"
	@file $(BINARY)
