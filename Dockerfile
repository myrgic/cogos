# CogOS Kernel — Multi-stage OCI build
#
# Build:
#   docker build -t ghcr.io/myrgic/cogos:dev .
#
# Run:
#   docker run -v /path/to/workspace:/workspace \
#              -p 6931:6931 ghcr.io/myrgic/cogos:dev \
#              serve --workspace /workspace --port 6931
#
# Multi-platform:
#   docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/myrgic/cogos:dev .

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

ARG BUILD_TIME=unknown

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /build

# Copy module files first for layer caching
COPY go.mod go.sum ./
COPY sdk/go.mod sdk/go.sum ./sdk/
COPY harness/go.mod harness/go.sum ./harness/
COPY envspec/go.mod ./envspec/

# Download dependencies
RUN go mod download

# Copy source
COPY . .

# Build with CGO for SQLite FTS5 support
RUN CGO_ENABLED=1 go build \
    -tags "fts5" \
    -ldflags="-s -w -X github.com/myrgic/cogos/internal/engine.BuildTime=${BUILD_TIME} -X github.com/myrgic/cogos/internal/engine.BuildTags=fts5" \
    -o /cog ./cmd/cogos

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache \
    ca-certificates \
    sqlite-libs \
    git \
    curl \
    python3

RUN addgroup -S cogos && adduser -S cogos -G cogos

WORKDIR /workspace

# Copy kernel binary
COPY --from=builder /cog /usr/local/bin/cog

# Release gate: internal/providers/site.gateArtifact() shells out to
# scripts/cogpublic-guard.py before any GH Pages deploy force-pushes a built
# artifact to a public repo. The gate is fail-closed by design (a check that
# cannot run must never look like a check that passed), so this image must
# actually carry the guard, its policy, and python3 — not just the binary —
# or every site deploy done from this image fails closed permanently.
#
# Installed at the fixed path compiledInGuardRoot names in site.go
# (internal/providers/site/site.go), and COGOS_REPO_ROOT is set to match so
# repoRootForGuard() resolves it without a directory walk. The compiled-in
# constant is a fallback for the (unsupported) case this ENV is stripped at
# `docker run` time; keep the two paths identical.
COPY scripts/cogpublic-guard.py /opt/cogos-release-gate/scripts/cogpublic-guard.py
COPY .cogpublic /opt/cogos-release-gate/.cogpublic
ENV COGOS_REPO_ROOT=/opt/cogos-release-gate

# Create workspace structure
RUN mkdir -p .cog/mem .cog/config .cog/run .cog/logs .cog/ledger \
    && chown -R cogos:cogos /workspace

USER cogos

# Kernel API port
EXPOSE 6931

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:6931/health || exit 1

ENTRYPOINT ["cog"]
CMD ["serve", "--port", "6931"]
