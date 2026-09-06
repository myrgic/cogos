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
    curl

RUN addgroup -S cogos && adduser -S cogos -G cogos

WORKDIR /workspace

# Copy kernel binary
COPY --from=builder /cog /usr/local/bin/cog

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
