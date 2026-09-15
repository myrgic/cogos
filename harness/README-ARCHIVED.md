# ARCHIVED: harness/ is unwired dark code

Status as of 2026-09-15: `harness/` is a standalone Go module (`go.mod`
declares `github.com/myrgic/cogos/harness`) that nothing in `cmd/` or
`internal/` imports. It is not built into the `cogos` daemon binary and does
not run in production.

Verified via:

```
grep -rn 'myrgic/cogos/harness' --include=*.go . | grep -v '^./harness/'
```

which returns no results — no package outside `harness/` references it.

Last commit that touched this directory: `48ed72f` (2026-09-15,
"fix(codex): drop removed --full-auto, discover default model + catalog
from ~/.codex", #634). Even that fix was applied in place without any
caller being wired up — it kept the module compiling but did not connect it
to anything.

## Why it's kept

`harness/` remains listed in the root `go.work` file (this repo confirmed
`go build ./...` and `go vet ./...` both pass with or without it in
`go.work` — removing it from `go.work` is clean, but it is left in place
here to avoid an unrelated churn to the workspace file; see the PR that
added this note for the disclosure). The directory is kept for reference —
it contains a previous agent-harness design (Claude/Codex process drivers,
tool classification, retry/otel wiring) that may inform future work — but it
should be treated as **dark code**: don't assume it reflects current
defaults, model catalogs, or provider behavior, and don't wire new code to
it without first confirming its assumptions against `internal/engine/` and
`sdk/`, which are the live, imported implementations.

If you are looking for the code that actually resolves models and dispatches
inference in the running daemon, see `internal/engine/` (providers) and
`internal/engine/resolve.go` (model resolution), not this directory.
