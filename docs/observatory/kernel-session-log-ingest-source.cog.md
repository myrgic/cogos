---
created: '2026-10-17'
type: spec
id: observatory-ingest-source-kernel-session-log
title: "Observatory ingest source — kernel-session-log (kernel-authored, live)"
status: draft
authority: authoritative
durability: archival
retention: append-only
tags:
- observatory
- ingest
- kernel
- acp
- session-log
description: >
  The kernel is the ACP client for every managed session, so it journals each
  relayed event as it drives. This file is the manifest fragment to fold into
  the operator's .cog/observatory/INGEST-MANIFEST.cog.md, declaring
  <state>/run/sessions/ as a source the kernel writes.
relates-to:
- cog://architecture/rfcs/observatory-phase1-session-substrate-observer
- cog://workspace/.cog/observatory/ontology/cogos.conversations.v1.yaml
---

# Observatory ingest source — kernel-session-log

## Insertion into `.cog/observatory/INGEST-MANIFEST.cog.md`

```yaml
uri:        cog://workspace/.cog/run/sessions/
carrier:    jsonl (append-only, one record per line, one file per session)
authority:  authoritative      # the kernel's own live record of the drive
durability: archival           # loss IS a defect; not reconstructible once pruned
retention:  append-only
tracked:    false
written-by: kernel (ACP client, internal/engine/session_journal.go)
schema:     cog://workspace/.cog/observatory/ontology/cogos.conversations.v1.yaml
```

Record shape (one line per relayed event):

```json
{"ts":"<RFC3339Nano>","session":"<acp session id>","room":"<room, when bound>",
 "actor":"<who the event belongs to>","kind":"turn|message|tool|permission|plan|end",
 "payload":{...bounded, secret-free...}}
```

## Why this supersedes the observer-reconstructed source

Today the observatory's turn log is reconstructed *afterwards* by
`scripts/observers/conversations-kernel-turns-observer.py`, which reads whatever
a harness happened to leave behind — Hermes `state.db`, a Claude journal, a
driver directory — and fans it into v0.1 records. That record is
**adapter-shaped**: it inherits the quirks, prunes and retention windows of
whichever harness wrote the files, and it can only ever be as complete as that
harness's disk state.

The kernel is the ACP **client** for every managed session: it already sees
every `session/update`, permission request/resolution and turn boundary as it
drives the session. Journalling at that chokepoint makes the record
kernel-authored and live rather than a post-hoc reconstruction:

- **Complete by construction** — one line per relayed event, at the moment it is
  relayed; nothing depends on a harness's post-run retention.
- **Uniform** — one schema regardless of which agent/profile backs the session,
  so the ontology mapping stops being per-adapter.
- **Attributed** — `session`, `room` and `actor` are the kernel's own bindings,
  not inferred from a file path afterwards.
- **Cheap and safe** — bounded payloads (long text truncated, secret-ish keys
  redacted), non-blocking queue, write failures logged and never fatal to the
  turn.

The kernel-turns observer remains for *historical* backfill of the pre-journal
window; from this slice forward, `<state>/run/sessions/` is the live source.
