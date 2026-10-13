package engine

import (
	"bufio"
	"io"
)

// maxStreamLineBytes bounds one line of an upstream model stream: an SSE line
// from an HTTP provider (Anthropic, OpenAI-compatible, Ollama NDJSON, the
// kernel's own chat client) or an NDJSON line from a subprocess provider
// (Claude Code, Codex, pi). bufio.Scanner's default limit is 64 KiB, and these
// transports were capped at 64 KiB, 256 KiB or 1 MiB. One event can exceed
// that: an input_json_delta carrying a large tool argument (a model writing a
// whole file in one write_file call) or a long text delta. When it did, Scan()
// stopped with "bufio.Scanner: token too long", the stream ended as an error
// mid-turn, the client (Hermes) saw a connection error or an empty response,
// retried the identical request into the identical failure, and gave up with
// empty_response_exhausted. Observed 47 times in serve.log between 2026-09-30
// and 2026-10-12; it is what ended several stage work threads with no report.
//
// 16 MiB is far above any real event while still bounding a hostile or
// broken upstream. The buffer grows on demand from 64 KiB, so ordinary
// streams pay nothing extra. Every streaming provider must build its line
// scanner with newStreamScanner (TestProviders_UseStreamScanner enforces it).
const maxStreamLineBytes = 16 << 20

// newStreamScanner returns a line scanner for an upstream stream body whose
// per-line limit is maxStreamLineBytes instead of bufio's 64 KiB default.
func newStreamScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxStreamLineBytes)
	return sc
}
