package acp

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

// A stream_event whose input_json_delta is one 2 MiB line (over the old
// 1 MiB cap) must arrive as a parsed event, and the stream must end cleanly.
func TestReadLoop_LargeLineArrives(t *testing.T) {
	arg := strings.Repeat("x", 2<<20)
	line := `{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"` + arg + `"}}}`
	s := &Subprocess{out: io.NopCloser(strings.NewReader(line + "\n")), events: make(chan Event, 4)}
	s.readLoop()
	var got []Event
	for ev := range s.events {
		got = append(got, ev)
	}
	if err := s.ReadErr(); err != nil {
		t.Fatalf("stream ended early on a %d-byte line: %v", len(line), err)
	}
	if len(got) != 1 || got[0].Stream == nil {
		t.Fatalf("want 1 stream event, got %d: %+v", len(got), got)
	}
}

// A line over the limit still ends the stream, but not silently: ReadErr says why.
func TestReadLoop_OverLimitLineIsReported(t *testing.T) {
	s := &Subprocess{out: io.NopCloser(strings.NewReader(strings.Repeat("y", maxLineBytes+1) + "\n")),
		events: make(chan Event, 4)}
	s.readLoop()
	for range s.events {
	}
	if !errors.Is(s.ReadErr(), bufio.ErrTooLong) {
		t.Fatalf("want bufio.ErrTooLong from ReadErr, got %v", s.ReadErr())
	}
}
