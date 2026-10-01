package engine

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

// maxInferenceRequestBodyBytes caps the request body accepted by the
// inference endpoints (/v1/chat/completions and /v1/messages).
//
// Multimodal conversations carry base64-encoded images inline (a single
// screenshot is ~250 KB), so long agent sessions routinely exceed a few MB.
// The previous 4 MB cap was enforced with io.LimitReader, which silently
// truncated the body and surfaced as a misleading
// "parse body: unexpected end of JSON input" 400. Bodies over this limit are
// now rejected with 413 instead (see readLimitedBody).
const maxInferenceRequestBodyBytes int64 = 32 << 20 // 32 MB

// maxConfigRequestBodyBytes caps config-mutation request bodies.
const maxConfigRequestBodyBytes int64 = 1 << 20 // 1 MB is vast for a scalar config

// errRequestBodyTooLarge is returned (wrapped) by readLimitedBody when the
// body exceeds the limit. Callers should respond 413.
var errRequestBodyTooLarge = errors.New("request body too large")

// readLimitedBody reads r.Body up to limit bytes. Unlike
// io.ReadAll(io.LimitReader(...)), it never truncates: a body larger than
// limit yields an error wrapping errRequestBodyTooLarge so the caller can
// answer 413 rather than attempting to parse a partial document.
func readLimitedBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return nil, fmt.Errorf("%w: exceeds limit of %d bytes", errRequestBodyTooLarge, mbe.Limit)
		}
		return nil, err
	}
	return body, nil
}
