package room

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// This file is a full RFC 8785 (JSON Canonicalization Scheme) encoder.
//
// P4-1 unknown U1 asked whether the ledger's canonicalJSON ("minimal RFC 8785",
// internal/engine/ledger.go) is good enough for entry bodies. It is not: it
// sorts keys by UTF-8 bytes where RFC 8785 sorts by UTF-16 code units, it
// escapes strings through encoding/json (which HTML-escapes <, > and & and
// escapes U+2028/U+2029), and it formats numbers through encoding/json rather
// than the ECMAScript Number-to-String algorithm. Entry ids and signatures
// must be byte-identical on every implementation, so this package ships its
// own JCS and tests it against the RFC 8785 vectors.

// ErrJCS wraps every canonicalization failure.
var ErrJCS = errors.New("jcs")

// CanonicalizeJSON returns the RFC 8785 canonical form of one JSON text.
// Duplicate object keys are rejected (I-JSON), as is trailing content.
func CanonicalizeJSON(raw []byte) ([]byte, error) {
	v, err := parseStrict(raw)
	if err != nil {
		return nil, err
	}
	return canonValue(v)
}

// Canonicalize marshals v with encoding/json and returns its canonical form.
func Canonicalize(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJCS, err)
	}
	return CanonicalizeJSON(raw)
}

func parseStrict(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := parseValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing content", ErrJCS)
	}
	return v, nil
}

func parseValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJCS, err)
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return tok, nil // string, json.Number, bool, nil
	}
	switch d {
	case '{':
		m := map[string]any{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrJCS, err)
			}
			k, _ := kt.(string)
			if _, dup := m[k]; dup {
				return nil, fmt.Errorf("%w: duplicate key %q", ErrJCS, k)
			}
			v, err := parseValue(dec)
			if err != nil {
				return nil, err
			}
			m[k] = v
		}
		if _, err := dec.Token(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrJCS, err)
		}
		return m, nil
	case '[':
		arr := []any{}
		for dec.More() {
			v, err := parseValue(dec)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		if _, err := dec.Token(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrJCS, err)
		}
		return arr, nil
	}
	return nil, fmt.Errorf("%w: unexpected delimiter", ErrJCS)
}

func canonValue(v any) ([]byte, error) {
	var b bytes.Buffer
	if err := canonWrite(&b, v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func canonWrite(b *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		return writeJCSString(b, x)
	case json.Number:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return fmt.Errorf("%w: number %q: %v", ErrJCS, string(x), err)
		}
		s, err := FormatNumber(f)
		if err != nil {
			return err
		}
		b.WriteString(s)
	case float64:
		s, err := FormatNumber(x)
		if err != nil {
			return err
		}
		b.WriteString(s)
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := canonWrite(b, e); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return lessUTF16(keys[i], keys[j]) })
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeJCSString(b, k); err != nil {
				return err
			}
			b.WriteByte(':')
			if err := canonWrite(b, x[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("%w: unsupported type %T", ErrJCS, v)
	}
	return nil
}

// lessUTF16 orders strings by UTF-16 code units (RFC 8785 section 3.2.3).
func lessUTF16(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

func writeJCSString(b *bytes.Buffer, s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("%w: invalid UTF-8", ErrJCS)
	}
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if r < 0x20 {
				fmt.Fprintf(b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return nil
}

// FormatNumber renders a float64 as ECMAScript Number::toString (RFC 8785
// section 3.2.2.3). NaN and the infinities are errors; -0 renders as "0".
func FormatNumber(f float64) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("%w: non-finite number", ErrJCS)
	}
	if f == 0 {
		return "0", nil
	}
	sign := ""
	if f < 0 {
		sign = "-"
		f = -f
	}
	// Shortest round-trip digits: d.ddde+XX.
	e := strconv.FormatFloat(f, 'e', -1, 64)
	mant, expStr, _ := strings.Cut(e, "e")
	digits := strings.Replace(mant, ".", "", 1)
	exp, _ := strconv.Atoi(expStr)
	k := len(digits)
	n := exp + 1 // position of the decimal point relative to the digits
	var out string
	switch {
	case k <= n && n <= 21:
		out = digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		out = digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		out = "0." + strings.Repeat("0", -n) + digits
	default:
		ex := n - 1
		es := "+"
		if ex < 0 {
			es = "-"
			ex = -ex
		}
		if k == 1 {
			out = digits + "e" + es + strconv.Itoa(ex)
		} else {
			out = digits[:1] + "." + digits[1:] + "e" + es + strconv.Itoa(ex)
		}
	}
	return sign + out, nil
}
