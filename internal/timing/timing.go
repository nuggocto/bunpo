// Package timing holds the timing file: recent durations per test, merging
// runs into it, and pruning tests that stopped appearing.
package timing

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/nuggocto/bunpo/internal/jsonfile"
)

const (
	FormatVersion  = 1
	MaxSamples     = 5
	PruneAfterRuns = 30
	MaxFileBytes   = 64 << 20

	// MaxDurationMS is 30 days. It keeps a total of split.MaxTests durations
	// inside int64.
	MaxDurationMS int64 = 30 * 24 * 60 * 60 * 1000
)

var (
	ErrUnsupportedVersion = jsonfile.ErrUnsupportedVersion
	ErrInvalidFile        = jsonfile.ErrInvalid
)

const kind = "timing file"

// File is a decoded timing file. Run counts merges.
type File struct {
	Run   int64
	Tests map[string]Entry
}

// Entry is one test's history.
type Entry struct {
	// Samples holds recent durations in milliseconds, oldest first. It is
	// empty for a test that has never fully passed.
	Samples []int64

	// LastSeen is the Run of the last merge that saw the test.
	LastSeen int64
}

// sampleSlice decodes a JSON array of non-null integers into []int64,
// rejecting null elements that encoding/json/v2 would otherwise zero out.
type sampleSlice []int64

func (s *sampleSlice) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() == 'n' {
		if _, err := dec.ReadToken(); err != nil {
			return err
		}

		*s = nil
		return nil
	}

	tok, err := dec.ReadToken()
	if err != nil {
		return err
	}

	if tok.Kind() != '[' {
		return fmt.Errorf("expected array, got %s", tok.Kind())
	}

	out := []int64{}
	for dec.PeekKind() != ']' {
		if dec.PeekKind() == 'n' {
			return errors.New("sample cannot be null")
		}

		var v int64
		if err := json.UnmarshalDecode(dec, &v); err != nil {
			return err
		}

		out = append(out, v)
	}

	if _, err := dec.ReadToken(); err != nil {
		return err
	}

	*s = out
	return nil
}

type entryDecodeJSON struct {
	Samples  sampleSlice `json:"samples"`
	LastSeen *int64      `json:"last_seen"`
}

type fileDecodeJSON struct {
	Version int64                      `json:"version"`
	Run     *int64                     `json:"run"` // a pointer tells absent from 0
	Tests   map[string]entryDecodeJSON `json:"tests"`
}

func (f *fileDecodeJSON) FileVersion() int64 { return f.Version }

type entryEncodeJSON struct {
	Samples  []int64 `json:"samples"`
	LastSeen int64   `json:"last_seen"`
}

type fileEncodeJSON struct {
	Version int64                      `json:"version"`
	Run     int64                      `json:"run"`
	Tests   map[string]entryEncodeJSON `json:"tests"`
}

func Decode(data []byte) (File, error) {
	var raw fileDecodeJSON
	if err := jsonfile.Decode(kind, data, MaxFileBytes, FormatVersion, &raw); err != nil {
		return File{}, err
	}

	err := jsonfile.Require(kind,
		jsonfile.Member{Name: "run", Present: raw.Run != nil},
		jsonfile.Member{Name: "tests", Present: raw.Tests != nil})
	if err != nil {
		return File{}, err
	}

	run := *raw.Run
	if run < 0 {
		return File{}, fmt.Errorf("%w: %s: run %d is negative", ErrInvalidFile, kind, run)
	}

	tests := make(map[string]Entry, len(raw.Tests))
	for _, id := range slices.Sorted(maps.Keys(raw.Tests)) {
		e := raw.Tests[id]
		if err := jsonfile.Require(fmt.Sprintf("%s test %q", kind, id),
			jsonfile.Member{Name: "samples", Present: e.Samples != nil},
			jsonfile.Member{Name: "last_seen", Present: e.LastSeen != nil}); err != nil {
			return File{}, err
		}

		entry := Entry{Samples: []int64(e.Samples), LastSeen: *e.LastSeen}
		if err := validateEntry(id, entry, run); err != nil {
			return File{}, err
		}

		tests[id] = entry
	}

	return File{Run: run, Tests: tests}, nil
}

// Encode returns the same bytes for the same file, so caches and diffs stay
// stable.
func Encode(f File) ([]byte, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}

	tests := make(map[string]entryEncodeJSON, len(f.Tests))
	for id, e := range f.Tests {
		samples := e.Samples
		if samples == nil {
			samples = []int64{}
		}

		tests[id] = entryEncodeJSON{Samples: samples, LastSeen: e.LastSeen}
	}

	return jsonfile.Encode(kind, fileEncodeJSON{Version: FormatVersion, Run: f.Run, Tests: tests}, MaxFileBytes)
}

// Durations returns each test's median. Tests without samples are left out,
// so a split treats them as unknown.
func (f File) Durations() map[string]int64 {
	out := make(map[string]int64, len(f.Tests))

	for id, e := range f.Tests {
		if ms, ok := Median(e.Samples); ok {
			out[id] = ms
		}
	}

	return out
}

func (f File) validate() error {
	if f.Run < 0 {
		return fmt.Errorf("%w: %s: run %d is negative", ErrInvalidFile, kind, f.Run)
	}

	// Sorted, so a bad file always reports the same test.
	for _, id := range slices.Sorted(maps.Keys(f.Tests)) {
		if err := validateEntry(id, f.Tests[id], f.Run); err != nil {
			return err
		}
	}

	return nil
}

func validateEntry(id string, e Entry, run int64) error {
	if !validID(id) {
		return fmt.Errorf("%w: %s has non-canonical test identifier %q", ErrInvalidFile, kind, id)
	}

	if len(e.Samples) > MaxSamples {
		return fmt.Errorf("%w: %s: test %q has %d samples; the limit is %d", ErrInvalidFile, kind, id, len(e.Samples), MaxSamples)
	}

	for _, ms := range e.Samples {
		if ms < 0 || ms > MaxDurationMS {
			return fmt.Errorf("%w: %s: test %q has duration %d ms outside 0 to %d", ErrInvalidFile, kind, id, ms, MaxDurationMS)
		}
	}

	if e.LastSeen < 0 || e.LastSeen > run {
		return fmt.Errorf("%w: %s: test %q was last seen at run %d, outside 0 to %d", ErrInvalidFile, kind, id, e.LastSeen, run)
	}

	return nil
}

// validID reports whether id is already in canonical POSIX form: non-empty,
// valid UTF-8, slash-separated with no empty, "." or ".." components, no
// backslashes or control separators, and no leading Windows drive. Kept here
// to preserve the one-way dependency from split to timing.
func validID(id string) bool {
	if id == "" || !utf8.ValidString(id) || strings.ContainsAny(id, "\x00\r\n\\") {
		return false
	}

	first := true
	for part := range strings.SplitSeq(id, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}

		if first && len(part) >= 2 && part[1] == ':' &&
			(('a' <= part[0] && part[0] <= 'z') || ('A' <= part[0] && part[0] <= 'Z')) {
			return false
		}

		first = false
	}

	return true
}

// Median averages the two middle values for an even count, rounding half up,
// like a spreadsheet. It reports false for no values. Values must not be
// negative.
func Median(values []int64) (int64, bool) {
	if len(values) == 0 {
		return 0, false
	}

	var sorted []int64
	var buf [MaxSamples]int64
	if len(values) <= len(buf) {
		sorted = buf[:len(values)]
		copy(sorted, values)
	} else {
		sorted = slices.Clone(values)
	}

	slices.Sort(sorted)

	if sorted[0] < 0 {
		panic(fmt.Sprintf("timing.Median: negative value %d", sorted[0]))
	}

	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid], true
	}

	// lo + d/2 + d%2 cannot overflow, unlike (lo+hi+1)/2.
	lo, hi := sorted[mid-1], sorted[mid]
	d := hi - lo
	return lo + d/2 + d%2, true
}
