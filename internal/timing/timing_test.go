package timing_test

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/timing"
)

func TestMedian(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
		want   int64
		wantOK bool
	}{
		{"no values", nil, 0, false},
		{"one value", []int64{7}, 7, true},
		{"odd count, unsorted", []int64{30, 10, 20}, 20, true},
		{"even count averages the two middles", []int64{40, 10, 30, 20}, 25, true},
		{"even count rounds half up", []int64{10, 21}, 16, true},
		{"two equal values", []int64{4, 4}, 4, true},
		{"one slow run does not move it", []int64{100, 102, 98, 5000, 101}, 101, true},
		{"zero durations", []int64{0, 0, 1}, 0, true},
		{"full nonnegative range without overflow", []int64{math.MaxInt64, 0}, 1 << 62, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := timing.Median(tc.values)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("Median(%v) = %d, %t; want %d, %t", tc.values, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestMedianLeavesInputUnchanged(t *testing.T) {
	values := []int64{3, 1, 2}
	timing.Median(values)

	if !slices.Equal(values, []int64{3, 1, 2}) {
		t.Fatalf("Median reordered its input to %v", values)
	}
}

func TestDecode(t *testing.T) {
	tests := []struct {
		name string
		data string
		want timing.File
	}{
		{
			name: "tests with and without samples",
			data: `{"version":1,"run":4,"tests":{` +
				`"a.py":{"samples":[10,20],"last_seen":4},` +
				`"b.py":{"samples":[],"last_seen":2}}}`,
			want: timing.File{Run: 4, Tests: map[string]timing.Entry{
				"a.py": {Samples: []int64{10, 20}, LastSeen: 4},
				"b.py": {LastSeen: 2},
			}},
		},

		{
			name: "no tests yet",
			data: `{"version":1,"run":0,"tests":{}}`,
			want: timing.File{Run: 0, Tests: map[string]timing.Entry{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := timing.Decode([]byte(tc.data))
			if err != nil {
				t.Fatal(err)
			}

			// Callers add to Tests, so it must never be nil.
			if got.Tests == nil {
				t.Fatal("Tests is nil; want a map")
			}

			assertFile(t, got, tc.want)
		})
	}
}

func TestDecodeRejects(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantErr error
		wantMsg string
	}{
		{"newer version", `{"version":2,"run":0,"tests":{},"new_field":1}`, timing.ErrUnsupportedVersion, "version 2"},
		{"missing version", `{"run":0,"tests":{}}`, timing.ErrUnsupportedVersion, "version 0"},
		{"not JSON", `run=1`, timing.ErrInvalidFile, ""},
		{"unknown member", `{"version":1,"run":0,"tests":{},"extra":true}`, timing.ErrInvalidFile, "extra"},
		{"duplicate test", `{"version":1,"run":1,"tests":{"a.py":{"samples":[1],"last_seen":1},"a.py":{"samples":[2],"last_seen":1}}}`, timing.ErrInvalidFile, "duplicate"},
		{"invalid UTF-8 name", "{\"version\":1,\"run\":1,\"tests\":{\"a\xff.py\":{\"samples\":[1],\"last_seen\":1}}}", timing.ErrInvalidFile, ""},
		{"six samples", `{"version":1,"run":1,"tests":{"a.py":{"samples":[1,2,3,4,5,6],"last_seen":1}}}`, timing.ErrInvalidFile, "a.py"},
		{"negative sample", `{"version":1,"run":1,"tests":{"a.py":{"samples":[-1],"last_seen":1}}}`, timing.ErrInvalidFile, "a.py"},
		{"sample above the limit", `{"version":1,"run":1,"tests":{"a.py":{"samples":[2592000001],"last_seen":1}}}`, timing.ErrInvalidFile, "a.py"},
		{"fractional sample", `{"version":1,"run":1,"tests":{"a.py":{"samples":[1.5],"last_seen":1}}}`, timing.ErrInvalidFile, ""},
		{"seen after the current run", `{"version":1,"run":1,"tests":{"a.py":{"samples":[1],"last_seen":2}}}`, timing.ErrInvalidFile, "a.py"},
		{"negative run", `{"version":1,"run":-1,"tests":{}}`, timing.ErrInvalidFile, "run"},
		{"no run member", `{"version":1,"tests":{}}`, timing.ErrInvalidFile, `no "run" member`},
		{"no tests member", `{"version":1,"run":0}`, timing.ErrInvalidFile, `no "tests" member`},
		{"null tests", `{"version":1,"run":0,"tests":null}`, timing.ErrInvalidFile, `no "tests" member`},
		{"entry missing samples", `{"version":1,"run":1,"tests":{"a.py":{"last_seen":1}}}`, timing.ErrInvalidFile, `no "samples" member`},
		{"entry null samples", `{"version":1,"run":1,"tests":{"a.py":{"samples":null,"last_seen":1}}}`, timing.ErrInvalidFile, `no "samples" member`},
		{"entry null sample element", `{"version":1,"run":1,"tests":{"a.py":{"samples":[null],"last_seen":1}}}`, timing.ErrInvalidFile, "null"},
		{"entry mixed null sample element", `{"version":1,"run":1,"tests":{"a.py":{"samples":[10,null],"last_seen":1}}}`, timing.ErrInvalidFile, "null"},
		{"entry non-array samples", `{"version":1,"run":1,"tests":{"a.py":{"samples":10,"last_seen":1}}}`, timing.ErrInvalidFile, "expected array"},
		{"entry missing last_seen", `{"version":1,"run":1,"tests":{"a.py":{"samples":[1]}}}`, timing.ErrInvalidFile, `no "last_seen" member`},
		{"empty entry", `{"version":1,"run":1,"tests":{"a.py":{}}}`, timing.ErrInvalidFile, `no "samples" member`},
		{"empty test identifier", `{"version":1,"run":1,"tests":{"":{"samples":[1],"last_seen":1}}}`, timing.ErrInvalidFile, "non-canonical"},
		{"relative dot test identifier", `{"version":1,"run":1,"tests":{"./a.py":{"samples":[1],"last_seen":1}}}`, timing.ErrInvalidFile, "non-canonical"},
		{"parent component test identifier", `{"version":1,"run":1,"tests":{"../a.py":{"samples":[1],"last_seen":1}}}`, timing.ErrInvalidFile, "non-canonical"},
		{"absolute test identifier", `{"version":1,"run":1,"tests":{"/a.py":{"samples":[1],"last_seen":1}}}`, timing.ErrInvalidFile, "non-canonical"},
		{"backslash test identifier", `{"version":1,"run":1,"tests":{"a\\b.py":{"samples":[1],"last_seen":1}}}`, timing.ErrInvalidFile, "non-canonical"},
		{"Windows drive test identifier", `{"version":1,"run":1,"tests":{"C:/a.py":{"samples":[1],"last_seen":1}}}`, timing.ErrInvalidFile, "non-canonical"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := timing.Decode([]byte(tc.data))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v; want %v", err, tc.wantErr)
			}

			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}
}

func TestDecodeRejectsOversizedFile(t *testing.T) {
	data := make([]byte, timing.MaxFileBytes+1)
	if _, err := timing.Decode(data); !errors.Is(err, timing.ErrInvalidFile) {
		t.Fatalf("error = %v; want %v", err, timing.ErrInvalidFile)
	}
}

func TestEncodeFormat(t *testing.T) {
	// Pins the wire format. Changing it needs a new FormatVersion.
	f := timing.File{Run: 2, Tests: map[string]timing.Entry{
		"b.py": {LastSeen: 1},
		"a.py": {Samples: []int64{5, 7}, LastSeen: 2},
	}}
	got, err := timing.Encode(f)
	if err != nil {
		t.Fatal(err)
	}

	want := `{"version":1,"run":2,"tests":{"a.py":{"samples":[5,7],"last_seen":2},"b.py":{"samples":[],"last_seen":1}}}`
	if string(got) != want {
		t.Fatalf("Encode =\n%s\nwant\n%s", got, want)
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	f := timing.File{Run: 9, Tests: map[string]timing.Entry{
		"dir/a_test.go": {Samples: []int64{1, 2, 3, 4, 5}, LastSeen: 9},
		"z.spec.js":     {Samples: []int64{0}, LastSeen: 3},
		"never-passed":  {LastSeen: 8},
	}}
	data, err := timing.Encode(f)
	if err != nil {
		t.Fatal(err)
	}

	got, err := timing.Decode(data)
	if err != nil {
		t.Fatal(err)
	}

	assertFile(t, got, f)
}

func TestEncodeRejectsInvalidFile(t *testing.T) {
	f := timing.File{Run: 1, Tests: map[string]timing.Entry{
		"a.py": {Samples: []int64{1, 2, 3, 4, 5, 6}, LastSeen: 1},
	}}
	if _, err := timing.Encode(f); !errors.Is(err, timing.ErrInvalidFile) {
		t.Fatalf("error = %v; want %v", err, timing.ErrInvalidFile)
	}
}

func TestDurations(t *testing.T) {
	f := timing.File{Run: 5, Tests: map[string]timing.Entry{
		"a.py":         {Samples: []int64{10, 30, 20}, LastSeen: 5},
		"b.py":         {Samples: []int64{10, 21}, LastSeen: 5},
		"never-passed": {LastSeen: 5},
	}}
	got := f.Durations()
	want := map[string]int64{"a.py": 20, "b.py": 16}
	if len(got) != len(want) {
		t.Fatalf("Durations() = %v; want %v", got, want)
	}

	for id, ms := range want {
		if got[id] != ms {
			t.Fatalf("Durations()[%q] = %d; want %d", id, got[id], ms)
		}
	}
}

// assertFile treats nil and empty samples as equal, as JSON does.
func assertFile(t *testing.T, got, want timing.File) {
	t.Helper()

	if got.Run != want.Run {
		t.Fatalf("Run = %d; want %d", got.Run, want.Run)
	}

	if len(got.Tests) != len(want.Tests) {
		t.Fatalf("Tests = %v; want %v", got.Tests, want.Tests)
	}

	for id, w := range want.Tests {
		g, ok := got.Tests[id]
		if !ok {
			t.Fatalf("test %q missing; got %v", id, got.Tests)
		}

		if g.LastSeen != w.LastSeen || !slices.Equal(g.Samples, w.Samples) {
			t.Fatalf("Tests[%q] = %+v; want %+v", id, g, w)
		}
	}
}
