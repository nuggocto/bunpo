package manifest_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/timing"
)

var digestA = strings.Repeat("a", 64)

func TestRoundTrip(t *testing.T) {
	m := manifest.Manifest{Index: 2, Machines: 3, Digest: digestA, Tests: []string{"a.py", "dir/b.py"}}
	data, err := manifest.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	if got, err := manifest.DecodeManifest(data); err != nil || !reflect.DeepEqual(got, m) {
		t.Fatalf("manifest round trip = %+v, %v; want %+v", got, err, m)
	}

	p := manifest.Plan{Machines: 2, Digest: digestA, Shards: [][]string{{"a.py"}, {}}}
	data, err = manifest.EncodePlan(p)
	if err != nil {
		t.Fatal(err)
	}

	if got, err := manifest.DecodePlan(data); err != nil || !reflect.DeepEqual(got, p) {
		t.Fatalf("plan round trip = %+v, %v; want %+v", got, err, p)
	}

	r := manifest.RunReport{Index: 0, Machines: 1, Tests: timing.Report{
		"a.py": {Timed: true, MS: 120},
		"b.py": {Timed: false},
	}}
	data, err = manifest.EncodeRunReport(r)
	if err != nil {
		t.Fatal(err)
	}

	if got, err := manifest.DecodeRunReport(data); err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("run report round trip = %+v, %v; want %+v", got, err, r)
	}
}

func TestEncodeFormats(t *testing.T) {
	// Pins the wire formats. Changing one needs a new FormatVersion.
	m, err := manifest.EncodeManifest(manifest.Manifest{Index: 0, Machines: 1, Digest: digestA, Tests: []string{"a.py"}})
	if err != nil {
		t.Fatal(err)
	}

	if want := `{"version":1,"index":0,"machines":1,"digest":"` + digestA + `","tests":["a.py"]}`; string(m) != want {
		t.Fatalf("manifest =\n%s\nwant\n%s", m, want)
	}

	r, err := manifest.EncodeRunReport(manifest.RunReport{Index: 0, Machines: 1, Tests: timing.Report{"a.py": {Timed: true, MS: 5}}})
	if err != nil {
		t.Fatal(err)
	}

	if want := `{"version":1,"index":0,"machines":1,"tests":{"a.py":{"timed":true,"ms":5}}}`; string(r) != want {
		t.Fatalf("run report =\n%s\nwant\n%s", r, want)
	}
}

func TestDecodeRejects(t *testing.T) {
	tests := []struct {
		name    string
		decode  func([]byte) error
		data    string
		wantMsg string
	}{
		{"manifest index out of range", manifestErr, `{"version":1,"index":3,"machines":3,"digest":"` + digestA + `","tests":[]}`, "index 3"},
		{"manifest with no machines", manifestErr, `{"version":1,"index":0,"machines":0,"digest":"` + digestA + `","tests":[]}`, "0 machines"},
		{"manifest digest too short", manifestErr, `{"version":1,"index":0,"machines":1,"digest":"abc","tests":[]}`, "digest"},
		{"manifest digest in capitals", manifestErr, `{"version":1,"index":0,"machines":1,"digest":"` + strings.Repeat("A", 64) + `","tests":[]}`, "lowercase"},
		{"manifest tests out of order", manifestErr, `{"version":1,"index":0,"machines":1,"digest":"` + digestA + `","tests":["b.py","a.py"]}`, `"a.py"`},
		{"manifest test listed twice", manifestErr, `{"version":1,"index":0,"machines":1,"digest":"` + digestA + `","tests":["a.py","a.py"]}`, `"a.py"`},
		{"manifest test not canonical", manifestErr, `{"version":1,"index":0,"machines":1,"digest":"` + digestA + `","tests":["./a.py"]}`, `"./a.py"`},
		{"manifest from a newer release", manifestErr, `{"version":2}`, "version 2"},
		{"plan with too few shards", planErr, `{"version":1,"machines":2,"digest":"` + digestA + `","shards":[["a.py"]]}`, "1 shards"},
		{"plan with a null shard", planErr, `{"version":1,"machines":2,"digest":"` + digestA + `","shards":[null,["a.py"]]}`, "shard 0 is null"},
		{"plan assigns a test twice", planErr, `{"version":1,"machines":2,"digest":"` + digestA + `","shards":[["a.py"],["a.py"]]}`, `"a.py"`},
		{"report with an untimed duration", reportErr, `{"version":1,"index":0,"machines":1,"tests":{"a.py":{"timed":false,"ms":5}}}`, `"a.py"`},
		{"report with a negative duration", reportErr, `{"version":1,"index":0,"machines":1,"tests":{"a.py":{"timed":true,"ms":-1}}}`, `"a.py"`},
		{"report with a backslash path", reportErr, `{"version":1,"index":0,"machines":1,"tests":{"a\\b.py":{"timed":true,"ms":1}}}`, "non-canonical"},
		{"report entry missing ms", reportErr, `{"version":1,"index":0,"machines":1,"tests":{"a.py":{"timed":true}}}`, `no "ms" member`},
		{"report entry missing timed", reportErr, `{"version":1,"index":0,"machines":1,"tests":{"a.py":{"ms":0}}}`, `no "timed" member`},
		{"empty report entry", reportErr, `{"version":1,"index":0,"machines":1,"tests":{"a.py":{}}}`, `no "timed" member`},
		{"manifest with no index", manifestErr, `{"version":1,"machines":1,"digest":"` + digestA + `","tests":[]}`, `no "index" member`},
		{"manifest with no tests", manifestErr, `{"version":1,"index":0,"machines":1,"digest":"` + digestA + `"}`, `no "tests" member`},
		{"plan with no shards", planErr, `{"version":1,"machines":1,"digest":"` + digestA + `"}`, `no "shards" member`},
		{"report with only a machine count", reportErr, `{"version":1,"machines":1}`, `no "index" member`},
		{"report with no tests", reportErr, `{"version":1,"index":0,"machines":1}`, `no "tests" member`},
		{"report with an unknown member", reportErr, `{"version":1,"index":0,"machines":1,"tests":{},"extra":1}`, "extra"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.decode([]byte(tc.data))
			if err == nil || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error = %v; want one mentioning %q", err, tc.wantMsg)
			}
		})
	}
}

func TestEncodeRejectsInvalid(t *testing.T) {
	_, err := manifest.EncodeManifest(manifest.Manifest{Index: 1, Machines: 1, Digest: digestA})
	if !errors.Is(err, manifest.ErrInvalidFile) {
		t.Fatalf("error = %v; want %v", err, manifest.ErrInvalidFile)
	}
}

func TestDecodeReturnsZeroOnValidationError(t *testing.T) {
	if got, err := manifest.DecodeManifest([]byte(`{"version":1,"index":2,"machines":1,"digest":"` + digestA + `","tests":[]}`)); err == nil || !reflect.ValueOf(got).IsZero() {
		t.Fatalf("DecodeManifest = %+v, %v; want zero value and error", got, err)
	}

	if got, err := manifest.DecodePlan([]byte(`{"version":1,"machines":2,"digest":"` + digestA + `","shards":[["a.py"]]}`)); err == nil || !reflect.ValueOf(got).IsZero() {
		t.Fatalf("DecodePlan = %+v, %v; want zero value and error", got, err)
	}

	if got, err := manifest.DecodeRunReport([]byte(`{"version":1,"index":2,"machines":1,"tests":{}}`)); err == nil || !reflect.ValueOf(got).IsZero() {
		t.Fatalf("DecodeRunReport = %+v, %v; want zero value and error", got, err)
	}
}

func manifestErr(b []byte) error { _, err := manifest.DecodeManifest(b); return err }
func planErr(b []byte) error     { _, err := manifest.DecodePlan(b); return err }
func reportErr(b []byte) error   { _, err := manifest.DecodeRunReport(b); return err }
