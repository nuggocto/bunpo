package junit_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/junit"
)

// total is a File reduced to what callers use.
type total struct {
	ms      int64
	cases   int
	skipped int
	failed  bool
}

func totals(r junit.Report) map[string]total {
	out := make(map[string]total, len(r.Files))
	for path, f := range r.Files {
		out[path] = total{f.MS(), f.Cases, f.Skipped, f.Failed}
	}

	return out
}

func assertTotals(t *testing.T, got junit.Report, want map[string]total) {
	t.Helper()

	g := totals(got)
	if len(g) != len(want) {
		t.Fatalf("totals = %+v; want %+v", g, want)
	}

	for path, w := range want {
		if g[path] != w {
			t.Fatalf("%s = %+v; want %+v", path, g[path], w)
		}
	}
}

func TestRead(t *testing.T) {
	tests := []struct {
		name string
		xml  string
		src  junit.Source
		want map[string]total
	}{
		{
			name: "times add up per file and round once",
			// Rounding each 0.4 ms case first would give 0, not 1.2 ms.
			xml: `<testsuite>
				<testcase file="a.py" time="0.0004"/>
				<testcase file="a.py" time="0.0004"/>
				<testcase file="a.py" time="0.0004"/>
				<testcase file="b.py" time="1.5"/>
			</testsuite>`,
			src:  junit.FileAttribute,
			want: map[string]total{"a.py": {1, 3, 0, false}, "b.py": {1500, 1, 0, false}},
		},

		{
			// Rounding to nanoseconds first would make this 0.5 ms, then 1 ms.
			name: "sub-nanosecond digits still round once",
			xml:  `<testsuite><testcase file="a.py" time="0.0004999996"/><testcase file="b.py" time="0.0005"/></testsuite>`,
			src:  junit.FileAttribute,
			want: map[string]total{"a.py": {0, 1, 0, false}, "b.py": {1, 1, 0, false}},
		},

		{
			name: "failure and error fail the file, skipped does not",
			xml: `<testsuites><testsuite>
				<testcase file="f.py" time="1"><failure message="x">trace</failure></testcase>
				<testcase file="e.py" time="1"><error/></testcase>
				<testcase file="s.py" time="1"><skipped/></testcase>
			</testsuite></testsuites>`,
			src:  junit.FileAttribute,
			want: map[string]total{"f.py": {1000, 1, 0, true}, "e.py": {1000, 1, 0, true}, "s.py": {1000, 1, 1, false}},
		},

		{
			name: "mixed skipped and passed cases, and duplicate skipped elements in one case",
			xml: `<testsuite>
				<testcase file="mix.py" time="0.5"/>
				<testcase file="mix.py" time="0"><skipped/><skipped/></testcase>
				<testcase file="all.py"><skipped/></testcase>
				<testcase file="all.py"><skipped message="win only"/></testcase>
			</testsuite>`,
			src:  junit.FileAttribute,
			want: map[string]total{"mix.py": {500, 2, 1, false}, "all.py": {0, 2, 2, false}},
		},

		{
			name: "a failure or skipped element deeper inside the case does not count",
			xml: `<testsuite><testcase file="a.py" time="1">
				<system-out><failure>printed text that looks like XML</failure><skipped/></system-out>
			</testcase></testsuite>`,
			src:  junit.FileAttribute,
			want: map[string]total{"a.py": {1000, 1, 0, false}},
		},

		{
			name: "the nearest suite's file attribute stands in for the case's",
			xml: `<testsuites><testsuite file="outer.js"><testsuite file="inner.js">
				<testcase time="1"/>
			</testsuite><testcase time="2"/></testsuite></testsuites>`,
			src:  junit.FileAttribute,
			want: map[string]total{"inner.js": {1000, 1, 0, false}, "outer.js": {2000, 1, 0, false}},
		},

		{
			name: "classname",
			xml:  `<testsuite name="ignored"><testcase classname="sub/a.spec.js" file="ignored" time="1"/></testsuite>`,
			src:  junit.Classname,
			want: map[string]total{"sub/a.spec.js": {1000, 1, 0, false}},
		},

		{
			name: "suite name",
			xml:  `<testsuites><testsuite name="a.spec.js"><testcase classname="ignored" time="1"/></testsuite></testsuites>`,
			src:  junit.SuiteName,
			want: map[string]total{"a.spec.js": {1000, 1, 0, false}},
		},

		{
			name: "a skipped case with no time counts as zero",
			xml:  `<testsuite><testcase classname="a.spec.js"><skipped/></testcase></testsuite>`,
			src:  junit.Classname,
			want: map[string]total{"a.spec.js": {0, 1, 1, false}},
		},

		{
			name: "an empty suite, with a declaration, comments and whitespace",
			xml:  "<?xml version=\"1.0\"?>\n<!-- run 1 -->\n<testsuites/>\n\n",
			src:  junit.FileAttribute,
			want: map[string]total{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := junit.Read(strings.NewReader(tc.xml), tc.src)
			if err != nil {
				t.Fatal(err)
			}

			assertTotals(t, got, tc.want)
		})
	}
}

func TestReadRejects(t *testing.T) {
	tests := []struct {
		name    string
		xml     string
		wantErr error
		wantMsg string
	}{
		// A runner that crashed before writing must not pass for an empty report.
		{"empty file", ``, junit.ErrInvalidReport, "no root element"},
		{"plain text", `not XML`, junit.ErrInvalidReport, "outside the root"},
		{"two roots", `<testsuite/><testsuite/>`, junit.ErrInvalidReport, "more than one root"},
		{"text after the root", `<testsuites/>trailing`, junit.ErrInvalidReport, "outside the root"},
		{"not a JUnit report", `<html><body/></html>`, junit.ErrInvalidReport, "<html>"},
		{"malformed XML", `<testsuite><testcase file="a.py"></testsuite>`, junit.ErrInvalidReport, ""},
		{"nesting too deep", strings.Repeat("<testsuite>", junit.MaxDepth+1), junit.ErrInvalidReport, "deeper"},

		// Leaving out a case without a file would also leave out its failure.
		{"a case names no file", `<testsuite><testcase name="t" time="1"/></testsuite>`, junit.ErrNoFile, `"t"`},
		{"one of several cases names no file", `<testsuite><testcase file="a.py" time="1"/><testcase name="broken" time="1"><failure/></testcase></testsuite>`, junit.ErrNoFile, `"broken"`},

		{"negative time", `<testsuite><testcase name="t" file="a.py" time="-1"/></testsuite>`, junit.ErrInvalidReport, `"t"`},
		{"time over the limit", `<testsuite><testcase name="t" file="a.py" time="2592000.001"/></testsuite>`, junit.ErrInvalidReport, "longer"},
		{"file total over the limit", `<testsuite><testcase file="a.py" time="2592000"/><testcase file="a.py" time="1"/></testsuite>`, junit.ErrInvalidReport, "a.py"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := junit.Read(strings.NewReader(tc.xml), junit.FileAttribute)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v; want %v", err, tc.wantErr)
			}

			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}
}

func TestReadLines(t *testing.T) {
	input := "tests/a.py\t1200\r\n" +
		"tests/with space.py 30\n" +
		"\n" +
		"tests/a.py 5\n" +
		"tests/fails.py -\n"

	got, err := junit.ReadLines(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}

	assertTotals(t, got, map[string]total{
		"tests/a.py":          {1205, 2, 0, false},
		"tests/with space.py": {30, 1, 0, false},
		"tests/fails.py":      {0, 1, 0, true},
	})
}

func TestReadLinesRejects(t *testing.T) {
	for _, line := range []string{"tests/a.py", "tests/a.py -5", "tests/a.py +5", "tests/a.py 1.5", "tests/a.py 2592000001", " 12"} {
		if _, err := junit.ReadLines(strings.NewReader(line)); !errors.Is(err, junit.ErrInvalidReport) {
			t.Fatalf("ReadLines(%q): error = %v; want %v", line, err, junit.ErrInvalidReport)
		}
	}
}
