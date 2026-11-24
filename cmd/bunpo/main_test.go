package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/timing"
)

var digestA = strings.Repeat("a", 64)

// suite mixes LF, CRLF and a blank line, as real lists do.
const suite = "tests/a.py\ntests/b.py\r\ntests/c.py\n\ntests/d.py\n"

// noEnv is an environment with nothing set: no GitHub Actions, no color.
func noEnv(string) string { return "" }

// bunpo runs one command in-process.
func bunpo(t *testing.T, stdin string, args ...string) (int, string, string) {
	t.Helper()

	var stdout, stderr bytes.Buffer
	code := run(args, strings.NewReader(stdin), &stdout, &stderr, noEnv)
	return code, stdout.String(), stderr.String()
}

func mustBunpo(t *testing.T, stdin string, args ...string) (string, string) {
	t.Helper()

	code, stdout, stderr := bunpo(t, stdin, args...)
	if code != 0 {
		t.Fatalf("bunpo %s exited %d:\n%s", strings.Join(args, " "), code, stderr)
	}

	return stdout, stderr
}

func put(t *testing.T, path string, data []byte) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeTimings(t *testing.T, dir string, f timing.File) {
	t.Helper()

	data, err := timing.Encode(f)
	if err != nil {
		t.Fatal(err)
	}

	put(t, filepath.Join(dir, "timings.json"), data)
}

func writeManifest(t *testing.T, dir string, m manifest.Manifest) {
	t.Helper()

	data, err := manifest.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}

	put(t, filepath.Join(dir, manifestName(m.Index)), data)
}

func readTimingFile(t *testing.T, dir string) timing.File {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, "timings.json"))
	if err != nil {
		t.Fatal(err)
	}

	f, err := timing.Decode(data)
	if err != nil {
		t.Fatal(err)
	}

	return f
}

func readRunReport(t *testing.T, dir string, index int) manifest.RunReport {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, reportName(index)))
	if err != nil {
		t.Fatal(err)
	}

	r, err := manifest.DecodeRunReport(data)
	if err != nil {
		t.Fatal(err)
	}

	return r
}

func samples(ms ...int64) timing.Entry {
	return timing.Entry{Samples: ms, LastSeen: 1}
}

func TestSplit(t *testing.T) {
	dir := t.TempDir()
	writeTimings(t, dir, timing.File{Run: 1, Tests: map[string]timing.Entry{
		"tests/a.py": samples(9000),
		"tests/b.py": samples(7000),
		"tests/c.py": samples(6000),
		"tests/d.py": samples(5000),
	}})

	stdout, stderr := mustBunpo(t, suite, "split", "--dir", dir, "--index", "1", "--total", "2")

	if stdout != "tests/b.py\ntests/c.py\n" {
		t.Fatalf("stdout = %q; want machine 1's share, one per line", stdout)
	}

	if !strings.Contains(stderr, "machine 1 of 2 gets 2 of 4 tests, predicted 13s") {
		t.Fatalf("stderr = %q; want the share and its predicted load", stderr)
	}

	data, err := os.ReadFile(filepath.Join(dir, "manifest-1.json"))
	if err != nil {
		t.Fatal(err)
	}

	m, err := manifest.DecodeManifest(data)
	if err != nil || m.Index != 1 || m.Machines != 2 || !slices.Equal(m.Tests, []string{"tests/b.py", "tests/c.py"}) {
		t.Fatalf("manifest = %+v, %v", m, err)
	}
}

func TestSplitWarnings(t *testing.T) {
	dominant := map[string]timing.Entry{
		"tests/a.py": samples(600_000),
		"tests/b.py": samples(1000),
		"tests/c.py": samples(1000),
		"tests/d.py": samples(1000),
	}

	tests := []struct {
		name    string
		timings map[string]timing.Entry
		stdin   string
		total   int
		want    []string
	}{
		{"no timing file", nil, suite, 2, []string{"no timings.json", "every test counts the same"}},
		{"timing file with no matching tests", map[string]timing.Entry{"tests/other.py": samples(4000)}, suite, 2, []string{"none of the 4 tests have a recorded time yet, so every test counts the same"}},
		{"one unknown test", map[string]timing.Entry{"tests/a.py": samples(4000), "tests/b.py": samples(4000), "tests/c.py": samples(4000)}, suite, 2, []string{"1 of 4 tests has no recorded time and counts as the median, 4s"}},
		{"unknown tests", map[string]timing.Entry{"tests/a.py": samples(4000)}, suite, 2, []string{"3 of 4 tests have no recorded time and count as the median, 4s"}},
		{"balanced suite", map[string]timing.Entry{
			"tests/a.py": samples(4000), "tests/b.py": samples(4000), "tests/c.py": samples(2000), "tests/d.py": samples(2000),
		}, suite, 2, []string{"predicted machine loads from 6s to 6s, average 6s; longest file tests/a.py at 4s"}},
		{"dominant file", dominant, suite, 2, []string{"longest file tests/a.py at 10m0s", "tests/a.py alone takes longer than the average machine load"}},
		{"empty shards", nil, "tests/a.py\n", 3, []string{"2 of 3 machines get no tests", "machine 2 has no tests"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.timings != nil {
				writeTimings(t, dir, timing.File{Run: 1, Tests: tc.timings})
			}

			last := strconv.Itoa(tc.total - 1)
			_, stderr := mustBunpo(t, tc.stdin, "split", "--dir", dir, "--index", last, "--total", strconv.Itoa(tc.total))

			for _, w := range tc.want {
				if !strings.Contains(stderr, w) {
					t.Fatalf("stderr = %q; want it to mention %q", stderr, w)
				}
			}
		})
	}
}

func TestPlanThenSplitFromPlan(t *testing.T) {
	planned, direct := t.TempDir(), t.TempDir()
	timings := timing.File{Run: 1, Tests: map[string]timing.Entry{
		"tests/a.py": samples(5),
		"tests/c.py": samples(9),
	}}
	writeTimings(t, planned, timings)
	writeTimings(t, direct, timings)

	mustBunpo(t, suite, "plan", "--dir", planned, "--total", "3")

	for i := range 3 {
		index := strconv.Itoa(i)
		fromPlan, _ := mustBunpo(t, "", "split", "--dir", planned, "--index", index, "--plan")
		fromList, _ := mustBunpo(t, suite, "split", "--dir", direct, "--index", index, "--total", "3")

		if fromPlan != fromList {
			t.Fatalf("machine %d: plan gives %q, direct split gives %q", i, fromPlan, fromList)
		}
	}

	// Both ways write the same digest, so verify accepts either.
	for _, dir := range []string{planned, direct} {
		if code, _, stderr := bunpo(t, suite, "verify", "--dir", dir); code != 0 {
			t.Fatalf("verify in %s exited %d: %s", dir, code, stderr)
		}
	}

	// split --plan must not create a missing directory before failing.
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if code, _, _ := bunpo(t, "", "split", "--dir", missing, "--index", "0", "--plan"); code != 2 {
		t.Fatalf("split --plan in missing dir exited %d; want 2", code)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("split --plan created missing directory %s", missing)
	}
}

func TestRecordRealReports(t *testing.T) {
	tests := []struct {
		runner string
		flags  []string
		files  []string
		fast   string
		fails  string
	}{
		{
			runner: "pytest",
			files:  []string{"tests/sub/test_fails.py", "tests/sub/test_skips.py", "tests/test_fast.py", "tests/test_slow.py"},
			fast:   "tests/test_fast.py",
			fails:  "tests/sub/test_fails.py",
		},
		{
			runner: "jest",
			files:  []string{"tests/fast.test.js", "tests/slow.test.js", "tests/sub/fails.test.js", "tests/sub/skips.test.js"},
			fast:   "tests/fast.test.js",
			fails:  "tests/sub/fails.test.js",
		},
		{
			runner: "vitest",
			files:  []string{"tests/fast.test.js", "tests/slow.test.js", "tests/sub/fails.test.js", "tests/sub/skips.test.js"},
			fast:   "tests/fast.test.js",
			fails:  "tests/sub/fails.test.js",
		},
		{
			runner: "playwright",
			flags:  []string{"--file-from", "classname", "--path-prefix", "tests"},
			files:  []string{"tests/fast.spec.js", "tests/slow.spec.js", "tests/sub/fails.spec.js", "tests/sub/skips.spec.js"},
			fast:   "tests/fast.spec.js",
			fails:  "tests/sub/fails.spec.js",
		},
		{
			runner: "rspec",
			files:  []string{"spec/fast_spec.rb", "spec/slow_spec.rb", "spec/sub/fails_spec.rb", "spec/sub/skips_spec.rb"},
			fast:   "spec/fast_spec.rb",
			fails:  "spec/sub/fails_spec.rb",
		},
	}

	for _, tc := range tests {
		t.Run(tc.runner, func(t *testing.T) {
			dir := t.TempDir()
			writeManifest(t, dir, manifest.Manifest{Index: 0, Machines: 1, Digest: digestA, Tests: tc.files})

			report := filepath.Join("..", "..", "internal", "junit", "testdata", tc.runner, "report.xml")
			args := slices.Concat([]string{"record", "--dir", dir, "--index", "0"}, tc.flags, []string{report})
			mustBunpo(t, "", args...)

			r := readRunReport(t, dir, 0)

			if len(r.Tests) != 4 {
				t.Fatalf("recorded %v; want the 4 sample files under their repository paths", r.Tests)
			}

			if o := r.Tests[tc.fails]; o.Timed {
				t.Fatalf("%s = %+v; a file with a failing case must record no time", tc.fails, o)
			}

			if o, ok := r.Tests[tc.fast]; !ok || !o.Timed {
				t.Fatalf("%s = %+v, %t; want a timed sample", tc.fast, o, ok)
			}
		})
	}
}

func TestRecordFailureKeepsEarlierTimes(t *testing.T) {
	dir := t.TempDir()
	writeTimings(t, dir, timing.File{Run: 1, Tests: map[string]timing.Entry{"a.py": samples(100, 110)}})
	writeManifest(t, dir, manifest.Manifest{Index: 0, Machines: 1, Digest: digestA, Tests: []string{"a.py", "b.py"}})

	report := filepath.Join(dir, "junit.xml")
	put(t, report, []byte(`<testsuite>
		<testcase file="a.py" time="0.5"/>
		<testcase file="a.py" time="0.1"><failure/></testcase>
		<testcase file="b.py" time="0.25"/>
	</testsuite>`))

	_, stderr := mustBunpo(t, "", "record", "--dir", dir, "--index", "0", report)
	if !strings.Contains(stderr, "recorded 2 files for machine 0; 1 had failures and keeps its earlier times") {
		t.Fatalf("stderr = %q", stderr)
	}

	mustBunpo(t, "", "merge", "--dir", dir)
	f := readTimingFile(t, dir)

	if a := f.Tests["a.py"]; !slices.Equal(a.Samples, []int64{100, 110}) || a.LastSeen != 2 {
		t.Fatalf("a.py = %+v; want its earlier samples kept and seen at run 2", a)
	}

	if b := f.Tests["b.py"]; !slices.Equal(b.Samples, []int64{250}) {
		t.Fatalf("b.py = %+v; want one 250 ms sample", b)
	}
}

func TestRecord(t *testing.T) {
	tests := []struct {
		name     string
		assigned []string
		noReport bool
		report   string
		format   string
		flags    []string
		wantCode int
		wantMsg  string
	}{
		{
			name:     "empty shard with no report",
			noReport: true,
			format:   "junit",
			wantCode: 0,
			wantMsg:  "recorded 0 files for machine 0",
		},
		{
			name:     "empty shard with invalid path prefix",
			noReport: true,
			format:   "junit",
			flags:    []string{"--path-prefix", "/abs"},
			wantCode: 2,
			wantMsg:  "invalid --path-prefix",
		},
		{
			name:     "empty shard that ran the whole suite",
			report:   "a.py 5\nb.py 5\n",
			format:   "lines",
			wantCode: 1,
			wantMsg:  "skip the test command on a machine with no tests",
		},
		{
			name:     "assigned tests but no report",
			assigned: []string{"a.py"},
			noReport: true,
			format:   "junit",
			wantCode: 2,
			wantMsg:  "no reports given",
		},
		{
			name:     "report without file paths",
			assigned: []string{"a.py"},
			report:   `<testsuite><testcase name="t" time="1"/></testsuite>`,
			format:   "junit",
			wantCode: 2,
			wantMsg:  "junit_family=xunit1",
		},
		{
			name:     "a file assigned elsewhere ran here",
			assigned: []string{"a.py"},
			report:   "a.py 5\nother.py 5\n",
			format:   "lines",
			wantCode: 1,
			wantMsg:  "1 file in the reports was not assigned to machine 0, so it may have run on two machines: other.py",
		},
		{
			name:     "nothing in the report matches",
			assigned: []string{"a.py"},
			report:   "x.py 5\n",
			format:   "lines",
			wantCode: 2,
			wantMsg:  "check --index, --file-from and --path-prefix",
		},
		{
			name:     "a failed file in lines format stays seen with no time",
			assigned: []string{"a.py", "b.py"},
			report:   "a.py 5\nb.py -\n",
			format:   "lines",
			wantCode: 0,
			wantMsg:  "recorded 2 files for machine 0; 1 had failures and keeps its earlier times",
		},
		{
			name:     "an assigned file is missing from the report",
			assigned: []string{"a.py", "b.py"},
			report:   "a.py 5\n",
			format:   "lines",
			wantCode: 0,
			wantMsg:  "1 assigned file has no test cases in the reports and stays unrecorded: b.py",
		},
		{
			name:     "an absolute path in the report",
			assigned: []string{"a.py"},
			report:   "/repo/a.py 5\n",
			format:   "lines",
			wantCode: 2,
			wantMsg:  "absolute",
		},
		{
			// "/a" must fail before the prefix turns it into "tests/a".
			name:     "an absolute path behind a prefix",
			assigned: []string{"tests/a"},
			report:   "/a 5\n",
			format:   "lines",
			flags:    []string{"--path-prefix", "tests"},
			wantCode: 2,
			wantMsg:  "absolute",
		},
		{
			// A runner that crashed before writing must not pass for an empty report.
			name:     "an empty report file",
			assigned: []string{"a.py"},
			report:   "",
			format:   "junit",
			wantCode: 2,
			wantMsg:  "no root element",
		},
		{
			name:     "a report that is not XML",
			assigned: []string{"a.py"},
			report:   "not XML",
			format:   "junit",
			wantCode: 2,
			wantMsg:  "outside the root",
		},
		{
			name:     "a failing case that names no file",
			assigned: []string{"a.py"},
			report:   `<testsuite><testcase file="a.py" time="1"/><testcase name="broken" time="1"><failure/></testcase></testsuite>`,
			format:   "junit",
			wantCode: 2,
			wantMsg:  `"broken"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeManifest(t, dir, manifest.Manifest{Index: 0, Machines: 1, Digest: digestA, Tests: tc.assigned})

			args := slices.Concat([]string{"record", "--dir", dir, "--index", "0", "--format", tc.format}, tc.flags)
			if !tc.noReport {
				path := filepath.Join(dir, "report.txt")
				put(t, path, []byte(tc.report))
				args = append(args, path)
			}

			code, _, stderr := bunpo(t, "", args...)
			if code != tc.wantCode || !strings.Contains(stderr, tc.wantMsg) {
				t.Fatalf("exit %d, stderr %q; want exit %d mentioning %q", code, stderr, tc.wantCode, tc.wantMsg)
			}
		})
	}
}

func TestRecordRefusesSymlinkedReport(t *testing.T) {
	dir, reports, elsewhere := t.TempDir(), t.TempDir(), t.TempDir()
	writeManifest(t, dir, manifest.Manifest{Index: 0, Machines: 1, Digest: digestA, Tests: []string{"a.py"}})

	target := filepath.Join(elsewhere, "times.txt")
	put(t, target, []byte("a.py 5\n"))

	link := filepath.Join(reports, "times.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("cannot create symlinks here: %v", err)
	}

	code, _, stderr := bunpo(t, "", "record", "--dir", dir, "--index", "0", "--format", "lines", link)
	if code != 2 {
		t.Fatalf("exit %d, stderr %q; want exit 2 for a report that leaves its directory", code, stderr)
	}
}

// failingWriter stands in for a standard output that is full or closed.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("no space left on device")
}

func TestSplitFailsWhenTheListCannotBeWritten(t *testing.T) {
	// The directory already holds a manifest from an earlier, successful
	// attempt, as on a machine that reuses its working directory.
	dir := t.TempDir()
	mustBunpo(t, suite, "split", "--dir", dir, "--index", "0", "--total", "1")

	var stderr bytes.Buffer
	code := run([]string{"split", "--dir", dir, "--index", "0", "--total", "1"}, strings.NewReader(suite), failingWriter{}, &stderr, noEnv)

	if code != 2 || !strings.Contains(stderr.String(), "writing the test list") {
		t.Fatalf("exit %d, stderr %q; want exit 2", code, &stderr)
	}

	// verify would accept a manifest whose list never reached the runner.
	if _, err := os.Stat(filepath.Join(dir, "manifest-0.json")); err == nil {
		t.Fatal("a manifest is left after the failed split")
	}
	if code, _, _ := bunpo(t, suite, "verify", "--dir", dir); code == 0 {
		t.Fatal("verify passed after the failed split")
	}
}

func TestPlanRemovesStalePlanOnFailure(t *testing.T) {
	dir := t.TempDir()
	mustBunpo(t, suite, "plan", "--dir", dir, "--total", "2")

	if code, _, _ := bunpo(t, "", "plan", "--dir", dir, "--total", "2"); code != 2 {
		t.Fatalf("plan with empty stdin exited %d; want 2", code)
	}

	if _, err := os.Stat(filepath.Join(dir, "plan.json")); err == nil {
		t.Fatal("stale plan.json remained after failed plan")
	}
}

func TestRecordRemovesStaleReportOnFailure(t *testing.T) {
	dir := t.TempDir()
	mustBunpo(t, suite, "split", "--dir", dir, "--index", "0", "--total", "1")

	good := filepath.Join(t.TempDir(), "good.txt")
	put(t, good, []byte("tests/a.py 10\ntests/b.py 10\ntests/c.py 10\ntests/d.py 10\n"))
	mustBunpo(t, "", "record", "--dir", dir, "--index", "0", "--format", "lines", good)

	bad := filepath.Join(t.TempDir(), "bad.xml")
	put(t, bad, []byte("not XML"))
	if code, _, _ := bunpo(t, "", "record", "--dir", dir, "--index", "0", bad); code != 2 {
		t.Fatalf("record with bad XML exited %d; want 2", code)
	}

	if _, err := os.Stat(filepath.Join(dir, "report-0.json")); err == nil {
		t.Fatal("stale report-0.json remained after failed record")
	}
	if code, _, _ := bunpo(t, "", "merge", "--dir", dir); code != 1 {
		t.Fatalf("merge after failed record exited %d; want 1", code)
	}
}

func TestFormatMS(t *testing.T) {
	tests := []struct {
		ms   int64
		want string
	}{
		{0, "0ms"},
		{850, "850ms"},
		{999, "999ms"},
		{1000, "1s"},
		{1499, "1s"},
		{1500, "2s"},
		{59_499, "59s"},
		{59_500, "1m0s"},
		{600_000, "10m0s"},
		{3_600_000, "1h0m0s"},
		{3_661_000, "1h1m1s"},
		// 4,000 files at MaxDurationMS exceeds time.Duration's ~292-year limit.
		{4000 * timing.MaxDurationMS, "2880000h0m0s"},
		{200_000 * timing.MaxDurationMS, "144000000h0m0s"},
	}

	for _, tc := range tests {
		if got := formatMS(tc.ms); got != tc.want {
			t.Errorf("formatMS(%d) = %q; want %q", tc.ms, got, tc.want)
		}
	}
}

// TestReusedDirectory reuses one directory across runs. A report from an
// earlier run must never stand in for the current one.
func TestReusedDirectory(t *testing.T) {
	times := filepath.Join(t.TempDir(), "times.txt")
	put(t, times, []byte("tests/a.py 19\ntests/b.py 5\ntests/c.py 5\ntests/d.py 5\n"))

	split := func(dir string) []string { return []string{"split", "--dir", dir, "--index", "0", "--total", "1"} }
	record := func(dir string) []string {
		return []string{"record", "--dir", dir, "--index", "0", "--format", "lines", times}
	}
	merge := func(dir string) []string { return []string{"merge", "--dir", dir} }

	type step func(string) []string

	tests := []struct {
		name      string
		firstRun  []step
		secondRun []step // ends just before the final merge
	}{
		{"merging the same run twice", []step{split, record, merge}, nil},
		{"a report from a skipped record step", []step{split, record}, []step{split}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, s := range tc.firstRun {
				mustBunpo(t, suite, s(dir)...)
			}

			before, _ := os.ReadFile(filepath.Join(dir, "timings.json"))

			for _, s := range tc.secondRun {
				mustBunpo(t, suite, s(dir)...)
			}

			code, _, stderr := bunpo(t, "", merge(dir)...)
			if code != 1 || !strings.Contains(stderr, "no run reports") {
				t.Fatalf("exit %d, stderr %q; want exit 1 for the missing report", code, stderr)
			}

			after, _ := os.ReadFile(filepath.Join(dir, "timings.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("the failed merge changed timings.json")
			}
		})
	}
}

func TestWriteFile(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	for _, data := range []string{"old", "new"} {
		if err := writeFile(root, "timings.json", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}

	if got, _ := os.ReadFile(filepath.Join(dir, "timings.json")); string(got) != "new" {
		t.Fatalf("timings.json = %q; want %q", got, "new")
	}

	// A destination that cannot be replaced fails, and the temporary file goes.
	put(t, filepath.Join(dir, "busy", "keep"), nil)
	if err := writeFile(root, "busy", []byte("x")); err == nil {
		t.Fatal("writeFile replaced a directory")
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		t.Fatalf("directory holds %d entries; want timings.json and busy, no temporary files", len(entries))
	}
}

func TestMergeRejects(t *testing.T) {
	report := func(index, machines int, tests timing.Report) []byte {
		data, err := manifest.EncodeRunReport(manifest.RunReport{Index: index, Machines: machines, Tests: tests})
		if err != nil {
			t.Fatal(err)
		}

		return data
	}

	tests := []struct {
		name    string
		reports map[int][]byte
		wantMsg string
	}{
		{
			name:    "no reports",
			wantMsg: "no run reports",
		},
		{
			name:    "a machine's report is missing",
			reports: map[int][]byte{0: report(0, 3, nil), 2: report(2, 3, nil)},
			wantMsg: "no run report from machine 1 of 3",
		},
		{
			name:    "multiple machines' reports are missing",
			reports: map[int][]byte{0: report(0, 3, nil)},
			wantMsg: "no run report from machines 1, 2 of 3",
		},
		{
			name: "a file in two reports",
			reports: map[int][]byte{
				0: report(0, 2, timing.Report{"a.py": {Timed: true, MS: 1}}),
				1: report(1, 2, timing.Report{"a.py": {Timed: true, MS: 2}}),
			},
			wantMsg: `"a.py" appears in reports`,
		},
		{
			name:    "machine counts disagree",
			reports: map[int][]byte{0: report(0, 2, nil), 1: report(1, 3, nil)},
			wantMsg: "disagree on the machine count",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for i, data := range tc.reports {
				put(t, filepath.Join(dir, reportName(i)), data)
			}

			code, _, stderr := bunpo(t, "", "merge", "--dir", dir)
			if code != 1 || !strings.Contains(stderr, tc.wantMsg) {
				t.Fatalf("exit %d, stderr %q; want exit 1 mentioning %q", code, stderr, tc.wantMsg)
			}

			if _, err := os.Stat(filepath.Join(dir, "timings.json")); err == nil {
				t.Fatal("merge wrote timings.json after a failed check")
			}
		})
	}
}

func TestVerify(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, manifest.Manifest{Index: 0, Machines: 2, Digest: digestA, Tests: []string{"tests/a.py", "tests/d.py"}})
	writeManifest(t, dir, manifest.Manifest{Index: 1, Machines: 2, Digest: digestA, Tests: []string{"tests/b.py", "tests/c.py"}})

	code, _, stderr := bunpo(t, suite, "verify", "--dir", dir)
	if code != 0 || !strings.Contains(stderr, "assignment verified: 4 tests across 2 machines") {
		t.Fatalf("exit %d, stderr %q; want the assignment verified", code, stderr)
	}

	code, _, stderr = bunpo(t, suite+"tests/e.py\n", "verify", "--dir", dir)
	if code != 1 || !strings.Contains(stderr, "1 test in the input but assigned to no machine: tests/e.py") {
		t.Fatalf("exit %d, stderr %q; want exit 1 naming the unassigned test", code, stderr)
	}
}

func TestExitCodes(t *testing.T) {
	dir, newer := t.TempDir(), t.TempDir()
	put(t, filepath.Join(newer, "timings.json"), []byte(`{"version":9}`))

	split := func(dir string, extra ...string) []string {
		return append([]string{"split", "--dir", dir}, extra...)
	}
	queueEnv := func(k string) string {
		switch k {
		case "BUNPO_QUEUE_TOKEN":
			return "write-secret-token-0123456789abcdef"
		case "BUNPO_QUEUE_READ_TOKEN":
			return "read-secret-token-fedcba9876543210"
		default:
			return ""
		}
	}

	tests := []struct {
		name  string
		stdin string
		args  []string
		env   func(string) string
		want  int
	}{
		{"no command", "", nil, nil, 2},
		{"help", "", []string{"help"}, nil, 0},
		{"command help", "", []string{"split", "-h"}, nil, 0},
		{"version", "", []string{"version"}, nil, 0},
		{"version flag", "", []string{"-version"}, nil, 0},
		{"version long flag", "", []string{"--version"}, nil, 0},
		{"unknown command", "", []string{"shuffle"}, nil, 2},
		{"unknown flag", "", []string{"split", "--machines", "2"}, nil, 2},
		{"missing index", suite, split(dir, "--total", "2"), nil, 2},
		{"index out of range", suite, split(dir, "--index", "2", "--total", "2"), nil, 2},
		{"invalid identifier", "/abs/a.py\n", split(dir, "--index", "0", "--total", "2"), nil, 2},
		{"colliding spellings", "a.py\n./a.py\n", split(dir, "--index", "0", "--total", "2"), nil, 2},
		// A lone CR is not a line ending, so it stays in the identifier.
		{"carriage return without a newline", "a.py\r", split(dir, "--index", "0", "--total", "2"), nil, 2},
		// A discovery command that found nothing must not pass with no tests run.
		{"empty test list to split", "", split(dir, "--index", "0", "--total", "2"), nil, 2},
		{"blank test list to plan", "\n\n", []string{"plan", "--dir", dir, "--total", "2"}, nil, 2},
		{"empty test list to verify", "", []string{"verify", "--dir", dir}, nil, 2},
		{"split from a missing plan", "", split(dir, "--index", "0", "--plan"), nil, 2},
		{"timing file from a newer release", suite, split(newer, "--index", "0", "--total", "1"), nil, 2},
		{"merge in a missing directory", "", []string{"merge", "--dir", filepath.Join(dir, "nope")}, nil, 2},
		// Live-queue (--queue) usage errors.
		{"queue help", "", []string{"--queue", "-h"}, nil, 0},
		{"queue missing build", suite, []string{"--queue", "http://127.0.0.1:8080", "--total", "1", "--report", "r.xml", "--", "true"}, queueEnv, 2},
		{"queue invalid build", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "../bad", "--total", "1", "--report", "r.xml", "--", "true"}, queueEnv, 2},
		{"queue missing token", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--total", "1", "--report", "r.xml", "--", "true"}, noEnv, 2},
		{"queue status missing token", "", []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--status"}, noEnv, 2},
		{"queue plain http to remote host", suite, []string{"--queue", "http://queue.example.com", "--build", "b1", "--total", "1", "--report", "r.xml", "--", "true"}, queueEnv, 2},
		{"queue missing report", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--total", "1", "--", "true"}, queueEnv, 2},
		{"queue bad format", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--total", "1", "--report", "r.xml", "--format", "yaml", "--", "true"}, queueEnv, 2},
		{"queue total 0", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--total", "0", "--report", "r.xml", "--", "true"}, queueEnv, 2},
		{"queue index out of range", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--total", "2", "--index", "256", "--report", "r.xml", "--", "true"}, queueEnv, 2},
		{"queue lease-ttl 0", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--total", "1", "--report", "r.xml", "--lease-ttl", "0s", "--", "true"}, queueEnv, 2},
		{"queue no command after --", suite, []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--total", "1", "--report", "r.xml", "--"}, queueEnv, 2},
		{"queue extra argument with --status", "", []string{"--queue", "http://127.0.0.1:8080", "--build", "b1", "--status", "extra"}, queueEnv, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getenv := tc.env
			if getenv == nil {
				getenv = noEnv
			}
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, strings.NewReader(tc.stdin), &stdout, &stderr, getenv); code != tc.want {
				t.Fatalf("exit %d; want %d\n%s", code, tc.want, stderr.String())
			}
		})
	}
}
