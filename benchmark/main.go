// Command benchmark measures how well Bunpō balances a test suite. It builds
// synthetic pytest suites whose files sleep for known durations, runs them
// through the bunpo binary on parallel "machines" for several runs, and
// writes predicted loads and measured times as Markdown.
//
//	go build -o bin/bunpo ./cmd/bunpo
//	go run ./benchmark -bunpo bin/bunpo -out docs/benchmark.md
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/timing"
)

// suite is one benchmark case: file durations in milliseconds, split across
// machines.
type suite struct {
	name     string
	about    string
	machines int
	files    []int64
}

var suites = []suite{
	{
		name:     "Many files",
		about:    "40 files from 100 to 900 ms on four machines, the shape of a typical suite. This is the declared benchmark for the 1.10 target.",
		machines: 4,
		files:    manyFiles(40),
	},

	{
		name:     "Tight",
		about:    "13 files that divide exactly into four machines of 3000 ms each, for example {1500, 900, 600}, {1400, 1000, 600}, {1300, 1200, 500} and {1100, 800, 700, 400}. Longest-first dealing misses every perfect split; finding one needs an exact search, which Bunpō does not run.",
		machines: 4,
		files:    []int64{1500, 900, 600, 1400, 1000, 600, 1300, 1200, 500, 1100, 800, 700, 400},
	},

	{
		name:     "Indivisible",
		about:    "One 6000 ms file and twelve 500 ms files on four machines. No file-level split can bring the slowest machine below 6000 ms, twice the 3000 ms average.",
		machines: 4,
		files:    []int64{6000, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500, 500},
	},
}

// manyFiles returns n durations from 100 to 900 ms in a fixed, uneven order.
func manyFiles(n int) []int64 {
	files := make([]int64, n)
	for i := range files {
		files[i] = 100 + int64(i*37%9)*100
	}

	return files
}

// machineRun is one machine's result in one run.
type machineRun struct {
	files     int
	predicted int64 // ms, from the timing file; 0 without history
	tests     int64 // ms, the case times pytest reported
	wall      time.Duration
}

func main() {
	bunpo := flag.String("bunpo", "bin/bunpo", "path to the bunpo binary")
	pytest := flag.String("pytest", "pytest", "path to pytest")
	runs := flag.Int("runs", 3, "runs per suite; the first has no history")
	out := flag.String("out", "", "Markdown file to write (default: standard output)")
	flag.Parse()

	if *runs < 1 {
		fail(fmt.Errorf("-runs must be at least 1, got %d", *runs))
	}

	b, err := filepath.Abs(*bunpo)
	if err != nil {
		fail(err)
	}

	work, err := os.MkdirTemp("", "bunpo-benchmark")
	if err != nil {
		fail(err)
	}
	defer os.RemoveAll(work)

	var report bytes.Buffer
	setup, err := measureSetup(work, *pytest)
	if err != nil {
		fail(err)
	}

	writeHeader(&report, *runs, setup)

	for _, s := range suites {
		results, err := runSuite(filepath.Join(work, s.name), s, b, *pytest, *runs)
		if err != nil {
			fail(fmt.Errorf("%s: %w", s.name, err))
		}

		writeSuite(&report, s, results)
	}

	if *out == "" {
		os.Stdout.Write(report.Bytes())
		return
	}

	if err := os.WriteFile(*out, report.Bytes(), 0o644); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "benchmark:", err)
	os.Exit(1)
}

// runSuite writes the suite, then for each run splits it, runs every
// machine's share at the same time, records, verifies and merges.
func runSuite(dir string, s suite, bunpo, pytest string, runs int) ([][]machineRun, error) {
	list, err := writeSuiteFiles(dir, s.files)
	if err != nil {
		return nil, err
	}

	final := filepath.Join(dir, "final", ".bunpo")
	if err := os.MkdirAll(final, 0o755); err != nil {
		return nil, err
	}

	var results [][]machineRun
	for run := range runs {
		var timings []byte
		if run > 0 {
			timings, err = os.ReadFile(filepath.Join(final, "timings.json"))
			if err != nil {
				return nil, fmt.Errorf("run %d: %w", run+1, err)
			}
		}

		machines := make([]machineRun, s.machines)
		errs := make([]error, s.machines)

		var wg sync.WaitGroup
		for i := range s.machines {
			wg.Go(func() {
				machines[i], errs[i] = runMachine(dir, run, i, s.machines, list, timings, bunpo, pytest)
			})
		}

		wg.Wait()

		for i, err := range errs {
			if err != nil {
				return nil, fmt.Errorf("run %d, machine %d: %w", run+1, i, err)
			}

			for _, name := range []string{fmt.Sprintf("manifest-%d.json", i), fmt.Sprintf("report-%d.json", i)} {
				if err := copyFile(filepath.Join(machineDir(dir, run, i), name), filepath.Join(final, name)); err != nil {
					return nil, err
				}
			}
		}

		for _, args := range [][]string{{"verify"}, {"merge"}} {
			if _, err := command(filepath.Dir(final), list, bunpo, args...); err != nil {
				return nil, err
			}
		}

		results = append(results, machines)
	}

	return results, nil
}

func runMachine(dir string, run, index, total int, list string, timings []byte, bunpo, pytest string) (machineRun, error) {
	state := machineDir(dir, run, index)
	if err := os.MkdirAll(state, 0o755); err != nil {
		return machineRun{}, err
	}

	if timings != nil {
		if err := os.WriteFile(filepath.Join(state, "timings.json"), timings, 0o644); err != nil {
			return machineRun{}, err
		}
	}

	idx := fmt.Sprint(index)
	share, err := command(dir, list, bunpo, "split", "--dir", state, "--index", idx, "--total", fmt.Sprint(total))
	if err != nil {
		return machineRun{}, err
	}

	files := strings.Fields(share)
	pred, err := predicted(timings, files)
	if err != nil {
		return machineRun{}, err
	}

	result := machineRun{files: len(files), predicted: pred}

	if len(files) == 0 {
		_, err := command(dir, "", bunpo, "record", "--dir", state, "--index", idx)
		return result, err
	}

	report := filepath.Join(state, "junit.xml")
	start := time.Now()

	if _, err := command(dir, "", pytest, slices.Concat(
		[]string{"-q", "-p", "no:cacheprovider", "-o", "junit_family=xunit1", "--junitxml=" + report, "--"},
		files,
	)...); err != nil {
		return machineRun{}, err
	}

	result.wall = time.Since(start)

	if _, err := command(dir, "", bunpo, "record", "--dir", state, "--index", idx, report); err != nil {
		return machineRun{}, err
	}

	result.tests, err = recordedMS(filepath.Join(state, fmt.Sprintf("report-%d.json", index)))
	return result, err
}

// predicted sums the median durations the timing file holds for files.
func predicted(timings []byte, files []string) (int64, error) {
	if timings == nil {
		return 0, nil
	}

	f, err := timing.Decode(timings)
	if err != nil {
		return 0, err
	}

	durations := f.Durations()
	var sum int64
	for _, id := range files {
		sum += durations[id]
	}

	return sum, nil
}

func recordedMS(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	r, err := manifest.DecodeRunReport(data)
	if err != nil {
		return 0, err
	}

	var sum int64
	for _, o := range r.Tests {
		sum += o.MS
	}

	return sum, nil
}

// measureSetup times pytest on one test that does nothing, which is the
// fixed cost every machine pays on top of its tests.
func measureSetup(work, pytest string) (time.Duration, error) {
	dir := filepath.Join(work, "setup")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}

	if err := os.WriteFile(filepath.Join(dir, "test_nothing.py"), []byte("def test_nothing():\n    pass\n"), 0o644); err != nil {
		return 0, err
	}

	var times []time.Duration
	for range 3 {
		start := time.Now()

		if _, err := command(dir, "", pytest, "-q", "-p", "no:cacheprovider", "test_nothing.py"); err != nil {
			return 0, err
		}

		times = append(times, time.Since(start))
	}

	slices.Sort(times)
	return times[1], nil
}

func writeSuiteFiles(dir string, durations []int64) (string, error) {
	tests := filepath.Join(dir, "tests")
	if err := os.MkdirAll(tests, 0o755); err != nil {
		return "", err
	}

	var list strings.Builder
	for i, ms := range durations {
		name := fmt.Sprintf("test_f%02d.py", i)
		body := fmt.Sprintf("import time\n\n\ndef test_wait():\n    time.sleep(%d / 1000)\n", ms)
		if err := os.WriteFile(filepath.Join(tests, name), []byte(body), 0o644); err != nil {
			return "", err
		}

		fmt.Fprintf(&list, "tests/%s\n", name)
	}

	return list.String(), nil
}

func machineDir(dir string, run, index int) string {
	return filepath.Join(dir, fmt.Sprintf("run-%d", run+1), fmt.Sprintf("machine-%d", index), ".bunpo")
}

// command runs name in dir with stdin and returns its standard output. A
// test failure fails the benchmark: every test here only sleeps.
func command(dir, stdin, name string, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s: %w\n%s", filepath.Base(name), strings.Join(args, " "), err, &stderr)
	}

	return stdout.String(), nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	return os.WriteFile(dst, data, 0o644)
}

func writeHeader(w io.Writer, runs int, setup time.Duration) {
	fmt.Fprintf(w, `# Benchmark

Generated by `+"`go run ./benchmark`"+` on %s, %s/%s, %d CPUs. Each suite runs
%d times on parallel machines: real pytest processes, one per machine, driven
by the real `+"`bunpo`"+` binary. The first run has no timing history, so files are
dealt out evenly; later runs use the merged timings.

- **Predicted** is the sum of the recorded medians for a machine's files.
- **Tests** is the time pytest reported for those files.
- **Wall** is the whole pytest process, including its setup. On this machine,
  pytest with one empty test takes %s, the fixed cost on every machine.

The ratio is slowest over fastest among machines with files. PROJECT.md
requires a predicted ratio of at most 1.10 on its declared benchmark, here
the Many files suite. It is a target for that benchmark, not a promise for
every suite: the Tight suite below shows where longest-first dealing falls
short.
`, time.Now().Format("2006-01-02"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runs, ms(setup))
}

func writeSuite(w io.Writer, s suite, results [][]machineRun) {
	fmt.Fprintf(w, "\n## %s\n\n%s\n\n", s.name, s.about)
	fmt.Fprintln(w, "| Run | Machine | Files | Predicted | Tests | Wall |")
	fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- |")

	for run, machines := range results {
		for i, m := range machines {
			pred := "none yet"
			if run > 0 {
				pred = fmt.Sprintf("%d ms", m.predicted)
			}

			fmt.Fprintf(w, "| %d | %d | %d | %s | %d ms | %s |\n", run+1, i, m.files, pred, m.tests, ms(m.wall))
		}
	}

	fmt.Fprintln(w, "\n| Run | Predicted ratio | Tests ratio | Wall ratio | Slowest wall |")
	fmt.Fprintln(w, "| --- | --- | --- | --- | --- |")

	for run, machines := range results {
		pred := "none yet"
		if run > 0 {
			pred = ratio(machines, func(m machineRun) float64 { return float64(m.predicted) })
		}

		tests := ratio(machines, func(m machineRun) float64 { return float64(m.tests) })
		wall := ratio(machines, func(m machineRun) float64 { return m.wall.Seconds() })

		var slowest time.Duration
		for _, m := range machines {
			slowest = max(slowest, m.wall)
		}

		fmt.Fprintf(w, "| %d | %s | %s | %s | %s |\n", run+1, pred, tests, wall, ms(slowest))
	}
}

// ratio is slowest over fastest among machines with files.
func ratio(machines []machineRun, value func(machineRun) float64) string {
	var values []float64
	for _, m := range machines {
		if m.files > 0 {
			values = append(values, value(m))
		}
	}

	if len(values) == 0 || slices.Min(values) == 0 {
		return "n/a"
	}

	return fmt.Sprintf("%.3f", slices.Max(values)/slices.Min(values))
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%d ms", d.Milliseconds())
}
