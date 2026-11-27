package main

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/timing"
)

// binary is built once and run as separate processes, the way CI machines
// run it.
var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "bunpo-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	binary = filepath.Join(dir, "bunpo")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}

	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building bunpo: %v\n%s", err, out)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type process struct {
	cmd            *exec.Cmd
	stdout, stderr bytes.Buffer
}

func start(t *testing.T, stdin string, args ...string) *process {
	t.Helper()

	p := &process{cmd: exec.Command(binary, args...)}
	p.cmd.Stdin = strings.NewReader(stdin)
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr

	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}

	return p
}

// wait returns the exit status, and fails the test on anything else, such as
// a crash.
func (p *process) wait(t *testing.T) int {
	t.Helper()

	if err := p.cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			t.Fatal(err)
		}
	}

	return p.cmd.ProcessState.ExitCode()
}

func exe(t *testing.T, stdin string, args ...string) (int, string, string) {
	t.Helper()

	p := start(t, stdin, args...)
	code := p.wait(t)
	return code, p.stdout.String(), p.stderr.String()
}

func copyInto(t *testing.T, dst, src string) {
	t.Helper()

	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}

	put(t, filepath.Join(dst, filepath.Base(src)), data)
}

func machineDir(work string, i int) string {
	return filepath.Join(work, fmt.Sprintf("machine-%d", i))
}

// splitAll runs every machine's split at the same time, each in its own
// directory, and returns each machine's share and messages.
func splitAll(t *testing.T, work, list string, machines int, timingsFor func(i int) *timing.File) ([][]string, []string) {
	t.Helper()

	procs := make([]*process, machines)
	for i := range machines {
		dir := machineDir(work, i)
		if f := timingsFor(i); f != nil {
			writeTimings(t, dir, *f)
		}

		procs[i] = start(t, list, "split", "--dir", dir, "--index", fmt.Sprint(i), "--total", fmt.Sprint(machines))
	}

	shares := make([][]string, machines)
	stderrs := make([]string, machines)
	for i, p := range procs {
		if code := p.wait(t); code != 0 {
			t.Fatalf("machine %d: split exited %d: %s", i, code, &p.stderr)
		}

		shares[i] = strings.Fields(p.stdout.String())
		stderrs[i] = p.stderr.String()
	}

	return shares, stderrs
}

// recordShare writes a two-column report for a machine's share and records
// it. An empty share records with no report.
func recordShare(t *testing.T, dir string, index int, share []string, ms func(id string) int) {
	t.Helper()

	args := []string{"record", "--dir", dir, "--index", fmt.Sprint(index), "--format", "lines"}

	if len(share) > 0 {
		var report strings.Builder
		for _, id := range share {
			fmt.Fprintf(&report, "%s %d\n", id, ms(id))
		}

		path := filepath.Join(dir, "times.txt")
		put(t, path, []byte(report.String()))
		args = append(args, path)
	}

	if code, _, stderr := exe(t, "", args...); code != 0 {
		t.Fatalf("machine %d: record exited %d: %s", index, code, stderr)
	}
}

func lines(ids []string) string {
	return strings.Join(ids, "\n") + "\n"
}

func TestEndToEndEightMachines(t *testing.T) {
	var ids []string
	durations := map[string]timing.Entry{}
	for i := range 40 {
		id := fmt.Sprintf("tests/test_%02d.py", i)
		ids = append(ids, id)

		if i%7 != 0 {
			durations[id] = samples(int64((i*37)%50+1) * 100)
		}
	}

	fresh := timing.File{Run: 1, Tests: durations}

	stale := timing.File{Run: 1, Tests: map[string]timing.Entry{}}
	maps.Copy(stale.Tests, durations)
	stale.Tests["tests/test_01.py"] = samples(99_000)

	tests := []struct {
		name     string
		staleOn  int // -1 for none
		wantCode int
		wantMsg  string
	}{
		{"clean run", -1, 0, "assignment verified: 40 tests across 8 machines"},
		{"one machine restored a stale timing file", 5, 1, "machine 5 split a different input or used different timings than the other machines"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			work := t.TempDir()
			shares, _ := splitAll(t, work, lines(ids), 8, func(i int) *timing.File {
				if i == tc.staleOn {
					return &stale
				}

				return &fresh
			})

			final := filepath.Join(work, "final")
			for i := range 8 {
				copyInto(t, final, filepath.Join(machineDir(work, i), manifestName(i)))
			}

			code, _, stderr := exe(t, lines(ids), "verify", "--dir", final)
			if code != tc.wantCode || !strings.Contains(stderr, tc.wantMsg) {
				t.Fatalf("verify exited %d: %s\nwant exit %d mentioning %q", code, stderr, tc.wantCode, tc.wantMsg)
			}

			if tc.staleOn < 0 {
				all := slices.Concat(shares...)
				slices.Sort(all)

				if !slices.Equal(all, ids) {
					t.Fatalf("the shares together are %q; want every test exactly once", all)
				}
			}
		})
	}
}

// TestEndToEndSkippedRunner: every machine split, but one runner was skipped
// and uploaded no report. verify passes and merge fails. The suite also has a
// dominant file and empty shards.
func TestEndToEndSkippedRunner(t *testing.T) {
	ids := []string{"tests/huge.py", "tests/a.py", "tests/b.py", "tests/c.py", "tests/d.py", "tests/e.py"}
	timings := timing.File{Run: 1, Tests: map[string]timing.Entry{
		"tests/huge.py": samples(600_000),
		"tests/a.py":    samples(2000),
		"tests/b.py":    samples(1500),
		"tests/c.py":    samples(1200),
		"tests/d.py":    samples(900),
		"tests/e.py":    samples(700),
	}}
	const machines, skipped = 8, 3

	work := t.TempDir()
	shares, stderrs := splitAll(t, work, lines(ids), machines, func(int) *timing.File { return &timings })

	for _, w := range []string{"tests/huge.py alone takes longer than the average machine load", "2 of 8 machines get no tests"} {
		if !strings.Contains(stderrs[0], w) {
			t.Fatalf("split stderr = %q; want a warning mentioning %q", stderrs[0], w)
		}
	}

	final := filepath.Join(work, "final")
	for i := range machines {
		dir := machineDir(work, i)
		copyInto(t, final, filepath.Join(dir, manifestName(i)))

		if i == skipped {
			continue
		}

		recordShare(t, dir, i, shares[i], func(string) int { return 1000 })
		copyInto(t, final, filepath.Join(dir, reportName(i)))
	}

	code, _, stderr := exe(t, lines(ids), "verify", "--dir", final)
	if code != 0 || !strings.Contains(stderr, "assignment verified") {
		t.Fatalf("verify exited %d: %s; want the assignment verified", code, stderr)
	}

	code, _, stderr = exe(t, "", "merge", "--dir", final)
	want := fmt.Sprintf("no run report from machine %d of %d", skipped, machines)
	if code != 1 || !strings.Contains(stderr, want) {
		t.Fatalf("merge exited %d: %s; want exit 1 mentioning %q", code, stderr, want)
	}
}

func TestEndToEndTimingsCarryOver(t *testing.T) {
	ids := []string{"tests/a.py", "tests/b.py", "tests/c.py", "tests/d.py"}
	times := map[string]int{"tests/a.py": 9000, "tests/b.py": 100, "tests/c.py": 100, "tests/d.py": 8000}

	work := t.TempDir()
	shares, stderrs := splitAll(t, work, lines(ids), 2, func(int) *timing.File { return nil })
	if !strings.Contains(stderrs[0], "every test counts the same") {
		t.Fatalf("first split stderr = %q; want the no-history note", stderrs[0])
	}

	final := filepath.Join(work, "final")
	for i := range 2 {
		dir := machineDir(work, i)
		recordShare(t, dir, i, shares[i], func(id string) int { return times[id] })
		copyInto(t, final, filepath.Join(dir, reportName(i)))
	}

	if code, _, stderr := exe(t, "", "merge", "--dir", final); code != 0 {
		t.Fatalf("merge exited %d: %s", code, stderr)
	}

	// With the merged times, the 9 s file gets machine 0 to itself.
	next := filepath.Join(work, "next")
	copyInto(t, next, filepath.Join(final, "timings.json"))

	code, stdout, stderr := exe(t, lines(ids), "split", "--dir", next, "--index", "0", "--total", "2")
	if code != 0 || strings.Contains(stderr, "every test counts the same") {
		t.Fatalf("second split exited %d: %s; want it to use the recorded times", code, stderr)
	}

	if got := strings.Fields(stdout); !slices.Equal(got, []string{"tests/a.py"}) {
		t.Fatalf("machine 0 gets %q; want only the 9 s file", got)
	}
}
