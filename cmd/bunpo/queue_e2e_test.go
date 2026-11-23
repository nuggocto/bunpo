package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/queuerunner"
	"github.com/nuggocto/bunpo/internal/timing"
)

const (
	e2eWriteTok = "e2e-write-token-0123456789abcdef"
	e2eReadTok  = "e2e-read-token-fedcba9876543210"
)

// TestQueueHelperProcess is invoked as a child test runner by real `bunpo --queue`
// processes in the E2E tests below. It does nothing during ordinary `go test`.
func TestQueueHelperProcess(t *testing.T) {
	mode := os.Getenv("BUNPO_E2E_HELPER")
	if mode == "" {
		return
	}

	// Verify that bunpo stripped queue tokens from the child environment.
	if os.Getenv("BUNPO_QUEUE_TOKEN") != "" || os.Getenv("BUNPO_QUEUE_READ_TOKEN") != "" {
		fmt.Fprintln(os.Stderr, "helper saw leaked queue token in environment")
		os.Exit(99)
	}

	batchFile := os.Getenv("BUNPO_TESTS_FILE")
	raw, err := os.ReadFile(batchFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper reading BUNPO_TESTS_FILE: %v\n", err)
		os.Exit(98)
	}
	var batch []string
	for line := range strings.Lines(string(raw)) {
		line = strings.TrimSpace(line)
		if line != "" {
			batch = append(batch, line)
		}
	}

	reportPath := os.Getenv("BUNPO_E2E_REPORT")
	markerDir := os.Getenv("BUNPO_E2E_MARKER_DIR")
	runnerTag := os.Getenv("BUNPO_E2E_RUNNER_TAG")

	writeMarker := func(name string) {
		if markerDir != "" {
			_ = os.WriteFile(filepath.Join(markerDir, name), []byte(fmt.Sprintf("%d", os.Getpid())), 0o644)
		}
	}

	writeJUnit := func(cases []string) {
		var b strings.Builder
		b.WriteString(`<testsuites><testsuite name="e2e">`)
		for _, c := range cases {
			b.WriteString(c)
		}
		b.WriteString(`</testsuite></testsuites>`)
		if err := os.WriteFile(reportPath, []byte(b.String()), 0o644); err != nil {
			os.Exit(97)
		}
	}

	switch mode {
	case "pass":
		var cases []string
		for _, f := range batch {
			cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.120"/>`, f))
		}
		writeJUnit(cases)
		os.Exit(0)

	case "kill-before-exec":
		if runnerTag == "victim" {
			writeMarker("leased-before-exec")
			time.Sleep(30 * time.Second)
		}
		var cases []string
		for _, f := range batch {
			cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.100"/>`, f))
		}
		writeJUnit(cases)
		os.Exit(0)

	case "kill-after-exec-before-report":
		var cases []string
		for _, f := range batch {
			cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.110"/>`, f))
		}
		writeJUnit(cases)
		if runnerTag == "victim" {
			// Execution finished and report is on disk; pause so the test kills bunpo before it reads/reports.
			writeMarker("executed-before-report")
			time.Sleep(30 * time.Second)
		}
		os.Exit(0)

	case "pause-stale":
		var cases []string
		for _, f := range batch {
			cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.090"/>`, f))
		}
		writeJUnit(cases)
		if runnerTag == "paused" {
			writeMarker("ready-to-stop")
			// Wait for the test to confirm bunpo is stopped via SIGSTOP before exiting,
			// so bunpo cannot read the report until SIGCONT arrives.
			release := filepath.Join(markerDir, "release-paused")
			for {
				if _, err := os.Stat(release); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		os.Exit(0)

	case "poison-file":
		for _, f := range batch {
			if f == "tests/poison.py" {
				// Simulate a test file that crashes the entire runner without writing a report.
				os.Exit(137)
			}
		}
		var cases []string
		for _, f := range batch {
			cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.050"/>`, f))
		}
		writeJUnit(cases)
		os.Exit(0)

	case "fail-sibling":
		anyFail := false
		var cases []string
		for _, f := range batch {
			if f == "tests/bad.py" {
				anyFail = true
				cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="boom" time="0.050"><failure message="boom"/></testcase>`, f))
			} else {
				cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.250"/>`, f))
			}
		}
		writeJUnit(cases)
		if anyFail {
			os.Exit(1)
		}
		os.Exit(0)

	case "playwright-all-skipped-nonzero":
		var cases []string
		for _, f := range batch {
			cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="skipped" time="0"><skipped/></testcase>`, f))
		}
		writeJUnit(cases)
		os.Exit(1)

	case "missing-report":
		// Write nothing and exit 0; missing report must be treated as error, never passed.
		os.Exit(0)

	case "extra-file":
		var cases []string
		for _, f := range batch {
			cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.050"/>`, f))
		}
		cases = append(cases, `<testcase file="tests/unassigned_decoy.py" name="decoy" time="0.050"/>`)
		writeJUnit(cases)
		os.Exit(0)

	case "pass-and-skip":
		var cases []string
		for _, f := range batch {
			if f == "tests/skip.py" {
				cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="skip" time="0"><skipped/></testcase>`, f))
			} else {
				cases = append(cases, fmt.Sprintf(`<testcase file="%s" name="ok" time="0.300"/>`, f))
			}
		}
		writeJUnit(cases)
		os.Exit(0)

	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		os.Exit(96)
	}
}

func startInProcessQueueServer(t *testing.T, minTTLMS int64) (*httptest.Server, *queueapi.Client) {
	t.Helper()
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = minTTLMS
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: e2eWriteTok,
		ReadToken:  e2eReadTok,
		Limits:     lim,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	rc, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:    srv.URL,
		Token:      e2eReadTok,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, rc
}

func startQueueProcess(t *testing.T, stdin string, env []string, args ...string) *process {
	t.Helper()
	p := &process{cmd: exec.Command(binary, args...)}
	p.cmd.Stdin = strings.NewReader(stdin)
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr
	p.cmd.Env = append(os.Environ(), env...)
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return p
}

func testSelfBinary(t *testing.T) string {
	t.Helper()
	exePath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exePath
}

func waitForMarkerPID(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && len(bytes.TrimSpace(data)) > 0 {
			var pid int
			if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for marker file %s", path)
	return 0
}

func killPID(pid int) {
	if pid <= 0 {
		return
	}
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}

// assertBuildIntegrity verifies the core correctness guarantee: every logical
// test has either one accepted terminal result or is unresolved, and verdict is
// never passed when any test did not complete as passed/skipped.
func assertBuildIntegrity(t *testing.T, rc *queueapi.Client, buildID string, wantVerdict queue.Verdict, wantTests int) queueapi.StatusResponse {
	t.Helper()
	ctx := context.Background()
	st, err := rc.Status(ctx, buildID)
	if err != nil {
		t.Fatalf("status(%s): %v", buildID, err)
	}
	if st.Verdict != wantVerdict {
		t.Fatalf("build %s verdict = %s; want %s (counts=%+v conflicts=%+v)", buildID, st.Verdict, wantVerdict, st.Counts, st.Conflicts)
	}
	if st.Counts.Total != wantTests || st.Counts.Pending != 0 || st.Counts.Leased != 0 {
		t.Fatalf("build %s counts = %+v; want %d total and 0 pending/leased", buildID, st.Counts, wantTests)
	}

	page, err := rc.ListTests(ctx, buildID, queue.ListTestsCommand{Limit: 1000})
	if err != nil {
		t.Fatalf("list tests(%s): %v", buildID, err)
	}
	if len(page.Tests) != wantTests {
		t.Fatalf("listed %d tests; want %d", len(page.Tests), wantTests)
	}
	for _, tv := range page.Tests {
		accepted := 0
		for _, a := range tv.Attempts {
			if a.Status == queue.AttemptPassed || a.Status == queue.AttemptFailed || a.Status == queue.AttemptSkipped {
				accepted++
			}
		}
		if accepted > 1 {
			t.Fatalf("test %s accepted %d times; want <= 1", tv.Test, accepted)
		}
		if tv.State == queue.StatusUnresolved {
			if st.Verdict == queue.VerdictPassed {
				t.Fatalf("build passed despite unresolved test %s", tv.Test)
			}
		} else if accepted != 1 {
			t.Fatalf("test %s in state %s has %d accepted attempts; want 1", tv.Test, tv.State, accepted)
		}
	}
	return st
}

func TestQueueE2EKillBeforeExec(t *testing.T) {
	srv, rc := startInProcessQueueServer(t, 200)
	work := t.TempDir()
	markers := filepath.Join(work, "markers")
	_ = os.MkdirAll(markers, 0o755)
	self := testSelfBinary(t)
	list := "tests/a.py\ntests/b.py\n"

	rep0 := filepath.Join(work, "r0.xml")
	p0 := startQueueProcess(t, list,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=kill-before-exec",
			"BUNPO_E2E_REPORT=" + rep0,
			"BUNPO_E2E_MARKER_DIR=" + markers,
			"BUNPO_E2E_RUNNER_TAG=victim",
		},
		"--queue", srv.URL, "--build", "e2e-kill-before", "--index", "0", "--total", "2",
		"--dir", filepath.Join(work, "d0"), "--report", rep0, "--lease-ttl", "300ms", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)

	childPID := waitForMarkerPID(t, filepath.Join(markers, "leased-before-exec"), 5*time.Second)
	_ = p0.cmd.Process.Kill()
	killPID(childPID)
	_ = p0.cmd.Wait()

	rep1 := filepath.Join(work, "r1.xml")
	p1 := startQueueProcess(t, list,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=kill-before-exec",
			"BUNPO_E2E_REPORT=" + rep1,
			"BUNPO_E2E_RUNNER_TAG=survivor",
		},
		"--queue", srv.URL, "--build", "e2e-kill-before", "--index", "1", "--total", "2",
		"--dir", filepath.Join(work, "d1"), "--report", rep1, "--lease-ttl", "300ms", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := p1.wait(t); code != 0 {
		t.Fatalf("survivor exited %d: %s", code, p1.stderr.String())
	}
	assertBuildIntegrity(t, rc, "e2e-kill-before", queue.VerdictPassed, 2)
}

func TestQueueE2EKillAfterExecBeforeReport(t *testing.T) {
	srv, rc := startInProcessQueueServer(t, 200)
	work := t.TempDir()
	markers := filepath.Join(work, "markers")
	_ = os.MkdirAll(markers, 0o755)
	self := testSelfBinary(t)
	list := "tests/a.py\ntests/b.py\n"

	rep0 := filepath.Join(work, "r0.xml")
	p0 := startQueueProcess(t, list,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=kill-after-exec-before-report",
			"BUNPO_E2E_REPORT=" + rep0,
			"BUNPO_E2E_MARKER_DIR=" + markers,
			"BUNPO_E2E_RUNNER_TAG=victim",
		},
		"--queue", srv.URL, "--build", "e2e-kill-after", "--index", "0", "--total", "2",
		"--dir", filepath.Join(work, "d0"), "--report", rep0, "--lease-ttl", "300ms", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)

	childPID := waitForMarkerPID(t, filepath.Join(markers, "executed-before-report"), 5*time.Second)
	_ = p0.cmd.Process.Kill()
	killPID(childPID)
	_ = p0.cmd.Wait()

	rep1 := filepath.Join(work, "r1.xml")
	p1 := startQueueProcess(t, list,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=kill-after-exec-before-report",
			"BUNPO_E2E_REPORT=" + rep1,
			"BUNPO_E2E_RUNNER_TAG=survivor",
		},
		"--queue", srv.URL, "--build", "e2e-kill-after", "--index", "1", "--total", "2",
		"--dir", filepath.Join(work, "d1"), "--report", rep1, "--lease-ttl", "300ms", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := p1.wait(t); code != 0 {
		t.Fatalf("survivor exited %d: %s", code, p1.stderr.String())
	}
	assertBuildIntegrity(t, rc, "e2e-kill-after", queue.VerdictPassed, 2)
}

type dropFirstListener struct {
	net.Listener
	mu      sync.Mutex
	dropped int
}

func (l *dropFirstListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.mu.Lock()
		drop := l.dropped == 0
		if drop {
			l.dropped++
		}
		l.mu.Unlock()
		if drop {
			_ = c.Close()
			continue
		}
		return c, nil
	}
}

func (l *dropFirstListener) droppedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dropped
}

func TestQueueE2EServerRefusesConnectionsThenRecovers(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 500
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: e2eWriteTok,
		ReadToken:  e2eReadTok,
		Limits:     lim,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drop the first accepted TCP connection immediately so the runner is
	// guaranteed to hit a real transport error and retry until recovery.
	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &dropFirstListener{Listener: rawLn}
	addr := rawLn.Addr().String()

	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	work := t.TempDir()
	rep := filepath.Join(work, "r.xml")
	p := startQueueProcess(t, "tests/a.py\n",
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=pass",
			"BUNPO_E2E_REPORT=" + rep,
		},
		"--queue", "http://"+addr, "--build", "e2e-recover", "--index", "0", "--total", "1",
		"--dir", filepath.Join(work, "d0"), "--report", rep, "--lease-ttl", "2s",
		"--", testSelfBinary(t), "-test.run=^TestQueueHelperProcess$",
	)
	if code := p.wait(t); code != 0 {
		t.Fatalf("runner failed to recover after server outage (exit %d): %s", code, p.stderr.String())
	}
	if ln.droppedCount() < 1 {
		t.Fatal("expected at least one dropped TCP connection before recovery")
	}
}

func TestQueueE2EPoisonFileIsolatedAndUnresolved(t *testing.T) {
	srv, rc := startInProcessQueueServer(t, 200)
	work := t.TempDir()
	rep := filepath.Join(work, "r.xml")
	list := "tests/good1.py\ntests/good2.py\ntests/poison.py\n"

	p := startQueueProcess(t, list,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=poison-file",
			"BUNPO_E2E_REPORT=" + rep,
		},
		"--queue", srv.URL, "--build", "e2e-poison", "--index", "0", "--total", "1",
		"--dir", filepath.Join(work, "d0"), "--report", rep, "--lease-ttl", "2s", "--max-attempts", "2", "--max-batch", "3",
		"--", testSelfBinary(t), "-test.run=^TestQueueHelperProcess$",
	)
	if code := p.wait(t); code != 1 {
		t.Fatalf("poison build exited %d; want 1 (%s)", code, p.stderr.String())
	}
	st := assertBuildIntegrity(t, rc, "e2e-poison", queue.VerdictUnresolved, 3)
	if st.Counts.Passed != 2 || st.Counts.Unresolved != 1 {
		t.Fatalf("counts = %+v; want 2 passed and 1 unresolved (poison must not drag healthy siblings down)", st.Counts)
	}
}

func TestQueueE2EFailingSiblingUntimedAndPlaywrightAllSkipped(t *testing.T) {
	srv, rc := startInProcessQueueServer(t, 200)
	work := t.TempDir()
	self := testSelfBinary(t)

	// 1. One failing file fails the build; passing sibling in same batch is accepted UNTIMED.
	rep1 := filepath.Join(work, "r1.xml")
	list1 := "tests/bad.py\ntests/good.py\ntests/good2.py\n"
	p1 := startQueueProcess(t, list1,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=fail-sibling",
			"BUNPO_E2E_REPORT=" + rep1,
		},
		"--queue", srv.URL, "--build", "e2e-fail-sib", "--index", "0", "--total", "1",
		"--dir", filepath.Join(work, "d1"), "--report", rep1, "--lease-ttl", "2s", "--max-batch", "3",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := p1.wait(t); code != 1 {
		t.Fatalf("fail-sibling exited %d; want 1 (%s)", code, p1.stderr.String())
	}
	assertBuildIntegrity(t, rc, "e2e-fail-sib", queue.VerdictFailed, 3)
	page, err := rc.ListTests(context.Background(), "e2e-fail-sib", queue.ListTestsCommand{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	// Lease 1 had 2 tests (tests/bad.py and tests/good.py) under GSS ceil(3/2)=2;
	// tests/good.py ran alongside tests/bad.py so it MUST be passed AND untimed.
	for _, tv := range page.Tests {
		if tv.Test == "tests/good.py" && (tv.State != queue.StatusPassed || tv.Timed || tv.DurationMS != 0) {
			t.Fatalf("sibling tests/good.py = %+v; want passed UNTIMED", tv)
		}
	}

	// 2. Playwright-style all-skipped with non-zero exit must NEVER be accepted as skipped.
	rep2 := filepath.Join(work, "r2.xml")
	p2 := startQueueProcess(t, "tests/a.spec.ts\n",
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=playwright-all-skipped-nonzero",
			"BUNPO_E2E_REPORT=" + rep2,
		},
		"--queue", srv.URL, "--build", "e2e-pw-skip", "--index", "0", "--total", "1",
		"--dir", filepath.Join(work, "d2"), "--report", rep2, "--lease-ttl", "2s", "--max-attempts", "2",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := p2.wait(t); code != 1 {
		t.Fatalf("playwright-all-skipped exited %d; want 1 (%s)", code, p2.stderr.String())
	}
	st := assertBuildIntegrity(t, rc, "e2e-pw-skip", queue.VerdictUnresolved, 1)
	if st.Counts.Skipped != 0 || st.Counts.Unresolved != 1 {
		t.Fatalf("counts = %+v; want 0 skipped, 1 unresolved", st.Counts)
	}
}

func TestQueueE2EInitConflictMissingReportExtraFileAndStatusMerge(t *testing.T) {
	srv, rc := startInProcessQueueServer(t, 200)
	work := t.TempDir()
	self := testSelfBinary(t)

	// 1. Init conflict (even after machine 0 already passed -> flips to unresolved!).
	repA := filepath.Join(work, "ra.xml")
	pA := startQueueProcess(t, "tests/a.py\n",
		[]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_HELPER=pass", "BUNPO_E2E_REPORT=" + repA},
		"--queue", srv.URL, "--build", "e2e-conflict", "--index", "0", "--total", "2",
		"--dir", filepath.Join(work, "da"), "--report", repA, "--lease-ttl", "2s",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := pA.wait(t); code != 0 {
		t.Fatalf("machine 0 before conflict exited %d: %s", code, pA.stderr.String())
	}
	repB := filepath.Join(work, "rb.xml")
	pB := startQueueProcess(t, "tests/a.py\ntests/b.py\n",
		[]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_HELPER=pass", "BUNPO_E2E_REPORT=" + repB},
		"--queue", srv.URL, "--build", "e2e-conflict", "--index", "1", "--total", "2",
		"--dir", filepath.Join(work, "db"), "--report", repB, "--lease-ttl", "2s",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := pB.wait(t); code != 1 {
		t.Fatalf("conflicting machine 1 exited %d; want 1 (%s)", code, pB.stderr.String())
	}
	// --status must now report unresolved (exit 1) and must NOT write report-0.json.
	statusDir := filepath.Join(work, "status-conflict")
	pSt := startQueueProcess(t, "",
		[]string{"BUNPO_QUEUE_READ_TOKEN=" + e2eReadTok},
		"--queue", srv.URL, "--build", "e2e-conflict", "--status", "--dir", statusDir,
	)
	if code := pSt.wait(t); code != 1 {
		t.Fatalf("status on conflicted build exited %d; want 1 (%s)", code, pSt.stderr.String())
	}
	if _, err := os.Stat(filepath.Join(statusDir, "report-0.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("report-0.json must not be written for conflicted build")
	}

	// 2. Missing report -> unresolved, exit 1.
	repMiss := filepath.Join(work, "rmiss.xml")
	pMiss := startQueueProcess(t, "tests/a.py\n",
		[]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_HELPER=missing-report", "BUNPO_E2E_REPORT=" + repMiss},
		"--queue", srv.URL, "--build", "e2e-missing", "--index", "0", "--total", "1",
		"--dir", filepath.Join(work, "dmiss"), "--report", repMiss, "--lease-ttl", "2s", "--max-attempts", "2",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := pMiss.wait(t); code != 1 {
		t.Fatalf("missing-report exited %d; want 1 (%s)", code, pMiss.stderr.String())
	}
	assertBuildIntegrity(t, rc, "e2e-missing", queue.VerdictUnresolved, 1)

	// 3. Extra file in report -> reports error, exits 1 immediately with record-style warning.
	repExt := filepath.Join(work, "rext.xml")
	pExt := startQueueProcess(t, "tests/a.py\n",
		[]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_HELPER=extra-file", "BUNPO_E2E_REPORT=" + repExt},
		"--queue", srv.URL, "--build", "e2e-extra", "--index", "0", "--total", "1",
		"--dir", filepath.Join(work, "dext"), "--report", repExt, "--lease-ttl", "2s", "--max-attempts", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := pExt.wait(t); code != 1 || !strings.Contains(pExt.stderr.String(), "tests/unassigned_decoy.py") {
		t.Fatalf("extra-file exited %d: %s", code, pExt.stderr.String())
	}
	assertBuildIntegrity(t, rc, "e2e-extra", queue.VerdictUnresolved, 1)

	// 4. Passing build with passed + skipped -> --status writes report-0.json -> bunpo merge updates timings.json.
	repOk := filepath.Join(work, "rok.xml")
	pOk := startQueueProcess(t, "tests/a.py\ntests/skip.py\n",
		[]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_HELPER=pass-and-skip", "BUNPO_E2E_REPORT=" + repOk},
		"--queue", srv.URL, "--build", "e2e-merge", "--index", "0", "--total", "1",
		"--dir", filepath.Join(work, "dok"), "--report", repOk, "--lease-ttl", "2s",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := pOk.wait(t); code != 0 {
		t.Fatalf("pass-and-skip exited %d: %s", code, pOk.stderr.String())
	}

	mergeDir := filepath.Join(work, "merge-dir")
	pStOk := startQueueProcess(t, "",
		[]string{"BUNPO_QUEUE_READ_TOKEN=" + e2eReadTok},
		"--queue", srv.URL, "--build", "e2e-merge", "--status", "--dir", mergeDir,
	)
	if code := pStOk.wait(t); code != 0 {
		t.Fatalf("status exited %d: %s", code, pStOk.stderr.String())
	}
	if code, _, stderr := exe(t, "", "merge", "--dir", mergeDir); code != 0 {
		t.Fatalf("merge exited %d: %s", code, stderr)
	}

	rawTimings, err := os.ReadFile(filepath.Join(mergeDir, "timings.json"))
	if err != nil {
		t.Fatal(err)
	}
	tf, err := timing.Decode(rawTimings)
	if err != nil {
		t.Fatal(err)
	}
	if tf.Run != 1 || len(tf.Tests["tests/a.py"].Samples) != 1 || tf.Tests["tests/a.py"].Samples[0] != 300 {
		t.Fatalf("unexpected timings for tests/a.py: %+v", tf.Tests["tests/a.py"])
	}
	if e, ok := tf.Tests["tests/skip.py"]; !ok || e.LastSeen != 1 || len(e.Samples) != 0 {
		t.Fatalf("skipped file must stay seen with no samples: %+v, ok=%t", e, ok)
	}
}

func TestQueueE2ERealBunpoServerBinarySmoke(t *testing.T) {
	work := t.TempDir()
	srvBin := filepath.Join(work, "bunpo-server")
	if runtime.GOOS == "windows" {
		srvBin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", srvBin, "../bunpo-server").CombinedOutput(); err != nil {
		t.Fatalf("build bunpo-server: %v\n%s", err, out)
	}

	// Find a free loopback port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srvCmd := exec.Command(srvBin, "--addr", addr)
	var srvStderr bytes.Buffer
	srvCmd.Stderr = &srvStderr
	srvCmd.Env = append(os.Environ(),
		"BUNPO_QUEUE_TOKEN="+e2eWriteTok,
		"BUNPO_QUEUE_READ_TOKEN="+e2eReadTok,
	)
	if err := srvCmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped && srvCmd.Process != nil {
			_ = srvCmd.Process.Kill()
			_ = srvCmd.Wait()
		}
	})

	// Wait for /healthz.
	baseURL := "http://" + addr
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bunpo-server did not become healthy: %s", srvStderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	self := testSelfBinary(t)
	list := "tests/a.py\ntests/b.py\ntests/c.py\n"
	rep0, rep1 := filepath.Join(work, "r0.xml"), filepath.Join(work, "r1.xml")
	p0 := startQueueProcess(t, list,
		[]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_HELPER=pass", "BUNPO_E2E_REPORT=" + rep0},
		"--queue", baseURL, "--build", "smoke-1", "--index", "0", "--total", "2",
		"--dir", filepath.Join(work, "d0"), "--report", rep0, "--lease-ttl", "10s", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	p1 := startQueueProcess(t, list,
		[]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_HELPER=pass", "BUNPO_E2E_REPORT=" + rep1},
		"--queue", baseURL, "--build", "smoke-1", "--index", "1", "--total", "2",
		"--dir", filepath.Join(work, "d1"), "--report", rep1, "--lease-ttl", "10s", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if c0 := p0.wait(t); c0 != 0 {
		t.Fatalf("p0 exited %d: %s", c0, p0.stderr.String())
	}
	if c1 := p1.wait(t); c1 != 0 {
		t.Fatalf("p1 exited %d: %s", c1, p1.stderr.String())
	}

	stDir := filepath.Join(work, "st")
	pSt := startQueueProcess(t, "",
		[]string{"BUNPO_QUEUE_READ_TOKEN=" + e2eReadTok},
		"--queue", baseURL, "--build", "smoke-1", "--status", "--dir", stDir,
	)
	if code := pSt.wait(t); code != 0 {
		t.Fatalf("status exited %d: %s", code, pSt.stderr.String())
	}
	if code, _, stderr := exe(t, "", "merge", "--dir", stDir); code != 0 {
		t.Fatalf("merge exited %d: %s", code, stderr)
	}

	// Verify graceful shutdown on SIGTERM (Unix) or Kill (Windows).
	if runtime.GOOS != "windows" {
		_ = srvCmd.Process.Signal(syscall.SIGTERM)
		if err := srvCmd.Wait(); err != nil {
			t.Fatalf("bunpo-server graceful shutdown error: %v (%s)", err, srvStderr.String())
		}
		stopped = true
	}
}

func TestQueueAtCapacityRetriesAndFormatsExit1Error(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"version":1,"error":{"code":"at_capacity","message":"server is at capacity (64 running builds)"}}`))
	}))
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:    srv.URL,
		Token:      e2eWriteTok,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = queuerunner.Run(context.Background(), queuerunner.RunConfig{
		Client:        c,
		BuildID:       "cap-1",
		Config:        queue.Config{Machines: 1, LeaseTTLMS: queue.DefaultLeaseTTLMS, BuildTimeoutMS: queue.DefaultBuildTimeoutMS, MaxAttempts: 3, MaxBatch: 1},
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{nil},
		Dir:           t.TempDir(),
		Command:       []string{"true"},
		ReportPaths:   []string{"unused"},
		Format:        "lines",
		ReadReports:   func([]string) (map[string]junit.File, error) { return nil, errors.New("unused") },
		Retry:         queuerunner.RetryConfig{BudgetMS: 120, BaseMS: 25, MaxWaitMS: 40},
	})
	if calls < 2 {
		t.Fatalf("expected 503 at_capacity to be retried before failing, got %d call(s)", calls)
	}
	formatted := formatQueueRunError(err)
	var ee *exitError
	if !errors.As(formatted, &ee) || ee.code != 1 {
		t.Fatalf("formatted error %T (%v) is not exit-1 exitError", formatted, formatted)
	}
	if !strings.Contains(formatted.Error(), "queue server is at capacity") {
		t.Fatalf("formatted error = %q; want message saying server is at capacity", formatted.Error())
	}
}
