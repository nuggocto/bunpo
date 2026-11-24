package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
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
	if os.Getenv("BUNPO_QUEUE_TOKEN") != "" || os.Getenv("BUNPO_QUEUE_READ_TOKEN") != "" {
		os.Exit(99)
	}
	raw, err := os.ReadFile(os.Getenv("BUNPO_TESTS_FILE"))
	if err != nil {
		os.Exit(98)
	}
	var batch []string
	for line := range strings.Lines(string(raw)) {
		if line = strings.TrimSpace(line); line != "" {
			batch = append(batch, line)
		}
	}
	writeMarker := func(name string) {
		if dir := os.Getenv("BUNPO_E2E_MARKER_DIR"); dir != "" {
			_ = os.WriteFile(filepath.Join(dir, name), []byte(fmt.Sprintf("%d", os.Getpid())), 0o644)
		}
	}
	writeJUnit := func() {
		var b strings.Builder
		b.WriteString(`<testsuites><testsuite name="e2e">`)
		for _, f := range batch {
			fmt.Fprintf(&b, `<testcase file="%s" name="ok" time="0.100"/>`, f)
		}
		b.WriteString(`</testsuite></testsuites>`)
		if err := os.WriteFile(os.Getenv("BUNPO_E2E_REPORT"), []byte(b.String()), 0o644); err != nil {
			os.Exit(97)
		}
	}
	tag := os.Getenv("BUNPO_E2E_RUNNER_TAG")
	switch mode {
	case "pass":
		writeJUnit()
	case "pass-slow":
		time.Sleep(180 * time.Millisecond)
		writeJUnit()
	case "kill-before-exec":
		if tag == "victim" {
			writeMarker("leased-before-exec")
			time.Sleep(30 * time.Second)
		}
		writeJUnit()
	case "kill-after-exec-before-report":
		writeJUnit()
		if tag == "victim" {
			writeMarker("executed-before-report")
			time.Sleep(30 * time.Second)
		}
	default:
		os.Exit(96)
	}
	os.Exit(0)
}

func startQueueProcess(t *testing.T, stdin string, env []string, args ...string) *process {
	t.Helper()
	p := &process{cmd: exec.Command(binary, args...)}
	p.cmd.Stdin, p.cmd.Stdout, p.cmd.Stderr = strings.NewReader(stdin), &p.stdout, &p.stderr
	p.cmd.Env = append(os.Environ(), env...)
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return p
}

func startQueueWorker(t *testing.T, url, build, idx, dir, rep, ttl, list string, extraEnv ...string) *process {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	env := append([]string{"BUNPO_QUEUE_TOKEN=" + e2eWriteTok, "BUNPO_E2E_REPORT=" + rep}, extraEnv...)
	return startQueueProcess(t, list, env,
		"--queue", url, "--build", build, "--index", idx, "--total", "2",
		"--dir", dir, "--report", rep, "--lease-ttl", ttl, "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
}

func TestQueueE2EKillBeforeAndAfterExec(t *testing.T) {
	for _, tc := range []struct{ name, mode, marker, build string }{
		{"before-exec", "kill-before-exec", "leased-before-exec", "e2e-kill-before"},
		{"after-exec-before-report", "kill-after-exec-before-report", "executed-before-report", "e2e-kill-after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lim := queue.DefaultLimits()
			lim.MinLeaseTTLMS = 200
			h, err := queueapi.NewHandler(queueapi.HandlerConfig{WriteToken: e2eWriteTok, ReadToken: e2eReadTok, Limits: lim})
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(h)
			t.Cleanup(srv.Close)
			rc, _ := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: e2eReadTok, HTTPClient: srv.Client()})

			work := t.TempDir()
			markers := filepath.Join(work, "markers")
			_ = os.MkdirAll(markers, 0o755)
			list := "tests/a.py\ntests/b.py\n"

			p0 := startQueueWorker(t, srv.URL, tc.build, "0", filepath.Join(work, "d0"), filepath.Join(work, "r0.xml"), "300ms", list,
				"BUNPO_E2E_HELPER="+tc.mode, "BUNPO_E2E_MARKER_DIR="+markers, "BUNPO_E2E_RUNNER_TAG=victim")
			markerPath, childPID := filepath.Join(markers, tc.marker), 0
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && childPID == 0; time.Sleep(15 * time.Millisecond) {
				if data, err := os.ReadFile(markerPath); err == nil {
					_, _ = fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &childPID)
				}
			}
			if childPID == 0 {
				t.Fatalf("timed out waiting for marker %s", markerPath)
			}
			_ = p0.cmd.Process.Kill()
			if proc, err := os.FindProcess(childPID); err == nil {
				_ = proc.Kill()
			}
			_ = p0.cmd.Wait()

			p1 := startQueueWorker(t, srv.URL, tc.build, "1", filepath.Join(work, "d1"), filepath.Join(work, "r1.xml"), "300ms", list,
				"BUNPO_E2E_HELPER="+tc.mode, "BUNPO_E2E_RUNNER_TAG=survivor")
			if code := p1.wait(t); code != 0 {
				t.Fatalf("survivor exited %d: %s", code, p1.stderr.String())
			}
			if st, err := rc.Status(context.Background(), tc.build); err != nil || st.Verdict != queue.VerdictPassed || st.Counts.Passed != 2 {
				t.Fatalf("build %s status = (%+v, %v); want passed (2)", tc.build, st, err)
			}
		})
	}
}

func TestQueueE2ERealServerCrashRestartAndSIGTERM(t *testing.T) {
	work := t.TempDir()
	dataDir, srvBin := filepath.Join(work, "data"), filepath.Join(work, "bunpo-server")
	if runtime.GOOS == "windows" {
		srvBin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", srvBin, "../bunpo-server").CombinedOutput(); err != nil {
		t.Fatalf("build bunpo-server: %v\n%s", err, out)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	baseURL := "http://" + addr

	startSrv := func() (*exec.Cmd, *bytes.Buffer) {
		cmd := exec.Command(srvBin, "--addr", addr, "--data", dataDir)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		cmd.Env = append(os.Environ(), "BUNPO_QUEUE_TOKEN="+e2eWriteTok, "BUNPO_QUEUE_READ_TOKEN="+e2eReadTok)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			if resp, err := http.Get(baseURL + "/healthz"); err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				return cmd, &stderr
			}
			if time.Now().After(deadline) {
				_ = cmd.Process.Kill()
				t.Fatalf("bunpo-server did not become healthy: %s", stderr.String())
			}
		}
	}

	srv1, _ := startSrv()
	rc, _ := queueapi.NewClient(queueapi.ClientConfig{BaseURL: baseURL, Token: e2eReadTok})
	list := "tests/a.py\ntests/b.py\ntests/c.py\ntests/d.py\ntests/e.py\ntests/f.py\n"
	p0 := startQueueWorker(t, baseURL, "crash-1", "0", filepath.Join(work, "d0"), filepath.Join(work, "r0.xml"), "10s", list, "BUNPO_E2E_HELPER=pass-slow")
	p1 := startQueueWorker(t, baseURL, "crash-1", "1", filepath.Join(work, "d1"), filepath.Join(work, "r1.xml"), "10s", list, "BUNPO_E2E_HELPER=pass-slow")

	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(25 * time.Millisecond) {
		if st, err := rc.Status(context.Background(), "crash-1"); err == nil && st.Counts.Passed >= 1 {
			break
		}
		if time.Now().After(deadline) {
			_ = srv1.Process.Kill()
			t.Fatal("timed out waiting for mid-build progress before SIGKILL")
		}
	}
	_ = srv1.Process.Kill()
	_ = srv1.Wait()

	srv2, srv2Err := startSrv()
	stopped := false
	t.Cleanup(func() {
		if !stopped && srv2.Process != nil {
			_ = srv2.Process.Kill()
			_ = srv2.Wait()
		}
	})
	for _, p := range []*process{p0, p1} {
		if c := p.wait(t); c != 0 {
			t.Fatalf("worker exited %d: %s", c, p.stderr.String())
		}
	}
	if st, err := rc.Status(context.Background(), "crash-1"); err != nil || st.Verdict != queue.VerdictPassed || st.Counts.Passed != 6 {
		t.Fatalf("post-crash status = (%+v, %v); want passed (6)", st, err)
	}
	lt, err := rc.ListTests(context.Background(), "crash-1", queue.ListTestsCommand{Limit: 100})
	if err != nil || len(lt.Tests) != 6 {
		t.Fatalf("list tests = (%d, %v)", len(lt.Tests), err)
	}
	for _, tv := range lt.Tests {
		accepted := 0
		for _, a := range tv.Attempts {
			if a.Status == queue.AttemptPassed {
				accepted++
			}
		}
		if tv.State != queue.StatusPassed || accepted != 1 {
			t.Fatalf("test %s state=%s accepted=%d; want passed/1", tv.Test, tv.State, accepted)
		}
	}
	stDir := filepath.Join(work, "st")
	if pSt := startQueueProcess(t, "", []string{"BUNPO_QUEUE_READ_TOKEN=" + e2eReadTok}, "--queue", baseURL, "--build", "crash-1", "--status", "--dir", stDir); pSt.wait(t) != 0 {
		t.Fatalf("status failed: %s", pSt.stderr.String())
	}
	if code, _, stderr := exe(t, "", "merge", "--dir", stDir); code != 0 {
		t.Fatalf("merge exited %d: %s", code, stderr)
	}
	if runtime.GOOS != "windows" {
		_ = srv2.Process.Signal(syscall.SIGTERM)
		if err := srv2.Wait(); err != nil {
			t.Fatalf("bunpo-server SIGTERM exit error: %v (%s)", err, srv2Err.String())
		}
		stopped = true
	}
}
