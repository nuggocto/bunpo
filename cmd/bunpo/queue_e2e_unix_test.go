//go:build unix

package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nuggocto/bunpo/internal/queue"
)

func TestQueueE2ESIGSTOPPastTTLThenSIGCONT(t *testing.T) {
	srv, rc := startInProcessQueueServer(t, 200)
	work := t.TempDir()
	markers := filepath.Join(work, "markers")
	_ = os.MkdirAll(markers, 0o755)
	self := testSelfBinary(t)
	list := "tests/a.py\n"

	rep0 := filepath.Join(work, "r0.xml")
	p0 := startQueueProcess(t, list,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=pause-stale",
			"BUNPO_E2E_REPORT=" + rep0,
			"BUNPO_E2E_MARKER_DIR=" + markers,
			"BUNPO_E2E_RUNNER_TAG=paused",
		},
		"--queue", srv.URL, "--build", "e2e-sigstop", "--index", "0", "--total", "2",
		"--dir", filepath.Join(work, "d0"), "--report", rep0, "--lease-ttl", "300ms", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)

	// Wait until runner 0's child wrote its report, then freeze runner 0 past the TTL.
	_ = waitForMarkerPID(t, filepath.Join(markers, "ready-to-stop"), 5*time.Second)
	if err := p0.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	resumed := false
	t.Cleanup(func() {
		if !resumed && p0.cmd.Process != nil {
			_ = p0.cmd.Process.Signal(syscall.SIGCONT)
			_ = p0.cmd.Process.Kill()
			_ = p0.cmd.Wait()
		}
	})

	// Runner 1 waits for TTL expiry, takes over attempt 0.2, and finishes the build.
	rep1 := filepath.Join(work, "r1.xml")
	p1 := startQueueProcess(t, list,
		[]string{
			"BUNPO_QUEUE_TOKEN=" + e2eWriteTok,
			"BUNPO_E2E_HELPER=pass",
			"BUNPO_E2E_REPORT=" + rep1,
		},
		"--queue", srv.URL, "--build", "e2e-sigstop", "--index", "1", "--total", "2",
		"--dir", filepath.Join(work, "d1"), "--report", rep1, "--lease-ttl", "300ms", "--max-batch", "1",
		"--", self, "-test.run=^TestQueueHelperProcess$",
	)
	if code := p1.wait(t); code != 0 {
		t.Fatalf("runner 1 exited %d: %s", code, p1.stderr.String())
	}

	// Resume runner 0 after the build already passed; it wakes up and submits its stale report.
	if err := p0.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	resumed = true
	if code := p0.wait(t); code != 0 {
		t.Fatalf("resumed runner 0 exited %d; want 0 (build passed): %s", code, p0.stderr.String())
	}

	assertBuildIntegrity(t, rc, "e2e-sigstop", queue.VerdictPassed, 1)

	page, err := rc.ListTests(context.Background(), "e2e-sigstop", queue.ListTestsCommand{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	tv := page.Tests[0]
	if tv.Generation != 2 || len(tv.History) != 1 || tv.History[0].Disposition != queue.DispositionStale {
		t.Fatalf("expected generation=2 and 1 stale history record; got gen=%d history=%+v", tv.Generation, tv.History)
	}
}
