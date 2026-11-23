package queuerunner

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

func ratMS(ms int64) *big.Rat {
	return big.NewRat(ms, 1000)
}

func TestMapOutcomesAllRows(t *testing.T) {
	attempts := []queue.LeasedAttemptView{
		{AttemptID: "0.1", TestIndex: 0, Generation: 1, Test: "tests/a.py"},
		{AttemptID: "1.1", TestIndex: 1, Generation: 1, Test: "tests/b.py"},
	}

	t.Run("exit 0: passed timed, all-skipped, absent error", func(t *testing.T) {
		three := []queue.LeasedAttemptView{
			{AttemptID: "0.1", TestIndex: 0, Generation: 1, Test: "a.py"},
			{AttemptID: "1.1", TestIndex: 1, Generation: 1, Test: "b.py"},
			{AttemptID: "2.1", TestIndex: 2, Generation: 1, Test: "c.py"},
		}
		files := map[string]junit.File{
			"a.py": {Seconds: ratMS(120), Cases: 2, Skipped: 1, Failed: false}, // partial skip -> passed timed
			"b.py": {Seconds: ratMS(0), Cases: 2, Skipped: 2, Failed: false},   // all skipped -> skipped
		}
		res, extra := MapOutcomes(three, 0, false, "junit", files, nil)
		if extra != nil {
			t.Fatalf("unexpected extra: %v", extra)
		}
		if res[0].Outcome != queue.OutcomePassed || !res[0].Timed || res[0].DurationMS != 120 {
			t.Fatalf("a.py = %+v; want passed timed 120", res[0])
		}
		if res[1].Outcome != queue.OutcomeSkipped || res[1].Timed || res[1].DurationMS != 0 {
			t.Fatalf("b.py = %+v; want skipped untimed", res[1])
		}
		if res[2].Outcome != queue.OutcomeError || res[2].Timed {
			t.Fatalf("c.py = %+v; want error", res[2])
		}
	})

	t.Run("normal non-zero JUnit with failed sibling: clean sibling passed untimed, skipped sibling error", func(t *testing.T) {
		three := []queue.LeasedAttemptView{
			{AttemptID: "0.1", TestIndex: 0, Generation: 1, Test: "fail.spec.ts"},
			{AttemptID: "1.1", TestIndex: 1, Generation: 1, Test: "pass.spec.ts"},
			{AttemptID: "2.1", TestIndex: 2, Generation: 1, Test: "skip.spec.ts"},
		}
		files := map[string]junit.File{
			"fail.spec.ts": {Seconds: ratMS(50), Cases: 1, Failed: true},
			"pass.spec.ts": {Seconds: ratMS(80), Cases: 2, Skipped: 0, Failed: false},
			"skip.spec.ts": {Seconds: ratMS(0), Cases: 2, Skipped: 2, Failed: false}, // Playwright maxFailures skip
		}
		res, extra := MapOutcomes(three, 1, false, "junit", files, nil)
		if extra != nil {
			t.Fatalf("unexpected extra: %v", extra)
		}
		if res[0].Outcome != queue.OutcomeFailed || res[0].Timed {
			t.Fatalf("fail = %+v; want failed untimed", res[0])
		}
		if res[1].Outcome != queue.OutcomePassed || res[1].Timed || res[1].DurationMS != 0 {
			t.Fatalf("pass = %+v; want passed UNTIMED", res[1])
		}
		if res[2].Outcome != queue.OutcomeError {
			t.Fatalf("skip = %+v; want error (must never be skipped on non-zero exit)", res[2])
		}
	})

	t.Run("signaled exit or lines format with failed sibling: sibling is error", func(t *testing.T) {
		files := map[string]junit.File{
			"tests/a.py": {Seconds: ratMS(50), Cases: 1, Failed: true},
			"tests/b.py": {Seconds: ratMS(80), Cases: 1, Failed: false},
		}
		resSig, _ := MapOutcomes(attempts, -1, true, "junit", files, nil)
		if resSig[0].Outcome != queue.OutcomeFailed || resSig[1].Outcome != queue.OutcomeError {
			t.Fatalf("signaled = %+v; want [failed, error]", resSig)
		}
		resLines, _ := MapOutcomes(attempts, 1, false, "lines", files, nil)
		if resLines[0].Outcome != queue.OutcomeFailed || resLines[1].Outcome != queue.OutcomeError {
			t.Fatalf("lines non-zero = %+v; want [failed, error]", resLines)
		}
	})

	t.Run("missing/invalid report -> all error", func(t *testing.T) {
		res, extra := MapOutcomes(attempts, 1, false, "junit", nil, errors.New("no such file"))
		if extra != nil {
			t.Fatalf("unexpected extra: %v", extra)
		}
		for i, r := range res {
			if r.Outcome != queue.OutcomeError {
				t.Fatalf("res[%d] = %+v; want error", i, r)
			}
		}
	})

	t.Run("extra file outside lease -> all error plus ExtraFilesError", func(t *testing.T) {
		files := map[string]junit.File{
			"tests/a.py":     {Seconds: ratMS(10), Cases: 1},
			"tests/b.py":     {Seconds: ratMS(10), Cases: 1},
			"tests/extra.py": {Seconds: ratMS(10), Cases: 1},
		}
		res, extra := MapOutcomes(attempts, 0, false, "junit", files, nil)
		if extra == nil || !slices.Equal(extra.Extra, []string{"tests/extra.py"}) {
			t.Fatalf("extra = %+v; want [tests/extra.py]", extra)
		}
		for i, r := range res {
			if r.Outcome != queue.OutcomeError {
				t.Fatalf("res[%d] = %+v; want error", i, r)
			}
		}
	})
}

func TestFilteredChildEnvStripsTokens(t *testing.T) {
	in := []string{
		"PATH=/usr/bin",
		"BUNPO_QUEUE_TOKEN=secret-write",
		"BUNPO_QUEUE_READ_TOKEN=secret-read",
		"bunpo_queue_token=lowercase-variant",
		"BUNPO_TESTS_FILE=/old/file.txt",
		"HOME=/tmp",
	}
	got := filteredChildEnv(in, "/new/batch.txt")
	for _, kv := range got {
		if strings.Contains(strings.ToUpper(kv), "BUNPO_QUEUE_TOKEN") || strings.Contains(strings.ToUpper(kv), "BUNPO_QUEUE_READ_TOKEN") {
			t.Fatalf("token leaked into child env: %q", kv)
		}
	}
	if !slices.Contains(got, "BUNPO_TESTS_FILE=/new/batch.txt") {
		t.Fatalf("missing new BUNPO_TESTS_FILE in %v", got)
	}
	if !slices.Contains(got, "PATH=/usr/bin") || !slices.Contains(got, "HOME=/tmp") {
		t.Fatalf("unrelated env vars dropped: %v", got)
	}
}

func TestHeartbeatSurvivesTransientFailureAndNoGoroutineLeaks(t *testing.T) {
	const (
		writeTok = "write-secret-token-0123456789abcdef"
		readTok  = "read-secret-token-fedcba9876543210"
	)

	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 180
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: writeTok,
		ReadToken:  readTok,
		Limits:     lim,
	})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	reportPath := filepath.Join(dir, "times.txt")
	releaseFile := filepath.Join(dir, "release-hb")

	var hbCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fail the first two heartbeats with 503 to verify the heartbeat loop retries within TTL.
		if strings.HasSuffix(r.URL.Path, "/heartbeat") {
			n := hbCalls.Add(1)
			if n <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			h.ServeHTTP(w, r)
			if n == 3 {
				// Release the child only after the third heartbeat actually succeeded.
				_ = os.WriteFile(releaseFile, []byte("ok"), 0o644)
			}
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	// Wait briefly for baseline goroutines to settle.
	runtime.GC()
	baseGoroutines := runtime.NumGoroutine()

	client, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:      srv.URL,
		Token:        writeTok,
		HTTPClient:   srv.Client(),
		RetryBudget:  2 * time.Second,
		RetryBase:    15 * time.Millisecond,
		RetryMaxWait: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Child waits on releaseFile (written after 2 failed + 1 successful heartbeat)
	// and then writes a lines report for tests/a.py.
	helperCmd := []string{os.Args[0], "-test.run=^TestRunnerChildHelper$"}
	t.Setenv("BUNPO_RUNNER_HELPER_MODE", "pass")
	t.Setenv("BUNPO_RUNNER_HELPER_REPORT", reportPath)
	t.Setenv("BUNPO_RUNNER_HELPER_WAIT_FILE", releaseFile)

	verdict, err := Run(context.Background(), RunConfig{
		Client:        client,
		BuildID:       "hb-survive",
		Config:        queue.Config{Machines: 1, LeaseTTLMS: 180, BuildTimeoutMS: 30_000, MaxAttempts: 2, MaxBatch: 1},
		Tests:         []string{"tests/a.py"},
		PredictionsMS: []*int64{nil},
		Dir:           dir,
		Command:       helperCmd,
		ReportPaths:   []string{reportPath},
		Format:        "lines",
		ReadReports: func(paths []string) (map[string]junit.File, error) {
			f, err := os.Open(paths[0])
			if err != nil {
				return nil, err
			}
			defer f.Close()
			rep, err := junit.ReadLines(f)
			return rep.Files, err
		},
	})
	if err != nil || verdict != queue.VerdictPassed {
		t.Fatalf("Run = (%q, %v); want (passed, nil)", verdict, err)
	}
	if hbCalls.Load() < 3 {
		t.Fatalf("expected >= 3 heartbeat calls (2 failed + >=1 succeeded), got %d", hbCalls.Load())
	}

	// Test cancelled exit path (SIGINT/SIGTERM) leaves zero leaked goroutines.
	// Child waits on a file that is never created so cancellation always interrupts it.
	t.Setenv("BUNPO_RUNNER_HELPER_WAIT_FILE", filepath.Join(dir, "never-created"))
	cancelCtx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	_, err = Run(cancelCtx, RunConfig{
		Client:        client,
		BuildID:       "hb-cancel",
		Config:        queue.Config{Machines: 1, LeaseTTLMS: 180, BuildTimeoutMS: 30_000, MaxAttempts: 2, MaxBatch: 1},
		Tests:         []string{"tests/a.py"},
		PredictionsMS: []*int64{nil},
		Dir:           dir,
		Command:       helperCmd,
		ReportPaths:   []string{reportPath},
		Format:        "lines",
		ReadReports:   func([]string) (map[string]junit.File, error) { return nil, errors.New("unused") },
		KillGrace:     100 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Run err = %v; want context deadline/canceled", err)
	}

	// Give runtime a moment to reap httptest server idle conns, then check goroutine count.
	srv.CloseClientConnections()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= baseGoroutines+2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseGoroutines+2 {
		t.Fatalf("possible goroutine leak: before=%d after=%d", baseGoroutines, got)
	}
}

func TestHeartbeatsKeepLeaseAliveDuringReportRetry(t *testing.T) {
	const (
		writeTok = "write-secret-token-0123456789abcdef"
		readTok  = "read-secret-token-fedcba9876543210"
	)

	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 180
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: writeTok,
		ReadToken:  readTok,
		Limits:     lim,
	})
	if err != nil {
		t.Fatal(err)
	}

	var firstReportNS atomic.Int64
	var hbDuringRetry atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/report") {
			now := time.Now().UnixNano()
			start := firstReportNS.Load()
			if start == 0 {
				firstReportNS.CompareAndSwap(0, now)
				start = firstReportNS.Load()
			}
			// Fail /report with 503 until both > 180ms TTL has passed AND at least
			// two heartbeats have landed during Report retries.
			if time.Duration(now-start) < 220*time.Millisecond || hbDuringRetry.Load() < 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		} else if strings.HasSuffix(r.URL.Path, "/heartbeat") && firstReportNS.Load() != 0 {
			hbDuringRetry.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	client, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:      srv.URL,
		Token:        writeTok,
		HTTPClient:   srv.Client(),
		RetryBudget:  3 * time.Second,
		RetryBase:    40 * time.Millisecond,
		RetryMaxWait: 80 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	reportPath := filepath.Join(dir, "times.txt")
	t.Setenv("BUNPO_RUNNER_HELPER_MODE", "pass")
	t.Setenv("BUNPO_RUNNER_HELPER_REPORT", reportPath)
	t.Setenv("BUNPO_RUNNER_HELPER_WAIT_FILE", "")

	verdict, err := Run(context.Background(), RunConfig{
		Client:        client,
		BuildID:       "hb-during-report",
		Config:        queue.Config{Machines: 1, LeaseTTLMS: 180, BuildTimeoutMS: 30_000, MaxAttempts: 1, MaxBatch: 1},
		Tests:         []string{"tests/a.py"},
		PredictionsMS: []*int64{nil},
		Dir:           dir,
		Command:       []string{os.Args[0], "-test.run=^TestRunnerChildHelper$"},
		ReportPaths:   []string{reportPath},
		Format:        "lines",
		ReadReports: func(paths []string) (map[string]junit.File, error) {
			f, err := os.Open(paths[0])
			if err != nil {
				return nil, err
			}
			defer f.Close()
			rep, err := junit.ReadLines(f)
			return rep.Files, err
		},
	})
	// With MaxAttempts=1, if the lease had expired during the 320ms report outage,
	// the test would have become unresolved and the report stale. Passing proves
	// heartbeats kept the lease alive through the report retry.
	if err != nil || verdict != queue.VerdictPassed {
		t.Fatalf("Run = (%q, %v); want (passed, nil)", verdict, err)
	}
	if hbDuringRetry.Load() == 0 {
		t.Fatal("expected at least one heartbeat while Report was retrying")
	}
	page, err := client.ListTests(context.Background(), "hb-during-report", queue.ListTestsCommand{Limit: 10})
	if err != nil {
		t.Fatalf("ListTests: %v", err)
	}
	if len(page.Tests) != 1 || page.Tests[0].Generation != 1 || len(page.Tests[0].Attempts) != 1 {
		t.Fatalf("expected acceptance on attempt 1, got %+v", page.Tests)
	}
}

type statusCaptureWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCaptureWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func TestLeaseStaleRequestRetriesWithFreshRequestID(t *testing.T) {
	const (
		writeTok = "write-secret-token-0123456789abcdef"
		readTok  = "read-secret-token-fedcba9876543210"
	)

	var fakeNow atomic.Int64
	fakeNow.Store(1_000_000)

	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 150
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: writeTok,
		ReadToken:  readTok,
		Limits:     lim,
		Clock:      func() int64 { return fakeNow.Load() },
	})
	if err != nil {
		t.Fatal(err)
	}

	var leaseCalls atomic.Int32
	var sawStale409 atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/lease") {
			n := leaseCalls.Add(1)
			if n == 1 {
				// Grant lease 1 on the server, advance the server clock past the
				// 150ms TTL, then drop the HTTP reply as 503.
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, r)
				fakeNow.Add(200)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if n == 2 {
				cw := &statusCaptureWriter{ResponseWriter: w, status: http.StatusOK}
				h.ServeHTTP(cw, r)
				if cw.status == http.StatusConflict {
					sawStale409.Add(1)
				}
				return
			}
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	client, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:      srv.URL,
		Token:        writeTok,
		HTTPClient:   srv.Client(),
		RetryBudget:  3 * time.Second,
		RetryBase:    20 * time.Millisecond,
		RetryMaxWait: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	reportPath := filepath.Join(dir, "times.txt")
	t.Setenv("BUNPO_RUNNER_HELPER_MODE", "pass")
	t.Setenv("BUNPO_RUNNER_HELPER_REPORT", reportPath)
	t.Setenv("BUNPO_RUNNER_HELPER_WAIT_FILE", "")

	verdict, err := Run(context.Background(), RunConfig{
		Client:        client,
		BuildID:       "stale-req-retry",
		Config:        queue.Config{Machines: 1, LeaseTTLMS: 150, BuildTimeoutMS: 30_000, MaxAttempts: 2, MaxBatch: 1},
		Tests:         []string{"tests/a.py"},
		PredictionsMS: []*int64{nil},
		Dir:           dir,
		Command:       []string{os.Args[0], "-test.run=^TestRunnerChildHelper$"},
		ReportPaths:   []string{reportPath},
		Format:        "lines",
		ReadReports: func(paths []string) (map[string]junit.File, error) {
			f, err := os.Open(paths[0])
			if err != nil {
				return nil, err
			}
			defer f.Close()
			rep, err := junit.ReadLines(f)
			return rep.Files, err
		},
	})
	if err != nil || verdict != queue.VerdictPassed {
		t.Fatalf("Run = (%q, %v); want (passed, nil)", verdict, err)
	}
	if sawStale409.Load() == 0 {
		t.Fatal("expected retried lease request to hit 409 stale_request")
	}
	if leaseCalls.Load() < 3 {
		t.Fatalf("expected >= 3 lease calls (dropped, 409 stale, fresh request_id), got %d", leaseCalls.Load())
	}
}

func TestHeartbeatPermanent4xxStopsBatchAndSurfacesError(t *testing.T) {
	const (
		writeTok = "write-secret-token-0123456789abcdef"
		readTok  = "read-secret-token-fedcba9876543210"
	)

	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 180
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: writeTok,
		ReadToken:  readTok,
		Limits:     lim,
	})
	if err != nil {
		t.Fatal(err)
	}

	var hbCalls atomic.Int32
	var revoked atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/heartbeat") {
			hbCalls.Add(1)
			revoked.Store(true)
			w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"version":1,"error":{"code":"unauthorized","message":"token revoked"}}`))
			return
		}
		if revoked.Load() && strings.HasSuffix(r.URL.Path, "/lease") {
			w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"version":1,"error":{"code":"unauthorized","message":"token revoked"}}`))
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	client, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:      srv.URL,
		Token:        writeTok,
		HTTPClient:   srv.Client(),
		RetryBudget:  2 * time.Second,
		RetryBase:    20 * time.Millisecond,
		RetryMaxWait: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	reportPath := filepath.Join(dir, "times.txt")
	t.Setenv("BUNPO_RUNNER_HELPER_MODE", "pass")
	t.Setenv("BUNPO_RUNNER_HELPER_REPORT", reportPath)
	t.Setenv("BUNPO_RUNNER_HELPER_WAIT_FILE", filepath.Join(dir, "never-created"))

	start := time.Now()
	_, err = Run(context.Background(), RunConfig{
		Client:        client,
		BuildID:       "hb-perm-401",
		Config:        queue.Config{Machines: 1, LeaseTTLMS: 300, BuildTimeoutMS: 30_000, MaxAttempts: 2, MaxBatch: 1},
		Tests:         []string{"tests/a.py"},
		PredictionsMS: []*int64{nil},
		Dir:           dir,
		Command:       []string{os.Args[0], "-test.run=^TestRunnerChildHelper$"},
		ReportPaths:   []string{reportPath},
		Format:        "lines",
		ReadReports:   func([]string) (map[string]junit.File, error) { return nil, errors.New("unused") },
		KillGrace:     200 * time.Millisecond,
	})
	elapsed := time.Since(start)

	var apiErr *queueapi.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("Run err = %v; want 401 APIError", err)
	}
	if hbCalls.Load() != 1 {
		t.Fatalf("expected 1 heartbeat call (permanent 401 must not retry), got %d", hbCalls.Load())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("permanent 401 took %v to stop child; expected prompt termination", elapsed)
	}
}

func TestBuildRunReportRejectsStuckCursorAndShortRead(t *testing.T) {
	status := queueapi.StatusResponse{
		Version: queue.ProtocolVersion,
		StatusResponse: queue.StatusResponse{
			BuildID: "b-page",
			Verdict: queue.VerdictPassed,
			Counts:  queue.Counts{Total: 2, Passed: 2},
		},
	}

	t.Run("stuck cursor", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
			_, _ = w.Write([]byte(`{"version":1,"build_id":"b-page","verdict":"passed","tests":[{"test":"a.py","index":0,"state":"passed","generation":1,"timed":true,"duration_ms":10,"attempts":[]}],"next_cursor":0}`))
		}))
		defer srv.Close()
		c, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: "tok", HTTPClient: srv.Client()})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := BuildRunReport(context.Background(), c, "b-page", status); err == nil || !strings.Contains(err.Error(), "cursor did not advance") {
			t.Fatalf("BuildRunReport stuck cursor err = %v; want cursor-did-not-advance error", err)
		}
	})

	t.Run("short read", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
			_, _ = w.Write([]byte(`{"version":1,"build_id":"b-page","verdict":"passed","tests":[{"test":"a.py","index":0,"state":"passed","generation":1,"timed":true,"duration_ms":10,"attempts":[]}],"next_cursor":null}`))
		}))
		defer srv.Close()
		c, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: "tok", HTTPClient: srv.Client()})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := BuildRunReport(context.Background(), c, "b-page", status); err == nil || !strings.Contains(err.Error(), "incomplete test list") {
			t.Fatalf("BuildRunReport short read err = %v; want incomplete-test-list error", err)
		}
	})
}

func TestRunnerChildHelper(t *testing.T) {
	mode := os.Getenv("BUNPO_RUNNER_HELPER_MODE")
	if mode == "" {
		return
	}
	if waitFile := os.Getenv("BUNPO_RUNNER_HELPER_WAIT_FILE"); waitFile != "" {
		for {
			if _, err := os.Stat(waitFile); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	batchPath := os.Getenv("BUNPO_TESTS_FILE")
	raw, err := os.ReadFile(batchPath)
	if err != nil {
		os.Exit(2)
	}
	var out strings.Builder
	for line := range strings.Lines(string(raw)) {
		line = strings.TrimSpace(line)
		if line != "" {
			out.WriteString(line + " 50\n")
		}
	}
	if err := os.WriteFile(os.Getenv("BUNPO_RUNNER_HELPER_REPORT"), []byte(out.String()), 0o644); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}
