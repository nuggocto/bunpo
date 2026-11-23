package queuerunner

import (
	"context"
	"errors"
	"math/big"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

func ratMS(ms int64) *big.Rat { return big.NewRat(ms, 1000) }

func TestMapOutcomesAllRows(t *testing.T) {
	attempts := []queue.LeasedAttemptView{
		{AttemptID: "0.1", TestIndex: 0, Generation: 1, Test: "a.py"},
		{AttemptID: "1.1", TestIndex: 1, Generation: 1, Test: "b.py"},
		{AttemptID: "2.1", TestIndex: 2, Generation: 1, Test: "c.py"},
	}
	t.Run("exit 0: passed timed, all-skipped, absent error", func(t *testing.T) {
		files := map[string]junit.File{
			"a.py": {Seconds: ratMS(120), Cases: 2, Skipped: 1},
			"b.py": {Seconds: ratMS(0), Cases: 2, Skipped: 2},
		}
		res, extra := MapOutcomes(attempts, 0, false, "junit", files, nil)
		if extra != nil || res[0].Outcome != queue.OutcomePassed || !res[0].Timed || res[0].DurationMS != 120 ||
			res[1].Outcome != queue.OutcomeSkipped || res[1].Timed || res[2].Outcome != queue.OutcomeError {
			t.Fatalf("got (%+v, %v)", res, extra)
		}
	})
	t.Run("non-zero JUnit with failed sibling: clean sibling untimed, skipped error", func(t *testing.T) {
		files := map[string]junit.File{
			"a.py": {Seconds: ratMS(50), Cases: 1, Failed: true},
			"b.py": {Seconds: ratMS(80), Cases: 2},
			"c.py": {Seconds: ratMS(0), Cases: 2, Skipped: 2},
		}
		res, extra := MapOutcomes(attempts, 1, false, "junit", files, nil)
		if extra != nil || res[0].Outcome != queue.OutcomeFailed || res[0].Timed ||
			res[1].Outcome != queue.OutcomePassed || res[1].Timed || res[1].DurationMS != 0 ||
			res[2].Outcome != queue.OutcomeError {
			t.Fatalf("got (%+v, %v)", res, extra)
		}
	})
	t.Run("signaled, lines non-zero, unreadable, and extra file", func(t *testing.T) {
		two := attempts[:2]
		files := map[string]junit.File{"a.py": {Seconds: ratMS(50), Cases: 1, Failed: true}, "b.py": {Seconds: ratMS(80), Cases: 1}}
		if res, _ := MapOutcomes(two, -1, true, "junit", files, nil); res[0].Outcome != queue.OutcomeFailed || res[1].Outcome != queue.OutcomeError {
			t.Fatalf("signaled = %+v", res)
		}
		if res, _ := MapOutcomes(two, 1, false, "lines", files, nil); res[0].Outcome != queue.OutcomeFailed || res[1].Outcome != queue.OutcomeError {
			t.Fatalf("lines non-zero = %+v", res)
		}
		if res, _ := MapOutcomes(two, 1, false, "junit", nil, errors.New("missing")); res[0].Outcome != queue.OutcomeError || res[1].Outcome != queue.OutcomeError {
			t.Fatalf("readErr = %+v", res)
		}
		files["extra.py"] = junit.File{Seconds: ratMS(10), Cases: 1}
		if res, extra := MapOutcomes(two, 0, false, "junit", files, nil); extra == nil || !slices.Equal(extra.Extra, []string{"extra.py"}) || res[0].Outcome != queue.OutcomeError {
			t.Fatalf("extra = (%+v, %v)", res, extra)
		}
	})
}

func TestFilteredChildEnvStripsTokens(t *testing.T) {
	in := []string{"PATH=/usr/bin", "BUNPO_QUEUE_TOKEN=secret-write", "BUNPO_QUEUE_READ_TOKEN=secret-read", "bunpo_queue_token=lower", "BUNPO_TESTS_FILE=/old"}
	got := filteredChildEnv(in, "/new/batch.txt")
	for _, kv := range got {
		if strings.Contains(strings.ToUpper(kv), "BUNPO_QUEUE_TOKEN") || strings.Contains(strings.ToUpper(kv), "BUNPO_QUEUE_READ_TOKEN") {
			t.Fatalf("token leaked into child env: %q", kv)
		}
	}
	if !slices.Contains(got, "BUNPO_TESTS_FILE=/new/batch.txt") || !slices.Contains(got, "PATH=/usr/bin") {
		t.Fatalf("unexpected filtered env: %v", got)
	}
}

func TestDriverCancelsCleanlyWithoutGoroutineLeaks(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 180
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: "write-secret-token-0123456789abcdef", ReadToken: "read-secret-token-fedcba9876543210", Limits: lim,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	client, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: "write-secret-token-0123456789abcdef", HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	baseGoroutines := runtime.NumGoroutine()
	dir := t.TempDir()
	t.Setenv("BUNPO_DRIVER_TEST_WAIT", filepath.Join(dir, "never-created"))
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = Run(ctx, RunConfig{
		Client: client, BuildID: "driver-cancel",
		Config: queue.Config{Machines: 1, LeaseTTLMS: 180, BuildTimeoutMS: 30_000, MaxAttempts: 2, MaxBatch: 1},
		Tests:  []string{"tests/a.py"}, PredictionsMS: []*int64{nil}, Dir: dir,
		Command:     []string{os.Args[0], "-test.run=^TestDriverChildHelper$"},
		ReportPaths: []string{filepath.Join(dir, "times.txt")}, Format: "lines",
		ReadReports: func([]string) (map[string]junit.File, error) { return nil, errors.New("unused") },
		KillGrace:   100 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Run err = %v; want context deadline/canceled", err)
	}
	srv.CloseClientConnections()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		runtime.GC()
		if runtime.NumGoroutine() <= baseGoroutines+2 {
			return
		}
	}
	t.Fatalf("possible goroutine leak: before=%d after=%d", baseGoroutines, runtime.NumGoroutine())
}

func TestDriverChildHelper(t *testing.T) {
	waitFile := os.Getenv("BUNPO_DRIVER_TEST_WAIT")
	if waitFile == "" {
		return
	}
	for ; ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(waitFile); err == nil {
			os.Exit(0)
		}
	}
}
