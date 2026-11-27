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
	type wantOutcome struct {
		outcome queue.Outcome
		timed   bool
		ms      int64
	}

	makeAttempts := func(paths ...string) []queue.LeasedAttemptView {
		out := make([]queue.LeasedAttemptView, len(paths))
		for i, p := range paths {
			out[i] = queue.LeasedAttemptView{
				AttemptID:  queue.FormatAttemptID(i, 1),
				TestIndex:  i,
				Generation: 1,
				Test:       p,
			}
		}

		return out
	}

	cases := []struct {
		name      string
		attempts  []queue.LeasedAttemptView
		exitCode  int
		signaled  bool
		format    string
		files     map[string]junit.File
		readErr   error
		want      []wantOutcome
		wantExtra []string
	}{
		{
			name:     "exit 0 junit: passed timed, all-skipped, absent error",
			attempts: makeAttempts("a.py", "b.py", "c.py"),
			exitCode: 0,
			format:   "junit",
			files: map[string]junit.File{
				"a.py": {Seconds: ratMS(120), Cases: 2, Skipped: 1},
				"b.py": {Seconds: ratMS(0), Cases: 2, Skipped: 2},
			},
			want: []wantOutcome{
				{queue.OutcomePassed, true, 120},
				{queue.OutcomeSkipped, false, 0},
				{queue.OutcomeError, false, 0},
			},
		},

		{
			name:     "exit 0 junit: failed case still reports failed untimed",
			attempts: makeAttempts("a.py", "b.py"),
			exitCode: 0,
			format:   "junit",
			files: map[string]junit.File{
				"a.py": {Seconds: ratMS(45), Cases: 2, Failed: true},
				"b.py": {Seconds: ratMS(80), Cases: 1},
			},
			want: []wantOutcome{
				{queue.OutcomeFailed, false, 0},
				{queue.OutcomePassed, true, 80},
			},
		},

		{
			name:     "exit 0 lines: passed timed and failed untimed",
			attempts: makeAttempts("a.py", "b.py"),
			exitCode: 0,
			format:   "lines",
			files: map[string]junit.File{
				"a.py": {Seconds: ratMS(90), Cases: 1},
				"b.py": {Seconds: ratMS(0), Cases: 1, Failed: true},
			},
			want: []wantOutcome{
				{queue.OutcomePassed, true, 90},
				{queue.OutcomeFailed, false, 0},
			},
		},

		{
			name:     "non-zero junit with failed sibling: clean sibling untimed, skipped and absent error",
			attempts: makeAttempts("a.py", "b.py", "c.py", "d.py", "e.py"),
			exitCode: 1,
			format:   "junit",
			files: map[string]junit.File{
				"a.py": {Seconds: ratMS(50), Cases: 1, Failed: true},
				"b.py": {Seconds: ratMS(80), Cases: 2},
				"c.py": {Seconds: ratMS(0), Cases: 2, Skipped: 2},
				"d.py": {Seconds: ratMS(30), Cases: 2, Skipped: 1},
			},
			want: []wantOutcome{
				{queue.OutcomeFailed, false, 0},
				{queue.OutcomePassed, false, 0},
				{queue.OutcomeError, false, 0},
				{queue.OutcomeError, false, 0},
				{queue.OutcomeError, false, 0},
			},
		},

		{
			name:     "non-zero junit with no failed file: all error, never passed",
			attempts: makeAttempts("a.py", "b.py"),
			exitCode: 1,
			format:   "junit",
			files: map[string]junit.File{
				"a.py": {Seconds: ratMS(50), Cases: 1},
				"b.py": {Seconds: ratMS(80), Cases: 1},
			},
			want: []wantOutcome{
				{queue.OutcomeError, false, 0},
				{queue.OutcomeError, false, 0},
			},
		},

		{
			name:     "non-zero lines with failed sibling: non-failed becomes error",
			attempts: makeAttempts("a.py", "b.py"),
			exitCode: 1,
			format:   "lines",
			files: map[string]junit.File{
				"a.py": {Seconds: ratMS(50), Cases: 1, Failed: true},
				"b.py": {Seconds: ratMS(80), Cases: 1},
			},
			want: []wantOutcome{
				{queue.OutcomeFailed, false, 0},
				{queue.OutcomeError, false, 0},
			},
		},

		{
			name:     "signaled junit with failed sibling: non-failed becomes error",
			attempts: makeAttempts("a.py", "b.py"),
			exitCode: -1,
			signaled: true,
			format:   "junit",
			files: map[string]junit.File{
				"a.py": {Seconds: ratMS(50), Cases: 1, Failed: true},
				"b.py": {Seconds: ratMS(80), Cases: 1},
			},
			want: []wantOutcome{
				{queue.OutcomeFailed, false, 0},
				{queue.OutcomeError, false, 0},
			},
		},

		{
			name:     "unreadable report: all error",
			attempts: makeAttempts("a.py", "b.py"),
			exitCode: 1,
			format:   "junit",
			readErr:  errors.New("missing report"),
			want: []wantOutcome{
				{queue.OutcomeError, false, 0},
				{queue.OutcomeError, false, 0},
			},
		},

		{
			name:     "extra unassigned file in report: all error plus ExtraFilesError",
			attempts: makeAttempts("a.py", "b.py"),
			exitCode: 0,
			format:   "junit",
			files: map[string]junit.File{
				"a.py":     {Seconds: ratMS(50), Cases: 1},
				"b.py":     {Seconds: ratMS(80), Cases: 1},
				"extra.py": {Seconds: ratMS(10), Cases: 1},
			},
			want: []wantOutcome{
				{queue.OutcomeError, false, 0},
				{queue.OutcomeError, false, 0},
			},
			wantExtra: []string{"extra.py"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, extra := mapOutcomes(tc.attempts, tc.exitCode, tc.signaled, tc.format, tc.files, tc.readErr)
			if len(tc.wantExtra) == 0 {
				if extra != nil {
					t.Fatalf("unexpected ExtraFilesError: %v", extra)
				}
			} else if extra == nil || !slices.Equal(extra.Extra, tc.wantExtra) {
				t.Fatalf("extra = %v; want %v", extra, tc.wantExtra)
			}

			if len(got) != len(tc.want) {
				t.Fatalf("len(results) = %d; want %d", len(got), len(tc.want))
			}

			for i, w := range tc.want {
				r := got[i]
				if r.Outcome != w.outcome || r.Timed != w.timed || r.DurationMS != w.ms {
					t.Errorf("result[%d] (%s) = (%s, timed=%t, ms=%d); want (%s, timed=%t, ms=%d)",
						i, tc.attempts[i].Test, r.Outcome, r.Timed, r.DurationMS, w.outcome, w.timed, w.ms)
				}
			}
		})
	}
}

func TestFilteredChildEnvStripsTokens(t *testing.T) {
	in := []string{
		"PATH=/usr/bin",
		"BUNPO_QUEUE_TOKEN=secret-write",
		"BUNPO_QUEUE_READ_TOKEN=secret-read",
		"bunpo_queue_token=lower",
		"BUNPO_TESTS_FILE=/old",
	}
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
	if outPath := os.Getenv("BUNPO_DRIVER_RECORD_BATCH_PATH"); outPath != "" {
		testsFile := os.Getenv("BUNPO_TESTS_FILE")
		if err := os.WriteFile(outPath, []byte(testsFile), 0o600); err != nil {
			os.Exit(2)
		}

		// Write a valid lines report for every file in the batch so Run finishes cleanly.
		raw, err := os.ReadFile(testsFile)
		if err != nil {
			os.Exit(3)
		}

		var rep strings.Builder
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			if line != "" {
				rep.WriteString(line + " 10\n")
			}
		}

		if repPath := os.Getenv("BUNPO_DRIVER_REPORT_PATH"); repPath != "" {
			if err := os.WriteFile(repPath, []byte(rep.String()), 0o600); err != nil {
				os.Exit(4)
			}
		}

		os.Exit(0)
	}

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

func TestRunSameMachineIndexUsesDistinctBatchFiles(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
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

	sharedDir := t.TempDir()
	idx := 0
	var batchPaths [2]string
	for i := range 2 {
		recordPath := filepath.Join(sharedDir, "seen-"+string(rune('0'+i))+".txt")
		reportPath := filepath.Join(sharedDir, "rep-"+string(rune('0'+i))+".lines")
		t.Setenv("BUNPO_DRIVER_RECORD_BATCH_PATH", recordPath)
		t.Setenv("BUNPO_DRIVER_REPORT_PATH", reportPath)

		v, err := Run(context.Background(), RunConfig{
			Client:        client,
			BuildID:       "same-index-build-" + string(rune('a'+i)),
			MachineIndex:  &idx,
			Config:        queue.Config{Machines: 1, LeaseTTLMS: 1000, BuildTimeoutMS: 30_000, MaxAttempts: 2, MaxBatch: 1},
			Tests:         []string{"tests/a.py"},
			PredictionsMS: []*int64{nil},
			Dir:           sharedDir,
			Command:       []string{os.Args[0], "-test.run=^TestDriverChildHelper$"},
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

		if err != nil || v != queue.VerdictPassed {
			t.Fatalf("runner %d: verdict=%q err=%v", i, v, err)
		}

		got, err := os.ReadFile(recordPath)
		if err != nil {
			t.Fatalf("read recorded batch path %d: %v", i, err)
		}

		batchPaths[i] = string(got)
	}

	if batchPaths[0] == "" || batchPaths[1] == "" {
		t.Fatalf("expected non-empty batch paths, got %q and %q", batchPaths[0], batchPaths[1])
	}

	if batchPaths[0] == batchPaths[1] {
		t.Fatalf("two runners with same --index and --dir shared batch file %q", batchPaths[0])
	}
}
