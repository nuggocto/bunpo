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

type fixedEntropy struct {
	seq int
}

func (f *fixedEntropy) Hex16() string {
	f.seq++
	return strings.Repeat("a", 31) + string(rune('0'+(f.seq%10)))
}

func (f *fixedEntropy) Int64N(int64) int64 { return 0 }

func startRunningLease(t *testing.T, retry RetryConfig) (*Machine, int64, uint64) {
	t.Helper()
	m, err := NewMachine(WorkerConfig{
		BuildID:       "hb-budget-build",
		Config:        queue.Config{Machines: 1, LeaseTTLMS: 10_000, BuildTimeoutMS: 600_000, MaxAttempts: 2, MaxBatch: 1},
		Tests:         []string{"tests/a.py"},
		PredictionsMS: []*int64{nil},
		Format:        "junit",
		Retry:         retry,
		Entropy:       &fixedEntropy{},
	})
	if err != nil {
		t.Fatalf("NewMachine: %v", err)
	}

	acts := m.Step(0, Event{Kind: EvStart})
	if len(acts) != 1 || acts[0].Kind != ActSend || acts[0].Req.Kind != ReqInit {
		t.Fatalf("start actions = %+v; want ReqInit", acts)
	}

	acts = m.Step(10, Event{
		Kind: EvResponse,
		OpID: acts[0].OpID,
		Resp: Response{Kind: ReqInit, Init: &queueapi.InitResponse{
			BuildID: "hb-budget-build", Created: true, Verdict: queue.VerdictRunning, TestCount: 1, DeadlineMS: 600_000,
		}},
	})
	var leaseOp uint64
	for _, a := range acts {
		if a.Kind == ActSend && a.Req.Kind == ReqLease {
			leaseOp = a.OpID
		}
	}
	if leaseOp == 0 {
		t.Fatalf("init actions = %+v; want ReqLease", acts)
	}

	acts = m.Step(20, Event{
		Kind: EvResponse,
		OpID: leaseOp,
		Resp: Response{Kind: ReqLease, Lease: &queueapi.LeaseResponse{
			Status:     queue.LeaseGranted,
			LeaseID:    1,
			DeadlineMS: 10_020,
			Attempts:   []queue.LeasedAttemptView{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Test: "tests/a.py"}},
		}},
	})
	var started bool
	var hbAt int64
	var hbGen uint64
	for _, a := range acts {
		if a.Kind == ActStartCommand && a.LeaseID == 1 {
			started = true
		}
		if a.Kind == ActSetTimer && a.TimerKind == TimerHeartbeat {
			hbAt = a.AtMS
			hbGen = a.TimerGen
		}
	}
	if !started || hbGen == 0 {
		t.Fatalf("lease actions = %+v; want ActStartCommand + heartbeat timer", acts)
	}

	return m, hbAt, hbGen
}

func TestMachineHeartbeatRetryBudgetStopsCommandWithoutReport(t *testing.T) {
	const budgetMS = int64(5_000)
	m, hbAt, hbGen := startRunningLease(t, RetryConfig{BudgetMS: budgetMS, BaseMS: 100, MaxWaitMS: 500})

	var stopped bool
	var stopLogCount int
	var sentReport bool
	var doneAct *Action
	firstFailAt := int64(-1)

	for step := range 40 {
		now := hbAt
		timerActs := m.Step(now, Event{Kind: EvTimer, TimerKind: TimerHeartbeat, TimerGen: hbGen})
		if len(timerActs) != 1 || timerActs[0].Kind != ActSend || timerActs[0].Req.Kind != ReqHeartbeat {
			t.Fatalf("step %d: timer actions = %+v; want ReqHeartbeat", step, timerActs)
		}

		respAt := now + 50
		if firstFailAt < 0 {
			firstFailAt = respAt
		}
		respActs := m.Step(respAt, Event{
			Kind: EvResponse,
			OpID: timerActs[0].OpID,
			Resp: Response{Kind: ReqHeartbeat, Err: queueapi.NewTransportError("connection reset")},
		})

		hbGen = 0
		stepWarns := 0
		for _, a := range respActs {
			switch a.Kind {
			case ActSetTimer:
				if a.TimerKind == TimerHeartbeat {
					hbAt = a.AtMS
					hbGen = a.TimerGen
				}
			case ActStopCommand:
				stopped = true
			case ActLogWarn:
				stepWarns++
			case ActSend:
				if a.Req.Kind == ReqReport {
					sentReport = true
				}
			case ActDone:
				cp := a
				doneAct = &cp
			}
		}

		if stopped {
			stopLogCount = stepWarns
			if respAt-firstFailAt < budgetMS {
				t.Fatalf("stopped early at elapsed %dms; want >= %dms", respAt-firstFailAt, budgetMS)
			}
			if stopLogCount != 1 {
				t.Fatalf("stop warning logs = %d; want 1", stopLogCount)
			}
			if doneAct != nil {
				t.Fatalf("emitted ActDone before EvCommandExited: %+v", doneAct)
			}
			if hbGen != 0 {
				t.Fatalf("scheduled another heartbeat timer after budget exhaustion")
			}
			break
		}

		if hbGen == 0 {
			t.Fatalf("step %d: expected retry heartbeat timer, got %+v", step, respActs)
		}
	}

	if !stopped {
		t.Fatalf("heartbeat failures never stopped the command past budget %dms", budgetMS)
	}

	// Command exits after SIGTERM/SIGKILL: must finish with an error and send no ReqReport.
	exitActs := m.Step(hbAt+100, Event{Kind: EvCommandExited, LeaseID: 1, ExitCode: -1, Signaled: true})
	for _, a := range exitActs {
		if a.Kind == ActReadReports || (a.Kind == ActSend && a.Req.Kind == ReqReport) {
			sentReport = true
		}
		if a.Kind == ActDone {
			cp := a
			doneAct = &cp
		}
	}

	if sentReport {
		t.Fatalf("expected no report read/send after heartbeat budget exhaustion")
	}
	if doneAct == nil || doneAct.Err == nil {
		t.Fatalf("exit actions = %+v; want ActDone with non-nil error", exitActs)
	}
	if !queueapi.IsTransientError(doneAct.Err) {
		t.Fatalf("ActDone.Err = %v; want wrapped transient error", doneAct.Err)
	}
}

func TestMachineHeartbeatSuccessResetsRetryBudget(t *testing.T) {
	const budgetMS = int64(5_000)
	m, hbAt, hbGen := startRunningLease(t, RetryConfig{BudgetMS: budgetMS, BaseMS: 100, MaxWaitMS: 500})

	sendHB := func(at int64, resp Response) []Action {
		t.Helper()
		timerActs := m.Step(at, Event{Kind: EvTimer, TimerKind: TimerHeartbeat, TimerGen: hbGen})
		if len(timerActs) != 1 || timerActs[0].Kind != ActSend || timerActs[0].Req.Kind != ReqHeartbeat {
			t.Fatalf("at=%d: timer actions = %+v; want ReqHeartbeat", at, timerActs)
		}
		acts := m.Step(at+10, Event{Kind: EvResponse, OpID: timerActs[0].OpID, Resp: resp})
		hbGen = 0
		for _, a := range acts {
			if a.Kind == ActSetTimer && a.TimerKind == TimerHeartbeat {
				hbAt = a.AtMS
				hbGen = a.TimerGen
			}
		}
		return acts
	}

	// Fail transiently for 3,000 ms (< 5,000 ms budget).
	firstWindowStart := hbAt + 10
	for hbAt+10-firstWindowStart < 3_000 {
		acts := sendHB(hbAt, Response{Kind: ReqHeartbeat, Err: queueapi.NewTransportError("transient")})
		if slices.ContainsFunc(acts, func(a Action) bool { return a.Kind == ActStopCommand }) {
			t.Fatalf("unexpected stop before budget at elapsed %dms", hbAt+10-firstWindowStart)
		}
	}

	// One successful heartbeat inside the window resets the failure timer.
	okActs := sendHB(hbAt, Response{Kind: ReqHeartbeat, Heartbeat: &queueapi.HeartbeatResponse{
		Held: true, DeadlineMS: hbAt + 10_010, Verdict: queue.VerdictRunning,
	}})
	if hbGen == 0 || slices.ContainsFunc(okActs, func(a Action) bool { return a.Kind == ActStopCommand }) {
		t.Fatalf("expected heartbeat timer after successful heartbeat, got %+v", okActs)
	}

	// Second failure run must get a full fresh 5,000 ms budget from its own start.
	secondWindowStart := hbAt + 10
	var stoppedAt int64
	for range 40 {
		respAt := hbAt + 10
		acts := sendHB(hbAt, Response{Kind: ReqHeartbeat, Err: queueapi.NewTransportError("transient-2")})
		if slices.ContainsFunc(acts, func(a Action) bool { return a.Kind == ActStopCommand }) {
			stoppedAt = respAt
			break
		}
		if hbGen == 0 {
			t.Fatalf("expected heartbeat retry timer, got %+v", acts)
		}
	}

	if stoppedAt == 0 {
		t.Fatalf("second failure run never exhausted budget")
	}
	if elapsed := stoppedAt - secondWindowStart; elapsed < budgetMS {
		t.Fatalf("second failure window stopped after %dms; want >= %dms (budget was not reset)", elapsed, budgetMS)
	}
}

func TestStatusMachineLateConflictOnFailedBuildSuppressesReport(t *testing.T) {
	sm := NewStatusMachine(StatusMachineConfig{BuildID: "failed-late-conflict", Entropy: &fixedEntropy{}})

	acts := sm.Step(0, Event{Kind: EvStart})
	if len(acts) != 1 || acts[0].Kind != ActSend || acts[0].Req.Kind != ReqStatus {
		t.Fatalf("start = %+v; want ReqStatus", acts)
	}

	// First /status: finished failed build with zero unresolved and zero conflicts.
	acts = sm.Step(10, Event{
		Kind: EvResponse,
		OpID: acts[0].OpID,
		Resp: Response{Kind: ReqStatus, Status: &queueapi.StatusResponse{
			BuildID: "failed-late-conflict",
			Verdict: queue.VerdictFailed,
			Counts:  queue.Counts{Total: 2, Passed: 1, Failed: 1},
		}},
	})
	if len(acts) != 1 || acts[0].Kind != ActSend || acts[0].Req.Kind != ReqListTests {
		t.Fatalf("after first status = %+v; want ReqListTests", acts)
	}

	// Last page of /tests: verdict is still failed.
	acts = sm.Step(20, Event{
		Kind: EvResponse,
		OpID: acts[0].OpID,
		Resp: Response{Kind: ReqListTests, ListTests: &queueapi.ListTestsResponse{
			BuildID: "failed-late-conflict",
			Verdict: queue.VerdictFailed,
			Tests: []queue.TestView{
				{TestIndex: 0, Test: "tests/a.py", State: queue.StatusPassed, Timed: true, DurationMS: 100},
				{TestIndex: 1, Test: "tests/b.py", State: queue.StatusFailed},
			},
		}},
	})
	if len(acts) != 1 || acts[0].Kind != ActSend || acts[0].Req.Kind != ReqStatus {
		t.Fatalf("after last page = %+v; want final confirmation ReqStatus", acts)
	}

	// Final /status shows a late init conflict while verdict remains failed.
	acts = sm.Step(30, Event{
		Kind: EvResponse,
		OpID: acts[0].OpID,
		Resp: Response{Kind: ReqStatus, Status: &queueapi.StatusResponse{
			BuildID: "failed-late-conflict",
			Verdict: queue.VerdictFailed,
			Counts:  queue.Counts{Total: 2, Passed: 1, Failed: 1},
			Conflicts: []queue.InitConflict{
				{RunnerID: "late-runner", GotDigest: "deadbeef", Reason: "test list differs", AtMS: 25},
			},
		}},
	})
	if len(acts) != 1 || acts[0].Kind != ActDone {
		t.Fatalf("final actions = %+v; want ActDone", acts)
	}
	done := acts[0]
	if done.Err != nil {
		t.Fatalf("unexpected err: %v", done.Err)
	}
	if done.Verdict != queue.VerdictFailed {
		t.Fatalf("verdict = %q; want failed", done.Verdict)
	}
	if done.HasReport || done.Report != nil {
		t.Fatalf("expected HasReport=false after late conflict on failed build")
	}
	if done.Status == nil || len(done.Status.Conflicts) != 1 {
		t.Fatalf("expected fresh status with 1 conflict, got %+v", done.Status)
	}
}

func TestStatusMachinePassedToUnresolvedRefetchesStatus(t *testing.T) {
	sm := NewStatusMachine(StatusMachineConfig{BuildID: "passed-to-unresolved", Entropy: &fixedEntropy{}})

	acts := sm.Step(0, Event{Kind: EvStart})
	acts = sm.Step(10, Event{
		Kind: EvResponse,
		OpID: acts[0].OpID,
		Resp: Response{Kind: ReqStatus, Status: &queueapi.StatusResponse{
			BuildID: "passed-to-unresolved",
			Verdict: queue.VerdictPassed,
			Counts:  queue.Counts{Total: 2, Passed: 2},
		}},
	})
	if len(acts) != 1 || acts[0].Req.Kind != ReqListTests {
		t.Fatalf("after status = %+v; want ReqListTests", acts)
	}

	// Page verdict flips passed -> unresolved mid-paging: must re-fetch /status immediately.
	nextCursor := 1
	acts = sm.Step(20, Event{
		Kind: EvResponse,
		OpID: acts[0].OpID,
		Resp: Response{Kind: ReqListTests, ListTests: &queueapi.ListTestsResponse{
			BuildID:    "passed-to-unresolved",
			Verdict:    queue.VerdictUnresolved,
			Tests:      []queue.TestView{{TestIndex: 0, Test: "tests/a.py", State: queue.StatusPassed, Timed: true, DurationMS: 100}},
			NextCursor: &nextCursor,
		}},
	})
	if len(acts) != 1 || acts[0].Kind != ActSend || acts[0].Req.Kind != ReqStatus {
		t.Fatalf("after verdict flip = %+v; want ReqStatus re-fetch", acts)
	}

	acts = sm.Step(30, Event{
		Kind: EvResponse,
		OpID: acts[0].OpID,
		Resp: Response{Kind: ReqStatus, Status: &queueapi.StatusResponse{
			BuildID: "passed-to-unresolved",
			Verdict: queue.VerdictUnresolved,
			Counts:  queue.Counts{Total: 2, Passed: 2},
			Conflicts: []queue.InitConflict{
				{RunnerID: "late-runner", GotDigest: "deadbeef", Reason: "config differs", AtMS: 15},
			},
		}},
	})
	if len(acts) != 1 || acts[0].Kind != ActDone || acts[0].Verdict != queue.VerdictUnresolved || acts[0].HasReport {
		t.Fatalf("done = %+v; want unresolved without report", acts)
	}
}
