package queue_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/timing"
)

func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }

func defaultConfig() queue.Config {
	return queue.Config{
		Machines:       2,
		LeaseTTLMS:     queue.DefaultLeaseTTLMS,
		BuildTimeoutMS: queue.DefaultBuildTimeoutMS,
		MaxAttempts:    queue.DefaultMaxAttempts,
		MaxBatch:       queue.DefaultMaxBatch,
	}
}

func mustApply(t *testing.T, s *queue.State, events []queue.Event) {
	t.Helper()
	for _, ev := range events {
		if err := s.Apply(ev); err != nil {
			t.Fatalf("Apply(%s): %v", ev.Type, err)
		}
	}
}

func assertQueueErr(t *testing.T, err error, wantStatus int, wantCode string) {
	t.Helper()
	var qe *queue.Error
	if !errors.As(err, &qe) || qe.HTTPStatus != wantStatus || qe.Code != wantCode {
		t.Fatalf("err = %v; want *queue.Error (%d %s)", err, wantStatus, wantCode)
	}
}

func TestValidIdentifiers(t *testing.T) {
	exactTestID := strings.Repeat("a", queue.MaxTestIDBytes)
	cases := []struct {
		name      string
		validator func(string) bool
		input     string
		want      bool
	}{
		{"build id: single char", queue.ValidBuildID, "a", true},
		{"build id: punctuation allowed", queue.ValidBuildID, "run-123_4.5", true},
		{"build id: exact max 128", queue.ValidBuildID, strings.Repeat("a", 128), true},
		{"build id: empty", queue.ValidBuildID, "", false},
		{"build id: leading dash", queue.ValidBuildID, "-bad", false},
		{"build id: leading dot", queue.ValidBuildID, ".bad", false},
		{"build id: double dot", queue.ValidBuildID, "a..b", false},
		{"build id: slash", queue.ValidBuildID, "a/b", false},
		{"build id: space", queue.ValidBuildID, "a b", false},
		{"build id: 129 bytes", queue.ValidBuildID, strings.Repeat("a", 129), false},
		{"runner id: exact max 64", queue.ValidRunnerID, strings.Repeat("r", 64), true},
		{"runner id: empty", queue.ValidRunnerID, "", false},
		{"runner id: 65 bytes", queue.ValidRunnerID, strings.Repeat("r", 65), false},
		{"runner id: slash", queue.ValidRunnerID, "bad/id", false},
		{"request id: exact max 128", queue.ValidRequestID, strings.Repeat("q", 128), true},
		{"request id: empty", queue.ValidRequestID, "", false},
		{"request id: 129 bytes", queue.ValidRequestID, strings.Repeat("q", 129), false},
		{"test id: exact max bytes", queue.ValidTestID, exactTestID, true},
		{"test id: one byte over max", queue.ValidTestID, exactTestID + "x", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.validator(tc.input); got != tc.want {
				t.Fatalf("validator(%q) = %t; want %t", tc.input, got, tc.want)
			}
		})
	}
}

func TestInitValidationAndExactLimits(t *testing.T) {
	lim := queue.Limits{MinLeaseTTLMS: queue.MinLeaseTTLMS, MaxTests: 3, MaxRunners: 2}
	baseCmd := func(mod func(*queue.InitCommand)) queue.InitCommand {
		c := queue.InitCommand{
			BuildID:       "b1",
			RunnerID:      "r1",
			Config:        defaultConfig(),
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		}
		if mod != nil {
			mod(&c)
		}
		return c
	}

	cases := []struct {
		name   string
		atMS   int64
		cmd    queue.InitCommand
		wantOK bool
	}{
		{"at_ms 0 accepted", 0, baseCmd(nil), true},
		{"at_ms -1 rejected", -1, baseCmd(nil), false},
		{"machines min (1) accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.Machines = 1 }), true},
		{"machines below min (0) rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.Machines = 0 }), false},
		{"machines max (256) accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.Machines = queue.MaxMachines }), true},
		{"machines past max (257) rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.Machines = queue.MaxMachines + 1 }), false},
		{"lease_ttl min accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.LeaseTTLMS = queue.MinLeaseTTLMS }), true},
		{"lease_ttl below min rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.LeaseTTLMS = queue.MinLeaseTTLMS - 1 }), false},
		{"lease_ttl max accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.LeaseTTLMS = queue.MaxLeaseTTLMS }), true},
		{"lease_ttl past max rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.LeaseTTLMS = queue.MaxLeaseTTLMS + 1 }), false},
		{"build_timeout == lease_ttl accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.BuildTimeoutMS = c.Config.LeaseTTLMS }), true},
		{"build_timeout < lease_ttl rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.BuildTimeoutMS = c.Config.LeaseTTLMS - 1 }), false},
		{"build_timeout max accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.BuildTimeoutMS = queue.MaxBuildTimeoutMS }), true},
		{"build_timeout past max rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.BuildTimeoutMS = queue.MaxBuildTimeoutMS + 1 }), false},
		{"max_attempts min (1) accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxAttempts = 1 }), true},
		{"max_attempts below min (0) rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxAttempts = 0 }), false},
		{"max_attempts max (10) accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxAttempts = queue.MaxAttemptsLimit }), true},
		{"max_attempts past max (11) rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxAttempts = queue.MaxAttemptsLimit + 1 }), false},
		{"max_batch min (1) accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxBatch = 1 }), true},
		{"max_batch below min (0) rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxBatch = 0 }), false},
		{"max_batch max (200) accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxBatch = queue.MaxBatchLimit }), true},
		{"max_batch past max (201) rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Config.MaxBatch = queue.MaxBatchLimit + 1 }), false},
		{"machine_index 0 accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.MachineIndex = intPtr(0) }), true},
		{"machine_index -1 rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.MachineIndex = intPtr(-1) }), false},
		{"machine_index 255 accepted", 1000, baseCmd(func(c *queue.InitCommand) { c.MachineIndex = intPtr(255) }), true},
		{"machine_index 256 rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.MachineIndex = intPtr(256) }), false},
		{"tests at MaxTests (3) accepted", 1000, baseCmd(func(c *queue.InitCommand) {
			c.Tests = []string{"a.py", "b.py", "c.py"}
			c.PredictionsMS = []*int64{int64Ptr(0), int64Ptr(timing.MaxDurationMS), nil}
		}), true},
		{"tests past MaxTests (4) rejected", 1000, baseCmd(func(c *queue.InitCommand) {
			c.Tests = []string{"a.py", "b.py", "c.py", "d.py"}
			c.PredictionsMS = make([]*int64, 4)
		}), false},
		{"invalid build ID rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.BuildID = "../bad" }), false},
		{"empty test list rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Tests, c.PredictionsMS = nil, nil }), false},
		{"unsorted tests rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Tests, c.PredictionsMS = []string{"b.py", "a.py"}, make([]*int64, 2) }), false},
		{"duplicate tests rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Tests, c.PredictionsMS = []string{"a.py", "a.py"}, make([]*int64, 2) }), false},
		{"non-canonical test path rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.Tests = []string{"./a.py"} }), false},
		{"predictions length mismatch rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.PredictionsMS = []*int64{} }), false},
		{"prediction -1 rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.PredictionsMS = []*int64{int64Ptr(-1)} }), false},
		{"prediction past MaxDurationMS rejected", 1000, baseCmd(func(c *queue.InitCommand) { c.PredictionsMS = []*int64{int64Ptr(timing.MaxDurationMS + 1)} }), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := queue.NewState(lim)
			evs, _, err := queue.DecideInit(s, tc.atMS, tc.cmd)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("DecideInit failed: %v", err)
				}
				mustApply(t, s, evs)
				return
			}
			assertQueueErr(t, err, 400, queue.ErrCodeInvalidRequest)
		})
	}
}

func TestInitConflictsBoundedAndFailedPrecedence(t *testing.T) {
	s := queue.NewState(queue.DefaultLimits())
	evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID:       "b-conf",
		RunnerID:      "r0",
		Config:        defaultConfig(),
		Tests:         []string{"a.py", "b.py"},
		PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)
	for i := range 12 {
		cEvs, _, err := queue.DecideInit(s, int64(1100+i), queue.InitCommand{
			BuildID:       "b-conf",
			RunnerID:      fmt.Sprintf("bad-%d", i),
			Config:        defaultConfig(),
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{int64Ptr(100)},
		})
		assertQueueErr(t, err, 409, queue.ErrCodeInitConflict)
		if (i < queue.MaxInitConflicts && len(cEvs) != 1) || (i >= queue.MaxInitConflicts && len(cEvs) != 0) {
			t.Fatalf("conflict %d emitted %d events", i, len(cEvs))
		}
		mustApply(t, s, cEvs)
	}
	if len(s.Conflicts) != queue.MaxInitConflicts || !s.ConflictsTruncated {
		t.Fatalf("conflicts = %d truncated=%t", len(s.Conflicts), s.ConflictsTruncated)
	}
	for step := range 2 {
		evs, l, err := queue.DecideLease(s, int64(2000+step*200), queue.LeaseCommand{RunnerID: "r0", RequestID: fmt.Sprintf("q%d", step)})
		if err != nil || l.Status != queue.LeaseGranted {
			break
		}
		mustApply(t, s, evs)
		results := make([]queue.ReportResult, len(l.Attempts))
		for i, a := range l.Attempts {
			out, timed, ms := queue.OutcomePassed, true, int64(10)
			if step == 0 && i == 0 {
				out, timed, ms = queue.OutcomeFailed, false, 0
			}
			results[i] = queue.ReportResult{
				AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: a.Generation,
				Outcome: out, Timed: timed, DurationMS: ms,
			}
		}
		evs, _, err = queue.DecideReport(s, int64(2100+step*200), queue.ReportCommand{RunnerID: "r0", LeaseID: l.LeaseID, Results: results})
		if err != nil {
			t.Fatal(err)
		}
		mustApply(t, s, evs)
	}
	if s.Verdict() != queue.VerdictFailed {
		t.Fatalf("verdict = %s; want failed", s.Verdict())
	}
}

func TestHostileStaleAndConflictHistoryBounded(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	s := queue.NewState(lim)
	cfg := defaultConfig()
	cfg.LeaseTTLMS = 2000
	cfg.MaxAttempts = 3
	cfg.MaxBatch = 1
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{BuildID: "b-hist", RunnerID: "r0", Config: cfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{int64Ptr(100)}})
	mustApply(t, s, evs)
	evs, l1, _ := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r0", RequestID: "q1"})
	mustApply(t, s, evs)
	evs, l2, _ := queue.DecideLease(s, 4000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q2"})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideReport(s, 4100, queue.ReportCommand{
		RunnerID: "r1", LeaseID: l2.LeaseID,
		Results: []queue.ReportResult{{AttemptID: "0.2", TestIndex: 0, Generation: 2, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 50}},
	})
	mustApply(t, s, evs)
	for i := range 20 {
		evs, rep, err := queue.DecideReport(s, int64(5000+i), queue.ReportCommand{
			RunnerID: "r0", LeaseID: l1.LeaseID,
			Results: []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: int64(10 + i)}},
		})
		if err != nil || rep.Results[0].Disposition != queue.DispositionStale {
			t.Fatalf("stale %d: rep=%+v err=%v", i, rep, err)
		}
		if i < queue.MaxTestHistory {
			mustApply(t, s, evs)
		} else if len(evs) != 0 {
			t.Fatalf("stale %d past cap emitted %d events; want 0", i, len(evs))
		}
	}
	if len(s.Tests[0].History) != queue.MaxTestHistory || !s.Tests[0].HistoryTruncated {
		t.Fatalf("history len=%d truncated=%t", len(s.Tests[0].History), s.Tests[0].HistoryTruncated)
	}
}

func TestTooManyRunners(t *testing.T) {
	lim := queue.Limits{MinLeaseTTLMS: 1000, MaxTests: 10, MaxRunners: 2}
	s := queue.NewState(lim)
	cfg := defaultConfig()
	cfg.LeaseTTLMS = 2000
	cfg.MaxBatch = 1
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID: "b-run", RunnerID: "r0", Config: cfg,
		Tests:         []string{"a.py", "b.py", "c.py"},
		PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100), int64Ptr(100)},
	})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r2", RequestID: "q2"})
	mustApply(t, s, evs)
	_, _, err := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r3", RequestID: "q3"})
	assertQueueErr(t, err, 429, queue.ErrCodeTooManyRunners)
}

func TestReportValidation(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	s := queue.NewState(lim)
	cfg := defaultConfig()
	cfg.LeaseTTLMS = 2000
	cfg.MaxBatch = 1
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID: "b-rep", RunnerID: "r1", Config: cfg,
		Tests:         []string{"a.py", "b.py"},
		PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100)},
	})
	mustApply(t, s, evs)
	evs, l1, _ := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	mustApply(t, s, evs)
	a := l1.Attempts[0]

	cases := []struct {
		name     string
		cmd      queue.ReportCommand
		wantCode string
	}{
		{
			name:     "unknown lease_id",
			cmd:      queue.ReportCommand{RunnerID: "r1", LeaseID: 999},
			wantCode: queue.ErrCodeUnknownLease,
		},
		{
			name:     "runner_id mismatch",
			cmd:      queue.ReportCommand{RunnerID: "r2", LeaseID: l1.LeaseID},
			wantCode: queue.ErrCodeRunnerMismatch,
		},
		{
			name:     "missing results",
			cmd:      queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID},
			wantCode: queue.ErrCodeInvalidRequest,
		},
		{
			name: "failed outcome cannot be timed",
			cmd: queue.ReportCommand{
				RunnerID: "r1", LeaseID: l1.LeaseID,
				Results: []queue.ReportResult{{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: 1, Outcome: queue.OutcomeFailed, Timed: true, DurationMS: 10}},
			},
			wantCode: queue.ErrCodeInvalidRequest,
		},
		{
			name: "untimed outcome must have duration_ms 0",
			cmd: queue.ReportCommand{
				RunnerID: "r1", LeaseID: l1.LeaseID,
				Results: []queue.ReportResult{{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: 1, Outcome: queue.OutcomePassed, Timed: false, DurationMS: 10}},
			},
			wantCode: queue.ErrCodeInvalidRequest,
		},
		{
			name: "mismatched attempt_id string",
			cmd: queue.ReportCommand{
				RunnerID: "r1", LeaseID: l1.LeaseID,
				Results: []queue.ReportResult{{AttemptID: "9.9", TestIndex: a.TestIndex, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10}},
			},
			wantCode: queue.ErrCodeInvalidRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := queue.DecideReport(s, 1200, tc.cmd)
			assertQueueErr(t, err, 400, tc.wantCode)
		})
	}
}

func makeListTestsFixture(t *testing.T) (*queue.State, int64) {
	t.Helper()
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	s := queue.NewState(lim)
	cfg := queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: 10_000, MaxAttempts: 2, MaxBatch: 1}
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID: "b-st", RunnerID: "r1", Config: cfg,
		Tests:         []string{"a.py", "b.py", "c.py"},
		PredictionsMS: []*int64{int64Ptr(300), int64Ptr(200), int64Ptr(100)},
	})
	mustApply(t, s, evs)
	leaseAndRep := func(tLease, tRep int64, q, att string, idx int, out queue.Outcome, timed bool, ms int64) {
		evs, l, _ := queue.DecideLease(s, tLease, queue.LeaseCommand{RunnerID: "r1", RequestID: q})
		mustApply(t, s, evs)
		evs, _, _ = queue.DecideReport(s, tRep, queue.ReportCommand{
			RunnerID: "r1", LeaseID: l.LeaseID,
			Results: []queue.ReportResult{{AttemptID: att, TestIndex: idx, Generation: 1, Outcome: out, Timed: timed, DurationMS: ms}},
		})
		mustApply(t, s, evs)
	}
	leaseAndRep(1000, 1200, "q1", "0.1", 0, queue.OutcomePassed, true, 350)
	leaseAndRep(1300, 1400, "q2", "1.1", 1, queue.OutcomeFailed, false, 0)
	evs, l3, _ := queue.DecideLease(s, 1500, queue.LeaseCommand{RunnerID: "r1", RequestID: "q3"})
	mustApply(t, s, evs)
	return s, l3.LeaseID
}

func TestListTestsFiltersAndPaging(t *testing.T) {
	s, _ := makeListTestsFixture(t)

	cases := []struct {
		name      string
		cmd       queue.ListTestsCommand
		wantPaths []string
		wantNext  *int
		wantErr   bool
	}{
		{
			name:      "all tests default limit",
			cmd:       queue.ListTestsCommand{Limit: queue.MaxTestsPageLimit},
			wantPaths: []string{"a.py", "b.py", "c.py"},
		},
		{
			name:      "page 1 with limit 1",
			cmd:       queue.ListTestsCommand{Cursor: 0, Limit: 1},
			wantPaths: []string{"a.py"},
			wantNext:  intPtr(1),
		},
		{
			name:      "page 2 from cursor 1 limit 2",
			cmd:       queue.ListTestsCommand{Cursor: 1, Limit: 2},
			wantPaths: []string{"b.py", "c.py"},
		},
		{
			name:      "state filter failed",
			cmd:       queue.ListTestsCommand{States: []queue.TestStatus{queue.StatusFailed}},
			wantPaths: []string{"b.py"},
		},
		{
			name:      "retried filter with no retries",
			cmd:       queue.ListTestsCommand{Retried: true},
			wantPaths: []string{},
		},
		{
			name:    "negative cursor rejected",
			cmd:     queue.ListTestsCommand{Cursor: -1},
			wantErr: true,
		},
		{
			name:    "limit past max rejected",
			cmd:     queue.ListTestsCommand{Limit: queue.MaxTestsPageLimit + 1},
			wantErr: true,
		},
		{
			name:    "unknown state rejected",
			cmd:     queue.ListTestsCommand{States: []queue.TestStatus{"bogus"}},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, resp, err := queue.DecideListTests(s, 1600, tc.cmd)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("DecideListTests(%+v) succeeded; want error", tc.cmd)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecideListTests(%+v) err = %v", tc.cmd, err)
			}
			gotPaths := make([]string, len(resp.Tests))
			for i, tv := range resp.Tests {
				gotPaths[i] = tv.Test
			}
			if !slices.Equal(gotPaths, tc.wantPaths) {
				t.Fatalf("paths = %v; want %v", gotPaths, tc.wantPaths)
			}
			if (resp.NextCursor == nil) != (tc.wantNext == nil) || (resp.NextCursor != nil && *resp.NextCursor != *tc.wantNext) {
				t.Fatalf("next_cursor = %v; want %v", resp.NextCursor, tc.wantNext)
			}
		})
	}
}

func TestFinalizeOverlayAtDeadline(t *testing.T) {
	s, leaseID := makeListTestsFixture(t)

	if hbEvs, hbResp, err := queue.DecideHeartbeat(s, s.DeadlineMS, queue.HeartbeatCommand{RunnerID: "r1", LeaseID: leaseID}); err != nil || hbResp.Held || len(hbEvs) == 0 {
		t.Fatalf("heartbeat at exact deadline: evs=%v resp=%+v err=%v", hbEvs, hbResp, err)
	}
	if stEvs, stResp, err := queue.DecideStatus(s, s.DeadlineMS); err != nil || len(stEvs) != 1 || stResp.Verdict != queue.VerdictFailed || len(stResp.Slowest) != 1 || stResp.Slowest[0].Test != "a.py" || stResp.Runners[0].ActiveLeaseID != 0 {
		t.Fatalf("finalizing status: evs=%v resp=%+v err=%v", stEvs, stResp, err)
	}
	if ltEvs, all, err := queue.DecideListTests(s, s.DeadlineMS, queue.ListTestsCommand{Limit: queue.MaxTestsPageLimit}); err != nil || len(ltEvs) != 1 || len(all.Tests) != 3 || all.Tests[2].Attempts[0].Status != queue.AttemptExpired || all.Tests[2].Attempts[0].FinishedAtMS != s.DeadlineMS {
		t.Fatalf("finalizing list overlay: evs=%v resp=%+v err=%v", ltEvs, all, err)
	}
}

func TestApplyRejectsDamagedEventsAtomically(t *testing.T) {
	lim := queue.Limits{MinLeaseTTLMS: 1000, MaxTests: 10, MaxRunners: 1}
	cfg1 := defaultConfig()
	cfg1.LeaseTTLMS = 2000
	cfg1.MaxBatch = 1

	cfg2 := cfg1
	cfg2.Machines = 1
	cfg2.MaxBatch = 2

	makeActiveSingle := func() *queue.State {
		s := queue.NewState(lim)
		evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{
			BuildID: "b-dmg", RunnerID: "r1", Config: cfg1,
			Tests: []string{"a.py", "b.py"}, PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100)},
		})
		mustApply(t, s, evs)
		evs, _, _ = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
		mustApply(t, s, evs)
		return s
	}

	makeActiveDouble := func() *queue.State {
		s := queue.NewState(lim)
		evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{
			BuildID: "b-dmg2", RunnerID: "r1", Config: cfg2,
			Tests: []string{"a.py", "b.py", "c.py"}, PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100), int64Ptr(100)},
		})
		mustApply(t, s, evs)
		evs, _, _ = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
		mustApply(t, s, evs)
		return s
	}

	makeExpiredSingle := func() *queue.State {
		s := makeActiveSingle()
		mustApply(t, s, []queue.Event{{Type: queue.EventExpired, AtMS: 3500, Expired: &queue.ExpiredData{LeaseID: 1}}})
		return s
	}

	cases := []struct {
		name  string
		setup func() *queue.State
		ev    queue.Event
	}{
		{
			name:  "initialized with deadline_ms < at_ms",
			setup: func() *queue.State { return queue.NewState(lim) },
			ev: queue.Event{
				Type: queue.EventInitialized, AtMS: 1000,
				Initialized: &queue.InitializedData{
					BuildID: "b", Digest: "d", Config: cfg1, DeadlineMS: 500,
					Tests: []string{"a.py"}, PredictionsMS: []*int64{nil},
				},
			},
		},
		{
			name:  "expired with negative at_ms",
			setup: makeActiveSingle,
			ev:    queue.Event{Type: queue.EventExpired, AtMS: -1, Expired: &queue.ExpiredData{LeaseID: 1}},
		},
		{
			name:  "leased with zero attempts (events.go:258 lower)",
			setup: makeActiveSingle,
			ev:    queue.Event{Type: queue.EventLeased, AtMS: 1100, Leased: &queue.LeasedData{LeaseID: 2, RunnerID: "r2", RequestID: "q2", DeadlineMS: 3100}},
		},
		{
			name:  "leased with attempts > MaxBatch (events.go:258 upper)",
			setup: makeExpiredSingle,
			ev: queue.Event{
				Type: queue.EventLeased, AtMS: 3600,
				Leased: &queue.LeasedData{
					LeaseID: 2, RunnerID: "r1", RequestID: "q2", DeadlineMS: 5600,
					Attempts: []queue.LeaseAttempt{{TestIndex: 0, Generation: 2}, {TestIndex: 1, Generation: 1}},
				},
			},
		},
		{
			name:  "leased exceeding MaxRunners",
			setup: makeActiveSingle,
			ev: queue.Event{
				Type: queue.EventLeased, AtMS: 1100,
				Leased: &queue.LeasedData{
					LeaseID: 2, RunnerID: "r2", RequestID: "q2", DeadlineMS: 3100,
					Attempts: []queue.LeaseAttempt{{TestIndex: 1, Generation: 1}},
				},
			},
		},
		{
			name:  "reported with zero results (events.go:338 lower)",
			setup: makeActiveSingle,
			ev:    queue.Event{Type: queue.EventReported, AtMS: 1100, Reported: &queue.ReportedData{LeaseID: 1, RunnerID: "r1", CloseLease: true}},
		},
		{
			name:  "reported with more results than lease attempts (events.go:338 upper)",
			setup: makeActiveSingle,
			ev: queue.Event{
				Type: queue.EventReported, AtMS: 1100,
				Reported: &queue.ReportedData{
					LeaseID: 1, RunnerID: "r1", CloseLease: true,
					Results: []queue.ReportedAttempt{
						{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
						{TestIndex: 1, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
					},
				},
			},
		},
		{
			name:  "reported closing an already-expired lease (events.go:341 status)",
			setup: makeExpiredSingle,
			ev: queue.Event{
				Type: queue.EventReported, AtMS: 3600,
				Reported: &queue.ReportedData{
					LeaseID: 1, RunnerID: "r1", CloseLease: true,
					Results: []queue.ReportedAttempt{
						{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
					},
				},
			},
		},
		{
			name:  "reported closing 2-attempt lease with only 1 result (events.go:341 len)",
			setup: makeActiveDouble,
			ev: queue.Event{
				Type: queue.EventReported, AtMS: 1100,
				Reported: &queue.ReportedData{
					LeaseID: 1, RunnerID: "r1", CloseLease: true,
					Results: []queue.ReportedAttempt{
						{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
					},
				},
			},
		},
		{
			name:  "reported active lease with CloseLease=false",
			setup: makeActiveSingle,
			ev: queue.Event{
				Type: queue.EventReported, AtMS: 1100,
				Reported: &queue.ReportedData{
					LeaseID: 1, RunnerID: "r1", CloseLease: false,
					Results: []queue.ReportedAttempt{
						{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
					},
				},
			},
		},
		{
			name:  "reported with unsorted/duplicate test_index (events.go:354)",
			setup: makeActiveDouble,
			ev: queue.Event{
				Type: queue.EventReported, AtMS: 1100,
				Reported: &queue.ReportedData{
					LeaseID: 1, RunnerID: "r1", CloseLease: true,
					Results: []queue.ReportedAttempt{
						{TestIndex: 1, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
						{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
					},
				},
			},
		},
		{
			name:  "reported with test_index not in lease (events.go:358 !ok)",
			setup: makeActiveSingle,
			ev: queue.Event{
				Type: queue.EventReported, AtMS: 1100,
				Reported: &queue.ReportedData{
					LeaseID: 1, RunnerID: "r1", CloseLease: true,
					Results: []queue.ReportedAttempt{
						{TestIndex: 1, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
					},
				},
			},
		},
		{
			name:  "reported with wrong generation for lease attempt (events.go:358 gen)",
			setup: makeActiveSingle,
			ev: queue.Event{
				Type: queue.EventReported, AtMS: 1100,
				Reported: &queue.ReportedData{
					LeaseID: 1, RunnerID: "r1", CloseLease: true,
					Results: []queue.ReportedAttempt{
						{TestIndex: 0, Generation: 9, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.setup()
			before := cloneState(t, s)
			if err := s.Apply(tc.ev); err == nil || !equalExact(t, s, before) {
				t.Fatalf("Apply(%s): err=%v or state mutated", tc.ev.Type, err)
			}
		})
	}
}
