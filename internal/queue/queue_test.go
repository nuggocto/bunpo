package queue_test

import (
	"errors"
	"fmt"
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
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v; want *queue.Error(%d, %s)", err, wantStatus, wantCode)
	}
	if qe.HTTPStatus != wantStatus || qe.Code != wantCode {
		t.Fatalf("err = (%d, %q, %q); want (%d, %q)", qe.HTTPStatus, qe.Code, qe.Message, wantStatus, wantCode)
	}
}

func TestValidIdentifiers(t *testing.T) {
	validBuilds := []string{"a", "build-1", "Run_2026.11.23", strings.Repeat("a", 128)}
	for _, id := range validBuilds {
		if !queue.ValidBuildID(id) {
			t.Errorf("ValidBuildID(%q) = false; want true", id)
		}
	}
	invalidBuilds := []string{"", ".", "..", ".hidden", "-dash", "_under", "a..b", "a/b", "a b", strings.Repeat("a", 129)}
	for _, id := range invalidBuilds {
		if queue.ValidBuildID(id) {
			t.Errorf("ValidBuildID(%q) = true; want false", id)
		}
	}

	if !queue.ValidRunnerID("0123456789abcdef0123456789abcdef") || queue.ValidRunnerID("") || queue.ValidRunnerID(strings.Repeat("x", 65)) || queue.ValidRunnerID("bad/id") {
		t.Error("ValidRunnerID boundary check failed")
	}
	if !queue.ValidRequestID("req-1.a_b") || queue.ValidRequestID("") || queue.ValidRequestID(strings.Repeat("x", 129)) || queue.ValidRequestID("bad id") {
		t.Error("ValidRequestID boundary check failed")
	}
	if !queue.ValidTestID("tests/a.py") || queue.ValidTestID("") || queue.ValidTestID("./a.py") || queue.ValidTestID("a/../b.py") || queue.ValidTestID(strings.Repeat("a", queue.MaxTestIDBytes+1)) {
		t.Error("ValidTestID boundary check failed")
	}
}

func TestInitValidationAndLimits(t *testing.T) {
	base := func() queue.InitCommand {
		return queue.InitCommand{
			BuildID:       "build-1",
			RunnerID:      "runner-1",
			MachineIndex:  intPtr(0),
			Config:        defaultConfig(),
			Tests:         []string{"a.py", "b.py"},
			PredictionsMS: []*int64{int64Ptr(100), nil},
		}
	}

	tests := []struct {
		name   string
		nowMS  int64
		mutate func(*queue.InitCommand)
	}{
		{"negative now", -1, func(c *queue.InitCommand) {}},
		{"bad build id ..", 1000, func(c *queue.InitCommand) { c.BuildID = ".." }},
		{"bad runner id", 1000, func(c *queue.InitCommand) { c.RunnerID = "bad/runner" }},
		{"negative machine index", 1000, func(c *queue.InitCommand) { c.MachineIndex = intPtr(-1) }},
		{"machine index too high", 1000, func(c *queue.InitCommand) { c.MachineIndex = intPtr(queue.MaxMachines) }},
		{"machines 0", 1000, func(c *queue.InitCommand) { c.Config.Machines = 0 }},
		{"machines over max", 1000, func(c *queue.InitCommand) { c.Config.Machines = queue.MaxMachines + 1 }},
		{"ttl below min", 1000, func(c *queue.InitCommand) { c.Config.LeaseTTLMS = queue.MinLeaseTTLMS - 1 }},
		{"ttl over max", 1000, func(c *queue.InitCommand) { c.Config.LeaseTTLMS = queue.MaxLeaseTTLMS + 1 }},
		{"timeout below ttl", 1000, func(c *queue.InitCommand) { c.Config.BuildTimeoutMS = c.Config.LeaseTTLMS - 1 }},
		{"timeout over max", 1000, func(c *queue.InitCommand) { c.Config.BuildTimeoutMS = queue.MaxBuildTimeoutMS + 1 }},
		{"max attempts 0", 1000, func(c *queue.InitCommand) { c.Config.MaxAttempts = 0 }},
		{"max attempts over limit", 1000, func(c *queue.InitCommand) { c.Config.MaxAttempts = queue.MaxAttemptsLimit + 1 }},
		{"max batch 0", 1000, func(c *queue.InitCommand) { c.Config.MaxBatch = 0 }},
		{"max batch over limit", 1000, func(c *queue.InitCommand) { c.Config.MaxBatch = queue.MaxBatchLimit + 1 }},
		{"empty tests", 1000, func(c *queue.InitCommand) { c.Tests = nil; c.PredictionsMS = nil }},
		{"predictions length mismatch", 1000, func(c *queue.InitCommand) { c.PredictionsMS = []*int64{int64Ptr(1)} }},
		{"unsorted tests", 1000, func(c *queue.InitCommand) { c.Tests = []string{"b.py", "a.py"} }},
		{"duplicate tests", 1000, func(c *queue.InitCommand) { c.Tests = []string{"a.py", "a.py"} }},
		{"non-canonical test", 1000, func(c *queue.InitCommand) { c.Tests = []string{"./a.py", "b.py"} }},
		{"negative prediction", 1000, func(c *queue.InitCommand) { c.PredictionsMS[0] = int64Ptr(-1) }},
		{"prediction over 30d", 1000, func(c *queue.InitCommand) { c.PredictionsMS[0] = int64Ptr(timing.MaxDurationMS + 1) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := queue.NewState(queue.DefaultLimits())
			cmd := base()
			tc.mutate(&cmd)
			events, _, err := queue.DecideInit(s, tc.nowMS, cmd)
			assertQueueErr(t, err, 400, queue.ErrCodeInvalidRequest)
			if len(events) != 0 {
				t.Fatalf("events = %v; want none", events)
			}
		})
	}

	// Over MaxTests limit.
	lim := queue.DefaultLimits()
	lim.MaxTests = 2
	s := queue.NewState(lim)
	cmd := base()
	cmd.Tests = []string{"a.py", "b.py", "c.py"}
	cmd.PredictionsMS = []*int64{nil, nil, nil}
	_, _, err := queue.DecideInit(s, 1000, cmd)
	assertQueueErr(t, err, 400, queue.ErrCodeInvalidRequest)
}

func TestUninitializedReturns404(t *testing.T) {
	s := queue.NewState(queue.DefaultLimits())
	if _, _, err := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"}); err == nil {
		t.Fatal("expected 404")
	} else {
		assertQueueErr(t, err, 404, queue.ErrCodeBuildNotFound)
	}
	if _, _, err := queue.DecideHeartbeat(s, 1000, queue.HeartbeatCommand{RunnerID: "r1", LeaseID: 1}); err == nil {
		t.Fatal("expected 404")
	} else {
		assertQueueErr(t, err, 404, queue.ErrCodeBuildNotFound)
	}
	if _, _, err := queue.DecideReport(s, 1000, queue.ReportCommand{RunnerID: "r1", LeaseID: 1}); err == nil {
		t.Fatal("expected 404")
	} else {
		assertQueueErr(t, err, 404, queue.ErrCodeBuildNotFound)
	}
	if _, _, err := queue.DecideStatus(s, 1000); err == nil {
		t.Fatal("expected 404")
	} else {
		assertQueueErr(t, err, 404, queue.ErrCodeBuildNotFound)
	}
	if _, _, err := queue.DecideListTests(s, 1000, queue.ListTestsCommand{}); err == nil {
		t.Fatal("expected 404")
	} else {
		assertQueueErr(t, err, 404, queue.ErrCodeBuildNotFound)
	}
}

func TestInitConflictsBoundedAndFailedPrecedence(t *testing.T) {
	s := queue.NewState(queue.DefaultLimits())
	cfg := defaultConfig()
	evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID:       "b1",
		RunnerID:      "r0",
		Config:        cfg,
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{int64Ptr(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	// 12 distinct conflicting runners -> only first 8 emit events, then truncated.
	for i := range 12 {
		alt := cfg
		alt.MaxAttempts = 4
		evs, _, err := queue.DecideInit(s, 1000+int64(i), queue.InitCommand{
			BuildID:       "b1",
			RunnerID:      fmt.Sprintf("bad-%d", i),
			Config:        alt,
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		})
		assertQueueErr(t, err, 409, queue.ErrCodeInitConflict)
		if i < queue.MaxInitConflicts {
			if len(evs) != 1 || evs[0].Type != queue.EventConflicted {
				t.Fatalf("conflict %d: events = %v; want [conflicted]", i, evs)
			}
			mustApply(t, s, evs)
		} else if len(evs) != 0 {
			t.Fatalf("conflict %d past cap emitted %d events; want 0", i, len(evs))
		}
	}
	if !s.ConflictsTruncated || len(s.Conflicts) != queue.MaxInitConflicts {
		t.Fatalf("conflicts = %d, truncated = %t; want %d, true", len(s.Conflicts), s.ConflictsTruncated, queue.MaxInitConflicts)
	}

	// When test fails, failed takes precedence over unresolved even with init conflicts.
	evs, resp, err := queue.DecideLease(s, 2000, queue.LeaseCommand{RunnerID: "r0", RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)
	evs, rep, err := queue.DecideReport(s, 2500, queue.ReportCommand{
		RunnerID: "r0",
		LeaseID:  resp.LeaseID,
		Results: []queue.ReportResult{{
			AttemptID:  "0.1",
			TestIndex:  0,
			Generation: 1,
			Outcome:    queue.OutcomeFailed,
			Timed:      false,
			DurationMS: 0,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)
	if rep.Verdict != queue.VerdictFailed || s.Verdict() != queue.VerdictFailed {
		t.Fatalf("verdict = (%s, %s); want failed", rep.Verdict, s.Verdict())
	}
}

func TestHostileStaleAndConflictHistoryBounded(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	s := queue.NewState(lim)
	cfg := defaultConfig()
	cfg.LeaseTTLMS = 1000
	cfg.BuildTimeoutMS = 60_000
	cfg.MaxAttempts = 1
	cfg.MaxBatch = 1

	evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID:       "b1",
		RunnerID:      "r0",
		Config:        cfg,
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{int64Ptr(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	evs, leaseResp, err := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r0", RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	// Accept attempt 0.1 as passed with 50ms.
	evs, _, err = queue.DecideReport(s, 1100, queue.ReportCommand{
		RunnerID: "r0",
		LeaseID:  leaseResp.LeaseID,
		Results:  []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 50}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	// Send 15 distinct conflicting durations -> only first 8 emit reported events.
	for i := range 15 {
		evs, rep, err := queue.DecideReport(s, 1200+int64(i), queue.ReportCommand{
			RunnerID: "r0",
			LeaseID:  leaseResp.LeaseID,
			Results:  []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: int64(100 + i)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Results[0].Disposition != queue.DispositionConflict {
			t.Fatalf("i=%d: disp = %s; want conflict", i, rep.Results[0].Disposition)
		}
		if i < queue.MaxTestHistory {
			if len(evs) != 1 {
				t.Fatalf("i=%d: events = %d; want 1", i, len(evs))
			}
			mustApply(t, s, evs)
		} else if len(evs) != 0 {
			t.Fatalf("i=%d past history cap emitted %d events; want 0", i, len(evs))
		}
	}
	if !s.Tests[0].HistoryTruncated || len(s.Tests[0].History) != queue.MaxTestHistory {
		t.Fatalf("history = %d, truncated = %t; want %d, true", len(s.Tests[0].History), s.Tests[0].HistoryTruncated, queue.MaxTestHistory)
	}
}

func TestTooManyRunnersAndReportValidation(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MaxRunners = 2
	s := queue.NewState(lim)
	cfg := defaultConfig()
	cfg.MaxBatch = 1

	evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID:       "b1",
		RunnerID:      "r0",
		Config:        cfg,
		Tests:         []string{"a.py", "b.py", "c.py"},
		PredictionsMS: []*int64{int64Ptr(300), int64Ptr(200), int64Ptr(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	evs, l1, err := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	evs, _, err = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r2", RequestID: "q2"})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	// 3rd distinct runner exceeds MaxRunners=2.
	_, _, err = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r3", RequestID: "q3"})
	assertQueueErr(t, err, 429, queue.ErrCodeTooManyRunners)

	// Report validation failures.
	badReports := []struct {
		name     string
		cmd      queue.ReportCommand
		wantCode string
	}{
		{"unknown lease", queue.ReportCommand{RunnerID: "r1", LeaseID: 99}, queue.ErrCodeUnknownLease},
		{"runner mismatch", queue.ReportCommand{RunnerID: "r2", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10}}}, queue.ErrCodeRunnerMismatch},
		{"partial report", queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: nil}, queue.ErrCodeInvalidRequest},
		{"wrong attempt_id", queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: "9.9", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10}}}, queue.ErrCodeInvalidRequest},
		{"timed failed", queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomeFailed, Timed: true, DurationMS: 10}}}, queue.ErrCodeInvalidRequest},
		{"untimed nonzero ms", queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: false, DurationMS: 10}}}, queue.ErrCodeInvalidRequest},
		{"invalid outcome", queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: "bogus", Timed: false, DurationMS: 0}}}, queue.ErrCodeInvalidRequest},
		{"wrong test index", queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: "1.1", TestIndex: 1, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10}}}, queue.ErrCodeInvalidRequest},
	}
	for _, tc := range badReports {
		t.Run(tc.name, func(t *testing.T) {
			evs, _, err := queue.DecideReport(s, 1100, tc.cmd)
			assertQueueErr(t, err, 400, tc.wantCode)
			if len(evs) != 0 {
				t.Fatalf("bad report emitted events %v", evs)
			}
		})
	}
}

func TestStatusAndListTestsPaginationAndFilters(t *testing.T) {
	s := queue.NewState(queue.DefaultLimits())
	cfg := defaultConfig()
	cfg.MaxBatch = 1

	evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID:       "b1",
		RunnerID:      "r0",
		Config:        cfg,
		Tests:         []string{"a.py", "b.py", "c.py"},
		PredictionsMS: []*int64{int64Ptr(300), int64Ptr(200), int64Ptr(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	// Lease and pass a.py (250ms), fail b.py, leave c.py pending.
	evs, l1, _ := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", MachineIndex: intPtr(0), RequestID: "q1"})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideReport(s, 1100, queue.ReportCommand{
		RunnerID: "r1",
		LeaseID:  l1.LeaseID,
		Results:  []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 250}},
	})
	mustApply(t, s, evs)

	evs, l2, _ := queue.DecideLease(s, 1200, queue.LeaseCommand{RunnerID: "r1", MachineIndex: intPtr(0), RequestID: "q2"})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideReport(s, 1300, queue.ReportCommand{
		RunnerID: "r1",
		LeaseID:  l2.LeaseID,
		Results:  []queue.ReportResult{{AttemptID: "1.1", TestIndex: 1, Generation: 1, Outcome: queue.OutcomeFailed, Timed: false, DurationMS: 0}},
	})
	mustApply(t, s, evs)

	evs, status, err := queue.DecideStatus(s, 1400)
	if err != nil || len(evs) != 0 {
		t.Fatalf("DecideStatus: evs=%v err=%v", evs, err)
	}
	if status.Verdict != queue.VerdictRunning || len(status.Slowest) != 1 || status.Slowest[0].Test != "a.py" || status.ProblemCount != 1 || status.Problems[0].Test != "b.py" {
		t.Fatalf("unexpected status: %+v", status)
	}

	// Paginate tests with limit=2.
	_, p1, err := queue.DecideListTests(s, 1400, queue.ListTestsCommand{Cursor: 0, Limit: 2})
	if err != nil || len(p1.Tests) != 2 || p1.NextCursor == nil || *p1.NextCursor != 2 {
		t.Fatalf("page 1 = %+v, err=%v", p1, err)
	}
	_, p2, err := queue.DecideListTests(s, 1400, queue.ListTestsCommand{Cursor: *p1.NextCursor, Limit: 2})
	if err != nil || len(p2.Tests) != 1 || p2.NextCursor != nil || p2.Tests[0].Test != "c.py" {
		t.Fatalf("page 2 = %+v, err=%v", p2, err)
	}

	// Filter by state=failed.
	_, pf, err := queue.DecideListTests(s, 1400, queue.ListTestsCommand{States: []queue.TestStatus{queue.StatusFailed}})
	if err != nil || len(pf.Tests) != 1 || pf.Tests[0].Test != "b.py" {
		t.Fatalf("failed filter = %+v, err=%v", pf, err)
	}

	// Bad ListTests inputs.
	if _, _, err := queue.DecideListTests(s, 1400, queue.ListTestsCommand{Cursor: -1}); err == nil {
		t.Fatal("expected 400 on negative cursor")
	} else {
		assertQueueErr(t, err, 400, queue.ErrCodeInvalidRequest)
	}
	if _, _, err := queue.DecideListTests(s, 1400, queue.ListTestsCommand{Limit: queue.MaxTestsPageLimit + 1}); err == nil {
		t.Fatal("expected 400 on oversized limit")
	} else {
		assertQueueErr(t, err, 400, queue.ErrCodeInvalidRequest)
	}
	if _, _, err := queue.DecideListTests(s, 1400, queue.ListTestsCommand{States: []queue.TestStatus{"bogus"}}); err == nil {
		t.Fatal("expected 400 on invalid state filter")
	} else {
		assertQueueErr(t, err, 400, queue.ErrCodeInvalidRequest)
	}

	// Heartbeat errors: unknown lease and runner mismatch.
	if _, _, err := queue.DecideHeartbeat(s, 1400, queue.HeartbeatCommand{RunnerID: "r1", LeaseID: 99}); err == nil {
		t.Fatal("expected 400 on unknown lease")
	} else {
		assertQueueErr(t, err, 400, queue.ErrCodeUnknownLease)
	}
	if _, _, err := queue.DecideHeartbeat(s, 1400, queue.HeartbeatCommand{RunnerID: "r2", LeaseID: l1.LeaseID}); err == nil {
		t.Fatal("expected 400 on runner mismatch")
	} else {
		assertQueueErr(t, err, 400, queue.ErrCodeRunnerMismatch)
	}

	// Init conflict naming differing path at same count.
	evs, _, err = queue.DecideInit(s, 1450, queue.InitCommand{
		BuildID:       "b1",
		RunnerID:      "r9",
		Config:        cfg,
		Tests:         []string{"a.py", "b.py", "z.py"},
		PredictionsMS: []*int64{nil, nil, nil},
	})
	assertQueueErr(t, err, 409, queue.ErrCodeInitConflict)
	if !strings.Contains(err.Error(), "z.py") || len(evs) != 1 {
		t.Fatalf("conflict err=%v evs=%v; want path diff mentioning z.py", err, evs)
	}

	// Finalize on ListTests read when build deadline passes.
	evs, finPage, err := queue.DecideListTests(s, 1000+cfg.BuildTimeoutMS, queue.ListTestsCommand{States: []queue.TestStatus{queue.StatusUnresolved}})
	if err != nil || len(evs) != 1 || evs[0].Type != queue.EventFinalized || finPage.Verdict != queue.VerdictFailed || len(finPage.Tests) != 1 || finPage.Tests[0].Test != "c.py" {
		t.Fatalf("finalize on ListTests: evs=%v page=%+v err=%v", evs, finPage, err)
	}
}

func TestApplyRejectsCorruptEventsWithoutPartialMutation(t *testing.T) {
	s := queue.NewState(queue.DefaultLimits())
	cfg := defaultConfig()
	evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID:       "b1",
		RunnerID:      "r0",
		Config:        cfg,
		Tests:         []string{"a.py", "b.py"},
		PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	evs, _, err = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)

	badEvents := []queue.Event{
		{Type: queue.EventInitialized, AtMS: 1000, Initialized: &queue.InitializedData{BuildID: "b1"}},
		{Type: queue.EventExpired, AtMS: 500, Expired: &queue.ExpiredData{LeaseID: 1}}, // clock backwards
		{Type: queue.EventExpired, AtMS: 1100, Expired: &queue.ExpiredData{LeaseID: 99}},
		{Type: queue.EventLeased, AtMS: 1100, Leased: &queue.LeasedData{LeaseID: 2, RunnerID: "r1", RequestID: "q2", DeadlineMS: 61100, Attempts: []queue.LeaseAttempt{{TestIndex: 1, Generation: 1}}}}, // runner already holds active lease
		{Type: queue.EventReported, AtMS: 1100, Reported: &queue.ReportedData{LeaseID: 1, RunnerID: "r1", CloseLease: true, Results: []queue.ReportedAttempt{{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionDuplicate}}}}, // duplicate not allowed in event
		{Type: queue.EventReported, AtMS: 1100, Reported: &queue.ReportedData{LeaseID: 1, RunnerID: "r1", CloseLease: false, Results: []queue.ReportedAttempt{{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted}}}},
		{Type: queue.EventFinalized, AtMS: 1100, Finalized: &queue.FinalizedData{Reason: "bogus"}},
		{Type: "unknown", AtMS: 1100},
	}

	for i, bad := range badEvents {
		before := s.Clone()
		if err := s.Apply(bad); err == nil {
			t.Fatalf("bad event %d (%s): Apply succeeded; want error", i, bad.Type)
		}
		if !s.EqualExact(before) {
			t.Fatalf("bad event %d (%s): Apply partially mutated state on error", i, bad.Type)
		}
	}
}
