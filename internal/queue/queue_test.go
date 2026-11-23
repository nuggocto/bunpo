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
		Machines: 2, LeaseTTLMS: queue.DefaultLeaseTTLMS, BuildTimeoutMS: queue.DefaultBuildTimeoutMS,
		MaxAttempts: queue.DefaultMaxAttempts, MaxBatch: queue.DefaultMaxBatch,
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
	for _, id := range []string{"a", "run-123_4.5", strings.Repeat("a", 128)} {
		if !queue.ValidBuildID(id) {
			t.Errorf("ValidBuildID(%q) = false", id)
		}
	}
	for _, id := range []string{"", "-bad", ".bad", "a..b", "a/b", "a b", strings.Repeat("a", 129)} {
		if queue.ValidBuildID(id) {
			t.Errorf("ValidBuildID(%q) = true", id)
		}
	}
	exactID := strings.Repeat("a", queue.MaxTestIDBytes)
	if !queue.ValidRunnerID(strings.Repeat("r", 64)) || queue.ValidRunnerID("") || queue.ValidRunnerID(strings.Repeat("r", 65)) || queue.ValidRunnerID("bad/id") ||
		!queue.ValidRequestID(strings.Repeat("q", 128)) || queue.ValidRequestID("") || queue.ValidRequestID(strings.Repeat("q", 129)) ||
		!queue.ValidTestID(exactID) || queue.ValidTestID(exactID+"x") {
		t.Fatal("identifier bounds failed")
	}
}

func TestInitValidationAndExactLimits(t *testing.T) {
	lim := queue.Limits{MinLeaseTTLMS: queue.MinLeaseTTLMS, MaxTests: 3, MaxRunners: 2}
	maxCfg := queue.Config{
		Machines: queue.MaxMachines, LeaseTTLMS: queue.MaxLeaseTTLMS, BuildTimeoutMS: queue.MaxBuildTimeoutMS,
		MaxAttempts: queue.MaxAttemptsLimit, MaxBatch: queue.MaxBatchLimit,
	}
	// Exact-at-limit values must be accepted (AtMS == 0, MaxDurationMS, BuildTimeoutMS == LeaseTTLMS).
	s0 := queue.NewState(lim)
	evs, _, err := queue.DecideInit(s0, 0, queue.InitCommand{
		BuildID: "exact-max", RunnerID: "r1", Config: maxCfg,
		Tests: []string{"a.py", "b.py", "c.py"}, PredictionsMS: []*int64{int64Ptr(0), int64Ptr(timing.MaxDurationMS), nil},
	})
	if err != nil {
		t.Fatalf("exact-limit init failed: %v", err)
	}
	mustApply(t, s0, evs)

	eqCfg := defaultConfig()
	eqCfg.BuildTimeoutMS = eqCfg.LeaseTTLMS
	if _, _, err := queue.DecideInit(queue.NewState(queue.DefaultLimits()), 1000, queue.InitCommand{
		BuildID: "eq-timeout", RunnerID: "r1", Config: eqCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil},
	}); err != nil {
		t.Fatalf("BuildTimeoutMS == LeaseTTLMS rejected: %v", err)
	}

	ok := func(mod func(*queue.InitCommand)) queue.InitCommand {
		c := queue.InitCommand{BuildID: "b1", RunnerID: "r1", Config: defaultConfig(), Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}
		mod(&c)
		return c
	}
	bad := []queue.InitCommand{
		ok(func(c *queue.InitCommand) { c.BuildID = "../bad" }),
		ok(func(c *queue.InitCommand) { c.MachineIndex = intPtr(256) }),
		ok(func(c *queue.InitCommand) { c.Tests, c.PredictionsMS = nil, nil }),
		ok(func(c *queue.InitCommand) {
			c.Tests = []string{"a.py", "b.py", "c.py", "d.py"}
			c.PredictionsMS = make([]*int64, 4)
		}),
		ok(func(c *queue.InitCommand) { c.Tests = []string{"b.py", "a.py"}; c.PredictionsMS = make([]*int64, 2) }),
		ok(func(c *queue.InitCommand) { c.Tests = []string{"a.py", "a.py"}; c.PredictionsMS = make([]*int64, 2) }),
		ok(func(c *queue.InitCommand) { c.Tests = []string{"./a.py"} }),
		ok(func(c *queue.InitCommand) { c.PredictionsMS = []*int64{} }),
		ok(func(c *queue.InitCommand) { c.PredictionsMS = []*int64{int64Ptr(-1)} }),
		ok(func(c *queue.InitCommand) { c.PredictionsMS = []*int64{int64Ptr(timing.MaxDurationMS + 1)} }),
		ok(func(c *queue.InitCommand) { c.Config.Machines = 0 }),
		ok(func(c *queue.InitCommand) { c.Config.LeaseTTLMS = 9_999 }),
		ok(func(c *queue.InitCommand) { c.Config.BuildTimeoutMS = c.Config.LeaseTTLMS - 1 }),
		ok(func(c *queue.InitCommand) { c.Config.MaxAttempts = 11 }),
		ok(func(c *queue.InitCommand) { c.Config.MaxBatch = 201 }),
	}
	for i, cmd := range bad {
		_, _, err := queue.DecideInit(queue.NewState(lim), 1000, cmd)
		assertQueueErr(t, err, 400, queue.ErrCodeInvalidRequest)
		_ = i
	}
}

func TestInitConflictsBoundedAndFailedPrecedence(t *testing.T) {
	s := queue.NewState(queue.DefaultLimits())
	evs, _, err := queue.DecideInit(s, 1000, queue.InitCommand{
		BuildID: "b-conf", RunnerID: "r0", Config: defaultConfig(),
		Tests: []string{"a.py", "b.py"}, PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100)},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, evs)
	for i := range 12 {
		cEvs, _, err := queue.DecideInit(s, int64(1100+i), queue.InitCommand{
			BuildID: "b-conf", RunnerID: fmt.Sprintf("bad-%d", i), Config: defaultConfig(),
			Tests: []string{"a.py"}, PredictionsMS: []*int64{int64Ptr(100)},
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
			results[i] = queue.ReportResult{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: a.Generation, Outcome: out, Timed: timed, DurationMS: ms}
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
	cfg.LeaseTTLMS, cfg.MaxAttempts, cfg.MaxBatch = 2000, 3, 1
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{BuildID: "b-hist", RunnerID: "r0", Config: cfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{int64Ptr(100)}})
	mustApply(t, s, evs)
	evs, l1, _ := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r0", RequestID: "q1"})
	mustApply(t, s, evs)
	evs, l2, _ := queue.DecideLease(s, 4000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q2"})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideReport(s, 4100, queue.ReportCommand{RunnerID: "r1", LeaseID: l2.LeaseID, Results: []queue.ReportResult{{AttemptID: "0.2", TestIndex: 0, Generation: 2, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 50}}})
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

func TestTooManyRunnersAndReportValidation(t *testing.T) {
	lim := queue.Limits{MinLeaseTTLMS: 1000, MaxTests: 10, MaxRunners: 2}
	s := queue.NewState(lim)
	cfg := defaultConfig()
	cfg.LeaseTTLMS, cfg.MaxBatch = 2000, 1
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{BuildID: "b-run", RunnerID: "r0", Config: cfg, Tests: []string{"a.py", "b.py", "c.py"}, PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100), int64Ptr(100)}})
	mustApply(t, s, evs)
	evs, l1, _ := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r2", RequestID: "q2"})
	mustApply(t, s, evs)
	_, _, err := queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r3", RequestID: "q3"})
	assertQueueErr(t, err, 429, queue.ErrCodeTooManyRunners)

	a := l1.Attempts[0]
	for _, tc := range []struct {
		cmd  queue.ReportCommand
		code string
	}{
		{queue.ReportCommand{RunnerID: "r1", LeaseID: 999}, queue.ErrCodeUnknownLease},
		{queue.ReportCommand{RunnerID: "r2", LeaseID: l1.LeaseID}, queue.ErrCodeRunnerMismatch},
		{queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID}, queue.ErrCodeInvalidRequest},
		{queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: 1, Outcome: queue.OutcomeFailed, Timed: true, DurationMS: 10}}}, queue.ErrCodeInvalidRequest},
		{queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: []queue.ReportResult{{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: 1, Outcome: queue.OutcomePassed, Timed: false, DurationMS: 10}}}, queue.ErrCodeInvalidRequest},
	} {
		_, _, err := queue.DecideReport(s, 1200, tc.cmd)
		assertQueueErr(t, err, 400, tc.code)
	}
}

func TestStatusListTestsAndFinalizeOverlay(t *testing.T) {
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	s := queue.NewState(lim)
	cfg := queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: 10_000, MaxAttempts: 2, MaxBatch: 1}
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{BuildID: "b-st", RunnerID: "r1", Config: cfg, Tests: []string{"a.py", "b.py", "c.py"}, PredictionsMS: []*int64{int64Ptr(300), int64Ptr(200), int64Ptr(100)}})
	mustApply(t, s, evs)
	leaseAndRep := func(tLease, tRep int64, q, att string, idx int, out queue.Outcome, timed bool, ms int64) {
		evs, l, _ := queue.DecideLease(s, tLease, queue.LeaseCommand{RunnerID: "r1", RequestID: q})
		mustApply(t, s, evs)
		evs, _, _ = queue.DecideReport(s, tRep, queue.ReportCommand{RunnerID: "r1", LeaseID: l.LeaseID, Results: []queue.ReportResult{{AttemptID: att, TestIndex: idx, Generation: 1, Outcome: out, Timed: timed, DurationMS: ms}}})
		mustApply(t, s, evs)
	}
	leaseAndRep(1000, 1200, "q1", "0.1", 0, queue.OutcomePassed, true, 350)
	leaseAndRep(1300, 1400, "q2", "1.1", 1, queue.OutcomeFailed, false, 0)
	evs, l3, _ := queue.DecideLease(s, 1500, queue.LeaseCommand{RunnerID: "r1", RequestID: "q3"})
	mustApply(t, s, evs)

	if hbEvs, hbResp, err := queue.DecideHeartbeat(s, s.DeadlineMS, queue.HeartbeatCommand{RunnerID: "r1", LeaseID: l3.LeaseID}); err != nil || hbResp.Held || len(hbEvs) == 0 {
		t.Fatalf("heartbeat at exact deadline: evs=%v resp=%+v err=%v", hbEvs, hbResp, err)
	}
	if stEvs, stResp, err := queue.DecideStatus(s, s.DeadlineMS); err != nil || len(stEvs) != 1 || stResp.Verdict != queue.VerdictFailed || len(stResp.Slowest) != 1 || stResp.Slowest[0].Test != "a.py" || stResp.Runners[0].ActiveLeaseID != 0 {
		t.Fatalf("finalizing status: evs=%v resp=%+v err=%v", stEvs, stResp, err)
	}
	if ltEvs, p1, err := queue.DecideListTests(s, s.DeadlineMS, queue.ListTestsCommand{Cursor: 0, Limit: 1}); err != nil || len(ltEvs) != 1 || len(p1.Tests) != 1 || p1.NextCursor == nil || *p1.NextCursor != 1 {
		t.Fatalf("list limit=1: evs=%v p1=%+v err=%v", ltEvs, p1, err)
	}
	if _, all, err := queue.DecideListTests(s, s.DeadlineMS, queue.ListTestsCommand{Limit: queue.MaxTestsPageLimit}); err != nil || len(all.Tests) != 3 || all.Tests[2].Attempts[0].Status != queue.AttemptExpired || all.Tests[2].Attempts[0].FinishedAtMS != s.DeadlineMS {
		t.Fatalf("finalizing list overlay: %+v err=%v", all, err)
	}
	if _, retried, err := queue.DecideListTests(s, 1600, queue.ListTestsCommand{Retried: true}); err != nil || len(retried.Tests) != 0 {
		t.Fatalf("retried filter: %+v err=%v", retried, err)
	}
	for _, cmd := range []queue.ListTestsCommand{{Cursor: -1}, {Limit: queue.MaxTestsPageLimit + 1}, {States: []queue.TestStatus{"bogus"}}} {
		if _, _, err := queue.DecideListTests(s, 1600, cmd); err == nil {
			t.Fatalf("DecideListTests(%+v) succeeded; want error", cmd)
		}
	}
}

func TestApplyRejectsDamagedEventsAtomically(t *testing.T) {
	lim := queue.Limits{MinLeaseTTLMS: 1000, MaxTests: 10, MaxRunners: 1}
	s := queue.NewState(lim)
	cfg := defaultConfig()
	cfg.LeaseTTLMS, cfg.MaxBatch = 2000, 1
	evs, _, _ := queue.DecideInit(s, 1000, queue.InitCommand{BuildID: "b-dmg", RunnerID: "r1", Config: cfg, Tests: []string{"a.py", "b.py"}, PredictionsMS: []*int64{int64Ptr(100), int64Ptr(100)}})
	mustApply(t, s, evs)
	evs, _, _ = queue.DecideLease(s, 1000, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	mustApply(t, s, evs)

	if err := queue.NewState(lim).Apply(queue.Event{Type: queue.EventInitialized, AtMS: 1000, Initialized: &queue.InitializedData{BuildID: "b", Digest: "d", Config: cfg, DeadlineMS: 500, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}}); err == nil {
		t.Fatal("expected error for deadline < at_ms")
	}
	bad := []queue.Event{
		{Type: queue.EventExpired, AtMS: -1, Expired: &queue.ExpiredData{LeaseID: 1}},
		{Type: queue.EventLeased, AtMS: 1100, Leased: &queue.LeasedData{LeaseID: 2, RunnerID: "r2", RequestID: "q2", DeadlineMS: 3100}},
		{Type: queue.EventLeased, AtMS: 1100, Leased: &queue.LeasedData{LeaseID: 2, RunnerID: "r2", RequestID: "q2", DeadlineMS: 3100, Attempts: []queue.LeaseAttempt{{TestIndex: 1, Generation: 1}}}},
		{Type: queue.EventReported, AtMS: 1100, Reported: &queue.ReportedData{LeaseID: 1, RunnerID: "r1", CloseLease: true}},
		{Type: queue.EventReported, AtMS: 1100, Reported: &queue.ReportedData{LeaseID: 1, RunnerID: "r1", CloseLease: true, Results: []queue.ReportedAttempt{{TestIndex: 1, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted}}}},
		{Type: queue.EventReported, AtMS: 1100, Reported: &queue.ReportedData{LeaseID: 1, RunnerID: "r1", CloseLease: false, Results: []queue.ReportedAttempt{{TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted}}}},
		{Type: queue.EventReported, AtMS: 1100, Reported: &queue.ReportedData{LeaseID: 1, RunnerID: "r1", CloseLease: true, Results: []queue.ReportedAttempt{{TestIndex: 0, Generation: 9, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10, Disposition: queue.DispositionAccepted}}}},
	}
	for i, ev := range bad {
		before := cloneState(t, s)
		if err := s.Apply(ev); err == nil || !equalExact(t, s, before) {
			t.Fatalf("bad[%d] (%s): err=%v or state mutated", i, ev.Type, err)
		}
	}
}
