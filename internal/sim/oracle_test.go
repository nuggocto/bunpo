package sim_test

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/queuerunner"
)

type oracleExec struct {
	leaseID        int64
	runnerID       int
	format         string
	attempts       []queue.LeasedAttemptView
	plannedReadErr error
	hadStaleOnDisk bool
	completed      bool
	stopped        bool
	exitCode       int
	signaled       bool
	filesWritten   map[string]junit.File
	readErr        error
}

type acceptedTruth struct {
	leaseID    int64
	attemptID  string
	outcome    queue.Outcome
	timed      bool
	durationMS int64
}

// simOracle tracks ground-truth command execution independently of queue logic
// and verifies safety, liveness, replay, status/slowest/runner counts, and bounds.
type simOracle struct {
	buildID             string
	tests               []string
	cfg                 queue.Config
	expectCleanPass     bool
	initialized         bool
	hadInitConflict     bool
	finishedVerdict     queue.Verdict
	execs               map[int64]*oracleExec
	accepted            map[string]acceptedTruth
	acceptedAttempt     map[string]acceptedTruth
	lastLeaseReq        map[string]struct{ reqID string; leaseID int64 }
	reportedLeases      map[int64]int
	runnerLeases        map[string]int
}

func newSimOracle(buildID string, tests []string, cfg queue.Config, expectCleanPass bool) *simOracle {
	return &simOracle{
		buildID: buildID, tests: tests, cfg: cfg, expectCleanPass: expectCleanPass,
		execs: make(map[int64]*oracleExec), accepted: make(map[string]acceptedTruth),
		acceptedAttempt: make(map[string]acceptedTruth),
		lastLeaseReq:    make(map[string]struct{ reqID string; leaseID int64 }),
		reportedLeases:  make(map[int64]int), runnerLeases: make(map[string]int),
	}
}

func (o *simOracle) recordCommandStart(leaseID int64, runnerID int, format string, attempts []queue.LeasedAttemptView, plannedReadErr error, hadStaleOnDisk bool) error {
	if prev, dup := o.execs[leaseID]; dup {
		return fmt.Errorf("oracle: runner %d started lease %d twice (prev runner %d) — stale lease response applied", runnerID, leaseID, prev.runnerID)
	}
	o.execs[leaseID] = &oracleExec{
		leaseID: leaseID, runnerID: runnerID, format: format,
		attempts: attempts, plannedReadErr: plannedReadErr, hadStaleOnDisk: hadStaleOnDisk,
	}
	return nil
}

func (o *simOracle) plannedReadErr(leaseID int64) error {
	if ex, ok := o.execs[leaseID]; ok {
		return ex.plannedReadErr
	}
	return nil
}

func (o *simOracle) markCommandCompleted(leaseID int64, exitCode int, signaled bool, files map[string]junit.File, readErr error) {
	if ex, ok := o.execs[leaseID]; ok {
		ex.completed, ex.exitCode, ex.signaled = true, exitCode, signaled
		ex.filesWritten, ex.readErr = cloneFiles(files), readErr
	}
}

func (o *simOracle) markCommandStopped(leaseID int64) {
	if ex, ok := o.execs[leaseID]; ok {
		ex.stopped = true
	}
}

func (o *simOracle) reportRetryAfterRestart(leaseID int64) bool {
	epoch, ok := o.reportedLeases[leaseID]
	return ok && epoch == 0
}

func (o *simOracle) onServerHandled(w *simWorld, req queuerunner.Request, status int, body []byte, newEvents []queue.Event) error {
	switch req.Kind {
	case queuerunner.ReqInit:
		if status == http.StatusOK {
			o.initialized = true
		} else if status == http.StatusConflict {
			o.hadInitConflict = true
			if o.finishedVerdict == queue.VerdictPassed {
				w.stats[stConflictAfterPassed]++
				o.finishedVerdict = queue.VerdictUnresolved
			}
		}

	case queuerunner.ReqLease:
		if status != http.StatusOK {
			return nil
		}
		resp, err := queueapi.ParseLeaseResponse(status, body)
		if err != nil {
			return fmt.Errorf("oracle: invalid 200 lease response: %w", err)
		}
		if resp.Status == queue.LeaseGranted {
			prev, seen := o.lastLeaseReq[req.Lease.RunnerID]
			if seen && prev.reqID == req.Lease.RequestID {
				if resp.LeaseID != prev.leaseID {
					return fmt.Errorf("oracle: repeated request_id %q got new lease %d (prev %d)", req.Lease.RequestID, resp.LeaseID, prev.leaseID)
				}
				for _, ev := range newEvents {
					if ev.Type == queue.EventLeased {
						return fmt.Errorf("oracle: repeated active request_id %q emitted leased event", req.Lease.RequestID)
					}
				}
			} else {
				o.lastLeaseReq[req.Lease.RunnerID] = struct {
					reqID   string
					leaseID int64
				}{req.Lease.RequestID, resp.LeaseID}
				o.runnerLeases[req.Lease.RunnerID]++
			}
		} else if resp.Status == queue.LeaseDone {
			return o.observeVerdict(resp.Verdict)
		}

	case queuerunner.ReqReport:
		if status != http.StatusOK {
			return nil
		}
		resp, err := queueapi.ParseReportResponse(status, body)
		if err != nil {
			return fmt.Errorf("oracle: invalid 200 report response: %w", err)
		}
		cmd := req.Report
		if _, seen := o.reportedLeases[cmd.LeaseID]; !seen {
			o.reportedLeases[cmd.LeaseID] = w.srvEpoch
		}
		if err := o.verifyReport(cmd, resp, newEvents); err != nil {
			return err
		}
		return o.observeVerdict(resp.Verdict)
	}
	return nil
}

func (o *simOracle) verifyReport(cmd *queue.ReportCommand, resp queueapi.ReportResponse, newEvents []queue.Event) error {
	resByID := make(map[string]queue.ReportResult, len(cmd.Results))
	for _, r := range cmd.Results {
		resByID[r.AttemptID] = r
	}
	ex := o.execs[cmd.LeaseID]
	allDup := true
	for _, d := range resp.Results {
		sub, ok := resByID[d.AttemptID]
		if !ok {
			return fmt.Errorf("oracle: response named unknown attempt %s", d.AttemptID)
		}
		testPath := o.tests[sub.TestIndex]
		if prev, wasAcc := o.acceptedAttempt[d.AttemptID]; wasAcc && prev.outcome == sub.Outcome && prev.timed == sub.Timed && prev.durationMS == sub.DurationMS {
			if d.Disposition != queue.DispositionDuplicate {
				return fmt.Errorf("oracle: identical resend of accepted attempt %s got %q; want duplicate", d.AttemptID, d.Disposition)
			}
		}
		if d.Disposition != queue.DispositionDuplicate {
			allDup = false
		}
		if d.Disposition == queue.DispositionAccepted {
			if sub.Outcome == queue.OutcomePassed || sub.Outcome == queue.OutcomeFailed || sub.Outcome == queue.OutcomeSkipped {
				if prev, dup := o.accepted[testPath]; dup {
					return fmt.Errorf("oracle: test %s accepted twice (prev=%+v, new=%s)", testPath, prev, d.AttemptID)
				}
				if err := verifyOutcomeTruth(testPath, sub, ex); err != nil {
					return err
				}
				truth := acceptedTruth{leaseID: cmd.LeaseID, attemptID: d.AttemptID, outcome: sub.Outcome, timed: sub.Timed, durationMS: sub.DurationMS}
				o.accepted[testPath] = truth
				o.acceptedAttempt[d.AttemptID] = truth
			} else {
				o.acceptedAttempt[d.AttemptID] = acceptedTruth{leaseID: cmd.LeaseID, attemptID: d.AttemptID, outcome: queue.OutcomeError}
			}
		}
	}
	if allDup && len(resp.Results) > 0 {
		for _, ev := range newEvents {
			if ev.Type == queue.EventReported {
				return fmt.Errorf("oracle: all-duplicate report for lease %d emitted reported event", cmd.LeaseID)
			}
		}
	}
	return nil
}

func verifyOutcomeTruth(testPath string, sub queue.ReportResult, ex *oracleExec) error {
	if ex == nil || !ex.completed || ex.stopped || ex.readErr != nil || ex.filesWritten == nil {
		return fmt.Errorf("oracle: accepted %s for %s without clean completed report", sub.Outcome, testPath)
	}
	assigned := make(map[string]bool, len(ex.attempts))
	for _, a := range ex.attempts {
		assigned[a.Test] = true
	}
	for f := range ex.filesWritten {
		if !assigned[f] {
			return fmt.Errorf("oracle: accepted %s for %s despite extra unassigned file %s", sub.Outcome, testPath, f)
		}
	}
	f, present := ex.filesWritten[testPath]
	if !present {
		return fmt.Errorf("oracle: accepted %s for %s absent from report", sub.Outcome, testPath)
	}
	switch sub.Outcome {
	case queue.OutcomeFailed:
		if !f.Failed || sub.Timed || sub.DurationMS != 0 {
			return fmt.Errorf("oracle: invalid failed for %s", testPath)
		}
	case queue.OutcomeSkipped:
		if ex.signaled || ex.exitCode != 0 || f.Failed || f.Cases == 0 || f.Skipped != f.Cases || sub.Timed || sub.DurationMS != 0 {
			return fmt.Errorf("oracle: accepted skipped for %s on exit=%d sig=%t cases=%d skipped=%d failed=%t",
				testPath, ex.exitCode, ex.signaled, f.Cases, f.Skipped, f.Failed)
		}
	case queue.OutcomePassed:
		if ex.signaled || f.Failed || f.Cases == 0 || f.Skipped == f.Cases {
			return fmt.Errorf("oracle: invalid passed for %s", testPath)
		}
		if ex.exitCode == 0 {
			if !sub.Timed || sub.DurationMS != f.MS() {
				return fmt.Errorf("oracle: passed %s on exit 0 got timed=%t ms=%d; want true/%d", testPath, sub.Timed, sub.DurationMS, f.MS())
			}
		} else {
			if ex.format != "junit" || f.Skipped != 0 || sub.Timed || sub.DurationMS != 0 {
				return fmt.Errorf("oracle: non-zero passed %s invalid", testPath)
			}
			siblingFailed := false
			for _, a := range ex.attempts {
				if sf, ok := ex.filesWritten[a.Test]; ok && sf.Failed {
					siblingFailed = true
					break
				}
			}
			if !siblingFailed {
				return fmt.Errorf("oracle: non-zero passed %s without failed sibling", testPath)
			}
		}
	}
	return nil
}

func (o *simOracle) observeVerdict(v queue.Verdict) error {
	if v == queue.VerdictRunning || v == "" {
		return nil
	}
	if o.finishedVerdict == "" {
		o.finishedVerdict = v
	} else if v != o.finishedVerdict {
		if !(o.finishedVerdict == queue.VerdictPassed && v == queue.VerdictUnresolved && o.hadInitConflict) {
			return fmt.Errorf("oracle: illegal verdict transition %s -> %s (hadInitConflict=%t)", o.finishedVerdict, v, o.hadInitConflict)
		}
		o.finishedVerdict = v
	}
	if v == queue.VerdictPassed {
		if o.hadInitConflict {
			return fmt.Errorf("oracle: verdict passed despite init conflict")
		}
		for _, p := range o.tests {
			if acc, ok := o.accepted[p]; !ok || (acc.outcome != queue.OutcomePassed && acc.outcome != queue.OutcomeSkipped) {
				return fmt.Errorf("oracle: verdict passed but %s has accepted=%+v (ok=%t)", p, acc, ok)
			}
		}
	}
	return nil
}

func (o *simOracle) onRunnerExit(r *simRunner) error {
	if r.exitErr == nil && r.verdict == queue.VerdictPassed {
		if err := o.observeVerdict(queue.VerdictPassed); err != nil {
			return err
		}
	}
	if r.exitErr != nil {
		var apiErr *queueapi.APIError
		if errors.As(r.exitErr, &apiErr) {
			if apiErr.Code == queue.ErrCodeStaleRequest {
				return fmt.Errorf("oracle: runner %d exited on 409 stale_request instead of looping with a fresh request_id", r.id)
			}
			if apiErr.Code == queue.ErrCodeLeaseHeld {
				return fmt.Errorf("oracle: runner %d exited on 409 lease_held (active lease abandoned after late/stale response)", r.id)
			}
		}
	}
	return nil
}

func (o *simOracle) onStatusDone(w *simWorld, a queuerunner.Action) error {
	if a.Err != nil {
		if w.disk.broken[w.buildID] {
			return nil
		}
		var apiErr *queueapi.APIError
		if errors.As(a.Err, &apiErr) && apiErr.HTTPStatus == http.StatusNotFound && !o.initialized {
			return nil
		}
		return fmt.Errorf("oracle: status failed unexpectedly: %v", a.Err)
	}
	if a.Status != nil {
		if err := o.observeVerdict(a.Status.Verdict); err != nil {
			return err
		}
	}
	if a.HasReport && a.Report != nil {
		st := a.Status
		if st.Verdict == queue.VerdictRunning || st.Counts.Unresolved > 0 || len(st.Conflicts) > 0 || len(a.Report.Tests) != len(o.tests) {
			return fmt.Errorf("oracle: invalid RunReport emission (verdict=%s tests=%d/%d)", st.Verdict, len(a.Report.Tests), len(o.tests))
		}
		for _, p := range o.tests {
			obs, ok := a.Report.Tests[p]
			acc, accOK := o.accepted[p]
			if !ok || !accOK {
				return fmt.Errorf("oracle: RunReport missing/unaccepted %s", p)
			}
			wantTimed := acc.outcome == queue.OutcomePassed && acc.timed
			wantMS := int64(0)
			if wantTimed {
				wantMS = acc.durationMS
			}
			if obs.Timed != wantTimed || obs.MS != wantMS {
				return fmt.Errorf("oracle: RunReport[%s]=%+v want timed=%t ms=%d", p, obs, wantTimed, wantMS)
			}
		}
	}
	return nil
}

func (o *simOracle) checkStep(w *simWorld) error {
	if !o.initialized || w.disk.broken[w.buildID] {
		return nil
	}
	st, nEvents, err := w.disk.replayBuild(w.buildID, w.lim)
	if err != nil {
		return fmt.Errorf("oracle: disk replay failed: %w", err)
	}
	if bound := 10 + len(o.tests)*(3*o.cfg.MaxAttempts+8); nEvents > bound {
		return fmt.Errorf("oracle: persisted events %d > bound %d", nEvents, bound)
	}
	if len(st.Conflicts) > queue.MaxInitConflicts {
		return fmt.Errorf("oracle: conflicts %d > %d", len(st.Conflicts), queue.MaxInitConflicts)
	}
	for _, t := range st.Tests {
		if len(t.History) > queue.MaxTestHistory {
			return fmt.Errorf("oracle: test %s history %d > %d", t.Path, len(t.History), queue.MaxTestHistory)
		}
	}
	seenR, seenT := make(map[string]int64), make(map[int]int64)
	for _, lid := range st.ActiveLeases {
		l := st.Leases[lid-1]
		if prev, dup := seenR[l.RunnerID]; dup {
			return fmt.Errorf("oracle: runner %s holds active leases %d and %d", l.RunnerID, prev, lid)
		}
		seenR[l.RunnerID] = lid
		for _, a := range l.Attempts {
			if prev, dup := seenT[a.TestIndex]; dup {
				return fmt.Errorf("oracle: test %d in active leases %d and %d", a.TestIndex, prev, lid)
			}
			seenT[a.TestIndex] = lid
		}
	}
	return nil
}

func (o *simOracle) checkFinal(w *simWorld) error {
	if w.disk.broken[w.buildID] {
		return nil
	}
	if !o.initialized {
		return fmt.Errorf("oracle: build never initialized")
	}
	stResp, httpStatus, err := w.queryServerStatus()
	if err != nil || httpStatus != http.StatusOK || stResp.Verdict == queue.VerdictRunning {
		return fmt.Errorf("oracle: final status invalid (http=%d verdict=%s counts=%+v err=%v)", httpStatus, stResp.Verdict, stResp.Counts, err)
	}
	if err := o.observeVerdict(stResp.Verdict); err != nil {
		return err
	}
	switch stResp.Verdict {
	case queue.VerdictPassed:
		w.stats[stPassed]++
	case queue.VerdictFailed:
		w.stats[stFailed]++
	case queue.VerdictUnresolved:
		w.stats[stUnresolved]++
	}
	if o.expectCleanPass && stResp.Verdict != queue.VerdictPassed {
		return fmt.Errorf("oracle: clean report-outage build finished %s; want passed (heartbeats must keep lease alive during Report retry)", stResp.Verdict)
	}
	for _, r := range w.runners {
		if !r.killed && !r.exited {
			return fmt.Errorf("oracle: liveness failure: runner %d never exited", r.id)
		}
	}

	// Verify runner lease counts, slowest list (only timed passed, descending), and timestamps.
	if stResp.CreatedAtMS <= 0 || stResp.FinishedAtMS == nil || *stResp.FinishedAtMS < stResp.CreatedAtMS {
		return fmt.Errorf("oracle: invalid build timestamps created=%d finished=%v", stResp.CreatedAtMS, stResp.FinishedAtMS)
	}
	for _, rs := range stResp.Runners {
		if rs.Leases != o.runnerLeases[rs.RunnerID] {
			return fmt.Errorf("oracle: runner %s leases=%d; oracle counted %d", rs.RunnerID, rs.Leases, o.runnerLeases[rs.RunnerID])
		}
	}
	for i, sl := range stResp.Slowest {
		acc, ok := o.accepted[sl.Test]
		if !ok || acc.outcome != queue.OutcomePassed || !acc.timed || acc.durationMS != sl.DurationMS {
			return fmt.Errorf("oracle: slowest[%d]=%+v does not match timed passed truth %+v", i, sl, acc)
		}
		if i > 0 && stResp.Slowest[i-1].DurationMS < sl.DurationMS {
			return fmt.Errorf("oracle: slowest list not sorted descending at %d", i)
		}
	}

	replayed, _, err := w.disk.replayBuild(w.buildID, w.lim)
	if err != nil {
		return fmt.Errorf("oracle: final disk replay failed: %w", err)
	}
	if replayed.Verdict() != stResp.Verdict || replayed.Counts != stResp.Counts ||
		len(replayed.Conflicts) != len(stResp.Conflicts) || replayed.ConflictsTruncated != stResp.ConflictsTruncated {
		return fmt.Errorf("oracle: replay mismatch vs live status")
	}
	for _, raw := range w.disk.durable[w.buildID] {
		if ev, err := queue.DecodeEvent(raw); err == nil && ev.Type == queue.EventFinalized {
			w.stats[stDeadline]++
		}
	}
	liveTests, err := w.queryAllServerTests()
	if err != nil {
		return err
	}
	if len(liveTests) != len(o.tests) {
		return fmt.Errorf("oracle: live test count %d != %d", len(liveTests), len(o.tests))
	}
	for i, tv := range liveTests {
		rt := replayed.Tests[i]
		if tv.State != rt.Status || tv.Generation != rt.Generation || tv.Timed != rt.Timed || tv.DurationMS != rt.DurationMS ||
			len(tv.History) != len(rt.History) || tv.HistoryTruncated != rt.HistoryTruncated {
			return fmt.Errorf("oracle: test %s replay mismatch", tv.Test)
		}
		if tv.State == queue.StatusUnresolved && tv.Generation >= o.cfg.MaxAttempts {
			w.stats[stExhausted]++
		}
		for _, a := range tv.Attempts {
			if a.Status == queue.AttemptLeased || a.FinishedAtMS == 0 {
				return fmt.Errorf("oracle: finished attempt %s has status=%s finished_at=%d", a.AttemptID, a.Status, a.FinishedAtMS)
			}
		}
	}
	return nil
}
