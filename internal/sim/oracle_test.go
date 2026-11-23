package sim_test

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/queuerunner"
)

type oracleExec struct {
	leaseID                      int64
	runnerID, exitCode           int
	format                       string
	attempts                     []queue.LeasedAttemptView
	plannedReadErr, readErr      error
	completed, stopped, signaled bool
	filesWritten                 map[string]junit.File
}

type acceptedTruth struct {
	leaseID, durationMS int64
	attemptID           string
	outcome             queue.Outcome
	timed               bool
}

type leaseReq struct {
	reqID   string
	leaseID int64
}

// simOracle tracks ground-truth command execution independently of queue logic,
// applies stored events incrementally on every step, and performs a full disk
// replay check at build completion (and on broken builds after restart).
type simOracle struct {
	buildID                                       string
	lim                                           queue.Limits
	expectCleanPass, initialized, hadInitConflict bool
	tests                                         []string
	cfg                                           queue.Config
	finishedVerdict                               queue.Verdict
	shadow                                        *queue.State
	storedEvents                                  int
	execs                                         map[int64]*oracleExec
	accepted, acceptedAttempt                     map[string]acceptedTruth
	lastLeaseReq                                  map[string]leaseReq
	reportedLeases                                map[int64]int
	runnerLeases                                  map[string]int
	hbOpLease                                     map[uint64]int64
	leaseLostWhileRunning                         map[int64]bool
}

func newSimOracle(buildID string, lim queue.Limits, expectCleanPass bool) *simOracle {
	return &simOracle{
		buildID: buildID, lim: lim, expectCleanPass: expectCleanPass, shadow: queue.NewState(lim),
		execs: make(map[int64]*oracleExec), accepted: make(map[string]acceptedTruth),
		acceptedAttempt: make(map[string]acceptedTruth), lastLeaseReq: make(map[string]leaseReq),
		reportedLeases: make(map[int64]int), runnerLeases: make(map[string]int),
		hbOpLease: make(map[uint64]int64), leaseLostWhileRunning: make(map[int64]bool),
	}
}

func (o *simOracle) recordCommandStart(leaseID int64, runnerID int, format string, attempts []queue.LeasedAttemptView, plannedReadErr error) error {
	if prev, dup := o.execs[leaseID]; dup {
		return fmt.Errorf("oracle: runner %d started lease %d twice (prev runner %d) — stale lease response applied", runnerID, leaseID, prev.runnerID)
	}
	o.execs[leaseID] = &oracleExec{leaseID: leaseID, runnerID: runnerID, format: format, attempts: attempts, plannedReadErr: plannedReadErr}
	return nil
}

func (o *simOracle) onServerHandled(w *simWorld, req queuerunner.Request, status int, body []byte, newEvents []queue.Event) error {
	for _, ev := range newEvents {
		if err := o.shadow.Apply(ev); err != nil {
			return fmt.Errorf("oracle: incremental replay failed on %s: %w", ev.Type, err)
		}
		o.storedEvents++
		switch ev.Type {
		case queue.EventInitialized:
			if ev.Initialized != nil {
				o.initialized, o.tests, o.cfg = true, slices.Clone(ev.Initialized.Tests), ev.Initialized.Config
			}
		case queue.EventConflicted:
			o.hadInitConflict = true
		case queue.EventLeased:
			if ev.Leased != nil {
				if prev, seen := o.lastLeaseReq[ev.Leased.RunnerID]; seen && prev.reqID == ev.Leased.RequestID {
					return fmt.Errorf("oracle: repeated request_id %q emitted new lease %d (prev %d)", ev.Leased.RequestID, ev.Leased.LeaseID, prev.leaseID)
				}
				o.runnerLeases[ev.Leased.RunnerID]++
				o.lastLeaseReq[ev.Leased.RunnerID] = leaseReq{ev.Leased.RequestID, ev.Leased.LeaseID}
			}
		case queue.EventReported:
			if ev.Reported == nil {
				continue
			}
			ex := o.execs[ev.Reported.LeaseID]
			for _, ra := range ev.Reported.Results {
				if ra.Disposition != queue.DispositionAccepted {
					continue
				}
				attID, testPath := queue.FormatAttemptID(ra.TestIndex, ra.Generation), o.tests[ra.TestIndex]
				if ra.Outcome == queue.OutcomeError {
					o.acceptedAttempt[attID] = acceptedTruth{leaseID: ev.Reported.LeaseID, attemptID: attID, outcome: queue.OutcomeError}
					continue
				}
				if prev, dup := o.accepted[testPath]; dup {
					return fmt.Errorf("oracle: test %s accepted twice (prev=%+v, new=%s)", testPath, prev, attID)
				}
				sub := queue.ReportResult{AttemptID: attID, TestIndex: ra.TestIndex, Generation: ra.Generation, Outcome: ra.Outcome, Timed: ra.Timed, DurationMS: ra.DurationMS}
				if err := verifyOutcomeTruth(testPath, sub, ex); err != nil {
					return err
				}
				truth := acceptedTruth{leaseID: ev.Reported.LeaseID, attemptID: attID, outcome: ra.Outcome, timed: ra.Timed, durationMS: ra.DurationMS}
				o.accepted[testPath], o.acceptedAttempt[attID] = truth, truth
			}
		}
	}
	if o.initialized && !w.disk.broken[w.buildID] && len(newEvents) > 0 {
		if err := o.checkShadowBounds(); err != nil {
			return err
		}
	}
	if status != http.StatusOK {
		if req.Kind == queuerunner.ReqInit && status == http.StatusConflict {
			o.hadInitConflict = true
			if o.finishedVerdict == queue.VerdictPassed {
				w.stats["conflictAfterPassed"]++
				o.finishedVerdict = queue.VerdictUnresolved
			}
		}
		return nil
	}
	switch req.Kind {
	case queuerunner.ReqLease:
		resp, err := queueapi.ParseLeaseResponse(status, body)
		if err != nil {
			return fmt.Errorf("oracle: invalid 200 lease response: %w", err)
		}
		if resp.Status == queue.LeaseGranted {
			if prev, seen := o.lastLeaseReq[req.Lease.RunnerID]; seen && prev.reqID == req.Lease.RequestID && resp.LeaseID != prev.leaseID {
				return fmt.Errorf("oracle: repeated request_id %q got new lease %d (prev %d)", req.Lease.RequestID, resp.LeaseID, prev.leaseID)
			}
		} else if resp.Status == queue.LeaseDone {
			return o.observeServerVerdict(resp.Verdict)
		}
	case queuerunner.ReqReport:
		resp, err := queueapi.ParseReportResponse(status, body)
		if err != nil {
			return fmt.Errorf("oracle: invalid 200 report response: %w", err)
		}
		if _, seen := o.reportedLeases[req.Report.LeaseID]; !seen {
			o.reportedLeases[req.Report.LeaseID] = w.srvEpoch
		}
		if err := o.verifyReport(req.Report, resp, newEvents); err != nil {
			return err
		}
		return o.observeServerVerdict(resp.Verdict)
	case queuerunner.ReqStatus:
		if resp, err := queueapi.ParseStatusResponse(status, body); err == nil {
			return o.observeServerVerdict(resp.Verdict)
		}
	}
	return nil
}

func (o *simOracle) checkShadowBounds() error {
	if bound := 10 + len(o.tests)*(3*o.cfg.MaxAttempts+8); o.storedEvents > bound {
		return fmt.Errorf("oracle: persisted events %d > bound %d", o.storedEvents, bound)
	}
	if len(o.shadow.Conflicts) > queue.MaxInitConflicts {
		return fmt.Errorf("oracle: conflicts %d > %d", len(o.shadow.Conflicts), queue.MaxInitConflicts)
	}
	if slices.ContainsFunc(o.shadow.Tests, func(t queue.TestState) bool { return len(t.History) > queue.MaxTestHistory }) {
		return fmt.Errorf("oracle: test history exceeded %d", queue.MaxTestHistory)
	}
	seenR, seenT := make(map[string]int64), make(map[int]int64)
	for _, lid := range o.shadow.ActiveLeases {
		l := o.shadow.Leases[lid-1]
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

func (o *simOracle) verifyReport(cmd *queue.ReportCommand, resp queueapi.ReportResponse, newEvents []queue.Event) error {
	resByID := make(map[string]queue.ReportResult, len(cmd.Results))
	for _, r := range cmd.Results {
		resByID[r.AttemptID] = r
	}
	allDup := true
	for _, d := range resp.Results {
		sub, ok := resByID[d.AttemptID]
		if !ok {
			return fmt.Errorf("oracle: response named unknown attempt %s", d.AttemptID)
		}
		if prev, wasAcc := o.acceptedAttempt[d.AttemptID]; wasAcc && prev.outcome == sub.Outcome && prev.timed == sub.Timed && prev.durationMS == sub.DurationMS &&
			d.Disposition != queue.DispositionDuplicate && d.Disposition != queue.DispositionAccepted {
			return fmt.Errorf("oracle: identical resend of accepted attempt %s got %q; want duplicate", d.AttemptID, d.Disposition)
		}
		allDup = allDup && d.Disposition == queue.DispositionDuplicate
	}
	if allDup && len(resp.Results) > 0 && slices.ContainsFunc(newEvents, func(ev queue.Event) bool { return ev.Type == queue.EventReported }) {
		return fmt.Errorf("oracle: all-duplicate report for lease %d emitted reported event", cmd.LeaseID)
	}
	return nil
}

func verifyOutcomeTruth(testPath string, sub queue.ReportResult, ex *oracleExec) error {
	if ex == nil || !ex.completed || ex.stopped || ex.readErr != nil || ex.filesWritten == nil {
		return fmt.Errorf("oracle: accepted %s for %s without clean completed report", sub.Outcome, testPath)
	}
	for f := range ex.filesWritten {
		if !slices.ContainsFunc(ex.attempts, func(a queue.LeasedAttemptView) bool { return a.Test == f }) {
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
			return fmt.Errorf("oracle: accepted skipped for %s on exit=%d sig=%t", testPath, ex.exitCode, ex.signaled)
		}
	case queue.OutcomePassed:
		if ex.signaled || f.Failed || f.Cases == 0 || f.Skipped == f.Cases {
			return fmt.Errorf("oracle: invalid passed for %s (signaled=%t)", testPath, ex.signaled)
		}
		if ex.exitCode == 0 {
			if !sub.Timed || sub.DurationMS != f.MS() {
				return fmt.Errorf("oracle: exit-0 passed %s timed=%t ms=%d want true/%d", testPath, sub.Timed, sub.DurationMS, f.MS())
			}
		} else if anyFail := slices.ContainsFunc(ex.attempts, func(a queue.LeasedAttemptView) bool { return ex.filesWritten[a.Test].Failed }); ex.format != "junit" || !anyFail || f.Skipped > 0 || sub.Timed || sub.DurationMS != 0 {
			return fmt.Errorf("oracle: invalid non-zero passed for %s", testPath)
		}
	}
	return nil
}

func (o *simOracle) observeServerVerdict(v queue.Verdict) error {
	if v == queue.VerdictRunning || v == "" {
		return nil
	}
	if o.finishedVerdict == "" {
		o.finishedVerdict = v
	} else if o.finishedVerdict != v {
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

func (o *simOracle) onRunnerExit(w *simWorld, r *simRunner) error {
	if r.exitErr == nil {
		if want, ok := r.sentVerdicts[r.lastOpID]; !ok || r.verdict != want {
			return fmt.Errorf("oracle: runner %d exited with verdict %q; server sent %q", r.id, r.verdict, want)
		}
		return nil
	}
	var extraErr *queuerunner.ExtraFilesError
	if errors.As(r.exitErr, &extraErr) {
		return nil
	}
	var apiErr *queueapi.APIError
	if errors.As(r.exitErr, &apiErr) {
		if apiErr.HTTPStatus == http.StatusConflict && apiErr.Code == queue.ErrCodeInitConflict {
			return nil
		}
		if apiErr.HTTPStatus == http.StatusConflict && apiErr.Code == queue.ErrCodeLeaseHeld {
			if w.nowMS-r.startedAtMS >= w.cfg.BuildTimeoutMS {
				return nil
			}
			return fmt.Errorf("oracle: runner %d exited on 409 lease_held after %dms (build_timeout=%dms)", r.id, w.nowMS-r.startedAtMS, w.cfg.BuildTimeoutMS)
		}
	}
	if w.disk.broken[w.buildID] {
		return nil
	}
	if queueapi.IsTransientError(r.exitErr) && r.firstFailMS > 0 && w.nowMS-r.firstFailMS >= 60_000 {
		return nil
	}
	return fmt.Errorf("oracle: runner %d exited with disallowed error %v (elapsedFail=%dms)", r.id, r.exitErr, w.nowMS-r.firstFailMS)
}

func (o *simOracle) onStatusDone(w *simWorld, sc *simStatusClient, a queuerunner.Action) error {
	if a.Err != nil {
		var apiErr *queueapi.APIError
		if w.disk.broken[w.buildID] || (errors.As(a.Err, &apiErr) && apiErr.HTTPStatus == http.StatusNotFound && sc.uninit404[sc.lastOpID]) {
			return nil
		}
		return fmt.Errorf("oracle: status failed unexpectedly: %v", a.Err)
	}
	if want, ok := sc.sentVerdicts[sc.lastOpID]; ok && a.Verdict != want {
		return fmt.Errorf("oracle: status returned verdict %q; server sent %q", a.Verdict, want)
	}
	if !a.HasReport || a.Report == nil {
		return nil
	}
	st := a.Status
	if st.Verdict == queue.VerdictRunning || st.Counts.Unresolved > 0 || len(st.Conflicts) > 0 || len(a.Report.Tests) != len(o.tests) {
		return fmt.Errorf("oracle: invalid RunReport emission (verdict=%s tests=%d/%d)", st.Verdict, len(a.Report.Tests), len(o.tests))
	}
	for _, p := range o.tests {
		obs, ok := a.Report.Tests[p]
		acc, accOK := o.accepted[p]
		wantTimed, wantMS := acc.outcome == queue.OutcomePassed && acc.timed, int64(0)
		if wantTimed {
			wantMS = acc.durationMS
		}
		if !ok || !accOK || obs.Timed != wantTimed || obs.MS != wantMS {
			return fmt.Errorf("oracle: RunReport[%s]=%+v (ok=%t) want timed=%t ms=%d", p, obs, ok && accOK, wantTimed, wantMS)
		}
	}
	return nil
}

func (o *simOracle) checkFinal(w *simWorld) error {
	queryStatus := func() (queueapi.StatusResponse, int, error) {
		code, body := w.invokeHandler(queueapi.EncodeStatus(w.buildID), simReadTok)
		if code != http.StatusOK {
			return queueapi.StatusResponse{}, code, nil
		}
		resp, err := queueapi.ParseStatusResponse(code, body)
		return resp, code, err
	}
	if w.disk.broken[w.buildID] {
		if _, code, err := queryStatus(); err != nil || code != http.StatusInternalServerError {
			return fmt.Errorf("oracle: broken build answered HTTP %d (err=%v); want 500", code, err)
		}
		if err := w.restartServer(); err != nil {
			return err
		}
		stResp, code, err := queryStatus()
		if err != nil {
			return err
		}
		replayed, err := w.disk.replayBuild(w.buildID, w.lim)
		if err != nil {
			return fmt.Errorf("oracle: broken-build disk replay failed: %w", err)
		}
		if !replayed.Initialized {
			if code != http.StatusNotFound {
				return fmt.Errorf("oracle: uninitialized restarted broken build status=%d; want 404", code)
			}
			return nil
		}
		if code != http.StatusOK || stResp.Verdict != replayed.Verdict() || stResp.Counts != replayed.Counts {
			return fmt.Errorf("oracle: restarted broken build mismatch: http=%d live=(%s %+v) replay=(%s %+v)", code, stResp.Verdict, stResp.Counts, replayed.Verdict(), replayed.Counts)
		}
		return nil
	}
	if !o.initialized {
		return fmt.Errorf("oracle: build never initialized")
	}
	stResp, code, err := queryStatus()
	if err != nil || code != http.StatusOK || stResp.Verdict == queue.VerdictRunning {
		return fmt.Errorf("oracle: final status invalid (http=%d verdict=%s counts=%+v err=%v)", code, stResp.Verdict, stResp.Counts, err)
	}
	if err := o.observeServerVerdict(stResp.Verdict); err != nil {
		return err
	}
	w.stats[string(stResp.Verdict)]++
	if o.expectCleanPass && stResp.Verdict != queue.VerdictPassed {
		return fmt.Errorf("oracle: clean report-outage build finished %s; want passed (heartbeats must keep lease alive during Report retry)", stResp.Verdict)
	}
	if slices.ContainsFunc(w.runners, func(r *simRunner) bool { return !r.killed && !r.exited }) {
		return fmt.Errorf("oracle: liveness failure: a live runner never exited")
	}
	if stResp.CreatedAtMS <= 0 || stResp.FinishedAtMS == nil || *stResp.FinishedAtMS < stResp.CreatedAtMS {
		return fmt.Errorf("oracle: invalid build timestamps created=%d finished=%v", stResp.CreatedAtMS, stResp.FinishedAtMS)
	}
	for _, rs := range stResp.Runners {
		if rs.Leases != o.runnerLeases[rs.RunnerID] || rs.ActiveLeaseID != 0 {
			return fmt.Errorf("oracle: runner %s leases=%d (want %d) active=%d", rs.RunnerID, rs.Leases, o.runnerLeases[rs.RunnerID], rs.ActiveLeaseID)
		}
	}
	for i, sl := range stResp.Slowest {
		acc, ok := o.accepted[sl.Test]
		if !ok || acc.outcome != queue.OutcomePassed || !acc.timed || acc.durationMS != sl.DurationMS || (i > 0 && stResp.Slowest[i-1].DurationMS < sl.DurationMS) {
			return fmt.Errorf("oracle: slowest[%d]=%+v invalid vs %+v", i, sl, acc)
		}
	}
	replayed, err := w.disk.replayBuild(w.buildID, w.lim)
	if err != nil || replayed.Verdict() != stResp.Verdict || replayed.Counts != stResp.Counts ||
		len(replayed.Conflicts) != len(stResp.Conflicts) || replayed.ConflictsTruncated != stResp.ConflictsTruncated {
		return fmt.Errorf("oracle: final replay mismatch (err=%v)", err)
	}
	for _, raw := range w.disk.durable[w.buildID] {
		if ev, err := queue.DecodeEvent(raw); err == nil && ev.Type == queue.EventFinalized {
			w.stats["deadline"]++
		}
	}
	var liveTests []queue.TestView
	for cursor := 0; ; {
		c, body := w.invokeHandler(queueapi.EncodeListTests(w.buildID, queue.ListTestsCommand{Cursor: cursor, Limit: 200}), simReadTok)
		var page queueapi.ListTestsResponse
		if c != http.StatusOK || json.Unmarshal(body, &page) != nil {
			return fmt.Errorf("list tests failed status=%d", c)
		}
		liveTests = append(liveTests, page.Tests...)
		if page.NextCursor == nil {
			break
		}
		if *page.NextCursor <= cursor {
			return fmt.Errorf("cursor did not advance: %d -> %d", cursor, *page.NextCursor)
		}
		cursor = *page.NextCursor
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
			w.stats["exhausted"]++
		}
		if slices.ContainsFunc(tv.Attempts, func(a queue.AttemptRecord) bool { return a.Status == queue.AttemptLeased || a.FinishedAtMS == 0 }) {
			return fmt.Errorf("oracle: test %s has unfinished attempt", tv.Test)
		}
	}
	return nil
}
