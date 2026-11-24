package sim_test

import (
	"bytes"
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
	leaseID        int64
	runnerID       int
	exitCode       int
	format         string
	attempts       []queue.LeasedAttemptView
	plannedReadErr error
	readErr        error
	completed      bool
	stopped        bool
	signaled       bool
	filesWritten   map[string]junit.File
}

type acceptedTruth struct {
	leaseID    int64
	durationMS int64
	attemptID  string
	outcome    queue.Outcome
	timed      bool
}

type leaseReq struct {
	reqID   string
	leaseID int64
}

type simOracle struct {
	buildID               string
	lim                   queue.Limits
	expectCleanPass       bool
	initialized           bool
	hadInitConflict       bool
	ackedInitialized      bool
	ackedFinishedVerdict  queue.Verdict
	finishedVerdict       queue.Verdict
	ackedLeases           map[int64]bool
	leaseLostWhileRunning map[int64]bool
	ackedAccepted         map[string]acceptedTruth
	accepted              map[string]acceptedTruth
	acceptedAttempt       map[string]acceptedTruth
	tests                 []string
	cfg                   queue.Config
	shadow                *queue.State
	storedEvents          int
	execs                 map[int64]*oracleExec
	lastLeaseReq          map[string]leaseReq
	reportedLeases        map[int64]int
	runnerLeases          map[string]int
	hbOpLease             map[uint64]int64
}

func newSimOracle(buildID string, lim queue.Limits, expectCleanPass bool) *simOracle {
	return &simOracle{
		buildID:               buildID,
		lim:                   lim,
		expectCleanPass:       expectCleanPass,
		shadow:                queue.NewState(lim),
		ackedLeases:           make(map[int64]bool),
		ackedAccepted:         make(map[string]acceptedTruth),
		execs:                 make(map[int64]*oracleExec),
		accepted:              make(map[string]acceptedTruth),
		acceptedAttempt:       make(map[string]acceptedTruth),
		lastLeaseReq:          make(map[string]leaseReq),
		reportedLeases:        make(map[int64]int),
		runnerLeases:          make(map[string]int),
		hbOpLease:             make(map[uint64]int64),
		leaseLostWhileRunning: make(map[int64]bool),
	}
}

func (o *simOracle) recordCommandStart(leaseID int64, runnerID int, format string, attempts []queue.LeasedAttemptView, plannedReadErr error) error {
	if prev, dup := o.execs[leaseID]; dup {
		return fmt.Errorf("oracle: runner %d started lease %d twice (prev runner %d)", runnerID, leaseID, prev.runnerID)
	}
	o.execs[leaseID] = &oracleExec{
		leaseID:        leaseID,
		runnerID:       runnerID,
		format:         format,
		attempts:       attempts,
		plannedReadErr: plannedReadErr,
	}
	return nil
}

func (o *simOracle) recordAcked200(req queuerunner.Request, body []byte, newEvents []queue.Event) {
	for _, ev := range newEvents {
		switch ev.Type {
		case queue.EventInitialized:
			o.ackedInitialized = true
		case queue.EventLeased:
			if ev.Leased != nil {
				o.ackedLeases[ev.Leased.LeaseID] = true
			}
		case queue.EventReported:
			if ev.Reported != nil {
				for _, ra := range ev.Reported.Results {
					if ra.Disposition == queue.DispositionAccepted && ra.Outcome != queue.OutcomeError {
						p := o.tests[ra.TestIndex]
						o.ackedAccepted[p] = acceptedTruth{
							leaseID:    ev.Reported.LeaseID,
							durationMS: ra.DurationMS,
							attemptID:  queue.FormatAttemptID(ra.TestIndex, ra.Generation),
							outcome:    ra.Outcome,
							timed:      ra.Timed,
						}
					}
				}
			}
		}
	}
	switch req.Kind {
	case queuerunner.ReqLease:
		if r, err := queueapi.ParseLeaseResponse(http.StatusOK, body); err == nil && r.Status == queue.LeaseDone && r.Verdict != queue.VerdictRunning {
			o.ackedFinishedVerdict = r.Verdict
		}
	case queuerunner.ReqReport:
		if r, err := queueapi.ParseReportResponse(http.StatusOK, body); err == nil {
			if r.Verdict != queue.VerdictRunning {
				o.ackedFinishedVerdict = r.Verdict
			}
			// If a resend after restart was answered 200 duplicate for an
			// attempt that was already in the loaded log (from page cache),
			// that accepted outcome is now acknowledged to the client and MUST
			// survive any subsequent power loss.
			for _, d := range r.Results {
				if d.Disposition == queue.DispositionDuplicate {
					if truth, ok := o.acceptedAttempt[d.AttemptID]; ok && truth.outcome != queue.OutcomeError {
						p := o.tests[d.TestIndex]
						o.ackedAccepted[p] = truth
					}
				}
			}
		}
	case queuerunner.ReqStatus:
		if r, err := queueapi.ParseStatusResponse(http.StatusOK, body); err == nil && r.Verdict != queue.VerdictRunning {
			o.ackedFinishedVerdict = r.Verdict
		}
	}
}

func (o *simOracle) applyShadowEvent(ev queue.Event, live bool) error {
	if err := o.shadow.Apply(ev); err != nil {
		return fmt.Errorf("oracle: shadow apply %s: %w", ev.Type, err)
	}
	o.storedEvents++
	switch ev.Type {
	case queue.EventInitialized:
		if ev.Initialized != nil {
			o.initialized = true
			o.tests = slices.Clone(ev.Initialized.Tests)
			o.cfg = ev.Initialized.Config
		}
	case queue.EventConflicted:
		o.hadInitConflict = true
	case queue.EventLeased:
		if ev.Leased != nil {
			if live {
				if prev, seen := o.lastLeaseReq[ev.Leased.RunnerID]; seen && prev.reqID == ev.Leased.RequestID {
					return fmt.Errorf("oracle: repeated request_id %q emitted new lease %d (prev %d)", ev.Leased.RequestID, ev.Leased.LeaseID, prev.leaseID)
				}
			}
			o.runnerLeases[ev.Leased.RunnerID]++
			o.lastLeaseReq[ev.Leased.RunnerID] = leaseReq{reqID: ev.Leased.RequestID, leaseID: ev.Leased.LeaseID}
		}
	case queue.EventReported:
		if ev.Reported == nil {
			return nil
		}
		ex := o.execs[ev.Reported.LeaseID]
		for _, ra := range ev.Reported.Results {
			if ra.Disposition != queue.DispositionAccepted {
				continue
			}
			attID := queue.FormatAttemptID(ra.TestIndex, ra.Generation)
			testPath := o.tests[ra.TestIndex]
			if ra.Outcome == queue.OutcomeError {
				o.acceptedAttempt[attID] = acceptedTruth{leaseID: ev.Reported.LeaseID, attemptID: attID, outcome: queue.OutcomeError}
				continue
			}
			if prev, dup := o.accepted[testPath]; dup {
				return fmt.Errorf("oracle: test %s accepted twice (prev=%+v, new=%s)", testPath, prev, attID)
			}
			if live {
				if prev, dup := o.ackedAccepted[testPath]; dup {
					return fmt.Errorf("oracle: acknowledged test %s accepted again after crash (prev=%+v, new=%s)", testPath, prev, attID)
				}
				sub := queue.ReportResult{
					AttemptID:  attID,
					TestIndex:  ra.TestIndex,
					Generation: ra.Generation,
					Outcome:    ra.Outcome,
					Timed:      ra.Timed,
					DurationMS: ra.DurationMS,
				}
				if err := verifyOutcomeTruth(testPath, sub, ex); err != nil {
					return err
				}
			}
			truth := acceptedTruth{
				leaseID:    ev.Reported.LeaseID,
				durationMS: ra.DurationMS,
				attemptID:  attID,
				outcome:    ra.Outcome,
				timed:      ra.Timed,
			}
			o.accepted[testPath] = truth
			o.acceptedAttempt[attID] = truth
		}
	}
	return nil
}

func (o *simOracle) verifyAfterRestart(w *simWorld, loaded queueapi.LoadResult) error {
	if w.expectDamaged || w.readErrDamaged {
		return nil
	}
	if slices.Contains(loaded.Damaged, w.buildFile) {
		return fmt.Errorf("oracle: build %s unexpectedly marked damaged after restart", w.buildID)
	}
	var st *queue.State
	for _, b := range loaded.Builds {
		if b.BuildID == w.buildID {
			st = b
			break
		}
	}
	if o.ackedInitialized && st == nil {
		return fmt.Errorf("oracle: build %s was answered 200 on Init before crash but missing after restart", w.buildID)
	}
	if st == nil {
		o.shadow = queue.NewState(o.lim)
		o.storedEvents = 0
		o.initialized = false
		o.hadInitConflict = false
		clear(o.runnerLeases)
		clear(o.lastLeaseReq)
		clear(o.accepted)
		clear(o.acceptedAttempt)
		return nil
	}
	for lid := range o.ackedLeases {
		if int(lid) > len(st.Leases) {
			return fmt.Errorf("oracle: lease %d answered 200 before crash is missing after restart (leases=%d)", lid, len(st.Leases))
		}
	}
	for p, want := range o.ackedAccepted {
		idx := slices.IndexFunc(st.Tests, func(ts queue.TestState) bool { return ts.Path == p })
		if idx < 0 {
			return fmt.Errorf("oracle: accepted test %s missing after restart", p)
		}
		ts := st.Tests[idx]
		if string(ts.Status) != string(want.outcome) || ts.Timed != want.timed || ts.DurationMS != want.durationMS {
			return fmt.Errorf("oracle: accepted %s (%+v) before crash lost/changed after restart (%+v)", p, want, ts)
		}
	}
	if o.ackedFinishedVerdict != "" && st.Verdict() == queue.VerdictRunning {
		return fmt.Errorf("oracle: finished build (%s) regressed to running after restart", o.ackedFinishedVerdict)
	}

	// Rebuild shadow state (including accepted and acceptedAttempt) from the
	// post-restart log so unacknowledged events lost to a crash do not cause
	// false double-accept errors, while o.ackedAccepted still catches any loss
	// or double-accept of acknowledged events.
	o.shadow = queue.NewState(o.lim)
	o.storedEvents = 0
	o.hadInitConflict = false
	o.initialized = false
	clear(o.runnerLeases)
	clear(o.lastLeaseReq)
	clear(o.accepted)
	clear(o.acceptedAttempt)
	for _, ev := range parseDiskEventsSlice(w.fs.pendingFiles[w.buildFile]) {
		if err := o.applyShadowEvent(ev, false); err != nil {
			return err
		}
	}
	return nil
}

func (o *simOracle) onServerHandled(w *simWorld, req queuerunner.Request, status int, body []byte, newEvents []queue.Event) error {
	for _, ev := range newEvents {
		if err := o.applyShadowEvent(ev, true); err != nil {
			return err
		}
	}
	if o.initialized && !w.buildBroken && len(newEvents) > 0 {
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
			if o.ackedFinishedVerdict != "" && resp.Verdict == queue.VerdictRunning {
				return fmt.Errorf("oracle: finished build (%s) answered running", o.ackedFinishedVerdict)
			}
			return o.observeServerVerdict(resp.Verdict)
		}
	}
	return nil
}

func (o *simOracle) checkShadowBounds() error {
	if bound := 10 + len(o.tests)*(3*o.cfg.MaxAttempts+8); o.storedEvents > bound {
		return fmt.Errorf("oracle: persisted events %d > bound %d", o.storedEvents, bound)
	}
	overHistory := slices.ContainsFunc(o.shadow.Tests, func(t queue.TestState) bool {
		return len(t.History) > queue.MaxTestHistory
	})
	if len(o.shadow.Conflicts) > queue.MaxInitConflicts || overHistory {
		return errors.New("oracle: conflicts/history bound exceeded")
	}
	seenR := make(map[string]int64)
	seenT := make(map[int]int64)
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
		if d.Disposition == queue.DispositionAccepted && !slices.ContainsFunc(newEvents, func(ev queue.Event) bool {
			return ev.Type == queue.EventReported && ev.Reported != nil && slices.ContainsFunc(ev.Reported.Results, func(ra queue.ReportedAttempt) bool {
				return ra.TestIndex == sub.TestIndex && ra.Generation == sub.Generation && ra.Disposition == queue.DispositionAccepted
			})
		}) {
			return fmt.Errorf("oracle: response accepted attempt %s without a stored reported event", d.AttemptID)
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
		} else {
			anyFail := slices.ContainsFunc(ex.attempts, func(a queue.LeasedAttemptView) bool { return ex.filesWritten[a.Test].Failed })
			if ex.format != "junit" || !anyFail || f.Skipped > 0 || sub.Timed || sub.DurationMS != 0 {
				return fmt.Errorf("oracle: invalid non-zero passed for %s", testPath)
			}
		}
	}
	return nil
}

func (o *simOracle) observeServerVerdict(v queue.Verdict) error {
	if v == queue.VerdictRunning || v == "" {
		return nil
	}
	if o.finishedVerdict != "" && o.finishedVerdict != v && !(o.finishedVerdict == queue.VerdictPassed && v == queue.VerdictUnresolved && o.hadInitConflict) {
		return fmt.Errorf("oracle: illegal verdict transition %s -> %s (hadInitConflict=%t)", o.finishedVerdict, v, o.hadInitConflict)
	}
	o.finishedVerdict = v
	if v == queue.VerdictPassed {
		if o.hadInitConflict {
			return errors.New("oracle: verdict passed despite init conflict")
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
	var apiErr *queueapi.APIError
	isAllowedConflict := errors.As(r.exitErr, &apiErr) &&
		apiErr.HTTPStatus == http.StatusConflict &&
		(apiErr.Code == queue.ErrCodeInitConflict ||
			(apiErr.Code == queue.ErrCodeLeaseHeld && w.nowMS-r.startedAtMS >= w.cfg.BuildTimeoutMS))
	isTransientTimeout := queueapi.IsTransientError(r.exitErr) &&
		r.firstFailMS > 0 &&
		w.nowMS-r.firstFailMS >= 60_000
	if errors.As(r.exitErr, &extraErr) || w.buildBroken || isAllowedConflict || isTransientTimeout {
		return nil
	}
	return fmt.Errorf("oracle: runner %d exited with disallowed error %v (elapsed=%dms fail=%dms)", r.id, r.exitErr, w.nowMS-r.startedAtMS, w.nowMS-r.firstFailMS)
}

func (o *simOracle) onStatusDone(w *simWorld, sc *simStatusClient, a queuerunner.Action) error {
	if a.Err != nil {
		var apiErr *queueapi.APIError
		if w.buildBroken ||
			(errors.As(a.Err, &apiErr) && apiErr.HTTPStatus == http.StatusNotFound && sc.uninit404[sc.lastOpID]) ||
			(queueapi.IsTransientError(a.Err) && sc.firstFailMS > 0 && w.nowMS-sc.firstFailMS >= 30_000) {
			return nil
		}
		return fmt.Errorf("oracle: status %d failed with disallowed error: %v", sc.id, a.Err)
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
		wantTimed := acc.outcome == queue.OutcomePassed && acc.timed
		var wantMS int64
		if wantTimed {
			wantMS = acc.durationMS
		}
		if !ok || !accOK || obs.Timed != wantTimed || obs.MS != wantMS {
			return fmt.Errorf("oracle: RunReport[%s]=%+v want timed=%t ms=%d", p, obs, wantTimed, wantMS)
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
	loadQuiet := func() (queueapi.LoadResult, error) {
		w.fs.suppressFaults = true
		defer func() { w.fs.suppressFaults = false }()
		return queueapi.NewDiskLog(w.fs, nil).Load(w.lim)
	}

	if w.buildBroken {
		if _, code, err := queryStatus(); err != nil || code != http.StatusInternalServerError {
			return fmt.Errorf("oracle: broken build answered HTTP %d (err=%v); want 500", code, err)
		}
		savedBytes := bytes.Clone(w.fs.durableFiles[w.buildFile])
		w.readErrDamaged = false
		w.fs.suppressFaults = true
		err := w.restartServer(true)
		w.fs.suppressFaults = false
		if err != nil {
			return err
		}
		stResp, code, err := queryStatus()
		if err != nil {
			return err
		}
		if !w.fs.durableDir[w.buildFile] {
			if code != http.StatusNotFound {
				return fmt.Errorf("oracle: uninitialized restarted broken build status=%d; want 404", code)
			}
			return nil
		}
		loaded, err := loadQuiet()
		if err != nil {
			return err
		}
		if slices.Contains(loaded.Damaged, w.buildFile) {
			if code != http.StatusInternalServerError {
				return fmt.Errorf("oracle: damaged build status=%d; want 500", code)
			}
			if !bytes.Equal(w.fs.durableFiles[w.buildFile], savedBytes) {
				return errors.New("oracle: damaged file was modified during restart")
			}
			return nil
		}
		if len(loaded.Builds) != 1 || code != http.StatusOK || stResp.Verdict != loaded.Builds[0].Verdict() || stResp.Counts != loaded.Builds[0].Counts {
			return fmt.Errorf("oracle: restarted broken build mismatch: http=%d live=(%s %+v)", code, stResp.Verdict, stResp.Counts)
		}
		return nil
	}

	if !o.initialized {
		return errors.New("oracle: build never initialized")
	}
	stResp, code, err := queryStatus()
	if err != nil || code != http.StatusOK || stResp.Verdict == queue.VerdictRunning {
		return fmt.Errorf("oracle: final status invalid (http=%d verdict=%s err=%v)", code, stResp.Verdict, err)
	}
	if err := o.observeServerVerdict(stResp.Verdict); err != nil {
		return err
	}
	w.stats[string(stResp.Verdict)]++
	if o.expectCleanPass && stResp.Verdict != queue.VerdictPassed {
		return fmt.Errorf("oracle: clean build finished %s; want passed", stResp.Verdict)
	}
	if slices.ContainsFunc(w.runners, func(r *simRunner) bool { return !r.killed && !r.exited }) ||
		slices.ContainsFunc(w.statusClients, func(sc *simStatusClient) bool { return !sc.exited }) {
		return errors.New("oracle: liveness failure: a live runner or status client never exited")
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
			return fmt.Errorf("oracle: slowest[%d]=%+v invalid", i, sl)
		}
	}

	loaded, err := loadQuiet()
	if err != nil || len(loaded.Damaged) > 0 || len(loaded.Builds) != 1 {
		return fmt.Errorf("oracle: final DiskLog.Load failed (err=%v damaged=%v builds=%d)", err, loaded.Damaged, len(loaded.Builds))
	}
	replayed := loaded.Builds[0]
	if replayed.Verdict() != stResp.Verdict || replayed.Counts != stResp.Counts ||
		len(replayed.Conflicts) != len(stResp.Conflicts) || replayed.ConflictsTruncated != stResp.ConflictsTruncated {
		return errors.New("oracle: final replay mismatch")
	}
	for _, ev := range parseDiskEventsSlice(w.fs.durableFiles[w.buildFile]) {
		if ev.Type == queue.EventFinalized {
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

	if w.faults.injectBitrot {
		w.expectDamaged = true
		if !w.fs.injectBitrot(w.buildFile) {
			return fmt.Errorf("oracle: failed to inject bitrot into %s", w.buildFile)
		}
		if err := w.restartServer(true); err != nil {
			return err
		}
		if _, c, _ := queryStatus(); c != http.StatusInternalServerError {
			return fmt.Errorf("oracle: bitrot build answered HTTP %d; want 500 (CRC ignored)", c)
		}
	}
	if w.faults.checkRetentionEvict {
		w.serverOffset += 3600_000 + 1000
		if _, c, _ := queryStatus(); c != http.StatusNotFound {
			return fmt.Errorf("oracle: expired build status=%d; want 404", c)
		}
		w.fs.crash()
		if w.fs.durableDir[w.buildFile] {
			return fmt.Errorf("oracle: evicted build file %s still durable on disk", w.buildFile)
		}
		wr, _ := queueapi.EncodeInit(queue.InitCommand{
			BuildID:       w.buildID,
			RunnerID:      "reinit-r",
			Config:        w.cfg,
			Tests:         w.tests,
			PredictionsMS: w.preds,
		})
		if c, _ := w.invokeHandler(wr, simWriteTok); c != http.StatusOK {
			return fmt.Errorf("oracle: re-init after retention eviction got HTTP %d; want 200", c)
		}
	}
	return nil
}
