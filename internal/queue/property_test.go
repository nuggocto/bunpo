package queue_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
)

// refTerminal is an independent observation-only model of each test's accepted
// terminal result and the build's verdict constraints. It never duplicates
// internal/queue's scheduling or expiry logic; it records only what Decide*
// externally reported as accepted or conflicted.
type refTerminal struct {
	outcome    queue.Outcome
	timed      bool
	durationMS int64
}

type refModel struct {
	nTests          int
	maxAttempts     int
	accepted        map[int]refTerminal
	maxGenSeen      []int
	sawInitConflict bool
	finishedVerdict queue.Verdict
}

func newRefModel(nTests, maxAttempts int) *refModel {
	return &refModel{
		nTests:      nTests,
		maxAttempts: maxAttempts,
		accepted:    make(map[int]refTerminal, nTests),
		maxGenSeen:  make([]int, nTests),
	}
}

func (m *refModel) observeReport(t *testing.T, cmd queue.ReportCommand, resp queue.ReportResponse) {
	t.Helper()
	byIdx := make(map[int]queue.ReportResult, len(cmd.Results))
	for _, r := range cmd.Results {
		byIdx[r.TestIndex] = r
	}
	for _, d := range resp.Results {
		if d.Disposition != queue.DispositionAccepted {
			continue
		}
		r := byIdx[d.TestIndex]
		if r.Outcome == queue.OutcomeError {
			continue
		}
		if prev, exists := m.accepted[d.TestIndex]; exists {
			t.Fatalf("test %d accepted a second terminal result %+v after %+v", d.TestIndex, r, prev)
		}
		m.accepted[d.TestIndex] = refTerminal{
			outcome:    r.Outcome,
			timed:      r.Timed,
			durationMS: r.DurationMS,
		}
	}
}

func (m *refModel) check(t *testing.T, s *queue.State, eventCount int) {
	t.Helper()

	// 1. Check Counts match full recount, generations never decrease, and
	// accepted terminal outcomes match the independent model.
	var recount queue.Counts
	recount.Total = len(s.Tests)
	activeTestOwner := make(map[int]int64)
	for i, ts := range s.Tests {
		if ts.Generation < m.maxGenSeen[i] {
			t.Fatalf("test %d generation decreased from %d to %d", i, m.maxGenSeen[i], ts.Generation)
		}
		if ts.Generation > m.maxAttempts {
			t.Fatalf("test %d generation %d exceeds max_attempts %d", i, ts.Generation, m.maxAttempts)
		}
		m.maxGenSeen[i] = ts.Generation

		switch ts.Status {
		case queue.StatusPending:
			recount.Pending++
		case queue.StatusLeased:
			recount.Leased++
			activeTestOwner[i] = ts.ActiveLeaseID
		case queue.StatusPassed:
			recount.Passed++
		case queue.StatusFailed:
			recount.Failed++
		case queue.StatusSkipped:
			recount.Skipped++
		case queue.StatusUnresolved:
			recount.Unresolved++
		}

		if want, ok := m.accepted[i]; ok {
			switch want.outcome {
			case queue.OutcomePassed:
				if ts.Status != queue.StatusPassed || ts.Timed != want.timed || ts.DurationMS != want.durationMS {
					t.Fatalf("test %d state = (%s, %t, %d); model accepted %+v", i, ts.Status, ts.Timed, ts.DurationMS, want)
				}
			case queue.OutcomeFailed:
				if ts.Status != queue.StatusFailed {
					t.Fatalf("test %d state = %s; model accepted failed", i, ts.Status)
				}
			case queue.OutcomeSkipped:
				if ts.Status != queue.StatusSkipped {
					t.Fatalf("test %d state = %s; model accepted skipped", i, ts.Status)
				}
			}
		} else if ts.Status == queue.StatusPassed || ts.Status == queue.StatusFailed || ts.Status == queue.StatusSkipped {
			t.Fatalf("test %d reached %s without an accepted report in reference model", i, ts.Status)
		}
	}
	if s.Counts != recount {
		t.Fatalf("Counts = %+v; recount = %+v", s.Counts, recount)
	}

	// 2. A test is in at most one active lease, and at most one active lease per runner.
	seenTestInActive := make(map[int]int64)
	seenRunnerActive := make(map[string]int64)
	for i, lid := range s.ActiveLeases {
		if i > 0 && s.ActiveLeases[i-1] >= lid {
			t.Fatalf("ActiveLeases not strictly ascending: %v", s.ActiveLeases)
		}
		l := s.Leases[lid-1]
		if l.Status != queue.LeaseActive {
			t.Fatalf("ActiveLeases contains non-active lease %d (%s)", lid, l.Status)
		}
		if prev, dup := seenRunnerActive[l.RunnerID]; dup {
			t.Fatalf("runner %q holds two active leases: %d and %d", l.RunnerID, prev, lid)
		}
		seenRunnerActive[l.RunnerID] = lid
		for _, a := range l.Attempts {
			if prev, dup := seenTestInActive[a.TestIndex]; dup {
				t.Fatalf("test %d appears in two active leases: %d and %d", a.TestIndex, prev, lid)
			}
			seenTestInActive[a.TestIndex] = lid
			if activeTestOwner[a.TestIndex] != lid {
				t.Fatalf("active lease %d holds test %d, but test points to lease %d", lid, a.TestIndex, activeTestOwner[a.TestIndex])
			}
		}
	}
	if len(seenTestInActive) != len(activeTestOwner) {
		t.Fatalf("leased test count %d != tests in active leases %d", len(activeTestOwner), len(seenTestInActive))
	}

	// 3. Verdict invariants:
	//    - passed implies every test accepted as passed or skipped and no init conflict.
	//    - a finished verdict may change ONLY passed -> unresolved due to an init conflict.
	v := s.Verdict()
	if v == queue.VerdictPassed {
		if m.sawInitConflict || len(s.Conflicts) > 0 {
			t.Fatalf("verdict is passed despite init conflict")
		}
		if len(m.accepted) != m.nTests {
			t.Fatalf("verdict is passed with only %d/%d accepted tests", len(m.accepted), m.nTests)
		}
		for i := range m.nTests {
			acc := m.accepted[i]
			if acc.outcome != queue.OutcomePassed && acc.outcome != queue.OutcomeSkipped {
				t.Fatalf("verdict is passed but test %d accepted %s", i, acc.outcome)
			}
		}
	}
	if m.finishedVerdict != "" && v != m.finishedVerdict {
		allowed := m.finishedVerdict == queue.VerdictPassed && v == queue.VerdictUnresolved && m.sawInitConflict
		if !allowed {
			t.Fatalf("finished verdict changed illegally from %s to %s (conflict=%t)", m.finishedVerdict, v, m.sawInitConflict)
		}
		m.finishedVerdict = v
	} else if m.finishedVerdict == "" && v != queue.VerdictRunning {
		m.finishedVerdict = v
	}

	// 4. Event log count upper bound (even under hostile repeated requests):
	//    1 initialized + MaxInitConflicts + 1 finalized
	//    + nTests * (3*maxAttempts + MaxTestHistory)
	//    (at most nTests*maxAttempts leases, expiries, and accepted reports, plus MaxTestHistory stale/conflict entries per test).
	maxEvents := 1 + queue.MaxInitConflicts + 1 + m.nTests*(3*m.maxAttempts+queue.MaxTestHistory)
	if eventCount > maxEvents {
		t.Fatalf("event count %d exceeded hard upper bound %d (tests=%d max_attempts=%d)",
			eventCount, maxEvents, m.nTests, m.maxAttempts)
	}
}

func TestPropertyRandomSequences(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261123, 42))

	for trial := range 200 {
		nTests := 1 + rng.IntN(12)
		maxAttempts := 1 + rng.IntN(4)
		machines := 1 + rng.IntN(4)
		maxBatch := 1 + rng.IntN(6)
		ttl := int64(1000 + rng.IntN(4000))
		timeout := ttl * int64(3+rng.IntN(8))

		lim := queue.DefaultLimits()
		lim.MinLeaseTTLMS = 1000
		st := queue.NewState(lim)
		model := newRefModel(nTests, maxAttempts)
		var eventLog []queue.Event

		tests := make([]string, nTests)
		preds := make([]*int64, nTests)
		for i := range nTests {
			tests[i] = fmt.Sprintf("tests/t%02d.py", i)
			switch rng.IntN(3) {
			case 0:
				preds[i] = nil
			case 1:
				preds[i] = int64Ptr(0)
			case 2:
				preds[i] = int64Ptr(int64(1 + rng.IntN(5000)))
			}
		}

		cfg := queue.Config{
			Machines:       machines,
			LeaseTTLMS:     ttl,
			BuildTimeoutMS: timeout,
			MaxAttempts:    maxAttempts,
			MaxBatch:       maxBatch,
		}

		now := int64(1000)
		initCmd := queue.InitCommand{
			BuildID:       fmt.Sprintf("prop-%d", trial),
			RunnerID:      "r0",
			MachineIndex:  intPtr(0),
			Config:        cfg,
			Tests:         tests,
			PredictionsMS: preds,
		}
		before := stateHash(t, st)
		evs, _, err := queue.DecideInit(st, now, initCmd)
		if err != nil {
			t.Fatalf("trial %d init: %v", trial, err)
		}
		if stateHash(t, st) != before {
			t.Fatalf("trial %d: DecideInit mutated state", trial)
		}
		for _, ev := range evs {
			mustApply(t, st, []queue.Event{ev})
			eventLog = append(eventLog, ev)
		}

		type knownLease struct {
			runnerID string
			leaseID  int64
			reqID    string
			attempts []queue.LeasedAttemptView
		}
		var knownLeases []knownLease
		nRunners := 2 + rng.IntN(5)
		reqSeq := 0

		steps := 80 + rng.IntN(80)
		for step := range steps {
			// Advance, hold, or occasionally step backwards to test clock clamping.
			switch rng.IntN(10) {
			case 0:
				now = max(0, now-int64(rng.IntN(2000)))
			case 1:
				now += ttl + int64(rng.IntN(2000))
			default:
				now += int64(rng.IntN(int(ttl / 2)))
			}

			runner := fmt.Sprintf("r%d", rng.IntN(nRunners))
			op := rng.IntN(100)
			// Check Decide non-mutation on every step of the first 5 trials and
			// every 20th step thereafter so -race stays comfortably under 5 s.
			checkNoMut := trial < 5 || step%20 == 0
			var before [32]byte
			if checkNoMut {
				before = stateHash(t, st)
			}

			switch {
			case op < 5: // Simulated restart
				st.ResetActiveLeaseDeadlines(now)

			case op < 12: // Hostile or matching init (including after finished!)
				cmd := initCmd
				cmd.RunnerID = fmt.Sprintf("init-%d", rng.IntN(12))
				if rng.IntN(2) == 0 {
					cmd.Config.MaxAttempts = (cfg.MaxAttempts % queue.MaxAttemptsLimit) + 1
				}
				evs, _, err := queue.DecideInit(st, now, cmd)
				if checkNoMut && stateHash(t, st) != before {
					t.Fatalf("trial %d step %d: DecideInit mutated state", trial, step)
				}
				if err != nil {
					model.sawInitConflict = true
				}
				for _, ev := range evs {
					mustApply(t, st, []queue.Event{ev})
					eventLog = append(eventLog, ev)
				}

			case op < 42: // Lease (fresh or repeated request_id)
				reqSeq++
				reqID := fmt.Sprintf("req-%d", reqSeq)
				if len(knownLeases) > 0 && rng.IntN(4) == 0 {
					kl := knownLeases[rng.IntN(len(knownLeases))]
					runner = kl.runnerID
					reqID = kl.reqID
				}
				evs, resp, err := queue.DecideLease(st, now, queue.LeaseCommand{
					RunnerID:     runner,
					MachineIndex: intPtr(rng.IntN(machines)),
					RequestID:    reqID,
				})
				if checkNoMut && stateHash(t, st) != before {
					t.Fatalf("trial %d step %d: DecideLease mutated state", trial, step)
				}
				for _, ev := range evs {
					mustApply(t, st, []queue.Event{ev})
					eventLog = append(eventLog, ev)
				}
				if err == nil && resp.Status == queue.LeaseGranted {
					st.ExtendLease(resp.LeaseID, resp.DeadlineMS)
					knownLeases = append(knownLeases, knownLease{
						runnerID: runner,
						leaseID:  resp.LeaseID,
						reqID:    reqID,
						attempts: resp.Attempts,
					})
				}

			case op < 55 && len(knownLeases) > 0: // Heartbeat
				kl := knownLeases[rng.IntN(len(knownLeases))]
				evs, resp, err := queue.DecideHeartbeat(st, now, queue.HeartbeatCommand{
					RunnerID: kl.runnerID,
					LeaseID:  kl.leaseID,
				})
				if checkNoMut && stateHash(t, st) != before {
					t.Fatalf("trial %d step %d: DecideHeartbeat mutated state", trial, step)
				}
				if err != nil {
					t.Fatalf("trial %d step %d: unexpected heartbeat error %v", trial, step, err)
				}
				for _, ev := range evs {
					mustApply(t, st, []queue.Event{ev})
					eventLog = append(eventLog, ev)
				}
				if resp.Held {
					st.ExtendLease(kl.leaseID, resp.DeadlineMS)
				}

			case op < 88 && len(knownLeases) > 0: // Report (fresh, duplicate, stale, or conflicting)
				kl := knownLeases[rng.IntN(len(knownLeases))]
				results := make([]queue.ReportResult, len(kl.attempts))
				for i, a := range kl.attempts {
					var out queue.Outcome
					timed := false
					var dur int64
					switch rng.IntN(5) {
					case 0, 1:
						out = queue.OutcomePassed
						timed = true
						dur = int64(rng.IntN(500))
					case 2:
						out = queue.OutcomePassed
						timed = false
					case 3:
						out = queue.OutcomeSkipped
					case 4:
						if rng.IntN(2) == 0 {
							out = queue.OutcomeError
						} else {
							out = queue.OutcomeFailed
						}
					}
					results[i] = queue.ReportResult{
						AttemptID:  a.AttemptID,
						TestIndex:  a.TestIndex,
						Generation: a.Generation,
						Outcome:    out,
						Timed:      timed,
						DurationMS: dur,
					}
				}
				cmd := queue.ReportCommand{
					RunnerID: kl.runnerID,
					LeaseID:  kl.leaseID,
					Results:  results,
				}
				evs, resp, err := queue.DecideReport(st, now, cmd)
				if checkNoMut && stateHash(t, st) != before {
					t.Fatalf("trial %d step %d: DecideReport mutated state", trial, step)
				}
				if err != nil {
					t.Fatalf("trial %d step %d: unexpected report error %v", trial, step, err)
				}
				for _, ev := range evs {
					mustApply(t, st, []queue.Event{ev})
					eventLog = append(eventLog, ev)
				}
				model.observeReport(t, cmd, resp)

			default: // Status or ListTests (may finalize on read)
				if rng.IntN(2) == 0 {
					evs, _, err := queue.DecideStatus(st, now)
					if checkNoMut && stateHash(t, st) != before {
						t.Fatalf("trial %d step %d: DecideStatus mutated state", trial, step)
					}
					if err != nil {
						t.Fatalf("trial %d step %d: DecideStatus error %v", trial, step, err)
					}
					for _, ev := range evs {
						mustApply(t, st, []queue.Event{ev})
						eventLog = append(eventLog, ev)
					}
				} else {
					evs, _, err := queue.DecideListTests(st, now, queue.ListTestsCommand{Cursor: 0, Limit: 5})
					if checkNoMut && stateHash(t, st) != before {
						t.Fatalf("trial %d step %d: DecideListTests mutated state", trial, step)
					}
					if err != nil {
						t.Fatalf("trial %d step %d: DecideListTests error %v", trial, step, err)
					}
					for _, ev := range evs {
						mustApply(t, st, []queue.Event{ev})
						eventLog = append(eventLog, ev)
					}
				}
			}

			model.check(t, st, len(eventLog))
		}

		// Replay the entire event log onto a fresh State and check persistent equality.
		replayed := queue.NewState(lim)
		for i, ev := range eventLog {
			if err := replayed.Apply(ev); err != nil {
				t.Fatalf("trial %d: replay event %d (%s): %v", trial, i, ev.Type, err)
			}
		}
		if !equalPersistent(t, st, replayed) {
			t.Fatalf("trial %d: replayed state differs from live persistent state", trial)
		}
	}
}
