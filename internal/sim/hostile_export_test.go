package sim_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/timing"
)

// TestExportHostileCommands exports seeded traces of hostile and boundary
// commands executed directly against queue.State when BUNPO_SIM_HOSTILE_DIR is
// set. The TypeScript differential suite replays the exact same JSON files and
// asserts byte-for-byte identical status, error codes, responses, and events.
func TestExportHostileCommands(t *testing.T) {
	dir := os.Getenv("BUNPO_SIM_HOSTILE_DIR")
	if dir == "" {
		t.Skip("BUNPO_SIM_HOSTILE_DIR not set")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	const numSeeds = 60
	for i := range numSeeds {
		seed := int64(10_000 + i)
		tr := generateHostileSeed(t, seed, i)
		data, err := json.Marshal(tr, json.Deterministic(true))
		if err != nil {
			t.Fatalf("marshal hostile seed %d: %v", seed, err)
		}
		if err := os.WriteFile(filepath.Join(dir, tr.Name+".json"), data, 0o644); err != nil {
			t.Fatalf("write hostile seed %d: %v", seed, err)
		}
	}
}

type hostileRunner struct {
	t     *testing.T
	st    *queue.State
	steps []exportedStep
}

func (h *hostileRunner) step(nowMS int64, cmd any, events []queue.Event, resp any, err error) {
	h.t.Helper()
	cmdJSON, mErr := json.Marshal(cmd, json.Deterministic(true))
	if mErr != nil {
		h.t.Fatalf("marshal command: %v", mErr)
	}
	status := 200
	errCode := ""
	var respJSON jsontext.Value
	if err != nil {
		var qe *queue.Error
		if !errors.As(err, &qe) {
			h.t.Fatalf("non-queue error: %v", err)
		}
		status = qe.HTTPStatus
		errCode = qe.Code
	} else {
		respJSON, mErr = json.Marshal(resp, json.Deterministic(true))
		if mErr != nil {
			h.t.Fatalf("marshal response: %v", mErr)
		}
	}
	evs := events
	if evs == nil {
		evs = []queue.Event{}
	}
	for _, ev := range evs {
		if aErr := h.st.Apply(ev); aErr != nil {
			h.t.Fatalf("apply %s: %v", ev.Type, aErr)
		}
	}
	if status == 200 {
		switch r := resp.(type) {
		case queue.HeartbeatResponse:
			if r.Held {
				if hc, ok := cmd.(struct {
					Type string `json:"type"`
					queue.HeartbeatCommand
				}); ok {
					h.st.ExtendLease(hc.LeaseID, r.DeadlineMS)
				}
			}
		case queue.LeaseResponse:
			if r.Status == queue.LeaseGranted {
				h.st.ExtendLease(r.LeaseID, r.DeadlineMS)
			}
		}
	}
	h.steps = append(h.steps, exportedStep{
		Kind:            "command",
		NowMS:           nowMS,
		Command:         cmdJSON,
		ExpectStatus:    status,
		ExpectErrorCode: errCode,
		ExpectResponse:  respJSON,
		ExpectEvents:    evs,
	})
}

func (h *hostileRunner) initCmd(nowMS int64, c queue.InitCommand) {
	evs, resp, err := queue.DecideInit(h.st, nowMS, c)
	h.step(nowMS, struct {
		Type string `json:"type"`
		queue.InitCommand
	}{Type: "init", InitCommand: c}, evs, resp, err)
}

func (h *hostileRunner) leaseCmd(nowMS int64, c queue.LeaseCommand) queue.LeaseResponse {
	evs, resp, err := queue.DecideLease(h.st, nowMS, c)
	h.step(nowMS, struct {
		Type string `json:"type"`
		queue.LeaseCommand
	}{Type: "lease", LeaseCommand: c}, evs, resp, err)
	return resp
}

func (h *hostileRunner) hbCmd(nowMS int64, c queue.HeartbeatCommand) {
	evs, resp, err := queue.DecideHeartbeat(h.st, nowMS, c)
	h.step(nowMS, struct {
		Type string `json:"type"`
		queue.HeartbeatCommand
	}{Type: "heartbeat", HeartbeatCommand: c}, evs, resp, err)
}

func (h *hostileRunner) reportCmd(nowMS int64, c queue.ReportCommand) {
	evs, resp, err := queue.DecideReport(h.st, nowMS, c)
	h.step(nowMS, struct {
		Type string `json:"type"`
		queue.ReportCommand
	}{Type: "report", ReportCommand: c}, evs, resp, err)
}

func (h *hostileRunner) statusCmd(nowMS int64) {
	evs, resp, err := queue.DecideStatus(h.st, nowMS)
	h.step(nowMS, struct {
		Type string `json:"type"`
	}{Type: "status"}, evs, resp, err)
}

func (h *hostileRunner) listTestsCmd(nowMS int64, c queue.ListTestsCommand) {
	evs, resp, err := queue.DecideListTests(h.st, nowMS, c)
	h.step(nowMS, struct {
		Type string `json:"type"`
		queue.ListTestsCommand
	}{Type: "list_tests", ListTestsCommand: c}, evs, resp, err)
}

func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }

func generateHostileSeed(t *testing.T, seed int64, variant int) exportedSeed {
	t.Helper()
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0xa5a5a5a55a5a5a5a))
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	if variant%5 == 4 {
		// Small runner cap so we deterministically hit 429 too_many_runners.
		lim.MaxRunners = 3
	}
	if variant%5 == 3 {
		lim.MaxTests = 35
	}

	h := &hostileRunner{t: t, st: queue.NewState(lim)}
	now := int64(1_000_000)
	if variant == 0 {
		// Exact lower-bound timestamp now_ms == 0.
		now = 0
	}
	bid := fmt.Sprintf("hostile-%d", seed)

	// 1. Pre-init reads and writes must all answer 404 build_not_found (or 400 on bad args).
	h.statusCmd(now)
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: 10})
	h.leaseCmd(now, queue.LeaseCommand{RunnerID: "r0", MachineIndex: intPtr(0), RequestID: "q0"})
	h.hbCmd(now, queue.HeartbeatCommand{RunnerID: "r0", LeaseID: 1})
	h.reportCmd(now, queue.ReportCommand{RunnerID: "r0", LeaseID: 1, Results: nil})

	goodCfg := queue.Config{
		Machines:       2,
		LeaseTTLMS:     1000,
		BuildTimeoutMS: 20_000,
		MaxAttempts:    2,
		MaxBatch:       4,
	}

	// 2. Hostile invalid init inputs at and past every bound.
	badInits := []struct {
		at  int64
		cmd queue.InitCommand
	}{
		{-1, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: "", RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: "-bad", RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: "a..b", RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: strings.Repeat("a", 129), RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "bad/id", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: strings.Repeat("r", 65), Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", MachineIndex: intPtr(-1), Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", MachineIndex: intPtr(queue.MaxMachines), Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 0, LeaseTTLMS: 1000, BuildTimeoutMS: 2000, MaxAttempts: 1, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: queue.MaxMachines + 1, LeaseTTLMS: 1000, BuildTimeoutMS: 2000, MaxAttempts: 1, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: lim.MinLeaseTTLMS - 1, BuildTimeoutMS: 2000, MaxAttempts: 1, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: queue.MaxLeaseTTLMS + 1, BuildTimeoutMS: queue.MaxLeaseTTLMS + 1, MaxAttempts: 1, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: 1999, MaxAttempts: 1, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: queue.MaxBuildTimeoutMS + 1, MaxAttempts: 1, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: 2000, MaxAttempts: 0, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: 2000, MaxAttempts: queue.MaxAttemptsLimit + 1, MaxBatch: 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: 2000, MaxAttempts: 1, MaxBatch: 0}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: queue.Config{Machines: 1, LeaseTTLMS: 2000, BuildTimeoutMS: 2000, MaxAttempts: 1, MaxBatch: queue.MaxBatchLimit + 1}, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{}, PredictionsMS: []*int64{}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py", "b.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"b.py", "a.py"}, PredictionsMS: []*int64{nil, nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py", "a.py"}, PredictionsMS: []*int64{nil, nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"./a.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"a/../b.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"/abs.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"C:/win.py"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{`a\b.py`}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"a\nb.py"}, PredictionsMS: []*int64{nil}}},
		// Non-ASCII UTF-8 byte length boundary: 1,024 4-byte emoji = 4,096 bytes (ok), +1 byte = 4,097 (rejected).
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{strings.Repeat("😀", 1024) + "a"}, PredictionsMS: []*int64{nil}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{int64Ptr(-1)}}},
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{int64Ptr(timing.MaxDurationMS + 1)}}},
		// UTF-8 vs UTF-16 ordering trap: U+FF01 (3 UTF-8 bytes 0xEF 0xBC 0x81) MUST sort BEFORE U+1F600 (4 UTF-8 bytes 0xF0 0x9F 0x98 0x80).
		{now, queue.InitCommand{BuildID: bid, RunnerID: "r0", Config: goodCfg, Tests: []string{"t/\U0001F600.py", "t/\uFF01.py"}, PredictionsMS: []*int64{nil, nil}}},
	}
	for _, b := range badInits {
		h.initCmd(b.at, b.cmd)
	}

	// 3. Real build with > 20 tests (to exercise top-20 slowest cap, > 50 problems on some variants,
	// non-ASCII + supplementary Unicode paths in exact UTF-8 byte order, and exact 4096-byte test path).
	numTests := 24 + rng.IntN(6)
	if variant%5 == 2 {
		numTests = 55 // exercises MaxStatusProblems = 50 cap!
	} else if variant%5 == 3 {
		numTests = lim.MaxTests
	}
	tests := make([]string, 0, numTests)
	tests = append(tests,
		"t/日本語/alpha.py",
		"t/\uE000_pua.py",
		"t/\uFF01_fullwidth.py",
		"t/\U0001F600_emoji.py",
		"t/"+strings.Repeat("é", 2046)+".py", // 2 + 4092 + 3 = 4097? Wait: 2045*2 = 4090 + 5 = 4095 bytes (valid!)
	)
	tests[4] = "t/" + strings.Repeat("é", 2045) + ".py"
	for len(tests) < numTests {
		tests = append(tests, fmt.Sprintf("tests/file_%03d.py", len(tests)))
	}
	slices.Sort(tests)
	preds := make([]*int64, len(tests))
	for i := range preds {
		if rng.IntN(4) != 0 {
			v := int64(rng.IntN(3000))
			if i == 0 && variant%5 == 0 {
				v = timing.MaxDurationMS // exact upper bound
			}
			preds[i] = &v
		}
	}

	initCfg := goodCfg
	if variant%5 == 1 {
		// Finish cleanly with skipped tests (no conflicts, no failures) to cover counts.skipped++ and verdict passed!
		initCfg.MaxBatch = 6
		initCfg.MaxAttempts = 1
	}
	if variant%5 == 3 {
		// Exact maxTests boundary rejected before valid init.
		overTests := make([]string, lim.MaxTests+1)
		overPreds := make([]*int64, lim.MaxTests+1)
		for i := range overTests {
			overTests[i] = fmt.Sprintf("z/%04d.py", i)
		}
		h.initCmd(now, queue.InitCommand{BuildID: bid, RunnerID: "r0", MachineIndex: intPtr(0), Config: initCfg, Tests: overTests, PredictionsMS: overPreds})
	}

	h.initCmd(now, queue.InitCommand{
		BuildID:       bid,
		RunnerID:      "r0",
		MachineIndex:  intPtr(queue.MaxMachines - 1), // exact upper bound 255
		Config:        initCfg,
		Tests:         tests,
		PredictionsMS: preds,
	})
	// Wrong build_id against initialized state.
	h.initCmd(now, queue.InitCommand{
		BuildID:       bid + "-other",
		RunnerID:      "r0",
		Config:        initCfg,
		Tests:         tests,
		PredictionsMS: preds,
	})
	// Idempotent matching init.
	h.initCmd(now+1, queue.InitCommand{
		BuildID:       bid,
		RunnerID:      "r1",
		Config:        initCfg,
		Tests:         tests,
		PredictionsMS: make([]*int64, len(tests)),
	})

	// 4. Hostile lease/heartbeat/report invalid inputs on initialized build.
	h.leaseCmd(now+2, queue.LeaseCommand{RunnerID: "bad/id", RequestID: "q1"})
	h.leaseCmd(now+2, queue.LeaseCommand{RunnerID: "r0", RequestID: ""})
	h.leaseCmd(now+2, queue.LeaseCommand{RunnerID: "r0", RequestID: strings.Repeat("q", 129)})
	h.leaseCmd(now+2, queue.LeaseCommand{RunnerID: "r0", MachineIndex: intPtr(queue.MaxMachines), RequestID: "q1"})
	h.hbCmd(now+2, queue.HeartbeatCommand{RunnerID: "bad/id", LeaseID: 1})
	h.hbCmd(now+2, queue.HeartbeatCommand{RunnerID: "r0", LeaseID: 0})
	h.hbCmd(now+2, queue.HeartbeatCommand{RunnerID: "r0", LeaseID: 999})
	h.reportCmd(now+2, queue.ReportCommand{RunnerID: "bad/id", LeaseID: 1})
	h.reportCmd(now+2, queue.ReportCommand{RunnerID: "r0", LeaseID: 0})
	h.reportCmd(now+2, queue.ReportCommand{RunnerID: "r0", LeaseID: 999})

	// 5. Run through leases, hostile reports, retries, >8 stale/conflict history records, skips, failures, and deadlines.
	now += 10
	l1 := h.leaseCmd(now, queue.LeaseCommand{RunnerID: "r0", MachineIndex: intPtr(0), RequestID: "req-r0-1"})
	if l1.Status == queue.LeaseGranted && len(l1.Attempts) > 0 {
		// Wrong runner on heartbeat and report.
		h.hbCmd(now+5, queue.HeartbeatCommand{RunnerID: "r1", LeaseID: l1.LeaseID})
		h.reportCmd(now+5, queue.ReportCommand{RunnerID: "r1", LeaseID: l1.LeaseID, Results: nil})
		// Wrong results count.
		h.reportCmd(now+5, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: nil})

		a0 := l1.Attempts[0]
		makeBase := func() []queue.ReportResult {
			out := make([]queue.ReportResult, len(l1.Attempts))
			for i, a := range l1.Attempts {
				out[i] = queue.ReportResult{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: a.Generation, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 100 + int64(i)}
			}
			return out
		}
		// Out-of-range test_index, out-of-range generation, mismatched attempt_id, invalid outcome/timed/duration, duplicate test_index, wrong test_index.
		bad1 := makeBase()
		bad1[0].TestIndex = -1
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad1})
		bad2 := makeBase()
		bad2[0].TestIndex = len(tests)
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad2})
		bad3 := makeBase()
		bad3[0].Generation = 0
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad3})
		bad4 := makeBase()
		bad4[0].Generation = a0.Generation + 1
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad4})
		bad5 := makeBase()
		bad5[0].AttemptID = "999.9"
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad5})
		bad6 := makeBase()
		bad6[0].Outcome = "bogus"
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad6})
		bad7 := makeBase()
		bad7[0].Timed = false
		bad7[0].DurationMS = 5
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad7})
		bad8 := makeBase()
		bad8[0].Outcome = queue.OutcomeFailed
		bad8[0].Timed = true
		bad8[0].DurationMS = 5
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad8})
		bad9 := makeBase()
		bad9[0].DurationMS = -1
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad9})
		bad10 := makeBase()
		bad10[0].DurationMS = timing.MaxDurationMS + 1
		h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: bad10})
		if len(l1.Attempts) >= 2 {
			badDup := makeBase()
			badDup[1] = badDup[0]
			h.reportCmd(now+6, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: badDup})
		}

		// Accept l1 with a mix of passed, skipped, error (unless variant%5==1 -> passed + skipped only), or failed.
		res := makeBase()
		for i := range res {
			switch {
			case variant%5 == 1 && i%2 == 0:
				res[i].Outcome = queue.OutcomeSkipped
				res[i].Timed = false
				res[i].DurationMS = 0
			case variant%5 == 0 && i == 0:
				res[i].Outcome = queue.OutcomeError
				res[i].Timed = false
				res[i].DurationMS = 0
			case variant%5 == 2:
				res[i].Outcome = queue.OutcomeFailed
				res[i].Timed = false
				res[i].DurationMS = 0
			default:
				res[i].Outcome = queue.OutcomePassed
				res[i].Timed = true
				res[i].DurationMS = int64(50 + rng.IntN(5000))
			}
		}
		h.reportCmd(now+20, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: res})
		// Duplicate report -> 0 events.
		h.reportCmd(now+21, queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: res})

		// Flood > 8 distinct conflicting reports on l1 to trigger MaxTestHistory=8 & history_truncated + dedupe!
		if variant%5 == 0 {
			for k := 1; k <= 10; k++ {
				conf := makeBase()
				for i := range conf {
					conf[i].Outcome = queue.OutcomePassed
					conf[i].Timed = true
					conf[i].DurationMS = int64(1000 + k*7)
				}
				h.reportCmd(now+22+int64(k), queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: conf})
				// Repeat exact same conflict to hit deduplication path.
				if k == 2 || k == 9 {
					h.reportCmd(now+22+int64(k), queue.ReportCommand{RunnerID: "r0", LeaseID: l1.LeaseID, Results: conf})
				}
			}
		}
	}

	// 5b. Exercise > 8 distinct STALE reports (after lease expiry) on variant%5 == 1.
	if variant%5 == 1 {
		now += 50
		lStale := h.leaseCmd(now, queue.LeaseCommand{RunnerID: "r-stale", MachineIndex: intPtr(1), RequestID: "req-stale-1"})
		if lStale.Status == queue.LeaseGranted && len(lStale.Attempts) > 0 {
			now += initCfg.LeaseTTLMS + 50
			for k := 1; k <= 10; k++ {
				staleRes := make([]queue.ReportResult, len(lStale.Attempts))
				for i, a := range lStale.Attempts {
					staleRes[i] = queue.ReportResult{
						AttemptID:  a.AttemptID,
						TestIndex:  a.TestIndex,
						Generation: a.Generation,
						Outcome:    queue.OutcomePassed,
						Timed:      true,
						DurationMS: int64(200 + k*11),
					}
				}
				h.reportCmd(now+int64(k), queue.ReportCommand{RunnerID: "r-stale", LeaseID: lStale.LeaseID, Results: staleRes})
				if k == 2 || k == 9 {
					h.reportCmd(now+int64(k), queue.ReportCommand{RunnerID: "r-stale", LeaseID: lStale.LeaseID, Results: staleRes})
				}
			}
		}
	}

	// 6. Exercise too_many_runners on variant%5 == 4.
	if variant%5 == 4 {
		for r := 1; r <= 4; r++ {
			now += 5
			h.leaseCmd(now, queue.LeaseCommand{RunnerID: fmt.Sprintf("r%d", r), RequestID: fmt.Sprintf("req-r%d-1", r)})
		}
	}

	// 7. Drain remaining tests OR stop before deadline so we test finalizing overlay on status & list_tests!
	stopAtDeadline := variant%5 == 3 || variant%5 == 4
	reqCounter := 2
	for step := 0; step < 80; step++ {
		now += 15
		if stopAtDeadline && step == 4 {
			// Lease one more batch and leave it ACTIVE (unreported) so reads at/after
			// deadline exercise the leased -> unresolved + attempt expired overlay!
			h.leaseCmd(now, queue.LeaseCommand{
				RunnerID:     "r0",
				MachineIndex: intPtr(0),
				RequestID:    fmt.Sprintf("req-open-deadline-%d", reqCounter),
			})
			break
		}
		l := h.leaseCmd(now, queue.LeaseCommand{
			RunnerID:     "r0",
			MachineIndex: intPtr(0),
			RequestID:    fmt.Sprintf("req-loop-%d", reqCounter),
		})
		reqCounter++
		if l.Status != queue.LeaseGranted {
			break
		}
		res := make([]queue.ReportResult, len(l.Attempts))
		for i, a := range l.Attempts {
			if variant%5 == 1 && (a.TestIndex%3 == 0) {
				res[i] = queue.ReportResult{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: a.Generation, Outcome: queue.OutcomeSkipped, Timed: false, DurationMS: 0}
			} else if variant%5 == 2 {
				res[i] = queue.ReportResult{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: a.Generation, Outcome: queue.OutcomeFailed, Timed: false, DurationMS: 0}
			} else {
				// Tied durations on every 5th test to exercise tie-breaking in top-20 slowest,
				// plus exact MaxDurationMS upper bound on step 0.
				dur := int64(500 + (a.TestIndex%5)*100)
				if step == 0 && i == 0 {
					dur = timing.MaxDurationMS
				} else if a.TestIndex%2 == 0 {
					dur = int64(1000 + rng.IntN(2000))
				}
				res[i] = queue.ReportResult{AttemptID: a.AttemptID, TestIndex: a.TestIndex, Generation: a.Generation, Outcome: queue.OutcomePassed, Timed: true, DurationMS: dur}
			}
		}
		now += 10
		h.reportCmd(now, queue.ReportCommand{RunnerID: "r0", LeaseID: l.LeaseID, Results: res})
	}

	// 8. ListTests invalid inputs + every filter + retried + pagination.
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: -1, Limit: 10})
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: -1})
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: queue.MaxTestsPageLimit + 1})
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: 10, States: []queue.TestStatus{"bogus"}})
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: 0}) // default 200
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: 5})
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: 100, States: []queue.TestStatus{queue.StatusPending, queue.StatusLeased, queue.StatusPassed, queue.StatusFailed, queue.StatusSkipped, queue.StatusUnresolved}})
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: 100, States: []queue.TestStatus{queue.StatusSkipped}})
	h.listTestsCmd(now, queue.ListTestsCommand{Cursor: 0, Limit: 100, Retried: true})

	// 9. Reads AT and AFTER deadline to exercise finalizing overlay in DecideStatus and DecideListTests!
	if stopAtDeadline {
		deadline := h.st.DeadlineMS
		if variant%5 == 3 {
			// Exact deadline on ListTests (exercises leased -> unresolved + attempt marked expired in overlay!)
			h.listTestsCmd(deadline, queue.ListTestsCommand{Cursor: 0, Limit: 100})
		} else {
			// Past deadline on Status (exercises verdictChanged overlay!)
			h.statusCmd(deadline + 500)
		}
	}

	// 10. Flood > 8 init conflicts (including config diff, test length diff, test path diff, and duplicate conflicts)
	// on variant%5 == 0 (after finish, flipping passed -> unresolved and filling MaxInitConflicts = 8!).
	if variant%5 == 0 {
		badCfg := initCfg
		badCfg.MaxBatch = 1
		h.initCmd(now+100, queue.InitCommand{BuildID: bid, RunnerID: "conf-cfg", Config: badCfg, Tests: tests, PredictionsMS: preds})
		// Duplicate conflict -> 0 events.
		h.initCmd(now+101, queue.InitCommand{BuildID: bid, RunnerID: "conf-cfg", Config: badCfg, Tests: tests, PredictionsMS: preds})
		// Length diff conflict.
		h.initCmd(now+102, queue.InitCommand{BuildID: bid, RunnerID: "conf-len", Config: initCfg, Tests: tests[:1], PredictionsMS: preds[:1]})
		// Same-length path diff conflicts until > 8!
		for k := 0; k < 9; k++ {
			modTests := slices.Clone(tests)
			modTests[len(modTests)-1] = fmt.Sprintf("zzz/conflict_%02d.py", k)
			h.initCmd(now+103+int64(k), queue.InitCommand{BuildID: bid, RunnerID: fmt.Sprintf("conf-%d", k), MachineIndex: intPtr(k), Config: initCfg, Tests: modTests, PredictionsMS: preds})
		}
	}

	h.statusCmd(now + 200)
	h.listTestsCmd(now+200, queue.ListTestsCommand{Cursor: 0, Limit: 100})
	h.listTestsCmd(now+200, queue.ListTestsCommand{Cursor: 0, Limit: 100, Retried: true})

	return exportedSeed{
		Seed:          seed,
		Name:          fmt.Sprintf("hostile-%05d", seed),
		MinLeaseTTLMS: lim.MinLeaseTTLMS,
		MaxTests:      lim.MaxTests,
		MaxRunners:    lim.MaxRunners,
		Steps:         h.steps,
	}
}
