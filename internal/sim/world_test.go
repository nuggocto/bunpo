package sim_test

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/queuerunner"
)

const (
	simWriteTok = "sim-write-token-0123456789abcdef"
	simReadTok  = "sim-read-token-fedcba9876543210"
	traceTail   = 40
)

var statNames = []string{
	"reqDrop", "respDrop", "reqDup", "respDup", "http503", "http429",
	"restarts", "clockJump", "diskUnwritten", "diskUncertain", "kills", "pauses",
	"divInit", "reportOutage", "staleResult", "staleRequest", "leaseHeld",
	"soloRetry", "exhausted", "deadline", "conflictAfterPassed",
	"reportAcrossRestart", "broken", "dupReport", "extraFile", "statusReport",
	"passed", "failed", "unresolved",
}

type simStats map[string]int

func (s simStats) summary() string {
	parts := make([]string, len(statNames))
	for i, k := range statNames {
		parts[i] = fmt.Sprintf("%s=%d", k, s[k])
	}
	return strings.Join(parts, " ")
}

type simEntropy struct{ rng *rand.Rand }

func (e *simEntropy) Hex16() string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], e.rng.Uint64())
	binary.LittleEndian.PutUint64(b[8:], e.rng.Uint64())
	return hex.EncodeToString(b[:])
}
func (e *simEntropy) Int64N(n int64) int64 { return e.rng.Int64N(n) }

type traceRecorder struct {
	h     hash.Hash
	lines [traceTail]string
	count int
}

func (tr *traceRecorder) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	fmt.Fprintln(tr.h, line)
	tr.lines[tr.count%traceTail] = line
	tr.count++
}
func (tr *traceRecorder) hashBytes(tag string, data []byte) { fmt.Fprintf(tr.h, "%s:%s\n", tag, data) }
func (tr *traceRecorder) digest() string                    { return hex.EncodeToString(tr.h.Sum(nil)) }
func (tr *traceRecorder) tail() string {
	var b strings.Builder
	for i := max(0, tr.count-traceTail); i < tr.count; i++ {
		b.WriteString(tr.lines[i%traceTail] + "\n")
	}
	return b.String()
}

type simDisk struct {
	rng                        *rand.Rand
	trace                      *traceRecorder
	stats                      simStats
	faultsActive               *bool
	unwrittenPct, uncertainPct int
	buildOrder                 []string
	durable, unsynced          map[string][][]byte
	broken                     map[string]bool
}

func (d *simDisk) Append(_ context.Context, buildID string, events []queue.Event) error {
	if len(events) == 0 {
		return nil
	}
	write := func() {
		if _, ok := d.durable[buildID]; !ok {
			d.buildOrder = append(d.buildOrder, buildID)
		}
		for _, ev := range events {
			raw, err := queue.EncodeEvent(ev)
			if err != nil {
				panic(err)
			}
			d.durable[buildID] = append(d.durable[buildID], raw)
			d.trace.logf("stored build=%s type=%s at=%d", buildID, ev.Type, ev.AtMS)
			d.trace.hashBytes("event", raw)
		}
		delete(d.unsynced, buildID)
	}
	if *d.faultsActive {
		if d.unwrittenPct > 0 && d.rng.IntN(100) < d.unwrittenPct {
			d.stats["diskUnwritten"]++
			d.trace.logf("fault disk_unwritten build=%s n=%d", buildID, len(events))
			return fmt.Errorf("simulated disk full: %w", queueapi.ErrAppendUnwritten)
		}
		if d.uncertainPct > 0 && d.rng.IntN(100) < d.uncertainPct {
			d.stats["diskUncertain"]++
			d.stats["broken"]++
			d.broken[buildID] = true
			landed := d.rng.IntN(2) == 0
			if landed {
				write()
			}
			d.trace.logf("fault disk_uncertain build=%s n=%d landed=%t", buildID, len(events), landed)
			return errors.New("simulated uncertain fsync error")
		}
		if d.rng.IntN(20) == 0 {
			d.unsynced[buildID] = append(d.unsynced[buildID], []byte(`{"type":"torn`))
		}
	}
	write()
	return nil
}

func (d *simDisk) replayBuild(buildID string, lim queue.Limits) (*queue.State, error) {
	st := queue.NewState(lim)
	for _, raw := range d.durable[buildID] {
		ev, err := queue.DecodeEvent(raw)
		if err != nil {
			return nil, err
		}
		if err := st.Apply(ev); err != nil {
			return nil, err
		}
	}
	return st, nil
}

type faultProfile struct {
	reqDropPct, respDropPct, reqDupPct, respDupPct, http503Pct, http429Pct int
	diskUnwrittenPct, diskUncertainPct, poisonIdx                          int
	netMaxDelayMS                                                          int64
	serverRestart, serverClockJump, clientSkew, clockBackward              bool
	runnerKill, runnerPause, runnerRestartSame, lateJoiner, divergentInit  bool
	conflictAfterPassed, shortDeadline, reportOutage, expectCleanPass      bool
	cmdFail, cmdSkip, cmdCrash, cmdHang, cmdNoReport, cmdOldReport         bool
	cmdTruncated, cmdExtraFile, cmdMixedSibling, cmdSigSibling, useLines   bool
}

func drawRandomProfile(rng *rand.Rand, numTests int) faultProfile {
	pct := func(on bool, base, span int) int {
		if !on {
			return 0
		}
		return base + rng.IntN(span)
	}
	p := faultProfile{
		poisonIdx: -1, netMaxDelayMS: 10 + rng.Int64N(350),
		reqDropPct: pct(rng.IntN(3) != 0, 2, 16), respDropPct: pct(rng.IntN(3) != 0, 2, 16),
		reqDupPct: pct(rng.IntN(2) == 0, 4, 16), respDupPct: pct(rng.IntN(3) != 0, 3, 15),
		http503Pct: pct(rng.IntN(3) == 0, 3, 12), http429Pct: pct(rng.IntN(3) == 0, 3, 10),
		diskUnwrittenPct: pct(rng.IntN(3) == 0, 3, 12), diskUncertainPct: pct(rng.IntN(12) == 0, 3, 8),
		serverRestart: rng.IntN(3) == 0, serverClockJump: rng.IntN(3) == 0, clientSkew: rng.IntN(2) == 0,
		runnerKill: rng.IntN(2) == 0, runnerPause: rng.IntN(2) == 0, runnerRestartSame: rng.IntN(3) == 0,
		lateJoiner: rng.IntN(2) == 0, divergentInit: rng.IntN(4) == 0, conflictAfterPassed: rng.IntN(5) == 0,
		shortDeadline: rng.IntN(10) == 0, reportOutage: rng.IntN(6) == 0,
		cmdFail: rng.IntN(3) == 0, cmdSkip: rng.IntN(2) == 0, cmdCrash: rng.IntN(2) == 0,
		cmdHang: rng.IntN(3) == 0, cmdNoReport: rng.IntN(3) == 0, cmdOldReport: rng.IntN(3) == 0,
		cmdTruncated: rng.IntN(3) == 0, cmdExtraFile: rng.IntN(4) == 0,
		cmdMixedSibling: rng.IntN(2) == 0, cmdSigSibling: rng.IntN(3) == 0, useLines: rng.IntN(4) == 0,
	}
	p.clockBackward = p.serverClockJump && rng.IntN(2) == 0
	if rng.IntN(4) == 0 && numTests > 0 {
		p.poisonIdx = rng.IntN(numTests)
	}
	return p
}

const (
	seRunnerStart, seDeliverReq, seDeliverResp, seTimer = 1, 2, 3, 4
	seCmdFinish, seReadReports, seKill, sePause         = 5, 6, 7, 8
	seResume, seRestart, seClockJump, seLateConflict    = 9, 10, 11, 12
	seStartStatus, seHeal                               = 13, 14
)

type simEvent struct {
	atMS, leaseID              int64
	seq, opID, timerGen        uint64
	kind, runnerIdx, statusIdx int
	respStatus, exitCode       int
	req                        queuerunner.Request
	wire                       queueapi.WireRequest
	token                      string
	respBody                   []byte
	respErr, readErr           error
	timerKind                  queuerunner.TimerKind
	signaled, isDup            bool
	files                      map[string]junit.File
}

type eventHeap []*simEvent

func (h eventHeap) Len() int      { return len(h) }
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*simEvent)) }
func (h eventHeap) Less(i, j int) bool {
	return h[i].atMS < h[j].atMS || (h[i].atMS == h[j].atMS && h[i].seq < h[j].seq)
}
func (h *eventHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return x
}

type simRunner struct {
	id                                                  int
	machineIdx                                          *int
	format                                              string
	machine                                             *queuerunner.Machine
	clockSkew, runningLeaseID, startedAtMS, firstFailMS int64
	killed, paused, exited, divergent, cmdStopped       bool
	verdict                                             queue.Verdict
	exitErr                                             error
	reportOnDisk                                        map[string]junit.File
	inFlightFG, inFlightHB, lastOpID                    uint64
	pausedQueue                                         []*simEvent
	reorderedLease                                      *simEvent
	reportSentEpoch                                     map[uint64]int
	sentVerdicts                                        map[uint64]queue.Verdict
}

type simStatusClient struct {
	id           int
	machine      *queuerunner.StatusMachine
	exited       bool
	lastOpID     uint64
	sentVerdicts map[uint64]queue.Verdict
	uninit404    map[uint64]bool
}

type simWorld struct {
	seed, nowMS, serverOffset, faultEndMS, maxEndMS, reportOutageUntilMS int64
	rng                                                                  *rand.Rand
	ent                                                                  *simEntropy
	trace                                                                *traceRecorder
	stats                                                                simStats
	faults                                                               faultProfile
	faultsActive                                                         bool
	nextSeq                                                              uint64
	pq                                                                   eventHeap
	buildID                                                              string
	lim                                                                  queue.Limits
	cfg                                                                  queue.Config
	tests                                                                []string
	preds                                                                []*int64
	disk                                                                 *simDisk
	handler                                                              *queueapi.Handler
	srvEpoch                                                             int
	runners                                                              []*simRunner
	statusClients                                                        []*simStatusClient
	oracle                                                               *simOracle
}

func newSimWorld(seed int64, override *faultProfile) (*simWorld, error) {
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
	numTests := 4 + rng.IntN(9)
	faults := drawRandomProfile(rng, numTests)
	if override != nil {
		faults = *override
	}
	tests, preds := make([]string, numTests), make([]*int64, numTests)
	hasPreds := rng.IntN(4) != 0
	for i := range numTests {
		tests[i] = fmt.Sprintf("tests/t%02d.py", i)
		if hasPreds && rng.IntN(5) != 0 {
			v := int64(200 + rng.IntN(1800))
			preds[i] = &v
		}
	}
	ttlMS, maxAttempts, buildTimeout := int64(2000+rng.IntN(4)*1000), 2+rng.IntN(3), int64(180_000)
	if faults.expectCleanPass {
		maxAttempts, ttlMS = 1, 3000
	}
	if faults.shortDeadline {
		buildTimeout = ttlMS * 3
	}
	numMachines, maxBatch := 2+rng.IntN(3), 1+rng.IntN(3)
	if faults.cmdMixedSibling || faults.cmdSigSibling {
		maxBatch = max(maxBatch, 2)
	}
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000

	w := &simWorld{
		seed: seed, rng: rng, ent: &simEntropy{rng: rng}, trace: &traceRecorder{h: sha256.New()},
		stats: make(simStats), faults: faults, nowMS: 1_000_000, faultsActive: true,
		faultEndMS: 1_025_000, maxEndMS: 1_400_000, buildID: fmt.Sprintf("sim-%d", seed), lim: lim,
		cfg:   queue.Config{Machines: numMachines, LeaseTTLMS: ttlMS, BuildTimeoutMS: buildTimeout, MaxAttempts: maxAttempts, MaxBatch: maxBatch},
		tests: tests, preds: preds,
	}
	if faults.shortDeadline {
		w.faultEndMS = w.nowMS + buildTimeout + ttlMS
	}
	w.disk = &simDisk{
		rng: rng, trace: w.trace, stats: w.stats, faultsActive: &w.faultsActive,
		unwrittenPct: faults.diskUnwrittenPct, uncertainPct: faults.diskUncertainPct,
		durable: make(map[string][][]byte), unsynced: make(map[string][][]byte), broken: make(map[string]bool),
	}
	if err := w.resetHandler(nil); err != nil {
		return nil, err
	}
	w.oracle = newSimOracle(w.buildID, lim, faults.expectCleanPass)
	for i := range numMachines {
		idx, startAt := i, w.nowMS+int64(i)*50
		if faults.lateJoiner && i == numMachines-1 {
			startAt = w.nowMS + ttlMS + 500
		}
		if err := w.addRunner(&idx, tests, w.cfg, false, startAt); err != nil {
			return nil, err
		}
	}
	if faults.divergentInit {
		idx := numMachines
		if err := w.addRunner(&idx, append(slices.Clone(tests), "tests/zz_extra.py"), w.cfg, true, w.nowMS+300); err != nil {
			return nil, err
		}
	}
	if faults.serverRestart {
		w.sched(w.nowMS+1200+rng.Int64N(3000), &simEvent{kind: seRestart})
	}
	if faults.serverClockJump {
		w.sched(w.nowMS+800+rng.Int64N(2000), &simEvent{kind: seClockJump})
	}
	if len(w.runners) > 1 && faults.runnerKill {
		w.sched(w.nowMS+800+rng.Int64N(2000), &simEvent{kind: seKill, runnerIdx: 0})
	}
	if len(w.runners) > 1 && faults.runnerPause {
		victim, pauseAt := 0, w.nowMS+600+rng.Int64N(1000)
		if faults.runnerKill {
			victim = 1
		}
		w.sched(pauseAt, &simEvent{kind: sePause, runnerIdx: victim})
		w.sched(pauseAt+ttlMS+1500, &simEvent{kind: seResume, runnerIdx: victim})
	}
	w.addStatusClient(w.nowMS + 1500)
	w.sched(w.faultEndMS, &simEvent{kind: seHeal})
	return w, nil
}

func (w *simWorld) resetHandler(restored []*queue.State) error {
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: simWriteTok, ReadToken: simReadTok, Clock: w.serverClock,
		Limits: w.lim, Log: w.disk, MaxBuilds: 16, MaxRunning: 8, RetentionMS: 3600_000, InitialBuilds: restored,
	})
	if err == nil {
		w.handler = h
	}
	return err
}

func (w *simWorld) serverClock() int64 { return w.nowMS + w.serverOffset }

func (w *simWorld) sched(atMS int64, ev *simEvent) {
	ev.atMS = max(atMS, w.nowMS)
	w.nextSeq++
	ev.seq = w.nextSeq
	heap.Push(&w.pq, ev)
}

func (w *simWorld) addRunner(machineIdx *int, tests []string, cfg queue.Config, divergent bool, startAt int64) error {
	format, preds := "junit", w.preds
	if w.faults.useLines {
		format = "lines"
	}
	if len(tests) != len(preds) {
		preds = make([]*int64, len(tests))
	}
	m, err := queuerunner.NewMachine(queuerunner.WorkerConfig{
		BuildID: w.buildID, MachineIndex: machineIdx, Config: cfg, Tests: tests, PredictionsMS: preds, Format: format,
		Retry: queuerunner.RetryConfig{BudgetMS: 60_000, BaseMS: 100, MaxWaitMS: 1000}, Entropy: w.ent,
	})
	if err != nil {
		return err
	}
	var skew int64
	if w.faults.clientSkew {
		skew = w.rng.Int64N(1000) - 500
	}
	var preDisk map[string]junit.File
	if w.faults.cmdOldReport && len(tests) > 0 {
		preDisk = map[string]junit.File{tests[0]: {Seconds: big.NewRat(77, 1000), Cases: 1}}
	}
	id := len(w.runners)
	w.runners = append(w.runners, &simRunner{
		id: id, machineIdx: machineIdx, format: format, machine: m, clockSkew: skew, reportOnDisk: preDisk,
		reportSentEpoch: make(map[uint64]int), sentVerdicts: make(map[uint64]queue.Verdict), divergent: divergent,
	})
	w.sched(startAt, &simEvent{kind: seRunnerStart, runnerIdx: id})
	return nil
}

func (w *simWorld) addStatusClient(startAt int64) {
	id := len(w.statusClients)
	sm := queuerunner.NewStatusMachine(queuerunner.StatusMachineConfig{BuildID: w.buildID, Retry: queuerunner.RetryConfig{BudgetMS: 30_000, BaseMS: 100, MaxWaitMS: 1000}, Entropy: w.ent})
	w.statusClients = append(w.statusClients, &simStatusClient{id: id, machine: sm, sentVerdicts: make(map[uint64]queue.Verdict), uninit404: make(map[uint64]bool)})
	w.sched(startAt, &simEvent{kind: seStartStatus, statusIdx: id})
}

func (w *simWorld) run() error {
	conflictScheduled, finalStatusScheduled := false, false
	for w.pq.Len() > 0 {
		ev := heap.Pop(&w.pq).(*simEvent)
		if ev.atMS > w.maxEndMS {
			return fmt.Errorf("liveness bound exceeded at virtual time %d ms", ev.atMS)
		}
		w.nowMS = ev.atMS
		if err := w.dispatch(ev); err != nil {
			return err
		}
		if w.faults.conflictAfterPassed && !conflictScheduled && w.oracle.finishedVerdict == queue.VerdictPassed {
			conflictScheduled = true
			w.sched(w.nowMS+100, &simEvent{kind: seLateConflict})
		}
		if !w.faultsActive && !finalStatusScheduled && !w.disk.broken[w.buildID] && w.oracle.finishedVerdict != "" {
			finalStatusScheduled = true
			w.addStatusClient(w.nowMS + 50)
		}
	}
	return w.oracle.checkFinal(w)
}

func (w *simWorld) activeRunnerFor(ev *simEvent) *simRunner {
	r := w.runners[ev.runnerIdx]
	if r.killed || r.exited {
		return nil
	}
	if r.paused {
		r.pausedQueue = append(r.pausedQueue, ev)
		return nil
	}
	return r
}

func (w *simWorld) dispatch(ev *simEvent) error {
	switch ev.kind {
	case seRunnerStart:
		if r := w.runners[ev.runnerIdx]; !r.killed && !r.exited {
			r.startedAtMS = w.nowMS
			w.trace.logf("t=%d runner=%d start id=%s", w.nowMS, r.id, r.machine.RunnerID()[:8])
			return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvStart})
		}
	case seTimer:
		if ev.statusIdx >= 0 {
			if sc := w.statusClients[ev.statusIdx]; !sc.exited {
				return w.stepStatus(sc, queuerunner.Event{Kind: queuerunner.EvTimer, TimerKind: ev.timerKind, TimerGen: ev.timerGen})
			}
		} else if r := w.activeRunnerFor(ev); r != nil {
			return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvTimer, TimerKind: ev.timerKind, TimerGen: ev.timerGen})
		}
	case seCmdFinish:
		r := w.activeRunnerFor(ev)
		if r == nil || r.runningLeaseID != ev.leaseID {
			return nil
		}
		r.runningLeaseID = 0
		if !r.cmdStopped {
			if ex := w.oracle.execs[ev.leaseID]; ex != nil {
				ex.completed, ex.exitCode, ex.signaled = true, ev.exitCode, ev.signaled
				ex.filesWritten, ex.readErr = cloneFiles(ev.files), ev.readErr
			}
			if ev.files != nil {
				r.reportOnDisk = cloneFiles(ev.files)
			}
		}
		w.trace.logf("t=%d runner=%d cmd_exit lease=%d code=%d sig=%t", w.nowMS, r.id, ev.leaseID, ev.exitCode, ev.signaled)
		return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvCommandExited, LeaseID: ev.leaseID, ExitCode: ev.exitCode, Signaled: ev.signaled})
	case seReadReports:
		if r := w.activeRunnerFor(ev); r != nil {
			files, readErr := cloneFiles(r.reportOnDisk), ev.readErr
			if readErr == nil && len(files) == 0 {
				readErr = errors.New("report file missing")
			}
			return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvReportsRead, LeaseID: ev.leaseID, Files: files, ReadErr: readErr})
		}
	case seDeliverReq:
		return w.handleServerRequest(ev)
	case seDeliverResp:
		resp := parseSimResponse(ev.req.Kind, ev.respStatus, ev.respBody, ev.respErr)
		if ev.statusIdx >= 0 {
			sc := w.statusClients[ev.statusIdx]
			if sc.exited {
				return nil
			}
			sc.lastOpID = ev.opID
			if resp.Status != nil {
				sc.sentVerdicts[ev.opID] = resp.Status.Verdict
			} else if resp.ListTests != nil {
				sc.sentVerdicts[ev.opID] = resp.ListTests.Verdict
			}
			return w.stepStatus(sc, queuerunner.Event{Kind: queuerunner.EvResponse, OpID: ev.opID, Resp: resp})
		}
		if r := w.activeRunnerFor(ev); r != nil {
			r.lastOpID = ev.opID
			if resp.Err == nil {
				if resp.Lease != nil && resp.Lease.Status == queue.LeaseDone {
					r.sentVerdicts[ev.opID] = resp.Lease.Verdict
				} else if resp.Report != nil {
					r.sentVerdicts[ev.opID] = resp.Report.Verdict
				}
			}
			w.observeRunnerResponse(r, ev, resp)
			w.trace.logf("t=%d runner=%d resp op=%d kind=%s st=%d err=%t", w.nowMS, r.id, ev.opID, ev.req.Kind, ev.respStatus, resp.Err != nil)
			return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvResponse, OpID: ev.opID, Resp: resp})
		}
	case seKill:
		if r := w.runners[ev.runnerIdx]; !r.killed && !r.exited {
			r.killed, r.runningLeaseID = true, 0
			w.stats["kills"]++
			w.trace.logf("t=%d fault kill runner=%d", w.nowMS, r.id)
			if w.faults.runnerRestartSame && r.machineIdx != nil {
				idx := *r.machineIdx
				return w.addRunner(&idx, w.tests, w.cfg, false, w.nowMS+200)
			}
		}
	case sePause:
		if r := w.runners[ev.runnerIdx]; !r.killed && !r.exited && !r.paused {
			r.paused = true
			w.stats["pauses"]++
			w.trace.logf("t=%d fault pause runner=%d", w.nowMS, r.id)
		}
	case seResume:
		if r := w.runners[ev.runnerIdx]; !r.killed && !r.exited && r.paused {
			r.paused = false
			q := r.pausedQueue
			r.pausedQueue = nil
			for _, qev := range q {
				w.sched(w.nowMS, qev)
			}
		}
	case seRestart:
		return w.restartServer()
	case seClockJump:
		jump := int64(w.rng.IntN(4000)) - 2000
		if w.faults.shortDeadline {
			jump = w.cfg.BuildTimeoutMS + 1000
		} else if w.faults.clockBackward {
			jump = -(1500 + w.rng.Int64N(3500))
		}
		w.serverOffset += jump
		w.stats["clockJump"]++
		w.trace.logf("t=%d fault clock_jump delta=%d", w.nowMS, jump)
	case seLateConflict:
		idx := len(w.runners)
		return w.addRunner(&idx, append(slices.Clone(w.tests), "tests/zz_late_conflict.py"), w.cfg, true, w.nowMS)
	case seStartStatus:
		return w.stepStatus(w.statusClients[ev.statusIdx], queuerunner.Event{Kind: queuerunner.EvStart})
	case seHeal:
		w.faultsActive, w.reportOutageUntilMS = false, 0
		w.trace.logf("t=%d faults_healed", w.nowMS)
		for _, r := range w.runners {
			if r.paused && !r.killed && !r.exited {
				w.sched(w.nowMS, &simEvent{kind: seResume, runnerIdx: r.id})
			}
		}
		if !w.disk.broken[w.buildID] && w.oracle.finishedVerdict == "" {
			return w.ensureHealthyRunner()
		}
	}
	return nil
}

func (w *simWorld) ensureHealthyRunner() error {
	for _, r := range w.runners {
		if !r.divergent && !r.killed && !r.exited {
			return nil
		}
	}
	tests := w.tests
	if len(w.oracle.tests) > 0 {
		tests = w.oracle.tests
	}
	idx := len(w.runners)
	return w.addRunner(&idx, tests, w.cfg, false, w.nowMS+10)
}

func (w *simWorld) restartServer() error {
	w.stats["restarts"]++
	w.srvEpoch++
	clear(w.disk.unsynced)
	clear(w.disk.broken)
	var restored []*queue.State
	for _, bid := range w.disk.buildOrder {
		st, err := w.disk.replayBuild(bid, w.lim)
		if err != nil {
			return fmt.Errorf("replay build %s on restart: %w", bid, err)
		}
		if st.Initialized {
			st.ResetActiveLeaseDeadlines(w.serverClock())
			restored = append(restored, st)
		}
	}
	if err := w.resetHandler(restored); err != nil {
		return err
	}
	w.trace.logf("t=%d fault restart epoch=%d restored=%d", w.nowMS, w.srvEpoch, len(restored))
	return nil
}

func (w *simWorld) stepRunner(r *simRunner, ev queuerunner.Event) error {
	expectStopLease := int64(0)
	if ev.Kind == queuerunner.EvResponse {
		if ev.OpID == r.inFlightHB {
			if ev.Resp.Kind == queuerunner.ReqHeartbeat && ev.Resp.Err == nil && ev.Resp.Heartbeat != nil && !ev.Resp.Heartbeat.Held && r.runningLeaseID > 0 && !r.cmdStopped {
				if ex := w.oracle.execs[r.runningLeaseID]; ex != nil && !ex.completed && !ex.stopped {
					expectStopLease = r.runningLeaseID
				}
			}
			r.inFlightHB = 0
		}
		if ev.OpID == r.inFlightFG {
			r.inFlightFG = 0
			if ev.Resp.Err == nil {
				r.firstFailMS = 0
			} else {
				var apiErr *queueapi.APIError
				if errors.As(ev.Resp.Err, &apiErr) && apiErr.HTTPStatus == 409 {
					switch apiErr.Code {
					case queue.ErrCodeStaleRequest:
						w.stats["staleRequest"]++
					case queue.ErrCodeLeaseHeld:
						w.stats["leaseHeld"]++
					}
				}
				if r.firstFailMS == 0 && queueapi.IsTransientError(ev.Resp.Err) {
					r.firstFailMS = w.nowMS
				}
			}
		}
	}
	stoppedExpected := false
	for _, a := range r.machine.Step(w.nowMS+r.clockSkew, ev) {
		switch a.Kind {
		case queuerunner.ActSend:
			slot := &r.inFlightFG
			if a.Req.Kind == queuerunner.ReqHeartbeat {
				slot = &r.inFlightHB
			}
			if *slot != 0 {
				return fmt.Errorf("oracle: runner %d sent concurrent %s op %d while op %d in flight (stale timer not fenced)", r.id, a.Req.Kind, a.OpID, *slot)
			}
			*slot = a.OpID
			wr, err := encodeSimRequest(a.Req)
			if err != nil {
				return err
			}
			if a.Req.Kind == queuerunner.ReqReport {
				r.reportSentEpoch[a.OpID] = w.srvEpoch
				if w.oracle.leaseLostWhileRunning[a.Req.Report.LeaseID] {
					return fmt.Errorf("oracle: runner %d reported lease %d after heartbeat held=false stopped it", r.id, a.Req.Report.LeaseID)
				}
			}
			if a.Req.Kind == queuerunner.ReqInit && r.divergent {
				w.stats["divInit"]++
			}
			w.enqueueNetReq(r.id, -1, a.OpID, a.Req, wr, simWriteTok)
		case queuerunner.ActSetTimer:
			w.sched(a.AtMS-r.clockSkew, &simEvent{kind: seTimer, runnerIdx: r.id, statusIdx: -1, timerKind: a.TimerKind, timerGen: a.TimerGen})
		case queuerunner.ActStartCommand:
			if a.ClearReports {
				r.reportOnDisk = nil
			}
			r.runningLeaseID, r.cmdStopped = a.LeaseID, false
			if len(a.Attempts) == 1 && a.Attempts[0].Generation > 1 {
				w.stats["soloRetry"]++
			}
			if err := w.startSimCommand(r, a.LeaseID, a.Attempts); err != nil {
				return err
			}
		case queuerunner.ActStopCommand:
			stoppedExpected = stoppedExpected || a.LeaseID == expectStopLease
			if r.runningLeaseID == a.LeaseID && !r.cmdStopped {
				r.cmdStopped = true
				if ex := w.oracle.execs[a.LeaseID]; ex != nil {
					ex.stopped = true
				}
				w.sched(w.nowMS+20, &simEvent{kind: seCmdFinish, runnerIdx: r.id, leaseID: a.LeaseID, exitCode: -1, signaled: true})
			}
		case queuerunner.ActReadReports:
			if w.faults.reportOutage && w.faultsActive && w.reportOutageUntilMS == 0 {
				w.reportOutageUntilMS = w.nowMS + w.cfg.LeaseTTLMS + w.cfg.LeaseTTLMS/2
				w.stats["reportOutage"]++
			}
			var plannedErr error
			if ex := w.oracle.execs[a.LeaseID]; ex != nil {
				plannedErr = ex.plannedReadErr
			}
			w.sched(w.nowMS+25+w.rng.Int64N(50), &simEvent{kind: seReadReports, runnerIdx: r.id, leaseID: a.LeaseID, readErr: plannedErr})
		case queuerunner.ActDone:
			r.exited, r.verdict, r.exitErr = true, a.Verdict, a.Err
			var extraErr *queuerunner.ExtraFilesError
			if errors.As(a.Err, &extraErr) {
				w.stats["extraFile"]++
			}
			w.trace.logf("t=%d runner=%d done verdict=%s err=%t", w.nowMS, r.id, a.Verdict, a.Err != nil)
			if err := w.oracle.onRunnerExit(w, r); err != nil {
				return err
			}
			if extraErr != nil && !w.faultsActive && !w.disk.broken[w.buildID] && w.oracle.finishedVerdict == "" {
				if err := w.ensureHealthyRunner(); err != nil {
					return err
				}
			}
		}
	}
	if expectStopLease > 0 && w.oracle.hbOpLease[ev.OpID] == expectStopLease {
		w.oracle.leaseLostWhileRunning[expectStopLease] = true
		if !stoppedExpected {
			return fmt.Errorf("oracle: runner %d ignored heartbeat held=false on active lease %d", r.id, expectStopLease)
		}
	}
	return nil
}

func (w *simWorld) stepStatus(sc *simStatusClient, ev queuerunner.Event) error {
	for _, a := range sc.machine.Step(w.nowMS, ev) {
		switch a.Kind {
		case queuerunner.ActSend:
			wr, err := encodeSimRequest(a.Req)
			if err != nil {
				return err
			}
			w.enqueueNetReq(-1, sc.id, a.OpID, a.Req, wr, simReadTok)
		case queuerunner.ActSetTimer:
			w.sched(a.AtMS, &simEvent{kind: seTimer, runnerIdx: -1, statusIdx: sc.id, timerKind: a.TimerKind, timerGen: a.TimerGen})
		case queuerunner.ActDone:
			sc.exited = true
			if a.HasReport && a.Report != nil {
				w.stats["statusReport"]++
			}
			w.trace.logf("t=%d status=%d done verdict=%s report=%t err=%t", w.nowMS, sc.id, a.Verdict, a.HasReport, a.Err != nil)
			if err := w.oracle.onStatusDone(w, sc, a); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *simWorld) enqueueNetReq(runnerIdx, statusIdx int, opID uint64, req queuerunner.Request, wr queueapi.WireRequest, tok string) {
	w.trace.hashBytes("req", wr.Body)
	if req.Kind == queuerunner.ReqHeartbeat && req.Heartbeat != nil {
		w.oracle.hbOpLease[opID] = req.Heartbeat.LeaseID
	}
	delay := int64(5) + w.rng.Int64N(w.faults.netMaxDelayMS)
	if w.faultsActive && statusIdx < 0 && w.faults.reqDropPct > 0 && w.rng.IntN(100) < w.faults.reqDropPct &&
		!(w.faults.expectCleanPass && req.Kind == queuerunner.ReqHeartbeat) {
		w.stats["reqDrop"]++
		w.sched(w.nowMS+delay, &simEvent{kind: seDeliverResp, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, respErr: queueapi.NewTransportError("request dropped")})
		return
	}
	w.sched(w.nowMS+delay, &simEvent{kind: seDeliverReq, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, wire: wr, token: tok})
	if w.faultsActive && statusIdx < 0 && w.faults.reqDupPct > 0 && w.rng.IntN(100) < w.faults.reqDupPct {
		w.stats["reqDup"]++
		dupEv := &simEvent{kind: seDeliverReq, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, wire: wr, token: tok, isDup: true}
		w.sched(w.nowMS+delay+50+w.rng.Int64N(w.cfg.LeaseTTLMS*2), dupEv)
		if req.Kind == queuerunner.ReqLease && w.runners[runnerIdx].reorderedLease == nil {
			cp := *dupEv
			w.runners[runnerIdx].reorderedLease = &cp
		}
	}
}

func (w *simWorld) handleServerRequest(ev *simEvent) error {
	respDelay := int64(5) + w.rng.Int64N(w.faults.netMaxDelayMS)
	replyErr := func(code int, stat, body string) {
		w.stats[stat]++
		w.trace.hashBytes("resp", []byte(body))
		w.sched(w.nowMS+respDelay, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respStatus: code, respBody: []byte(body)})
	}
	switch {
	case w.reportOutageUntilMS > 0 && w.nowMS < w.reportOutageUntilMS && ev.req.Kind == queuerunner.ReqReport:
		replyErr(http.StatusServiceUnavailable, "http503", `{"version":1,"error":{"code":"storage_error","message":"simulated report outage"}}`)
		return nil
	case w.faultsActive && ev.statusIdx < 0 && w.faults.http503Pct > 0 && w.rng.IntN(100) < w.faults.http503Pct:
		replyErr(http.StatusServiceUnavailable, "http503", `{"version":1,"error":{"code":"storage_error","message":"simulated 503"}}`)
		return nil
	case w.faultsActive && ev.statusIdx < 0 && w.faults.http429Pct > 0 && w.rng.IntN(100) < w.faults.http429Pct:
		replyErr(http.StatusTooManyRequests, "http429", `{"version":1,"error":{"code":"rate_limited","message":"simulated 429"}}`)
		return nil
	}

	triggerRestart := w.faults.serverRestart && w.faultsActive && ev.req.Kind == queuerunner.ReqReport && w.stats["reportAcrossRestart"] == 0 && w.srvEpoch == 0
	beforeN, wasInit := len(w.disk.durable[w.buildID]), w.oracle.initialized
	status, body := w.invokeHandler(ev.wire, ev.token)
	if ev.statusIdx >= 0 && status == http.StatusNotFound && !wasInit {
		w.statusClients[ev.statusIdx].uninit404[ev.opID] = true
	}
	w.trace.hashBytes("resp", body)

	var newEvents []queue.Event
	for _, raw := range w.disk.durable[w.buildID][beforeN:] {
		dec, err := queue.DecodeEvent(raw)
		if err != nil {
			return err
		}
		newEvents = append(newEvents, dec)
	}
	if err := w.oracle.onServerHandled(w, ev.req, status, body, newEvents); err != nil {
		return err
	}

	dropReply := func(delay int64, msg string) {
		w.stats["respDrop"]++
		w.sched(w.nowMS+delay, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respErr: queueapi.NewTransportError(msg)})
	}
	if triggerRestart && status == http.StatusOK {
		if err := w.restartServer(); err != nil {
			return err
		}
		dropReply(respDelay, "reset by restart")
		return nil
	}
	// Deliver a previously duplicated earlier Lease packet right after a later
	// Report closes a lease, modeling network reordering that creates an orphan lease.
	if w.faultsActive && ev.statusIdx < 0 && ev.req.Kind == queuerunner.ReqReport && status == http.StatusOK {
		if r := w.runners[ev.runnerIdx]; r.reorderedLease != nil && r.reorderedLease.opID < ev.opID-2 {
			re := r.reorderedLease
			r.reorderedLease = nil
			w.sched(w.nowMS+1, re)
		}
	}
	if w.faultsActive && w.faults.runnerPause && ev.statusIdx < 0 && !ev.isDup && ev.req.Kind == queuerunner.ReqLease &&
		status == http.StatusOK && w.stats["staleRequest"] == 0 && strings.Contains(string(body), `"status":"leased"`) {
		dropReply(w.cfg.LeaseTTLMS+400, "lease reply lost past TTL")
		return nil
	}
	if w.faultsActive && ev.statusIdx < 0 && !ev.isDup && w.faults.respDropPct > 0 && w.rng.IntN(100) < w.faults.respDropPct &&
		!(w.faults.expectCleanPass && ev.req.Kind == queuerunner.ReqHeartbeat) {
		dropReply(respDelay, "response dropped")
		return nil
	}
	w.sched(w.nowMS+respDelay, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respStatus: status, respBody: body})
	if w.faultsActive && ev.statusIdx < 0 && w.faults.respDupPct > 0 && w.rng.IntN(100) < w.faults.respDupPct {
		w.stats["respDup"]++
		w.sched(w.nowMS+respDelay+50+w.rng.Int64N(w.cfg.LeaseTTLMS), &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respStatus: status, respBody: body, isDup: true})
	}
	return nil
}

func (w *simWorld) invokeHandler(wr queueapi.WireRequest, tok string) (int, []byte) {
	req := httptest.NewRequest(wr.Method, "http://sim"+wr.Path, bytes.NewReader(wr.Body))
	if wr.ContentType != "" {
		req.Header.Set("Content-Type", wr.ContentType)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	w.handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

func (w *simWorld) observeRunnerResponse(r *simRunner, ev *simEvent, resp queuerunner.Response) {
	if resp.Err != nil || ev.req.Kind != queuerunner.ReqReport || resp.Report == nil {
		return
	}
	epoch, repSeen := w.oracle.reportedLeases[ev.req.Report.LeaseID]
	if sentEpoch, ok := r.reportSentEpoch[ev.opID]; (ok && w.srvEpoch > sentEpoch) || (w.srvEpoch > 0 && repSeen && epoch == 0) {
		w.stats["reportAcrossRestart"]++
	}
	for _, d := range resp.Report.Results {
		if d.Disposition == queue.DispositionStale {
			w.stats["staleResult"]++
		} else if d.Disposition == queue.DispositionDuplicate {
			w.stats["dupReport"]++
		}
	}
}

func (w *simWorld) startSimCommand(r *simRunner, leaseID int64, attempts []queue.LeasedAttemptView) error {
	durMS, exitCode, signaled := int64(150+w.rng.IntN(700)), 0, false
	var files map[string]junit.File
	var readErr error
	passAll := func() map[string]junit.File {
		out := make(map[string]junit.File, len(attempts))
		for _, a := range attempts {
			out[a.Test] = junit.File{Seconds: big.NewRat(int64(50+w.rng.IntN(400)), 1000), Cases: 1}
		}
		return out
	}
	switch {
	case slices.ContainsFunc(attempts, func(a queue.LeasedAttemptView) bool { return a.TestIndex == w.faults.poisonIdx }):
		exitCode, signaled = 137, true
	case w.faultsActive && w.faults.cmdHang && w.rng.IntN(4) == 0:
		durMS, files = w.cfg.LeaseTTLMS*2+500, passAll()
	case w.faultsActive && w.faults.cmdExtraFile && w.rng.IntN(4) == 0:
		files = passAll()
		files["tests/unassigned_extra.py"] = junit.File{Seconds: big.NewRat(50, 1000), Cases: 1}
	case w.faultsActive && w.faults.cmdSigSibling && len(attempts) >= 2 && w.rng.IntN(3) == 0:
		exitCode, signaled, files = 137, true, passAll()
		files[attempts[0].Test] = junit.File{Seconds: big.NewRat(80, 1000), Cases: 1, Failed: true}
	case w.faultsActive && w.faults.cmdTruncated && w.rng.IntN(5) == 0:
		readErr = errors.New("unexpected EOF in JUnit XML")
	case w.faultsActive && w.faults.cmdNoReport && (w.rng.IntN(5) == 0 || (r.reportOnDisk != nil && slices.ContainsFunc(attempts, func(a queue.LeasedAttemptView) bool { _, ok := r.reportOnDisk[a.Test]; return ok }))):
	case w.faultsActive && w.faults.cmdCrash && w.rng.IntN(5) == 0:
		exitCode, signaled = 137, true
	case w.faultsActive && w.faults.cmdMixedSibling && len(attempts) >= 2 && w.rng.IntN(3) == 0:
		exitCode, files = 1, make(map[string]junit.File, len(attempts))
		skipSecond := w.rng.IntN(2) == 0
		for i, a := range attempts {
			if i == 0 {
				files[a.Test] = junit.File{Seconds: big.NewRat(80, 1000), Cases: 1, Failed: true}
			} else if i == 1 && !skipSecond {
				files[a.Test] = junit.File{Seconds: big.NewRat(120, 1000), Cases: 2}
			} else {
				files[a.Test] = junit.File{Seconds: big.NewRat(0, 1000), Cases: 2, Skipped: 2}
			}
		}
	case w.faultsActive && w.faults.cmdFail && w.rng.IntN(5) == 0:
		exitCode, files = 1, passAll()
		files[attempts[0].Test] = junit.File{Seconds: big.NewRat(90, 1000), Cases: 1, Failed: true}
	case w.faults.cmdSkip && w.rng.IntN(4) == 0:
		files = passAll()
		files[attempts[0].Test] = junit.File{Seconds: big.NewRat(0, 1000), Cases: 2, Skipped: 2}
	default:
		files = passAll()
	}
	if err := w.oracle.recordCommandStart(leaseID, r.id, r.format, attempts, readErr); err != nil {
		return err
	}
	w.sched(w.nowMS+durMS, &simEvent{kind: seCmdFinish, runnerIdx: r.id, leaseID: leaseID, exitCode: exitCode, signaled: signaled, files: files, readErr: readErr})
	return nil
}

func cloneFiles(in map[string]junit.File) map[string]junit.File {
	if in == nil {
		return nil
	}
	out := make(map[string]junit.File, len(in))
	for k, v := range in {
		if v.Seconds != nil {
			v.Seconds = new(big.Rat).Set(v.Seconds)
		}
		out[k] = v
	}
	return out
}

func encodeSimRequest(req queuerunner.Request) (queueapi.WireRequest, error) {
	switch req.Kind {
	case queuerunner.ReqInit:
		return queueapi.EncodeInit(*req.Init)
	case queuerunner.ReqLease:
		return queueapi.EncodeLease(req.BuildID, *req.Lease)
	case queuerunner.ReqHeartbeat:
		return queueapi.EncodeHeartbeat(req.BuildID, *req.Heartbeat)
	case queuerunner.ReqReport:
		return queueapi.EncodeReport(req.BuildID, *req.Report)
	case queuerunner.ReqStatus:
		return queueapi.EncodeStatus(req.BuildID), nil
	case queuerunner.ReqListTests:
		return queueapi.EncodeListTests(req.BuildID, *req.ListTests), nil
	}
	return queueapi.WireRequest{}, fmt.Errorf("unknown request kind %q", req.Kind)
}

func parseSimResponse(kind queuerunner.RequestKind, status int, body []byte, transportErr error) queuerunner.Response {
	resp := queuerunner.Response{Kind: kind, Err: transportErr}
	if transportErr != nil {
		return resp
	}
	switch kind {
	case queuerunner.ReqInit:
		r, err := queueapi.ParseInitResponse(status, body)
		resp.Init, resp.Err = &r, err
	case queuerunner.ReqLease:
		r, err := queueapi.ParseLeaseResponse(status, body)
		resp.Lease, resp.Err = &r, err
	case queuerunner.ReqHeartbeat:
		r, err := queueapi.ParseHeartbeatResponse(status, body)
		resp.Heartbeat, resp.Err = &r, err
	case queuerunner.ReqReport:
		r, err := queueapi.ParseReportResponse(status, body)
		resp.Report, resp.Err = &r, err
	case queuerunner.ReqStatus:
		r, err := queueapi.ParseStatusResponse(status, body)
		resp.Status, resp.Err = &r, err
	case queuerunner.ReqListTests:
		r, err := queueapi.ParseListTestsResponse(status, body)
		resp.ListTests, resp.Err = &r, err
	}
	return resp
}
