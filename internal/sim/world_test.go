package sim_test

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
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

type statKind int

const (
	stReqDrop statKind = iota
	stRespDrop
	stReqDup
	stRespDup
	stRestart
	stClockJump
	stDiskUnwritten
	stDiskUncertain
	stKill
	stPause
	stDivInit
	stReportOutage
	stStaleResult
	stStaleRequest
	stLeaseHeld
	stSoloRetry
	stExhausted
	stDeadline
	stConflictAfterPassed
	stReportAcrossRestart
	stBroken
	stDupReport
	stExtraFile
	stStatusReport
	stPassed
	stFailed
	stUnresolved
	statCount
)

var statNames = [statCount]string{
	"reqDrop", "respDrop", "reqDup", "respDup", "restarts", "clockJump",
	"diskUnwritten", "diskUncertain", "kills", "pauses", "divInit", "reportOutage",
	"staleResult", "staleRequest", "leaseHeld", "soloRetry", "exhausted", "deadline",
	"conflictAfterPassed", "reportAcrossRestart", "broken", "dupReport", "extraFile",
	"statusReport", "passed", "failed", "unresolved",
}

type simStats [statCount]int

func (s *simStats) add(o simStats) {
	for i := range s {
		s[i] += o[i]
	}
}

func (s simStats) summary() string {
	var b strings.Builder
	for i, name := range statNames {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%d", name, s[i])
	}
	return b.String()
}

type simEntropy struct{ rng *rand.Rand }

func (e *simEntropy) Hex16() string {
	var b [16]byte
	hi, lo := e.rng.Uint64(), e.rng.Uint64()
	for i := range 8 {
		b[i], b[8+i] = byte(hi>>(8*i)), byte(lo>>(8*i))
	}
	return hex.EncodeToString(b[:])
}

func (e *simEntropy) Int64N(n int64) int64 { return e.rng.Int64N(n) }

type traceRecorder struct {
	h     hash.Hash
	lines [traceTail]string
	count int
}

func newTraceRecorder() *traceRecorder { return &traceRecorder{h: sha256.New()} }

func (tr *traceRecorder) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	_, _ = tr.h.Write([]byte(line))
	_, _ = tr.h.Write([]byte{'\n'})
	tr.lines[tr.count%traceTail] = line
	tr.count++
}

func (tr *traceRecorder) digest() string { return hex.EncodeToString(tr.h.Sum(nil)) }

func (tr *traceRecorder) tail() string {
	var b strings.Builder
	for i := max(0, tr.count-traceTail); i < tr.count; i++ {
		b.WriteString(tr.lines[i%traceTail])
		b.WriteByte('\n')
	}
	return b.String()
}

// simDisk implements queueapi.EventLog on a deterministic simulated disk with
// unwritten/disk-full errors, uncertain fsync errors, and unsynced torn bytes.
type simDisk struct {
	rng          *rand.Rand
	trace        *traceRecorder
	stats        *simStats
	faultsActive *bool
	unwrittenPct int
	uncertainPct int

	buildOrder []string
	durable    map[string][][]byte
	unsynced   map[string][][]byte
	broken     map[string]bool
}

func newSimDisk(rng *rand.Rand, tr *traceRecorder, st *simStats, active *bool, unwrittenPct, uncertainPct int) *simDisk {
	return &simDisk{
		rng: rng, trace: tr, stats: st, faultsActive: active,
		unwrittenPct: unwrittenPct, uncertainPct: uncertainPct,
		durable: make(map[string][][]byte), unsynced: make(map[string][][]byte), broken: make(map[string]bool),
	}
}

func (d *simDisk) Append(_ context.Context, buildID string, events []queue.Event) error {
	if len(events) == 0 {
		return nil
	}
	if *d.faultsActive {
		if d.unwrittenPct > 0 && d.rng.IntN(100) < d.unwrittenPct {
			d.stats[stDiskUnwritten]++
			d.trace.logf("fault disk_unwritten build=%s n=%d", buildID, len(events))
			return fmt.Errorf("simulated disk full: %w", queueapi.ErrAppendUnwritten)
		}
		if d.uncertainPct > 0 && d.rng.IntN(100) < d.uncertainPct {
			d.stats[stDiskUncertain]++
			d.stats[stBroken]++
			d.broken[buildID] = true
			landed := d.rng.IntN(2) == 0
			if landed {
				d.writeDurable(buildID, events)
			}
			d.trace.logf("fault disk_uncertain build=%s n=%d landed=%t", buildID, len(events), landed)
			return errors.New("simulated uncertain fsync error")
		}
		if d.rng.IntN(20) == 0 {
			d.unsynced[buildID] = append(d.unsynced[buildID], []byte(`{"type":"torn`))
		}
	}
	d.writeDurable(buildID, events)
	return nil
}

func (d *simDisk) writeDurable(buildID string, events []queue.Event) {
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
	}
	delete(d.unsynced, buildID)
}

func (d *simDisk) replayBuild(buildID string, lim queue.Limits) (*queue.State, int, error) {
	frames := d.durable[buildID]
	st := queue.NewState(lim)
	for _, raw := range frames {
		ev, err := queue.DecodeEvent(raw)
		if err != nil {
			return nil, len(frames), err
		}
		if err := st.Apply(ev); err != nil {
			return nil, len(frames), err
		}
	}
	return st, len(frames), nil
}

type faultProfile struct {
	reqDropPct, respDropPct, reqDupPct, respDupPct int
	http503Pct, http429Pct                         int
	diskUnwrittenPct, diskUncertainPct             int
	netMaxDelayMS                                  int64
	serverRestart, serverClockJump, clientSkew     bool
	runnerKill, runnerPause, runnerRestartSame     bool
	lateJoiner, divergentInit, conflictAfterPassed bool
	leaseHeldProbe, shortDeadline, reportOutage    bool
	cmdFail, cmdSkip, cmdCrash, cmdHang            bool
	cmdNoReport, cmdOldReport, cmdTruncated        bool
	cmdExtraFile, cmdMixedSibling, useLines        bool
	poisonIdx                                      int
}

func drawFaultProfile(seed int64, rng *rand.Rand, numTests int) faultProfile {
	p := faultProfile{poisonIdx: -1, netMaxDelayMS: 10 + rng.Int64N(400)}
	if rng.IntN(3) != 0 {
		p.reqDropPct = 2 + rng.IntN(16)
	}
	if rng.IntN(3) != 0 {
		p.respDropPct = 2 + rng.IntN(16)
	}
	if rng.IntN(3) != 0 {
		p.reqDupPct = 3 + rng.IntN(15)
	}
	if rng.IntN(3) != 0 {
		p.respDupPct = 3 + rng.IntN(15)
	}
	if rng.IntN(3) == 0 {
		p.http503Pct = 3 + rng.IntN(12)
	}
	if rng.IntN(4) == 0 {
		p.http429Pct = 3 + rng.IntN(10)
	}
	if rng.IntN(3) == 0 {
		p.diskUnwrittenPct = 3 + rng.IntN(12)
	}
	if rng.IntN(10) == 0 {
		p.diskUncertainPct = 3 + rng.IntN(8)
	}
	p.serverRestart = rng.IntN(3) == 0
	p.serverClockJump = rng.IntN(3) == 0
	p.clientSkew = rng.IntN(2) == 0
	p.runnerKill = rng.IntN(2) == 0
	p.runnerPause = rng.IntN(2) == 0
	p.runnerRestartSame = rng.IntN(3) == 0
	p.lateJoiner = rng.IntN(2) == 0
	p.divergentInit = rng.IntN(4) == 0
	p.conflictAfterPassed = rng.IntN(5) == 0
	p.leaseHeldProbe = rng.IntN(3) == 0
	p.shortDeadline = rng.IntN(8) == 0
	p.reportOutage = rng.IntN(5) == 0
	p.cmdFail = rng.IntN(3) == 0
	p.cmdSkip = rng.IntN(2) == 0
	p.cmdCrash = rng.IntN(2) == 0
	p.cmdHang = rng.IntN(3) == 0
	p.cmdNoReport = rng.IntN(3) == 0
	p.cmdOldReport = rng.IntN(3) == 0
	p.cmdTruncated = rng.IntN(3) == 0
	p.cmdExtraFile = rng.IntN(5) == 0
	p.cmdMixedSibling = rng.IntN(2) == 0
	if rng.IntN(4) == 0 && numTests > 0 {
		p.poisonIdx = rng.IntN(numTests)
	}
	p.useLines = rng.IntN(4) == 0

	// Dedicated baseline profiles on seed%10 guarantee every required scenario
	// is exercised reliably in small CI seed counts too.
	switch seed % 10 {
	case 0:
		p = faultProfile{poisonIdx: -1, netMaxDelayMS: 30, conflictAfterPassed: true, cmdSkip: true}
	case 1:
		p = faultProfile{poisonIdx: -1, netMaxDelayMS: 20, reportOutage: true, cmdSkip: true}
	case 2:
		p = faultProfile{poisonIdx: -1, netMaxDelayMS: 60, serverRestart: true, reqDupPct: 15, respDropPct: 12, respDupPct: 10, cmdSkip: true}
	case 3:
		p = faultProfile{poisonIdx: -1, netMaxDelayMS: 40, cmdOldReport: true, cmdNoReport: true, cmdMixedSibling: true, cmdFail: true, cmdSkip: true}
	case 4:
		p = faultProfile{poisonIdx: -1, netMaxDelayMS: 40, runnerPause: true, respDropPct: 15, leaseHeldProbe: true, cmdCrash: true}
	case 5:
		p = faultProfile{poisonIdx: 0, netMaxDelayMS: 30, cmdCrash: true}
	case 6:
		p = faultProfile{poisonIdx: -1, netMaxDelayMS: 30, shortDeadline: true, serverClockJump: true, cmdHang: true}
	case 7:
		p = faultProfile{poisonIdx: -1, netMaxDelayMS: 30, diskUncertainPct: 25, diskUnwrittenPct: 15}
	}
	return p
}

type simEventKind int

const (
	seRunnerStart simEventKind = iota + 1
	seDeliverReq
	seDeliverResp
	seTimer
	seCmdFinish
	seReadReports
	seKill
	sePause
	seResume
	seRestart
	seClockJump
	seLeaseHeldProbe
	seLateConflict
	seStartStatus
	seHeal
)

type simEvent struct {
	atMS       int64
	seq        uint64
	kind       simEventKind
	runnerIdx  int
	statusIdx  int
	opID       uint64
	req        queuerunner.Request
	wire       queueapi.WireRequest
	token      string
	respStatus int
	respBody   []byte
	respErr    error
	timerKind  queuerunner.TimerKind
	timerGen   uint64
	leaseID    int64
	exitCode   int
	signaled   bool
	files      map[string]junit.File
	readErr    error
	isDup      bool
}

type eventHeap []*simEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].atMS != h[j].atMS {
		return h[i].atMS < h[j].atMS
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*simEvent)) }
func (h *eventHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return x
}

type simRunner struct {
	id              int
	machineIdx      *int
	format          string
	machine         *queuerunner.Machine
	clockSkew       int64
	killed, paused  bool
	exited          bool
	divergent       bool
	verdict         queue.Verdict
	exitErr         error
	reportOnDisk    map[string]junit.File
	runningLeaseID  int64
	cmdStopped      bool
	pausedQueue     []*simEvent
	reportSentEpoch map[uint64]int
}

type simStatusClient struct {
	id      int
	machine *queuerunner.StatusMachine
	exited  bool
}

type simWorld struct {
	seed         int64
	rng          *rand.Rand
	ent          *simEntropy
	trace        *traceRecorder
	stats        simStats
	faults       faultProfile
	nowMS        int64
	serverOffset int64
	faultsActive bool
	faultEndMS   int64
	maxEndMS     int64
	nextSeq      uint64
	pq           eventHeap

	buildID             string
	lim                 queue.Limits
	cfg                 queue.Config
	tests               []string
	preds               []*int64
	disk                *simDisk
	handler             *queueapi.Handler
	srvEpoch            int
	reportOutageUntilMS int64
	runners             []*simRunner
	statusClients       []*simStatusClient
	oracle              *simOracle
}

func newSimWorld(seed int64) (*simWorld, error) {
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
	numTests := 4 + rng.IntN(9)
	faults := drawFaultProfile(seed, rng, numTests)

	tests := make([]string, numTests)
	preds := make([]*int64, numTests)
	hasPreds := rng.IntN(4) != 0
	for i := range numTests {
		tests[i] = fmt.Sprintf("tests/t%02d.py", i)
		if hasPreds && rng.IntN(5) != 0 {
			v := int64(200 + rng.IntN(1800))
			preds[i] = &v
		}
	}

	ttlMS := int64(2000 + rng.IntN(4)*1000)
	maxAttempts := 2 + rng.IntN(3)
	if faults.reportOutage {
		maxAttempts, ttlMS = 1, 3000
	}
	buildTimeout := int64(180_000)
	if faults.shortDeadline {
		buildTimeout = ttlMS * 3
	}
	numMachines := 2 + rng.IntN(3)
	maxBatch := 1 + rng.IntN(3)
	if faults.cmdMixedSibling {
		maxBatch = max(maxBatch, 2)
	}
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000

	w := &simWorld{
		seed: seed, rng: rng, ent: &simEntropy{rng: rng}, trace: newTraceRecorder(),
		faults: faults, nowMS: 1_000_000, faultsActive: true,
		faultEndMS: 1_025_000, maxEndMS: 1_400_000,
		buildID: fmt.Sprintf("sim-%d", seed), lim: lim,
		cfg: queue.Config{
			Machines: numMachines, LeaseTTLMS: ttlMS,
			BuildTimeoutMS: buildTimeout, MaxAttempts: maxAttempts, MaxBatch: maxBatch,
		},
		tests: tests, preds: preds,
	}
	if faults.shortDeadline {
		w.faultEndMS = w.nowMS + buildTimeout + ttlMS
	}
	w.disk = newSimDisk(rng, w.trace, &w.stats, &w.faultsActive, faults.diskUnwrittenPct, faults.diskUncertainPct)
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: simWriteTok, ReadToken: simReadTok, Clock: w.serverClock,
		Limits: lim, Log: w.disk, MaxBuilds: 16, MaxRunning: 8, RetentionMS: 3600_000,
	})
	if err != nil {
		return nil, err
	}
	w.handler = h
	w.oracle = newSimOracle(w.buildID, tests, w.cfg, seed%10 == 1)

	for i := range numMachines {
		idx := i
		startAt := w.nowMS + int64(i)*50
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
		w.sched(w.nowMS+1000+rng.Int64N(2500), &simEvent{kind: seClockJump})
	}
	if faults.runnerKill && len(w.runners) > 1 {
		w.sched(w.nowMS+800+rng.Int64N(2000), &simEvent{kind: seKill, runnerIdx: 0})
	}
	if faults.runnerPause && len(w.runners) > 1 {
		victim := 0
		if faults.runnerKill {
			victim = 1
		}
		pauseAt := w.nowMS + 600 + rng.Int64N(1000)
		w.sched(pauseAt, &simEvent{kind: sePause, runnerIdx: victim})
		w.sched(pauseAt+ttlMS+1500, &simEvent{kind: seResume, runnerIdx: victim})
	}
	if faults.leaseHeldProbe {
		w.sched(w.nowMS+700+rng.Int64N(800), &simEvent{kind: seLeaseHeldProbe})
	}
	w.addStatusClient(w.nowMS + 1500)
	w.sched(w.faultEndMS, &simEvent{kind: seHeal})
	return w, nil
}

func (w *simWorld) serverClock() int64 { return w.nowMS + w.serverOffset }

func (w *simWorld) sched(atMS int64, ev *simEvent) {
	ev.atMS = max(atMS, w.nowMS)
	w.nextSeq++
	ev.seq = w.nextSeq
	heap.Push(&w.pq, ev)
}

func (w *simWorld) addRunner(machineIdx *int, tests []string, cfg queue.Config, divergent bool, startAt int64) error {
	format := "junit"
	if w.faults.useLines {
		format = "lines"
	}
	preds := w.preds
	if len(tests) != len(preds) {
		preds = make([]*int64, len(tests))
	}
	m, err := queuerunner.NewMachine(queuerunner.WorkerConfig{
		BuildID: w.buildID, MachineIndex: machineIdx, Config: cfg,
		Tests: tests, PredictionsMS: preds, Format: format,
		Retry:   queuerunner.RetryConfig{BudgetMS: 60_000, BaseMS: 100, MaxWaitMS: 1000},
		Entropy: w.ent,
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
		id: id, machineIdx: machineIdx, format: format, machine: m, clockSkew: skew,
		reportOnDisk: preDisk, reportSentEpoch: make(map[uint64]int), divergent: divergent,
	})
	w.sched(startAt, &simEvent{kind: seRunnerStart, runnerIdx: id})
	return nil
}

func (w *simWorld) addStatusClient(startAt int64) {
	id := len(w.statusClients)
	sm := queuerunner.NewStatusMachine(queuerunner.StatusMachineConfig{
		BuildID: w.buildID,
		Retry:   queuerunner.RetryConfig{BudgetMS: 30_000, BaseMS: 100, MaxWaitMS: 1000},
		Entropy: w.ent,
	})
	w.statusClients = append(w.statusClients, &simStatusClient{id: id, machine: sm})
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
		if err := w.oracle.checkStep(w); err != nil {
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

func (w *simWorld) dispatch(ev *simEvent) error {
	switch ev.kind {
	case seRunnerStart:
		r := w.runners[ev.runnerIdx]
		if r.killed || r.exited {
			return nil
		}
		w.trace.logf("t=%d runner=%d start id=%s", w.nowMS, r.id, r.machine.RunnerID()[:8])
		return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvStart})

	case seTimer:
		if ev.statusIdx >= 0 {
			sc := w.statusClients[ev.statusIdx]
			if sc.exited {
				return nil
			}
			w.trace.logf("t=%d status=%d timer gen=%d", w.nowMS, sc.id, ev.timerGen)
			return w.stepStatus(sc, queuerunner.Event{Kind: queuerunner.EvTimer, TimerKind: ev.timerKind, TimerGen: ev.timerGen})
		}
		r := w.runners[ev.runnerIdx]
		if r.killed || r.exited {
			return nil
		}
		if r.paused {
			r.pausedQueue = append(r.pausedQueue, ev)
			return nil
		}
		w.trace.logf("t=%d runner=%d timer kind=%d gen=%d", w.nowMS, r.id, ev.timerKind, ev.timerGen)
		return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvTimer, TimerKind: ev.timerKind, TimerGen: ev.timerGen})

	case seCmdFinish:
		r := w.runners[ev.runnerIdx]
		if r.killed || r.exited || r.runningLeaseID != ev.leaseID {
			return nil
		}
		if r.paused {
			r.pausedQueue = append(r.pausedQueue, ev)
			return nil
		}
		r.runningLeaseID = 0
		if !r.cmdStopped {
			w.oracle.markCommandCompleted(ev.leaseID, ev.exitCode, ev.signaled, ev.files, ev.readErr)
			if ev.files != nil {
				r.reportOnDisk = cloneFiles(ev.files)
			}
		}
		w.trace.logf("t=%d runner=%d cmd_exit lease=%d code=%d sig=%t", w.nowMS, r.id, ev.leaseID, ev.exitCode, ev.signaled)
		return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvCommandExited, LeaseID: ev.leaseID, ExitCode: ev.exitCode, Signaled: ev.signaled})

	case seReadReports:
		r := w.runners[ev.runnerIdx]
		if r.killed || r.exited {
			return nil
		}
		if r.paused {
			r.pausedQueue = append(r.pausedQueue, ev)
			return nil
		}
		files := cloneFiles(r.reportOnDisk)
		readErr := ev.readErr
		if readErr == nil && len(files) == 0 {
			readErr = errors.New("report file missing")
		}
		w.trace.logf("t=%d runner=%d reports_read lease=%d files=%d err=%t", w.nowMS, r.id, ev.leaseID, len(files), readErr != nil)
		return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvReportsRead, LeaseID: ev.leaseID, Files: files, ReadErr: readErr})

	case seDeliverReq:
		return w.handleServerRequest(ev)

	case seDeliverResp:
		resp := parseSimResponse(ev.req.Kind, ev.respStatus, ev.respBody, ev.respErr)
		if ev.statusIdx >= 0 {
			sc := w.statusClients[ev.statusIdx]
			if sc.exited {
				return nil
			}
			w.trace.logf("t=%d status=%d resp op=%d kind=%s st=%d err=%t", w.nowMS, sc.id, ev.opID, ev.req.Kind, ev.respStatus, resp.Err != nil)
			return w.stepStatus(sc, queuerunner.Event{Kind: queuerunner.EvResponse, OpID: ev.opID, Resp: resp})
		}
		r := w.runners[ev.runnerIdx]
		if r.killed || r.exited {
			return nil
		}
		if r.paused {
			r.pausedQueue = append(r.pausedQueue, ev)
			return nil
		}
		w.observeRunnerResponse(r, ev, resp)
		w.trace.logf("t=%d runner=%d resp op=%d kind=%s st=%d err=%t", w.nowMS, r.id, ev.opID, ev.req.Kind, ev.respStatus, resp.Err != nil)
		return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvResponse, OpID: ev.opID, Resp: resp})

	case seKill:
		r := w.runners[ev.runnerIdx]
		if r.killed || r.exited {
			return nil
		}
		r.killed, r.runningLeaseID = true, 0
		w.stats[stKill]++
		w.trace.logf("t=%d fault kill runner=%d", w.nowMS, r.id)
		if w.faults.runnerRestartSame && r.machineIdx != nil {
			idx := *r.machineIdx
			return w.addRunner(&idx, w.tests, w.cfg, false, w.nowMS+200)
		}

	case sePause:
		r := w.runners[ev.runnerIdx]
		if !r.killed && !r.exited && !r.paused {
			r.paused = true
			w.stats[stPause]++
			w.trace.logf("t=%d fault pause runner=%d", w.nowMS, r.id)
		}

	case seResume:
		r := w.runners[ev.runnerIdx]
		if !r.killed && !r.exited && r.paused {
			r.paused = false
			w.trace.logf("t=%d fault resume runner=%d q=%d", w.nowMS, r.id, len(r.pausedQueue))
			q := r.pausedQueue
			r.pausedQueue = nil
			for _, qev := range q {
				w.sched(w.nowMS, qev)
			}
		}

	case seRestart:
		return w.restartServer()

	case seClockJump:
		jump := int64(w.rng.IntN(4000)) - 1500
		if w.faults.shortDeadline {
			jump = w.cfg.BuildTimeoutMS + 1000
		}
		w.serverOffset += jump
		w.stats[stClockJump]++
		w.trace.logf("t=%d fault clock_jump delta=%d", w.nowMS, jump)

	case seLeaseHeldProbe:
		return w.runLeaseHeldProbe()

	case seLateConflict:
		idx := len(w.runners)
		return w.addRunner(&idx, append(slices.Clone(w.tests), "tests/zz_late_conflict.py"), w.cfg, true, w.nowMS)

	case seStartStatus:
		sc := w.statusClients[ev.statusIdx]
		w.trace.logf("t=%d status=%d start", w.nowMS, sc.id)
		return w.stepStatus(sc, queuerunner.Event{Kind: queuerunner.EvStart})

	case seHeal:
		w.faultsActive, w.reportOutageUntilMS = false, 0
		w.trace.logf("t=%d faults_healed", w.nowMS)
		aliveHealthy := false
		for _, r := range w.runners {
			if r.paused && !r.killed && !r.exited {
				w.sched(w.nowMS, &simEvent{kind: seResume, runnerIdx: r.id})
			}
			if !r.divergent && !r.killed && !r.exited {
				aliveHealthy = true
			}
		}
		if !w.disk.broken[w.buildID] && w.oracle.finishedVerdict == "" && !aliveHealthy {
			idx := len(w.runners)
			return w.addRunner(&idx, w.tests, w.cfg, false, w.nowMS+10)
		}
	}
	return nil
}

func (w *simWorld) restartServer() error {
	w.stats[stRestart]++
	w.srvEpoch++
	clear(w.disk.unsynced)
	var restored []*queue.State
	restartNow := w.serverClock()
	for _, bid := range w.disk.buildOrder {
		if w.disk.broken[bid] {
			continue
		}
		st, _, err := w.disk.replayBuild(bid, w.lim)
		if err != nil {
			return fmt.Errorf("replay build %s on restart: %w", bid, err)
		}
		if st.Initialized {
			st.ResetActiveLeaseDeadlines(restartNow)
			restored = append(restored, st)
		}
	}
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: simWriteTok, ReadToken: simReadTok, Clock: w.serverClock,
		Limits: w.lim, Log: w.disk, MaxBuilds: 16, MaxRunning: 8, RetentionMS: 3600_000,
		InitialBuilds: restored,
	})
	if err != nil {
		return err
	}
	w.handler = h
	w.trace.logf("t=%d fault restart epoch=%d restored=%d", w.nowMS, w.srvEpoch, len(restored))
	return nil
}

func (w *simWorld) runLeaseHeldProbe() error {
	for _, r := range w.runners {
		if r.killed || r.exited || r.runningLeaseID == 0 {
			continue
		}
		wr, err := queueapi.EncodeLease(w.buildID, queue.LeaseCommand{
			RunnerID: r.machine.RunnerID(), MachineIndex: r.machineIdx, RequestID: "probe-" + w.ent.Hex16()[:8],
		})
		if err != nil {
			return err
		}
		status, body := w.invokeHandler(wr, simWriteTok)
		if status == http.StatusConflict && strings.Contains(string(body), queue.ErrCodeLeaseHeld) {
			w.stats[stLeaseHeld]++
			w.trace.logf("t=%d state lease_held runner=%d", w.nowMS, r.id)
		}
		return nil
	}
	if w.faultsActive && w.nowMS+300 < w.faultEndMS {
		w.sched(w.nowMS+300, &simEvent{kind: seLeaseHeldProbe})
	}
	return nil
}

func (w *simWorld) stepRunner(r *simRunner, ev queuerunner.Event) error {
	for _, a := range r.machine.Step(w.nowMS+r.clockSkew, ev) {
		w.trace.logf("t=%d runner=%d act=%d op=%d lease=%d", w.nowMS, r.id, a.Kind, a.OpID, a.LeaseID)
		switch a.Kind {
		case queuerunner.ActSend:
			wr, err := encodeSimRequest(a.Req)
			if err != nil {
				return err
			}
			if a.Req.Kind == queuerunner.ReqReport {
				r.reportSentEpoch[a.OpID] = w.srvEpoch
			}
			if a.Req.Kind == queuerunner.ReqInit && r.divergent {
				w.stats[stDivInit]++
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
				w.stats[stSoloRetry]++
				w.trace.logf("t=%d state solo_retry runner=%d att=%s", w.nowMS, r.id, a.Attempts[0].AttemptID)
			}
			if err := w.startSimCommand(r, a.LeaseID, a.Attempts); err != nil {
				return err
			}

		case queuerunner.ActStopCommand:
			if r.runningLeaseID == a.LeaseID && !r.cmdStopped {
				r.cmdStopped = true
				w.oracle.markCommandStopped(a.LeaseID)
				w.sched(w.nowMS+20, &simEvent{kind: seCmdFinish, runnerIdx: r.id, leaseID: a.LeaseID, exitCode: -1, signaled: true})
			}

		case queuerunner.ActReadReports:
			if w.faults.reportOutage && w.faultsActive && w.reportOutageUntilMS == 0 {
				w.reportOutageUntilMS = w.nowMS + w.cfg.LeaseTTLMS + w.cfg.LeaseTTLMS/2
				w.stats[stReportOutage]++
				w.trace.logf("t=%d fault report_outage until=%d", w.nowMS, w.reportOutageUntilMS)
			}
			w.sched(w.nowMS+25+w.rng.Int64N(50), &simEvent{kind: seReadReports, runnerIdx: r.id, leaseID: a.LeaseID, readErr: w.oracle.plannedReadErr(a.LeaseID)})

		case queuerunner.ActDone:
			r.exited, r.verdict, r.exitErr = true, a.Verdict, a.Err
			var extraErr *queuerunner.ExtraFilesError
			if errors.As(a.Err, &extraErr) {
				w.stats[stExtraFile]++
			}
			w.trace.logf("t=%d runner=%d done verdict=%s err=%t", w.nowMS, r.id, a.Verdict, a.Err != nil)
			if err := w.oracle.onRunnerExit(r); err != nil {
				return err
			}
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
				w.stats[stStatusReport]++
			}
			w.trace.logf("t=%d status=%d done verdict=%s report=%t err=%t", w.nowMS, sc.id, a.Verdict, a.HasReport, a.Err != nil)
			if err := w.oracle.onStatusDone(w, a); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *simWorld) enqueueNetReq(runnerIdx, statusIdx int, opID uint64, req queuerunner.Request, wr queueapi.WireRequest, tok string) {
	delay := int64(5) + w.rng.Int64N(w.faults.netMaxDelayMS)
	if w.faultsActive && statusIdx < 0 && w.faults.reqDropPct > 0 && w.rng.IntN(100) < w.faults.reqDropPct &&
		!(w.faults.reportOutage && req.Kind == queuerunner.ReqHeartbeat) {
		w.stats[stReqDrop]++
		w.trace.logf("t=%d fault req_drop runner=%d op=%d kind=%s", w.nowMS, runnerIdx, opID, req.Kind)
		w.sched(w.nowMS+delay, &simEvent{kind: seDeliverResp, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, respErr: queueapi.NewTransportError("request dropped")})
		return
	}
	w.sched(w.nowMS+delay, &simEvent{kind: seDeliverReq, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, wire: wr, token: tok})
	if w.faultsActive && statusIdx < 0 && w.faults.reqDupPct > 0 && w.rng.IntN(100) < w.faults.reqDupPct {
		w.stats[stReqDup]++
		// Duplicate/reordered request can arrive up to 2*TTL later (modeling a
		// timed-out request that still reaches the server after the runner moved on).
		lateDelay := delay + 50 + w.rng.Int64N(w.cfg.LeaseTTLMS*2)
		w.trace.logf("t=%d fault req_dup runner=%d op=%d kind=%s delay=%d", w.nowMS, runnerIdx, opID, req.Kind, lateDelay)
		w.sched(w.nowMS+lateDelay, &simEvent{kind: seDeliverReq, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, wire: wr, token: tok, isDup: true})
	}
}

func (w *simWorld) handleServerRequest(ev *simEvent) error {
	respDelay := int64(5) + w.rng.Int64N(w.faults.netMaxDelayMS)
	replyErr := func(code int, body string) {
		w.sched(w.nowMS+respDelay, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respStatus: code, respBody: []byte(body)})
	}
	if w.reportOutageUntilMS > 0 && w.nowMS < w.reportOutageUntilMS && ev.req.Kind == queuerunner.ReqReport {
		replyErr(http.StatusServiceUnavailable, `{"version":1,"error":{"code":"storage_error","message":"simulated report outage"}}`)
		return nil
	}
	if w.faultsActive && ev.statusIdx < 0 {
		if w.faults.http503Pct > 0 && w.rng.IntN(100) < w.faults.http503Pct {
			replyErr(http.StatusServiceUnavailable, `{"version":1,"error":{"code":"storage_error","message":"simulated 503"}}`)
			return nil
		}
		if w.faults.http429Pct > 0 && w.rng.IntN(100) < w.faults.http429Pct {
			replyErr(http.StatusTooManyRequests, `{"version":1,"error":{"code":"rate_limited","message":"simulated 429"}}`)
			return nil
		}
	}

	triggerRestart := w.faults.serverRestart && w.faultsActive && ev.req.Kind == queuerunner.ReqReport && w.stats[stReportAcrossRestart] == 0 && w.srvEpoch == 0
	beforeN := len(w.disk.durable[w.buildID])
	status, body := w.invokeHandler(ev.wire, ev.token)

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

	if triggerRestart && status == http.StatusOK {
		if err := w.restartServer(); err != nil {
			return err
		}
		w.stats[stRespDrop]++
		w.sched(w.nowMS+respDelay, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respErr: queueapi.NewTransportError("reset by restart")})
		return nil
	}
	if w.faultsActive && w.faults.runnerPause && ev.statusIdx < 0 && !ev.isDup && ev.req.Kind == queuerunner.ReqLease &&
		status == http.StatusOK && w.stats[stStaleRequest] == 0 && strings.Contains(string(body), `"status":"leased"`) {
		w.stats[stRespDrop]++
		w.trace.logf("t=%d fault drop_lease_past_ttl runner=%d op=%d", w.nowMS, ev.runnerIdx, ev.opID)
		w.sched(w.nowMS+w.cfg.LeaseTTLMS+400, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respErr: queueapi.NewTransportError("lease reply lost past TTL")})
		return nil
	}
	if w.faultsActive && ev.statusIdx < 0 && !ev.isDup && w.faults.respDropPct > 0 && w.rng.IntN(100) < w.faults.respDropPct &&
		!(w.faults.reportOutage && ev.req.Kind == queuerunner.ReqHeartbeat) {
		w.stats[stRespDrop]++
		w.trace.logf("t=%d fault resp_drop runner=%d op=%d kind=%s", w.nowMS, ev.runnerIdx, ev.opID, ev.req.Kind)
		w.sched(w.nowMS+respDelay, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respErr: queueapi.NewTransportError("response dropped")})
		return nil
	}

	w.sched(w.nowMS+respDelay, &simEvent{kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req, respStatus: status, respBody: body})
	if w.faultsActive && ev.statusIdx < 0 && w.faults.respDupPct > 0 && w.rng.IntN(100) < w.faults.respDupPct {
		w.stats[stRespDup]++
		w.trace.logf("t=%d fault resp_dup runner=%d op=%d kind=%s", w.nowMS, ev.runnerIdx, ev.opID, ev.req.Kind)
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
	if resp.Err != nil {
		var apiErr *queueapi.APIError
		if errors.As(resp.Err, &apiErr) && apiErr.HTTPStatus == 409 && apiErr.Code == queue.ErrCodeStaleRequest {
			w.stats[stStaleRequest]++
		}
		return
	}
	if ev.req.Kind == queuerunner.ReqReport && resp.Report != nil {
		if sentEpoch, ok := r.reportSentEpoch[ev.opID]; (ok && w.srvEpoch > sentEpoch) || (w.srvEpoch > 0 && w.oracle.reportRetryAfterRestart(ev.req.Report.LeaseID)) {
			w.stats[stReportAcrossRestart]++
		}
		for _, d := range resp.Report.Results {
			if d.Disposition == queue.DispositionStale {
				w.stats[stStaleResult]++
			}
			if d.Disposition == queue.DispositionDuplicate {
				w.stats[stDupReport]++
			}
		}
	}
}

func (w *simWorld) startSimCommand(r *simRunner, leaseID int64, attempts []queue.LeasedAttemptView) error {
	durMS := int64(150 + w.rng.IntN(700))
	exitCode, signaled := 0, false
	var files map[string]junit.File
	var readErr error

	hasPoison := false
	for _, a := range attempts {
		if a.TestIndex == w.faults.poisonIdx {
			hasPoison = true
			break
		}
	}
	switch {
	case hasPoison:
		exitCode, signaled = 137, true
	case w.faultsActive && w.faults.cmdHang && w.rng.IntN(4) == 0:
		durMS, files = w.cfg.LeaseTTLMS*2+500, passAllFiles(attempts, w.rng)
	case w.faultsActive && w.faults.cmdExtraFile && w.rng.IntN(5) == 0:
		files = passAllFiles(attempts, w.rng)
		files["tests/unassigned_extra.py"] = junit.File{Seconds: big.NewRat(50, 1000), Cases: 1}
	case w.faultsActive && w.faults.cmdTruncated && w.rng.IntN(5) == 0:
		readErr = errors.New("unexpected EOF in JUnit XML")
	case w.faultsActive && w.faults.cmdNoReport && w.rng.IntN(5) == 0:
		// Exit 0 without writing a report (catches missing ClearReports).
	case w.faultsActive && w.faults.cmdCrash && w.rng.IntN(5) == 0:
		exitCode, signaled = 137, true
	case w.faultsActive && w.faults.cmdMixedSibling && len(attempts) >= 2 && w.rng.IntN(3) == 0:
		exitCode = 1
		files = make(map[string]junit.File, len(attempts))
		skipSecond := w.rng.IntN(2) == 0
		for i, a := range attempts {
			switch {
			case i == 0:
				files[a.Test] = junit.File{Seconds: big.NewRat(80, 1000), Cases: 1, Failed: true}
			case i == 1 && !skipSecond:
				files[a.Test] = junit.File{Seconds: big.NewRat(120, 1000), Cases: 2}
			default:
				files[a.Test] = junit.File{Seconds: big.NewRat(0, 1000), Cases: 2, Skipped: 2}
			}
		}
	case w.faultsActive && w.faults.cmdFail && w.rng.IntN(5) == 0:
		exitCode = 1
		files = make(map[string]junit.File, len(attempts))
		for i, a := range attempts {
			files[a.Test] = junit.File{Seconds: big.NewRat(90, 1000), Cases: 1, Failed: i == 0}
		}
	case w.faults.cmdSkip && w.rng.IntN(4) == 0:
		files = make(map[string]junit.File, len(attempts))
		for i, a := range attempts {
			if i == 0 {
				files[a.Test] = junit.File{Seconds: big.NewRat(0, 1000), Cases: 2, Skipped: 2}
			} else {
				files[a.Test] = junit.File{Seconds: big.NewRat(int64(40+w.rng.IntN(300)), 1000), Cases: 2, Skipped: 1}
			}
		}
	default:
		files = passAllFiles(attempts, w.rng)
	}

	if err := w.oracle.recordCommandStart(leaseID, r.id, r.format, attempts, readErr, r.reportOnDisk != nil); err != nil {
		return err
	}
	w.sched(w.nowMS+durMS, &simEvent{kind: seCmdFinish, runnerIdx: r.id, leaseID: leaseID, exitCode: exitCode, signaled: signaled, files: files, readErr: readErr})
	return nil
}

func passAllFiles(attempts []queue.LeasedAttemptView, rng *rand.Rand) map[string]junit.File {
	out := make(map[string]junit.File, len(attempts))
	for _, a := range attempts {
		out[a.Test] = junit.File{Seconds: big.NewRat(int64(50+rng.IntN(400)), 1000), Cases: 1}
	}
	return out
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
	default:
		return queueapi.WireRequest{}, fmt.Errorf("unknown request kind %q", req.Kind)
	}
}

func parseSimResponse(kind queuerunner.RequestKind, status int, body []byte, transportErr error) queuerunner.Response {
	if transportErr != nil {
		return queuerunner.Response{Kind: kind, Err: transportErr}
	}
	switch kind {
	case queuerunner.ReqInit:
		r, err := queueapi.ParseInitResponse(status, body)
		return queuerunner.Response{Kind: kind, Init: &r, Err: err}
	case queuerunner.ReqLease:
		r, err := queueapi.ParseLeaseResponse(status, body)
		return queuerunner.Response{Kind: kind, Lease: &r, Err: err}
	case queuerunner.ReqHeartbeat:
		r, err := queueapi.ParseHeartbeatResponse(status, body)
		return queuerunner.Response{Kind: kind, Heartbeat: &r, Err: err}
	case queuerunner.ReqReport:
		r, err := queueapi.ParseReportResponse(status, body)
		return queuerunner.Response{Kind: kind, Report: &r, Err: err}
	case queuerunner.ReqStatus:
		r, err := queueapi.ParseStatusResponse(status, body)
		return queuerunner.Response{Kind: kind, Status: &r, Err: err}
	case queuerunner.ReqListTests:
		r, err := queueapi.ParseListTestsResponse(status, body)
		return queuerunner.Response{Kind: kind, ListTests: &r, Err: err}
	default:
		return queuerunner.Response{Kind: kind, Err: fmt.Errorf("unknown kind %q", kind)}
	}
}

func (w *simWorld) queryServerStatus() (queueapi.StatusResponse, int, error) {
	status, body := w.invokeHandler(queueapi.EncodeStatus(w.buildID), simReadTok)
	if status != http.StatusOK {
		return queueapi.StatusResponse{}, status, nil
	}
	resp, err := queueapi.ParseStatusResponse(status, body)
	return resp, status, err
}

func (w *simWorld) queryAllServerTests() ([]queue.TestView, error) {
	var all []queue.TestView
	for cursor := 0; ; {
		status, body := w.invokeHandler(queueapi.EncodeListTests(w.buildID, queue.ListTestsCommand{Cursor: cursor, Limit: 200}), simReadTok)
		if status != http.StatusOK {
			return nil, fmt.Errorf("list tests status %d: %s", status, body)
		}
		var resp queueapi.ListTestsResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.Tests...)
		if resp.NextCursor == nil {
			return all, nil
		}
		if *resp.NextCursor <= cursor {
			return nil, fmt.Errorf("cursor did not advance: %d -> %d", cursor, *resp.NextCursor)
		}
		cursor = *resp.NextCursor
	}
}
