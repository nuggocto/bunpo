package sim_test

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"math/big"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
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

var (
	errSimCrash = errors.New("simulated crash mid-syscall")
	statNames   = []string{
		"reqDrop", "respDrop", "reqDup", "respDup", "http503", "http429",
		"restarts", "midAppendCrash", "procKill", "loadReadErr", "tornTrunc",
		"clockJump", "diskUnwritten", "diskUncertain", "kills", "pauses",
		"divInit", "reportOutage", "staleResult", "staleRequest", "leaseHeld",
		"soloRetry", "exhausted", "deadline", "conflictAfterPassed",
		"reportAcrossRestart", "broken", "dupReport", "extraFile",
		"statusReport", "passed", "failed", "unresolved",
	}
)

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

// simFS implements queueapi.LogFS with separate pending (page cache) vs durable
// (disk) file and directory state, partial writes, write/read/truncate/sync
// errors, mid-op process kills (page cache survives), and power-loss crashes.
type simFS struct {
	rng            *rand.Rand
	trace          *traceRecorder
	stats          simStats
	faultsActive   *bool
	faults         *faultProfile
	suppressFaults bool
	crashed        bool
	killNextFsync  bool
	inLoad         bool
	durableDir     map[string]bool
	pendingDir     map[string]bool
	durableFiles   map[string][]byte
	pendingFiles   map[string][]byte
}

func newSimFS(rng *rand.Rand, tr *traceRecorder, st simStats, active *bool, fp *faultProfile) *simFS {
	return &simFS{
		rng:          rng,
		trace:        tr,
		stats:        st,
		faultsActive: active,
		faults:       fp,
		durableDir:   make(map[string]bool),
		pendingDir:   make(map[string]bool),
		durableFiles: make(map[string][]byte),
		pendingFiles: make(map[string][]byte),
	}
}

func (fs *simFS) hit(pct int) bool {
	return *fs.faultsActive && !fs.suppressFaults && !fs.crashed && pct > 0 && fs.rng.IntN(100) < pct
}

func (fs *simFS) triggerCrash(op string, powerLoss bool) error {
	fs.crashed = true
	fs.stats["midAppendCrash"]++
	if !powerLoss {
		fs.stats["procKill"]++
		fs.trace.logf("fault mid_op_proc_kill op=%s", op)
		return errSimCrash
	}
	fs.trace.logf("fault mid_op_power_loss op=%s", op)
	fs.crash()
	return errSimCrash
}

func (fs *simFS) preOp(op string) error {
	if fs.crashed {
		return errSimCrash
	}
	if !fs.hit(fs.faults.crashMidAppendPct) {
		return nil
	}
	// Model both power-loss crashes and process kills (page cache intact).
	powerLoss := fs.rng.IntN(3) != 0
	return fs.triggerCrash(op, powerLoss)
}

// crash simulates power loss: unsynced directory entries revert to durableDir,
// and unsynced file bytes are dropped or kept as a torn prefix.
func (fs *simFS) crash() {
	fs.pendingDir = maps.Clone(fs.durableDir)
	for name := range fs.pendingFiles {
		if !fs.durableDir[name] {
			delete(fs.pendingFiles, name)
			delete(fs.durableFiles, name)
		}
	}
	for name := range fs.durableDir {
		dur, pend := fs.durableFiles[name], fs.pendingFiles[name]
		if bytes.Equal(dur, pend) {
			continue
		}
		if len(pend) > len(dur) && fs.rng.IntN(2) == 0 {
			extra := 1 + fs.rng.IntN(len(pend)-len(dur))
			fs.pendingFiles[name] = bytes.Clone(pend[:len(dur)+extra])
		} else {
			fs.pendingFiles[name] = bytes.Clone(dur)
		}
		fs.durableFiles[name] = bytes.Clone(fs.pendingFiles[name])
	}
}

func (fs *simFS) OpenFile(name string, flag int, _ os.FileMode) (queueapi.LogFile, error) {
	if fs.crashed {
		return nil, errSimCrash
	}
	if flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL {
		if fs.pendingDir[name] {
			return nil, os.ErrExist
		}
		if err := fs.preOp("create_before"); err != nil {
			return nil, err
		}
		fs.pendingDir[name] = true
		fs.pendingFiles[name] = nil
		if err := fs.preOp("create_after"); err != nil {
			return nil, err
		}
		return &simFile{fs: fs, name: name}, nil
	}
	if !fs.pendingDir[name] {
		return nil, os.ErrNotExist
	}
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		if err := fs.preOp("open_write"); err != nil {
			return nil, err
		}
	}
	return &simFile{fs: fs, name: name}, nil
}

func (fs *simFS) ReadDir() ([]string, error) {
	out := make([]string, 0, len(fs.pendingDir))
	for k := range fs.pendingDir {
		out = append(out, k)
	}
	slices.Sort(out)
	return out, nil
}

func (fs *simFS) Remove(name string) error {
	if fs.crashed {
		return errSimCrash
	}
	if !fs.pendingDir[name] {
		return os.ErrNotExist
	}
	if err := fs.preOp("remove"); err != nil {
		return err
	}
	delete(fs.pendingDir, name)
	delete(fs.pendingFiles, name)
	return nil
}

func (fs *simFS) SyncDir() error {
	if fs.crashed {
		return errSimCrash
	}
	if !fs.inLoad && fs.hit(fs.faults.diskUncertainPct) {
		if fs.rng.IntN(2) == 0 {
			fs.commitDir()
		}
		fs.stats["diskUncertain"]++
		return errors.New("simulated syncdir EIO")
	}
	if err := fs.preOp("syncdir"); err != nil {
		return err
	}
	fs.commitDir()
	return nil
}

func (fs *simFS) commitDir() {
	fs.durableDir = maps.Clone(fs.pendingDir)
	for name := range fs.durableFiles {
		if !fs.durableDir[name] {
			delete(fs.durableFiles, name)
			delete(fs.pendingFiles, name)
		}
	}
}

type simFile struct {
	fs     *simFS
	name   string
	pos    int
	closed bool
}

func (f *simFile) Close() error {
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	return nil
}

func (f *simFile) StatSize() (int64, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	return int64(len(f.fs.pendingFiles[f.name])), nil
}

func (f *simFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.fs.inLoad && f.fs.faults.loadReadErrPct > 0 {
		// Return small chunks when loadReadErrPct is active so EIO can occur
		// after the header (inside a record or Peek), not only on the first read.
		if len(p) > 128 {
			p = p[:128]
		}
		if f.fs.hit(f.fs.faults.loadReadErrPct) {
			f.fs.stats["loadReadErr"]++
			return 0, errors.New("simulated EIO on read")
		}
	}
	data := f.fs.pendingFiles[f.name]
	if f.pos >= len(data) {
		return 0, io.EOF
	}
	n := copy(p, data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *simFile) Write(p []byte) (int, error) {
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.fs.crashed {
		return 0, errSimCrash
	}
	if f.fs.hit(f.fs.faults.crashMidAppendPct) {
		if len(p) > 0 {
			k := f.fs.rng.IntN(len(p) + 1)
			f.fs.pendingFiles[f.name] = append(f.fs.pendingFiles[f.name], p[:k]...)
		}
		return 0, f.fs.triggerCrash("write", true)
	}
	if f.fs.hit(f.fs.faults.diskUnwrittenPct) {
		k := 1
		if len(p) > 2 {
			k = 1 + f.fs.rng.IntN(len(p)-1)
		}
		f.fs.pendingFiles[f.name] = append(f.fs.pendingFiles[f.name], p[:k]...)
		f.fs.stats["diskUnwritten"]++
		return k, errors.New("simulated ENOSPC mid-write")
	}
	f.fs.pendingFiles[f.name] = append(f.fs.pendingFiles[f.name], p...)
	f.fs.trace.hashBytes("disk", p)
	return len(p), nil
}

func (f *simFile) Truncate(size int64) error {
	if f.closed {
		return os.ErrClosed
	}
	if f.fs.crashed {
		return errSimCrash
	}
	if !f.fs.inLoad && f.fs.hit(f.fs.faults.diskUncertainPct) {
		f.fs.stats["diskUncertain"]++
		return errors.New("simulated truncate EIO")
	}
	if err := f.fs.preOp("truncate"); err != nil {
		return err
	}
	if cur := f.fs.pendingFiles[f.name]; int(size) < len(cur) {
		f.fs.pendingFiles[f.name] = bytes.Clone(cur[:size])
	}
	return nil
}

func (f *simFile) Sync() error {
	if f.closed {
		return os.ErrClosed
	}
	if f.fs.crashed {
		return errSimCrash
	}
	if f.fs.killNextFsync && !f.fs.inLoad {
		f.fs.killNextFsync = false
		return f.fs.triggerCrash("fsync", false)
	}
	if !f.fs.inLoad && f.fs.hit(f.fs.faults.diskUncertainPct) {
		if f.fs.rng.IntN(2) == 0 {
			f.fs.durableFiles[f.name] = bytes.Clone(f.fs.pendingFiles[f.name])
		}
		f.fs.stats["diskUncertain"]++
		return errors.New("simulated fsync EIO")
	}
	if err := f.fs.preOp("fsync"); err != nil {
		return err
	}
	f.fs.durableFiles[f.name] = bytes.Clone(f.fs.pendingFiles[f.name])
	return nil
}

func (fs *simFS) injectBitrot(name string) bool {
	data := fs.durableFiles[name]
	idx := bytes.Index(data, []byte(`"deadline_ms":`))
	if idx < 0 {
		return false
	}
	pos := idx + len(`"deadline_ms":`)
	for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
		pos++
	}
	pos--
	if pos < idx+len(`"deadline_ms":`) {
		return false
	}
	cp := bytes.Clone(data)
	if cp[pos] == '9' {
		cp[pos] = '8'
	} else {
		cp[pos]++
	}
	fs.durableFiles[name] = cp
	fs.pendingFiles[name] = bytes.Clone(cp)
	return true
}

func parseDiskEventsSlice(data []byte) []queue.Event {
	var out []queue.Event
	for len(data) > 0 {
		nl := bytes.IndexByte(data, '\n')
		if nl < 0 {
			break
		}
		line := data[:nl]
		data = data[nl+1:]
		if len(line) == 0 || bytes.Equal(line, []byte("bunpo-queue-log 1")) {
			continue
		}
		var evs []queue.Event
		if len(line) < 10 || line[8] != ' ' || json.Unmarshal(line[9:], &evs) != nil {
			break
		}
		out = append(out, evs...)
	}
	return out
}

type faultProfile struct {
	reqDropPct            int
	respDropPct           int
	reqDupPct             int
	respDupPct            int
	http503Pct            int
	http429Pct            int
	diskUnwrittenPct      int
	diskUncertainPct      int
	crashMidAppendPct     int
	loadReadErrPct        int
	poisonIdx             int
	netMaxDelayMS         int64
	serverRestart         bool
	restartDowntime       bool
	procKillThenPowerLoss bool
	serverClockJump       bool
	clientSkew            bool
	clockBackward         bool
	runnerKill            bool
	runnerPause           bool
	runnerRestartSame     bool
	lateJoiner            bool
	divergentInit         bool
	conflictAfterPassed   bool
	shortDeadline         bool
	reportOutage          bool
	expectCleanPass       bool
	injectBitrot          bool
	checkRetentionEvict   bool
	cmdFail               bool
	cmdSkip               bool
	cmdCrash              bool
	cmdHang               bool
	cmdNoReport           bool
	cmdOldReport          bool
	cmdTruncated          bool
	cmdExtraFile          bool
	cmdMixedSibling       bool
	cmdSigSibling         bool
	cmdNonZeroClean       bool
	useLines              bool
}

func drawRandomProfile(rng *rand.Rand, numTests int) faultProfile {
	pct := func(on bool, base, span int) int {
		if !on {
			return 0
		}
		return base + rng.IntN(span)
	}
	p := faultProfile{
		poisonIdx:           -1,
		netMaxDelayMS:       10 + rng.Int64N(350),
		reqDropPct:          pct(rng.IntN(3) != 0, 2, 16),
		respDropPct:         pct(rng.IntN(3) != 0, 2, 16),
		reqDupPct:           pct(rng.IntN(2) == 0, 4, 16),
		respDupPct:          pct(rng.IntN(3) != 0, 3, 15),
		http503Pct:          pct(rng.IntN(3) == 0, 3, 12),
		http429Pct:          pct(rng.IntN(3) == 0, 3, 10),
		diskUnwrittenPct:    pct(rng.IntN(3) == 0, 3, 12),
		diskUncertainPct:    pct(rng.IntN(14) == 0, 2, 6),
		crashMidAppendPct:   pct(rng.IntN(6) == 0, 4, 12),
		loadReadErrPct:      pct(rng.IntN(18) == 0, 5, 15),
		serverRestart:       rng.IntN(3) == 0,
		serverClockJump:     rng.IntN(3) == 0,
		clientSkew:          rng.IntN(2) == 0,
		runnerKill:          rng.IntN(2) == 0,
		runnerPause:         rng.IntN(2) == 0,
		runnerRestartSame:   rng.IntN(3) == 0,
		lateJoiner:          rng.IntN(2) == 0,
		divergentInit:       rng.IntN(4) == 0,
		conflictAfterPassed: rng.IntN(5) == 0,
		shortDeadline:       rng.IntN(10) == 0,
		reportOutage:        rng.IntN(6) == 0,
		cmdFail:             rng.IntN(3) == 0,
		cmdSkip:             rng.IntN(2) == 0,
		cmdCrash:            rng.IntN(2) == 0,
		cmdHang:             rng.IntN(3) == 0,
		cmdNoReport:         rng.IntN(3) == 0,
		cmdOldReport:        rng.IntN(3) == 0,
		cmdTruncated:        rng.IntN(3) == 0,
		cmdExtraFile:        rng.IntN(4) == 0,
		cmdMixedSibling:     rng.IntN(2) == 0,
		cmdSigSibling:       rng.IntN(3) == 0,
		cmdNonZeroClean:     rng.IntN(3) == 0,
		useLines:            rng.IntN(4) == 0,
	}
	p.clockBackward = p.serverClockJump && rng.IntN(2) == 0
	if rng.IntN(4) == 0 && numTests > 0 {
		p.poisonIdx = rng.IntN(numTests)
	}
	return p
}

const (
	seRunnerStart = iota + 1
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
	seLateConflict
	seStartStatus
	seHeal
)

type simEvent struct {
	atMS       int64
	leaseID    int64
	seq        uint64
	opID       uint64
	timerGen   uint64
	kind       int
	runnerIdx  int
	statusIdx  int
	respStatus int
	exitCode   int
	req        queuerunner.Request
	wire       queueapi.WireRequest
	token      string
	respBody   []byte
	respErr    error
	readErr    error
	timerKind  queuerunner.TimerKind
	signaled   bool
	isDup      bool
	files      map[string]junit.File
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
	id              int
	machineIdx      *int
	format          string
	machine         *queuerunner.Machine
	clockSkew       int64
	runningLeaseID  int64
	startedAtMS     int64
	firstFailMS     int64
	killed          bool
	paused          bool
	exited          bool
	divergent       bool
	cmdStopped      bool
	verdict         queue.Verdict
	exitErr         error
	reportOnDisk    map[string]junit.File
	inFlightFG      uint64
	inFlightHB      uint64
	lastOpID        uint64
	pausedQueue     []*simEvent
	reorderedLease  *simEvent
	reportSentEpoch map[uint64]int
	sentVerdicts    map[uint64]queue.Verdict
}

type simStatusClient struct {
	id           int
	machine      *queuerunner.StatusMachine
	exited       bool
	firstFailMS  int64
	lastOpID     uint64
	sentVerdicts map[uint64]queue.Verdict
	uninit404    map[uint64]bool
}

type simWorld struct {
	seed                int64
	nowMS               int64
	serverOffset        int64
	faultEndMS          int64
	maxEndMS            int64
	reportOutageUntilMS int64
	rng                 *rand.Rand
	ent                 *simEntropy
	trace               *traceRecorder
	stats               simStats
	faults              faultProfile
	faultsActive        bool
	buildBroken         bool
	expectDamaged       bool
	readErrDamaged      bool
	procKillStage       int // 0=arm on first report, 1=restarted after proc kill, 2=power loss done
	nextSeq             uint64
	pq                  eventHeap
	buildID             string
	buildFile           string
	lim                 queue.Limits
	cfg                 queue.Config
	tests               []string
	preds               []*int64
	fs                  *simFS
	diskLog             *queueapi.DiskLog
	handler             *queueapi.Handler
	srvEpoch            int
	runners             []*simRunner
	statusClients       []*simStatusClient
	oracle              *simOracle
	exporter            *simExporter
}

func newSimWorld(seed int64, override *faultProfile) (*simWorld, error) {
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
	numTests := 4 + rng.IntN(9)
	faults := drawRandomProfile(rng, numTests)
	if override != nil {
		faults = *override
	}
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
	buildTimeout := int64(180_000)
	if faults.expectCleanPass {
		maxAttempts = 1
		ttlMS = 3000
	}
	if faults.shortDeadline {
		buildTimeout = ttlMS * 3
	}
	numMachines := 2 + rng.IntN(3)
	maxBatch := 1 + rng.IntN(3)
	if faults.cmdMixedSibling || faults.cmdSigSibling || faults.cmdNonZeroClean {
		maxBatch = max(maxBatch, 2)
	}
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 1000
	bid := fmt.Sprintf("sim-%d", seed)

	w := &simWorld{
		seed:         seed,
		rng:          rng,
		ent:          &simEntropy{rng: rng},
		trace:        &traceRecorder{h: sha256.New()},
		stats:        make(simStats),
		faults:       faults,
		nowMS:        1_000_000,
		faultsActive: true,
		faultEndMS:   1_025_000,
		maxEndMS:     1_400_000,
		buildID:      bid,
		buildFile:    queueapi.BuildFileName(bid),
		lim:          lim,
		cfg: queue.Config{
			Machines:       numMachines,
			LeaseTTLMS:     ttlMS,
			BuildTimeoutMS: buildTimeout,
			MaxAttempts:    maxAttempts,
			MaxBatch:       maxBatch,
		},
		tests: tests,
		preds: preds,
	}
	if faults.shortDeadline {
		w.faultEndMS = w.nowMS + buildTimeout + ttlMS
	}
	w.fs = newSimFS(rng, w.trace, w.stats, &w.faultsActive, &w.faults)
	w.diskLog = queueapi.NewDiskLog(w.fs, nil)
	if err := w.resetHandler(nil, nil); err != nil {
		return nil, err
	}
	w.oracle = newSimOracle(w.buildID, lim, faults.expectCleanPass)

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
		w.sched(w.nowMS+800+rng.Int64N(2000), &simEvent{kind: seClockJump})
	}
	if len(w.runners) > 1 && faults.runnerKill {
		w.sched(w.nowMS+800+rng.Int64N(2000), &simEvent{kind: seKill, runnerIdx: 0})
	}
	if len(w.runners) > 1 && faults.runnerPause {
		victim := 0
		if faults.runnerKill {
			victim = 1
		}
		pauseAt := w.nowMS + 600 + rng.Int64N(1000)
		w.sched(pauseAt, &simEvent{kind: sePause, runnerIdx: victim})
		w.sched(pauseAt+ttlMS+1500, &simEvent{kind: seResume, runnerIdx: victim})
	}
	w.addStatusClient(w.nowMS + 1500)
	w.sched(w.faultEndMS, &simEvent{kind: seHeal})
	return w, nil
}

func (w *simWorld) resetHandler(restored []*queue.State, damaged []string) error {
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken:     simWriteTok,
		ReadToken:      simReadTok,
		Clock:          w.serverClock,
		Limits:         w.lim,
		Log:            w.diskLog,
		MaxBuilds:      16,
		MaxRunning:     8,
		RetentionMS:    3600_000,
		InitialBuilds:  restored,
		InitialDamaged: damaged,
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
	format := "junit"
	if w.faults.useLines {
		format = "lines"
	}
	preds := w.preds
	if len(tests) != len(preds) {
		preds = make([]*int64, len(tests))
	}
	m, err := queuerunner.NewMachine(queuerunner.WorkerConfig{
		BuildID:       w.buildID,
		MachineIndex:  machineIdx,
		Config:        cfg,
		Tests:         tests,
		PredictionsMS: preds,
		Format:        format,
		Retry:         queuerunner.RetryConfig{BudgetMS: 60_000, BaseMS: 100, MaxWaitMS: 1000},
		Entropy:       w.ent,
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
		id:              id,
		machineIdx:      machineIdx,
		format:          format,
		machine:         m,
		clockSkew:       skew,
		reportOnDisk:    preDisk,
		reportSentEpoch: make(map[uint64]int),
		sentVerdicts:    make(map[uint64]queue.Verdict),
		divergent:       divergent,
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
	w.statusClients = append(w.statusClients, &simStatusClient{
		id:           id,
		machine:      sm,
		sentVerdicts: make(map[uint64]queue.Verdict),
		uninit404:    make(map[uint64]bool),
	})
	w.sched(startAt, &simEvent{kind: seStartStatus, statusIdx: id})
}

func (w *simWorld) run() error {
	conflictScheduled := false
	finalStatusScheduled := false
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
		if !w.faultsActive && !finalStatusScheduled && !w.buildBroken && w.oracle.finishedVerdict != "" {
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
				ex.completed = true
				ex.exitCode = ev.exitCode
				ex.signaled = ev.signaled
				ex.filesWritten = cloneFiles(ev.files)
				ex.readErr = ev.readErr
			}
			if ev.files != nil {
				r.reportOnDisk = cloneFiles(ev.files)
			}
		}
		w.trace.logf("t=%d runner=%d cmd_exit lease=%d code=%d sig=%t", w.nowMS, r.id, ev.leaseID, ev.exitCode, ev.signaled)
		return w.stepRunner(r, queuerunner.Event{Kind: queuerunner.EvCommandExited, LeaseID: ev.leaseID, ExitCode: ev.exitCode, Signaled: ev.signaled})
	case seReadReports:
		if r := w.activeRunnerFor(ev); r != nil {
			files := cloneFiles(r.reportOnDisk)
			readErr := ev.readErr
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
			if resp.Err == nil {
				sc.firstFailMS = 0
				if resp.Status != nil {
					sc.sentVerdicts[ev.opID] = resp.Status.Verdict
				} else if resp.ListTests != nil {
					sc.sentVerdicts[ev.opID] = resp.ListTests.Verdict
				}
			} else if sc.firstFailMS == 0 && queueapi.IsTransientError(resp.Err) {
				sc.firstFailMS = w.nowMS
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
			r.killed = true
			r.runningLeaseID = 0
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
		if w.faults.restartDowntime {
			w.serverOffset += w.cfg.LeaseTTLMS + 500
		}
		return w.restartServer(true)
	case seClockJump:
		jump := int64(w.rng.IntN(4000)) - 2000
		if w.faults.shortDeadline {
			jump = w.cfg.BuildTimeoutMS + 1000
		} else if w.faults.clockBackward {
			jump = -(1500 + w.rng.Int64N(3500))
		}
		w.serverOffset += jump
		w.stats["clockJump"]++
	case seLateConflict:
		idx := len(w.runners)
		return w.addRunner(&idx, append(slices.Clone(w.tests), "tests/zz_late_conflict.py"), w.cfg, true, w.nowMS)
	case seStartStatus:
		return w.stepStatus(w.statusClients[ev.statusIdx], queuerunner.Event{Kind: queuerunner.EvStart})
	case seHeal:
		w.faultsActive = false
		w.reportOutageUntilMS = 0
		w.trace.logf("t=%d faults_healed", w.nowMS)
		for _, r := range w.runners {
			if r.paused && !r.killed && !r.exited {
				w.sched(w.nowMS, &simEvent{kind: seResume, runnerIdx: r.id})
			}
		}
		if !w.buildBroken && w.oracle.finishedVerdict == "" {
			return w.ensureHealthyRunner()
		}
	}
	return nil
}

func (w *simWorld) ensureHealthyRunner() error {
	if slices.ContainsFunc(w.runners, func(r *simRunner) bool { return !r.divergent && !r.killed && !r.exited }) {
		return nil
	}
	tests := w.tests
	if len(w.oracle.tests) > 0 {
		tests = w.oracle.tests
	}
	idx := len(w.runners)
	return w.addRunner(&idx, tests, w.cfg, false, w.nowMS+10)
}

func (w *simWorld) restartServer(powerLoss bool) error {
	w.stats["restarts"]++
	w.srvEpoch++
	w.buildBroken = false
	w.readErrDamaged = false
	w.fs.crashed = false
	if powerLoss {
		w.fs.crash()
	}

	hadFile := w.fs.pendingDir[w.buildFile]
	beforeFile := bytes.Clone(w.fs.pendingFiles[w.buildFile])
	readErrBefore := w.stats["loadReadErr"]

	w.diskLog = queueapi.NewDiskLog(w.fs, nil)
	w.fs.inLoad = true
	loaded, err := w.diskLog.Load(w.lim)
	w.fs.inLoad = false

	// If the simulated server crashed mid-Load (e.g., during truncate/remove/fsync
	// or SyncDir), reboot cleanly to verify recovery from an interrupted Load.
	if w.fs.crashed || err != nil {
		w.fs.crashed = false
		prevSuppress := w.fs.suppressFaults
		w.fs.suppressFaults = true
		w.diskLog = queueapi.NewDiskLog(w.fs, nil)
		w.fs.inLoad = true
		loaded, err = w.diskLog.Load(w.lim)
		w.fs.inLoad = false
		w.fs.suppressFaults = prevSuppress
		if err != nil {
			return fmt.Errorf("DiskLog.Load after interrupted load: %w", err)
		}
	} else if w.stats["loadReadErr"] > readErrBefore {
		// A read error during Load must NEVER delete or truncate the build log.
		if w.fs.pendingDir[w.buildFile] != hadFile || !bytes.Equal(w.fs.pendingFiles[w.buildFile], beforeFile) {
			return fmt.Errorf("oracle: build log modified/deleted on read error during Load (had=%t now=%t before=%d after=%d)",
				hadFile, w.fs.pendingDir[w.buildFile], len(beforeFile), len(w.fs.pendingFiles[w.buildFile]))
		}
		if hadFile && !slices.Contains(loaded.Damaged, w.buildFile) {
			return fmt.Errorf("oracle: build log with read error during Load was not marked damaged")
		}
		w.readErrDamaged = true
	}

	w.stats["tornTrunc"] += loaded.Truncated
	w.buildBroken = slices.Contains(loaded.Damaged, w.buildFile)

	prevSuppress := w.fs.suppressFaults
	w.fs.suppressFaults = true
	restartNow := w.serverClock()
	preRestartDisk := bytes.Clone(w.fs.pendingFiles[w.buildFile])
	hErr := w.resetHandler(loaded.Builds, loaded.Damaged)
	w.fs.suppressFaults = prevSuppress
	if hErr != nil {
		return hErr
	}
	if err := w.exporter.recordRestart(restartNow, w.buildBroken, preRestartDisk); err != nil {
		return err
	}
	if err := w.oracle.verifyAfterRestart(w, loaded); err != nil {
		return err
	}
	w.trace.logf("t=%d fault restart epoch=%d powerLoss=%t restored=%d damaged=%d trunc=%d",
		w.nowMS, w.srvEpoch, powerLoss, len(loaded.Builds), len(loaded.Damaged), loaded.Truncated)
	return nil
}

func (w *simWorld) stepRunner(r *simRunner, ev queuerunner.Event) error {
	expectStopLease := int64(0)
	allowStop := ev.Kind == queuerunner.EvSignal
	if ev.Kind == queuerunner.EvResponse {
		if ev.OpID == r.inFlightHB {
			if ev.Resp.Kind == queuerunner.ReqHeartbeat {
				if ev.Resp.Err != nil && !queueapi.IsTransientError(ev.Resp.Err) {
					allowStop = true
				} else if ev.Resp.Err == nil && ev.Resp.Heartbeat != nil && !ev.Resp.Heartbeat.Held {
					allowStop = true
					if r.runningLeaseID > 0 && !r.cmdStopped {
						if ex := w.oracle.execs[r.runningLeaseID]; ex != nil && !ex.completed && !ex.stopped {
							expectStopLease = r.runningLeaseID
						}
					}
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
				return fmt.Errorf("oracle: runner %d sent concurrent %s op %d while op %d in flight", r.id, a.Req.Kind, a.OpID, *slot)
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
			w.sched(a.AtMS-r.clockSkew, &simEvent{
				kind: seTimer, runnerIdx: r.id, statusIdx: -1, timerKind: a.TimerKind, timerGen: a.TimerGen,
			})
		case queuerunner.ActStartCommand:
			if a.ClearReports {
				r.reportOnDisk = nil
			}
			r.runningLeaseID = a.LeaseID
			r.cmdStopped = false
			if len(a.Attempts) == 1 && a.Attempts[0].Generation > 1 {
				w.stats["soloRetry"]++
			}
			if err := w.startSimCommand(r, a.LeaseID, a.Attempts); err != nil {
				return err
			}
		case queuerunner.ActStopCommand:
			if !allowStop {
				return fmt.Errorf("oracle: runner %d stopped lease %d without held=false, permanent heartbeat error, or signal", r.id, a.LeaseID)
			}
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
			r.exited = true
			r.verdict = a.Verdict
			r.exitErr = a.Err
			if after := r.machine.Step(w.nowMS+r.clockSkew+1, queuerunner.Event{Kind: queuerunner.EvSignal}); len(after) > 0 {
				return fmt.Errorf("oracle: runner %d emitted %d actions after ActDone", r.id, len(after))
			}
			var extraErr *queuerunner.ExtraFilesError
			if errors.As(a.Err, &extraErr) {
				w.stats["extraFile"]++
			}
			w.trace.logf("t=%d runner=%d done verdict=%s err=%t", w.nowMS, r.id, a.Verdict, a.Err != nil)
			if err := w.oracle.onRunnerExit(w, r); err != nil {
				return err
			}
			if extraErr != nil && !w.faultsActive && !w.buildBroken && w.oracle.finishedVerdict == "" {
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
			w.sched(a.AtMS, &simEvent{
				kind: seTimer, runnerIdx: -1, statusIdx: sc.id, timerKind: a.TimerKind, timerGen: a.TimerGen,
			})
		case queuerunner.ActDone:
			sc.exited = true
			if after := sc.machine.Step(w.nowMS+1, queuerunner.Event{Kind: queuerunner.EvSignal}); len(after) > 0 {
				return fmt.Errorf("oracle: status %d emitted %d actions after ActDone", sc.id, len(after))
			}
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
	if w.faultsActive && w.faults.reqDropPct > 0 && w.rng.IntN(100) < w.faults.reqDropPct && !(w.faults.expectCleanPass && req.Kind == queuerunner.ReqHeartbeat) {
		w.stats["reqDrop"]++
		w.sched(w.nowMS+delay, &simEvent{
			kind: seDeliverResp, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req,
			respErr: queueapi.NewTransportError("request dropped"),
		})
		return
	}
	w.sched(w.nowMS+delay, &simEvent{
		kind: seDeliverReq, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, wire: wr, token: tok,
	})
	if w.faultsActive && statusIdx < 0 && w.faults.reqDupPct > 0 && w.rng.IntN(100) < w.faults.reqDupPct {
		w.stats["reqDup"]++
		dupEv := &simEvent{
			kind: seDeliverReq, runnerIdx: runnerIdx, statusIdx: statusIdx, opID: opID, req: req, wire: wr, token: tok, isDup: true,
		}
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
		w.sched(w.nowMS+respDelay, &simEvent{
			kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req,
			respStatus: code, respBody: []byte(body),
		})
	}
	switch {
	case w.reportOutageUntilMS > 0 && w.nowMS < w.reportOutageUntilMS && ev.req.Kind == queuerunner.ReqReport:
		replyErr(http.StatusServiceUnavailable, "http503", `{"version":1,"error":{"code":"storage_error","message":"simulated report outage"}}`)
		return nil
	case w.faultsActive && w.faults.http503Pct > 0 && w.rng.IntN(100) < w.faults.http503Pct:
		replyErr(http.StatusServiceUnavailable, "http503", `{"version":1,"error":{"code":"storage_error","message":"simulated 503"}}`)
		return nil
	case w.faultsActive && w.faults.http429Pct > 0 && w.rng.IntN(100) < w.faults.http429Pct:
		replyErr(http.StatusTooManyRequests, "http429", `{"version":1,"error":{"code":"rate_limited","message":"simulated 429"}}`)
		return nil
	}

	// For procKillThenPowerLoss: kill the server process on the fsync of the first
	// Report (leaving its bytes in page cache), restart via Load, wait for the
	// runner's duplicate resend to get 200 without a new append, then trigger power loss.
	if w.faults.procKillThenPowerLoss && w.procKillStage == 0 && ev.req.Kind == queuerunner.ReqReport {
		w.fs.killNextFsync = true
	}

	triggerRestart := w.faults.serverRestart && !w.faults.restartDowntime && w.faultsActive &&
		ev.req.Kind == queuerunner.ReqReport && w.stats["reportAcrossRestart"] == 0 && w.srvEpoch == 0
	beforeLen := len(w.fs.pendingFiles[w.buildFile])
	wasInit := w.oracle.initialized
	w.fs.crashed = false
	reqNow := w.serverClock()
	status, body := w.invokeHandler(ev.wire, ev.token)

	dropReply := func(delay int64, msg string) {
		w.stats["respDrop"]++
		w.sched(w.nowMS+delay, &simEvent{
			kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req,
			respErr: queueapi.NewTransportError(msg),
		})
	}
	if w.fs.crashed {
		if w.faults.procKillThenPowerLoss && w.procKillStage == 0 {
			w.procKillStage = 1
		}
		if err := w.restartServer(false); err != nil {
			return err
		}
		dropReply(respDelay, "server crashed mid-append")
		return nil
	}
	if status == http.StatusInternalServerError {
		if w.stats["diskUncertain"] == 0 && !w.expectDamaged && !w.readErrDamaged {
			return fmt.Errorf("oracle: unexpected HTTP 500 without any uncertain disk error (kind=%s)", ev.req.Kind)
		}
		if !w.buildBroken {
			w.buildBroken = true
			w.stats["broken"]++
			w.exporter.stop()
		}
	}
	if ev.statusIdx >= 0 && status == http.StatusNotFound && !wasInit {
		w.statusClients[ev.statusIdx].uninit404[ev.opID] = true
	}
	w.trace.hashBytes("resp", body)

	// Only parse newly appended events when Handler actually applied them (any
	// non-5xx response; 409 stale_request can still persist EventExpired).
	var newEvents []queue.Event
	if status < http.StatusInternalServerError {
		if after := w.fs.pendingFiles[w.buildFile]; len(after) > beforeLen {
			newEvents = parseDiskEventsSlice(after[beforeLen:])
		}
		if err := w.exporter.recordCommand(reqNow, ev.req, status, body, newEvents); err != nil {
			return err
		}
	}
	if err := w.oracle.onServerHandled(w, ev.req, status, body, newEvents); err != nil {
		return err
	}

	if triggerRestart && status == http.StatusOK {
		if err := w.restartServer(true); err != nil {
			return err
		}
		dropReply(respDelay, "reset by restart")
		return nil
	}
	if w.faults.restartDowntime && w.srvEpoch == 0 && ev.req.Kind == queuerunner.ReqLease && status == http.StatusOK && strings.Contains(string(body), `"status":"leased"`) {
		w.serverOffset += w.cfg.LeaseTTLMS + 500
		if err := w.restartServer(true); err != nil {
			return err
		}
	}
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
	if w.faultsActive && !ev.isDup && w.faults.respDropPct > 0 && w.rng.IntN(100) < w.faults.respDropPct && !(w.faults.expectCleanPass && ev.req.Kind == queuerunner.ReqHeartbeat) {
		dropReply(respDelay, "response dropped")
		return nil
	}
	if status == http.StatusOK {
		w.oracle.recordAcked200(ev.req, body, newEvents)
		if w.faults.procKillThenPowerLoss && w.procKillStage == 1 && ev.req.Kind == queuerunner.ReqReport && len(newEvents) == 0 {
			// The server just answered 200 duplicate from the page-cache record
			// loaded after the process kill. Now cut power!
			w.procKillStage = 2
			if err := w.restartServer(true); err != nil {
				return err
			}
		}
	}
	w.sched(w.nowMS+respDelay, &simEvent{
		kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req,
		respStatus: status, respBody: body,
	})
	if w.faultsActive && ev.statusIdx < 0 && w.faults.respDupPct > 0 && w.rng.IntN(100) < w.faults.respDupPct {
		w.stats["respDup"]++
		w.sched(w.nowMS+respDelay+50+w.rng.Int64N(w.cfg.LeaseTTLMS), &simEvent{
			kind: seDeliverResp, runnerIdx: ev.runnerIdx, statusIdx: ev.statusIdx, opID: ev.opID, req: ev.req,
			respStatus: status, respBody: body, isDup: true,
		})
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
	durMS := int64(150 + w.rng.IntN(700))
	exitCode := 0
	signaled := false
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
		exitCode = 137
		signaled = true
	case w.faultsActive && w.faults.cmdHang && w.rng.IntN(4) == 0:
		durMS = w.cfg.LeaseTTLMS*2 + 500
		files = passAll()
	case w.faultsActive && w.faults.cmdExtraFile && w.rng.IntN(4) == 0:
		files = passAll()
		files["tests/unassigned_extra.py"] = junit.File{Seconds: big.NewRat(50, 1000), Cases: 1}
	case w.faultsActive && w.faults.cmdSigSibling && len(attempts) >= 2 && w.rng.IntN(3) == 0:
		exitCode = 137
		signaled = true
		files = passAll()
		files[attempts[0].Test] = junit.File{Seconds: big.NewRat(80, 1000), Cases: 1, Failed: true}
	case w.faultsActive && w.faults.cmdNonZeroClean && w.rng.IntN(4) == 0:
		exitCode = 1
		files = passAll()
	case w.faultsActive && w.faults.cmdTruncated && w.rng.IntN(5) == 0:
		readErr = errors.New("unexpected EOF in JUnit XML")
	case w.faultsActive && w.faults.cmdNoReport && (w.rng.IntN(5) == 0 || (r.reportOnDisk != nil && slices.ContainsFunc(attempts, func(a queue.LeasedAttemptView) bool { _, ok := r.reportOnDisk[a.Test]; return ok }))):
	case w.faultsActive && w.faults.cmdCrash && w.rng.IntN(5) == 0:
		exitCode = 137
		signaled = true
	case w.faultsActive && w.faults.cmdMixedSibling && len(attempts) >= 2 && w.rng.IntN(3) == 0:
		exitCode = 1
		files = make(map[string]junit.File, len(attempts))
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
		exitCode = 1
		files = passAll()
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
	w.sched(w.nowMS+durMS, &simEvent{
		kind: seCmdFinish, runnerIdx: r.id, leaseID: leaseID,
		exitCode: exitCode, signaled: signaled, files: files, readErr: readErr,
	})
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
