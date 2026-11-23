// Package queuerunner implements Bunpō's live-queue worker and --status state
// machines (sans-I/O) plus the thin production driver that executes their
// actions over HTTP, timers, subprocesses, and the filesystem.
package queuerunner

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/timing"
)

const (
	DefaultRetryBudgetMS  int64 = 2 * 60 * 1000
	DefaultRetryBaseMS    int64 = 200
	DefaultRetryMaxWaitMS int64 = 5_000
	MinHeartbeatTickMS    int64 = 20
)

// Entropy supplies IDs and retry jitter. Production uses CryptoEntropy;
// simulation tests pass a seeded deterministic PRNG.
type Entropy interface {
	Hex16() string
	Int64N(n int64) int64
}

// CryptoEntropy draws IDs from crypto/rand and jitter from math/rand/v2.
type CryptoEntropy struct{}

func (CryptoEntropy) Hex16() string {
	var buf [16]byte
	_, _ = crand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

func (CryptoEntropy) Int64N(n int64) int64 {
	return rand.Int64N(n)
}

// RetryConfig bounds exponential backoff with jitter for transient HTTP errors.
type RetryConfig struct {
	BudgetMS  int64
	BaseMS    int64
	MaxWaitMS int64
}

func (c RetryConfig) normalized() RetryConfig {
	if c.BudgetMS <= 0 {
		c.BudgetMS = DefaultRetryBudgetMS
	}
	if c.BaseMS <= 0 {
		c.BaseMS = DefaultRetryBaseMS
	}
	if c.MaxWaitMS <= 0 {
		c.MaxWaitMS = DefaultRetryMaxWaitMS
	}
	return c
}

type retryState struct {
	active     bool
	deadlineMS int64
	backoffMS  int64
}

func (r *retryState) reset() {
	r.active = false
	r.deadlineMS = 0
	r.backoffMS = 0
}

func (r *retryState) nextDelay(nowMS int64, cfg RetryConfig, ent Entropy) (int64, bool) {
	if !r.active {
		r.active = true
		r.deadlineMS = nowMS + cfg.BudgetMS
		r.backoffMS = cfg.BaseMS
	}
	if nowMS >= r.deadlineMS {
		return 0, false
	}
	wait := min(r.backoffMS, cfg.MaxWaitMS)
	if jitterMax := wait / 4; jitterMax > 0 {
		wait += ent.Int64N(jitterMax + 1)
	}
	if rem := r.deadlineMS - nowMS; wait > rem {
		wait = rem
	}
	if r.backoffMS < cfg.MaxWaitMS {
		r.backoffMS = min(r.backoffMS*2, cfg.MaxWaitMS)
	}
	return wait, true
}

// ExtraFilesError is returned when test reports name files that were not in
// the leased batch. The batch has already been reported as error to the server.
type ExtraFilesError struct {
	Extra []string
	Total int
}

func (e *ExtraFilesError) Error() string {
	shown := strings.Join(e.Extra[:min(5, len(e.Extra))], ", ")
	if len(e.Extra) > 5 {
		shown += fmt.Sprintf(", and %d more", len(e.Extra)-5)
	}
	return fmt.Sprintf("%d file(s) in the reports were not in the leased batch: %s", len(e.Extra), shown)
}

// RequestKind identifies one live-queue API call emitted by a state machine.
type RequestKind string

const (
	ReqInit      RequestKind = "init"
	ReqLease     RequestKind = "lease"
	ReqHeartbeat RequestKind = "heartbeat"
	ReqReport    RequestKind = "report"
	ReqStatus    RequestKind = "status"
	ReqListTests RequestKind = "list_tests"
)

// Request is an outgoing API call emitted by ActSend.
type Request struct {
	Kind      RequestKind
	BuildID   string
	Init      *queue.InitCommand
	Lease     *queue.LeaseCommand
	Heartbeat *queue.HeartbeatCommand
	Report    *queue.ReportCommand
	ListTests *queue.ListTestsCommand
}

// Response is the decoded outcome of one ActSend delivered via EvResponse.
type Response struct {
	Kind      RequestKind
	Init      *queueapi.InitResponse
	Lease     *queueapi.LeaseResponse
	Heartbeat *queueapi.HeartbeatResponse
	Report    *queueapi.ReportResponse
	Status    *queueapi.StatusResponse
	ListTests *queueapi.ListTestsResponse
	Err       error
}

// TimerKind distinguishes the foreground retry/wait slot from heartbeats.
type TimerKind int

const (
	TimerForeground TimerKind = iota + 1
	TimerHeartbeat
)

// EventKind identifies an input event to Machine.Step or StatusMachine.Step.
type EventKind int

const (
	EvStart EventKind = iota + 1
	EvResponse
	EvTimer
	EvCommandExited
	EvReportsRead
	EvSignal
)

// Event is one input to a sans-I/O machine.
type Event struct {
	Kind      EventKind
	OpID      uint64
	Resp      Response
	TimerKind TimerKind
	TimerGen  uint64
	LeaseID   int64
	ExitCode  int
	Signaled  bool
	Files     map[string]junit.File
	ReadErr   error
}

// ActionKind identifies a side effect requested by Machine.Step or StatusMachine.Step.
type ActionKind int

const (
	ActSend ActionKind = iota + 1
	ActSetTimer
	ActCancelTimer
	ActStartCommand
	ActStopCommand
	ActReadReports
	ActLogInfo
	ActLogWarn
	ActDone
)

// Action is one side effect emitted by a sans-I/O machine.
type Action struct {
	Kind         ActionKind
	OpID         uint64
	Req          Request
	TimerKind    TimerKind
	TimerGen     uint64
	AtMS         int64
	LeaseID      int64
	Attempts     []queue.LeasedAttemptView
	ClearReports bool
	Message      string
	Verdict      queue.Verdict
	Status       *queueapi.StatusResponse
	Report       *manifest.RunReport
	HasReport    bool
	Err          error
}

// WorkerConfig configures a sans-I/O live-queue worker Machine.
type WorkerConfig struct {
	BuildID       string
	MachineIndex  *int
	Config        queue.Config
	Tests         []string
	PredictionsMS []*int64
	Format        string // "junit" or "lines"
	Retry         RetryConfig
	Entropy       Entropy
}

type workerPhase int

const (
	phaseNew workerPhase = iota
	phaseInit
	phaseLeasing
	phaseWaiting
	phaseRunningCmd
	phaseStoppingLost
	phaseReadingReports
	phaseReporting
	phaseStoppingSignal
	phaseDone
)

// Machine is the pure sans-I/O state machine for one live-queue worker.
// It performs no I/O, reads no clock, and uses only the injected Entropy.
// Two independent op/timer slots (foreground and heartbeat) keep heartbeats
// running across report reads and Report retries, while OpID and TimerGen
// fencing discards delayed or duplicated responses from prior states.
type Machine struct {
	cfg      WorkerConfig
	retryCfg RetryConfig
	ent      Entropy
	runnerID string

	phase workerPhase

	nextOpID   uint64
	fgOpID     uint64
	hbOpID     uint64
	fgTimerGen uint64
	hbTimerGen uint64
	fgRetry    retryState

	hbIntervalMS int64
	hbRetryMS    int64
	buildStartMS int64
	startedAtSet bool

	curReqID    string
	curLease    queueapi.LeaseResponse
	cmdExitCode int
	cmdSignaled bool
	curResults  []queue.ReportResult
	curExtraErr *ExtraFilesError
}

// NewMachine initializes a sans-I/O worker state machine.
func NewMachine(cfg WorkerConfig) (*Machine, error) {
	if cfg.Format != "junit" && cfg.Format != "lines" {
		return nil, fmt.Errorf("invalid format %q", cfg.Format)
	}
	ent := cfg.Entropy
	if ent == nil {
		ent = CryptoEntropy{}
	}
	hbInterval := max(cfg.Config.LeaseTTLMS/3, MinHeartbeatTickMS)
	hbRetry := max(min(hbInterval/2, 1000), 15)
	return &Machine{
		cfg:          cfg,
		retryCfg:     cfg.Retry.normalized(),
		ent:          ent,
		runnerID:     ent.Hex16(),
		phase:        phaseNew,
		hbIntervalMS: hbInterval,
		hbRetryMS:    hbRetry,
	}, nil
}

// RunnerID returns this worker's generated runner_id.
func (m *Machine) RunnerID() string { return m.runnerID }

// Step advances the worker state machine for one event at nowMS.
func (m *Machine) Step(nowMS int64, ev Event) []Action {
	if m.phase == phaseDone {
		return nil
	}

	if ev.Kind == EvSignal {
		return m.handleSignal()
	}

	switch ev.Kind {
	case EvStart:
		if m.phase != phaseNew {
			return nil
		}
		m.buildStartMS = nowMS
		m.startedAtSet = true
		m.phase = phaseInit
		return []Action{m.sendInit()}

	case EvTimer:
		return m.handleTimer(nowMS, ev)

	case EvResponse:
		if ev.OpID == 0 {
			return nil
		}
		if ev.OpID == m.hbOpID {
			return m.handleHeartbeatResponse(nowMS, ev.Resp)
		}
		if ev.OpID == m.fgOpID {
			return m.handleForegroundResponse(nowMS, ev.Resp)
		}
		// Late or duplicated response from an earlier op: discard.
		return nil

	case EvCommandExited:
		if ev.LeaseID != m.curLease.LeaseID {
			return nil
		}
		switch m.phase {
		case phaseRunningCmd:
			m.cmdExitCode = ev.ExitCode
			m.cmdSignaled = ev.Signaled
			m.phase = phaseReadingReports
			return []Action{{Kind: ActReadReports, LeaseID: m.curLease.LeaseID}}

		case phaseStoppingLost:
			actions := []Action{
				{Kind: ActLogWarn, Message: fmt.Sprintf("lease %d lost while running; discarding batch and requesting new work", m.curLease.LeaseID)},
			}
			m.curLease = queueapi.LeaseResponse{}
			actions = append(actions, m.startFreshLease()...)
			return actions

		case phaseStoppingSignal:
			m.phase = phaseDone
			return []Action{{Kind: ActDone, Err: context.Canceled}}
		}
		return nil

	case EvReportsRead:
		if m.phase != phaseReadingReports || ev.LeaseID != m.curLease.LeaseID {
			return nil
		}
		var actions []Action
		if ev.ReadErr != nil {
			actions = append(actions, Action{
				Kind:    ActLogWarn,
				Message: fmt.Sprintf("lease %d report unreadable (%v); reporting batch as error", m.curLease.LeaseID, ev.ReadErr),
			})
		}
		results, extraErr := MapOutcomes(m.curLease.Attempts, m.cmdExitCode, m.cmdSignaled, m.cfg.Format, ev.Files, ev.ReadErr)
		m.curResults = results
		m.curExtraErr = extraErr
		m.fgRetry.reset()
		m.phase = phaseReporting
		actions = append(actions, m.sendReport())
		return actions
	}

	return nil
}

func (m *Machine) handleSignal() []Action {
	var actions []Action
	actions = append(actions, m.cancelForeground()...)
	actions = append(actions, m.cancelHeartbeat()...)
	switch m.phase {
	case phaseRunningCmd:
		m.phase = phaseStoppingSignal
		actions = append(actions, Action{Kind: ActStopCommand, LeaseID: m.curLease.LeaseID})
	case phaseStoppingLost:
		// Command stop was already requested; wait for it to exit then finish cancelled.
		m.phase = phaseStoppingSignal
	case phaseStoppingSignal:
		// Already stopping.
	default:
		m.phase = phaseDone
		actions = append(actions, Action{Kind: ActDone, Err: context.Canceled})
	}
	return actions
}

func (m *Machine) handleTimer(nowMS int64, ev Event) []Action {
	if ev.TimerKind == TimerHeartbeat {
		if ev.TimerGen != m.hbTimerGen {
			return nil
		}
		if m.phase != phaseRunningCmd && m.phase != phaseReadingReports && m.phase != phaseReporting {
			return nil
		}
		return []Action{m.sendHeartbeat()}
	}
	if ev.TimerKind == TimerForeground {
		if ev.TimerGen != m.fgTimerGen {
			return nil
		}
		switch m.phase {
		case phaseInit:
			return []Action{m.sendInit()}
		case phaseLeasing:
			return []Action{m.sendLease()}
		case phaseWaiting:
			return m.startFreshLease()
		case phaseReporting:
			return []Action{m.sendReport()}
		}
	}
	return nil
}

func (m *Machine) handleHeartbeatResponse(nowMS int64, resp Response) []Action {
	m.hbOpID = 0
	if m.phase != phaseRunningCmd && m.phase != phaseReadingReports && m.phase != phaseReporting {
		return nil
	}
	if resp.Err != nil {
		if queueapi.IsTransientError(resp.Err) {
			return []Action{
				{Kind: ActLogWarn, Message: fmt.Sprintf("heartbeat for lease %d failed (%v); retrying", m.curLease.LeaseID, resp.Err)},
				m.setHeartbeatTimer(nowMS + m.hbRetryMS),
			}
		}
		// Permanent 4xx (400/401/403/404) or protocol error can never succeed:
		// treat as lost lease so the child stops and the next lease call reports it.
		actions := []Action{
			{Kind: ActLogWarn, Message: fmt.Sprintf("heartbeat for lease %d permanently rejected (%v); stopping batch", m.curLease.LeaseID, resp.Err)},
		}
		actions = append(actions, m.onLeaseLost()...)
		return actions
	}
	if resp.Heartbeat == nil || !resp.Heartbeat.Held {
		return m.onLeaseLost()
	}
	return []Action{m.setHeartbeatTimer(nowMS + m.hbIntervalMS)}
}

func (m *Machine) onLeaseLost() []Action {
	actions := m.cancelHeartbeat()
	if m.phase == phaseRunningCmd {
		m.phase = phaseStoppingLost
		actions = append(actions, Action{Kind: ActStopCommand, LeaseID: m.curLease.LeaseID})
	}
	return actions
}

func (m *Machine) handleForegroundResponse(nowMS int64, resp Response) []Action {
	m.fgOpID = 0
	switch m.phase {
	case phaseInit:
		if resp.Err != nil {
			return m.handleTransientOrFail(nowMS, resp.Err)
		}
		m.fgRetry.reset()
		initResp := resp.Init
		var msg string
		if initResp.Created {
			msg = fmt.Sprintf("initialized build %s with %d tests", m.cfg.BuildID, initResp.TestCount)
		} else {
			msg = fmt.Sprintf("joined build %s (%d tests, verdict %s)", m.cfg.BuildID, initResp.TestCount, initResp.Verdict)
		}
		actions := []Action{{Kind: ActLogInfo, Message: msg}}
		actions = append(actions, m.startFreshLease()...)
		return actions

	case phaseLeasing:
		if resp.Err != nil {
			var apiErr *queueapi.APIError
			if errors.As(resp.Err, &apiErr) && apiErr.HTTPStatus == 409 {
				if apiErr.Code == queue.ErrCodeStaleRequest {
					actions := []Action{{Kind: ActLogWarn, Message: "lease request expired before response arrived; requesting a fresh lease"}}
					actions = append(actions, m.startFreshLease()...)
					return actions
				}
				if apiErr.Code == queue.ErrCodeLeaseHeld {
					// A delayed duplicate lease request can grant an orphan lease on the
					// server after this runner moved on; wait one heartbeat interval for
					// it to expire and try again with a fresh request_id, bounded by the
					// build deadline.
					if m.startedAtSet && m.cfg.Config.BuildTimeoutMS > 0 && nowMS >= m.buildStartMS+m.cfg.Config.BuildTimeoutMS {
						return m.fail(resp.Err)
					}
					m.fgRetry.reset()
					m.phase = phaseWaiting
					return []Action{
						{Kind: ActLogWarn, Message: "server reports an active lease for this runner; waiting before requesting a fresh lease"},
						m.setForegroundTimer(nowMS + m.hbIntervalMS),
					}
				}
			}
			return m.handleTransientOrFail(nowMS, resp.Err)
		}
		m.fgRetry.reset()
		lease := *resp.Lease
		switch lease.Status {
		case queue.LeaseDone:
			switch lease.Verdict {
			case queue.VerdictPassed, queue.VerdictFailed, queue.VerdictUnresolved:
				m.phase = phaseDone
				return []Action{{Kind: ActDone, Verdict: lease.Verdict}}
			default:
				return m.fail(fmt.Errorf("unexpected done verdict %q", lease.Verdict))
			}

		case queue.LeaseWait:
			if lease.RetryAfterMS <= 0 {
				return m.fail(fmt.Errorf("invalid wait retry_after_ms %d", lease.RetryAfterMS))
			}
			wait := lease.RetryAfterMS
			if jitterMax := wait / 4; jitterMax > 0 {
				wait += m.ent.Int64N(jitterMax + 1)
			}
			m.phase = phaseWaiting
			return []Action{m.setForegroundTimer(nowMS + wait)}

		case queue.LeaseGranted:
			m.curLease = lease
			m.curResults = nil
			m.curExtraErr = nil
			m.phase = phaseRunningCmd
			return []Action{
				{Kind: ActStartCommand, LeaseID: lease.LeaseID, Attempts: lease.Attempts, ClearReports: true},
				m.setHeartbeatTimer(nowMS + m.hbIntervalMS),
			}

		default:
			return m.fail(fmt.Errorf("unexpected lease status %q", lease.Status))
		}

	case phaseReporting:
		if resp.Err != nil {
			return m.handleTransientOrFail(nowMS, resp.Err)
		}
		actions := m.cancelHeartbeat()
		m.fgRetry.reset()
		rep := *resp.Report
		if m.curExtraErr != nil {
			m.phase = phaseDone
			actions = append(actions, Action{Kind: ActDone, Verdict: rep.Verdict, Err: m.curExtraErr})
			return actions
		}
		for _, d := range rep.Results {
			if d.Disposition == queue.DispositionStale {
				actions = append(actions, Action{
					Kind:    ActLogWarn,
					Message: fmt.Sprintf("attempt %s in lease %d was reported stale", d.AttemptID, m.curLease.LeaseID),
				})
			}
		}
		m.curLease = queueapi.LeaseResponse{}
		m.curResults = nil
		switch rep.Verdict {
		case queue.VerdictRunning:
			actions = append(actions, m.startFreshLease()...)
			return actions
		case queue.VerdictPassed, queue.VerdictFailed, queue.VerdictUnresolved:
			m.phase = phaseDone
			actions = append(actions, Action{Kind: ActDone, Verdict: rep.Verdict})
			return actions
		default:
			m.phase = phaseDone
			actions = append(actions, Action{Kind: ActDone, Err: fmt.Errorf("unexpected report verdict %q", rep.Verdict)})
			return actions
		}
	}
	return nil
}

func (m *Machine) handleTransientOrFail(nowMS int64, err error) []Action {
	if queueapi.IsTransientError(err) {
		if delay, ok := m.fgRetry.nextDelay(nowMS, m.retryCfg, m.ent); ok {
			return []Action{m.setForegroundTimer(nowMS + delay)}
		}
	}
	return m.fail(err)
}

func (m *Machine) fail(err error) []Action {
	actions := m.cancelForeground()
	actions = append(actions, m.cancelHeartbeat()...)
	m.phase = phaseDone
	actions = append(actions, Action{Kind: ActDone, Err: err})
	return actions
}

func (m *Machine) startFreshLease() []Action {
	m.fgRetry.reset()
	m.curReqID = m.ent.Hex16()
	m.phase = phaseLeasing
	return []Action{m.sendLease()}
}

func (m *Machine) nextOp() uint64 {
	m.nextOpID++
	return m.nextOpID
}

func (m *Machine) sendInit() Action {
	op := m.nextOp()
	m.fgOpID = op
	cmd := queue.InitCommand{
		BuildID:       m.cfg.BuildID,
		RunnerID:      m.runnerID,
		MachineIndex:  m.cfg.MachineIndex,
		Config:        m.cfg.Config,
		Tests:         m.cfg.Tests,
		PredictionsMS: m.cfg.PredictionsMS,
	}
	return Action{
		Kind: ActSend,
		OpID: op,
		Req:  Request{Kind: ReqInit, BuildID: m.cfg.BuildID, Init: &cmd},
	}
}

func (m *Machine) sendLease() Action {
	op := m.nextOp()
	m.fgOpID = op
	cmd := queue.LeaseCommand{
		RunnerID:     m.runnerID,
		MachineIndex: m.cfg.MachineIndex,
		RequestID:    m.curReqID,
	}
	return Action{
		Kind: ActSend,
		OpID: op,
		Req:  Request{Kind: ReqLease, BuildID: m.cfg.BuildID, Lease: &cmd},
	}
}

func (m *Machine) sendHeartbeat() Action {
	op := m.nextOp()
	m.hbOpID = op
	cmd := queue.HeartbeatCommand{
		RunnerID: m.runnerID,
		LeaseID:  m.curLease.LeaseID,
	}
	return Action{
		Kind: ActSend,
		OpID: op,
		Req:  Request{Kind: ReqHeartbeat, BuildID: m.cfg.BuildID, Heartbeat: &cmd},
	}
}

func (m *Machine) sendReport() Action {
	op := m.nextOp()
	m.fgOpID = op
	cmd := queue.ReportCommand{
		RunnerID: m.runnerID,
		LeaseID:  m.curLease.LeaseID,
		Results:  m.curResults,
	}
	return Action{
		Kind: ActSend,
		OpID: op,
		Req:  Request{Kind: ReqReport, BuildID: m.cfg.BuildID, Report: &cmd},
	}
}

func (m *Machine) setForegroundTimer(atMS int64) Action {
	m.fgTimerGen++
	return Action{Kind: ActSetTimer, TimerKind: TimerForeground, TimerGen: m.fgTimerGen, AtMS: atMS}
}

func (m *Machine) setHeartbeatTimer(atMS int64) Action {
	m.hbTimerGen++
	return Action{Kind: ActSetTimer, TimerKind: TimerHeartbeat, TimerGen: m.hbTimerGen, AtMS: atMS}
}

func (m *Machine) cancelForeground() []Action {
	m.fgOpID = 0
	m.fgTimerGen++
	return []Action{{Kind: ActCancelTimer, TimerKind: TimerForeground}}
}

func (m *Machine) cancelHeartbeat() []Action {
	m.hbOpID = 0
	m.hbTimerGen++
	return []Action{{Kind: ActCancelTimer, TimerKind: TimerHeartbeat}}
}

// StatusMachineConfig configures a sans-I/O --status reader machine.
type StatusMachineConfig struct {
	BuildID string
	Retry   RetryConfig
	Entropy Entropy
}

type statusPhase int

const (
	statusPhaseNew statusPhase = iota
	statusPhaseFetching
	statusPhasePaging
	statusPhaseDone
)

// StatusMachine fetches build status and, when finished with zero unresolved
// tests and zero init conflicts, paginates /tests into a manifest.RunReport.
type StatusMachine struct {
	buildID    string
	retryCfg   RetryConfig
	ent        Entropy
	phase      statusPhase
	nextOpID   uint64
	fgOpID     uint64
	fgTimerGen uint64
	fgRetry    retryState
	status     queueapi.StatusResponse
	report     manifest.RunReport
	cursor     int
}

func NewStatusMachine(cfg StatusMachineConfig) *StatusMachine {
	ent := cfg.Entropy
	if ent == nil {
		ent = CryptoEntropy{}
	}
	return &StatusMachine{
		buildID:  cfg.BuildID,
		retryCfg: cfg.Retry.normalized(),
		ent:      ent,
		phase:    statusPhaseNew,
	}
}

func (s *StatusMachine) Step(nowMS int64, ev Event) []Action {
	if s.phase == statusPhaseDone {
		return nil
	}
	if ev.Kind == EvSignal {
		s.fgOpID = 0
		s.fgTimerGen++
		s.phase = statusPhaseDone
		return []Action{
			{Kind: ActCancelTimer, TimerKind: TimerForeground},
			{Kind: ActDone, Err: context.Canceled},
		}
	}

	switch ev.Kind {
	case EvStart:
		if s.phase != statusPhaseNew {
			return nil
		}
		s.phase = statusPhaseFetching
		return []Action{s.sendStatus()}

	case EvTimer:
		if ev.TimerKind != TimerForeground || ev.TimerGen != s.fgTimerGen {
			return nil
		}
		if s.phase == statusPhaseFetching {
			return []Action{s.sendStatus()}
		}
		if s.phase == statusPhasePaging {
			return []Action{s.sendListTests()}
		}

	case EvResponse:
		if ev.OpID == 0 || ev.OpID != s.fgOpID {
			return nil
		}
		s.fgOpID = 0
		if ev.Resp.Err != nil {
			if queueapi.IsTransientError(ev.Resp.Err) {
				if delay, ok := s.fgRetry.nextDelay(nowMS, s.retryCfg, s.ent); ok {
					s.fgTimerGen++
					return []Action{{Kind: ActSetTimer, TimerKind: TimerForeground, TimerGen: s.fgTimerGen, AtMS: nowMS + delay}}
				}
			}
			s.phase = statusPhaseDone
			return []Action{{Kind: ActDone, Err: ev.Resp.Err}}
		}
		s.fgRetry.reset()

		if s.phase == statusPhaseFetching {
			s.status = *ev.Resp.Status
			if s.status.Verdict == queue.VerdictRunning || s.status.Counts.Unresolved > 0 || len(s.status.Conflicts) > 0 {
				st := s.status
				s.phase = statusPhaseDone
				return []Action{{Kind: ActDone, Verdict: st.Verdict, Status: &st, HasReport: false}}
			}
			s.report = manifest.RunReport{
				Index:    0,
				Machines: 1,
				Tests:    make(timing.Report, s.status.Counts.Total),
			}
			s.cursor = 0
			s.phase = statusPhasePaging
			return []Action{s.sendListTests()}
		}

		if s.phase == statusPhasePaging {
			page := *ev.Resp.ListTests
			for _, tv := range page.Tests {
				if tv.State == queue.StatusPassed && tv.Timed {
					s.report.Tests[tv.Test] = timing.Observation{Timed: true, MS: tv.DurationMS}
				} else {
					s.report.Tests[tv.Test] = timing.Observation{Timed: false, MS: 0}
				}
			}
			if page.NextCursor == nil {
				if len(s.report.Tests) != s.status.Counts.Total {
					s.phase = statusPhaseDone
					return []Action{{Kind: ActDone, Err: fmt.Errorf("incomplete test list from server: got %d tests, status reported %d", len(s.report.Tests), s.status.Counts.Total)}}
				}
				st := s.status
				rep := s.report
				s.phase = statusPhaseDone
				return []Action{{Kind: ActDone, Verdict: st.Verdict, Status: &st, Report: &rep, HasReport: true}}
			}
			if *page.NextCursor <= s.cursor {
				s.phase = statusPhaseDone
				return []Action{{Kind: ActDone, Err: fmt.Errorf("list tests cursor did not advance (%d -> %d)", s.cursor, *page.NextCursor)}}
			}
			s.cursor = *page.NextCursor
			return []Action{s.sendListTests()}
		}
	}
	return nil
}

func (s *StatusMachine) sendStatus() Action {
	s.nextOpID++
	s.fgOpID = s.nextOpID
	return Action{
		Kind: ActSend,
		OpID: s.fgOpID,
		Req:  Request{Kind: ReqStatus, BuildID: s.buildID},
	}
}

func (s *StatusMachine) sendListTests() Action {
	s.nextOpID++
	s.fgOpID = s.nextOpID
	cmd := queue.ListTestsCommand{Cursor: s.cursor, Limit: queue.MaxTestsPageLimit}
	return Action{
		Kind: ActSend,
		OpID: s.fgOpID,
		Req:  Request{Kind: ReqListTests, BuildID: s.buildID, ListTests: &cmd},
	}
}

// MapOutcomes maps a batch execution's exit status and parsed reports onto
// per-attempt queue outcomes following section 7.1 of docs/queue-protocol.md.
func MapOutcomes(
	attempts []queue.LeasedAttemptView,
	exitCode int,
	signaled bool,
	format string,
	files map[string]junit.File,
	readErr error,
) ([]queue.ReportResult, *ExtraFilesError) {
	errorAll := func() []queue.ReportResult {
		out := make([]queue.ReportResult, len(attempts))
		for i, a := range attempts {
			out[i] = queue.ReportResult{
				AttemptID:  a.AttemptID,
				TestIndex:  a.TestIndex,
				Generation: a.Generation,
				Outcome:    queue.OutcomeError,
				Timed:      false,
				DurationMS: 0,
			}
		}
		return out
	}

	if readErr != nil {
		return errorAll(), nil
	}

	assigned := make(map[string]bool, len(attempts))
	for _, a := range attempts {
		assigned[a.Test] = true
	}

	var extra []string
	for path := range files {
		if !assigned[path] {
			extra = append(extra, path)
		}
	}
	if len(extra) > 0 {
		slices.Sort(extra)
		return errorAll(), &ExtraFilesError{Extra: extra, Total: len(files)}
	}

	anySiblingFailed := false
	for _, a := range attempts {
		if f, ok := files[a.Test]; ok && f.Failed {
			anySiblingFailed = true
			break
		}
	}

	results := make([]queue.ReportResult, len(attempts))
	for i, a := range attempts {
		f, present := files[a.Test]
		var out queue.Outcome
		timed := false
		var dur int64

		switch {
		case present && f.Failed:
			out = queue.OutcomeFailed

		case !signaled && exitCode == 0:
			switch {
			case !present || f.Cases == 0:
				out = queue.OutcomeError
			case f.Skipped == f.Cases:
				out = queue.OutcomeSkipped
			default:
				out = queue.OutcomePassed
				timed = true
				dur = f.MS()
			}

		case !signaled && exitCode != 0 && format == "junit" && anySiblingFailed:
			if present && f.Cases > 0 && f.Skipped == 0 {
				out = queue.OutcomePassed
				timed = false
			} else {
				out = queue.OutcomeError
			}

		default:
			out = queue.OutcomeError
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
	return results, nil
}
