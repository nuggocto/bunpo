package queuerunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

const defaultKillGrace = 5 * time.Second

// RunConfig configures one live-queue worker process.
type RunConfig struct {
	Client        *queueapi.Client
	BuildID       string
	MachineIndex  *int
	Config        queue.Config
	Tests         []string
	PredictionsMS []*int64
	Dir           string
	Command       []string
	ReportPaths   []string
	Format        string // "junit" or "lines"
	ReadReports   func(paths []string) (map[string]junit.File, error)
	Stdout        io.Writer
	Stderr        io.Writer
	LogInfo       func(format string, a ...any)
	LogWarn       func(format string, a ...any)
	KillGrace     time.Duration
	Retry         RetryConfig
	Entropy       Entropy
}

// StatusConfig configures one live-queue --status invocation.
type StatusConfig struct {
	Client  *queueapi.Client
	BuildID string
	Retry   RetryConfig
	Entropy Entropy
}

// StatusResult holds the fetched status and optional RunReport from RunStatus.
type StatusResult struct {
	Status    queueapi.StatusResponse
	Report    manifest.RunReport
	HasReport bool
}

// Run executes the live-queue worker loop by driving Machine over real HTTP,
// timers, subprocesses, and batch/report files.
func Run(ctx context.Context, cfg RunConfig) (queue.Verdict, error) {
	if len(cfg.Command) == 0 {
		return "", errors.New("test command is required after --")
	}
	if cfg.ReadReports == nil {
		return "", errors.New("report reader is required")
	}
	if cfg.KillGrace <= 0 {
		cfg.KillGrace = defaultKillGrace
	}

	m, err := NewMachine(WorkerConfig{
		BuildID:       cfg.BuildID,
		MachineIndex:  cfg.MachineIndex,
		Config:        cfg.Config,
		Tests:         cfg.Tests,
		PredictionsMS: cfg.PredictionsMS,
		Format:        cfg.Format,
		Retry:         cfg.Retry,
		Entropy:       cfg.Entropy,
	})
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return "", err
	}
	dirRoot, err := os.OpenRoot(cfg.Dir)
	if err != nil {
		return "", err
	}
	defer dirRoot.Close()

	absDir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return "", err
	}
	batchFileName := fmt.Sprintf("queue-%s.txt", m.RunnerID()[:8])
	if cfg.MachineIndex != nil {
		batchFileName = fmt.Sprintf("queue-%d.txt", *cfg.MachineIndex)
	}
	batchFileAbs := filepath.Join(absDir, batchFileName)

	done, err := driveMachine(ctx, m.Step, driverDeps{
		client:        cfg.Client,
		dirRoot:       dirRoot,
		batchFileName: batchFileName,
		batchFileAbs:  batchFileAbs,
		command:       cfg.Command,
		reportPaths:   cfg.ReportPaths,
		readReports:   cfg.ReadReports,
		stdout:        cfg.Stdout,
		stderr:        cfg.Stderr,
		killGrace:     cfg.KillGrace,
		logInfo:       cfg.LogInfo,
		logWarn:       cfg.LogWarn,
		ent:           m.ent,
	})
	if err != nil {
		return "", err
	}
	return done.Verdict, done.Err
}

// RunStatus fetches build status and, when finished with zero unresolved tests
// and zero init conflicts, paginates /tests into a RunReport.
func RunStatus(ctx context.Context, cfg StatusConfig) (StatusResult, error) {
	sm := NewStatusMachine(StatusMachineConfig{
		BuildID: cfg.BuildID,
		Retry:   cfg.Retry,
		Entropy: cfg.Entropy,
	})
	done, err := driveMachine(ctx, sm.Step, driverDeps{
		client: cfg.Client,
	})
	if err != nil {
		return StatusResult{}, err
	}
	if done.Err != nil {
		return StatusResult{}, done.Err
	}
	var res StatusResult
	if done.Status != nil {
		res.Status = *done.Status
	}
	if done.HasReport && done.Report != nil {
		res.Report = *done.Report
		res.HasReport = true
	}
	return res, nil
}

type driverDeps struct {
	client        *queueapi.Client
	dirRoot       *os.Root
	batchFileName string
	batchFileAbs  string
	command       []string
	reportPaths   []string
	readReports   func(paths []string) (map[string]junit.File, error)
	stdout        io.Writer
	stderr        io.Writer
	killGrace     time.Duration
	logInfo       func(format string, a ...any)
	logWarn       func(format string, a ...any)
	ent           Entropy
}

type runningCmd struct {
	leaseID  int64
	stopCh   chan struct{}
	stopOnce sync.Once
}

func driveMachine(ctx context.Context, step func(nowMS int64, ev Event) []Action, d driverDeps) (Action, error) {
	if d.logInfo == nil {
		d.logInfo = func(string, ...any) {}
	}
	if d.logWarn == nil {
		d.logWarn = func(string, ...any) {}
	}
	if d.ent == nil {
		d.ent = cryptoEntropy{}
	}

	// Monotonic clock anchored at startTime so NTP steps never skew retry budgets or timers.
	startTime := time.Now()
	nowMS := func() int64 { return time.Since(startTime).Milliseconds() }

	evCh := make(chan Event, 64)
	driverDone := make(chan struct{})
	opCtx, opCancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var activeCmd *runningCmd
	timers := map[TimerKind]chan struct{}{}

	cancelTimer := func(kind TimerKind) {
		if ch, ok := timers[kind]; ok {
			close(ch)
			delete(timers, kind)
		}
	}

	defer func() {
		for k := range timers {
			cancelTimer(k)
		}
		opCancel()
		close(driverDone)
		wg.Wait()
	}()

	post := func(ev Event) {
		select {
		case evCh <- ev:
		case <-driverDone:
		}
	}

	var doneAction *Action
	execActions := func(actions []Action) error {
		for _, a := range actions {
			switch a.Kind {
			case ActLogInfo:
				d.logInfo("%s", a.Message)
			case ActLogWarn:
				d.logWarn("%s", a.Message)

			case ActCancelTimer:
				cancelTimer(a.TimerKind)

			case ActSetTimer:
				cancelTimer(a.TimerKind)
				stopCh := make(chan struct{})
				timers[a.TimerKind] = stopCh
				delay := max(time.Duration(a.AtMS-nowMS())*time.Millisecond, 0)
				kind, gen := a.TimerKind, a.TimerGen
				wg.Add(1)
				go func() {
					defer wg.Done()
					t := time.NewTimer(delay)
					defer t.Stop()
					select {
					case <-t.C:
						post(Event{Kind: EvTimer, TimerKind: kind, TimerGen: gen})
					case <-stopCh:
					case <-driverDone:
					}
				}()

			case ActSend:
				opID, req := a.OpID, a.Req
				wg.Add(1)
				go func() {
					defer wg.Done()
					resp := doRequest(opCtx, d.client, req)
					post(Event{Kind: EvResponse, OpID: opID, Resp: resp})
				}()

			case ActStartCommand:
				if err := writeBatchFile(d.dirRoot, d.batchFileName, a.Attempts, d.ent); err != nil {
					return fmt.Errorf("writing batch file: %w", err)
				}
				if a.ClearReports {
					if err := removeReportFiles(d.reportPaths); err != nil {
						return fmt.Errorf("removing old report file: %w", err)
					}
				}
				cmd := exec.Command(d.command[0], d.command[1:]...)
				cmd.Stdin = nil
				cmd.Stdout = d.stdout
				cmd.Stderr = d.stderr
				cmd.Env = filteredChildEnv(os.Environ(), d.batchFileAbs)
				configureProcessGroup(cmd)
				if err := cmd.Start(); err != nil {
					d.logWarn("failed to start command: %v", err)
					post(Event{Kind: EvCommandExited, LeaseID: a.LeaseID, ExitCode: -1, Signaled: true})
					continue
				}
				rc := &runningCmd{
					leaseID: a.LeaseID,
					stopCh:  make(chan struct{}),
				}
				activeCmd = rc
				wg.Add(1)
				go func() {
					defer wg.Done()
					childDone := make(chan error, 1)
					go func() { childDone <- cmd.Wait() }()
					var waitErr error
					select {
					case waitErr = <-childDone:
					case <-rc.stopCh:
						// Wait until stopProcessGroup has killed the entire process
						// group (including grandchildren) before posting EvCommandExited.
						waitErr = stopProcessGroup(cmd, childDone, d.killGrace)
					}
					exitCode, signaled := classifyExit(waitErr)
					post(Event{Kind: EvCommandExited, LeaseID: rc.leaseID, ExitCode: exitCode, Signaled: signaled})
				}()

			case ActStopCommand:
				if rc := activeCmd; rc != nil && rc.leaseID == a.LeaseID {
					rc.stopOnce.Do(func() { close(rc.stopCh) })
				}

			case ActReadReports:
				leaseID := a.LeaseID
				wg.Add(1)
				go func() {
					defer wg.Done()
					files, err := d.readReports(d.reportPaths)
					post(Event{Kind: EvReportsRead, LeaseID: leaseID, Files: files, ReadErr: err})
				}()

			case ActDone:
				cp := a
				if errors.Is(cp.Err, context.Canceled) && ctx.Err() != nil {
					cp.Err = ctx.Err()
				}
				doneAction = &cp
			}
		}
		return nil
	}

	if err := execActions(step(nowMS(), Event{Kind: EvStart})); err != nil {
		return Action{}, err
	}
	if doneAction != nil {
		return *doneAction, nil
	}

	sigCh := ctx.Done()
	for doneAction == nil {
		select {
		case <-sigCh:
			sigCh = nil // deliver EvSignal once
			if err := execActions(step(nowMS(), Event{Kind: EvSignal})); err != nil {
				return Action{}, err
			}
		case ev := <-evCh:
			if ev.Kind == EvCommandExited && activeCmd != nil && activeCmd.leaseID == ev.LeaseID {
				activeCmd = nil
			}
			if err := execActions(step(nowMS(), ev)); err != nil {
				return Action{}, err
			}
		}
	}
	return *doneAction, nil
}

func doRequest(ctx context.Context, client *queueapi.Client, req Request) Response {
	switch req.Kind {
	case ReqInit:
		r, err := client.Init(ctx, *req.Init)
		if err != nil {
			return Response{Kind: req.Kind, Err: err}
		}
		return Response{Kind: req.Kind, Init: &r}

	case ReqLease:
		r, err := client.Lease(ctx, req.BuildID, *req.Lease)
		if err != nil {
			return Response{Kind: req.Kind, Err: err}
		}
		return Response{Kind: req.Kind, Lease: &r}

	case ReqHeartbeat:
		r, err := client.Heartbeat(ctx, req.BuildID, *req.Heartbeat)
		if err != nil {
			return Response{Kind: req.Kind, Err: err}
		}
		return Response{Kind: req.Kind, Heartbeat: &r}

	case ReqReport:
		r, err := client.Report(ctx, req.BuildID, *req.Report)
		if err != nil {
			return Response{Kind: req.Kind, Err: err}
		}
		return Response{Kind: req.Kind, Report: &r}

	case ReqStatus:
		r, err := client.Status(ctx, req.BuildID)
		if err != nil {
			return Response{Kind: req.Kind, Err: err}
		}
		return Response{Kind: req.Kind, Status: &r}

	case ReqListTests:
		r, err := client.ListTests(ctx, req.BuildID, *req.ListTests)
		if err != nil {
			return Response{Kind: req.Kind, Err: err}
		}
		return Response{Kind: req.Kind, ListTests: &r}

	default:
		return Response{Kind: req.Kind, Err: fmt.Errorf("unknown request kind %q", req.Kind)}
	}
}

func classifyExit(waitErr error) (exitCode int, signaled bool) {
	if waitErr == nil {
		return 0, false
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return exitErr.ExitCode(), processSignaled(exitErr.ProcessState)
	}
	return -1, true
}

// filteredChildEnv strips BUNPO_QUEUE_TOKEN and BUNPO_QUEUE_READ_TOKEN from
// env and sets BUNPO_TESTS_FILE so untrusted test code does not inherit queue
// credentials directly.
func filteredChildEnv(env []string, testsFile string) []string {
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		key, _, _ := strings.Cut(kv, "=")
		if strings.EqualFold(key, "BUNPO_QUEUE_TOKEN") ||
			strings.EqualFold(key, "BUNPO_QUEUE_READ_TOKEN") ||
			strings.EqualFold(key, "BUNPO_TESTS_FILE") {
			continue
		}
		out = append(out, kv)
	}
	out = append(out, "BUNPO_TESTS_FILE="+testsFile)
	return out
}

func writeBatchFile(root *os.Root, name string, attempts []queue.LeasedAttemptView, ent Entropy) (err error) {
	var b strings.Builder
	for _, a := range attempts {
		b.WriteString(a.Test)
		b.WriteByte('\n')
	}
	tmp := fmt.Sprintf(".%s.%s.tmp", name, ent.Hex16()[:8])
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			root.Remove(tmp)
		}
	}()
	if _, err = io.WriteString(f, b.String()); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return root.Rename(tmp, name)
}

func removeReportFiles(paths []string) error {
	for _, p := range paths {
		root, err := os.OpenRoot(filepath.Dir(p))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		err = root.Remove(filepath.Base(p))
		_ = root.Close()
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}
