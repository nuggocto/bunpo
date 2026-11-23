// Package queuerunner runs Bunpō's live-queue worker loop: initializing the
// build, leasing batches, executing the user's test command with heartbeats,
// mapping report outcomes, and collecting accepted results for --status.
package queuerunner

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/timing"
)

const (
	defaultKillGrace = 5 * time.Second
	minHeartbeatTick = 20 * time.Millisecond
)

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
}

// Run executes the live-queue loop until the build finishes, ctx is cancelled,
// or a fatal protocol/configuration error occurs.
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
	logInfo := cfg.LogInfo
	if logInfo == nil {
		logInfo = func(string, ...any) {}
	}
	logWarn := cfg.LogWarn
	if logWarn == nil {
		logWarn = func(string, ...any) {}
	}

	runnerID := randomHex16()

	initResp, err := cfg.Client.Init(ctx, queue.InitCommand{
		BuildID:       cfg.BuildID,
		RunnerID:      runnerID,
		MachineIndex:  cfg.MachineIndex,
		Config:        cfg.Config,
		Tests:         cfg.Tests,
		PredictionsMS: cfg.PredictionsMS,
	})
	if err != nil {
		return "", err
	}
	if initResp.Created {
		logInfo("initialized build %s with %d tests", cfg.BuildID, initResp.TestCount)
	} else {
		logInfo("joined build %s (%d tests, verdict %s)", cfg.BuildID, initResp.TestCount, initResp.Verdict)
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
	batchFileName := fmt.Sprintf("queue-%s.txt", runnerID[:8])
	if cfg.MachineIndex != nil {
		batchFileName = fmt.Sprintf("queue-%d.txt", *cfg.MachineIndex)
	}
	batchFileAbs := filepath.Join(absDir, batchFileName)

	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}

		reqID := randomHex16()
		lease, err := cfg.Client.Lease(ctx, cfg.BuildID, queue.LeaseCommand{
			RunnerID:     runnerID,
			MachineIndex: cfg.MachineIndex,
			RequestID:    reqID,
		})
		if err != nil {
			// A lost lease reply followed by an outage longer than the TTL turns a
			// retried request_id into 409 stale_request. Ask again with a fresh ID.
			var apiErr *queueapi.APIError
			if errors.As(err, &apiErr) && apiErr.HTTPStatus == 409 && apiErr.Code == queue.ErrCodeStaleRequest {
				logWarn("lease request expired before response arrived; requesting a fresh lease")
				continue
			}
			return "", err
		}

		switch lease.Status {
		case queue.LeaseDone:
			switch lease.Verdict {
			case queue.VerdictPassed, queue.VerdictFailed, queue.VerdictUnresolved:
				return lease.Verdict, nil
			default:
				return "", fmt.Errorf("unexpected done verdict %q", lease.Verdict)
			}

		case queue.LeaseWait:
			if lease.RetryAfterMS <= 0 {
				return "", fmt.Errorf("invalid wait retry_after_ms %d", lease.RetryAfterMS)
			}
			wait := time.Duration(lease.RetryAfterMS) * time.Millisecond
			if jitterMax := wait / 4; jitterMax > 0 {
				wait += time.Duration(rand.Int64N(int64(jitterMax) + 1))
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return "", ctx.Err()
			case <-timer.C:
			}
			continue

		case queue.LeaseGranted:
		default:
			return "", fmt.Errorf("unexpected lease status %q", lease.Status)
		}

		if err := writeBatchFile(dirRoot, batchFileName, lease.Attempts); err != nil {
			return "", fmt.Errorf("writing batch file: %w", err)
		}
		if err := removeReportFiles(cfg.ReportPaths); err != nil {
			return "", fmt.Errorf("removing old report file: %w", err)
		}

		done, verdict, err := runOneBatch(ctx, cfg, runnerID, lease, batchFileAbs, logWarn)
		if err != nil {
			return verdict, err
		}
		if done {
			return verdict, nil
		}
	}
}

// runOneBatch executes the child command, keeps heartbeats running through
// report reading and Report retries so a brief outage cannot turn a finished
// batch stale, and always joins the heartbeat goroutine before returning.
func runOneBatch(
	ctx context.Context,
	cfg RunConfig,
	runnerID string,
	lease queueapi.LeaseResponse,
	batchFileAbs string,
	logWarn func(string, ...any),
) (done bool, verdict queue.Verdict, err error) {
	hbCtx, hbCancel := context.WithCancel(ctx)
	var lostLease atomic.Bool
	lostCh := make(chan struct{})
	var lostOnce sync.Once
	signalLost := func() {
		lostLease.Store(true)
		lostOnce.Do(func() { close(lostCh) })
	}

	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		runHeartbeatLoop(hbCtx, cfg.Client, cfg.BuildID, runnerID, lease.LeaseID, cfg.Config.LeaseTTLMS, signalLost, logWarn)
	}()
	defer func() {
		hbCancel()
		hbWG.Wait()
	}()

	execRes, err := execBatch(ctx, cfg, batchFileAbs, lostCh, &lostLease, logWarn)
	if err != nil {
		return false, "", err
	}
	if execRes.lostLease {
		logWarn("lease %d lost while running; discarding batch and requesting new work", lease.LeaseID)
		return false, "", nil
	}

	files, readErr := cfg.ReadReports(cfg.ReportPaths)
	if readErr != nil {
		logWarn("lease %d report unreadable (%v); reporting batch as error", lease.LeaseID, readErr)
	}

	results, extraErr := MapOutcomes(lease.Attempts, execRes.exitCode, execRes.signaled, cfg.Format, files, readErr)

	// Heartbeats stay alive while Client.Report retries across brief outages.
	rep, err := cfg.Client.Report(ctx, cfg.BuildID, queue.ReportCommand{
		RunnerID: runnerID,
		LeaseID:  lease.LeaseID,
		Results:  results,
	})
	if err != nil {
		return false, "", err
	}

	if extraErr != nil {
		return true, rep.Verdict, extraErr
	}

	for _, d := range rep.Results {
		if d.Disposition == queue.DispositionStale {
			logWarn("attempt %s in lease %d was reported stale", d.AttemptID, lease.LeaseID)
		}
	}

	switch rep.Verdict {
	case queue.VerdictRunning:
		return false, queue.VerdictRunning, nil
	case queue.VerdictPassed, queue.VerdictFailed, queue.VerdictUnresolved:
		return true, rep.Verdict, nil
	default:
		return false, "", fmt.Errorf("unexpected report verdict %q", rep.Verdict)
	}
}

type batchExecResult struct {
	exitCode  int
	signaled  bool
	lostLease bool
}

func execBatch(
	ctx context.Context,
	cfg RunConfig,
	batchFileAbs string,
	lostCh <-chan struct{},
	lostLease *atomic.Bool,
	logWarn func(string, ...any),
) (batchExecResult, error) {
	cmd := exec.Command(cfg.Command[0], cfg.Command[1:]...)
	cmd.Stdin = nil
	cmd.Stdout = cfg.Stdout
	cmd.Stderr = cfg.Stderr
	cmd.Env = filteredChildEnv(os.Environ(), batchFileAbs)
	configureProcessGroup(cmd)

	if err := cmd.Start(); err != nil {
		// Command failed to start; report batch as error (signaled=true forces error).
		logWarn("failed to start command: %v", err)
		return batchExecResult{exitCode: -1, signaled: true}, nil
	}

	childDone := make(chan error, 1)
	go func() {
		childDone <- cmd.Wait()
	}()

	var waitErr error
	cancelled := false
	select {
	case waitErr = <-childDone:
	case <-lostCh:
		waitErr = stopProcessGroup(cmd, childDone, cfg.KillGrace)
	case <-ctx.Done():
		cancelled = true
		_ = stopProcessGroup(cmd, childDone, cfg.KillGrace)
	}

	if cancelled {
		return batchExecResult{}, ctx.Err()
	}
	if lostLease.Load() {
		return batchExecResult{lostLease: true}, nil
	}
	if waitErr == nil {
		return batchExecResult{exitCode: 0, signaled: false}, nil
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return batchExecResult{
			exitCode: exitErr.ExitCode(),
			signaled: processSignaled(exitErr.ProcessState),
		}, nil
	}
	return batchExecResult{exitCode: -1, signaled: true}, nil
}

func runHeartbeatLoop(
	ctx context.Context,
	client *queueapi.Client,
	buildID, runnerID string,
	leaseID int64,
	ttlMS int64,
	onLost func(),
	logWarn func(string, ...any),
) {
	interval := max(time.Duration(ttlMS/3)*time.Millisecond, minHeartbeatTick)
	retryWait := max(min(interval/2, time.Second), 15*time.Millisecond)
	delay := interval

	for {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		reqCtx, cancel := context.WithTimeout(ctx, max(interval, 2*time.Second))
		resp, err := client.Heartbeat(reqCtx, buildID, queue.HeartbeatCommand{
			RunnerID: runnerID,
			LeaseID:  leaseID,
		})
		cancel()

		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if queueapi.IsTransientError(err) {
				// Survive transient heartbeat failures by retrying faster within TTL.
				logWarn("heartbeat for lease %d failed (%v); retrying", leaseID, err)
				delay = retryWait
				continue
			}
			// Permanent 4xx (400/401/403/404) or protocol error can never succeed:
			// treat as a lost lease so the child stops and the next lease call reports it.
			logWarn("heartbeat for lease %d permanently rejected (%v); stopping batch", leaseID, err)
			onLost()
			return
		}
		if !resp.Held {
			onLost()
			return
		}
		delay = interval
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

// BuildRunReport paginates accepted results from a finished build with zero
// unresolved tests and zero init conflicts, returning a single-machine
// RunReport ready for bunpo merge.
func BuildRunReport(ctx context.Context, client *queueapi.Client, buildID string, st queueapi.StatusResponse) (manifest.RunReport, bool, error) {
	if st.Verdict == queue.VerdictRunning || st.Counts.Unresolved > 0 || len(st.Conflicts) > 0 {
		return manifest.RunReport{}, false, nil
	}

	report := manifest.RunReport{
		Index:    0,
		Machines: 1,
		Tests:    make(timing.Report, st.Counts.Total),
	}

	cursor := 0
	for {
		page, err := client.ListTests(ctx, buildID, queue.ListTestsCommand{
			Cursor: cursor,
			Limit:  queue.MaxTestsPageLimit,
		})
		if err != nil {
			return manifest.RunReport{}, false, err
		}
		for _, tv := range page.Tests {
			if tv.State == queue.StatusPassed && tv.Timed {
				report.Tests[tv.Test] = timing.Observation{Timed: true, MS: tv.DurationMS}
			} else {
				report.Tests[tv.Test] = timing.Observation{Timed: false, MS: 0}
			}
		}
		if page.NextCursor == nil {
			break
		}
		if *page.NextCursor <= cursor {
			return manifest.RunReport{}, false, fmt.Errorf("list tests cursor did not advance (%d -> %d)", cursor, *page.NextCursor)
		}
		cursor = *page.NextCursor
	}
	if len(report.Tests) != st.Counts.Total {
		return manifest.RunReport{}, false, fmt.Errorf("incomplete test list from server: got %d tests, status reported %d", len(report.Tests), st.Counts.Total)
	}
	return report, true, nil
}

// FilteredChildEnv strips BUNPO_QUEUE_TOKEN and BUNPO_QUEUE_READ_TOKEN from
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

func writeBatchFile(root *os.Root, name string, attempts []queue.LeasedAttemptView) (err error) {
	var b strings.Builder
	for _, a := range attempts {
		b.WriteString(a.Test)
		b.WriteByte('\n')
	}
	tmp := fmt.Sprintf(".%s.%s.tmp", name, randomHex16()[:8])
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

func randomHex16() string {
	var buf [16]byte
	_, _ = crand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
