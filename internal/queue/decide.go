package queue

import (
	"cmp"
	"fmt"
	"slices"
)

// InitCommand initializes a build or verifies idempotent agreement.
type InitCommand struct {
	BuildID       string   `json:"build_id"`
	RunnerID      string   `json:"runner_id"`
	MachineIndex  *int     `json:"machine_index"`
	Config        Config   `json:"config"`
	Tests         []string `json:"tests"`
	PredictionsMS []*int64 `json:"predictions_ms"`
}

// InitResponse is returned on 200 OK from init.
type InitResponse struct {
	BuildID    string  `json:"build_id"`
	Created    bool    `json:"created"`
	Digest     string  `json:"digest"`
	Verdict    Verdict `json:"verdict"`
	TestCount  int     `json:"test_count"`
	DeadlineMS int64   `json:"deadline_ms"`
}

// LeaseCommand asks for the next batch of tests.
type LeaseCommand struct {
	RunnerID     string `json:"runner_id"`
	MachineIndex *int   `json:"machine_index"`
	RequestID    string `json:"request_id"`
}

// LeasedAttemptView describes one leased test attempt in a LeaseResponse.
type LeasedAttemptView struct {
	AttemptID  string `json:"attempt_id"`
	TestIndex  int    `json:"test_index"`
	Generation int    `json:"generation"`
	Test       string `json:"test"`
}

// LeaseResponse is returned on 200 OK from lease.
type LeaseResponse struct {
	Status       LeaseResponseStatus `json:"status"`
	LeaseID      int64               `json:"lease_id,omitempty"`
	DeadlineMS   int64               `json:"deadline_ms,omitempty"`
	Attempts     []LeasedAttemptView `json:"attempts,omitempty"`
	RetryAfterMS int64               `json:"retry_after_ms,omitempty"`
	Verdict      Verdict             `json:"verdict,omitempty"`
}

// HeartbeatCommand extends an active lease in memory.
type HeartbeatCommand struct {
	RunnerID string `json:"runner_id"`
	LeaseID  int64  `json:"lease_id"`
}

// HeartbeatResponse is returned on 200 OK from heartbeat.
type HeartbeatResponse struct {
	Held       bool    `json:"held"`
	DeadlineMS int64   `json:"deadline_ms"`
	Verdict    Verdict `json:"verdict"`
}

// ReportResult is one attempt result submitted by a runner.
type ReportResult struct {
	AttemptID  string  `json:"attempt_id"`
	TestIndex  int     `json:"test_index"`
	Generation int     `json:"generation"`
	Outcome    Outcome `json:"outcome"`
	Timed      bool    `json:"timed"`
	DurationMS int64   `json:"duration_ms"`
}

// ReportCommand submits all-or-nothing outcomes for a lease.
type ReportCommand struct {
	RunnerID string         `json:"runner_id"`
	LeaseID  int64          `json:"lease_id"`
	Results  []ReportResult `json:"results"`
}

// ReportDisposition describes how the queue handled one submitted result.
type ReportDisposition struct {
	AttemptID   string      `json:"attempt_id"`
	TestIndex   int         `json:"test_index"`
	Generation  int         `json:"generation"`
	Disposition Disposition `json:"disposition"`
}

// ReportResponse is returned on 200 OK from report.
type ReportResponse struct {
	Verdict Verdict             `json:"verdict"`
	Results []ReportDisposition `json:"results"`
}

// RunnerSummary is one runner's entry in StatusResponse.
type RunnerSummary struct {
	RunnerID      string `json:"runner_id"`
	MachineIndex  *int   `json:"machine_index"`
	FirstSeenMS   int64  `json:"first_seen_ms"`
	Leases        int    `json:"leases"`
	Accepted      int    `json:"accepted"`
	ActiveLeaseID int64  `json:"active_lease_id"`
}

// SlowestTest is one entry in StatusResponse.Slowest.
type SlowestTest struct {
	TestIndex  int    `json:"test_index"`
	Test       string `json:"test"`
	DurationMS int64  `json:"duration_ms"`
}

// ProblemTest is one entry in StatusResponse.Problems.
type ProblemTest struct {
	TestIndex  int        `json:"test_index"`
	Test       string     `json:"test"`
	State      TestStatus `json:"state"`
	Generation int        `json:"generation"`
}

// StatusResponse is returned on 200 OK from GET /builds/{build}.
type StatusResponse struct {
	BuildID            string          `json:"build_id"`
	Digest             string          `json:"digest"`
	Verdict            Verdict         `json:"verdict"`
	CreatedAtMS        int64           `json:"created_at_ms"`
	DeadlineMS         int64           `json:"deadline_ms"`
	FinishedAtMS       *int64          `json:"finished_at_ms"`
	LastSeenMS         int64           `json:"last_seen_ms"`
	Config             Config          `json:"config"`
	Counts             Counts          `json:"counts"`
	ConflictsTruncated bool            `json:"conflicts_truncated"`
	Conflicts          []InitConflict  `json:"conflicts"`
	RunnerCount        int             `json:"runner_count"`
	Runners            []RunnerSummary `json:"runners"`
	Slowest            []SlowestTest   `json:"slowest"`
	ProblemCount       int             `json:"problem_count"`
	Problems           []ProblemTest   `json:"problems"`
}

// ListTestsCommand queries a page of tests in a build.
type ListTestsCommand struct {
	Cursor  int          `json:"cursor"`
	Limit   int          `json:"limit"`
	States  []TestStatus `json:"states"`
	Retried bool         `json:"retried"`
}

// TestView is one test record in ListTestsResponse.
type TestView struct {
	TestIndex        int             `json:"test_index"`
	Test             string          `json:"test"`
	State            TestStatus      `json:"state"`
	Generation       int             `json:"generation"`
	PredictedMS      int64           `json:"predicted_ms"`
	Timed            bool            `json:"timed"`
	DurationMS       int64           `json:"duration_ms"`
	Attempts         []AttemptRecord `json:"attempts"`
	History          []HistoryRecord `json:"history"`
	HistoryTruncated bool            `json:"history_truncated"`
}

// ListTestsResponse is returned on 200 OK from GET /builds/{build}/tests.
type ListTestsResponse struct {
	BuildID    string     `json:"build_id"`
	Verdict    Verdict    `json:"verdict"`
	Tests      []TestView `json:"tests"`
	NextCursor *int       `json:"next_cursor"`
}

// expiryOverlay captures the non-mutating effect of lazy expiry at now.
type expiryOverlay struct {
	nowMS         int64
	finalized     bool
	expiredLeases map[int64]bool
	expiredTests  map[int]TestStatus
	counts        Counts
	events        []Event
}

func evaluateExpiry(s *State, nowMS int64) expiryOverlay {
	now := max(nowMS, s.LastSeenMS)
	ov := expiryOverlay{
		nowMS:  now,
		counts: s.Counts,
	}
	if !s.Initialized || s.Verdict() != VerdictRunning {
		return ov
	}

	if now >= s.DeadlineMS {
		ov.finalized = true
		ov.counts.Unresolved += ov.counts.Pending + ov.counts.Leased
		ov.counts.Pending = 0
		ov.counts.Leased = 0
		ov.events = []Event{{
			Type:      EventFinalized,
			AtMS:      now,
			Finalized: &FinalizedData{Reason: "deadline"},
		}}
		return ov
	}

	for _, leaseID := range s.ActiveLeases {
		lease := &s.Leases[leaseID-1]
		if lease.DeadlineMS > now {
			continue
		}
		if ov.expiredLeases == nil {
			ov.expiredLeases = make(map[int64]bool)
			ov.expiredTests = make(map[int]TestStatus)
		}
		ov.expiredLeases[leaseID] = true
		for _, a := range lease.Attempts {
			t := &s.Tests[a.TestIndex]
			if t.Status != StatusLeased || t.ActiveLeaseID != leaseID {
				continue
			}
			ov.counts.Leased--
			if t.Generation >= s.Config.MaxAttempts {
				ov.expiredTests[a.TestIndex] = StatusUnresolved
				ov.counts.Unresolved++
			} else {
				ov.expiredTests[a.TestIndex] = StatusPending
				ov.counts.Pending++
			}
		}
		ov.events = append(ov.events, Event{
			Type:    EventExpired,
			AtMS:    now,
			Expired: &ExpiredData{LeaseID: leaseID},
		})
	}
	return ov
}

func (ov *expiryOverlay) testStatus(s *State, idx int) TestStatus {
	if ov.finalized {
		st := s.Tests[idx].Status
		if st == StatusPending || st == StatusLeased {
			return StatusUnresolved
		}
		return st
	}
	if st, ok := ov.expiredTests[idx]; ok {
		return st
	}
	return s.Tests[idx].Status
}

func (ov *expiryOverlay) leaseActive(s *State, leaseID int64) bool {
	if leaseID < 1 || leaseID > int64(len(s.Leases)) {
		return false
	}
	if s.Leases[leaseID-1].Status != LeaseActive {
		return false
	}
	if ov.finalized || ov.expiredLeases[leaseID] {
		return false
	}
	return true
}

func (ov *expiryOverlay) verdict(s *State) Verdict {
	return computeVerdict(ov.counts, len(s.Conflicts) > 0)
}

// DecideInit decides an init command without mutating s.
//
// On a 409 init_conflict, DecideInit may return BOTH a conflicted event AND a
// *Error with HTTPStatus 409 (in any build state, including finished builds,
// unless an identical conflict is already stored or the 8-conflict cap is
// full). Callers must persist and apply any returned event before sending 409.
func DecideInit(s *State, nowMS int64, cmd InitCommand) ([]Event, InitResponse, error) {
	if nowMS < 0 {
		return nil, InitResponse{}, errInvalid("now_ms must not be negative")
	}
	if !ValidBuildID(cmd.BuildID) {
		return nil, InitResponse{}, errInvalid("invalid build_id %q", cmd.BuildID)
	}
	if !ValidRunnerID(cmd.RunnerID) {
		return nil, InitResponse{}, errInvalid("invalid runner_id %q", cmd.RunnerID)
	}
	if err := validateMachineIndex(cmd.MachineIndex); err != nil {
		return nil, InitResponse{}, err
	}
	if err := cmd.Config.validate(s.Limits); err != nil {
		return nil, InitResponse{}, err
	}
	if err := validateTestList(cmd.Tests, cmd.PredictionsMS, s.Limits.effective().MaxTests); err != nil {
		return nil, InitResponse{}, err
	}

	digest := configDigest(cmd.Config, cmd.Tests)

	if !s.Initialized {
		preds := make([]*int64, len(cmd.PredictionsMS))
		for i, p := range cmd.PredictionsMS {
			preds[i] = cloneInt64Ptr(p)
		}
		deadlineMS := nowMS + cmd.Config.BuildTimeoutMS
		ev := Event{
			Type: EventInitialized,
			AtMS: nowMS,
			Initialized: &InitializedData{
				BuildID:       cmd.BuildID,
				Digest:        digest,
				Config:        cmd.Config,
				DeadlineMS:    deadlineMS,
				Tests:         slices.Clone(cmd.Tests),
				PredictionsMS: preds,
			},
		}
		resp := InitResponse{
			BuildID:    cmd.BuildID,
			Created:    true,
			Digest:     digest,
			Verdict:    VerdictRunning,
			TestCount:  len(cmd.Tests),
			DeadlineMS: deadlineMS,
		}
		return []Event{ev}, resp, nil
	}

	if s.BuildID != cmd.BuildID {
		return nil, InitResponse{}, errInvalid("state belongs to build %q, not %q", s.BuildID, cmd.BuildID)
	}

	if digest == s.Digest {
		return nil, InitResponse{
			BuildID:    s.BuildID,
			Created:    false,
			Digest:     s.Digest,
			Verdict:    s.Verdict(),
			TestCount:  len(s.Tests),
			DeadlineMS: s.DeadlineMS,
		}, nil
	}

	reason := describeInitDiff(s, cmd)
	err := &Error{
		HTTPStatus: 409,
		Code:       ErrCodeInitConflict,
		Message:    reason,
	}
	if len(s.Conflicts) >= MaxInitConflicts || hasInitConflict(s.Conflicts, cmd.RunnerID, digest) {
		return nil, InitResponse{}, err
	}

	now := max(nowMS, s.LastSeenMS)
	ev := Event{
		Type: EventConflicted,
		AtMS: now,
		Conflicted: &ConflictedData{
			RunnerID:     cmd.RunnerID,
			MachineIndex: cloneIntPtr(cmd.MachineIndex),
			GotDigest:    digest,
			Reason:       reason,
		},
	}
	return []Event{ev}, InitResponse{}, err
}

func describeInitDiff(s *State, cmd InitCommand) string {
	if s.Config != cmd.Config {
		return fmt.Sprintf("config differs from initialized build (got machines=%d ttl=%d timeout=%d attempts=%d batch=%d, want machines=%d ttl=%d timeout=%d attempts=%d batch=%d)",
			cmd.Config.Machines, cmd.Config.LeaseTTLMS, cmd.Config.BuildTimeoutMS, cmd.Config.MaxAttempts, cmd.Config.MaxBatch,
			s.Config.Machines, s.Config.LeaseTTLMS, s.Config.BuildTimeoutMS, s.Config.MaxAttempts, s.Config.MaxBatch)
	}
	if len(cmd.Tests) != len(s.Tests) {
		return fmt.Sprintf("test list differs: got %d tests, initialized build has %d", len(cmd.Tests), len(s.Tests))
	}
	for i, path := range cmd.Tests {
		if path != s.Tests[i].Path {
			return fmt.Sprintf("test list differs at index %d: got %q, initialized build has %q", i, path, s.Tests[i].Path)
		}
	}
	return "init digest differs from initialized build"
}

// DecideLease decides a lease request without mutating s.
//
// When a repeated request_id matches an active lease, DecideLease returns that
// lease with an extended DeadlineMS (and no new leased event); the caller
// updates the in-memory deadline via State.ExtendLease.
func DecideLease(s *State, nowMS int64, cmd LeaseCommand) ([]Event, LeaseResponse, error) {
	if !s.Initialized {
		return nil, LeaseResponse{}, &Error{HTTPStatus: 404, Code: ErrCodeBuildNotFound, Message: "build not initialized"}
	}
	if !ValidRunnerID(cmd.RunnerID) {
		return nil, LeaseResponse{}, errInvalid("invalid runner_id %q", cmd.RunnerID)
	}
	if !ValidRequestID(cmd.RequestID) {
		return nil, LeaseResponse{}, errInvalid("invalid request_id %q", cmd.RequestID)
	}
	if err := validateMachineIndex(cmd.MachineIndex); err != nil {
		return nil, LeaseResponse{}, err
	}

	ov := evaluateExpiry(s, nowMS)
	now := ov.nowMS

	// Check request_id deduplication and single-active-lease-per-runner (after lazy expiry).
	if r, ok := s.Runners[cmd.RunnerID]; ok {
		if r.LastRequestID == cmd.RequestID && r.LastLeaseID > 0 {
			if ov.leaseActive(s, r.LastLeaseID) {
				lease := &s.Leases[r.LastLeaseID-1]
				newDeadline := max(lease.DeadlineMS, now+s.Config.LeaseTTLMS)
				return ov.events, leaseResponseFromLease(s, lease, newDeadline), nil
			}
			return ov.events, LeaseResponse{}, &Error{
				HTTPStatus: 409,
				Code:       ErrCodeStaleRequest,
				Message:    fmt.Sprintf("request_id %q refers to lease %d, which is no longer active", cmd.RequestID, r.LastLeaseID),
			}
		}
		if r.ActiveLeaseID > 0 && ov.leaseActive(s, r.ActiveLeaseID) {
			return ov.events, LeaseResponse{}, &Error{
				HTTPStatus: 409,
				Code:       ErrCodeLeaseHeld,
				Message:    fmt.Sprintf("runner %q already holds active lease %d", cmd.RunnerID, r.ActiveLeaseID),
			}
		}
	}

	if v := ov.verdict(s); v != VerdictRunning {
		return ov.events, LeaseResponse{Status: LeaseDone, Verdict: v}, nil
	}

	if _, exists := s.Runners[cmd.RunnerID]; !exists && len(s.Runners) >= s.Limits.effective().MaxRunners {
		return ov.events, LeaseResponse{}, &Error{
			HTTPStatus: 429,
			Code:       ErrCodeTooManyRunners,
			Message:    fmt.Sprintf("build already has %d distinct runners", len(s.Runners)),
		}
	}

	if ov.counts.Pending == 0 {
		retryAfter := computeWaitMS(s, &ov, now)
		return ov.events, LeaseResponse{Status: LeaseWait, RetryAfterMS: retryAfter}, nil
	}

	chosen := selectBatch(s, &ov)
	slices.Sort(chosen)

	leaseID := int64(len(s.Leases) + 1)
	deadlineMS := now + s.Config.LeaseTTLMS
	attempts := make([]LeaseAttempt, len(chosen))
	views := make([]LeasedAttemptView, len(chosen))
	for i, idx := range chosen {
		gen := s.Tests[idx].Generation + 1
		attempts[i] = LeaseAttempt{TestIndex: idx, Generation: gen}
		views[i] = LeasedAttemptView{
			AttemptID:  FormatAttemptID(idx, gen),
			TestIndex:  idx,
			Generation: gen,
			Test:       s.Tests[idx].Path,
		}
	}

	events := append(slices.Clone(ov.events), Event{
		Type: EventLeased,
		AtMS: now,
		Leased: &LeasedData{
			LeaseID:      leaseID,
			RunnerID:     cmd.RunnerID,
			MachineIndex: cloneIntPtr(cmd.MachineIndex),
			RequestID:    cmd.RequestID,
			DeadlineMS:   deadlineMS,
			Attempts:     attempts,
		},
	})
	resp := LeaseResponse{
		Status:     LeaseGranted,
		LeaseID:    leaseID,
		DeadlineMS: deadlineMS,
		Attempts:   views,
	}
	return events, resp, nil
}

func leaseResponseFromLease(s *State, lease *Lease, deadlineMS int64) LeaseResponse {
	views := make([]LeasedAttemptView, len(lease.Attempts))
	for i, a := range lease.Attempts {
		views[i] = LeasedAttemptView{
			AttemptID:  FormatAttemptID(a.TestIndex, a.Generation),
			TestIndex:  a.TestIndex,
			Generation: a.Generation,
			Test:       s.Tests[a.TestIndex].Path,
		}
	}
	return LeaseResponse{
		Status:     LeaseGranted,
		LeaseID:    lease.LeaseID,
		DeadlineMS: deadlineMS,
		Attempts:   views,
	}
}

func selectBatch(s *State, ov *expiryOverlay) []int {
	var firstPending []int
	firstRetry := -1
	var remainingMS int64
	totalPending := 0

	for _, idx := range s.DealOrder {
		if ov.testStatus(s, idx) != StatusPending {
			continue
		}
		totalPending++
		remainingMS += s.Tests[idx].EffectiveMS
		if s.Tests[idx].Generation == 0 {
			firstPending = append(firstPending, idx)
		} else if firstRetry == -1 {
			firstRetry = idx
		}
	}

	// First attempts precede retries; retries are always leased alone.
	if len(firstPending) == 0 {
		return []int{firstRetry}
	}

	denom := int64(2 * s.Config.Machines)
	maxBatch := s.Config.MaxBatch

	if remainingMS == 0 {
		targetCount := int((int64(totalPending) + denom - 1) / denom)
		targetCount = max(1, min(targetCount, maxBatch, len(firstPending)))
		return slices.Clone(firstPending[:targetCount])
	}

	targetMS := (remainingMS + denom - 1) / denom
	var batch []int
	var batchMS int64
	for _, idx := range firstPending {
		batch = append(batch, idx)
		batchMS += s.Tests[idx].EffectiveMS
		if batchMS >= targetMS || len(batch) >= maxBatch {
			break
		}
	}
	return batch
}

func computeWaitMS(s *State, ov *expiryOverlay, now int64) int64 {
	var earliestDeadline int64
	var latestExpectedEnd int64
	found := false
	for _, id := range s.ActiveLeases {
		if !ov.leaseActive(s, id) {
			continue
		}
		l := &s.Leases[id-1]
		expectedEnd := l.LeasedAtMS
		for _, a := range l.Attempts {
			expectedEnd += s.Tests[a.TestIndex].EffectiveMS
		}
		if !found || l.DeadlineMS < earliestDeadline {
			earliestDeadline = l.DeadlineMS
		}
		if !found || expectedEnd > latestExpectedEnd {
			latestExpectedEnd = expectedEnd
		}
		found = true
	}
	maxWait := min(s.Config.LeaseTTLMS/2, MaxWaitMS)
	if maxWait < 1 {
		maxWait = 1
	}
	minWait := min(MinWaitMS, maxWait)
	if !found {
		return minWait
	}
	var wait int64
	if now < latestExpectedEnd {
		wait = latestExpectedEnd - now
	} else {
		// Overdue or no predictions (EffectiveMS == 0): back off linearly so
		// idle machines do not hammer the server on a long overdue test.
		wait = (now - latestExpectedEnd) / 4
	}
	wait = min(wait, earliestDeadline-now)
	return max(minWait, min(wait, maxWait))
}

// DecideHeartbeat decides a heartbeat without mutating s.
//
// Healthy heartbeats return zero events and Held=true with the new in-memory
// DeadlineMS; the caller applies it via State.ExtendLease.
func DecideHeartbeat(s *State, nowMS int64, cmd HeartbeatCommand) ([]Event, HeartbeatResponse, error) {
	if !s.Initialized {
		return nil, HeartbeatResponse{}, &Error{HTTPStatus: 404, Code: ErrCodeBuildNotFound, Message: "build not initialized"}
	}
	if !ValidRunnerID(cmd.RunnerID) {
		return nil, HeartbeatResponse{}, errInvalid("invalid runner_id %q", cmd.RunnerID)
	}
	if cmd.LeaseID < 1 || cmd.LeaseID > int64(len(s.Leases)) {
		return nil, HeartbeatResponse{}, &Error{HTTPStatus: 400, Code: ErrCodeUnknownLease, Message: fmt.Sprintf("unknown lease_id %d", cmd.LeaseID)}
	}
	lease := &s.Leases[cmd.LeaseID-1]
	if lease.RunnerID != cmd.RunnerID {
		return nil, HeartbeatResponse{}, &Error{HTTPStatus: 400, Code: ErrCodeRunnerMismatch, Message: fmt.Sprintf("lease %d belongs to another runner", cmd.LeaseID)}
	}

	now := max(nowMS, s.LastSeenMS)

	// Fast path: healthy active lease before build deadline writes zero events.
	if s.Verdict() == VerdictRunning && now < s.DeadlineMS && lease.Status == LeaseActive && lease.DeadlineMS > now {
		newDeadline := max(lease.DeadlineMS, now+s.Config.LeaseTTLMS)
		return nil, HeartbeatResponse{
			Held:       true,
			DeadlineMS: newDeadline,
			Verdict:    VerdictRunning,
		}, nil
	}

	// Build deadline reached, target lease expired, or lease already inactive:
	// run full lazy expiry in lease_id order so persisted state stays ordered.
	ov := evaluateExpiry(s, nowMS)
	return ov.events, HeartbeatResponse{
		Held:       false,
		DeadlineMS: 0,
		Verdict:    ov.verdict(s),
	}, nil
}

// DecideReport decides an all-or-nothing batch report without mutating s.
func DecideReport(s *State, nowMS int64, cmd ReportCommand) ([]Event, ReportResponse, error) {
	if !s.Initialized {
		return nil, ReportResponse{}, &Error{HTTPStatus: 404, Code: ErrCodeBuildNotFound, Message: "build not initialized"}
	}
	if !ValidRunnerID(cmd.RunnerID) {
		return nil, ReportResponse{}, errInvalid("invalid runner_id %q", cmd.RunnerID)
	}
	if cmd.LeaseID < 1 || cmd.LeaseID > int64(len(s.Leases)) {
		return nil, ReportResponse{}, &Error{HTTPStatus: 400, Code: ErrCodeUnknownLease, Message: fmt.Sprintf("unknown lease_id %d", cmd.LeaseID)}
	}
	lease := &s.Leases[cmd.LeaseID-1]
	if lease.RunnerID != cmd.RunnerID {
		return nil, ReportResponse{}, &Error{HTTPStatus: 400, Code: ErrCodeRunnerMismatch, Message: fmt.Sprintf("lease %d belongs to another runner", cmd.LeaseID)}
	}
	if len(cmd.Results) != len(lease.Attempts) {
		return nil, ReportResponse{}, errInvalid("report has %d results, lease %d has %d attempts", len(cmd.Results), cmd.LeaseID, len(lease.Attempts))
	}

	// Step 1: validate every result and align to lease attempt order (all-or-nothing).
	byIndex := make(map[int]ReportResult, len(cmd.Results))
	for _, res := range cmd.Results {
		if res.TestIndex < 0 || res.TestIndex >= len(s.Tests) {
			return nil, ReportResponse{}, errInvalid("test_index %d out of range", res.TestIndex)
		}
		t := &s.Tests[res.TestIndex]
		if res.Generation < 1 || res.Generation > t.Generation {
			return nil, ReportResponse{}, errInvalid("test %d generation %d outside 1..%d", res.TestIndex, res.Generation, t.Generation)
		}
		wantAttemptID := FormatAttemptID(res.TestIndex, res.Generation)
		if res.AttemptID != "" && res.AttemptID != wantAttemptID {
			return nil, ReportResponse{}, errInvalid("attempt_id %q does not match %q", res.AttemptID, wantAttemptID)
		}
		if err := validateReportValues(res.Outcome, res.Timed, res.DurationMS); err != nil {
			return nil, ReportResponse{}, err
		}
		if _, dup := byIndex[res.TestIndex]; dup {
			return nil, ReportResponse{}, errInvalid("duplicate result for test_index %d", res.TestIndex)
		}
		byIndex[res.TestIndex] = res
	}

	ordered := make([]ReportResult, len(lease.Attempts))
	for i, want := range lease.Attempts {
		got, ok := byIndex[want.TestIndex]
		if !ok || got.Generation != want.Generation {
			return nil, ReportResponse{}, errInvalid("report must contain attempt %s of lease %d", FormatAttemptID(want.TestIndex, want.Generation), cmd.LeaseID)
		}
		ordered[i] = got
	}

	// Step 2: lazy expiry at clamped now.
	ov := evaluateExpiry(s, nowMS)
	now := ov.nowMS
	leaseActive := ov.leaseActive(s, cmd.LeaseID)

	// Step 3: evaluate each attempt in the protocol's fixed precedence order.
	var stored []ReportedAttempt
	disps := make([]ReportDisposition, len(ordered))
	counts := ov.counts
	allAccepted := leaseActive

	for i, res := range ordered {
		t := &s.Tests[res.TestIndex]
		att := &t.Attempts[res.Generation-1]
		var disp Disposition
		persist := false

		switch {
		case att.Status.HasReportedOutcome():
			if att.Status == outcomeToAttemptStatus(res.Outcome) && att.Timed == res.Timed && att.DurationMS == res.DurationMS {
				disp = DispositionDuplicate
			} else {
				disp = DispositionConflict
				rec := HistoryRecord{
					AttemptID:   FormatAttemptID(res.TestIndex, res.Generation),
					Generation:  res.Generation,
					LeaseID:     cmd.LeaseID,
					RunnerID:    cmd.RunnerID,
					Outcome:     res.Outcome,
					Timed:       res.Timed,
					DurationMS:  res.DurationMS,
					Disposition: disp,
				}
				if len(t.History) < MaxTestHistory && !hasHistoryRecord(t.History, rec) {
					persist = true
				}
			}
			allAccepted = false

		case leaseActive && res.Generation == t.Generation && ov.testStatus(s, res.TestIndex) == StatusLeased && t.ActiveLeaseID == cmd.LeaseID:
			disp = DispositionAccepted
			persist = true
			counts.Leased--
			switch res.Outcome {
			case OutcomePassed:
				counts.Passed++
			case OutcomeFailed:
				counts.Failed++
			case OutcomeSkipped:
				counts.Skipped++
			case OutcomeError:
				if res.Generation >= s.Config.MaxAttempts {
					counts.Unresolved++
				} else {
					counts.Pending++
				}
			}

		default:
			disp = DispositionStale
			rec := HistoryRecord{
				AttemptID:   FormatAttemptID(res.TestIndex, res.Generation),
				Generation:  res.Generation,
				LeaseID:     cmd.LeaseID,
				RunnerID:    cmd.RunnerID,
				Outcome:     res.Outcome,
				Timed:       res.Timed,
				DurationMS:  res.DurationMS,
				Disposition: disp,
			}
			if len(t.History) < MaxTestHistory && !hasHistoryRecord(t.History, rec) {
				persist = true
			}
			allAccepted = false
		}

		if persist {
			stored = append(stored, ReportedAttempt{
				TestIndex:   res.TestIndex,
				Generation:  res.Generation,
				Outcome:     res.Outcome,
				Timed:       res.Timed,
				DurationMS:  res.DurationMS,
				Disposition: disp,
			})
		}
		disps[i] = ReportDisposition{
			AttemptID:   FormatAttemptID(res.TestIndex, res.Generation),
			TestIndex:   res.TestIndex,
			Generation:  res.Generation,
			Disposition: disp,
		}
	}

	events := slices.Clone(ov.events)
	if len(stored) > 0 {
		events = append(events, Event{
			Type: EventReported,
			AtMS: now,
			Reported: &ReportedData{
				LeaseID:    cmd.LeaseID,
				RunnerID:   cmd.RunnerID,
				CloseLease: allAccepted,
				Results:    stored,
			},
		})
	}

	return events, ReportResponse{
		Verdict: computeVerdict(counts, len(s.Conflicts) > 0),
		Results: disps,
	}, nil
}

// DecideStatus decides a read-only build status query without mutating s.
//
// Only if lazy expiry at nowMS transitions the build from running to terminal
// does DecideStatus return events; the caller must persist and apply them
// before returning the terminal verdict.
func DecideStatus(s *State, nowMS int64) ([]Event, StatusResponse, error) {
	if !s.Initialized {
		return nil, StatusResponse{}, &Error{HTTPStatus: 404, Code: ErrCodeBuildNotFound, Message: "build not initialized"}
	}

	ov := evaluateExpiry(s, nowMS)
	verdictChanged := s.Verdict() == VerdictRunning && ov.verdict(s) != VerdictRunning

	var events []Event
	useOverlay := false
	if verdictChanged {
		events = ov.events
		useOverlay = true
	}

	counts := s.Counts
	verdict := s.Verdict()
	lastSeen := s.LastSeenMS
	var finishedAt *int64
	if s.FinishedAtMS > 0 {
		v := s.FinishedAtMS
		finishedAt = &v
	}
	if useOverlay {
		counts = ov.counts
		verdict = ov.verdict(s)
		lastSeen = ov.nowMS
		v := ov.nowMS
		finishedAt = &v
	}

	conflicts := make([]InitConflict, len(s.Conflicts))
	for i, c := range s.Conflicts {
		c.MachineIndex = cloneIntPtr(c.MachineIndex)
		conflicts[i] = c
	}

	runnerCap := min(len(s.RunnerOrder), MaxStatusRunners)
	runners := make([]RunnerSummary, 0, runnerCap)
	for _, id := range s.RunnerOrder[:runnerCap] {
		r := s.Runners[id]
		activeID := r.ActiveLeaseID
		if useOverlay && activeID > 0 && !ov.leaseActive(s, activeID) {
			activeID = 0
		}
		runners = append(runners, RunnerSummary{
			RunnerID:      r.RunnerID,
			MachineIndex:  cloneIntPtr(r.MachineIndex),
			FirstSeenMS:   r.FirstSeenMS,
			Leases:        r.Leases,
			Accepted:      r.Accepted,
			ActiveLeaseID: activeID,
		})
	}

	var slowest []SlowestTest
	var failedProblems, unresolvedProblems []ProblemTest
	for i := range s.Tests {
		t := &s.Tests[i]
		st := t.Status
		if useOverlay {
			st = ov.testStatus(s, i)
		}
		if st == StatusPassed && t.Timed {
			slowest = append(slowest, SlowestTest{
				TestIndex:  t.Index,
				Test:       t.Path,
				DurationMS: t.DurationMS,
			})
		}
		switch st {
		case StatusFailed:
			failedProblems = append(failedProblems, ProblemTest{
				TestIndex:  t.Index,
				Test:       t.Path,
				State:      st,
				Generation: t.Generation,
			})
		case StatusUnresolved:
			unresolvedProblems = append(unresolvedProblems, ProblemTest{
				TestIndex:  t.Index,
				Test:       t.Path,
				State:      st,
				Generation: t.Generation,
			})
		}
	}

	slices.SortFunc(slowest, func(a, b SlowestTest) int {
		return cmp.Or(cmp.Compare(b.DurationMS, a.DurationMS), cmp.Compare(a.TestIndex, b.TestIndex))
	})
	if len(slowest) > MaxStatusSlowest {
		slowest = slowest[:MaxStatusSlowest]
	}
	if slowest == nil {
		slowest = []SlowestTest{}
	}

	problemCount := len(failedProblems) + len(unresolvedProblems)
	problems := make([]ProblemTest, 0, min(problemCount, MaxStatusProblems))
	for _, p := range failedProblems {
		if len(problems) >= MaxStatusProblems {
			break
		}
		problems = append(problems, p)
	}
	for _, p := range unresolvedProblems {
		if len(problems) >= MaxStatusProblems {
			break
		}
		problems = append(problems, p)
	}

	return events, StatusResponse{
		BuildID:            s.BuildID,
		Digest:             s.Digest,
		Verdict:            verdict,
		CreatedAtMS:        s.CreatedAtMS,
		DeadlineMS:         s.DeadlineMS,
		FinishedAtMS:       finishedAt,
		LastSeenMS:         lastSeen,
		Config:             s.Config,
		Counts:             counts,
		ConflictsTruncated: s.ConflictsTruncated,
		Conflicts:          conflicts,
		RunnerCount:        len(s.Runners),
		Runners:            runners,
		Slowest:            slowest,
		ProblemCount:       problemCount,
		Problems:           problems,
	}, nil
}

// DecideListTests returns a filtered, paginated slice of tests without mutating s.
// Like DecideStatus, it emits events only if lazy expiry transitions the build
// from running to terminal.
func DecideListTests(s *State, nowMS int64, cmd ListTestsCommand) ([]Event, ListTestsResponse, error) {
	if !s.Initialized {
		return nil, ListTestsResponse{}, &Error{HTTPStatus: 404, Code: ErrCodeBuildNotFound, Message: "build not initialized"}
	}
	if cmd.Cursor < 0 {
		return nil, ListTestsResponse{}, errInvalid("cursor %d must not be negative", cmd.Cursor)
	}
	limit := cmd.Limit
	if limit == 0 {
		limit = DefaultTestsPageLimit
	}
	if limit < 1 || limit > MaxTestsPageLimit {
		return nil, ListTestsResponse{}, errInvalid("limit %d is outside 1..%d", limit, MaxTestsPageLimit)
	}
	var stateFilter map[TestStatus]bool
	if len(cmd.States) > 0 {
		stateFilter = make(map[TestStatus]bool, len(cmd.States))
		for _, st := range cmd.States {
			switch st {
			case StatusPending, StatusLeased, StatusPassed, StatusFailed, StatusSkipped, StatusUnresolved:
				stateFilter[st] = true
			default:
				return nil, ListTestsResponse{}, errInvalid("invalid state filter %q", st)
			}
		}
	}

	ov := evaluateExpiry(s, nowMS)
	useOverlay := s.Verdict() == VerdictRunning && ov.verdict(s) != VerdictRunning
	var events []Event
	verdict := s.Verdict()
	if useOverlay {
		events = ov.events
		verdict = ov.verdict(s)
	}

	tests := make([]TestView, 0, min(limit, max(0, len(s.Tests)-cmd.Cursor)))
	var nextCursor *int

	for i := cmd.Cursor; i < len(s.Tests); i++ {
		t := &s.Tests[i]
		st := t.Status
		if useOverlay {
			st = ov.testStatus(s, i)
		}
		if stateFilter != nil && !stateFilter[st] {
			continue
		}
		if cmd.Retried && t.Generation <= 1 && len(t.History) == 0 && !t.HistoryTruncated {
			continue
		}
		if len(tests) == limit {
			c := i
			nextCursor = &c
			break
		}

		attempts := slices.Clone(t.Attempts)
		if attempts == nil {
			attempts = []AttemptRecord{}
		}
		if useOverlay && t.Status == StatusLeased && st == StatusUnresolved && len(attempts) > 0 {
			last := &attempts[len(attempts)-1]
			if last.Status == AttemptLeased {
				last.Status = AttemptExpired
				last.FinishedAtMS = ov.nowMS
			}
		}
		history := slices.Clone(t.History)
		if history == nil {
			history = []HistoryRecord{}
		}
		tests = append(tests, TestView{
			TestIndex:        t.Index,
			Test:             t.Path,
			State:            st,
			Generation:       t.Generation,
			PredictedMS:      t.EffectiveMS,
			Timed:            t.Timed,
			DurationMS:       t.DurationMS,
			Attempts:         attempts,
			History:          history,
			HistoryTruncated: t.HistoryTruncated,
		})
	}

	return events, ListTestsResponse{
		BuildID:    s.BuildID,
		Verdict:    verdict,
		Tests:      tests,
		NextCursor: nextCursor,
	}, nil
}
