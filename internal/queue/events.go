package queue

import (
	"cmp"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"

	"github.com/nuggocto/bunpo/internal/timing"
)

// EventType names the persisted event kinds.
type EventType string

const (
	EventInitialized EventType = "initialized"
	EventConflicted  EventType = "conflicted"
	EventLeased      EventType = "leased"
	EventReported    EventType = "reported"
	EventExpired     EventType = "expired"
	EventFinalized   EventType = "finalized"
)

// Event is one persisted state transition. Only the field matching Type is non-nil.
type Event struct {
	Type        EventType        `json:"type"`
	AtMS        int64            `json:"at_ms"`
	Initialized *InitializedData `json:"initialized,omitempty"`
	Conflicted  *ConflictedData  `json:"conflicted,omitempty"`
	Leased      *LeasedData      `json:"leased,omitempty"`
	Reported    *ReportedData    `json:"reported,omitempty"`
	Expired     *ExpiredData     `json:"expired,omitempty"`
	Finalized   *FinalizedData   `json:"finalized,omitempty"`
}

// InitializedData is the payload of an initialized event.
type InitializedData struct {
	BuildID       string   `json:"build_id"`
	Digest        string   `json:"digest"`
	Config        Config   `json:"config"`
	DeadlineMS    int64    `json:"deadline_ms"`
	Tests         []string `json:"tests"`
	PredictionsMS []*int64 `json:"predictions_ms"`
}

// ConflictedData is the payload of a conflicted event.
type ConflictedData struct {
	RunnerID     string `json:"runner_id"`
	MachineIndex *int   `json:"machine_index"`
	GotDigest    string `json:"got_digest"`
	Reason       string `json:"reason"`
}

// LeasedData is the payload of a leased event.
type LeasedData struct {
	LeaseID      int64          `json:"lease_id"`
	RunnerID     string         `json:"runner_id"`
	MachineIndex *int           `json:"machine_index"`
	RequestID    string         `json:"request_id"`
	DeadlineMS   int64          `json:"deadline_ms"`
	Attempts     []LeaseAttempt `json:"attempts"`
}

// ReportedAttempt is one evaluated attempt inside a reported event.
type ReportedAttempt struct {
	TestIndex   int         `json:"test_index"`
	Generation  int         `json:"generation"`
	Outcome     Outcome     `json:"outcome"`
	Timed       bool        `json:"timed"`
	DurationMS  int64       `json:"duration_ms"`
	Disposition Disposition `json:"disposition"`
}

// ReportedData is the payload of a reported event.
type ReportedData struct {
	LeaseID    int64             `json:"lease_id"`
	RunnerID   string            `json:"runner_id"`
	CloseLease bool              `json:"close_lease"`
	Results    []ReportedAttempt `json:"results"`
}

// ExpiredData is the payload of an expired event.
type ExpiredData struct {
	LeaseID int64 `json:"lease_id"`
}

// FinalizedData is the payload of a finalized event.
type FinalizedData struct {
	Reason string `json:"reason"`
}

// EncodeEvent marshals one event deterministically.
func EncodeEvent(ev Event) ([]byte, error) {
	return json.Marshal(ev, json.Deterministic(true))
}

// DecodeEvent strictly decodes one event.
func DecodeEvent(data []byte) (Event, error) {
	var ev Event
	if err := json.Unmarshal(data, &ev, json.RejectUnknownMembers(true)); err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}

	return ev, nil
}

// Apply transitions s by one persisted event. Apply is pure with respect to
// time: it never reads a clock and only uses ev.AtMS.
func (s *State) Apply(ev Event) error {
	if ev.AtMS < 0 {
		return errors.New("event at_ms must not be negative")
	}

	if s.Initialized && ev.AtMS < s.LastSeenMS {
		return fmt.Errorf("event at_ms %d precedes last_seen_ms %d", ev.AtMS, s.LastSeenMS)
	}

	switch ev.Type {
	case EventInitialized:
		return s.applyInitialized(ev.AtMS, ev.Initialized)
	case EventConflicted:
		return s.applyConflicted(ev.AtMS, ev.Conflicted)
	case EventLeased:
		return s.applyLeased(ev.AtMS, ev.Leased)
	case EventReported:
		return s.applyReported(ev.AtMS, ev.Reported)
	case EventExpired:
		return s.applyExpired(ev.AtMS, ev.Expired)
	case EventFinalized:
		return s.applyFinalized(ev.AtMS, ev.Finalized)
	default:
		return fmt.Errorf("unknown event type %q", ev.Type)
	}
}

func (s *State) applyInitialized(atMS int64, d *InitializedData) error {
	if s.Initialized {
		return errors.New("build already initialized")
	}

	if d == nil {
		return errors.New("initialized event missing payload")
	}

	if !ValidBuildID(d.BuildID) {
		return fmt.Errorf("invalid build_id %q", d.BuildID)
	}

	if err := d.Config.validate(s.Limits); err != nil {
		return err
	}

	if err := validateTestList(d.Tests, d.PredictionsMS, s.Limits.effective().MaxTests); err != nil {
		return err
	}

	wantDigest := configDigest(d.Config, d.Tests)
	if d.Digest != wantDigest {
		return fmt.Errorf("initialized digest %q does not match computed %q", d.Digest, wantDigest)
	}

	if d.DeadlineMS < atMS {
		return fmt.Errorf("initialized deadline_ms %d precedes at_ms %d", d.DeadlineMS, atMS)
	}

	eff := computeEffectivePredictions(d.PredictionsMS)
	tests := make([]TestState, len(d.Tests))
	order := make([]int, len(d.Tests))
	for i, path := range d.Tests {
		tests[i] = TestState{
			Index:       i,
			Path:        path,
			PredictedMS: cloneInt64Ptr(d.PredictionsMS[i]),
			EffectiveMS: eff[i],
			Status:      StatusPending,
		}
		order[i] = i
	}

	slices.SortStableFunc(order, func(a, b int) int {
		return cmp.Or(cmp.Compare(eff[b], eff[a]), cmp.Compare(a, b))
	})

	s.Initialized = true
	s.BuildID = d.BuildID
	s.Digest = d.Digest
	s.Config = d.Config
	s.CreatedAtMS = atMS
	s.DeadlineMS = d.DeadlineMS
	s.LastSeenMS = atMS
	s.Tests = tests
	s.DealOrder = order
	s.Counts = Counts{Total: len(tests), Pending: len(tests)}
	if s.Runners == nil {
		s.Runners = make(map[string]*RunnerState)
	}

	return nil
}

func (s *State) applyConflicted(atMS int64, d *ConflictedData) error {
	if !s.Initialized {
		return errors.New("conflicted event before initialized")
	}

	if d == nil {
		return errors.New("conflicted event missing payload")
	}

	if !ValidRunnerID(d.RunnerID) {
		return fmt.Errorf("invalid runner_id %q", d.RunnerID)
	}

	if err := validateMachineIndex(d.MachineIndex); err != nil {
		return err
	}

	if d.GotDigest == "" || d.GotDigest == s.Digest {
		return errors.New("conflicted event must carry a differing got_digest")
	}

	if len(s.Conflicts) >= MaxInitConflicts {
		return errors.New("conflicted event exceeds max stored init conflicts")
	}

	if hasInitConflict(s.Conflicts, d.RunnerID, d.GotDigest) {
		return errors.New("duplicate conflicted event")
	}

	s.LastSeenMS = max(s.LastSeenMS, atMS)
	s.Conflicts = append(s.Conflicts, InitConflict{
		RunnerID:     d.RunnerID,
		MachineIndex: cloneIntPtr(d.MachineIndex),
		GotDigest:    d.GotDigest,
		Reason:       d.Reason,
		AtMS:         atMS,
	})
	if len(s.Conflicts) == MaxInitConflicts {
		s.ConflictsTruncated = true
	}

	return nil
}

func hasInitConflict(conflicts []InitConflict, runnerID, gotDigest string) bool {
	for _, c := range conflicts {
		if c.RunnerID == runnerID && c.GotDigest == gotDigest {
			return true
		}
	}

	return false
}

func (s *State) applyLeased(atMS int64, d *LeasedData) error {
	if !s.Initialized {
		return errors.New("leased event before initialized")
	}

	if d == nil {
		return errors.New("leased event missing payload")
	}

	wantID := int64(len(s.Leases) + 1)
	if d.LeaseID != wantID {
		return fmt.Errorf("leased event lease_id %d, want %d", d.LeaseID, wantID)
	}

	if !ValidRunnerID(d.RunnerID) || !ValidRequestID(d.RequestID) {
		return errors.New("leased event has invalid runner_id or request_id")
	}

	if err := validateMachineIndex(d.MachineIndex); err != nil {
		return err
	}

	if d.DeadlineMS < atMS {
		return errors.New("leased deadline_ms precedes at_ms")
	}

	if len(d.Attempts) == 0 || len(d.Attempts) > s.Config.MaxBatch {
		return fmt.Errorf("leased attempts count %d outside 1..%d", len(d.Attempts), s.Config.MaxBatch)
	}

	if r, exists := s.Runners[d.RunnerID]; !exists && len(s.Runners) >= s.Limits.effective().MaxRunners {
		return errors.New("runner limit exceeded")
	} else if exists && r.ActiveLeaseID != 0 {
		return fmt.Errorf("runner %q already holds active lease %d", d.RunnerID, r.ActiveLeaseID)
	}

	// Validate every attempt before mutating state.
	for i, a := range d.Attempts {
		if a.TestIndex < 0 || a.TestIndex >= len(s.Tests) {
			return fmt.Errorf("leased test_index %d out of range", a.TestIndex)
		}

		if i > 0 && d.Attempts[i-1].TestIndex >= a.TestIndex {
			return errors.New("leased attempts must be unique and sorted by test_index")
		}

		t := &s.Tests[a.TestIndex]
		if t.Status != StatusPending {
			return fmt.Errorf("test %d is %s, not pending", a.TestIndex, t.Status)
		}

		if a.Generation != t.Generation+1 || a.Generation > s.Config.MaxAttempts {
			return fmt.Errorf("test %d generation %d invalid (current %d, max %d)", a.TestIndex, a.Generation, t.Generation, s.Config.MaxAttempts)
		}
	}

	s.LastSeenMS = max(s.LastSeenMS, atMS)
	attempts := slices.Clone(d.Attempts)
	lease := Lease{
		LeaseID:      d.LeaseID,
		RunnerID:     d.RunnerID,
		MachineIndex: cloneIntPtr(d.MachineIndex),
		RequestID:    d.RequestID,
		Status:       LeaseActive,
		LeasedAtMS:   atMS,
		DeadlineMS:   d.DeadlineMS,
		Attempts:     attempts,
	}
	s.Leases = append(s.Leases, lease)
	s.ActiveLeases = append(s.ActiveLeases, d.LeaseID)

	r := s.ensureRunner(d.RunnerID, d.MachineIndex, atMS)
	r.Leases++
	r.ActiveLeaseID = d.LeaseID
	r.LastRequestID = d.RequestID
	r.LastLeaseID = d.LeaseID

	for _, a := range attempts {
		t := &s.Tests[a.TestIndex]
		t.Status = StatusLeased
		t.Generation = a.Generation
		t.ActiveLeaseID = d.LeaseID
		t.Attempts = append(t.Attempts, AttemptRecord{
			AttemptID:  FormatAttemptID(a.TestIndex, a.Generation),
			Generation: a.Generation,
			LeaseID:    d.LeaseID,
			RunnerID:   d.RunnerID,
			Status:     AttemptLeased,
			LeasedAtMS: atMS,
		})
		s.Counts.Pending--
		s.Counts.Leased++
	}

	return nil
}

func (s *State) applyReported(atMS int64, d *ReportedData) error {
	if !s.Initialized {
		return errors.New("reported event before initialized")
	}

	if d == nil {
		return errors.New("reported event missing payload")
	}

	if d.LeaseID < 1 || d.LeaseID > int64(len(s.Leases)) {
		return fmt.Errorf("reported unknown lease_id %d", d.LeaseID)
	}

	lease := &s.Leases[d.LeaseID-1]
	if lease.RunnerID != d.RunnerID {
		return errors.New("reported runner_id mismatch")
	}

	if len(d.Results) == 0 || len(d.Results) > len(lease.Attempts) {
		return errors.New("reported results length is invalid for lease")
	}

	if d.CloseLease && (lease.Status != LeaseActive || len(d.Results) != len(lease.Attempts)) {
		return fmt.Errorf("cannot close lease %d with status %s and %d results", d.LeaseID, lease.Status, len(d.Results))
	}

	if !d.CloseLease && lease.Status == LeaseActive {
		return fmt.Errorf("reported event for active lease %d must set close_lease", d.LeaseID)
	}

	// Build lookup of lease attempts and validate every result before mutating state.
	leaseAttempts := make(map[int]int, len(lease.Attempts))
	for _, a := range lease.Attempts {
		leaseAttempts[a.TestIndex] = a.Generation
	}

	for i, res := range d.Results {
		if i > 0 && d.Results[i-1].TestIndex >= res.TestIndex {
			return errors.New("reported results must be unique and sorted by test_index")
		}

		wantGen, ok := leaseAttempts[res.TestIndex]
		if !ok || res.Generation != wantGen {
			return fmt.Errorf("reported result %d does not match lease attempt", i)
		}

		if err := validateReportValues(res.Outcome, res.Timed, res.DurationMS); err != nil {
			return err
		}

		t := &s.Tests[res.TestIndex]
		if res.Generation < 1 || res.Generation > len(t.Attempts) {
			return fmt.Errorf("reported generation %d out of range", res.Generation)
		}

		switch res.Disposition {
		case DispositionAccepted:
			if !d.CloseLease || t.Status != StatusLeased || t.ActiveLeaseID != d.LeaseID || t.Generation != res.Generation {
				return fmt.Errorf("cannot accept result for test %d", res.TestIndex)
			}
		case DispositionStale, DispositionConflict:
			if d.CloseLease {
				return fmt.Errorf("closing reported event cannot contain %s result", res.Disposition)
			}

			if len(t.History) >= MaxTestHistory {
				return fmt.Errorf("test %d history is already full", res.TestIndex)
			}

			rec := HistoryRecord{
				AttemptID:   FormatAttemptID(res.TestIndex, res.Generation),
				Generation:  res.Generation,
				LeaseID:     d.LeaseID,
				RunnerID:    d.RunnerID,
				Outcome:     res.Outcome,
				Timed:       res.Timed,
				DurationMS:  res.DurationMS,
				Disposition: res.Disposition,
			}
			if hasHistoryRecord(t.History, rec) {
				return fmt.Errorf("duplicate %s history record for test %d", res.Disposition, res.TestIndex)
			}
		default:
			return fmt.Errorf("invalid persisted disposition %q", res.Disposition)
		}
	}

	wasRunning := s.Verdict() == VerdictRunning
	s.LastSeenMS = max(s.LastSeenMS, atMS)
	r := s.Runners[d.RunnerID]

	for _, res := range d.Results {
		t := &s.Tests[res.TestIndex]
		switch res.Disposition {
		case DispositionAccepted:
			att := &t.Attempts[res.Generation-1]
			att.Status = outcomeToAttemptStatus(res.Outcome)
			att.Timed = res.Timed
			att.DurationMS = res.DurationMS
			att.FinishedAtMS = atMS

			t.ActiveLeaseID = 0
			s.Counts.Leased--

			if r != nil {
				r.Accepted++
			}

			switch res.Outcome {
			case OutcomePassed:
				t.Status = StatusPassed
				t.Timed = res.Timed
				t.DurationMS = res.DurationMS
				s.Counts.Passed++
			case OutcomeFailed:
				t.Status = StatusFailed
				s.Counts.Failed++
			case OutcomeSkipped:
				t.Status = StatusSkipped
				s.Counts.Skipped++
			case OutcomeError:
				if t.Generation >= s.Config.MaxAttempts {
					t.Status = StatusUnresolved
					s.Counts.Unresolved++
				} else {
					t.Status = StatusPending
					s.Counts.Pending++
				}
			}

		case DispositionStale, DispositionConflict:
			t.History = append(t.History, HistoryRecord{
				AttemptID:   FormatAttemptID(res.TestIndex, res.Generation),
				Generation:  res.Generation,
				LeaseID:     d.LeaseID,
				RunnerID:    d.RunnerID,
				Outcome:     res.Outcome,
				Timed:       res.Timed,
				DurationMS:  res.DurationMS,
				Disposition: res.Disposition,
				AtMS:        atMS,
			})
			if len(t.History) == MaxTestHistory {
				t.HistoryTruncated = true
			}
		}
	}

	if d.CloseLease {
		lease.Status = LeaseReported
		lease.FinishedAtMS = atMS
		s.removeActiveLease(d.LeaseID)

		if r != nil && r.ActiveLeaseID == d.LeaseID {
			r.ActiveLeaseID = 0
		}
	}

	if wasRunning && s.Verdict() != VerdictRunning {
		s.FinishedAtMS = atMS
	}

	return nil
}

func hasHistoryRecord(history []HistoryRecord, rec HistoryRecord) bool {
	for _, h := range history {
		if h.Generation == rec.Generation &&
			h.LeaseID == rec.LeaseID &&
			h.RunnerID == rec.RunnerID &&
			h.Outcome == rec.Outcome &&
			h.Timed == rec.Timed &&
			h.DurationMS == rec.DurationMS &&
			h.Disposition == rec.Disposition {
			return true
		}
	}

	return false
}

func (s *State) applyExpired(atMS int64, d *ExpiredData) error {
	if !s.Initialized {
		return errors.New("expired event before initialized")
	}

	if d == nil {
		return errors.New("expired event missing payload")
	}

	if d.LeaseID < 1 || d.LeaseID > int64(len(s.Leases)) {
		return fmt.Errorf("expired unknown lease_id %d", d.LeaseID)
	}

	lease := &s.Leases[d.LeaseID-1]
	if lease.Status != LeaseActive {
		return fmt.Errorf("expired lease %d is %s, not active", d.LeaseID, lease.Status)
	}

	wasRunning := s.Verdict() == VerdictRunning
	s.LastSeenMS = max(s.LastSeenMS, atMS)
	lease.Status = LeaseExpired
	lease.FinishedAtMS = atMS
	s.removeActiveLease(d.LeaseID)

	if r := s.Runners[lease.RunnerID]; r != nil && r.ActiveLeaseID == d.LeaseID {
		r.ActiveLeaseID = 0
	}

	for _, a := range lease.Attempts {
		t := &s.Tests[a.TestIndex]
		if t.Status != StatusLeased || t.ActiveLeaseID != d.LeaseID {
			continue
		}

		att := &t.Attempts[a.Generation-1]
		att.Status = AttemptExpired
		att.FinishedAtMS = atMS

		t.ActiveLeaseID = 0
		s.Counts.Leased--
		if t.Generation >= s.Config.MaxAttempts {
			t.Status = StatusUnresolved
			s.Counts.Unresolved++
		} else {
			t.Status = StatusPending
			s.Counts.Pending++
		}
	}

	if wasRunning && s.Verdict() != VerdictRunning {
		s.FinishedAtMS = atMS
	}

	return nil
}

func (s *State) applyFinalized(atMS int64, d *FinalizedData) error {
	if !s.Initialized {
		return errors.New("finalized event before initialized")
	}

	if d == nil || d.Reason != "deadline" {
		return errors.New("finalized event requires reason \"deadline\"")
	}

	if s.Verdict() != VerdictRunning {
		return errors.New("finalized event after build finished")
	}

	s.LastSeenMS = max(s.LastSeenMS, atMS)
	for _, leaseID := range s.ActiveLeases {
		lease := &s.Leases[leaseID-1]
		lease.Status = LeaseExpired
		lease.FinishedAtMS = atMS
		if r := s.Runners[lease.RunnerID]; r != nil && r.ActiveLeaseID == leaseID {
			r.ActiveLeaseID = 0
		}

		for _, a := range lease.Attempts {
			t := &s.Tests[a.TestIndex]
			if t.Status == StatusLeased && t.ActiveLeaseID == leaseID {
				att := &t.Attempts[a.Generation-1]
				att.Status = AttemptExpired
				att.FinishedAtMS = atMS
			}
		}
	}

	s.ActiveLeases = s.ActiveLeases[:0]

	for i := range s.Tests {
		t := &s.Tests[i]
		switch t.Status {
		case StatusPending:
			t.Status = StatusUnresolved
			s.Counts.Pending--
			s.Counts.Unresolved++
		case StatusLeased:
			t.Status = StatusUnresolved
			t.ActiveLeaseID = 0
			s.Counts.Leased--
			s.Counts.Unresolved++
		}
	}

	s.FinishedAtMS = atMS
	return nil
}

// ExtendLease bumps an active lease's in-memory deadline (heartbeats and
// active-lease request_id replays). It never shortens a deadline and never
// affects non-active leases.
func (s *State) ExtendLease(leaseID int64, deadlineMS int64) {
	if leaseID < 1 || leaseID > int64(len(s.Leases)) {
		return
	}

	lease := &s.Leases[leaseID-1]
	if lease.Status != LeaseActive {
		return
	}

	if deadlineMS > lease.DeadlineMS {
		lease.DeadlineMS = deadlineMS
	}
}

// ResetActiveLeaseDeadlines resets every active lease's in-memory deadline to
// max(deadline, now + lease_ttl_ms) after a server restart or Durable Object eviction.
func (s *State) ResetActiveLeaseDeadlines(nowMS int64) {
	if !s.Initialized {
		return
	}

	now := max(nowMS, s.LastSeenMS)
	newDeadline := now + s.Config.LeaseTTLMS
	for _, id := range s.ActiveLeases {
		lease := &s.Leases[id-1]
		if newDeadline > lease.DeadlineMS {
			lease.DeadlineMS = newDeadline
		}
	}
}

func (s *State) ensureRunner(runnerID string, machineIndex *int, atMS int64) *RunnerState {
	if r, ok := s.Runners[runnerID]; ok {
		return r
	}

	r := &RunnerState{
		RunnerID:     runnerID,
		MachineIndex: cloneIntPtr(machineIndex),
		FirstSeenMS:  atMS,
	}
	s.Runners[runnerID] = r
	s.RunnerOrder = append(s.RunnerOrder, runnerID)
	// Keep RunnerOrder sorted by FirstSeenMS asc, RunnerID asc.
	slices.SortFunc(s.RunnerOrder, func(a, b string) int {
		ra, rb := s.Runners[a], s.Runners[b]
		return cmp.Or(cmp.Compare(ra.FirstSeenMS, rb.FirstSeenMS), cmp.Compare(a, b))
	})

	return r
}

func (s *State) removeActiveLease(leaseID int64) {
	if idx, ok := slices.BinarySearch(s.ActiveLeases, leaseID); ok {
		s.ActiveLeases = slices.Delete(s.ActiveLeases, idx, idx+1)
	}
}

func validateReportValues(outcome Outcome, timed bool, durationMS int64) error {
	if !outcome.Valid() {
		return errInvalid("invalid outcome %q", outcome)
	}

	if !timed {
		if durationMS != 0 {
			return errInvalid("untimed result must have duration_ms 0, got %d", durationMS)
		}

		return nil
	}

	if outcome != OutcomePassed {
		return errInvalid("only passed outcomes may be timed, got %q", outcome)
	}

	if durationMS < 0 || durationMS > timing.MaxDurationMS {
		return errInvalid("duration_ms %d is outside 0..%d", durationMS, timing.MaxDurationMS)
	}

	return nil
}

func outcomeToAttemptStatus(o Outcome) AttemptStatus {
	switch o {
	case OutcomePassed:
		return AttemptPassed
	case OutcomeFailed:
		return AttemptFailed
	case OutcomeSkipped:
		return AttemptSkipped
	default:
		return AttemptError
	}
}
