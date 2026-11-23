// Package queue implements Bunpō's pure live-queue state machine.
//
// It performs no I/O, reads no clock, uses no randomness, and starts no
// goroutines. Callers decide transitions with Decide*, persist the returned
// events, and then apply them with State.Apply.
package queue

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/nuggocto/bunpo/internal/split"
	"github.com/nuggocto/bunpo/internal/timing"
)

const (
	ProtocolVersion = 1

	MaxTests       = 20_000
	MaxTestIDBytes = 4_096
	MaxMachines    = 256
	MaxRunners     = 1_024

	MinLeaseTTLMS     int64 = 10_000
	MaxLeaseTTLMS     int64 = 600_000
	DefaultLeaseTTLMS int64 = 60_000

	MaxBuildTimeoutMS     int64 = 24 * 60 * 60 * 1000
	DefaultBuildTimeoutMS int64 = 6 * 60 * 60 * 1000

	MaxAttemptsLimit   = 10
	DefaultMaxAttempts = 3

	MaxBatchLimit   = 200
	DefaultMaxBatch = 200

	MaxInitConflicts = 8
	MaxTestHistory   = 8

	MaxStatusRunners  = 256
	MaxStatusSlowest  = 20
	MaxStatusProblems = 50

	DefaultTestsPageLimit = 200
	MaxTestsPageLimit     = 1_000

	MinWaitMS int64 = 1_000
	MaxWaitMS int64 = 15_000
)

// Error codes returned by Decide and mapped to HTTP error bodies.
const (
	ErrCodeInvalidRequest = "invalid_request"
	ErrCodeBuildNotFound  = "build_not_found"
	ErrCodeInitConflict   = "init_conflict"
	ErrCodeLeaseHeld      = "lease_held"
	ErrCodeStaleRequest   = "stale_request"
	ErrCodeTooManyRunners = "too_many_runners"
	ErrCodeUnknownLease   = "unknown_lease"
	ErrCodeRunnerMismatch = "runner_mismatch"
)

// Error is a domain error with its protocol error code and HTTP status.
type Error struct {
	HTTPStatus int
	Code       string
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s (%d): %s", e.Code, e.HTTPStatus, e.Message)
}

func errInvalid(format string, a ...any) *Error {
	return &Error{HTTPStatus: 400, Code: ErrCodeInvalidRequest, Message: fmt.Sprintf(format, a...)}
}

// Limits configures runtime bounds that tests may tighten or relax.
type Limits struct {
	MinLeaseTTLMS int64
	MaxTests      int
	MaxRunners    int
}

// DefaultLimits returns the production protocol limits.
func DefaultLimits() Limits {
	return Limits{
		MinLeaseTTLMS: MinLeaseTTLMS,
		MaxTests:      MaxTests,
		MaxRunners:    MaxRunners,
	}
}

func (l Limits) effective() Limits {
	if l.MinLeaseTTLMS <= 0 {
		l.MinLeaseTTLMS = MinLeaseTTLMS
	}
	if l.MaxTests <= 0 {
		l.MaxTests = MaxTests
	}
	if l.MaxRunners <= 0 {
		l.MaxRunners = MaxRunners
	}
	return l
}

// Verdict is a build's overall status.
type Verdict string

const (
	VerdictRunning    Verdict = "running"
	VerdictPassed     Verdict = "passed"
	VerdictFailed     Verdict = "failed"
	VerdictUnresolved Verdict = "unresolved"
)

// TestStatus is a logical test's lifecycle state.
type TestStatus string

const (
	StatusPending    TestStatus = "pending"
	StatusLeased     TestStatus = "leased"
	StatusPassed     TestStatus = "passed"
	StatusFailed     TestStatus = "failed"
	StatusSkipped    TestStatus = "skipped"
	StatusUnresolved TestStatus = "unresolved"
)

// Outcome is what a runner reports for one attempt.
type Outcome string

const (
	OutcomePassed  Outcome = "passed"
	OutcomeFailed  Outcome = "failed"
	OutcomeSkipped Outcome = "skipped"
	OutcomeError   Outcome = "error"
)

func (o Outcome) Valid() bool {
	return o == OutcomePassed || o == OutcomeFailed || o == OutcomeSkipped || o == OutcomeError
}

// AttemptStatus is the stored outcome of one execution attempt.
type AttemptStatus string

const (
	AttemptLeased  AttemptStatus = "leased"
	AttemptExpired AttemptStatus = "expired"
	AttemptPassed  AttemptStatus = "passed"
	AttemptFailed  AttemptStatus = "failed"
	AttemptSkipped AttemptStatus = "skipped"
	AttemptError   AttemptStatus = "error"
)

func (a AttemptStatus) HasReportedOutcome() bool {
	return a == AttemptPassed || a == AttemptFailed || a == AttemptSkipped || a == AttemptError
}

// Disposition is how the server classified a submitted attempt result.
type Disposition string

const (
	DispositionAccepted  Disposition = "accepted"
	DispositionDuplicate Disposition = "duplicate"
	DispositionStale     Disposition = "stale"
	DispositionConflict  Disposition = "conflict"
)

// LeaseStatus is the lifecycle state of a lease.
type LeaseStatus string

const (
	LeaseActive   LeaseStatus = "active"
	LeaseReported LeaseStatus = "reported"
	LeaseExpired  LeaseStatus = "expired"
)

// LeaseResponseStatus is the status field of a LeaseResponse.
type LeaseResponseStatus string

const (
	LeaseGranted LeaseResponseStatus = "leased"
	LeaseWait    LeaseResponseStatus = "wait"
	LeaseDone    LeaseResponseStatus = "done"
)

// Config holds the build-wide queue settings fixed at init.
type Config struct {
	Machines       int   `json:"machines"`
	LeaseTTLMS     int64 `json:"lease_ttl_ms"`
	BuildTimeoutMS int64 `json:"build_timeout_ms"`
	MaxAttempts    int   `json:"max_attempts"`
	MaxBatch       int   `json:"max_batch"`
}

func (c Config) validate(lim Limits) error {
	lim = lim.effective()
	if c.Machines < 1 || c.Machines > MaxMachines {
		return errInvalid("machines %d is outside 1..%d", c.Machines, MaxMachines)
	}
	if c.LeaseTTLMS < lim.MinLeaseTTLMS || c.LeaseTTLMS > MaxLeaseTTLMS {
		return errInvalid("lease_ttl_ms %d is outside %d..%d", c.LeaseTTLMS, lim.MinLeaseTTLMS, MaxLeaseTTLMS)
	}
	if c.BuildTimeoutMS < c.LeaseTTLMS || c.BuildTimeoutMS > MaxBuildTimeoutMS {
		return errInvalid("build_timeout_ms %d is outside %d..%d", c.BuildTimeoutMS, c.LeaseTTLMS, MaxBuildTimeoutMS)
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > MaxAttemptsLimit {
		return errInvalid("max_attempts %d is outside 1..%d", c.MaxAttempts, MaxAttemptsLimit)
	}
	if c.MaxBatch < 1 || c.MaxBatch > MaxBatchLimit {
		return errInvalid("max_batch %d is outside 1..%d", c.MaxBatch, MaxBatchLimit)
	}
	return nil
}

// Counts holds per-status test totals for a build.
type Counts struct {
	Total      int `json:"total"`
	Pending    int `json:"pending"`
	Leased     int `json:"leased"`
	Passed     int `json:"passed"`
	Failed     int `json:"failed"`
	Skipped    int `json:"skipped"`
	Unresolved int `json:"unresolved"`
}

// AttemptRecord records one generation's execution of a test.
type AttemptRecord struct {
	AttemptID    string        `json:"attempt_id"`
	Generation   int           `json:"generation"`
	LeaseID      int64         `json:"lease_id"`
	RunnerID     string        `json:"runner_id"`
	Status       AttemptStatus `json:"status"`
	Timed        bool          `json:"timed"`
	DurationMS   int64         `json:"duration_ms"`
	LeasedAtMS   int64         `json:"leased_at_ms"`
	FinishedAtMS int64         `json:"finished_at_ms"`
}

// HistoryRecord stores a stale or conflicting submission for visibility.
type HistoryRecord struct {
	AttemptID   string      `json:"attempt_id"`
	Generation  int         `json:"generation"`
	LeaseID     int64       `json:"lease_id"`
	RunnerID    string      `json:"runner_id"`
	Outcome     Outcome     `json:"outcome"`
	Timed       bool        `json:"timed"`
	DurationMS  int64       `json:"duration_ms"`
	Disposition Disposition `json:"disposition"`
	AtMS        int64       `json:"at_ms"`
}

// TestState holds the full state of one logical test inside a build.
type TestState struct {
	Index            int
	Path             string
	PredictedMS      *int64
	EffectiveMS      int64
	Status           TestStatus
	Generation       int
	ActiveLeaseID    int64
	Timed            bool
	DurationMS       int64
	Attempts         []AttemptRecord
	History          []HistoryRecord
	HistoryTruncated bool
}

// LeaseAttempt identifies one test generation inside a lease.
type LeaseAttempt struct {
	TestIndex  int `json:"test_index"`
	Generation int `json:"generation"`
}

// Lease is a first-class batch assignment to one runner.
type Lease struct {
	LeaseID      int64          `json:"lease_id"`
	RunnerID     string         `json:"runner_id"`
	MachineIndex *int           `json:"machine_index"`
	RequestID    string         `json:"request_id"`
	Status       LeaseStatus    `json:"status"`
	LeasedAtMS   int64          `json:"leased_at_ms"`
	DeadlineMS   int64          `json:"deadline_ms"`
	FinishedAtMS int64          `json:"finished_at_ms"`
	Attempts     []LeaseAttempt `json:"attempts"`
}

// RunnerState tracks one runner process within a build.
type RunnerState struct {
	RunnerID      string
	MachineIndex  *int
	FirstSeenMS   int64
	Leases        int
	Accepted      int
	ActiveLeaseID int64
	LastRequestID string
	LastLeaseID   int64
}

// InitConflict records a mismatched init attempt.
type InitConflict struct {
	RunnerID     string `json:"runner_id"`
	MachineIndex *int   `json:"machine_index"`
	GotDigest    string `json:"got_digest"`
	Reason       string `json:"reason"`
	AtMS         int64  `json:"at_ms"`
}

// State is the complete in-memory state of one build.
//
// Decide* functions treat State as strictly read-only. Apply mutates State
// only after events have been persisted (or during replay).
type State struct {
	Limits             Limits
	Initialized        bool
	BuildID            string
	Digest             string
	Config             Config
	CreatedAtMS        int64
	DeadlineMS         int64
	FinishedAtMS       int64
	LastSeenMS         int64
	Tests              []TestState
	DealOrder          []int // immutable permutation of test indexes: EffectiveMS desc, index asc
	Counts             Counts
	Leases             []Lease // 0-indexed; lease_id == index + 1
	ActiveLeases       []int64 // strictly ascending active lease_ids
	Runners            map[string]*RunnerState
	RunnerOrder        []string // deterministic order by FirstSeenMS asc, RunnerID asc
	Conflicts          []InitConflict
	ConflictsTruncated bool
}

// NewState returns an empty, uninitialized build state using the given limits.
func NewState(lim Limits) *State {
	return &State{
		Limits:  lim.effective(),
		Runners: make(map[string]*RunnerState),
	}
}

// Verdict returns the build's current verdict.
func (s *State) Verdict() Verdict {
	if !s.Initialized {
		return VerdictRunning
	}
	return computeVerdict(s.Counts, len(s.Conflicts) > 0)
}

func computeVerdict(c Counts, hasConflict bool) Verdict {
	if c.Pending > 0 || c.Leased > 0 {
		return VerdictRunning
	}
	if c.Failed > 0 {
		return VerdictFailed
	}
	if c.Unresolved > 0 || hasConflict {
		return VerdictUnresolved
	}
	return VerdictPassed
}

// FormatAttemptID returns the deterministic attempt ID "<index>.<generation>".
func FormatAttemptID(testIndex, generation int) string {
	return strconv.Itoa(testIndex) + "." + strconv.Itoa(generation)
}

// ValidBuildID reports whether id satisfies ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$
// and contains no "..".
func ValidBuildID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	if !isAlnum(id[0]) {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if !isAlnum(c) && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return !strings.Contains(id, "..")
}

// ValidRunnerID reports whether id satisfies ^[A-Za-z0-9._-]{1,64}$.
func ValidRunnerID(id string) bool {
	return validToken(id, 64)
}

// ValidRequestID reports whether id satisfies ^[A-Za-z0-9._-]{1,128}$.
func ValidRequestID(id string) bool {
	return validToken(id, 128)
}

func validToken(s string, maxLen int) bool {
	if len(s) == 0 || len(s) > maxLen {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !isAlnum(c) && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func isAlnum(c byte) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// ValidTestID checks that id is canonical POSIX and within MaxTestIDBytes.
func ValidTestID(id string) bool {
	if len(id) == 0 || len(id) > MaxTestIDBytes {
		return false
	}
	c, err := split.Canonical(id, split.POSIX)
	return err == nil && c == id
}

func validateMachineIndex(idx *int) error {
	if idx == nil {
		return nil
	}
	if *idx < 0 || *idx >= MaxMachines {
		return errInvalid("machine_index %d is outside 0..%d", *idx, MaxMachines-1)
	}
	return nil
}

func validateTestList(tests []string, predictions []*int64, maxTests int) error {
	if len(tests) == 0 {
		return errInvalid("tests must not be empty")
	}
	if len(tests) > maxTests {
		return errInvalid("tests has %d entries, over limit %d", len(tests), maxTests)
	}
	if len(predictions) != len(tests) {
		return errInvalid("predictions_ms length %d does not match tests length %d", len(predictions), len(tests))
	}
	for i, id := range tests {
		if !ValidTestID(id) {
			return errInvalid("test %d (%q) is not a canonical test identifier", i, id)
		}
		if i > 0 && tests[i-1] >= id {
			return errInvalid("tests must be unique and sorted in byte order (%q after %q)", id, tests[i-1])
		}
		if p := predictions[i]; p != nil {
			if *p < 0 || *p > timing.MaxDurationMS {
				return errInvalid("predictions_ms[%d] = %d is outside 0..%d", i, *p, timing.MaxDurationMS)
			}
		}
	}
	return nil
}

// ConfigDigest computes the lowercase hex SHA-256 build digest over config and
// sorted canonical tests, excluding predictions.
func ConfigDigest(cfg Config, tests []string) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h,
		"bunpo queue %d\nmachines %d\nlease_ttl_ms %d\nbuild_timeout_ms %d\nmax_attempts %d\nmax_batch %d\n",
		ProtocolVersion, cfg.Machines, cfg.LeaseTTLMS, cfg.BuildTimeoutMS, cfg.MaxAttempts, cfg.MaxBatch,
	)
	for _, id := range tests {
		_, _ = h.Write([]byte(id))
		_, _ = h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func computeEffectivePredictions(predictions []*int64) []int64 {
	known := make([]int64, 0, len(predictions))
	for _, p := range predictions {
		if p != nil {
			known = append(known, *p)
		}
	}
	fill, _ := timing.Median(known)
	eff := make([]int64, len(predictions))
	for i, p := range predictions {
		if p != nil {
			eff[i] = *p
		} else {
			eff[i] = fill
		}
	}
	return eff
}

func cloneIntPtr(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneInt64Ptr(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
