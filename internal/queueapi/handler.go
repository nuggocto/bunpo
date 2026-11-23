package queueapi

import (
	"cmp"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nuggocto/bunpo/internal/queue"
)

// ErrAppendUnwritten may be wrapped by an EventLog.Append implementation to
// prove that nothing was persisted (for example, the write failed before
// touching storage or was truncated/rolled back cleanly). Any other Append
// error leaves persistence uncertain, so the handler marks the build broken
// rather than risking a divergent, unreplayable log.
var ErrAppendUnwritten = errors.New("event append left storage unchanged")

// EventLog persists decided events before they are applied to in-memory state.
// One Append call must be all-or-nothing on replay. On error, the log must
// either prove nothing was written by wrapping ErrAppendUnwritten, or accept
// that the handler will mark the build broken (500).
type EventLog interface {
	Append(ctx context.Context, buildID string, events []queue.Event) error
}

// NopEventLog is an EventLog that succeeds immediately without writing.
type NopEventLog struct{}

func (NopEventLog) Append(context.Context, string, []queue.Event) error { return nil }

// HandlerConfig configures a live-queue HTTP handler.
type HandlerConfig struct {
	WriteToken  string
	ReadToken   string
	Clock       func() int64
	Limits      queue.Limits
	MaxBuilds   int
	MaxRunning  int
	RetentionMS int64
	Log         EventLog
	Logger      *slog.Logger
}

type authRole int

const (
	roleNone authRole = iota
	roleRead
	roleWrite
)

type buildEntry struct {
	mu             sync.Mutex
	state          *queue.State
	lastActivityMS int64
	broken         bool
	failedCreate   bool
}

// Handler serves Bunpō's live-queue HTTP API.
type Handler struct {
	writeDigest [sha256.Size]byte
	readDigest  [sha256.Size]byte
	clock       func() int64
	limits      queue.Limits
	maxBuilds   int
	maxRunning  int
	retentionMS int64
	log         EventLog
	logger      *slog.Logger

	mu     sync.Mutex
	builds map[string]*buildEntry
}

// NewHandler validates configuration and constructs a live-queue HTTP handler.
func NewHandler(cfg HandlerConfig) (*Handler, error) {
	if err := ValidateTokens(cfg.WriteToken, cfg.ReadToken); err != nil {
		return nil, err
	}
	if cfg.Clock == nil {
		cfg.Clock = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.MaxBuilds <= 0 {
		cfg.MaxBuilds = DefaultMaxBuilds
	}
	if cfg.MaxRunning <= 0 {
		cfg.MaxRunning = DefaultMaxRunning
	}
	if cfg.RetentionMS <= 0 {
		cfg.RetentionMS = DefaultRetentionMS
	}
	if cfg.Log == nil {
		cfg.Log = NopEventLog{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Handler{
		writeDigest: sha256.Sum256([]byte(cfg.WriteToken)),
		readDigest:  sha256.Sum256([]byte(cfg.ReadToken)),
		clock:       cfg.Clock,
		limits:      cfg.Limits,
		maxBuilds:   cfg.MaxBuilds,
		maxRunning:  cfg.MaxRunning,
		retentionMS: cfg.RetentionMS,
		log:         cfg.Log,
		logger:      cfg.Logger,
		builds:      make(map[string]*buildEntry),
	}, nil
}

// ServeHTTP dispatches live-queue HTTP requests. Auth runs before reading any
// body or revealing whether a build exists.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSON(w, http.StatusMethodNotAllowed, errorEnvelope{
				Version: queue.ProtocolVersion,
				Error:   errorBody{Code: ErrCodeMethodNotAllowed, Message: "method not allowed"},
			})
			return
		}
		writeJSON(w, http.StatusOK, HealthResponse{OK: true})
		return
	}

	if !strings.HasPrefix(r.URL.Path, "/api/v1/") {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: ErrCodeNotFound, Message: "not found"})
		return
	}

	// Authenticate every /api/v1/... request before checking routes, builds, or reading bodies.
	role := h.authenticate(r)
	if role == roleNone {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusUnauthorized, Code: ErrCodeUnauthorized, Message: "missing or invalid bearer token"})
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/")

	if rest == "builds" {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeAPIError(w, &APIError{HTTPStatus: http.StatusMethodNotAllowed, Code: ErrCodeMethodNotAllowed, Message: "method not allowed"})
			return
		}
		h.handleListBuilds(w, r)
		return
	}

	if !strings.HasPrefix(rest, "builds/") {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: ErrCodeNotFound, Message: "not found"})
		return
	}

	tail := strings.TrimPrefix(rest, "builds/")
	buildID, action, hasSlash := strings.Cut(tail, "/")
	if strings.Contains(action, "/") {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: ErrCodeNotFound, Message: "not found"})
		return
	}

	if !hasSlash {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeAPIError(w, &APIError{HTTPStatus: http.StatusMethodNotAllowed, Code: ErrCodeMethodNotAllowed, Message: "method not allowed"})
			return
		}
		if !queue.ValidBuildID(buildID) {
			writeAPIError(w, &APIError{HTTPStatus: http.StatusBadRequest, Code: queue.ErrCodeInvalidRequest, Message: "invalid build ID"})
			return
		}
		h.handleStatus(w, r, buildID)
		return
	}

	switch action {
	case "tests":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeAPIError(w, &APIError{HTTPStatus: http.StatusMethodNotAllowed, Code: ErrCodeMethodNotAllowed, Message: "method not allowed"})
			return
		}
		if !queue.ValidBuildID(buildID) {
			writeAPIError(w, &APIError{HTTPStatus: http.StatusBadRequest, Code: queue.ErrCodeInvalidRequest, Message: "invalid build ID"})
			return
		}
		h.handleListTests(w, r, buildID)
		return

	case "init", "lease", "heartbeat", "report":
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeAPIError(w, &APIError{HTTPStatus: http.StatusMethodNotAllowed, Code: ErrCodeMethodNotAllowed, Message: "method not allowed"})
			return
		}
		if role != roleWrite {
			writeAPIError(w, &APIError{HTTPStatus: http.StatusForbidden, Code: ErrCodeForbidden, Message: "write token required"})
			return
		}
		if err := checkJSONContentType(r); err != nil {
			writeAPIError(w, err)
			return
		}
		if !queue.ValidBuildID(buildID) {
			writeAPIError(w, &APIError{HTTPStatus: http.StatusBadRequest, Code: queue.ErrCodeInvalidRequest, Message: "invalid build ID"})
			return
		}
		switch action {
		case "init":
			h.handleInit(w, r, buildID)
		case "lease":
			h.handleLease(w, r, buildID)
		case "heartbeat":
			h.handleHeartbeat(w, r, buildID)
		case "report":
			h.handleReport(w, r, buildID)
		}
		return

	default:
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: ErrCodeNotFound, Message: "not found"})
	}
}

func (h *Handler) authenticate(r *http.Request) authRole {
	auth := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok || token == "" {
		return roleNone
	}
	digest := sha256.Sum256([]byte(token))
	isWrite := subtle.ConstantTimeCompare(digest[:], h.writeDigest[:]) == 1
	isRead := subtle.ConstantTimeCompare(digest[:], h.readDigest[:]) == 1
	if isWrite {
		return roleWrite
	}
	if isRead {
		return roleRead
	}
	return roleNone
}

func checkJSONContentType(r *http.Request) *APIError {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil || mediaType != "application/json" {
		return &APIError{
			HTTPStatus: http.StatusBadRequest,
			Code:       queue.ErrCodeInvalidRequest,
			Message:    "Content-Type must be application/json",
		}
	}
	return nil
}

func (h *Handler) handleInit(w http.ResponseWriter, r *http.Request, buildID string) {
	var raw initRequestWire
	if apiErr := readStrictJSON(w, r, MaxInitBodyBytes, &raw, func() *int64 { return raw.Version }); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	if apiErr := firstErr(
		requireField("runner_id", raw.RunnerID != nil),
		requireField("machine_index", raw.MachineIndex.Present),
		requireField("config", raw.Config != nil),
		requireField("tests", raw.Tests != nil),
		requireField("predictions_ms", raw.PredictionsMS != nil),
	); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	if apiErr := firstErr(
		requireField("config.machines", raw.Config.Machines != nil),
		requireField("config.lease_ttl_ms", raw.Config.LeaseTTLMS != nil),
		requireField("config.build_timeout_ms", raw.Config.BuildTimeoutMS != nil),
		requireField("config.max_attempts", raw.Config.MaxAttempts != nil),
		requireField("config.max_batch", raw.Config.MaxBatch != nil),
	); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}

	cmd := queue.InitCommand{
		BuildID:      buildID,
		RunnerID:     *raw.RunnerID,
		MachineIndex: raw.MachineIndex.Value,
		Config: queue.Config{
			Machines:       *raw.Config.Machines,
			LeaseTTLMS:     *raw.Config.LeaseTTLMS,
			BuildTimeoutMS: *raw.Config.BuildTimeoutMS,
			MaxAttempts:    *raw.Config.MaxAttempts,
			MaxBatch:       *raw.Config.MaxBatch,
		},
		Tests:         raw.Tests,
		PredictionsMS: []*int64(raw.PredictionsMS),
	}

	// Validate inputs before reserving a build slot.
	if _, _, err := queue.DecideInit(queue.NewState(h.limits), h.clock(), cmd); err != nil {
		writeQueueError(w, err)
		return
	}

	entry, created, apiErr := h.getOrCreateBuildLocked(r.Context(), buildID)
	if apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	if entry.broken {
		entry.mu.Unlock()
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	now := h.clock()
	entry.lastActivityMS = max(entry.lastActivityMS, now)
	events, resp, err := queue.DecideInit(entry.state, now, cmd)
	if persistErr := h.persistAndApply(r.Context(), buildID, entry, events); persistErr != nil {
		if created && !entry.state.Initialized && !entry.broken {
			entry.failedCreate = true
			entry.mu.Unlock()
			h.removeFailedCreate(buildID, entry)
		} else {
			entry.mu.Unlock()
		}
		writeAPIError(w, persistErr)
		return
	}
	entry.mu.Unlock()

	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, InitResponse{Version: queue.ProtocolVersion, InitResponse: resp})
}

func (h *Handler) handleLease(w http.ResponseWriter, r *http.Request, buildID string) {
	var raw leaseRequestWire
	if apiErr := readStrictJSON(w, r, MaxOtherBodyBytes, &raw, func() *int64 { return raw.Version }); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	if apiErr := firstErr(
		requireField("runner_id", raw.RunnerID != nil),
		requireField("machine_index", raw.MachineIndex.Present),
		requireField("request_id", raw.RequestID != nil),
	); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}

	entry := h.lookupBuildLocked(buildID)
	if entry == nil {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: queue.ErrCodeBuildNotFound, Message: "build not found"})
		return
	}
	if entry.broken {
		entry.mu.Unlock()
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	now := h.clock()
	entry.lastActivityMS = max(entry.lastActivityMS, now)
	cmd := queue.LeaseCommand{
		RunnerID:     *raw.RunnerID,
		MachineIndex: raw.MachineIndex.Value,
		RequestID:    *raw.RequestID,
	}
	events, resp, err := queue.DecideLease(entry.state, now, cmd)
	persistErr := h.persistAndApply(r.Context(), buildID, entry, events)
	if persistErr == nil && err == nil && resp.Status == queue.LeaseGranted {
		entry.state.ExtendLease(resp.LeaseID, resp.DeadlineMS)
	}
	entry.mu.Unlock()

	if persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, LeaseResponse{Version: queue.ProtocolVersion, LeaseResponse: resp})
}

func (h *Handler) handleHeartbeat(w http.ResponseWriter, r *http.Request, buildID string) {
	var raw heartbeatRequestWire
	if apiErr := readStrictJSON(w, r, MaxOtherBodyBytes, &raw, func() *int64 { return raw.Version }); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	if apiErr := firstErr(
		requireField("runner_id", raw.RunnerID != nil),
		requireField("lease_id", raw.LeaseID != nil),
	); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}

	entry := h.lookupBuildLocked(buildID)
	if entry == nil {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: queue.ErrCodeBuildNotFound, Message: "build not found"})
		return
	}
	if entry.broken {
		entry.mu.Unlock()
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	now := h.clock()
	entry.lastActivityMS = max(entry.lastActivityMS, now)
	cmd := queue.HeartbeatCommand{
		RunnerID: *raw.RunnerID,
		LeaseID:  *raw.LeaseID,
	}
	events, resp, err := queue.DecideHeartbeat(entry.state, now, cmd)
	persistErr := h.persistAndApply(r.Context(), buildID, entry, events)
	if persistErr == nil && err == nil && resp.Held {
		entry.state.ExtendLease(cmd.LeaseID, resp.DeadlineMS)
	}
	entry.mu.Unlock()

	if persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, HeartbeatResponse{Version: queue.ProtocolVersion, HeartbeatResponse: resp})
}

func (h *Handler) handleReport(w http.ResponseWriter, r *http.Request, buildID string) {
	var raw reportRequestWire
	if apiErr := readStrictJSON(w, r, MaxOtherBodyBytes, &raw, func() *int64 { return raw.Version }); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	if apiErr := firstErr(
		requireField("runner_id", raw.RunnerID != nil),
		requireField("lease_id", raw.LeaseID != nil),
		requireField("results", raw.Results != nil),
	); apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}

	results := make([]queue.ReportResult, len(raw.Results))
	for i, rr := range raw.Results {
		if apiErr := firstErr(
			requireField("results[].attempt_id", rr.AttemptID != nil),
			requireField("results[].test_index", rr.TestIndex != nil),
			requireField("results[].generation", rr.Generation != nil),
			requireField("results[].outcome", rr.Outcome != nil),
			requireField("results[].timed", rr.Timed != nil),
			requireField("results[].duration_ms", rr.DurationMS != nil),
		); apiErr != nil {
			writeAPIError(w, apiErr)
			return
		}
		results[i] = queue.ReportResult{
			AttemptID:  *rr.AttemptID,
			TestIndex:  *rr.TestIndex,
			Generation: *rr.Generation,
			Outcome:    *rr.Outcome,
			Timed:      *rr.Timed,
			DurationMS: *rr.DurationMS,
		}
	}

	entry := h.lookupBuildLocked(buildID)
	if entry == nil {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: queue.ErrCodeBuildNotFound, Message: "build not found"})
		return
	}
	if entry.broken {
		entry.mu.Unlock()
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	now := h.clock()
	entry.lastActivityMS = max(entry.lastActivityMS, now)
	cmd := queue.ReportCommand{
		RunnerID: *raw.RunnerID,
		LeaseID:  *raw.LeaseID,
		Results:  results,
	}
	events, resp, err := queue.DecideReport(entry.state, now, cmd)
	persistErr := h.persistAndApply(r.Context(), buildID, entry, events)
	entry.mu.Unlock()

	if persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ReportResponse{Version: queue.ProtocolVersion, ReportResponse: resp})
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request, buildID string) {
	entry := h.lookupBuildLocked(buildID)
	if entry == nil {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: queue.ErrCodeBuildNotFound, Message: "build not found"})
		return
	}
	if entry.broken {
		entry.mu.Unlock()
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	events, resp, err := queue.DecideStatus(entry.state, h.clock())
	persistErr := h.persistAndApply(r.Context(), buildID, entry, events)
	entry.mu.Unlock()

	if persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, StatusResponse{Version: queue.ProtocolVersion, StatusResponse: resp})
}

func (h *Handler) handleListTests(w http.ResponseWriter, r *http.Request, buildID string) {
	q := r.URL.Query()
	var cmd queue.ListTestsCommand

	if rawCursor := q.Get("cursor"); rawCursor != "" {
		c, err := strconv.Atoi(rawCursor)
		if err != nil || c < 0 {
			writeAPIError(w, &APIError{HTTPStatus: http.StatusBadRequest, Code: queue.ErrCodeInvalidRequest, Message: "cursor must be a non-negative integer"})
			return
		}
		cmd.Cursor = c
	}
	if rawLimit := q.Get("limit"); rawLimit != "" {
		l, err := strconv.Atoi(rawLimit)
		if err != nil || l < 1 || l > queue.MaxTestsPageLimit {
			writeAPIError(w, &APIError{HTTPStatus: http.StatusBadRequest, Code: queue.ErrCodeInvalidRequest, Message: fmt.Sprintf("limit must be 1..%d", queue.MaxTestsPageLimit)})
			return
		}
		cmd.Limit = l
	}
	if rawState := q.Get("state"); rawState != "" {
		for part := range strings.SplitSeq(rawState, ",") {
			cmd.States = append(cmd.States, queue.TestStatus(part))
		}
	}
	if rawRetried := q.Get("retried"); rawRetried != "" {
		switch rawRetried {
		case "true":
			cmd.Retried = true
		case "false":
			cmd.Retried = false
		default:
			writeAPIError(w, &APIError{HTTPStatus: http.StatusBadRequest, Code: queue.ErrCodeInvalidRequest, Message: "retried must be true or false"})
			return
		}
	}

	entry := h.lookupBuildLocked(buildID)
	if entry == nil {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusNotFound, Code: queue.ErrCodeBuildNotFound, Message: "build not found"})
		return
	}
	if entry.broken {
		entry.mu.Unlock()
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	events, resp, err := queue.DecideListTests(entry.state, h.clock(), cmd)
	persistErr := h.persistAndApply(r.Context(), buildID, entry, events)
	entry.mu.Unlock()

	if persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ListTestsResponse{Version: queue.ProtocolVersion, ListTestsResponse: resp})
}

func (h *Handler) handleListBuilds(w http.ResponseWriter, r *http.Request) {
	now := h.clock()

	type namedEntry struct {
		id    string
		entry *buildEntry
	}
	h.mu.Lock()
	entries := make([]namedEntry, 0, len(h.builds))
	for id, e := range h.builds {
		entries = append(entries, namedEntry{id: id, entry: e})
	}
	h.mu.Unlock()
	// Sort by build ID before touching EventLog so deadline finalization order
	// is independent of Go's randomized map iteration.
	slices.SortFunc(entries, func(a, b namedEntry) int {
		return cmp.Compare(a.id, b.id)
	})

	var expiredIDs []string
	summaries := make([]BuildSummary, 0, len(entries))
	for _, ne := range entries {
		e := ne.entry
		e.mu.Lock()
		if !e.state.Initialized || e.broken {
			e.mu.Unlock()
			continue
		}
		if e.state.Verdict() == queue.VerdictRunning && now >= e.state.DeadlineMS {
			events, _, err := queue.DecideStatus(e.state, now)
			if err == nil && len(events) > 0 {
				_ = h.persistAndApply(r.Context(), e.state.BuildID, e, events)
			}
			if e.broken {
				e.mu.Unlock()
				continue
			}
		}
		v := e.state.Verdict()
		if v != queue.VerdictRunning && e.state.FinishedAtMS > 0 && now-e.state.FinishedAtMS >= h.retentionMS {
			expiredIDs = append(expiredIDs, e.state.BuildID)
			e.mu.Unlock()
			continue
		}
		var fin *int64
		if e.state.FinishedAtMS > 0 {
			t := e.state.FinishedAtMS
			fin = &t
		}
		summaries = append(summaries, BuildSummary{
			BuildID:         e.state.BuildID,
			Verdict:         v,
			CreatedAtMS:     e.state.CreatedAtMS,
			FinishedAtMS:    fin,
			TestCount:       e.state.Counts.Total,
			FailedCount:     e.state.Counts.Failed,
			UnresolvedCount: e.state.Counts.Unresolved,
			HasConflicts:    len(e.state.Conflicts) > 0,
		})
		e.mu.Unlock()
	}

	if len(expiredIDs) > 0 {
		h.mu.Lock()
		for _, id := range expiredIDs {
			delete(h.builds, id)
		}
		h.mu.Unlock()
	}

	slices.SortFunc(summaries, func(a, b BuildSummary) int {
		return cmp.Or(cmp.Compare(b.CreatedAtMS, a.CreatedAtMS), cmp.Compare(a.BuildID, b.BuildID))
	})
	if len(summaries) > h.maxBuilds {
		summaries = summaries[:h.maxBuilds]
	}

	writeJSON(w, http.StatusOK, ListBuildsResponse{
		Version: queue.ProtocolVersion,
		Builds:  summaries,
	})
}

func (h *Handler) persistAndApply(ctx context.Context, buildID string, entry *buildEntry, events []queue.Event) *APIError {
	if len(events) == 0 {
		return nil
	}
	// Detach from request cancellation so a client disconnect mid-write cannot
	// leave an in-progress journal append half-written.
	persistCtx := context.WithoutCancel(ctx)
	if err := h.log.Append(persistCtx, buildID, events); err != nil {
		if errors.Is(err, ErrAppendUnwritten) {
			h.logger.Error("event append failed cleanly", "build_id", buildID, "events", len(events), "err", err)
			return &APIError{
				HTTPStatus: http.StatusServiceUnavailable,
				Code:       ErrCodeStorageError,
				Message:    "failed to persist queue events",
			}
		}
		entry.broken = true
		h.logger.Error("event append failed with uncertain write state; marking build broken", "build_id", buildID, "events", len(events), "err", err)
		return &APIError{
			HTTPStatus: http.StatusInternalServerError,
			Code:       ErrCodeInternalError,
			Message:    "build state is inconsistent after storage error",
		}
	}
	for _, ev := range events {
		if err := entry.state.Apply(ev); err != nil {
			entry.broken = true
			h.logger.Error("apply failed after append; marking build broken", "build_id", buildID, "event_type", ev.Type, "err", err)
			return &APIError{
				HTTPStatus: http.StatusInternalServerError,
				Code:       ErrCodeInternalError,
				Message:    "internal state error",
			}
		}
	}
	return nil
}

// lookupBuildLocked looks up buildID, locks it, and checks retention.
// Returns nil if not found or expired past retention.
func (h *Handler) lookupBuildLocked(buildID string) *buildEntry {
	now := h.clock()
	for {
		h.mu.Lock()
		entry, ok := h.builds[buildID]
		h.mu.Unlock()
		if !ok {
			return nil
		}

		entry.mu.Lock()
		if entry.failedCreate {
			entry.mu.Unlock()
			continue
		}
		if !entry.state.Initialized {
			entry.mu.Unlock()
			return nil
		}
		if !entry.broken && entry.state.Verdict() != queue.VerdictRunning && entry.state.FinishedAtMS > 0 && now-entry.state.FinishedAtMS >= h.retentionMS {
			entry.mu.Unlock()
			h.mu.Lock()
			if h.builds[buildID] == entry {
				delete(h.builds, buildID)
			}
			h.mu.Unlock()
			return nil
		}
		return entry
	}
}

// getOrCreateBuildLocked returns the locked buildEntry for buildID, creating
// one (with deadline finalization, retention, and capacity eviction under
// pressure) if absent. Lock ordering is strictly h.mu -> entry.mu only for
// newly created or TryLock-scanned entries, so a slow Append on another build
// never blocks unrelated builds.
func (h *Handler) getOrCreateBuildLocked(ctx context.Context, buildID string) (*buildEntry, bool, *APIError) {
	finalizedDeadline := false
	for {
		now := h.clock()
		h.mu.Lock()
		if existing, ok := h.builds[buildID]; ok {
			h.mu.Unlock()
			existing.mu.Lock()
			if existing.failedCreate {
				existing.mu.Unlock()
				continue
			}
			if !existing.broken && existing.state.Initialized && existing.state.Verdict() != queue.VerdictRunning && existing.state.FinishedAtMS > 0 && now-existing.state.FinishedAtMS >= h.retentionMS {
				existing.mu.Unlock()
				h.mu.Lock()
				if h.builds[buildID] == existing {
					delete(h.builds, buildID)
				}
				h.mu.Unlock()
				continue
			}
			return existing, false, nil
		}

		type finCandidate struct {
			id         string
			finishedAt int64
			createdAt  int64
		}
		type idleCandidate struct {
			id             string
			lastActivityMS int64
		}
		type deadlineCandidate struct {
			id    string
			entry *buildEntry
		}

		running := 0
		var brokenIDs []string
		var finished []finCandidate
		var idleRunning []idleCandidate
		var pastDeadline []deadlineCandidate

		for id, e := range h.builds {
			if !e.mu.TryLock() {
				// Conservatively count busy builds as running so we never exceed MaxRunning.
				running++
				continue
			}
			if e.broken {
				brokenIDs = append(brokenIDs, id)
				e.mu.Unlock()
				continue
			}
			if !e.state.Initialized {
				running++
				e.mu.Unlock()
				continue
			}
			if e.state.Verdict() == queue.VerdictRunning {
				if now >= e.state.DeadlineMS {
					pastDeadline = append(pastDeadline, deadlineCandidate{id: id, entry: e})
				} else {
					running++
					if now-e.lastActivityMS > 2*e.state.Config.LeaseTTLMS {
						idleRunning = append(idleRunning, idleCandidate{id: id, lastActivityMS: e.lastActivityMS})
					}
				}
				e.mu.Unlock()
				continue
			}
			fin := e.state.FinishedAtMS
			created := e.state.CreatedAtMS
			e.mu.Unlock()
			if fin > 0 && now-fin >= h.retentionMS {
				delete(h.builds, id)
				continue
			}
			finished = append(finished, finCandidate{id: id, finishedAt: fin, createdAt: created})
		}

		// Finalize deadline-expired builds through the EventLog outside h.mu in
		// deterministic build-ID order.
		if len(pastDeadline) > 0 && !finalizedDeadline {
			finalizedDeadline = true
			h.mu.Unlock()
			slices.SortFunc(pastDeadline, func(a, b deadlineCandidate) int {
				return cmp.Compare(a.id, b.id)
			})
			for _, dc := range pastDeadline {
				dc.entry.mu.Lock()
				if !dc.entry.broken && dc.entry.state.Initialized && dc.entry.state.Verdict() == queue.VerdictRunning && now >= dc.entry.state.DeadlineMS {
					events, _, err := queue.DecideStatus(dc.entry.state, now)
					if err == nil && len(events) > 0 {
						_ = h.persistAndApply(ctx, dc.id, dc.entry, events)
					}
				}
				dc.entry.mu.Unlock()
			}
			continue
		}
		// Any still-unfinalized past-deadline builds (for example if Append failed cleanly)
		// count conservatively toward running so we don't spin.
		running += len(pastDeadline)

		// Under running-capacity pressure, evict idle abandoned running builds
		// (no activity for > 2 * lease_ttl_ms), longest idle first.
		slices.SortFunc(idleRunning, func(a, b idleCandidate) int {
			return cmp.Or(cmp.Compare(a.lastActivityMS, b.lastActivityMS), cmp.Compare(a.id, b.id))
		})
		evictedIdle := 0
		for running >= h.maxRunning && evictedIdle < len(idleRunning) {
			delete(h.builds, idleRunning[evictedIdle].id)
			running--
			evictedIdle++
		}
		if running >= h.maxRunning {
			h.mu.Unlock()
			return nil, false, &APIError{
				HTTPStatus: http.StatusServiceUnavailable,
				Code:       ErrCodeAtCapacity,
				Message:    fmt.Sprintf("server at running build capacity (%d)", h.maxRunning),
			}
		}

		// Under total-build pressure, evict broken builds, then oldest finished
		// builds, then remaining idle abandoned running builds.
		if len(h.builds) >= h.maxBuilds {
			slices.Sort(brokenIDs)
			for i := 0; len(h.builds) >= h.maxBuilds && i < len(brokenIDs); i++ {
				delete(h.builds, brokenIDs[i])
			}
		}
		if len(h.builds) >= h.maxBuilds {
			slices.SortFunc(finished, func(a, b finCandidate) int {
				return cmp.Or(cmp.Compare(a.finishedAt, b.finishedAt), cmp.Compare(a.createdAt, b.createdAt), cmp.Compare(a.id, b.id))
			})
			for i := 0; len(h.builds) >= h.maxBuilds && i < len(finished); i++ {
				delete(h.builds, finished[i].id)
			}
		}
		if len(h.builds) >= h.maxBuilds {
			for ; len(h.builds) >= h.maxBuilds && evictedIdle < len(idleRunning); evictedIdle++ {
				delete(h.builds, idleRunning[evictedIdle].id)
			}
		}
		if len(h.builds) >= h.maxBuilds {
			h.mu.Unlock()
			return nil, false, &APIError{
				HTTPStatus: http.StatusServiceUnavailable,
				Code:       ErrCodeAtCapacity,
				Message:    fmt.Sprintf("server at build capacity (%d)", h.maxBuilds),
			}
		}

		entry := &buildEntry{state: queue.NewState(h.limits), lastActivityMS: now}
		entry.mu.Lock()
		h.builds[buildID] = entry
		h.mu.Unlock()
		return entry, true, nil
	}
}

func (h *Handler) removeFailedCreate(buildID string, entry *buildEntry) {
	h.mu.Lock()
	if h.builds[buildID] == entry {
		delete(h.builds, buildID)
	}
	h.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		w.Header().Set("Content-Type", ContentTypeJSON)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"version":1,"error":{"code":"internal_error","message":"failed to encode response"}}`))
		return
	}
	w.Header().Set("Content-Type", ContentTypeJSON)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeAPIError(w http.ResponseWriter, err *APIError) {
	writeJSON(w, err.HTTPStatus, errorEnvelope{
		Version: queue.ProtocolVersion,
		Error:   errorBody{Code: err.Code, Message: err.Message},
	})
}

func writeQueueError(w http.ResponseWriter, err error) {
	var qe *queue.Error
	if errors.As(err, &qe) {
		writeAPIError(w, &APIError{HTTPStatus: qe.HTTPStatus, Code: qe.Code, Message: qe.Message})
		return
	}
	writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "internal error"})
}

func firstErr(errs ...*APIError) *APIError {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
