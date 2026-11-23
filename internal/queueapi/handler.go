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

// EventLog persists decided events before they are applied to in-memory state.
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
	mu           sync.Mutex
	state        *queue.State
	broken       bool
	failedCreate bool
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

	entry, created, apiErr := h.getOrCreateBuildLocked(buildID)
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
	events, resp, err := queue.DecideInit(entry.state, now, cmd)
	if persistErr := h.persistAndApply(r.Context(), buildID, entry, events); persistErr != nil {
		if created && !entry.state.Initialized {
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
	defer entry.mu.Unlock()

	if entry.broken {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	cmd := queue.LeaseCommand{
		RunnerID:     *raw.RunnerID,
		MachineIndex: raw.MachineIndex.Value,
		RequestID:    *raw.RequestID,
	}
	events, resp, err := queue.DecideLease(entry.state, h.clock(), cmd)
	if persistErr := h.persistAndApply(r.Context(), buildID, entry, events); persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	if resp.Status == queue.LeaseGranted {
		entry.state.ExtendLease(resp.LeaseID, resp.DeadlineMS)
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
	defer entry.mu.Unlock()

	if entry.broken {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	cmd := queue.HeartbeatCommand{
		RunnerID: *raw.RunnerID,
		LeaseID:  *raw.LeaseID,
	}
	events, resp, err := queue.DecideHeartbeat(entry.state, h.clock(), cmd)
	if persistErr := h.persistAndApply(r.Context(), buildID, entry, events); persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	if resp.Held {
		entry.state.ExtendLease(cmd.LeaseID, resp.DeadlineMS)
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
	defer entry.mu.Unlock()

	if entry.broken {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	cmd := queue.ReportCommand{
		RunnerID: *raw.RunnerID,
		LeaseID:  *raw.LeaseID,
		Results:  results,
	}
	events, resp, err := queue.DecideReport(entry.state, h.clock(), cmd)
	if persistErr := h.persistAndApply(r.Context(), buildID, entry, events); persistErr != nil {
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
	defer entry.mu.Unlock()

	if entry.broken {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	events, resp, err := queue.DecideStatus(entry.state, h.clock())
	if persistErr := h.persistAndApply(r.Context(), buildID, entry, events); persistErr != nil {
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
	defer entry.mu.Unlock()

	if entry.broken {
		writeAPIError(w, &APIError{HTTPStatus: http.StatusInternalServerError, Code: ErrCodeInternalError, Message: "build state is inconsistent"})
		return
	}

	events, resp, err := queue.DecideListTests(entry.state, h.clock(), cmd)
	if persistErr := h.persistAndApply(r.Context(), buildID, entry, events); persistErr != nil {
		writeAPIError(w, persistErr)
		return
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ListTestsResponse{Version: queue.ProtocolVersion, ListTestsResponse: resp})
}

func (h *Handler) handleListBuilds(w http.ResponseWriter, _ *http.Request) {
	now := h.clock()

	h.mu.Lock()
	entries := make([]*buildEntry, 0, len(h.builds))
	for _, e := range h.builds {
		entries = append(entries, e)
	}
	h.mu.Unlock()

	var expiredIDs []string
	summaries := make([]BuildSummary, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		if !e.state.Initialized || e.broken {
			e.mu.Unlock()
			continue
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
	if err := h.log.Append(ctx, buildID, events); err != nil {
		h.logger.Error("event append failed", "build_id", buildID, "events", len(events), "err", err)
		return &APIError{
			HTTPStatus: http.StatusServiceUnavailable,
			Code:       ErrCodeStorageError,
			Message:    "failed to persist queue events",
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
// one (with retention/capacity enforcement) if absent. Lock ordering is
// strictly h.mu -> entry.mu only for newly created or TryLock-scanned entries,
// so a slow Append on another build never blocks unrelated builds.
func (h *Handler) getOrCreateBuildLocked(buildID string) (*buildEntry, bool, *APIError) {
	now := h.clock()
	for {
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

		// Evict expired finished builds and count running builds using TryLock so
		// a build blocked inside a slow Append never stalls other builds' init.
		type finCandidate struct {
			id         string
			finishedAt int64
			createdAt  int64
		}
		running := 0
		var finished []finCandidate
		for id, e := range h.builds {
			if !e.mu.TryLock() {
				// Conservatively count busy builds as running so we never exceed MaxRunning.
				running++
				continue
			}
			if !e.state.Initialized || e.state.Verdict() == queue.VerdictRunning {
				running++
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

		if running >= h.maxRunning {
			h.mu.Unlock()
			return nil, false, &APIError{
				HTTPStatus: http.StatusServiceUnavailable,
				Code:       ErrCodeAtCapacity,
				Message:    fmt.Sprintf("server at running build capacity (%d)", h.maxRunning),
			}
		}

		if len(h.builds) >= h.maxBuilds {
			if len(finished) == 0 {
				h.mu.Unlock()
				return nil, false, &APIError{
					HTTPStatus: http.StatusServiceUnavailable,
					Code:       ErrCodeAtCapacity,
					Message:    fmt.Sprintf("server at build capacity (%d)", h.maxBuilds),
				}
			}
			slices.SortFunc(finished, func(a, b finCandidate) int {
				return cmp.Or(cmp.Compare(a.finishedAt, b.finishedAt), cmp.Compare(a.createdAt, b.createdAt), cmp.Compare(a.id, b.id))
			})
			evict := len(h.builds) - h.maxBuilds + 1
			for i := 0; i < evict && i < len(finished); i++ {
				delete(h.builds, finished[i].id)
			}
			if len(h.builds) >= h.maxBuilds {
				h.mu.Unlock()
				return nil, false, &APIError{
					HTTPStatus: http.StatusServiceUnavailable,
					Code:       ErrCodeAtCapacity,
					Message:    fmt.Sprintf("server at build capacity (%d)", h.maxBuilds),
				}
			}
		}

		entry := &buildEntry{state: queue.NewState(h.limits)}
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
