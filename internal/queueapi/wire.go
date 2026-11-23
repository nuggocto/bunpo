// Package queueapi implements the HTTP JSON wire format, server handler, and
// client for Bunpō's live queue.
package queueapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/nuggocto/bunpo/internal/queue"
)

const (
	MaxInitBodyBytes  = 4 << 20
	MaxOtherBodyBytes = 1 << 20
	MaxResponseBytes  = 16 << 20

	MinTokenBytes = 16

	DefaultMaxBuilds   = 200
	DefaultMaxRunning  = 64
	DefaultRetentionMS = 7 * 24 * 60 * 60 * 1000

	ContentTypeJSON = "application/json; charset=utf-8"
)

// Error codes defined at the HTTP transport layer.
const (
	ErrCodeUnauthorized       = "unauthorized"
	ErrCodeForbidden          = "forbidden"
	ErrCodeNotFound           = "not_found"
	ErrCodeMethodNotAllowed   = "method_not_allowed"
	ErrCodeUnsupportedVersion = "unsupported_version"
	ErrCodePayloadTooLarge    = "payload_too_large"
	ErrCodeAtCapacity         = "at_capacity"
	ErrCodeStorageError       = "storage_error"
	ErrCodeInternalError      = "internal_error"
)

// APIError is an error returned by the live queue HTTP API.
type APIError struct {
	HTTPStatus int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("queue API %s (%d): %s", e.Code, e.HTTPStatus, e.Message)
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type errorEnvelope struct {
	Version int64     `json:"version"`
	Error   errorBody `json:"error"`
}

// ValidateTokens verifies that write and read tokens are present, at least
// MinTokenBytes long, and distinct.
func ValidateTokens(writeToken, readToken string) error {
	if len(writeToken) < MinTokenBytes {
		return fmt.Errorf("BUNPO_QUEUE_TOKEN must be set and at least %d bytes", MinTokenBytes)
	}
	if len(readToken) < MinTokenBytes {
		return fmt.Errorf("BUNPO_QUEUE_READ_TOKEN must be set and at least %d bytes", MinTokenBytes)
	}
	wh := sha256.Sum256([]byte(writeToken))
	rh := sha256.Sum256([]byte(readToken))
	if subtle.ConstantTimeCompare(wh[:], rh[:]) == 1 {
		return errors.New("BUNPO_QUEUE_TOKEN and BUNPO_QUEUE_READ_TOKEN must not be equal")
	}
	return nil
}

// --- Request wire types (pointer fields enforce required-member presence) ---

type configWire struct {
	Machines       *int   `json:"machines"`
	LeaseTTLMS     *int64 `json:"lease_ttl_ms"`
	BuildTimeoutMS *int64 `json:"build_timeout_ms"`
	MaxAttempts    *int   `json:"max_attempts"`
	MaxBatch       *int   `json:"max_batch"`
}

type predictionsWire []*int64

func (p *predictionsWire) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() == 'n' {
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		*p = nil
		return nil
	}
	tok, err := dec.ReadToken()
	if err != nil {
		return err
	}
	if tok.Kind() != '[' {
		return fmt.Errorf("predictions_ms must be an array, got %s", tok.Kind())
	}
	out := make([]*int64, 0)
	for dec.PeekKind() != ']' {
		if dec.PeekKind() == 'n' {
			if _, err := dec.ReadToken(); err != nil {
				return err
			}
			out = append(out, nil)
			continue
		}
		var v int64
		if err := json.UnmarshalDecode(dec, &v); err != nil {
			return err
		}
		cp := v
		out = append(out, &cp)
	}
	if _, err := dec.ReadToken(); err != nil {
		return err
	}
	*p = out
	return nil
}

type initRequestWire struct {
	Version       *int64          `json:"version"`
	RunnerID      *string         `json:"runner_id"`
	MachineIndex  optionalInt     `json:"machine_index"`
	Config        *configWire     `json:"config"`
	Tests         []string        `json:"tests"`
	PredictionsMS predictionsWire `json:"predictions_ms"`
}

type leaseRequestWire struct {
	Version      *int64      `json:"version"`
	RunnerID     *string     `json:"runner_id"`
	MachineIndex optionalInt `json:"machine_index"`
	RequestID    *string     `json:"request_id"`
}

type heartbeatRequestWire struct {
	Version  *int64  `json:"version"`
	RunnerID *string `json:"runner_id"`
	LeaseID  *int64  `json:"lease_id"`
}

type reportResultWire struct {
	AttemptID  *string        `json:"attempt_id"`
	TestIndex  *int           `json:"test_index"`
	Generation *int           `json:"generation"`
	Outcome    *queue.Outcome `json:"outcome"`
	Timed      *bool          `json:"timed"`
	DurationMS *int64         `json:"duration_ms"`
}

type reportRequestWire struct {
	Version  *int64             `json:"version"`
	RunnerID *string            `json:"runner_id"`
	LeaseID  *int64             `json:"lease_id"`
	Results  []reportResultWire `json:"results"`
}

// optionalInt distinguishes an absent JSON key from an explicit null or int.
type optionalInt struct {
	Present bool
	Value   *int
}

func (o *optionalInt) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	o.Present = true
	if dec.PeekKind() == 'n' {
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		o.Value = nil
		return nil
	}
	var v int
	if err := json.UnmarshalDecode(dec, &v); err != nil {
		return err
	}
	o.Value = &v
	return nil
}

// --- Response wire types ---

type InitResponse struct {
	Version int64 `json:"version"`
	queue.InitResponse
}

type LeaseResponse struct {
	Version int64 `json:"version"`
	queue.LeaseResponse
}

type HeartbeatResponse struct {
	Version int64 `json:"version"`
	queue.HeartbeatResponse
}

type ReportResponse struct {
	Version int64 `json:"version"`
	queue.ReportResponse
}

type StatusResponse struct {
	Version int64 `json:"version"`
	queue.StatusResponse
}

type ListTestsResponse struct {
	Version int64 `json:"version"`
	queue.ListTestsResponse
}

// BuildSummary is one item returned by GET /api/v1/builds.
type BuildSummary struct {
	BuildID         string        `json:"build_id"`
	Verdict         queue.Verdict `json:"verdict"`
	CreatedAtMS     int64         `json:"created_at_ms"`
	FinishedAtMS    *int64        `json:"finished_at_ms"`
	TestCount       int           `json:"test_count"`
	FailedCount     int           `json:"failed_count"`
	UnresolvedCount int           `json:"unresolved_count"`
	HasConflicts    bool          `json:"has_conflicts"`
}

type ListBuildsResponse struct {
	Version int64          `json:"version"`
	Builds  []BuildSummary `json:"builds"`
}

type HealthResponse struct {
	OK bool `json:"ok"`
}

// --- Client-side request wire structs ---

type initRequestOut struct {
	Version       int64        `json:"version"`
	RunnerID      string       `json:"runner_id"`
	MachineIndex  *int         `json:"machine_index"`
	Config        queue.Config `json:"config"`
	Tests         []string     `json:"tests"`
	PredictionsMS []*int64     `json:"predictions_ms"`
}

type leaseRequestOut struct {
	Version      int64  `json:"version"`
	RunnerID     string `json:"runner_id"`
	MachineIndex *int   `json:"machine_index"`
	RequestID    string `json:"request_id"`
}

type heartbeatRequestOut struct {
	Version  int64  `json:"version"`
	RunnerID string `json:"runner_id"`
	LeaseID  int64  `json:"lease_id"`
}

type reportRequestOut struct {
	Version  int64                `json:"version"`
	RunnerID string               `json:"runner_id"`
	LeaseID  int64                `json:"lease_id"`
	Results  []queue.ReportResult `json:"results"`
}

// readStrictJSON reads a size-bounded body and strictly decodes it into v,
// checking protocol version first on any error.
func readStrictJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, v any, getVersion func() *int64) *APIError {
	body := http.MaxBytesReader(w, r.Body, maxBytes)
	data, err := io.ReadAll(body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return &APIError{
				HTTPStatus: http.StatusRequestEntityTooLarge,
				Code:       ErrCodePayloadTooLarge,
				Message:    fmt.Sprintf("request body exceeds %d byte limit", maxBytes),
			}
		}
		return &APIError{
			HTTPStatus: http.StatusBadRequest,
			Code:       queue.ErrCodeInvalidRequest,
			Message:    "failed to read request body",
		}
	}

	if err := json.Unmarshal(data, v, json.RejectUnknownMembers(true)); err != nil {
		var hdr struct {
			Version *int64 `json:"version"`
		}
		if hdrErr := json.Unmarshal(data, &hdr); hdrErr == nil && hdr.Version != nil && *hdr.Version != queue.ProtocolVersion {
			return &APIError{
				HTTPStatus: http.StatusBadRequest,
				Code:       ErrCodeUnsupportedVersion,
				Message:    fmt.Sprintf("unsupported protocol version %d; server expects version %d", *hdr.Version, queue.ProtocolVersion),
			}
		}
		return &APIError{
			HTTPStatus: http.StatusBadRequest,
			Code:       queue.ErrCodeInvalidRequest,
			Message:    fmt.Sprintf("invalid JSON: %v", err),
		}
	}

	ver := getVersion()
	if ver == nil {
		return &APIError{
			HTTPStatus: http.StatusBadRequest,
			Code:       queue.ErrCodeInvalidRequest,
			Message:    `missing required member "version"`,
		}
	}
	if *ver != queue.ProtocolVersion {
		return &APIError{
			HTTPStatus: http.StatusBadRequest,
			Code:       ErrCodeUnsupportedVersion,
			Message:    fmt.Sprintf("unsupported protocol version %d; server expects version %d", *ver, queue.ProtocolVersion),
		}
	}
	return nil
}

func requireField(name string, present bool) *APIError {
	if !present {
		return &APIError{
			HTTPStatus: http.StatusBadRequest,
			Code:       queue.ErrCodeInvalidRequest,
			Message:    fmt.Sprintf("missing required member %q", name),
		}
	}
	return nil
}
