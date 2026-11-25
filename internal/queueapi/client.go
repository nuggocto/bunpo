package queueapi

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nuggocto/bunpo/internal/queue"
)

const defaultHTTPTimeout = 30 * time.Second

// ClientConfig configures a single-shot live-queue HTTP client.
type ClientConfig struct {
	BaseURL           string
	Token             string
	AllowInsecureHTTP bool
	HTTPClient        *http.Client
}

// Client speaks Bunpō's live-queue HTTP JSON protocol. Each method performs one
// HTTP round trip; retry and backoff policy live in internal/queuerunner.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// WireRequest is a serialized HTTP request ready for transport or simulation.
type WireRequest struct {
	Method      string
	Path        string
	ContentType string
	Body        []byte
}

// NewClient validates BaseURL and returns a hardened single-shot HTTP client
// that never follows redirects, bounds response bodies, and never includes
// tokens in errors.
func NewClient(cfg ClientConfig) (*Client, error) {
	u, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid queue URL %q", cfg.BaseURL)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !cfg.AllowInsecureHTTP && !isLoopbackHost(u.Hostname()) {
			return nil, fmt.Errorf("refusing plain http:// URL %q (only loopback is allowed without --allow-insecure-http)", cfg.BaseURL)
		}
	default:
		return nil, fmt.Errorf("unsupported queue URL scheme %q (want https or loopback http)", u.Scheme)
	}
	if u.User != nil {
		return nil, errors.New("queue URL must not embed credentials; pass tokens via environment variables")
	}

	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: defaultHTTPTimeout}
	} else {
		cp := *hc
		hc = &cp
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &Client{
		baseURL:    strings.TrimRight(u.String(), "/"),
		token:      cfg.Token,
		httpClient: hc,
	}, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validVerdict(v queue.Verdict) bool {
	return v == queue.VerdictRunning || v == queue.VerdictPassed || v == queue.VerdictFailed || v == queue.VerdictUnresolved
}

func checkRespVersion(got int64, endpoint string) error {
	if got != queue.ProtocolVersion {
		return fmt.Errorf("%s: invalid response protocol version %d (want %d)", endpoint, got, queue.ProtocolVersion)
	}
	return nil
}

func EncodeInit(cmd queue.InitCommand) (WireRequest, error) {
	body, err := json.Marshal(initRequestOut{
		Version:       queue.ProtocolVersion,
		RunnerID:      cmd.RunnerID,
		MachineIndex:  cmd.MachineIndex,
		Config:        cmd.Config,
		Tests:         cmd.Tests,
		PredictionsMS: cmd.PredictionsMS,
	})
	if err != nil {
		return WireRequest{}, fmt.Errorf("encode init request: %w", err)
	}
	return WireRequest{
		Method:      http.MethodPost,
		Path:        "/api/v1/builds/" + url.PathEscape(cmd.BuildID) + "/init",
		ContentType: ContentTypeJSON,
		Body:        body,
	}, nil
}

func ParseInitResponse(status int, raw []byte) (InitResponse, error) {
	var resp InitResponse
	if err := decodeWireResponse(status, raw, &resp); err != nil {
		return InitResponse{}, err
	}
	if err := checkRespVersion(resp.Version, "init"); err != nil {
		return InitResponse{}, err
	}
	if !validVerdict(resp.Verdict) {
		return InitResponse{}, fmt.Errorf("init: invalid verdict %q", resp.Verdict)
	}
	return resp, nil
}

func EncodeLease(buildID string, cmd queue.LeaseCommand) (WireRequest, error) {
	body, err := json.Marshal(leaseRequestOut{
		Version:      queue.ProtocolVersion,
		RunnerID:     cmd.RunnerID,
		MachineIndex: cmd.MachineIndex,
		RequestID:    cmd.RequestID,
	})
	if err != nil {
		return WireRequest{}, fmt.Errorf("encode lease request: %w", err)
	}
	return WireRequest{
		Method:      http.MethodPost,
		Path:        "/api/v1/builds/" + url.PathEscape(buildID) + "/lease",
		ContentType: ContentTypeJSON,
		Body:        body,
	}, nil
}

func ParseLeaseResponse(status int, raw []byte) (LeaseResponse, error) {
	var resp LeaseResponse
	if err := decodeWireResponse(status, raw, &resp); err != nil {
		return LeaseResponse{}, err
	}
	if err := checkRespVersion(resp.Version, "lease"); err != nil {
		return LeaseResponse{}, err
	}
	switch resp.Status {
	case queue.LeaseGranted:
		if resp.LeaseID <= 0 || len(resp.Attempts) == 0 {
			return LeaseResponse{}, fmt.Errorf("lease: invalid leased response (lease_id=%d attempts=%d)", resp.LeaseID, len(resp.Attempts))
		}
	case queue.LeaseWait:
		if resp.RetryAfterMS <= 0 {
			return LeaseResponse{}, fmt.Errorf("lease: wait response missing positive retry_after_ms (%d)", resp.RetryAfterMS)
		}
	case queue.LeaseDone:
		if !validVerdict(resp.Verdict) || resp.Verdict == queue.VerdictRunning {
			return LeaseResponse{}, fmt.Errorf("lease: done response has invalid verdict %q", resp.Verdict)
		}
	default:
		return LeaseResponse{}, fmt.Errorf("lease: unknown status %q", resp.Status)
	}
	return resp, nil
}

func EncodeHeartbeat(buildID string, cmd queue.HeartbeatCommand) (WireRequest, error) {
	body, err := json.Marshal(heartbeatRequestOut{
		Version:  queue.ProtocolVersion,
		RunnerID: cmd.RunnerID,
		LeaseID:  cmd.LeaseID,
	})
	if err != nil {
		return WireRequest{}, fmt.Errorf("encode heartbeat request: %w", err)
	}
	return WireRequest{
		Method:      http.MethodPost,
		Path:        "/api/v1/builds/" + url.PathEscape(buildID) + "/heartbeat",
		ContentType: ContentTypeJSON,
		Body:        body,
	}, nil
}

func ParseHeartbeatResponse(status int, raw []byte) (HeartbeatResponse, error) {
	var resp HeartbeatResponse
	if err := decodeWireResponse(status, raw, &resp); err != nil {
		return HeartbeatResponse{}, err
	}
	if err := checkRespVersion(resp.Version, "heartbeat"); err != nil {
		return HeartbeatResponse{}, err
	}
	if !validVerdict(resp.Verdict) {
		return HeartbeatResponse{}, fmt.Errorf("heartbeat: invalid verdict %q", resp.Verdict)
	}
	return resp, nil
}

func EncodeReport(buildID string, cmd queue.ReportCommand) (WireRequest, error) {
	body, err := json.Marshal(reportRequestOut{
		Version:  queue.ProtocolVersion,
		RunnerID: cmd.RunnerID,
		LeaseID:  cmd.LeaseID,
		Results:  cmd.Results,
	})
	if err != nil {
		return WireRequest{}, fmt.Errorf("encode report request: %w", err)
	}
	return WireRequest{
		Method:      http.MethodPost,
		Path:        "/api/v1/builds/" + url.PathEscape(buildID) + "/report",
		ContentType: ContentTypeJSON,
		Body:        body,
	}, nil
}

func ParseReportResponse(status int, raw []byte) (ReportResponse, error) {
	var resp ReportResponse
	if err := decodeWireResponse(status, raw, &resp); err != nil {
		return ReportResponse{}, err
	}
	if err := checkRespVersion(resp.Version, "report"); err != nil {
		return ReportResponse{}, err
	}
	if !validVerdict(resp.Verdict) {
		return ReportResponse{}, fmt.Errorf("report: invalid verdict %q", resp.Verdict)
	}
	return resp, nil
}

func EncodeStatus(buildID string) WireRequest {
	return WireRequest{
		Method: http.MethodGet,
		Path:   "/api/v1/builds/" + url.PathEscape(buildID),
	}
}

func ParseStatusResponse(status int, raw []byte) (StatusResponse, error) {
	var resp StatusResponse
	if err := decodeWireResponse(status, raw, &resp); err != nil {
		return StatusResponse{}, err
	}
	if err := checkRespVersion(resp.Version, "status"); err != nil {
		return StatusResponse{}, err
	}
	if !validVerdict(resp.Verdict) {
		return StatusResponse{}, fmt.Errorf("status: invalid verdict %q", resp.Verdict)
	}
	return resp, nil
}

func EncodeListTests(buildID string, cmd queue.ListTestsCommand) WireRequest {
	q := url.Values{}
	if cmd.Cursor > 0 {
		q.Set("cursor", strconv.Itoa(cmd.Cursor))
	}
	if cmd.Limit > 0 {
		q.Set("limit", strconv.Itoa(cmd.Limit))
	}
	if len(cmd.States) > 0 {
		parts := make([]string, len(cmd.States))
		for i, st := range cmd.States {
			parts[i] = string(st)
		}
		q.Set("state", strings.Join(parts, ","))
	}
	if cmd.Retried {
		q.Set("retried", "true")
	}
	path := "/api/v1/builds/" + url.PathEscape(buildID) + "/tests"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	return WireRequest{
		Method: http.MethodGet,
		Path:   path,
	}
}

func ParseListTestsResponse(status int, raw []byte) (ListTestsResponse, error) {
	var resp ListTestsResponse
	if err := decodeWireResponse(status, raw, &resp); err != nil {
		return ListTestsResponse{}, err
	}
	if err := checkRespVersion(resp.Version, "list_tests"); err != nil {
		return ListTestsResponse{}, err
	}
	if !validVerdict(resp.Verdict) {
		return ListTestsResponse{}, fmt.Errorf("list_tests: invalid verdict %q", resp.Verdict)
	}
	return resp, nil
}

func (c *Client) Init(ctx context.Context, cmd queue.InitCommand) (InitResponse, error) {
	req, err := EncodeInit(cmd)
	if err != nil {
		return InitResponse{}, err
	}
	status, raw, err := c.doRaw(ctx, req)
	if err != nil {
		return InitResponse{}, err
	}
	return ParseInitResponse(status, raw)
}

func (c *Client) Lease(ctx context.Context, buildID string, cmd queue.LeaseCommand) (LeaseResponse, error) {
	req, err := EncodeLease(buildID, cmd)
	if err != nil {
		return LeaseResponse{}, err
	}
	status, raw, err := c.doRaw(ctx, req)
	if err != nil {
		return LeaseResponse{}, err
	}
	return ParseLeaseResponse(status, raw)
}

func (c *Client) Heartbeat(ctx context.Context, buildID string, cmd queue.HeartbeatCommand) (HeartbeatResponse, error) {
	req, err := EncodeHeartbeat(buildID, cmd)
	if err != nil {
		return HeartbeatResponse{}, err
	}
	status, raw, err := c.doRaw(ctx, req)
	if err != nil {
		return HeartbeatResponse{}, err
	}
	return ParseHeartbeatResponse(status, raw)
}

func (c *Client) Report(ctx context.Context, buildID string, cmd queue.ReportCommand) (ReportResponse, error) {
	req, err := EncodeReport(buildID, cmd)
	if err != nil {
		return ReportResponse{}, err
	}
	status, raw, err := c.doRaw(ctx, req)
	if err != nil {
		return ReportResponse{}, err
	}
	return ParseReportResponse(status, raw)
}

func (c *Client) Status(ctx context.Context, buildID string) (StatusResponse, error) {
	status, raw, err := c.doRaw(ctx, EncodeStatus(buildID))
	if err != nil {
		return StatusResponse{}, err
	}
	return ParseStatusResponse(status, raw)
}

func (c *Client) ListTests(ctx context.Context, buildID string, cmd queue.ListTestsCommand) (ListTestsResponse, error) {
	status, raw, err := c.doRaw(ctx, EncodeListTests(buildID, cmd))
	if err != nil {
		return ListTestsResponse{}, err
	}
	return ParseListTestsResponse(status, raw)
}

func (c *Client) doRaw(ctx context.Context, wr WireRequest) (int, []byte, error) {
	var body io.Reader
	if wr.Body != nil {
		body = bytes.NewReader(wr.Body)
	}
	req, err := http.NewRequestWithContext(ctx, wr.Method, c.baseURL+wr.Path, body)
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	if wr.ContentType != "" {
		req.Header.Set("Content-Type", wr.ContentType)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Decide cancellation from the caller's ctx, not errors.Is(err, context.DeadlineExceeded),
		// because http.Client.Timeout also wraps DeadlineExceeded and must be retried.
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, &transportError{msg: fmt.Sprintf("queue request %s %s failed: %v", wr.Method, wr.Path, sanitizeTransportError(err))}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return 0, nil, fmt.Errorf("queue server returned redirect %d (redirects are not followed)", resp.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, &transportError{msg: fmt.Sprintf("reading queue response: %v", err)}
	}
	if len(raw) > MaxResponseBytes {
		return 0, nil, fmt.Errorf("queue response exceeds %d byte limit", MaxResponseBytes)
	}
	return resp.StatusCode, raw, nil
}

func decodeWireResponse(status int, raw []byte, out any) error {
	if status != http.StatusOK {
		var env errorEnvelope
		if decErr := json.Unmarshal(raw, &env); decErr == nil && env.Error.Code != "" {
			return &APIError{
				HTTPStatus: status,
				Code:       env.Error.Code,
				Message:    env.Error.Message,
			}
		}
		return &APIError{
			HTTPStatus: status,
			Code:       "http_error",
			Message:    http.StatusText(status),
		}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decoding queue response: %w", err)
	}
	return nil
}

type transportError struct {
	msg string
}

func (e *transportError) Error() string { return e.msg }

// NewTransportError constructs a transient transport error (used by simulation
// tests to match live HTTP transport failures).
func NewTransportError(msg string) error {
	return &transportError{msg: msg}
}

// IsTransientError reports whether err from Client is a transient network,
// rate-limit, or 5xx server failure that may succeed on retry. Permanent 4xx
// responses, protocol/decode errors, and context cancellation return false.
func IsTransientError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var te *transportError
	if errors.As(err, &te) {
		return true
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.HTTPStatus == http.StatusTooManyRequests || apiErr.HTTPStatus >= 500
	}
	return false
}

func sanitizeTransportError(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err.Error()
	}
	return err.Error()
}
