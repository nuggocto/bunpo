package queueapi

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nuggocto/bunpo/internal/queue"
)

const (
	DefaultRetryBudget  = 2 * time.Minute
	DefaultRetryBase    = 200 * time.Millisecond
	DefaultRetryMaxWait = 5 * time.Second
	DefaultHTTPTimeout  = 30 * time.Second
)

// ClientConfig configures a live-queue HTTP client.
type ClientConfig struct {
	BaseURL           string
	Token             string
	AllowInsecureHTTP bool
	HTTPClient        *http.Client
	RetryBudget       time.Duration
	RetryBase         time.Duration
	RetryMaxWait      time.Duration
}

// Client speaks Bunpō's live-queue HTTP JSON protocol.
type Client struct {
	baseURL      string
	token        string
	httpClient   *http.Client
	retryBudget  time.Duration
	retryBase    time.Duration
	retryMaxWait time.Duration
}

// NewClient validates BaseURL and returns a hardened HTTP client that never
// follows redirects, bounds response bodies, and never includes tokens in errors.
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
		hc = &http.Client{Timeout: DefaultHTTPTimeout}
	} else {
		cp := *hc
		hc = &cp
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	budget := cfg.RetryBudget
	if budget <= 0 {
		budget = DefaultRetryBudget
	}
	base := cfg.RetryBase
	if base <= 0 {
		base = DefaultRetryBase
	}
	maxWait := cfg.RetryMaxWait
	if maxWait <= 0 {
		maxWait = DefaultRetryMaxWait
	}

	return &Client{
		baseURL:      strings.TrimRight(u.String(), "/"),
		token:        cfg.Token,
		httpClient:   hc,
		retryBudget:  budget,
		retryBase:    base,
		retryMaxWait: maxWait,
	}, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Client) Init(ctx context.Context, cmd queue.InitCommand) (InitResponse, error) {
	body := initRequestOut{
		Version:       queue.ProtocolVersion,
		RunnerID:      cmd.RunnerID,
		MachineIndex:  cmd.MachineIndex,
		Config:        cmd.Config,
		Tests:         cmd.Tests,
		PredictionsMS: cmd.PredictionsMS,
	}
	var resp InitResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/builds/"+url.PathEscape(cmd.BuildID)+"/init", body, &resp, true)
	return resp, err
}

func (c *Client) Lease(ctx context.Context, buildID string, cmd queue.LeaseCommand) (LeaseResponse, error) {
	body := leaseRequestOut{
		Version:      queue.ProtocolVersion,
		RunnerID:     cmd.RunnerID,
		MachineIndex: cmd.MachineIndex,
		RequestID:    cmd.RequestID,
	}
	var resp LeaseResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/builds/"+url.PathEscape(buildID)+"/lease", body, &resp, true)
	return resp, err
}

// Heartbeat sends a single heartbeat without the multi-minute retry loop; the
// caller's heartbeat goroutine controls retrying within the lease TTL.
func (c *Client) Heartbeat(ctx context.Context, buildID string, cmd queue.HeartbeatCommand) (HeartbeatResponse, error) {
	body := heartbeatRequestOut{
		Version:  queue.ProtocolVersion,
		RunnerID: cmd.RunnerID,
		LeaseID:  cmd.LeaseID,
	}
	var resp HeartbeatResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/builds/"+url.PathEscape(buildID)+"/heartbeat", body, &resp, false)
	return resp, err
}

func (c *Client) Report(ctx context.Context, buildID string, cmd queue.ReportCommand) (ReportResponse, error) {
	body := reportRequestOut{
		Version:  queue.ProtocolVersion,
		RunnerID: cmd.RunnerID,
		LeaseID:  cmd.LeaseID,
		Results:  cmd.Results,
	}
	var resp ReportResponse
	err := c.doJSON(ctx, http.MethodPost, "/api/v1/builds/"+url.PathEscape(buildID)+"/report", body, &resp, true)
	return resp, err
}

func (c *Client) Status(ctx context.Context, buildID string) (StatusResponse, error) {
	var resp StatusResponse
	err := c.doJSON(ctx, http.MethodGet, "/api/v1/builds/"+url.PathEscape(buildID), nil, &resp, true)
	return resp, err
}

func (c *Client) ListTests(ctx context.Context, buildID string, cmd queue.ListTestsCommand) (ListTestsResponse, error) {
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
	var resp ListTestsResponse
	err := c.doJSON(ctx, http.MethodGet, path, nil, &resp, true)
	return resp, err
}

func (c *Client) ListBuilds(ctx context.Context) (ListBuildsResponse, error) {
	var resp ListBuildsResponse
	err := c.doJSON(ctx, http.MethodGet, "/api/v1/builds", nil, &resp, true)
	return resp, err
}

func (c *Client) doJSON(ctx context.Context, method, path string, reqBody any, respOut any, retry bool) error {
	var payload []byte
	if reqBody != nil {
		var err error
		payload, err = json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}

	deadline := time.Now().Add(c.retryBudget)
	backoff := c.retryBase

	for attempt := 0; ; attempt++ {
		err, retryable := c.doOnce(ctx, method, path, payload, respOut)
		if err == nil || !retry || !retryable {
			return err
		}

		now := time.Now()
		if !now.Before(deadline) {
			return err
		}

		wait := backoff
		if wait > c.retryMaxWait {
			wait = c.retryMaxWait
		}
		// Add up to 25% jitter.
		if jitterMax := wait / 4; jitterMax > 0 {
			wait += time.Duration(rand.Int64N(int64(jitterMax) + 1))
		}
		if rem := deadline.Sub(now); wait > rem {
			wait = rem
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < c.retryMaxWait {
			backoff = min(backoff*2, c.retryMaxWait)
		}
	}
}

func (c *Client) doOnce(ctx context.Context, method, path string, payload []byte, respOut any) (error, bool) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err), false
	}
	if payload != nil {
		req.Header.Set("Content-Type", ContentTypeJSON)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err, false
		}
		return fmt.Errorf("queue request %s %s failed: %v", method, path, sanitizeTransportError(err)), true
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return fmt.Errorf("queue server returned redirect %d (redirects are not followed)", resp.StatusCode), false
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("reading queue response: %v", err), true
	}
	if len(raw) > MaxResponseBytes {
		return fmt.Errorf("queue response exceeds %d byte limit", MaxResponseBytes), false
	}

	if resp.StatusCode != http.StatusOK {
		var env errorEnvelope
		if decErr := json.Unmarshal(raw, &env); decErr == nil && env.Error.Code != "" {
			apiErr := &APIError{
				HTTPStatus: resp.StatusCode,
				Code:       env.Error.Code,
				Message:    env.Error.Message,
			}
			return apiErr, resp.StatusCode >= 500
		}
		apiErr := &APIError{
			HTTPStatus: resp.StatusCode,
			Code:       "http_error",
			Message:    http.StatusText(resp.StatusCode),
		}
		return apiErr, resp.StatusCode >= 500
	}

	if err := json.Unmarshal(raw, respOut); err != nil {
		return fmt.Errorf("decoding queue response: %w", err), false
	}
	return nil, false
}

func sanitizeTransportError(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err.Error()
	}
	return err.Error()
}
