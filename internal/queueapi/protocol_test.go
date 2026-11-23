package queueapi_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

type protocolEnv struct {
	baseURL    string
	writeToken string
	readToken  string
	ttlMS      int64
	httpClient *http.Client
}

func getListBuilds(t *testing.T, baseURL, token string, hc *http.Client) queueapi.ListBuildsResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/builds", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/builds: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /api/v1/builds: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/builds status = %d: %s", resp.StatusCode, raw)
	}
	var out queueapi.ListBuildsResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode /api/v1/builds: %v", err)
	}
	if out.Version != queue.ProtocolVersion {
		t.Fatalf("GET /api/v1/builds version = %d; want %d", out.Version, queue.ProtocolVersion)
	}
	return out
}

// newProtocolEnv connects to an external server when BUNPO_TEST_SERVER_URL is
// set (so the Cloudflare Worker can run this exact black-box suite), or starts
// an in-process httptest.Server otherwise.
func newProtocolEnv(t *testing.T) protocolEnv {
	t.Helper()

	if base := os.Getenv("BUNPO_TEST_SERVER_URL"); base != "" {
		writeTok := os.Getenv("BUNPO_TEST_WRITE_TOKEN")
		readTok := os.Getenv("BUNPO_TEST_READ_TOKEN")
		if writeTok == "" || readTok == "" {
			t.Fatal("BUNPO_TEST_WRITE_TOKEN and BUNPO_TEST_READ_TOKEN must be set with BUNPO_TEST_SERVER_URL")
		}
		ttl := int64(10_000)
		if raw := os.Getenv("BUNPO_TEST_TTL_MS"); raw != "" {
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || v <= 0 {
				t.Fatalf("invalid BUNPO_TEST_TTL_MS %q", raw)
			}
			ttl = v
		}
		return protocolEnv{
			baseURL:    strings.TrimRight(base, "/"),
			writeToken: writeTok,
			readToken:  readTok,
			ttlMS:      ttl,
			httpClient: &http.Client{Timeout: 30 * time.Second},
		}
	}

	writeTok := "write-secret-token-0123456789abcdef"
	readTok := "read-secret-token-fedcba9876543210"
	lim := queue.DefaultLimits()
	lim.MinLeaseTTLMS = 100
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: writeTok,
		ReadToken:  readTok,
		Limits:     lim,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	return protocolEnv{
		baseURL:    srv.URL,
		writeToken: writeTok,
		readToken:  readTok,
		ttlMS:      200,
		httpClient: srv.Client(),
	}
}

var buildSeq atomic.Int64

func uniqueBuildID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), buildSeq.Add(1))
}

func (e protocolEnv) rawRequest(t *testing.T, method, path, token string, body []byte) (*http.Response, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, e.baseURL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", queueapi.ContentTypeJSON)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

func (e protocolEnv) client(t *testing.T, token string) *queueapi.Client {
	t.Helper()
	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:           e.baseURL,
		Token:             token,
		AllowInsecureHTTP: true,
		HTTPClient:        e.httpClient,
		RetryBudget:       5 * time.Second,
		RetryBase:         20 * time.Millisecond,
		RetryMaxWait:      100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func assertErrorBody(t *testing.T, resp *http.Response, data []byte, wantStatus int, wantCode string) {
	t.Helper()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d; want %d (body: %s)", resp.StatusCode, wantStatus, data)
	}
	if ct := resp.Header.Get("Content-Type"); ct != queueapi.ContentTypeJSON {
		t.Fatalf("Content-Type = %q; want %q", ct, queueapi.ContentTypeJSON)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q; want no-store", cc)
	}
	if nosniff := resp.Header.Get("X-Content-Type-Options"); nosniff != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q; want nosniff", nosniff)
	}
	var env struct {
		Version int64 `json:"version"`
		Error   struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("decode error envelope %q: %v", data, err)
	}
	if env.Version != queue.ProtocolVersion || env.Error.Code != wantCode || env.Error.Message == "" {
		t.Fatalf("error envelope = %+v; want version=1 code=%q non-empty message", env, wantCode)
	}
}

func TestProtocolBlackBox(t *testing.T) {
	env := newProtocolEnv(t)
	ctx := context.Background()
	wc := env.client(t, env.writeToken)
	rc := env.client(t, env.readToken)

	t.Run("healthz and unknown routes and methods", func(t *testing.T) {
		resp, data := env.rawRequest(t, http.MethodGet, "/healthz", "", nil)
		if resp.StatusCode != http.StatusOK || string(data) != `{"ok":true}` {
			t.Fatalf("healthz = %d %s; want 200 {\"ok\":true}", resp.StatusCode, data)
		}
		resp, data = env.rawRequest(t, http.MethodPost, "/healthz", "", []byte(`{}`))
		assertErrorBody(t, resp, data, http.StatusMethodNotAllowed, queueapi.ErrCodeMethodNotAllowed)
		if resp.Header.Get("Allow") != http.MethodGet {
			t.Fatalf("Allow = %q; want GET", resp.Header.Get("Allow"))
		}

		resp, data = env.rawRequest(t, http.MethodGet, "/does-not-exist", "", nil)
		assertErrorBody(t, resp, data, http.StatusNotFound, queueapi.ErrCodeNotFound)

		resp, data = env.rawRequest(t, http.MethodPost, "/api/v1/builds", env.writeToken, []byte(`{}`))
		assertErrorBody(t, resp, data, http.StatusMethodNotAllowed, queueapi.ErrCodeMethodNotAllowed)
		if resp.Header.Get("Allow") != http.MethodGet {
			t.Fatalf("Allow = %q; want GET", resp.Header.Get("Allow"))
		}

		resp, data = env.rawRequest(t, http.MethodGet, "/api/v1/builds/foo/init", env.writeToken, nil)
		assertErrorBody(t, resp, data, http.StatusMethodNotAllowed, queueapi.ErrCodeMethodNotAllowed)
		if resp.Header.Get("Allow") != http.MethodPost {
			t.Fatalf("Allow = %q; want POST", resp.Header.Get("Allow"))
		}
	})

	t.Run("auth on every route before build lookup or body read", func(t *testing.T) {
		buildID := uniqueBuildID("auth")
		routes := []struct {
			method string
			path   string
			isPost bool
		}{
			{http.MethodPost, "/api/v1/builds/" + buildID + "/init", true},
			{http.MethodPost, "/api/v1/builds/" + buildID + "/lease", true},
			{http.MethodPost, "/api/v1/builds/" + buildID + "/heartbeat", true},
			{http.MethodPost, "/api/v1/builds/" + buildID + "/report", true},
			{http.MethodGet, "/api/v1/builds/" + buildID, false},
			{http.MethodGet, "/api/v1/builds/" + buildID + "/tests", false},
			{http.MethodGet, "/api/v1/builds", false},
		}

		for _, rt := range routes {
			// No token -> 401 (never 404 or 400).
			resp, data := env.rawRequest(t, rt.method, rt.path, "", []byte("not-json"))
			assertErrorBody(t, resp, data, http.StatusUnauthorized, queueapi.ErrCodeUnauthorized)

			// Wrong token -> 401.
			resp, data = env.rawRequest(t, rt.method, rt.path, "wrong-token-0123456789abcdef", []byte("not-json"))
			assertErrorBody(t, resp, data, http.StatusUnauthorized, queueapi.ErrCodeUnauthorized)

			// Read token on POST -> 403 before reading body or revealing 404.
			if rt.isPost {
				resp, data = env.rawRequest(t, rt.method, rt.path, env.readToken, []byte("not-json"))
				assertErrorBody(t, resp, data, http.StatusForbidden, queueapi.ErrCodeForbidden)
			}
		}

		// Unknown build with valid read token -> 404 build_not_found.
		resp, data := env.rawRequest(t, http.MethodGet, "/api/v1/builds/"+buildID, env.readToken, nil)
		assertErrorBody(t, resp, data, http.StatusNotFound, queue.ErrCodeBuildNotFound)
	})

	t.Run("strict JSON and limits at and past bounds", func(t *testing.T) {
		// Build ID 128 chars OK vs 129 chars 400 vs ".." 400.
		maxID := strings.Repeat("a", 128)
		resp, data := env.rawRequest(t, http.MethodGet, "/api/v1/builds/"+maxID, env.readToken, nil)
		assertErrorBody(t, resp, data, http.StatusNotFound, queue.ErrCodeBuildNotFound)

		tooLongID := strings.Repeat("a", 129)
		resp, data = env.rawRequest(t, http.MethodGet, "/api/v1/builds/"+tooLongID, env.readToken, nil)
		assertErrorBody(t, resp, data, http.StatusBadRequest, queue.ErrCodeInvalidRequest)

		resp, data = env.rawRequest(t, http.MethodGet, "/api/v1/builds/a..b", env.readToken, nil)
		assertErrorBody(t, resp, data, http.StatusBadRequest, queue.ErrCodeInvalidRequest)

		bid := uniqueBuildID("strict")
		// Unknown member -> 400 invalid_request.
		resp, data = env.rawRequest(t, http.MethodPost, "/api/v1/builds/"+bid+"/lease", env.writeToken,
			[]byte(`{"version":1,"runner_id":"r1","machine_index":0,"request_id":"q1","extra":true}`))
		assertErrorBody(t, resp, data, http.StatusBadRequest, queue.ErrCodeInvalidRequest)

		// Missing required member -> 400 invalid_request.
		resp, data = env.rawRequest(t, http.MethodPost, "/api/v1/builds/"+bid+"/lease", env.writeToken,
			[]byte(`{"version":1,"runner_id":"r1","machine_index":0}`))
		assertErrorBody(t, resp, data, http.StatusBadRequest, queue.ErrCodeInvalidRequest)

		// Unsupported version (even with unknown member!) -> 400 unsupported_version naming both versions.
		resp, data = env.rawRequest(t, http.MethodPost, "/api/v1/builds/"+bid+"/lease", env.writeToken,
			[]byte(`{"version":99,"future_field":123}`))
		assertErrorBody(t, resp, data, http.StatusBadRequest, queueapi.ErrCodeUnsupportedVersion)
		if !strings.Contains(string(data), "99") || !strings.Contains(string(data), "1") {
			t.Fatalf("unsupported_version message must name both versions: %s", data)
		}

		// Invalid UTF-8 -> 400.
		resp, data = env.rawRequest(t, http.MethodPost, "/api/v1/builds/"+bid+"/lease", env.writeToken,
			[]byte{'"', 0xff, '"'})
		assertErrorBody(t, resp, data, http.StatusBadRequest, queue.ErrCodeInvalidRequest)

		// Over 1 MiB on non-init POST -> 413 payload_too_large.
		over1M := bytes.Repeat([]byte(" "), queueapi.MaxOtherBodyBytes+1)
		resp, data = env.rawRequest(t, http.MethodPost, "/api/v1/builds/"+bid+"/lease", env.writeToken, over1M)
		assertErrorBody(t, resp, data, http.StatusRequestEntityTooLarge, queueapi.ErrCodePayloadTooLarge)

		// Over 4 MiB on init POST -> 413 payload_too_large.
		over4M := bytes.Repeat([]byte(" "), queueapi.MaxInitBodyBytes+1)
		resp, data = env.rawRequest(t, http.MethodPost, "/api/v1/builds/"+bid+"/init", env.writeToken, over4M)
		assertErrorBody(t, resp, data, http.StatusRequestEntityTooLarge, queueapi.ErrCodePayloadTooLarge)

		// Pagination limit 1001 -> 400.
		resp, data = env.rawRequest(t, http.MethodGet, "/api/v1/builds/"+bid+"/tests?limit=1001", env.readToken, nil)
		assertErrorBody(t, resp, data, http.StatusBadRequest, queue.ErrCodeInvalidRequest)
	})

	t.Run("full lifecycle, idempotency, conflicts, expiry, pagination", func(t *testing.T) {
		bid := uniqueBuildID("life")
		cfg := queue.Config{
			Machines:       2,
			LeaseTTLMS:     env.ttlMS,
			BuildTimeoutMS: max(env.ttlMS*20, 10_000),
			MaxAttempts:    3,
			MaxBatch:       1,
		}
		tests := []string{"tests/a.py", "tests/b.py", "tests/c.py"}
		p100, p200 := int64(100), int64(200)
		idx0, idx1 := 0, 1

		// Init created=true, then idempotent init with different predictions created=false.
		init1, err := wc.Init(ctx, queue.InitCommand{
			BuildID:       bid,
			RunnerID:      "runner-1",
			MachineIndex:  &idx0,
			Config:        cfg,
			Tests:         tests,
			PredictionsMS: []*int64{&p100, &p200, nil},
		})
		if err != nil || !init1.Created || init1.TestCount != 3 || init1.Verdict != queue.VerdictRunning {
			t.Fatalf("init1 = %+v, err=%v", init1, err)
		}
		init2, err := wc.Init(ctx, queue.InitCommand{
			BuildID:       bid,
			RunnerID:      "runner-2",
			MachineIndex:  &idx1,
			Config:        cfg,
			Tests:         tests,
			PredictionsMS: []*int64{nil, nil, nil},
		})
		if err != nil || init2.Created || init2.Digest != init1.Digest {
			t.Fatalf("init2 = %+v, err=%v", init2, err)
		}

		// Lease by runner-1, repeat same request_id -> same lease; fresh request_id while active -> 409 lease_held.
		l1, err := wc.Lease(ctx, bid, queue.LeaseCommand{RunnerID: "runner-1", MachineIndex: &idx0, RequestID: "r1-q1"})
		if err != nil || l1.Status != queue.LeaseGranted || len(l1.Attempts) != 1 {
			t.Fatalf("l1 = %+v, err=%v", l1, err)
		}
		l1Dup, err := wc.Lease(ctx, bid, queue.LeaseCommand{RunnerID: "runner-1", MachineIndex: &idx0, RequestID: "r1-q1"})
		if err != nil || l1Dup.LeaseID != l1.LeaseID {
			t.Fatalf("l1Dup = %+v, err=%v", l1Dup, err)
		}
		if _, err := wc.Lease(ctx, bid, queue.LeaseCommand{RunnerID: "runner-1", MachineIndex: &idx0, RequestID: "r1-q2"}); err == nil {
			t.Fatal("expected 409 lease_held")
		} else {
			var apiErr *queueapi.APIError
			if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 409 || apiErr.Code != queue.ErrCodeLeaseHeld {
				t.Fatalf("err = %v; want 409 lease_held", err)
			}
		}

		// Heartbeat active lease -> held=true.
		hb, err := wc.Heartbeat(ctx, bid, queue.HeartbeatCommand{RunnerID: "runner-1", LeaseID: l1.LeaseID})
		if err != nil || !hb.Held {
			t.Fatalf("hb = %+v, err=%v", hb, err)
		}

		// Wait past TTL so l1 expires, then heartbeat returns held=false and stale_request on old request_id.
		time.Sleep(time.Duration(env.ttlMS+80) * time.Millisecond)
		hbDead, err := wc.Heartbeat(ctx, bid, queue.HeartbeatCommand{RunnerID: "runner-1", LeaseID: l1.LeaseID})
		if err != nil || hbDead.Held {
			t.Fatalf("hbDead = %+v, err=%v; want held=false", hbDead, err)
		}
		if _, err := wc.Lease(ctx, bid, queue.LeaseCommand{RunnerID: "runner-1", MachineIndex: &idx0, RequestID: "r1-q1"}); err == nil {
			t.Fatal("expected 409 stale_request")
		} else {
			var apiErr *queueapi.APIError
			if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 409 || apiErr.Code != queue.ErrCodeStaleRequest {
				t.Fatalf("err = %v; want 409 stale_request", err)
			}
		}

		// Complete all 3 tests with runner-2.
		for step := range 3 {
			l, err := wc.Lease(ctx, bid, queue.LeaseCommand{
				RunnerID:     "runner-2",
				MachineIndex: &idx1,
				RequestID:    fmt.Sprintf("r2-%d", step),
			})
			if err != nil || l.Status != queue.LeaseGranted {
				t.Fatalf("step %d lease = %+v, err=%v", step, l, err)
			}
			a := l.Attempts[0]
			rep, err := wc.Report(ctx, bid, queue.ReportCommand{
				RunnerID: "runner-2",
				LeaseID:  l.LeaseID,
				Results: []queue.ReportResult{{
					AttemptID:  a.AttemptID,
					TestIndex:  a.TestIndex,
					Generation: a.Generation,
					Outcome:    queue.OutcomePassed,
					Timed:      true,
					DurationMS: int64(50 + step*10),
				}},
			})
			if err != nil || rep.Results[0].Disposition != queue.DispositionAccepted {
				t.Fatalf("step %d report = %+v, err=%v", step, rep, err)
			}
		}

		// Late report from runner-1 is stale.
		late, err := wc.Report(ctx, bid, queue.ReportCommand{
			RunnerID: "runner-1",
			LeaseID:  l1.LeaseID,
			Results: []queue.ReportResult{{
				AttemptID:  l1.Attempts[0].AttemptID,
				TestIndex:  l1.Attempts[0].TestIndex,
				Generation: l1.Attempts[0].Generation,
				Outcome:    queue.OutcomeFailed,
				Timed:      false,
				DurationMS: 0,
			}},
		})
		if err != nil || late.Results[0].Disposition != queue.DispositionStale || late.Verdict != queue.VerdictPassed {
			t.Fatalf("late report = %+v, err=%v", late, err)
		}

		// Read status, paginated tests (limit=2), retried filter, and builds list with read token.
		st, err := rc.Status(ctx, bid)
		if err != nil || st.Verdict != queue.VerdictPassed || st.Counts.Passed != 3 {
			t.Fatalf("status = %+v, err=%v", st, err)
		}
		p1, err := rc.ListTests(ctx, bid, queue.ListTestsCommand{Limit: 2})
		if err != nil || len(p1.Tests) != 2 || p1.NextCursor == nil {
			t.Fatalf("page1 = %+v, err=%v", p1, err)
		}
		p2, err := rc.ListTests(ctx, bid, queue.ListTestsCommand{Cursor: *p1.NextCursor, Limit: 2})
		if err != nil || len(p2.Tests) != 1 || p2.NextCursor != nil {
			t.Fatalf("page2 = %+v, err=%v", p2, err)
		}
		retried, err := rc.ListTests(ctx, bid, queue.ListTestsCommand{Retried: true})
		if err != nil || len(retried.Tests) != 1 || retried.Tests[0].TestIndex != l1.Attempts[0].TestIndex {
			t.Fatalf("retried = %+v, err=%v", retried, err)
		}
		builds := getListBuilds(t, env.baseURL, env.readToken, env.httpClient)
		found := false
		for _, b := range builds.Builds {
			if b.BuildID == bid && b.Verdict == queue.VerdictPassed {
				found = true
			}
		}
		if !found {
			t.Fatalf("build %q not listed in %+v", bid, builds.Builds)
		}

		// Post-pass init conflict flips verdict passed -> unresolved.
		_, err = wc.Init(ctx, queue.InitCommand{
			BuildID:       bid,
			RunnerID:      "runner-late",
			MachineIndex:  nil,
			Config:        cfg,
			Tests:         []string{"tests/a.py"},
			PredictionsMS: []*int64{nil},
		})
		var apiErr *queueapi.APIError
		if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 409 || apiErr.Code != queue.ErrCodeInitConflict {
			t.Fatalf("post-pass conflict err = %v; want 409 init_conflict", err)
		}
		stAfter, err := rc.Status(ctx, bid)
		if err != nil || stAfter.Verdict != queue.VerdictUnresolved || len(stAfter.Conflicts) != 1 {
			t.Fatalf("status after conflict = %+v, err=%v", stAfter, err)
		}
	})

	t.Run("concurrent runners under -race accept every test exactly once", func(t *testing.T) {
		const (
			numTests   = 120
			numRunners = 16
		)
		bid := uniqueBuildID("conc")
		tests := make([]string, numTests)
		preds := make([]*int64, numTests)
		for i := range numTests {
			tests[i] = fmt.Sprintf("tests/file_%03d.py", i)
			v := int64((i%7 + 1) * 100)
			preds[i] = &v
		}
		cfg := queue.Config{
			Machines:       numRunners,
			LeaseTTLMS:     max(env.ttlMS, 2000),
			BuildTimeoutMS: 60_000,
			MaxAttempts:    3,
			MaxBatch:       10,
		}

		var accepted [numTests]atomic.Int32
		var wg sync.WaitGroup
		errCh := make(chan error, numRunners)

		for r := range numRunners {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				runnerID := fmt.Sprintf("conc-runner-%02d", idx)
				mi := idx
				if _, err := wc.Init(ctx, queue.InitCommand{
					BuildID:       bid,
					RunnerID:      runnerID,
					MachineIndex:  &mi,
					Config:        cfg,
					Tests:         tests,
					PredictionsMS: preds,
				}); err != nil {
					errCh <- fmt.Errorf("%s init: %w", runnerID, err)
					return
				}

				for req := 1; ; req++ {
					l, err := wc.Lease(ctx, bid, queue.LeaseCommand{
						RunnerID:     runnerID,
						MachineIndex: &mi,
						RequestID:    fmt.Sprintf("req-%d", req),
					})
					if err != nil {
						errCh <- fmt.Errorf("%s lease: %w", runnerID, err)
						return
					}
					switch l.Status {
					case queue.LeaseDone:
						return
					case queue.LeaseWait:
						time.Sleep(10 * time.Millisecond)
						continue
					case queue.LeaseGranted:
						if _, err := wc.Heartbeat(ctx, bid, queue.HeartbeatCommand{
							RunnerID: runnerID,
							LeaseID:  l.LeaseID,
						}); err != nil {
							errCh <- fmt.Errorf("%s heartbeat: %w", runnerID, err)
							return
						}
						results := make([]queue.ReportResult, len(l.Attempts))
						for i, a := range l.Attempts {
							results[i] = queue.ReportResult{
								AttemptID:  a.AttemptID,
								TestIndex:  a.TestIndex,
								Generation: a.Generation,
								Outcome:    queue.OutcomePassed,
								Timed:      true,
								DurationMS: 25,
							}
						}
						rep, err := wc.Report(ctx, bid, queue.ReportCommand{
							RunnerID: runnerID,
							LeaseID:  l.LeaseID,
							Results:  results,
						})
						if err != nil {
							errCh <- fmt.Errorf("%s report: %w", runnerID, err)
							return
						}
						for _, d := range rep.Results {
							if d.Disposition == queue.DispositionAccepted {
								accepted[d.TestIndex].Add(1)
							}
						}
					}
				}
			}(r)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}

		for i := range numTests {
			if got := accepted[i].Load(); got != 1 {
				t.Fatalf("test %d accepted %d times; want 1", i, got)
			}
		}
		st, err := rc.Status(ctx, bid)
		if err != nil || st.Verdict != queue.VerdictPassed || st.Counts.Passed != numTests {
			t.Fatalf("final status = %+v, err=%v", st, err)
		}
	})
}
