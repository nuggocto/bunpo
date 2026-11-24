package queueapi_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

const (
	testWriteTok = "write-secret-token-0123456789abcdef"
	testReadTok  = "read-secret-token-fedcba9876543210"
)

var defaultTestCfg = queue.Config{
	Machines:       1,
	LeaseTTLMS:     queue.DefaultLeaseTTLMS,
	BuildTimeoutMS: queue.DefaultBuildTimeoutMS,
	MaxAttempts:    3,
	MaxBatch:       1,
}

func newTestServer(t *testing.T, cfg queueapi.HandlerConfig) (*httptest.Server, *queueapi.Client) {
	t.Helper()
	if cfg.WriteToken == "" {
		cfg.WriteToken = testWriteTok
	}
	if cfg.ReadToken == "" {
		cfg.ReadToken = testReadTok
	}
	h, err := queueapi.NewHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:    srv.URL,
		Token:      cfg.WriteToken,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, c
}

type blockingLog struct {
	blockBuild    string
	entered       chan struct{}
	release       chan struct{}
	enteredOnce   sync.Once
	failUnwritten atomic.Bool
	failUncertain atomic.Bool
	sawCancelled  atomic.Bool
	mu            sync.Mutex
	appended      map[string][]queue.EventType
}

func (l *blockingLog) Append(ctx context.Context, buildID string, events []queue.Event) error {
	if buildID == l.blockBuild {
		l.enteredOnce.Do(func() { close(l.entered) })
		<-l.release
		if ctx.Err() != nil {
			l.sawCancelled.Store(true)
		}
	}
	if l.failUnwritten.Load() {
		return fmt.Errorf("simulated clean write error: %w", queueapi.ErrAppendUnwritten)
	}
	if l.failUncertain.Load() {
		return errors.New("simulated uncertain fsync error")
	}
	l.mu.Lock()
	if l.appended == nil {
		l.appended = make(map[string][]queue.EventType)
	}
	for _, ev := range events {
		l.appended[buildID] = append(l.appended[buildID], ev.Type)
	}
	l.mu.Unlock()
	return nil
}

func (l *blockingLog) Remove(_ context.Context, _ string) error { return nil }

func assertAPIErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var apiErr *queueapi.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != status || (code != "" && apiErr.Code != code) {
		t.Fatalf("err = %v; want HTTP %d %s", err, status, code)
	}
}

func TestValidateTokens(t *testing.T) {
	exactWrite := strings.Repeat("w", queueapi.MinTokenBytes)
	exactRead := strings.Repeat("r", queueapi.MinTokenBytes)

	cases := []struct {
		name      string
		writeTok  string
		readTok   string
		wantValid bool
	}{
		{"exact minimum distinct tokens", exactWrite, exactRead, true},
		{"empty write token", "", exactRead, false},
		{"empty read token", exactWrite, "", false},
		{"write token one byte short", exactWrite[:len(exactWrite)-1], exactRead, false},
		{"read token one byte short", exactWrite, exactRead[:len(exactRead)-1], false},
		{"equal tokens", exactWrite, exactWrite, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := queueapi.ValidateTokens(tc.writeTok, tc.readTok)
			if (err == nil) != tc.wantValid {
				t.Fatalf("ValidateTokens = %v; want valid=%t", err, tc.wantValid)
			}
		})
	}
}

func TestSlowAppendDoesNotBlockOtherBuilds(t *testing.T) {
	log := &blockingLog{
		blockBuild: "b-slow",
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	_, c := newTestServer(t, queueapi.HandlerConfig{Log: log})
	ctx := context.Background()
	initOne := func(id, runner string) error {
		_, err := c.Init(ctx, queue.InitCommand{
			BuildID:       id,
			RunnerID:      runner,
			Config:        defaultTestCfg,
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		})
		return err
	}

	slowDone := make(chan error, 1)
	go func() { slowDone <- initOne("b-slow", "r1") }()
	<-log.entered
	if err := initOne("b-fast", "r2"); err != nil {
		close(log.release)
		t.Fatalf("b-fast blocked by b-slow: %v", err)
	}
	close(log.release)
	if err := <-slowDone; err != nil {
		t.Fatalf("b-slow failed: %v", err)
	}
}

func TestAppendErrorsPreserveInvariants(t *testing.T) {
	cases := []struct {
		name          string
		op            string
		unwritten     bool
		wantStatus    int
		wantCode      string
		wantGetStatus int
		wantGetCode   string
	}{
		{
			name:          "unwritten on init returns 503 and build stays absent (404)",
			op:            "init",
			unwritten:     true,
			wantStatus:    503,
			wantCode:      queueapi.ErrCodeStorageError,
			wantGetStatus: 404,
			wantGetCode:   queue.ErrCodeBuildNotFound,
		},
		{
			name:          "uncertain on init returns 500 and marks build broken (500)",
			op:            "init",
			unwritten:     false,
			wantStatus:    500,
			wantCode:      queueapi.ErrCodeInternalError,
			wantGetStatus: 500,
			wantGetCode:   queueapi.ErrCodeInternalError,
		},
		{
			name:          "unwritten on lease returns 503 and build stays healthy (200)",
			op:            "lease",
			unwritten:     true,
			wantStatus:    503,
			wantCode:      queueapi.ErrCodeStorageError,
			wantGetStatus: 200,
		},
		{
			name:          "uncertain on lease returns 500 and marks build broken (500)",
			op:            "lease",
			unwritten:     false,
			wantStatus:    500,
			wantCode:      queueapi.ErrCodeInternalError,
			wantGetStatus: 500,
			wantGetCode:   queueapi.ErrCodeInternalError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &blockingLog{}
			_, c := newTestServer(t, queueapi.HandlerConfig{Log: log})
			ctx := context.Background()
			bid := "b-" + tc.op

			if tc.op == "lease" {
				if _, err := c.Init(ctx, queue.InitCommand{
					BuildID:       bid,
					RunnerID:      "r1",
					Config:        defaultTestCfg,
					Tests:         []string{"a.py"},
					PredictionsMS: []*int64{nil},
				}); err != nil {
					t.Fatal(err)
				}
			}

			if tc.unwritten {
				log.failUnwritten.Store(true)
			} else {
				log.failUncertain.Store(true)
			}
			var err error
			if tc.op == "init" {
				_, err = c.Init(ctx, queue.InitCommand{
					BuildID:       bid,
					RunnerID:      "r1",
					Config:        defaultTestCfg,
					Tests:         []string{"a.py"},
					PredictionsMS: []*int64{nil},
				})
			} else {
				_, err = c.Lease(ctx, bid, queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
			}
			log.failUnwritten.Store(false)
			log.failUncertain.Store(false)
			assertAPIErr(t, err, tc.wantStatus, tc.wantCode)

			st, getErr := c.Status(ctx, bid)
			if tc.wantGetStatus == 200 {
				if getErr != nil || st.Verdict != queue.VerdictRunning || st.Counts.Leased != 0 {
					t.Fatalf("GET after unwritten lease = (%+v, %v); want running with 0 leased", st, getErr)
				}
				return
			}
			assertAPIErr(t, getErr, tc.wantGetStatus, tc.wantGetCode)
		})
	}
}

func TestIsTransientError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"HTTP 429 rate limited", &queueapi.APIError{HTTPStatus: 429, Code: "rate_limited"}, true},
		{"HTTP 500 boundary", &queueapi.APIError{HTTPStatus: 500, Code: queueapi.ErrCodeInternalError}, true},
		{"HTTP 503 storage error", &queueapi.APIError{HTTPStatus: 503, Code: queueapi.ErrCodeStorageError}, true},
		{"HTTP 499 just below 500", &queueapi.APIError{HTTPStatus: 499, Code: "client_closed"}, false},
		{"HTTP 400 invalid request", &queueapi.APIError{HTTPStatus: 400, Code: queue.ErrCodeInvalidRequest}, false},
		{"HTTP 404 not found", &queueapi.APIError{HTTPStatus: 404, Code: queue.ErrCodeBuildNotFound}, false},
		{"HTTP 409 conflict", &queueapi.APIError{HTTPStatus: 409, Code: queue.ErrCodeInitConflict}, false},
		{"transport error", queueapi.NewTransportError("connection reset"), true},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
		{"plain non-transport error", errors.New("decode error"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := queueapi.IsTransientError(tc.err); got != tc.want {
				t.Fatalf("IsTransientError(%v) = %t; want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestCapacityRetentionAndIdleEviction(t *testing.T) {
	var nowMS atomic.Int64
	nowMS.Store(1_000_000)
	log := &blockingLog{}
	srv, c := newTestServer(t, queueapi.HandlerConfig{
		Clock:       func() int64 { return nowMS.Load() },
		MaxBuilds:   2,
		MaxRunning:  2,
		RetentionMS: 100_000,
		Log:         log,
	})
	ctx := context.Background()
	cfg := queue.Config{
		Machines:       1,
		LeaseTTLMS:     10_000,
		BuildTimeoutMS: 60_000,
		MaxAttempts:    3,
		MaxBatch:       1,
	}
	initBuild := func(id string) error {
		_, err := c.Init(ctx, queue.InitCommand{
			BuildID:       id,
			RunnerID:      "r1",
			Config:        cfg,
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		})
		return err
	}

	if err := initBuild("b1"); err != nil {
		t.Fatal(err)
	}
	nowMS.Store(1_005_000)
	if err := initBuild("b2"); err != nil {
		t.Fatal(err)
	}

	// GET /api/v1/builds finalizes past-deadline running builds through EventLog.
	nowMS.Store(1_065_000)
	list := getListBuilds(t, srv.URL, testWriteTok, srv.Client())
	foundB2 := false
	for _, b := range list.Builds {
		if b.BuildID == "b2" && b.Verdict == queue.VerdictUnresolved && b.FinishedAtMS != nil {
			foundB2 = true
		}
	}
	log.mu.Lock()
	b2Evs := log.appended["b2"]
	log.mu.Unlock()
	if !foundB2 || len(b2Evs) == 0 || b2Evs[len(b2Evs)-1] != queue.EventFinalized {
		t.Fatalf("b2 deadline finalization failed: found=%t evs=%v", foundB2, b2Evs)
	}

	// Past retention, GET returns 404 and evicts the build.
	nowMS.Store(1_165_000)
	_, err := c.Status(ctx, "b2")
	assertAPIErr(t, err, 404, queue.ErrCodeBuildNotFound)

	// A broken build is evicted under capacity pressure while quarantined in h.damaged.
	if err := initBuild("b3"); err != nil {
		t.Fatal(err)
	}
	if err := initBuild("b4"); err != nil {
		t.Fatal(err)
	}
	log.failUncertain.Store(true)
	_, _ = c.Lease(ctx, "b3", queue.LeaseCommand{RunnerID: "r1", RequestID: "q3"})
	log.failUncertain.Store(false)
	if err := initBuild("b5"); err != nil {
		t.Fatalf("b5 should evict broken b3: %v", err)
	}
	_, err = c.Status(ctx, "b3")
	assertAPIErr(t, err, 500, queueapi.ErrCodeInternalError)
}

func TestClientURLRules(t *testing.T) {
	cases := []struct {
		name          string
		rawURL        string
		allowInsecure bool
		wantOK        bool
	}{
		{"https host", "https://queue.example.com", false, true},
		{"loopback IPv4 http", "http://127.0.0.1:8080", false, true},
		{"loopback IPv6 http", "http://[::1]:8080", false, true},
		{"localhost http", "http://localhost:8080", false, true},
		{"remote host plain http refused", "http://queue.example.com", false, false},
		{"remote host plain http allowed with flag", "http://queue.example.com", true, true},
		{"non-loopback IPv4 plain http refused", "http://192.0.2.1:8080", false, false},
		{"non-loopback IPv4 plain http allowed with flag", "http://192.0.2.1:8080", true, true},
		{"non-loopback IPv6 plain http refused", "http://[2001:db8::1]:8080", false, false},
		{"non-loopback IPv6 plain http allowed with flag", "http://[2001:db8::1]:8080", true, true},
		{"unsupported ftp scheme", "ftp://queue.example.com", true, false},
		{"embedded userinfo refused", "https://user:pass@queue.example.com", false, false},
		{"missing host refused", "https://", false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := queueapi.NewClient(queueapi.ClientConfig{
				BaseURL:           tc.rawURL,
				Token:             testWriteTok,
				AllowInsecureHTTP: tc.allowInsecure,
			})
			if (err == nil) != tc.wantOK {
				t.Fatalf("NewClient(%q, insecure=%t) err=%v; want ok=%t", tc.rawURL, tc.allowInsecure, err, tc.wantOK)
			}
		})
	}
}

func TestClientMalformed200Bodies(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty object", `{}`},
		{"unknown lease status", `{"version":1,"status":"mystery"}`},
		{"wait missing retry_after_ms", `{"version":1,"status":"wait"}`},
		{"done with bogus verdict", `{"version":1,"status":"done","verdict":"bogus"}`},
		{"leased with empty attempts", `{"version":1,"status":"leased","lease_id":1,"attempts":[]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c, err := queueapi.NewClient(queueapi.ClientConfig{
				BaseURL:    srv.URL,
				Token:      testWriteTok,
				HTTPClient: srv.Client(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Lease(context.Background(), "b1", queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"}); err == nil {
				t.Fatalf("Lease(%s) succeeded; want error", tc.body)
			}
		})
	}
}

func TestClientRefusesRedirectWithoutLeakingToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example/steal", http.StatusFound)
	}))
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:    srv.URL,
		Token:      testWriteTok,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Status(context.Background(), "b1")
	if err == nil || !strings.Contains(err.Error(), "redirect") || strings.Contains(err.Error(), testWriteTok) {
		t.Fatalf("redirect err = %v", err)
	}
}

func TestClientTimeoutIsTransient(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		close(release)
		srv.Close()
	}()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:    srv.URL,
		Token:      testReadTok,
		HTTPClient: &http.Client{Timeout: 60 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Status(context.Background(), "b1")
	if err == nil || !queueapi.IsTransientError(err) {
		t.Fatalf("http.Client.Timeout err = %v (transient=%t)", err, queueapi.IsTransientError(err))
	}
}

type notifyFirstWriteWriter struct {
	http.ResponseWriter
	once   sync.Once
	notify chan struct{}
}

func (w *notifyFirstWriteWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.notify) })
	return w.ResponseWriter.Write(p)
}

func TestSlowReaderDoesNotHoldBuildLock(t *testing.T) {
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{WriteToken: testWriteTok, ReadToken: testReadTok})
	if err != nil {
		t.Fatal(err)
	}
	writeStarted := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tests") {
			w = &notifyFirstWriteWriter{ResponseWriter: w, notify: writeStarted}
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: testWriteTok, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pad := strings.Repeat("dir/", 110)
	tests := make([]string, 1000)
	preds := make([]*int64, 1000)
	for i := range tests {
		tests[i] = fmt.Sprintf("%st%04d.py", pad, i)
	}
	if _, err := c.Init(ctx, queue.InitCommand{BuildID: "slow-read", RunnerID: "r1", Config: defaultTestCfg, Tests: tests, PredictionsMS: preds}); err != nil {
		t.Fatal(err)
	}
	l, err := c.Lease(ctx, "slow-read", queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "GET /api/v1/builds/slow-read/tests?limit=1000 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n", testReadTok)
	select {
	case <-writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for slow-reader response write")
	}

	start := time.Now()
	if hb, err := c.Heartbeat(ctx, "slow-read", queue.HeartbeatCommand{RunnerID: "r1", LeaseID: l.LeaseID}); err != nil || !hb.Held || time.Since(start) > time.Second {
		t.Fatalf("heartbeat during stalled reader = (%+v, %v, %v)", hb, err, time.Since(start))
	}
}

func TestClientDisconnectDoesNotCancelAppendContext(t *testing.T) {
	log := &blockingLog{blockBuild: "b-disc", entered: make(chan struct{}), release: make(chan struct{})}
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{WriteToken: testWriteTok, ReadToken: testReadTok, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	reqCancelled := make(chan struct{})
	var cancelOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-r.Context().Done():
				cancelOnce.Do(func() { close(reqCancelled) })
			case <-done:
			}
		}()
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: testWriteTok, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	reqCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = c.Init(reqCtx, queue.InitCommand{BuildID: "b-disc", RunnerID: "r1", Config: defaultTestCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}})
		close(done)
	}()
	<-log.entered
	cancel()
	select {
	case <-reqCancelled:
	case <-time.After(5 * time.Second):
		close(log.release)
		t.Fatal("timed out waiting for server request context cancellation")
	}
	close(log.release)
	<-done
	if log.sawCancelled.Load() {
		t.Fatal("EventLog.Append context was cancelled when client disconnected")
	}
}
