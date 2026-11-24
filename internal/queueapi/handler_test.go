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
	Machines: 1, LeaseTTLMS: queue.DefaultLeaseTTLMS, BuildTimeoutMS: queue.DefaultBuildTimeoutMS, MaxAttempts: 3, MaxBatch: 1,
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
	c, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: cfg.WriteToken, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return srv, c
}

type blockingLog struct {
	blockBuild                                 string
	entered, release                           chan struct{}
	enteredOnce                                sync.Once
	failUnwritten, failUncertain, sawCancelled atomic.Bool
	mu                                         sync.Mutex
	appended                                   map[string][]queue.EventType
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
	exactWrite, exactRead := strings.Repeat("w", queueapi.MinTokenBytes), strings.Repeat("r", queueapi.MinTokenBytes)
	if err := queueapi.ValidateTokens(exactWrite, exactRead); err != nil {
		t.Fatalf("exact %d-byte distinct tokens rejected: %v", queueapi.MinTokenBytes, err)
	}
	for _, tc := range [][2]string{
		{"", exactRead}, {exactWrite, ""}, {exactWrite[:len(exactWrite)-1], exactRead},
		{exactWrite, exactRead[:len(exactRead)-1]}, {exactWrite, exactWrite},
	} {
		if err := queueapi.ValidateTokens(tc[0], tc[1]); err == nil {
			t.Errorf("ValidateTokens(%q, %q) = nil; want error", tc[0], tc[1])
		}
	}
}

func TestSlowAppendDoesNotBlockOtherBuildsAndErrorsPreserveInvariants(t *testing.T) {
	log := &blockingLog{blockBuild: "b-slow", entered: make(chan struct{}), release: make(chan struct{})}
	_, c := newTestServer(t, queueapi.HandlerConfig{Log: log})
	ctx := context.Background()
	initOne := func(id, runner string) error {
		_, err := c.Init(ctx, queue.InitCommand{BuildID: id, RunnerID: runner, Config: defaultTestCfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}})
		return err
	}
	assertHTTP := func(err error, status int, code string) {
		t.Helper()
		var apiErr *queueapi.APIError
		if !errors.As(err, &apiErr) || apiErr.HTTPStatus != status || (code != "" && apiErr.Code != code) {
			t.Fatalf("err = %v; want HTTP %d %s", err, status, code)
		}
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

	log.failUnwritten.Store(true)
	err := initOne("b-clean", "r3")
	log.failUnwritten.Store(false)
	assertHTTP(err, 503, queueapi.ErrCodeStorageError)
	_, err = c.Status(ctx, "b-clean")
	assertHTTP(err, 404, "")

	log.failUncertain.Store(true)
	err = initOne("b-broken-init", "r4")
	log.failUncertain.Store(false)
	assertHTTP(err, 500, queueapi.ErrCodeInternalError)
	_, err = c.Status(ctx, "b-broken-init")
	assertHTTP(err, 500, "")

	log.failUncertain.Store(true)
	_, err = c.Lease(ctx, "b-fast", queue.LeaseCommand{RunnerID: "r2", RequestID: "q-uncertain"})
	log.failUncertain.Store(false)
	assertHTTP(err, 500, queueapi.ErrCodeInternalError)
	_, err = c.Status(ctx, "b-fast")
	assertHTTP(err, 500, "")
}

func TestCapacityRetentionAndIdleEviction(t *testing.T) {
	var nowMS atomic.Int64
	nowMS.Store(1_000_000)
	log := &blockingLog{entered: make(chan struct{}), release: make(chan struct{})}
	srv, c := newTestServer(t, queueapi.HandlerConfig{
		Clock: func() int64 { return nowMS.Load() }, MaxBuilds: 2, MaxRunning: 2, RetentionMS: 100_000, Log: log,
	})
	ctx := context.Background()
	cfg := queue.Config{Machines: 1, LeaseTTLMS: 10_000, BuildTimeoutMS: 60_000, MaxAttempts: 3, MaxBatch: 1}
	initBuild := func(id string) error {
		_, err := c.Init(ctx, queue.InitCommand{BuildID: id, RunnerID: "r1", Config: cfg, Tests: []string{"a.py"}, PredictionsMS: []*int64{nil}})
		return err
	}
	if err := initBuild("b1"); err != nil {
		t.Fatal(err)
	}
	nowMS.Store(1_005_000)
	if err := initBuild("b2"); err != nil {
		t.Fatal(err)
	}
	var apiErr *queueapi.APIError
	if err := initBuild("b3"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 503 || apiErr.Code != queueapi.ErrCodeAtCapacity {
		t.Fatalf("at capacity err = %v; want 503 at_capacity", err)
	}

	nowMS.Store(1_015_000)
	if _, err := c.Lease(ctx, "b2", queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"}); err != nil {
		t.Fatal(err)
	}
	nowMS.Store(1_025_000)
	if err := initBuild("b3"); err != nil {
		t.Fatalf("b3 should evict idle b1: %v", err)
	}
	if _, err := c.Status(ctx, "b1"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
		t.Fatalf("evicted b1 err = %v; want 404", err)
	}

	nowMS.Store(1_065_000)
	list := getListBuilds(t, srv.URL, testWriteTok, srv.Client())
	foundB2 := false
	for _, b := range list.Builds {
		foundB2 = foundB2 || (b.BuildID == "b2" && b.Verdict == queue.VerdictUnresolved && b.FinishedAtMS != nil)
	}
	log.mu.Lock()
	b2Evs := log.appended["b2"]
	log.mu.Unlock()
	if !foundB2 || len(b2Evs) == 0 || b2Evs[len(b2Evs)-1] != queue.EventFinalized {
		t.Fatalf("b2 deadline finalization failed: found=%t evs=%v", foundB2, b2Evs)
	}
	nowMS.Store(1_165_000)
	if _, err := c.Status(ctx, "b2"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
		t.Fatalf("b2 past retention err = %v; want 404", err)
	}
	log.failUncertain.Store(true)
	_, _ = c.Lease(ctx, "b3", queue.LeaseCommand{RunnerID: "r1", RequestID: "q3"})
	log.failUncertain.Store(false)
	if err := initBuild("b4"); err != nil {
		t.Fatalf("b4 should evict broken b3: %v", err)
	}
}

func TestClientHardeningURLRedirectsTimeoutAndMalformed200(t *testing.T) {
	if _, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: "http://queue.example.com", Token: testWriteTok}); err == nil {
		t.Fatal("expected plain http:// non-loopback to fail")
	}
	if _, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: "https://user:pass@queue.example.com", Token: testWriteTok}); err == nil {
		t.Fatal("expected embedded credentials to fail")
	}
	if _, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: "http://127.0.0.1:8080", Token: testWriteTok}); err != nil {
		t.Fatalf("loopback http rejected: %v", err)
	}
	redirSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example/steal", http.StatusFound)
	}))
	defer redirSrv.Close()
	rc, _ := queueapi.NewClient(queueapi.ClientConfig{BaseURL: redirSrv.URL, Token: testWriteTok, HTTPClient: redirSrv.Client()})
	if _, err := rc.Status(context.Background(), "b1"); err == nil || !strings.Contains(err.Error(), "redirect") || strings.Contains(err.Error(), testWriteTok) {
		t.Fatalf("redirect err = %v", err)
	}
	releaseHang := make(chan struct{})
	hangSrv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-releaseHang:
		}
	}))
	defer func() { close(releaseHang); hangSrv.Close() }()
	tc, _ := queueapi.NewClient(queueapi.ClientConfig{BaseURL: hangSrv.URL, Token: testReadTok, HTTPClient: &http.Client{Timeout: 60 * time.Millisecond}})
	if _, err := tc.Status(context.Background(), "b1"); err == nil || !queueapi.IsTransientError(err) {
		t.Fatalf("http.Client.Timeout err = %v (transient=%t)", err, queueapi.IsTransientError(err))
	}
	for _, body := range []string{`{}`, `{"version":1,"status":"mystery"}`, `{"version":1,"status":"wait"}`, `{"version":1,"status":"done","verdict":"bogus"}`} {
		badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
			_, _ = w.Write([]byte(body))
		}))
		bc, _ := queueapi.NewClient(queueapi.ClientConfig{BaseURL: badSrv.URL, Token: testWriteTok, HTTPClient: badSrv.Client()})
		_, err := bc.Lease(context.Background(), "b1", queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
		badSrv.Close()
		if err == nil {
			t.Fatalf("Lease(%s) succeeded; want error", body)
		}
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
	c, _ := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: testWriteTok, HTTPClient: srv.Client()})
	ctx := context.Background()
	pad, tests, preds := strings.Repeat("dir/", 110), make([]string, 1000), make([]*int64, 1000)
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
	c, _ := queueapi.NewClient(queueapi.ClientConfig{BaseURL: srv.URL, Token: testWriteTok, HTTPClient: srv.Client()})
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
