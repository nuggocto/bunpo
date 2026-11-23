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

func TestValidateTokens(t *testing.T) {
	if err := queueapi.ValidateTokens("", testReadTok); err == nil {
		t.Fatal("expected error for empty write token")
	}
	if err := queueapi.ValidateTokens("short", testReadTok); err == nil {
		t.Fatal("expected error for short write token")
	}
	if err := queueapi.ValidateTokens(testWriteTok, "short"); err == nil {
		t.Fatal("expected error for short read token")
	}
	if err := queueapi.ValidateTokens(testWriteTok, testWriteTok); err == nil {
		t.Fatal("expected error for equal tokens")
	}
	if err := queueapi.ValidateTokens(testWriteTok, testReadTok); err != nil {
		t.Fatalf("valid tokens rejected: %v", err)
	}
}

type blockingLog struct {
	blockBuild    string
	entered       chan struct{}
	release       chan struct{}
	failUnwritten atomic.Bool
	failUncertain atomic.Bool
	corrupt       atomic.Bool
	sawCancelled  atomic.Bool
	mu            sync.Mutex
	appended      map[string][]queue.EventType
}

func (l *blockingLog) Append(ctx context.Context, buildID string, events []queue.Event) error {
	if l.failUnwritten.Load() {
		return fmt.Errorf("disk full before write: %w", queueapi.ErrAppendUnwritten)
	}
	if l.failUncertain.Load() {
		return errors.New("partial write I/O error")
	}
	if buildID == l.blockBuild {
		close(l.entered)
		<-l.release
		if ctx.Err() != nil {
			l.sawCancelled.Store(true)
		}
	}
	if l.corrupt.Swap(false) && len(events) > 0 {
		// Deliberately mutate the event slice to simulate an Apply failure after
		// Append succeeded, verifying the handler marks the build broken (500).
		events[0].Type = "corrupt_event_type"
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

func TestSlowAppendDoesNotBlockOtherBuildsAndErrorsPreserveInvariants(t *testing.T) {
	log := &blockingLog{
		blockBuild: "build-slow",
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: testWriteTok,
		ReadToken:  testReadTok,
		Log:        log,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:      srv.URL,
		Token:        testWriteTok,
		HTTPClient:   srv.Client(),
		RetryBudget:  10 * time.Millisecond, // fail fast on 503
		RetryBase:    5 * time.Millisecond,
		RetryMaxWait: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := queue.Config{
		Machines:       2,
		LeaseTTLMS:     queue.DefaultLeaseTTLMS,
		BuildTimeoutMS: queue.DefaultBuildTimeoutMS,
		MaxAttempts:    3,
		MaxBatch:       10,
	}
	ctx := context.Background()

	// Start slow init on build-slow.
	slowDone := make(chan error, 1)
	go func() {
		_, err := c.Init(ctx, queue.InitCommand{
			BuildID:       "build-slow",
			RunnerID:      "r1",
			Config:        cfg,
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		})
		slowDone <- err
	}()
	<-log.entered

	// While build-slow is blocked in Append, build-fast must succeed without waiting.
	fastDone := make(chan error, 1)
	go func() {
		_, err := c.Init(ctx, queue.InitCommand{
			BuildID:       "build-fast",
			RunnerID:      "r2",
			Config:        cfg,
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		})
		fastDone <- err
	}()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatalf("build-fast failed while build-slow was blocked: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(log.release)
		t.Fatal("build-fast was blocked by slow Append on build-slow")
	}
	close(log.release)
	if err := <-slowDone; err != nil {
		t.Fatalf("build-slow failed: %v", err)
	}

	// Cleanly unwritten Append failure on brand-new build -> 503 storage_error, slot cleaned up.
	log.failUnwritten.Store(true)
	_, err = c.Init(ctx, queue.InitCommand{
		BuildID:       "build-fail-init",
		RunnerID:      "r3",
		Config:        cfg,
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{nil},
	})
	log.failUnwritten.Store(false)
	var apiErr *queueapi.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 503 || apiErr.Code != queueapi.ErrCodeStorageError {
		t.Fatalf("failed init err = %v; want 503 storage_error", err)
	}
	if _, err := c.Status(ctx, "build-fail-init"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
		t.Fatalf("failed init leaked build entry: %v", err)
	}

	// Cleanly unwritten Append failure on lease leaves memory unchanged (test still pending).
	log.failUnwritten.Store(true)
	_, err = c.Lease(ctx, "build-fast", queue.LeaseCommand{RunnerID: "r2", RequestID: "q1"})
	log.failUnwritten.Store(false)
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 503 || apiErr.Code != queueapi.ErrCodeStorageError {
		t.Fatalf("failed lease err = %v; want 503 storage_error", err)
	}
	st, err := c.Status(ctx, "build-fast")
	if err != nil || st.Counts.Pending != 1 || st.Counts.Leased != 0 {
		t.Fatalf("state changed after failed Append: %+v, err=%v", st, err)
	}

	// Uncertain Append error (does not wrap ErrAppendUnwritten) marks build broken (500 forever).
	if _, err := c.Init(ctx, queue.InitCommand{
		BuildID:       "build-uncertain",
		RunnerID:      "r4",
		Config:        cfg,
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{nil},
	}); err != nil {
		t.Fatal(err)
	}
	log.failUncertain.Store(true)
	_, err = c.Lease(ctx, "build-uncertain", queue.LeaseCommand{RunnerID: "r4", RequestID: "q1"})
	log.failUncertain.Store(false)
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 500 || apiErr.Code != queueapi.ErrCodeInternalError {
		t.Fatalf("uncertain append err = %v; want 500 internal_error", err)
	}
	if _, err := c.Status(ctx, "build-uncertain"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 500 {
		t.Fatalf("uncertain-broken build subsequent status err = %v; want 500", err)
	}

	// Apply failure after successful Append marks that build broken (500 forever),
	// while other builds stay healthy.
	log.corrupt.Store(true)
	_, err = c.Lease(ctx, "build-fast", queue.LeaseCommand{RunnerID: "r2", RequestID: "q2"})
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 500 || apiErr.Code != queueapi.ErrCodeInternalError {
		t.Fatalf("corrupt apply err = %v; want 500 internal_error", err)
	}
	_, err = c.Status(ctx, "build-fast")
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 500 || apiErr.Code != queueapi.ErrCodeInternalError {
		t.Fatalf("broken build subsequent status err = %v; want 500 internal_error", err)
	}
	if stSlow, err := c.Status(ctx, "build-slow"); err != nil || stSlow.Verdict != queue.VerdictRunning {
		t.Fatalf("other build affected by broken build: %+v, err=%v", stSlow, err)
	}
}

func TestCapacityAndRetentionWithInjectedClock(t *testing.T) {
	var nowMS atomic.Int64
	nowMS.Store(1_000_000)

	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken:  testWriteTok,
		ReadToken:   testReadTok,
		Clock:       func() int64 { return nowMS.Load() },
		MaxBuilds:   3,
		MaxRunning:  2,
		RetentionMS: 100_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:      srv.URL,
		Token:        testWriteTok,
		HTTPClient:   srv.Client(),
		RetryBudget:  10 * time.Millisecond,
		RetryBase:    5 * time.Millisecond,
		RetryMaxWait: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cfg := queue.Config{
		Machines:       1,
		LeaseTTLMS:     queue.DefaultLeaseTTLMS,
		BuildTimeoutMS: queue.DefaultBuildTimeoutMS,
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
	finishBuild := func(id string) {
		t.Helper()
		l, err := c.Lease(ctx, id, queue.LeaseCommand{RunnerID: "r1", RequestID: "q-" + id})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Report(ctx, id, queue.ReportCommand{
			RunnerID: "r1",
			LeaseID:  l.LeaseID,
			Results:  []queue.ReportResult{{AttemptID: "0.1", TestIndex: 0, Generation: 1, Outcome: queue.OutcomePassed, Timed: true, DurationMS: 10}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := initBuild("b1"); err != nil {
		t.Fatal(err)
	}
	if err := initBuild("b2"); err != nil {
		t.Fatal(err)
	}
	// MaxRunning=2 -> b3 gets 503 at_capacity.
	var apiErr *queueapi.APIError
	if err := initBuild("b3"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 503 || apiErr.Code != queueapi.ErrCodeAtCapacity {
		t.Fatalf("b3 at MaxRunning err = %v; want 503 at_capacity", err)
	}

	// Finish b1 and b2, add b3, then adding b4 evicts oldest finished build (b1).
	finishBuild("b1")
	nowMS.Add(1000)
	finishBuild("b2")
	nowMS.Add(1000)
	if err := initBuild("b3"); err != nil {
		t.Fatal(err)
	}
	if err := initBuild("b4"); err != nil {
		t.Fatalf("b4 should evict oldest finished b1: %v", err)
	}
	if _, err := c.Status(ctx, "b1"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
		t.Fatalf("b1 should have been evicted: %v", err)
	}

	// Advance past retention (100_000 ms) -> finished b2 expires to 404.
	nowMS.Add(150_000)
	if _, err := c.Status(ctx, "b2"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
		t.Fatalf("b2 should have expired past retention: %v", err)
	}
}

func TestClientHardening(t *testing.T) {
	// Rejects plain http:// for non-loopback unless AllowInsecureHTTP.
	if _, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: "http://queue.example.com", Token: testWriteTok}); err == nil {
		t.Fatal("expected error for non-loopback http://")
	}
	if _, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: "http://queue.example.com", Token: testWriteTok, AllowInsecureHTTP: true}); err != nil {
		t.Fatalf("AllowInsecureHTTP rejected: %v", err)
	}
	if _, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: "http://127.0.0.1:8080", Token: testWriteTok}); err != nil {
		t.Fatalf("loopback http rejected: %v", err)
	}
	if _, err := queueapi.NewClient(queueapi.ClientConfig{BaseURL: "https://user:pass@queue.example.com", Token: testWriteTok}); err == nil {
		t.Fatal("expected error for embedded credentials")
	}

	// Refuses redirects and never leaks token in error messages.
	redirSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example.com", http.StatusFound)
	}))
	defer redirSrv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:     redirSrv.URL,
		Token:       testWriteTok,
		HTTPClient:  redirSrv.Client(),
		RetryBudget: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Status(context.Background(), "b1")
	if err == nil || !strings.Contains(err.Error(), "redirect") || strings.Contains(err.Error(), testWriteTok) {
		t.Fatalf("redirect err = %v; want redirect error without token", err)
	}
}

func TestSlowReaderDoesNotHoldBuildLock(t *testing.T) {
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: testWriteTok,
		ReadToken:  testReadTok,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:     srv.URL,
		Token:       testWriteTok,
		HTTPClient:  srv.Client(),
		RetryBudget: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Create 1,000 tests with ~500-byte paths so GET /tests?limit=1000 is ~600 KB
	// and fills a small TCP receive buffer when the reader stops reading.
	pad := strings.Repeat("dir/", 110)
	tests := make([]string, 1000)
	preds := make([]*int64, 1000)
	for i := range tests {
		tests[i] = fmt.Sprintf("%st%04d.py", pad, i)
	}
	cfg := queue.Config{
		Machines:       1,
		LeaseTTLMS:     queue.DefaultLeaseTTLMS,
		BuildTimeoutMS: queue.DefaultBuildTimeoutMS,
		MaxAttempts:    3,
		MaxBatch:       1,
	}
	if _, err := c.Init(ctx, queue.InitCommand{
		BuildID:       "slow-read",
		RunnerID:      "r1",
		Config:        cfg,
		Tests:         tests,
		PredictionsMS: preds,
	}); err != nil {
		t.Fatal(err)
	}
	l, err := c.Lease(ctx, "slow-read", queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"})
	if err != nil {
		t.Fatal(err)
	}

	// Open a raw TCP connection, send GET /tests?limit=1000, and NEVER read the response.
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
	}
	defer conn.Close()

	req := fmt.Sprintf("GET /api/v1/builds/slow-read/tests?limit=1000 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n", testReadTok)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	// Give the server a moment to start writing to the stalled socket.
	time.Sleep(50 * time.Millisecond)

	// Heartbeat on the exact same build must complete well under 1 second.
	start := time.Now()
	hb, err := c.Heartbeat(ctx, "slow-read", queue.HeartbeatCommand{RunnerID: "r1", LeaseID: l.LeaseID})
	elapsed := time.Since(start)
	if err != nil || !hb.Held {
		t.Fatalf("heartbeat during stalled reader = (%+v, %v); want held=true", hb, err)
	}
	if elapsed > time.Second {
		t.Fatalf("heartbeat took %v while slow reader stalled; build lock was held across write", elapsed)
	}
}

func TestAbandonedIdleAndBrokenBuildsEvictedUnderPressure(t *testing.T) {
	var nowMS atomic.Int64
	nowMS.Store(1_000_000)

	log := &blockingLog{entered: make(chan struct{}), release: make(chan struct{})}
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken:  testWriteTok,
		ReadToken:   testReadTok,
		Clock:       func() int64 { return nowMS.Load() },
		MaxBuilds:   2,
		MaxRunning:  2,
		RetentionMS: 100_000,
		Log:         log,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:     srv.URL,
		Token:       testWriteTok,
		HTTPClient:  srv.Client(),
		RetryBudget: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
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

	// Start b1 at t=1_000_000, b2 at t=1_005_000. Both are running (MaxRunning=2).
	if err := initBuild("b1"); err != nil {
		t.Fatal(err)
	}
	nowMS.Add(5_000)
	if err := initBuild("b2"); err != nil {
		t.Fatal(err)
	}

	// At t=1_010_000 neither is idle > 2*TTL (20s) -> b3 rejected 503.
	nowMS.Add(5_000)
	var apiErr *queueapi.APIError
	if err := initBuild("b3"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 503 {
		t.Fatalf("b3 before idle threshold err = %v; want 503", err)
	}

	// Touch b2 so it stays active, then advance past b1's 2*TTL idle threshold (20s).
	if _, err := c.Lease(ctx, "b2", queue.LeaseCommand{RunnerID: "r1", RequestID: "q2"}); err != nil {
		t.Fatal(err)
	}
	nowMS.Add(15_000) // t=1_025_000: b1 idle 25s (>20s), b2 idle 15s (<=20s)

	// Under pressure, b3 evicts longest-idle abandoned b1 (404), keeping active b2!
	if err := initBuild("b3"); err != nil {
		t.Fatalf("b3 should evict abandoned b1: %v", err)
	}
	if _, err := c.Status(ctx, "b1"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
		t.Fatalf("evicted b1 err = %v; want 404", err)
	}
	if st2, err := c.Status(ctx, "b2"); err != nil || st2.Verdict != queue.VerdictRunning {
		t.Fatalf("active b2 should remain running: %+v, err=%v", st2, err)
	}

	// Advance past b2's deadline (1_005_000 + 60_000 = 1_065_000): GET /builds finalizes b2 through EventLog!
	nowMS.Store(1_070_000)
	list, err := c.ListBuilds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundB2 := false
	for _, b := range list.Builds {
		if b.BuildID == "b2" && b.Verdict == queue.VerdictUnresolved {
			foundB2 = true
		}
	}
	if !foundB2 {
		t.Fatalf("b2 past deadline not finalized in ListBuilds: %+v", list.Builds)
	}
	log.mu.Lock()
	b2Events := log.appended["b2"]
	log.mu.Unlock()
	if len(b2Events) == 0 || b2Events[len(b2Events)-1] != queue.EventFinalized {
		t.Fatalf("b2 finalized event not written through EventLog: %v", b2Events)
	}

	// Mark b3 broken via uncertain Append -> broken build does NOT block MaxRunning or MaxBuilds.
	log.failUncertain.Store(true)
	_, _ = c.Lease(ctx, "b3", queue.LeaseCommand{RunnerID: "r1", RequestID: "q3"})
	log.failUncertain.Store(false)
	if err := initBuild("b4"); err != nil {
		t.Fatalf("b4 should evict broken b3 under MaxBuilds=2: %v", err)
	}
}

func TestClientRetriesHTTPTimeoutAndRejectsMalformed200(t *testing.T) {
	// 1. Server hangs once past http.Client.Timeout, then succeeds on attempt 2.
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			time.Sleep(150 * time.Millisecond)
		}
		w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
		_, _ = w.Write([]byte(`{"version":1,"builds":[]}`))
	}))
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:      srv.URL,
		Token:        testReadTok,
		HTTPClient:   &http.Client{Timeout: 60 * time.Millisecond},
		RetryBudget:  2 * time.Second,
		RetryBase:    10 * time.Millisecond,
		RetryMaxWait: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListBuilds(context.Background()); err != nil {
		t.Fatalf("client did not retry after http.Client.Timeout: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d; want 2", calls.Load())
	}

	// 2. Proxy returning 200 {} or unknown status/verdict must always error.
	badBodies := []string{
		`{}`,
		`{"version":1,"status":"mystery"}`,
		`{"version":1,"status":"wait"}`,
		`{"version":1,"status":"done","verdict":"bogus"}`,
	}
	for _, body := range badBodies {
		badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", queueapi.ContentTypeJSON)
			_, _ = w.Write([]byte(body))
		}))
		bc, err := queueapi.NewClient(queueapi.ClientConfig{
			BaseURL:     badSrv.URL,
			Token:       testWriteTok,
			HTTPClient:  badSrv.Client(),
			RetryBudget: 10 * time.Millisecond,
		})
		if err != nil {
			badSrv.Close()
			t.Fatal(err)
		}
		if _, err := bc.Lease(context.Background(), "b1", queue.LeaseCommand{RunnerID: "r1", RequestID: "q1"}); err == nil {
			badSrv.Close()
			t.Fatalf("Lease(%s) succeeded; want error", body)
		}
		badSrv.Close()
	}
}

func TestClientDisconnectDoesNotCancelAppendContext(t *testing.T) {
	log := &blockingLog{
		blockBuild: "b-disc",
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	h, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: testWriteTok,
		ReadToken:  testReadTok,
		Log:        log,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:     srv.URL,
		Token:       testWriteTok,
		HTTPClient:  srv.Client(),
		RetryBudget: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	reqCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = c.Init(reqCtx, queue.InitCommand{
			BuildID:       "b-disc",
			RunnerID:      "r1",
			Config:        queue.Config{Machines: 1, LeaseTTLMS: queue.DefaultLeaseTTLMS, BuildTimeoutMS: queue.DefaultBuildTimeoutMS, MaxAttempts: 3, MaxBatch: 1},
			Tests:         []string{"a.py"},
			PredictionsMS: []*int64{nil},
		})
		close(done)
	}()

	<-log.entered
	cancel()
	time.Sleep(40 * time.Millisecond)
	close(log.release)
	<-done

	if log.sawCancelled.Load() {
		t.Fatal("EventLog.Append context was cancelled when client disconnected")
	}
}

func TestIsTransientErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"400", &queueapi.APIError{HTTPStatus: 400, Code: "invalid_request"}, false},
		{"401", &queueapi.APIError{HTTPStatus: 401, Code: "unauthorized"}, false},
		{"403", &queueapi.APIError{HTTPStatus: 403, Code: "forbidden"}, false},
		{"404", &queueapi.APIError{HTTPStatus: 404, Code: "not_found"}, false},
		{"409", &queueapi.APIError{HTTPStatus: 409, Code: "stale_request"}, false},
		{"413", &queueapi.APIError{HTTPStatus: 413, Code: "payload_too_large"}, false},
		{"429", &queueapi.APIError{HTTPStatus: 429, Code: "rate_limited"}, true},
		{"500", &queueapi.APIError{HTTPStatus: 500, Code: "internal_error"}, true},
		{"503", &queueapi.APIError{HTTPStatus: 503, Code: queueapi.ErrCodeAtCapacity}, true},
		{"decode/plain", errors.New("decoding queue response: unexpected EOF"), false},
	}
	for _, tc := range cases {
		if got := queueapi.IsTransientError(tc.err); got != tc.want {
			t.Errorf("IsTransientError(%s) = %t; want %t", tc.name, got, tc.want)
		}
	}

	// Verify a real network/transport failure from Client is classified as transient.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srvURL := srv.URL
	srv.Close()
	c, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:     srvURL,
		Token:       testWriteTok,
		RetryBudget: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Heartbeat(context.Background(), "b1", queue.HeartbeatCommand{RunnerID: "r1", LeaseID: 1})
	if err == nil || !queueapi.IsTransientError(err) {
		t.Fatalf("closed-server Heartbeat err = %v (transient=%t); want transient error", err, queueapi.IsTransientError(err))
	}
}
