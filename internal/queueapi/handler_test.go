package queueapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
	blockBuild string
	entered    chan struct{}
	release    chan struct{}
	failAll    atomic.Bool
	corrupt    atomic.Bool
}

func (l *blockingLog) Append(_ context.Context, buildID string, events []queue.Event) error {
	if l.failAll.Load() {
		return errors.New("disk full")
	}
	if buildID == l.blockBuild {
		close(l.entered)
		<-l.release
	}
	if l.corrupt.Swap(false) && len(events) > 0 {
		// Deliberately mutate the event slice to simulate an Apply failure after
		// Append succeeded, verifying the handler marks the build broken (500).
		events[0].Type = "corrupt_event_type"
	}
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

	// Failed Append on brand-new build -> 503 storage_error, and slot is cleaned up.
	log.failAll.Store(true)
	_, err = c.Init(ctx, queue.InitCommand{
		BuildID:       "build-fail-init",
		RunnerID:      "r3",
		Config:        cfg,
		Tests:         []string{"a.py"},
		PredictionsMS: []*int64{nil},
	})
	log.failAll.Store(false)
	var apiErr *queueapi.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 503 || apiErr.Code != queueapi.ErrCodeStorageError {
		t.Fatalf("failed init err = %v; want 503 storage_error", err)
	}
	if _, err := c.Status(ctx, "build-fail-init"); !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
		t.Fatalf("failed init leaked build entry: %v", err)
	}

	// Failed Append on lease leaves memory unchanged (test still pending).
	log.failAll.Store(true)
	_, err = c.Lease(ctx, "build-fast", queue.LeaseCommand{RunnerID: "r2", RequestID: "q1"})
	log.failAll.Store(false)
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 503 || apiErr.Code != queueapi.ErrCodeStorageError {
		t.Fatalf("failed lease err = %v; want 503 storage_error", err)
	}
	st, err := c.Status(ctx, "build-fast")
	if err != nil || st.Counts.Pending != 1 || st.Counts.Leased != 0 {
		t.Fatalf("state changed after failed Append: %+v, err=%v", st, err)
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
