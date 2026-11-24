package dashboard_test

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/dashboard"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
)

const (
	testWriteTok = "dashboard-write-token-0123456789abcdef"
	testReadTok  = "dashboard-read-token-fedcba9876543210"
)

func newWrappedHandler(t *testing.T) http.Handler {
	t.Helper()
	api, err := queueapi.NewHandler(queueapi.HandlerConfig{
		WriteToken: testWriteTok,
		ReadToken:  testReadTok,
		Limits:     queue.DefaultLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return dashboard.Wrap(api)
}

func TestDashboardServesFilesAndHeaders(t *testing.T) {
	h := newWrappedHandler(t)

	cases := []struct {
		path      string
		wantCT    string
		wantCache string
		wantSub   string
	}{
		{"/", "text/html; charset=utf-8", dashboard.CacheHTML, `<script type="module" src="/dashboard/app.js"></script>`},
		{"/builds/release-1.2.3", "text/html; charset=utf-8", dashboard.CacheHTML, `<title>Bunpō Queue</title>`},
		{"/builds/app.js", "text/html; charset=utf-8", dashboard.CacheHTML, `<title>Bunpō Queue</title>`},
		{"/dashboard/app.js", "text/javascript; charset=utf-8", dashboard.CacheAsset, `Authorization`},
		{"/dashboard/core.js", "text/javascript; charset=utf-8", dashboard.CacheAsset, `export function pollStep`},
		{"/dashboard/style.css", "text/css; charset=utf-8", dashboard.CacheAsset, `prefers-color-scheme: dark`},
		{"/dashboard/favicon.svg", "image/svg+xml", dashboard.CacheAsset, `<svg `},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("GET %s = %d; want 200", tc.path, rec.Code)
			}
			if got := rec.Header().Get("Content-Security-Policy"); got != dashboard.CSP {
				t.Fatalf("CSP = %q; want %q", got, dashboard.CSP)
			}
			if got := rec.Header().Get("X-Content-Type-Options"); got != dashboard.NoSniff {
				t.Fatalf("nosniff = %q", got)
			}
			if got := rec.Header().Get("Referrer-Policy"); got != dashboard.ReferrerPolicy {
				t.Fatalf("Referrer-Policy = %q", got)
			}
			if got := rec.Header().Get("Cache-Control"); got != tc.wantCache {
				t.Fatalf("Cache-Control = %q; want %q", got, tc.wantCache)
			}
			if got := rec.Header().Get("Content-Type"); got != tc.wantCT {
				t.Fatalf("Content-Type = %q; want %q", got, tc.wantCT)
			}
			if !strings.Contains(rec.Body.String(), tc.wantSub) {
				t.Fatalf("body missing %q", tc.wantSub)
			}
		})
	}

	// HEAD returns headers and empty body; POST returns 405.
	headRec := httptest.NewRecorder()
	h.ServeHTTP(headRec, httptest.NewRequest(http.MethodHead, "/", nil))
	if headRec.Code != http.StatusOK || headRec.Body.Len() != 0 {
		t.Fatalf("HEAD / = %d len=%d; want 200 len=0", headRec.Code, headRec.Body.Len())
	}

	postRec := httptest.NewRecorder()
	h.ServeHTTP(postRec, httptest.NewRequest(http.MethodPost, "/", nil))
	if postRec.Code != http.StatusMethodNotAllowed || postRec.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST / = %d Allow=%q", postRec.Code, postRec.Header().Get("Allow"))
	}
}

func TestDashboardRejectsTraversalDirectoryListingAndUnknownPaths(t *testing.T) {
	h := newWrappedHandler(t)

	notFoundPaths := []string{
		"/dashboard",
		"/dashboard/",
		"/dashboard/nope.js",
		"/dashboard/../index.html",
		"/dashboard/%2e%2e/index.html",
		"/_headers",
		"/builds",
		"/builds/",
		"/builds/..",
		"/builds/a..b",
		"/builds/a/b",
		"/unknown",
	}

	for _, p := range notFoundPaths {
		t.Run(p, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+p, nil)
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("GET %s = %d; want 404 (body=%s)", p, rec.Code, rec.Body.String())
			}
			var env struct {
				Version int `json:"version"`
				Error   struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatalf("GET %s body is not JSON 404: %v (%q)", p, err, rec.Body.String())
			}
			if env.Version != 1 || env.Error.Code != "not_found" {
				t.Fatalf("GET %s JSON = %+v; want version=1 code=not_found", p, env)
			}
		})
	}
}

func TestDashboardPreservesAPIAndHealthzAndHeadersParity(t *testing.T) {
	h := newWrappedHandler(t)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"ok":true}` {
		t.Fatalf("GET /healthz = %d %q", rec.Code, rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/builds", nil)
	req.Header.Set("Authorization", "Bearer "+testReadTok)
	apiRec := httptest.NewRecorder()
	h.ServeHTTP(apiRec, req)
	if apiRec.Code != http.StatusOK || !strings.Contains(apiRec.Body.String(), `"builds":[]`) {
		t.Fatalf("GET /api/v1/builds = %d %q", apiRec.Code, apiRec.Body.String())
	}

	// Verify static/_headers (used by Cloudflare Workers Static Assets) matches Go constants.
	rawHeaders, err := os.ReadFile("static/_headers")
	if err != nil {
		t.Fatal(err)
	}
	text := string(rawHeaders)
	for _, want := range []string{
		"Content-Security-Policy: " + dashboard.CSP,
		"X-Content-Type-Options: " + dashboard.NoSniff,
		"Referrer-Policy: " + dashboard.ReferrerPolicy,
		"Cache-Control: " + dashboard.CacheHTML,
		"Cache-Control: " + dashboard.CacheAsset,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("static/_headers missing %q", want)
		}
	}
}
