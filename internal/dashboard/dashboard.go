// Package dashboard embeds the shared static live-queue web UI and wraps an
// API http.Handler so bunpo-server serves both from one listener.
package dashboard

import (
	"embed"
	"net/http"
	"strings"

	"github.com/nuggocto/bunpo/internal/queue"
)

const (
	CSP            = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
	NoSniff        = "nosniff"
	ReferrerPolicy = "no-referrer"
	CacheHTML      = "no-cache"
	CacheAsset     = "public, max-age=300"
)

//go:embed static/index.html static/dashboard/app.js static/dashboard/core.js static/dashboard/style.css static/dashboard/favicon.svg
var staticFS embed.FS

type assetSpec struct {
	embedPath   string
	contentType string
	cache       string
}

var assets = map[string]assetSpec{
	"/dashboard/app.js":      {embedPath: "static/dashboard/app.js", contentType: "text/javascript; charset=utf-8", cache: CacheAsset},
	"/dashboard/core.js":     {embedPath: "static/dashboard/core.js", contentType: "text/javascript; charset=utf-8", cache: CacheAsset},
	"/dashboard/style.css":   {embedPath: "static/dashboard/style.css", contentType: "text/css; charset=utf-8", cache: CacheAsset},
	"/dashboard/favicon.svg": {embedPath: "static/dashboard/favicon.svg", contentType: "image/svg+xml", cache: CacheAsset},
}

var indexSpec = assetSpec{
	embedPath:   "static/index.html",
	contentType: "text/html; charset=utf-8",
	cache:       CacheHTML,
}

// Wrap returns an http.Handler that serves the embedded dashboard at / ,
// /builds/<valid-build-id>, and /dashboard/<asset>, forwarding /api/v1/*,
// /healthz, and all unknown paths to api unchanged.
func Wrap(api http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasPrefix(path, "/api/v1/") || path == "/healthz" {
			api.ServeHTTP(w, r)
			return
		}

		// Reject raw encoded traversal or slashes before matching allowlisted paths.
		if r.URL.RawPath != "" && r.URL.RawPath != path {
			api.ServeHTTP(w, r)
			return
		}

		spec, ok := matchRoute(path)
		if !ok {
			api.ServeHTTP(w, r)
			return
		}

		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("X-Content-Type-Options", NoSniff)
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte(`{"version":1,"error":{"code":"method_not_allowed","message":"method not allowed"}}` + "\n"))
			return
		}

		data, err := staticFS.ReadFile(spec.embedPath)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Security-Policy", CSP)
		w.Header().Set("X-Content-Type-Options", NoSniff)
		w.Header().Set("Referrer-Policy", ReferrerPolicy)
		w.Header().Set("Cache-Control", spec.cache)
		w.Header().Set("Content-Type", spec.contentType)
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	})
}

func matchRoute(path string) (assetSpec, bool) {
	if path == "/" {
		return indexSpec, true
	}
	if spec, ok := assets[path]; ok {
		return spec, true
	}
	if buildID, ok := strings.CutPrefix(path, "/builds/"); ok {
		if !strings.Contains(buildID, "/") && queue.ValidBuildID(buildID) {
			return indexSpec, true
		}
	}
	return assetSpec{}, false
}
