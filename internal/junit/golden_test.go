package junit_test

import (
	"bytes"
	"encoding/json/v2"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/nuggocto/bunpo/internal/junit"
)

var update = flag.Bool("update", false, "rewrite the expected results of the golden tests")

// TestGolden reads reports from the real runners (testdata/generate.sh).
// Each sample suite has a fast, a slow, a failing and a skipping file.
func TestGolden(t *testing.T) {
	tests := []struct {
		runner                   string
		src                      junit.Source
		fast, slow, fails, skips string
	}{
		{"pytest", junit.FileAttribute, "tests/test_fast.py", "tests/test_slow.py", "tests/sub/test_fails.py", "tests/sub/test_skips.py"},
		{"jest", junit.FileAttribute, "tests/fast.test.js", "tests/slow.test.js", "tests/sub/fails.test.js", "tests/sub/skips.test.js"},
		{"vitest", junit.FileAttribute, "tests/fast.test.js", "tests/slow.test.js", "tests/sub/fails.test.js", "tests/sub/skips.test.js"},
		// Relative to Playwright's testDir, "tests".
		{"playwright", junit.Classname, "fast.spec.js", "slow.spec.js", "sub/fails.spec.js", "sub/skips.spec.js"},
		{"rspec", junit.FileAttribute, "./spec/fast_spec.rb", "./spec/slow_spec.rb", "./spec/sub/fails_spec.rb", "./spec/sub/skips_spec.rb"},
	}

	for _, tc := range tests {
		t.Run(tc.runner, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", tc.runner, "report.xml"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			got, err := junit.Read(f, tc.src)
			if err != nil {
				t.Fatal(err)
			}

			// These hold whatever the timings of a regeneration.
			if len(got.Files) != 4 {
				t.Fatalf("files = %v; want the 4 sample files", got.Files)
			}

			want := map[string]struct {
				cases   int
				skipped int
				failed  bool
			}{tc.fast: {2, 0, false}, tc.slow: {1, 0, false}, tc.fails: {2, 0, true}, tc.skips: {2, 1, false}}
			for path, w := range want {
				g, ok := got.Files[path]
				if !ok || g.Cases != w.cases || g.Skipped != w.skipped || g.Failed != w.failed {
					t.Fatalf("%s = %+v, %t; want %d cases, %d skipped, failed %t", path, g, ok, w.cases, w.skipped, w.failed)
				}
			}

			if ms := got.Files[tc.slow].MS(); ms < 150 {
				t.Fatalf("%s took %d ms; the sample sleeps 150 ms", tc.slow, ms)
			}

			compareGolden(t, filepath.Join("testdata", tc.runner, "expected.json"), got)
		})
	}
}

func compareGolden(t *testing.T, path string, got junit.Report) {
	t.Helper()

	type goldenFile struct {
		MS      int64 `json:"ms"`
		Cases   int   `json:"cases"`
		Skipped int   `json:"skipped"`
		Failed  bool  `json:"failed"`
	}

	view := make(map[string]goldenFile, len(got.Files))
	for path, f := range got.Files {
		view[path] = goldenFile{f.MS(), f.Cases, f.Skipped, f.Failed}
	}

	data, err := json.Marshal(view, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}

	data = append(data, '\n')
	if *update {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}

		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test -update after testdata/generate.sh)", err)
	}

	if !bytes.Equal(data, want) {
		t.Fatalf("totals differ from %s:\ngot  %s\nwant %s", path, data, want)
	}
}
