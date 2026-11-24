package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// TestCLIDocFlagsMatchHelp ensures docs/cli.md documents every real bunpo flag
// and lists no stale flags, preventing documentation drift.
func TestCLIDocFlagsMatchHelp(t *testing.T) {
	docBytes, err := os.ReadFile(filepath.Join("..", "..", "docs", "cli.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(docBytes)

	helpFlagRe := regexp.MustCompile(`(?m)^\s*-([a-z][a-z0-9-]*)\b`)
	docFlagRe := regexp.MustCompile("(?m)^\\|\\s*`--([a-z][a-z0-9-]*)`\\s*\\|")

	cases := []struct {
		heading string
		args    []string
	}{
		{"### `bunpo split`", []string{"split", "-h"}},
		{"### `bunpo plan`", []string{"plan", "-h"}},
		{"### `bunpo record`", []string{"record", "-h"}},
		{"### `bunpo merge`", []string{"merge", "-h"}},
		{"### `bunpo verify`", []string{"verify", "-h"}},
		{"### `bunpo --queue`", []string{"--queue", "-h"}},
	}

	// Ensure every entry in the commands map is covered.
	covered := map[string]bool{}
	for _, tc := range cases {
		covered[tc.args[0]] = true
	}
	for name := range commands {
		if !covered[name] {
			t.Fatalf("command %q is registered in commands map but missing from TestCLIDocFlagsMatchHelp", name)
		}
	}

	for _, tc := range cases {
		t.Run(tc.heading, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, strings.NewReader(""), &stdout, &stderr, func(string) string { return "" })
			if code != 0 {
				t.Fatalf("run(%v) = %d; want 0 (stderr=%s)", tc.args, code, stderr.String())
			}

			var helpFlags []string
			for _, m := range helpFlagRe.FindAllStringSubmatch(stderr.String(), -1) {
				helpFlags = append(helpFlags, m[1])
			}
			slices.Sort(helpFlags)
			helpFlags = slices.Compact(helpFlags)

			sec := markdownSection(t, doc, tc.heading)
			var docFlags []string
			for _, m := range docFlagRe.FindAllStringSubmatch(sec, -1) {
				docFlags = append(docFlags, m[1])
			}
			slices.Sort(docFlags)
			docFlags = slices.Compact(docFlags)

			if !slices.Equal(helpFlags, docFlags) {
				t.Fatalf("%s flag mismatch:\n  real (-h): %v\n  docs/cli.md: %v", tc.heading, helpFlags, docFlags)
			}
		})
	}
}

func markdownSection(t *testing.T, doc, heading string) string {
	t.Helper()
	idx := strings.Index(doc, heading+"\n")
	if idx < 0 {
		t.Fatalf("docs/cli.md missing heading %q", heading)
	}
	rest := doc[idx+len(heading)+1:]
	if next := strings.Index(rest, "\n##"); next >= 0 {
		return rest[:next]
	}
	return rest
}
