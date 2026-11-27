package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestRunConfigAndPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	notADir := filepath.Join(t.TempDir(), "file-not-dir")
	if err := os.WriteFile(notADir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	noEnv := func(string) string { return "" }

	validEnv := func(k string) string {
		switch k {
		case "BUNPO_QUEUE_TOKEN":
			return "write-secret-token-0123456789abcdef"
		case "BUNPO_QUEUE_READ_TOKEN":
			return "read-secret-token-fedcba9876543210"
		default:
			return ""
		}
	}

	cases := []struct {
		name     string
		args     []string
		env      func(string) string
		wantCode int
	}{
		{"missing tokens", nil, noEnv, 2},
		{"unexpected positional argument", []string{"extra"}, noEnv, 2},
		{"help flag", []string{"-h"}, noEnv, 0},
		{"unknown flag", []string{"--nope"}, validEnv, 2},
		{"port already in use", []string{"--addr", ln.Addr().String()}, validEnv, 1},
		{"--data pointing at a file", []string{"--addr", "127.0.0.1:0", "--data", notADir}, validEnv, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			if code := run(tc.args, tc.env, &stderr); code != tc.wantCode {
				t.Fatalf("run(%v) = %d; want %d (stderr=%s)", tc.args, code, tc.wantCode, stderr.String())
			}
		})
	}
}

func TestServerDocFlagsMatchHelp(t *testing.T) {
	var stderr bytes.Buffer
	if code := run([]string{"-h"}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run(-h) = %d; want 0", code)
	}

	helpFlagRe := regexp.MustCompile(`(?m)^\s*-([a-z][a-z0-9-]*)\b`)
	var helpFlags []string
	for _, m := range helpFlagRe.FindAllStringSubmatch(stderr.String(), -1) {
		helpFlags = append(helpFlags, m[1])
	}

	slices.Sort(helpFlags)
	helpFlags = slices.Compact(helpFlags)

	docBytes, err := os.ReadFile(filepath.Join("..", "..", "docs", "cli.md"))
	if err != nil {
		t.Fatal(err)
	}

	doc := string(docBytes)
	const heading = "### `bunpo-server`"
	idx := strings.Index(doc, heading+"\n")
	if idx < 0 {
		t.Fatalf("docs/cli.md missing %q", heading)
	}

	sec := doc[idx+len(heading)+1:]
	if next := strings.Index(sec, "\n##"); next >= 0 {
		sec = sec[:next]
	}

	docFlagRe := regexp.MustCompile("(?m)^\\|\\s*`--([a-z][a-z0-9-]*)`\\s*\\|")
	var docFlags []string
	for _, m := range docFlagRe.FindAllStringSubmatch(sec, -1) {
		docFlags = append(docFlags, m[1])
	}

	slices.Sort(docFlags)
	docFlags = slices.Compact(docFlags)

	if !slices.Equal(helpFlags, docFlags) {
		t.Fatalf("bunpo-server flag mismatch:\n  real (-h): %v\n  docs/cli.md: %v", helpFlags, docFlags)
	}
}
