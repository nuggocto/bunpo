package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestConsole(t *testing.T) {
	tests := []struct {
		name    string
		console func(w *bytes.Buffer) *console
		want    []string
	}{
		{
			name:    "plain",
			console: func(w *bytes.Buffer) *console { return &console{w: w} },
			want: []string{
				"bunpo: 2 of 8 machines get no tests\n",
				"bunpo: warning: tests/huge.py is dominant\n",
				"bunpo split: error: --total is required\n",
				"bunpo: assignment verified: 40 tests\n",
			},
		},

		{
			// Only the label is colored, so the message reads on any theme.
			name:    "terminal colors",
			console: func(w *bytes.Buffer) *console { return &console{w: w, colors: colors256} },
			want: []string{
				"bunpo: 2 of 8 machines get no tests\n",
				"bunpo: \x1b[1;38;5;137mwarning:\x1b[0m tests/huge.py is dominant\n",
				"bunpo split: \x1b[1;38;5;168merror:\x1b[0m --total is required\n",
				"bunpo: \x1b[1;38;5;65massignment verified:\x1b[0m 40 tests\n",
			},
		},

		{
			name:    "GitHub Actions",
			console: func(w *bytes.Buffer) *console { return &console{w: w, github: true} },
			want: []string{
				"bunpo: 2 of 8 machines get no tests\n",
				"::warning title=bunpo::tests/huge.py is dominant\n",
				"::error title=bunpo split::--total is required\n",
				"::notice title=bunpo::assignment verified: 40 tests\n",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var w bytes.Buffer
			c := tc.console(&w)

			c.info("2 of 8 machines get no tests")
			c.warn("tests/huge.py is dominant")
			c.fail("split", errors.New("--total is required"))
			c.success("assignment verified", "40 tests")

			if got := w.String(); got != strings.Join(tc.want, "") {
				t.Fatalf("output =\n%q\nwant\n%q", got, strings.Join(tc.want, ""))
			}
		})
	}
}

func TestConsoleEscapesWorkflowCommands(t *testing.T) {
	var w bytes.Buffer
	c := &console{w: &w, github: true}

	// A newline would end the command early and print the rest as plain log.
	c.fail("verify", errors.New("assignment check failed:\n  - 100% of\r tests"))

	want := "::error title=bunpo verify::assignment check failed:%0A  - 100%25 of%0D tests\n"
	if w.String() != want {
		t.Fatalf("output = %q; want %q", w.String(), want)
	}
}

func TestNewConsole(t *testing.T) {
	env := func(vars map[string]string) func(string) string {
		return func(k string) string { return vars[k] }
	}

	if c := newConsole(&bytes.Buffer{}, env(map[string]string{"GITHUB_ACTIONS": "true"})); !c.github || c.colors != (palette{}) {
		t.Fatalf("in GitHub Actions: github %t, colors %q; want workflow commands and no color", c.github, c.colors)
	}

	// Output that is not a terminal, such as a CI log file, gets no color.
	if c := newConsole(&bytes.Buffer{}, env(map[string]string{"COLORTERM": "truecolor"})); c.colors != (palette{}) {
		t.Fatalf("colors %q for output that is not a terminal; want none", c.colors)
	}
}

func TestCommandsInGitHubActions(t *testing.T) {
	github := func(k string) string {
		if k == "GITHUB_ACTIONS" {
			return "true"
		}

		return ""
	}

	dir := t.TempDir()
	var stdout, stderr bytes.Buffer

	code := run([]string{"split", "--dir", dir, "--index", "0", "--total", "3"}, strings.NewReader("tests/a.py\n"), &stdout, &stderr, github)
	if code != 0 || !strings.Contains(stderr.String(), "::warning title=bunpo::2 of 3 machines get no tests") {
		t.Fatalf("exit %d, stderr %q; want a warning annotation", code, &stderr)
	}

	stderr.Reset()
	code = run([]string{"verify", "--dir", dir}, strings.NewReader("tests/a.py\n"), &stdout, &stderr, github)
	if code != 1 || !strings.Contains(stderr.String(), "::error title=bunpo verify::assignment check failed:%0A") {
		t.Fatalf("exit %d, stderr %q; want an error annotation", code, &stderr)
	}
}
