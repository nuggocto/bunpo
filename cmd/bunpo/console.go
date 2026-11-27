package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
)

// console prints Bunpō's messages on standard error. In GitHub Actions,
// warnings, errors and the verify result become workflow commands, which
// GitHub shows on the run and the pull request. In a terminal, labels get a
// color; the message itself keeps the terminal's own color, so it reads on
// light and dark themes alike.
type console struct {
	w      io.Writer
	github bool
	colors palette // zero value: no color
}

// palette holds ANSI escape sequences for the labels.
type palette struct {
	warn, fail, ok string
}

// Soft honey, rose and mint, each at least 3:1 against white and 4.5:1
// against a dark background. The 256-color fallbacks pass the same checks;
// the cube has no mid-tone mint, so a sage green stands in.
var (
	trueColors = palette{
		warn: "\x1b[1;38;2;191;138;64m",
		fail: "\x1b[1;38;2;209;124;136m",
		ok:   "\x1b[1;38;2;67;165;129m",
	}
	colors256 = palette{
		warn: "\x1b[1;38;5;137m",
		fail: "\x1b[1;38;5;168m",
		ok:   "\x1b[1;38;5;65m",
	}
)

const reset = "\x1b[0m"

func newConsole(w io.Writer, getenv func(string) string) *console {
	c := &console{w: w, github: getenv("GITHUB_ACTIONS") == "true"}

	if !c.github && colorful(w, getenv) {
		c.colors = colors256
		if ct := getenv("COLORTERM"); ct == "truecolor" || ct == "24bit" {
			c.colors = trueColors
		}
	}

	return c
}

// colorful follows the NO_COLOR convention and colors only a terminal.
func colorful(w io.Writer, getenv func(string) string) bool {
	if getenv("NO_COLOR") != "" || getenv("TERM") == "dumb" {
		return false
	}

	f, ok := w.(*os.File)
	if !ok {
		return false
	}

	info, err := f.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice == 0 {
		return false
	}

	// The old Windows console prints escape codes as text; modern Windows
	// terminals set one of these.
	if runtime.GOOS == "windows" && getenv("WT_SESSION") == "" && getenv("TERM") == "" {
		return false
	}

	return true
}

func (c *console) info(format string, a ...any) {
	fmt.Fprintf(c.w, "bunpo: %s\n", fmt.Sprintf(format, a...))
}

func (c *console) warn(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)

	if c.github {
		fmt.Fprintf(c.w, "::warning title=bunpo::%s\n", escapeData(msg))
		return
	}

	fmt.Fprintf(c.w, "bunpo: %s %s\n", c.paint(c.colors.warn, "warning:"), msg)
}

// fail prints an error. cmd is empty for errors outside a command.
func (c *console) fail(cmd string, err error) {
	who := strings.TrimSpace("bunpo " + cmd)

	if c.github {
		fmt.Fprintf(c.w, "::error title=%s::%s\n", escapeProperty(who), escapeData(err.Error()))
		return
	}

	fmt.Fprintf(c.w, "%s: %s %v\n", who, c.paint(c.colors.fail, "error:"), err)
}

// success prints a result worth seeing at a glance, such as a verified
// assignment.
func (c *console) success(label, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)

	if c.github {
		fmt.Fprintf(c.w, "::notice title=bunpo::%s\n", escapeData(label+": "+msg))
		return
	}

	fmt.Fprintf(c.w, "bunpo: %s %s\n", c.paint(c.colors.ok, label+":"), msg)
}

func (c *console) paint(color, text string) string {
	if color == "" {
		return text
	}

	return color + text + reset
}

// escapeData and escapeProperty follow GitHub's workflow-command encoding,
// so a message with a newline or a percent sign arrives intact.
var (
	dataEscaper     = strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A")
	propertyEscaper = strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A", ":", "%3A", ",", "%2C")
)

func escapeData(s string) string     { return dataEscaper.Replace(s) }
func escapeProperty(s string) string { return propertyEscaper.Replace(s) }
