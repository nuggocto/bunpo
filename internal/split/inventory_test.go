package split_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nuggocto/bunpo/internal/split"
	"github.com/nuggocto/bunpo/internal/timing"
)

func TestNewInventory(t *testing.T) {
	tests := []struct {
		name  string
		style split.PathStyle
		raw   []string
		want  []string
	}{
		{
			name:  "sorts by bytes, not locale",
			style: split.POSIX,
			raw:   []string{"b.py", "a.py", "B.py", "é.py", "z.py"},
			want:  []string{"B.py", "a.py", "b.py", "z.py", "é.py"},
		},

		{
			name:  "removes exact repeats",
			style: split.POSIX,
			raw:   []string{"a.py", "b.py", "a.py"},
			want:  []string{"a.py", "b.py"},
		},

		{
			name:  "removes . and empty components",
			style: split.POSIX,
			raw:   []string{"./tests//a.py"},
			want:  []string{"tests/a.py"},
		},

		{
			name:  "keeps spaces and case",
			style: split.POSIX,
			raw:   []string{"Tests/my test.py", "tests/my test.py"},
			want:  []string{"Tests/my test.py", "tests/my test.py"},
		},

		{
			name:  "converts Windows separators",
			style: split.Windows,
			raw:   []string{`tests\unit\a.py`, "tests/b.py"},
			want:  []string{"tests/b.py", "tests/unit/a.py"},
		},

		{
			name:  "empty list",
			style: split.POSIX,
			want:  []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv, err := split.NewInventory(tc.raw, tc.style)
			if err != nil {
				t.Fatal(err)
			}

			if got := inv.IDs(); !slices.Equal(got, tc.want) {
				t.Fatalf("IDs() = %q; want %q", got, tc.want)
			}
		})
	}
}

func TestNewInventoryRejects(t *testing.T) {
	tests := []struct {
		name    string
		style   split.PathStyle
		raw     []string
		wantErr error
		wantMsg string
	}{
		// Reading the backslash as a separator would merge these two files
		// and let a one-file split verify.
		{"POSIX backslash", split.POSIX, []string{`tests/a\b`, "tests/a/b"}, split.ErrInvalidID, `tests/a\\b`},

		{"Windows spellings of one file", split.Windows, []string{`tests\a.py`, "x.py", "tests/a.py"}, split.ErrCollision, `"tests\\a.py" and "tests/a.py" both name "tests/a.py"`},
		{"dot spelling of one file", split.POSIX, []string{"a.py", "./a.py"}, split.ErrCollision, `"a.py" and "./a.py" both name "a.py"`},

		{"empty identifier", split.POSIX, []string{""}, split.ErrInvalidID, "empty"},
		{"only separators", split.POSIX, []string{"./"}, split.ErrInvalidID, "names no file"},
		{"parent component", split.POSIX, []string{"tests/../a.py"}, split.ErrInvalidID, ".."},

		{"absolute POSIX path", split.POSIX, []string{"/repo/a.py"}, split.ErrInvalidID, "absolute"},
		{"absolute Windows path", split.Windows, []string{`C:\repo\a.py`}, split.ErrInvalidID, "absolute"},
		{"UNC path", split.Windows, []string{`\\server\share\a.py`}, split.ErrInvalidID, "absolute"},
		{"drive behind ./", split.Windows, []string{"./C:/suite/a.py"}, split.ErrInvalidID, "absolute"},
		{"drive behind .\\", split.Windows, []string{`.\C:\suite\a.py`}, split.ErrInvalidID, "absolute"},
		{"drive-relative path", split.Windows, []string{".//C:a.py"}, split.ErrInvalidID, "absolute"},
		// Rejected in POSIX style too, so Linux and Windows machines agree.
		{"drive in a POSIX list", split.POSIX, []string{"./C:/a.py"}, split.ErrInvalidID, "absolute"},

		{"NUL byte", split.POSIX, []string{"a\x00.py"}, split.ErrInvalidID, ""},
		{"newline", split.POSIX, []string{"a\n.py"}, split.ErrInvalidID, ""},
		{"carriage return", split.POSIX, []string{"a.py\r"}, split.ErrInvalidID, ""},
		{"invalid UTF-8", split.POSIX, []string{"a\xff.py"}, split.ErrInvalidID, "UTF-8"},

		{"unknown path style", split.PathStyle(0), []string{"a.py"}, split.ErrPathStyle, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := split.NewInventory(tc.raw, tc.style)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v; want %v", err, tc.wantErr)
			}

			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}
}

func TestNewInventoryLimit(t *testing.T) {
	raw := make([]string, split.MaxTests+1)
	for i := range raw {
		raw[i] = fmt.Sprintf("t%d.py", i)
	}

	if _, err := split.NewInventory(raw[:split.MaxTests], split.POSIX); err != nil {
		t.Fatalf("at the limit: %v", err)
	}

	if _, err := split.NewInventory(raw, split.POSIX); !errors.Is(err, split.ErrTooManyTests) {
		t.Fatalf("over the limit: error = %v; want %v", err, split.ErrTooManyTests)
	}
}

func TestTimingIDRulesMatchCanonical(t *testing.T) {
	// timing.validID is duplicated from split.canonicalize to keep imports one-way.
	// Verify both packages accept and reject the exact same canonical POSIX IDs.
	cases := []string{
		"a.py",
		"tests/sub/a_test.py",
		"Tests/my test.py",
		"é.py",
		"a:b.py",
		"foo/C:/bar.py",
		"a..b/c.py",
		".hidden/test.py",
		"",
		".",
		"..",
		"./a.py",
		"tests//a.py",
		"tests/a.py/",
		"tests/./a.py",
		"tests/../a.py",
		"/a.py",
		"C:/a.py",
		"c:a.py",
		"./C:/a.py",
		`tests\a.py`,
		"a\x00.py",
		"a\n.py",
		"a\r.py",
	}

	for _, id := range cases {
		c, err := split.Canonical(id, split.POSIX)
		wantOK := err == nil && c == id

		_, timingErr := timing.Encode(timing.File{
			Run:   1,
			Tests: map[string]timing.Entry{id: {Samples: []int64{10}, LastSeen: 1}},
		})
		gotOK := timingErr == nil

		if gotOK != wantOK {
			t.Errorf("id %q: timing accepted=%t (err=%v), split canonical=%t (c=%q, err=%v)",
				id, gotOK, timingErr, wantOK, c, err)
		}
	}
}
