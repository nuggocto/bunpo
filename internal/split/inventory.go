package split

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// MaxTests bounds the distinct tests in one split.
const MaxTests = 200_000

var (
	ErrInvalidID    = errors.New("invalid test identifier")
	ErrCollision    = errors.New("test identifiers collide")
	ErrTooManyTests = errors.New("too many tests")
	ErrPathStyle    = errors.New("unknown path style")
)

// PathStyle says how to read separators in raw identifiers.
type PathStyle int

const (
	// POSIX rejects backslashes: they are legal in POSIX file names, so
	// reading one as a separator could merge two files.
	POSIX PathStyle = iota + 1

	// Windows turns every backslash into a slash.
	Windows
)

// Inventory is a canonical list of test identifiers: repository-relative,
// slash-separated, unique and sorted by bytes. Only NewInventory builds one.
type Inventory struct {
	ids []string
}

// NewInventory canonicalizes raw identifiers. Two spellings of one file fail,
// naming both; exact repeats are dropped.
func NewInventory(raw []string, style PathStyle) (Inventory, error) {
	if style != POSIX && style != Windows {
		return Inventory{}, fmt.Errorf("%w %d", ErrPathStyle, style)
	}

	// Repeated lines can make raw much longer than the tests it names.
	size := min(len(raw), MaxTests)
	spelling := make(map[string]string, size) // identifier → first spelling
	ids := make([]string, 0, size)

	for _, original := range raw {
		id, err := canonicalize(original, style)
		if err != nil {
			return Inventory{}, err
		}

		if first, seen := spelling[id]; seen {
			if first == original {
				continue
			}

			return Inventory{}, fmt.Errorf("%w: %q and %q both name %q", ErrCollision, first, original, id)
		}

		if len(ids) == MaxTests {
			return Inventory{}, fmt.Errorf("%w: more than %d distinct tests", ErrTooManyTests, MaxTests)
		}

		spelling[id] = original
		ids = append(ids, id)
	}

	// Byte order is the same on every platform and in every locale.
	slices.Sort(ids)
	return Inventory{ids: ids}, nil
}

// Canonical applies NewInventory's rules to one identifier, so paths read
// from test reports match the inventory.
func Canonical(raw string, style PathStyle) (string, error) {
	if style != POSIX && style != Windows {
		return "", fmt.Errorf("%w %d", ErrPathStyle, style)
	}

	return canonicalize(raw, style)
}

// IDs returns a copy of the identifiers in byte order.
func (inv Inventory) IDs() []string {
	return slices.Clone(inv.ids)
}

func (inv Inventory) Len() int {
	return len(inv.ids)
}

func invalidID(original, reason string) error {
	return fmt.Errorf("%w %q: %s", ErrInvalidID, original, reason)
}

func canonicalize(original string, style PathStyle) (string, error) {
	switch {
	case original == "":
		return "", invalidID(original, "empty")

	// The timing file is JSON, which cannot hold invalid UTF-8.
	case !utf8.ValidString(original):
		return "", invalidID(original, "not valid UTF-8")

	// The digest and the input format use these as separators.
	case strings.ContainsAny(original, "\x00\r\n"):
		return "", invalidID(original, "contains a NUL, carriage return or newline")

	case style == POSIX && strings.Contains(original, `\`):
		return "", invalidID(original, `contains a backslash; POSIX input must separate with "/"`)
	}

	p := original
	if style == Windows {
		p = strings.ReplaceAll(p, `\`, "/")
	}

	if strings.HasPrefix(p, "/") {
		return "", invalidID(original, "is absolute; use a repository-relative path")
	}

	// Most inputs are already canonical, so validate components first without
	// allocating a slice or rebuilding the string.
	needsClean := false
	parts := 0
	for part := range strings.SplitSeq(p, "/") {
		switch part {
		case "", ".":
			needsClean = true
			continue

		// Resolving ".." lexically can disagree with the filesystem through
		// a symlink.
		case "..":
			return "", invalidID(original, `contains a ".." component`)
		}

		// Rejected in both styles, so a mixed fleet of Linux and Windows machines
		// agrees on every list. Checked after skipping "." and empty components:
		// "./C:/a.py" only shows its drive once the "." is gone.
		if parts == 0 && hasDrive(part) {
			return "", invalidID(original, "is an absolute Windows path; use a repository-relative path")
		}

		parts++
	}

	if parts == 0 {
		return "", invalidID(original, "names no file")
	}

	if !needsClean {
		return p, nil
	}

	var b strings.Builder
	b.Grow(len(p))

	for part := range strings.SplitSeq(p, "/") {
		if part == "" || part == "." {
			continue
		}

		if b.Len() > 0 {
			b.WriteByte('/')
		}

		b.WriteString(part)
	}

	return b.String(), nil
}

func hasDrive(component string) bool {
	if len(component) < 2 || component[1] != ':' {
		return false
	}

	c := component[0]
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}
