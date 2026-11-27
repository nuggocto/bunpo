package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/nuggocto/bunpo/internal/split"
	"github.com/nuggocto/bunpo/internal/timing"
)

const (
	defaultDir    = ".bunpo"
	timingsName   = "timings.json"
	planName      = "plan.json"
	maxListBytes  = 64 << 20
	maxAllReports = 1 << 30 // across all reports given to one record call
)

func pathStyle() split.PathStyle {
	if runtime.GOOS == "windows" {
		return split.Windows
	}

	return split.POSIX
}

// openDir opens dir as an os.Root, so no name inside it can escape through
// ".." or a symlink.
func openDir(dir string, create bool) (*os.Root, error) {
	if create {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	root, err := os.OpenRoot(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("directory %s does not exist", dir)
	}

	return root, err
}

// readFile reads name from root. A missing file returns false and no error.
func readFile(root *os.Root, name string, max int) ([]byte, bool, error) {
	f, err := root.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	data, err := readBounded(f, max)
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w", name, err)
	}

	return data, true, nil
}

func readBounded(r io.Reader, max int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}

	if len(data) > max {
		return nil, fmt.Errorf("larger than %d bytes", max)
	}

	return data, nil
}

// writeFile writes to a temporary file and renames it over name, so a
// failed write leaves the old file intact.
func writeFile(root *os.Root, name string, data []byte) (err error) {
	tmp := fmt.Sprintf(".%s.%d.tmp", name, rand.Uint64())

	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			root.Remove(tmp)
		}
	}()

	if _, err = f.Write(data); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return err
	}

	if err = f.Close(); err != nil {
		return err
	}

	return root.Rename(tmp, name)
}

// removeFile deletes name if it exists.
func removeFile(root *os.Root, name string) error {
	err := root.Remove(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	return err
}

// readTimings returns an empty timing file when there is none yet.
func readTimings(root *os.Root) (timing.File, bool, error) {
	data, ok, err := readFile(root, timingsName, timing.MaxFileBytes)
	if err != nil || !ok {
		return timing.File{Tests: map[string]timing.Entry{}}, false, err
	}

	f, err := timing.Decode(data)
	if err != nil {
		return timing.File{}, false, fmt.Errorf("%s: %w", timingsName, err)
	}

	return f, true, nil
}

// readInventory reads the test list from standard input. An empty list is
// an error: a discovery command that found nothing would otherwise pass with
// no tests run.
func readInventory(r io.Reader) (split.Inventory, error) {
	raw, err := readList(r)
	if err != nil {
		return split.Inventory{}, err
	}

	inv, err := split.NewInventory(raw, pathStyle())
	if err != nil {
		return split.Inventory{}, err
	}

	if inv.Len() == 0 {
		return split.Inventory{}, errors.New("the test list on standard input is empty; check the command that lists your test files")
	}

	return inv, nil
}

// readList reads one identifier per line, LF or CRLF, skipping blank lines.
func readList(r io.Reader) ([]string, error) {
	data, err := readBounded(r, maxListBytes)
	if err != nil {
		return nil, fmt.Errorf("test list: %w", err)
	}

	var ids []string
	for line := range strings.Lines(string(data)) {
		// A carriage return is stripped only as part of CRLF; anywhere else
		// it stays, and the identifier rules reject it.
		if trimmed, ok := strings.CutSuffix(line, "\n"); ok {
			line = strings.TrimSuffix(trimmed, "\r")
		}

		if line != "" {
			ids = append(ids, line)
		}
	}

	return ids, nil
}

// indexedFiles returns the indexes of files named <prefix><index>.json, in
// order. Other names, and indexes with leading zeros, are ignored.
func indexedFiles(root *os.Root, prefix string) ([]int, error) {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return nil, err
	}

	var indexes []int
	for _, e := range entries {
		rest, ok := strings.CutPrefix(e.Name(), prefix)
		if !ok || !e.Type().IsRegular() {
			continue
		}

		digits, ok := strings.CutSuffix(rest, ".json")
		n, err := strconv.Atoi(digits)
		if !ok || err != nil || n < 0 || strconv.Itoa(n) != digits {
			continue
		}

		indexes = append(indexes, n)
	}

	slices.Sort(indexes)
	return indexes, nil
}

func manifestName(index int) string { return fmt.Sprintf("manifest-%d.json", index) }
func reportName(index int) string   { return fmt.Sprintf("report-%d.json", index) }

// formatMS prints "850ms", "4m32s", or "1h2m3s" using integer milliseconds,
// avoiding time.Duration's 292-year nanosecond overflow on extreme totals.
func formatMS(ms int64) string {
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}

	sec := (ms + 500) / 1000
	if sec < 60 {
		return fmt.Sprintf("%ds", sec)
	}

	min := sec / 60
	remSec := sec % 60
	if min < 60 {
		return fmt.Sprintf("%dm%ds", min, remSec)
	}

	return fmt.Sprintf("%dh%dm%ds", min/60, min%60, remSec)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}

	return many
}

// sample lists the first n items and counts the rest.
func sample(items []string, n int) string {
	shown := strings.Join(items[:min(n, len(items))], ", ")
	if len(items) > n {
		shown += fmt.Sprintf(", and %d more", len(items)-n)
	}

	return shown
}
