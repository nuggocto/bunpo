package main

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/split"
	"github.com/nuggocto/bunpo/internal/timing"
)

const noFileHint = `turn on file paths in your runner's JUnit report:
  pytest:     -o junit_family=xunit1
  Jest:       jest-junit with addFileAttribute: "true"
  Vitest:     junit reporter with addFileAttribute: true
  Playwright: run bunpo record with --file-from classname --path-prefix <testDir>`

var fileSources = map[string]junit.Source{
	"file":      junit.FileAttribute,
	"classname": junit.Classname,
	"suite":     junit.SuiteName,
}

type reportOptions struct {
	format string
	src    junit.Source
	prefix string
}

func cmdRecord(args []string, _ io.Reader, _ io.Writer, con *console) error {
	fs := newFlags("record", "record --index N [--dir DIR] [--format junit|lines] [--file-from file|classname|suite] [--path-prefix DIR] [REPORT...]", con)
	dir := fs.String("dir", defaultDir, "directory for Bunpō's files")
	index := fs.Int("index", -1, "this machine's index, as given to split (required)")
	format := fs.String("format", "junit", `"junit" for JUnit XML, or "lines" for a path and milliseconds per line`)
	fileFrom := fs.String("file-from", "file", `where JUnit reports keep each test's file: "file", "classname" or "suite"`)
	prefix := fs.String("path-prefix", "", "directory to put before every path in the reports")

	if err := fs.parse(args); err != nil {
		return err
	}

	src, ok := fileSources[*fileFrom]

	switch {
	case *index < 0:
		return fs.fail("--index is required")

	case *format != "junit" && *format != "lines":
		return fs.fail(`--format must be "junit" or "lines"`)

	case !ok:
		return fs.fail(`--file-from must be "file", "classname" or "suite"`)
	}

	if *prefix != "" {
		if _, err := split.Canonical(*prefix, pathStyle()); err != nil {
			return fs.fail("invalid --path-prefix: %v", err)
		}
	}

	root, err := openDir(*dir, false)
	if err != nil {
		return fmt.Errorf("%w; run bunpo split first", err)
	}
	defer root.Close()

	// Remove any report left by an earlier record attempt on this machine, so
	// a failure below never leaves a stale report behind for merge.
	if err := removeFile(root, reportName(*index)); err != nil {
		return err
	}

	m, err := readManifest(root, *dir, *index)
	if err != nil {
		return err
	}

	if fs.NArg() == 0 && len(m.Tests) > 0 {
		return fmt.Errorf("no reports given, but machine %d was assigned %d tests", *index, len(m.Tests))
	}

	files, err := readReports(fs.Args(), reportOptions{*format, src, *prefix})
	if err != nil {
		return err
	}

	if err := compareWithManifest(con, m, files); err != nil {
		return err
	}

	report, failed := runReport(m, files)
	data, err := manifest.EncodeRunReport(report)
	if err != nil {
		return err
	}

	if err := writeFile(root, reportName(*index), data); err != nil {
		return err
	}

	summary := fmt.Sprintf("recorded %d %s for machine %d", len(files), plural(len(files), "file", "files"), *index)
	if failed > 0 {
		summary += fmt.Sprintf("; %d had failures and %s %s earlier times",
			failed, plural(failed, "keeps", "keep"), plural(failed, "its", "their"))
	}

	con.info("%s", summary)
	return nil
}

func readManifest(root *os.Root, dir string, index int) (manifest.Manifest, error) {
	name := manifestName(index)

	data, found, err := readFile(root, name, manifest.MaxFileBytes)
	if err != nil {
		return manifest.Manifest{}, err
	}

	if !found {
		return manifest.Manifest{}, fmt.Errorf("no %s in %s; run bunpo split on this machine first", name, dir)
	}

	m, err := manifest.DecodeManifest(data)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("%s: %w", name, err)
	}

	if m.Index != index {
		return manifest.Manifest{}, fmt.Errorf("%s is for machine %d", name, m.Index)
	}

	return m, nil
}

// compareWithManifest fails on reported files that were not assigned to this
// machine, since they may have run twice, and warns about assigned files the
// reports left out.
func compareWithManifest(con *console, m manifest.Manifest, files map[string]junit.File) error {
	assigned := make(map[string]bool, len(m.Tests))
	for _, id := range m.Tests {
		assigned[id] = true
	}

	var extra, missing []string
	for _, id := range slices.Sorted(maps.Keys(files)) {
		if !assigned[id] {
			extra = append(extra, id)
		}
	}

	for _, id := range m.Tests {
		if _, ok := files[id]; !ok {
			missing = append(missing, id)
		}
	}

	switch {
	case len(extra) > 0 && len(m.Tests) == 0:
		return checkFailed("machine %d was assigned no tests, but the reports name %d files; "+
			"given no files, most runners run the whole suite, so skip the test command on a machine with no tests",
			m.Index, len(files))

	case len(extra) > 0 && len(extra) == len(files):
		return fmt.Errorf("none of the %d files in the reports were assigned to machine %d (first: %s); "+
			"check --index, --file-from and --path-prefix", len(files), m.Index, extra[0])

	case len(extra) > 0:
		n := len(extra)
		return checkFailed("%d %s in the reports %s not assigned to machine %d, so %s may have run on two machines: %s\n"+
			"some runners treat file arguments as patterns; see the guide for your runner",
			n, plural(n, "file", "files"), plural(n, "was", "were"), m.Index, plural(n, "it", "they"), sample(extra, 5))
	}

	if n := len(missing); n > 0 {
		con.warn("%d assigned %s no test cases in the reports and %s unrecorded: %s",
			n, plural(n, "file has", "files have"), plural(n, "stays", "stay"), sample(missing, 5))
	}

	return nil
}

// runReport turns report totals into observations. A file with a failure
// records no time.
func runReport(m manifest.Manifest, files map[string]junit.File) (manifest.RunReport, int) {
	report := manifest.RunReport{Index: m.Index, Machines: m.Machines, Tests: make(timing.Report, len(files))}
	failed := 0

	for id, f := range files {
		if f.Failed {
			report.Tests[id] = timing.Observation{}
			failed++
			continue
		}

		report.Tests[id] = timing.Observation{Timed: true, MS: f.MS()}
	}

	return report, failed
}

// readReports adds up every report per canonical identifier, so
// "./spec/a_spec.rb" and "spec/a_spec.rb" count as one file.
func readReports(paths []string, opts reportOptions) (map[string]junit.File, error) {
	files := make(map[string]junit.File)
	var bytesRead int64

	for _, path := range paths {
		r, n, err := readReport(path, opts)
		if err != nil {
			return nil, err
		}

		bytesRead += n
		if bytesRead > maxAllReports {
			return nil, fmt.Errorf("reports add up to more than %d bytes", maxAllReports)
		}

		// Sorted, so a bad report always names the same file.
		for _, raw := range slices.Sorted(maps.Keys(r.Files)) {
			file := r.Files[raw]
			id, err := reportID(raw, opts.prefix)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}

			sum, ok := files[id].Add(file)
			if !ok {
				return nil, fmt.Errorf("%s adds up to more than %d ms across the reports", id, timing.MaxDurationMS)
			}

			files[id] = sum
		}
	}

	return files, nil
}

// reportID checks the path as the report wrote it before adding the prefix,
// so "/a" with prefix "tests" fails instead of becoming "tests/a".
func reportID(raw, prefix string) (string, error) {
	id, err := split.Canonical(raw, pathStyle())
	if err != nil || prefix == "" {
		return id, err
	}

	return split.Canonical(prefix+"/"+id, pathStyle())
}

// readReport reads one report through an os.Root of its directory, so a
// symlink cannot point the read elsewhere. It returns the bytes read.
func readReport(path string, opts reportOptions) (junit.Report, int64, error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return junit.Report{}, 0, err
	}
	defer root.Close()

	f, err := root.Open(filepath.Base(path))
	if err != nil {
		return junit.Report{}, 0, fmt.Errorf("%s: %w", path, err)
	}
	defer f.Close()

	counted := &countingReader{r: f}

	var r junit.Report
	if opts.format == "lines" {
		r, err = junit.ReadLines(counted)
	} else {
		r, err = junit.Read(counted, opts.src)
	}

	switch {
	case errors.Is(err, junit.ErrNoFile):
		return junit.Report{}, 0, fmt.Errorf("%s: %w; %s", path, err, noFileHint)

	case err != nil:
		return junit.Report{}, 0, fmt.Errorf("%s: %w", path, err)
	}

	return r, counted.n, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
