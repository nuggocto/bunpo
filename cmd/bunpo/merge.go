package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/timing"
)

func cmdMerge(args []string, _ io.Reader, _ io.Writer, con *console) error {
	fs := newFlags("merge", "merge [--dir DIR]", con)
	dir := fs.String("dir", defaultDir, "directory holding every machine's run report and the timing file")

	if err := fs.parse(args); err != nil {
		return err
	}

	if fs.NArg() > 0 {
		return fs.fail("unexpected argument %q", fs.Arg(0))
	}

	root, err := openDir(*dir, false)
	if err != nil {
		return err
	}
	defer root.Close()

	base, _, err := readTimings(root)
	if err != nil {
		return err
	}

	reports, err := readRunReports(root, *dir)
	if err != nil {
		return err
	}

	merged, err := timing.Merge(base, reports)
	if errors.Is(err, timing.ErrInvalidReport) {
		return checkFailed("%v", err)
	}

	if err != nil {
		return fmt.Errorf("%s: %w", timingsName, err)
	}

	data, err := timing.Encode(merged)
	if err != nil {
		return err
	}

	if err := writeFile(root, timingsName, data); err != nil {
		return err
	}

	// Merged reports are consumed, so merging again in a reused directory
	// cannot count the same run twice.
	for i := range reports {
		if err := removeFile(root, reportName(i)); err != nil {
			return fmt.Errorf("%s is updated, but removing %s failed, and merging again would count it twice: %w",
				timingsName, reportName(i), err)
		}
	}

	printMergeSummary(con, base, merged, reports)
	return nil
}

// readRunReports returns one report per machine, in machine order. A missing
// machine fails the check, because that run's timings are incomplete.
func readRunReports(root *os.Root, dir string) ([]timing.Report, error) {
	indexes, err := indexedFiles(root, "report-")
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		return nil, checkFailed("no run reports in %s; each machine must run bunpo record and upload its files", dir)
	}

	reports := make([]timing.Report, 0, len(indexes))
	have := make(map[int]bool, len(indexes))
	machines := 0

	for _, i := range indexes {
		data, _, err := readFile(root, reportName(i), manifest.MaxFileBytes)
		if err != nil {
			return nil, err
		}

		r, err := manifest.DecodeRunReport(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", reportName(i), err)
		}

		switch {
		case r.Index != i:
			return nil, fmt.Errorf("%s is for machine %d", reportName(i), r.Index)

		case machines != 0 && r.Machines != machines:
			return nil, checkFailed("run reports disagree on the machine count: %d and %d", machines, r.Machines)
		}

		machines = r.Machines
		have[i] = true
		reports = append(reports, r.Tests)
	}

	var missing []string
	for i := range machines {
		if !have[i] {
			missing = append(missing, strconv.Itoa(i))
		}
	}

	if len(missing) > 0 {
		return nil, checkFailed("no run report from %s %s of %d; %s left unchanged",
			plural(len(missing), "machine", "machines"), strings.Join(missing, ", "), machines, timingsName)
	}

	return reports, nil
}

func printMergeSummary(con *console, base, merged timing.File, reports []timing.Report) {
	seen, timed, dropped := 0, 0, 0

	for _, r := range reports {
		for _, o := range r {
			seen++

			if o.Timed {
				timed++
			}
		}
	}

	for id := range base.Tests {
		if _, ok := merged.Tests[id]; !ok {
			dropped++
		}
	}

	summary := fmt.Sprintf("merged %d run %s into %s (run %d): %d files seen, %d timed",
		len(reports), plural(len(reports), "report", "reports"), timingsName, merged.Run, seen, timed)
	if dropped > 0 {
		summary += fmt.Sprintf(", %d dropped after %d unseen runs", dropped, timing.PruneAfterRuns)
	}

	con.info("%s", summary)
}
