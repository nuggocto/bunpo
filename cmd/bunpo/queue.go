package main

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nuggocto/bunpo/internal/junit"
	"github.com/nuggocto/bunpo/internal/manifest"
	"github.com/nuggocto/bunpo/internal/queue"
	"github.com/nuggocto/bunpo/internal/queueapi"
	"github.com/nuggocto/bunpo/internal/queuerunner"
	"github.com/nuggocto/bunpo/internal/split"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	if v == "" {
		return errors.New("path must not be empty")
	}
	*s = append(*s, v)
	return nil
}

// isQueueInvocation reports whether args starts with --queue / -queue and
// returns the URL (if inline) plus the remaining flags.
func isQueueInvocation(args []string) (queueURL string, rest []string, ok bool) {
	if len(args) == 0 {
		return "", nil, false
	}
	a := args[0]
	switch {
	case a == "--queue" || a == "-queue":
		if len(args) == 1 {
			return "", nil, true
		}
		if args[1] == "-h" || args[1] == "--help" || args[1] == "-help" {
			return "", []string{"-h"}, true
		}
		return args[1], args[2:], true
	case strings.HasPrefix(a, "--queue="):
		return strings.TrimPrefix(a, "--queue="), args[1:], true
	case strings.HasPrefix(a, "-queue="):
		return strings.TrimPrefix(a, "-queue="), args[1:], true
	default:
		return "", nil, false
	}
}

func cmdQueue(queueURL string, args []string, stdin io.Reader, stdout io.Writer, con *console, getenv func(string) string) error {
	fs := newFlags("queue", "--queue URL --build ID [--index N] --total M [--dir DIR] --report PATH... [flags] < tests.txt -- command args...\n       bunpo --queue URL --build ID --status [--dir DIR]", con)
	dir := fs.String("dir", defaultDir, "directory for Bunpō's files")
	buildID := fs.String("build", "", "build identifier (required)")
	statusMode := fs.Bool("status", false, "print build verdict and write report-0.json when resolved")
	index := fs.Int("index", -1, "optional machine index metadata (0 to 255)")
	total := fs.Int("total", 0, "number of parallel machines (required in worker mode)")
	var reportPaths stringList
	fs.Var(&reportPaths, "report", "test report path written by the command (repeatable, required in worker mode)")
	format := fs.String("format", "junit", `"junit" for JUnit XML, or "lines" for two-column timings`)
	fileFrom := fs.String("file-from", "file", `where JUnit reports keep each test's file: "file", "classname" or "suite"`)
	prefix := fs.String("path-prefix", "", "directory to put before every path in the reports")
	leaseTTL := fs.Duration("lease-ttl", time.Duration(queue.DefaultLeaseTTLMS)*time.Millisecond, "lease TTL duration (minimum enforced by server)")
	buildTimeout := fs.Duration("build-timeout", time.Duration(queue.DefaultBuildTimeoutMS)*time.Millisecond, "build deadline duration")
	maxAttempts := fs.Int("max-attempts", queue.DefaultMaxAttempts, "maximum attempts per test (1 to 10)")
	maxBatch := fs.Int("max-batch", queue.DefaultMaxBatch, "maximum tests per lease (1 to 200)")
	allowInsecure := fs.Bool("allow-insecure-http", false, "allow plain http:// outside loopback (for private networks)")

	if err := fs.parse(args); err != nil {
		return err
	}

	if queueURL == "" {
		return fs.fail("--queue URL is required")
	}
	if *buildID == "" {
		return fs.fail("--build is required")
	}
	if !queue.ValidBuildID(*buildID) {
		return fs.fail("invalid --build %q", *buildID)
	}

	if *allowInsecure {
		if u, err := url.Parse(queueURL); err == nil && u.Scheme == "http" {
			con.warn("using plain http:// with --allow-insecure-http; ensure transport security on this network")
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *statusMode {
		if fs.NArg() > 0 {
			return fs.fail("unexpected argument %q with --status", fs.Arg(0))
		}
		tok := getenv("BUNPO_QUEUE_READ_TOKEN")
		if tok == "" {
			tok = getenv("BUNPO_QUEUE_TOKEN")
		}
		if tok == "" {
			return fs.fail("BUNPO_QUEUE_READ_TOKEN or BUNPO_QUEUE_TOKEN must be set")
		}
		client, err := queueapi.NewClient(queueapi.ClientConfig{
			BaseURL:           queueURL,
			Token:             tok,
			AllowInsecureHTTP: *allowInsecure,
		})
		if err != nil {
			return err
		}
		return runQueueStatus(ctx, client, *buildID, *dir, con)
	}

	src, ok := fileSources[*fileFrom]
	switch {
	case *total < 1 || *total > queue.MaxMachines:
		return fs.fail("--total is required, from 1 to %d", queue.MaxMachines)
	case *index < -1 || *index >= queue.MaxMachines:
		return fs.fail("--index must be from 0 to %d", queue.MaxMachines-1)
	case len(reportPaths) == 0:
		return fs.fail("at least one --report path is required")
	case *format != "junit" && *format != "lines":
		return fs.fail(`--format must be "junit" or "lines"`)
	case !ok:
		return fs.fail(`--file-from must be "file", "classname" or "suite"`)
	case *leaseTTL <= 0 || leaseTTL.Milliseconds() <= 0:
		return fs.fail("--lease-ttl must be positive")
	case *buildTimeout <= 0 || buildTimeout.Milliseconds() <= 0:
		return fs.fail("--build-timeout must be positive")
	case *maxAttempts < 1 || *maxAttempts > queue.MaxAttemptsLimit:
		return fs.fail("--max-attempts must be from 1 to %d", queue.MaxAttemptsLimit)
	case *maxBatch < 1 || *maxBatch > queue.MaxBatchLimit:
		return fs.fail("--max-batch must be from 1 to %d", queue.MaxBatchLimit)
	case fs.NArg() == 0:
		return fs.fail("test command is required after --")
	}

	if *prefix != "" {
		if _, err := split.Canonical(*prefix, pathStyle()); err != nil {
			return fs.fail("invalid --path-prefix: %v", err)
		}
	}

	tok := getenv("BUNPO_QUEUE_TOKEN")
	if tok == "" {
		return fs.fail("BUNPO_QUEUE_TOKEN must be set")
	}
	client, err := queueapi.NewClient(queueapi.ClientConfig{
		BaseURL:           queueURL,
		Token:             tok,
		AllowInsecureHTTP: *allowInsecure,
	})
	if err != nil {
		return err
	}

	inv, err := readInventory(stdin)
	if err != nil {
		return err
	}

	root, err := openDir(*dir, true)
	if err != nil {
		return err
	}
	timings, _, err := readTimings(root)
	_ = root.Close()
	if err != nil {
		return err
	}

	ids := inv.IDs()
	durations := timings.Durations()
	preds := make([]*int64, len(ids))
	for i, id := range ids {
		if ms, ok := durations[id]; ok {
			v := ms
			preds[i] = &v
		}
	}

	var machineIdx *int
	if *index >= 0 {
		v := *index
		machineIdx = &v
	}

	opts := reportOptions{format: *format, src: src, prefix: *prefix}
	verdict, err := queuerunner.Run(ctx, queuerunner.RunConfig{
		Client:        client,
		BuildID:       *buildID,
		MachineIndex:  machineIdx,
		Config:        queue.Config{Machines: *total, LeaseTTLMS: leaseTTL.Milliseconds(), BuildTimeoutMS: buildTimeout.Milliseconds(), MaxAttempts: *maxAttempts, MaxBatch: *maxBatch},
		Tests:         ids,
		PredictionsMS: preds,
		Dir:           *dir,
		Command:       fs.Args(),
		ReportPaths:   reportPaths,
		Format:        *format,
		ReadReports: func(paths []string) (map[string]junit.File, error) {
			return readReports(paths, opts)
		},
		Stdout:  stdout,
		Stderr:  con.w,
		LogInfo: con.info,
		LogWarn: con.warn,
	})
	if err != nil {
		return formatQueueRunError(err)
	}

	if verdict == queue.VerdictPassed {
		con.success("queue passed", "build %s passed (%d tests)", *buildID, len(ids))
		return nil
	}
	return checkFailed("build %s finished with verdict %s", *buildID, verdict)
}

func formatQueueRunError(err error) error {
	var extraErr *queuerunner.ExtraFilesError
	if errors.As(err, &extraErr) {
		return checkFailed("%v\nsome runners treat file arguments as patterns; see the guide for your runner", extraErr)
	}
	var apiErr *queueapi.APIError
	if errors.As(err, &apiErr) {
		if apiErr.HTTPStatus == 400 {
			return err
		}
		if apiErr.Code == queueapi.ErrCodeAtCapacity {
			return checkFailed("queue server is at capacity: %s", apiErr.Message)
		}
		return checkFailed("%v", err)
	}
	return checkFailed("%v", err)
}

func runQueueStatus(ctx context.Context, client *queueapi.Client, buildID, dir string, con *console) error {
	root, err := openDir(dir, true)
	if err != nil {
		return err
	}
	defer root.Close()

	// Remove any leftover report-0.json first so a stale report never survives.
	if err := removeFile(root, reportName(0)); err != nil {
		return err
	}
	existing, err := indexedFiles(root, "report-")
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return checkFailed("found other run report(s) in %s (for example %s); clean the directory before running --status", dir, reportName(existing[0]))
	}

	st, err := client.Status(ctx, buildID)
	if err != nil {
		return checkFailed("%v", err)
	}

	con.info("build %s: verdict %s (%d passed, %d skipped, %d failed, %d unresolved, %d pending, %d leased)",
		st.BuildID, st.Verdict, st.Counts.Passed, st.Counts.Skipped, st.Counts.Failed, st.Counts.Unresolved, st.Counts.Pending, st.Counts.Leased)

	for _, c := range st.Conflicts {
		con.warn("init conflict from runner %s: %s", c.RunnerID, c.Reason)
	}
	if st.ConflictsTruncated {
		con.warn("more init conflicts occurred past the 8-entry limit")
	}
	for _, p := range st.Problems {
		con.warn("%s: %s (attempts: %d)", p.State, p.Test, p.Generation)
	}

	report, ok, err := queuerunner.BuildRunReport(ctx, client, buildID, st)
	if err != nil {
		return checkFailed("%v", err)
	}
	if ok {
		data, err := manifest.EncodeRunReport(report)
		if err != nil {
			return err
		}
		if err := writeFile(root, reportName(0), data); err != nil {
			return err
		}
		con.info("wrote %s (%d files) for bunpo merge", reportName(0), len(report.Tests))
	}

	if st.Verdict == queue.VerdictPassed {
		con.success("queue passed", "build %s passed (%d tests)", st.BuildID, st.Counts.Total)
		return nil
	}
	return checkFailed("build %s verdict is %s", st.BuildID, st.Verdict)
}
