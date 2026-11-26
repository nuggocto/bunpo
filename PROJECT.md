---
tags: [go, project, product, bunpo, ci, testing, test-splitting, cli, developer-tools]
created: 2026-11-11
updated: 2026-11-26
status: active
license: Apache-2.0
domain: bunpo.org
domain_status: live
---

# Bunpō (分蜂) — The bees dividing their hive

**One line:** a free, open-source CLI that balances CI test suites across parallel
machines by timing and verifies that every input test file was assigned once.

Split test files by name and one machine gets the slow files while the others sit
idle. Bunpō remembers how long each file took, deals them out longest first in
whole milliseconds, and fails the build if two machines ever saw different lists
or timings.

**Current release:** `v0.1.2` (static CLI, self-hosted live queue with
Cloudflare Worker, `bunpo-server`, and dashboard, GitHub Action, five runner
guides, CLI reference, real-repo trials, and `bunpo.org`).

**License and model:** Apache-2.0, free and open source forever. No paid SaaS, no
accounts, no telemetry. Both the static splitter and the live queue are
meant to run on a team's own infrastructure.

## Why

- **Open splitters leave gaps.** Knapsack Pro sells timing-based splitting as a
  hosted service; its original open-source gem is archived. Other open tools are
  either single-language (`pytest-split`, `testris`), tied to one CI vendor
  (`ci_split_tests`), or skip verification and multi-run medians (`split_tests`,
  `split-test`). None combine integer-only determinism, failure-time exclusion,
  assignment verification, manifest-checked recording, and real-runner tested
  guides for pytest, Jest, Vitest, RSpec, and Playwright.
- **CI vendors keep it to themselves.** CircleCI and Depot offer splitting inside
  their own platforms. Teams on GitHub Actions or self-hosted runners have nothing
  built in.
- **Nobody checks the split.** If two machines see a different file list or a
  stale timing cache, assignments can overlap or drop files while the build stays
  green. Bunpō's `verify` and `record` commands catch that mismatch; `verify`
  checks assignments and does not observe execution.
- **The risk is plain.** CI vendors may keep adding built-in splitting. Bunpō has
  to stay small, dependable, and easy to adopt without vendor lock-in.

## Scope

| Stage | Scope |
| --- | --- |
| Shipped (`v0.0.1`–`v0.1.2`) | `split`, `plan`, `record`, `merge`, `verify`, `version`, `--queue`, `--status`. Static file-level splitting from JUnit XML or two-column timings, assignment verification, free self-hosted live queue (`bunpo --queue`, Cloudflare Workers + Durable Objects server, standalone `bunpo-server`, shared dashboard, HTTP protocol spec), cross-compiled binaries, GitHub Action (`nuggocto/bunpo@v0`), five runner guides, CLI reference, benchmarks, real-repo trials, and `bunpo.org`. |
| Extensions (`0.2.0`) | CI env-var auto-detection, `$GITHUB_STEP_SUMMARY` and flaky-test summaries from timing history, Go package splitting, sub-file splitting for dominant files, and more runner guides (`bun test`, Cypress, Minitest, PHPUnit). |

Bunpō decides which tests run where. In static mode it does not run tests, retry
them, start machines, or replace a CI system. The live queue coordinates retryable
assignments; execution still belongs to the test runner.

## Core rules

### The split

- The input is a list of test identifiers, one per line. In the first build an
  identifier is a file path.
- Use repository-relative identifiers with `/` separators. Convert native Windows separators at input; reject literal backslashes in POSIX filenames rather than reinterpreting them.
- Drop `.` and empty components. Reject an identifier that is empty, absolute,
  starts with a Windows drive, contains a `..` component, contains a NUL, a
  carriage return or a newline, or is not valid UTF-8. Check for a drive after
  dropping components, because `./C:/a.py` only shows its drive once the `.`
  is gone. The timing file is JSON, which cannot hold invalid UTF-8.
- Keep the original spelling until collision checks finish. If distinct input paths normalize to one identifier, fail and name both before emitting any assignment or digest. Then remove exact repeats and sort by bytes.
- Durations are whole milliseconds. No floating-point arithmetic takes part in
  the split, so every platform computes the same answer. One duration is at
  most 30 days, so a total of 200,000 of them fits in a 64-bit integer.
- Deal the tests out longest first. Each test goes to the machine with the
  least total time so far. On a tie between tests, the lower path goes first.
  On a tie between machines, the one with fewer tests wins, then the lower
  index. Without the test count, files recorded at 0 ms would all land on
  machine 0, because its total never rises.
- The split uses recorded times as they are, zero included.
- A test with no recorded time gets the median of the known times. With no
  history at all, every test counts as 0 ms, and the fewer-tests rule makes
  the split an even deal.
- The median of an even number of values is the average of the two middle
  values, rounded half up, in integer arithmetic. The same rule applies to a
  test's own samples and to the median of known times. It matches the median a
  spreadsheet or Python's `statistics.median` gives, so a user can check any
  prediction by hand. Taking the lower middle value would pick the faster of
  two samples and under-predict during the first runs.
- The same list, the same timings and the same machine count always give the
  same split. This is the property everything else depends on.
- Report the largest predicted file time and the average load per machine.
  Warn when one file exceeds that average or some shards must be empty.
  A 100-minute file and nine one-minute files cannot balance on two machines;
  file-level scheduling cannot divide the dominant file.

A worked example to check by hand: six files taking 9, 7, 6, 5, 4 and 3
minutes, on two machines. Splitting by name gives 22 and 12. Bunpō gives 9, 5
and 3 to one machine and 7, 6 and 4 to the other: 17 and 17.

This method does not always find the perfect split. Its worst case is known:
for identical machines and fixed nonnegative durations, the largest assigned
total is at most four thirds of the optimal makespan. This concerns the input
estimates, not actual CI wall time. Measure how close it gets on real suites.

### Verification of assignments

- Each `split` writes a small manifest: the machine index, the machine count,
  the list it was given, and a SHA-256 digest of everything the split depends
  on.
- The digest covers the split algorithm version, the machine count, the
  sorted input list, and the effective duration of each test: its median, or
  the fill-in value for a test with no history. Equal digests mean equal
  splits. Two timing files that differ but give the same effective durations
  produce the same digest, which is correct because the split is the same.
- The algorithm version is an integer, raised whenever a change could alter
  any split. A machine that downloaded a different Bunpō with a changed
  algorithm then shows a different digest.
- The digest input is a version line, a machine-count line, then one record
  per test in sorted order: the path, a NUL byte, the duration in decimal
  milliseconds, and a newline. Paths cannot contain NUL and the line-based
  input cannot carry a newline, so the encoding is unambiguous.
- `verify` reads every manifest. It fails unless all digests match, every
  index from 0 to the count minus one is present once, no test appears in two
  assigned lists, and their union equals the independently supplied canonical
  input list. Keep the full input distinct from each machine's assigned list.
- When digests differ, `verify` groups machines by digest and names the
  machines in each group, so the message points at the machine that differed.
- A success says "assignment verified". A manifest records what a runner was
  asked to do; it cannot prove that the runner started, completed or executed
  each case once. A skipped runner can still have a valid assignment manifest.
- The workflow separately checks runner exit statuses and expected report
  artifacts, including an explicit outcome for an empty shard. Reported file
  coverage is separate evidence from assignment coverage. Detecting missing
  individual cases requires an independent case inventory; JUnit timings alone
  do not provide it. Timing samples from passed cases are not execution proof.
- A team that prefers prevention to detection can run `bunpo plan` once in an
  early job and pass the plan file to every machine. Each machine then only
  reads its own list from the plan.

### Timings

- `record` reads JUnit XML and adds up the time of each test case per file.
  A file's total counts for a run only when every case in that file passed.
  A failed test's time says little about how long it normally takes, and
  adding up only the passed cases of a file with a failure records a total
  that is too short.
- A skipped case does not fail its file. Otherwise a file with one
  platform-specific skip would never get a time.
- JUnit writes times as decimal seconds. Convert them to milliseconds from the
  decimal text, rounding half up, and never through a float.
- Add up a file's case times exactly and round once per file. Rounding each
  case first turns a thousand 0.4 ms cases into 0 ms.
- The timing file keeps the last five times per test and uses their median.
  One slow run does not reshuffle everything.
- `merge` combines timing files from all machines and adds one to a run
  counter stored in the timing file. Each test stores the counter value of
  the last run that saw it.
- A test counts as seen when it appears in a report, whether it passed,
  failed or was skipped. A file with a failure records no time for that run
  but stays seen, so a file that keeps failing while a team fixes it keeps its
  history.
- A test in two reports from one run ran on two machines, so the split
  overlapped. `merge` fails rather than keep one of the two times, which would
  make the result depend on report order. A merge with no reports also fails,
  because it would age every test without seeing any.
- `merge` drops a test once the counter is more than 30 runs past its
  last-seen value, so the file cannot grow without limit. Counting merges
  instead of days needs no clock, and an idle repository keeps its timings.
- The timing file is versioned JSON. An unknown version is an error, not a
  guess.
- Timings should be saved from the main branch only, and only when every test
  job passed. A pull request or failing run reads them and does not overwrite
  them.

### Reading reports

- A JUnit report must say which file each test came from. Some runners write
  that only when asked. Bunpō reports a clear error with the setting to change
  when the file is missing; it never guesses a file from a class name.
- Every test case must name its file. One case without a file fails `record`:
  leaving it out would also leave out its failure, and the file would record
  an incomplete time.
- A report must be one JUnit document: exactly one root element and no text
  outside it. An empty file, plain text or two concatenated reports fail, so a
  runner that crashed before writing cannot pass for an empty suite.
- `--file-from classname` or `--file-from suite` reads the path from that
  attribute instead, and `--path-prefix` goes in front of every path, after
  the path as written passes the identifier rules, so `/a` cannot become
  `tests/a`. The user
  sets them once, following the runner's guide; Playwright needs both.
- A plain two-column format, identifier and milliseconds, is accepted for any
  runner that has no usable report.
- Reports are untrusted input. Bound their size, depth and entry count.
- `record` compares the report with the machine's manifest. A file that was
  not assigned to the machine fails with status 1, because it may have run
  twice. If nothing matches, the flags are more likely wrong, and the error
  says so with status 2. An assigned file missing from the report is a
  warning.
- With no reports, `record` writes an empty run report, but only when the
  machine's share was empty.
- Golden reports from pytest, Jest, Vitest, RSpec and Playwright come from the real runners. A
  script in `testdata/` runs each one, at a pinned version, on a small sample
  suite, and its output is committed. CI reads the committed files and installs
  no runner.

### Command-line interface

- Every file Bunpō reads or writes lives in one directory, `.bunpo/` by
  default, set with `--dir`. CI caches or uploads that one folder.

  | File | Written by | Read by |
  | --- | --- | --- |
  | `timings.json` | `merge` | `split`, `plan`, `merge` |
  | `manifest-<index>.json` | `split` | `record`, `verify` |
  | `report-<index>.json` | `record` | `merge` |
  | `plan.json` | `plan` | `split` |

  Per-machine files carry the machine index in their names, so the final job
  can download every machine's folder into one without collisions.
- `split`, `plan` and `verify` read the test list from standard input, one
  identifier per line. Lines may end in LF or CRLF; a carriage return
  anywhere else stays in the identifier and fails the identifier rules.
- An empty test list is an error. A discovery command that found nothing, or
  failed, would otherwise pass with no tests run.
- Standard output carries only file lists, so it can be piped into a test
  command. Messages and warnings go to standard error.
- Every file Bunpō writes is versioned JSON, read as strictly as the timing
  file: unknown members, duplicate keys and invalid UTF-8 are errors, and so
  is an unknown version. Every member is required, because a missing one
  would read as zero: a run report without `index` would pass for machine 0.
- Files are written to a temporary name and renamed into place, so a failed
  write leaves the previous file intact.
- `split` writes the test list before the manifest, and fails if the list
  cannot be written. A list that never reached the runner leaves no manifest
  for `verify` to accept.
- `merge` needs a run report from every machine. A missing one fails with
  status 1 and leaves the timing file unchanged, because a run with a machine
  missing has incomplete timings.
- A report from an earlier run must never stand in for the current one.
  `merge` deletes the reports it merged, `split` starts by deleting its own
  machine's leftover manifest and report, and `plan` starts by deleting any
  leftover `plan.json`, so a reused directory cannot count a run twice and a
  failed split or plan leaves no stale file behind.
- Exit codes: 0 for success, 1 when a check fails (`verify` finds a mismatch,
  `merge` finds a test in two reports), 2 for bad usage or bad input (an
  unknown flag, an unreadable file, an invalid identifier). Warnings, such as a
  dominant file or empty shards, go to standard error and still exit 0. This
  matches `diff` and `grep`, so a workflow can tell a failed check from a
  mistake in the call.

## Technical choices

| Area | Choice | Notes |
| --- | --- | --- |
| Language and build | Go 1.27, one module, patch pinned by the `toolchain` directive in `go.mod` | The `go` line states the minimum. The `toolchain` line names the patch Go downloads automatically (1.27.4 on 2026-11-16). CI reads it through `setup-go`; release builds set `GOTOOLCHAIN` to that exact version. No extra version manager is required. |
| Dependencies | Standard library only | `flag`, `encoding/xml`, `crypto/sha256`. A tool that runs in other people's CI should bring nothing with it. |
| Timing file | `encoding/json/v2` | It rejects duplicate keys and invalid UTF-8, which is right for a new format. |
| File access | One `os.Root` per report or timing directory the user names | The directory can be anywhere, such as an absolute cache path. File names read inside it cannot leave it through `..` or a symlink. |
| Output | File lists on standard output, messages on standard error | Lets the list be piped straight into a test command. |
| Tools | Staticcheck and `govulncheck` pinned with `tool` directives | Run through `go tool`. |
| Release | Cross-compiled binaries with checksums on GitHub releases | Linux, macOS and Windows, x86-64 and ARM64. |
| Self-hosted live queue | Cloudflare Workers + Durable Objects, plus a small Go HTTP server | Same small JSON protocol for both. Workers gives zero-ops scale-to-zero; the Go binary covers teams not on Cloudflare. |

Go is chosen because it produces one file per platform from one machine and
starts in milliseconds. People will download this at the start of every CI
run, so both matter.

### Package layout

```text
cmd/bunpo/               subcommands, flags, files, messages and exit codes
internal/split/          sort, deal out, digest; plain functions
internal/timing/         the timing file, median, merge, pruning
internal/junit/          bounded JUnit XML and two-column report reading
internal/manifest/       manifests, plans, run reports and the verify check
internal/jsonfile/       strict versioned-JSON encoding and decoding
```

`internal/split` takes a list and a map of durations and returns lists. It
reads no files and no environment variables, so the whole algorithm is tested
as plain functions.

### Verification

- Table-driven tests for the split, including the worked example, ties, an
  empty list, one test, more machines than tests and missing timings.
- A property test: for any list, timings and machine count, the lists are
  disjoint, their union is the input, and shuffling the input changes nothing.
- For small generated cases, enumerate assignments with an independent bounded
  solver to find the exact optimum. Check `3 * LPT <= 4 * optimum` with checked
  integer arithmetic. Bound the job count, machine count and durations so the
  oracle remains cheap.
- Keep four jobs of duration 3 on three machines as a regression case: LPT and
  the optimum are 6, while `max(longest, total / machines)` is only 4.
  Four thirds of that lower bound is not a valid ceiling for LPT.
- A report where one case in a file failed: `record` writes no time for that
  file in that run and keeps its earlier times.
- Fuzz the JUnit reader and the timing-file reader with bounded input.
- Golden tests for real reports from pytest, Jest, Vitest, RSpec and Playwright, kept
  in the repository.
- An end-to-end test that runs the built binary as eight processes and then
  `verify`, once clean and once with one machine given a stale timing file.
- A workflow fixture with valid manifests but one skipped runner: assignment
  verification passes with that label, while the missing-report check fails.
  Exercise a dominant file and empty shards without asserting a 10% balance.
- CI runs `gofmt`, `go vet`, Staticcheck, `govulncheck` and `go test -race`.
  Bunpō splits its own tests once it can.

## Initial limits

Starting guesses. Revise them from the first real users.

| Resource | Starting value | Why |
| --- | --- | --- |
| Tests in one split | 200,000 | Far above a large monorepo's file count. |
| Test files in one queue build | 20,000 on the Go server; 5,000 by default on the Worker | Protocol and Go server keep 20,000; Worker `MAX_TESTS` defaults to 5,000 and goes up to 20,000, which worked on a real Free-plan account. Static splitting keeps 200,000. |
| Machines | 256 | More than any matrix I expect; bounds the manifest set. |
| One JUnit report / all reports | 64 MiB / 1 GiB | Real reports are a few megabytes. |
| JUnit nesting / test cases per report | 32 levels / 2,000,000 | Real reports nest four levels. |
| Test list on standard input | 64 MiB | 200,000 paths at about 300 bytes each. |
| Timing file | 64 MiB | 200,000 tests at about 250 bytes each: a path, five times and a last-seen run. |
| Times kept per test | 5 | Enough for a stable median. |
| Runs before an unseen test is dropped | 30 | Deleted tests leave the file within weeks. |
| Longest single duration | 30 days | Far above any real file, and 200,000 of them still fit in a 64-bit total. |
| Run time of `split` | Under 1 second for 50,000 tests | It runs on every machine, every build. Measure it. |

## Distribution and license

- **Download:** cross-compiled static binaries on GitHub Releases with
  `SHA256SUMS` (Linux, macOS, and Windows on x86-64 and ARM64).
- **GitHub Action:** `nuggocto/bunpo@v0` downloads the right binary, verifies it
  against `SHA256SUMS`, and puts `bunpo` on `PATH`.
- **Hosting:** none for static mode (timings live in the user's CI cache);
  self-hosted on the user's own infrastructure for the live queue.
- **Website:** static Astro site on Cloudflare Pages at `bunpo.org` in
  `nuggocto/bunpo-site`. Pin Bun, commit `bun.lock`, and verify built output
  before publishing. The CLI never contacts the site.
- **License: Apache-2.0.** Permissive by design so teams can adopt Bunpō in CI
  without legal review or vendor lock-in.

## Self-hosted queue tradeoffs

Keeping the live queue free, open source, and self-hosted removes accounts,
billing, and third-party data sharing, while introducing clear operational
tradeoffs:

- **Cloudflare Workers + Durable Objects (primary option):** One Durable Object
  owns each build's queue. It scales to zero between CI runs, needs no OS
  patching or database upkeep, and deploys to a team's own Cloudflare account in
  one command. Tradeoff: requires a Cloudflare account.
- **Lightweight Go queue server (non-Cloudflare option):** The queue HTTP
  protocol stays small and documented (init, lease, heartbeat, report, status)
  so the CLI is never coupled to Cloudflare. A small standalone Go server binary
  (and container image) speaking the exact same protocol gives teams on a VPS,
  Kubernetes, or self-hosted runners a drop-in path without Cloudflare. Tradeoff:
  teams manage the running process and TLS themselves.
- **Bundled dashboard without duplication:** One small static web UI talks to
  the queue's relative JSON read endpoints (`/api/v1/...`) and is built once,
  then served unchanged by both queue servers — through Workers Static Assets on
  Cloudflare and `//go:embed` in the Go binary. Auth stays a single shared read
  token (or Cloudflare Access). For static-mode users with no queue server, the
  CLI prints a plain Markdown summary to stderr and `$GITHUB_STEP_SUMMARY`
  rather than maintaining a second HTML UI.
- **No direct revenue:** Acceptable by design. A reliable free tool that saves
  developers CI time is the goal.

## Open questions

- Do teams prefer a final `verify` + `merge` job or an upfront `bunpo plan` job?
- How often does one giant test file dominate a suite and require sub-file
  splitting rather than file-level splitting?
- For the live queue, do teams mostly deploy the Cloudflare Worker or prefer a
  single-binary Go server next to their self-hosted runners?

## Roadmap

### `0.2.0` — Polish and CLI extensions

Focus on practical improvements driven by real CI usage:

- Detect machine index and total automatically from CI environment variables
  (GitHub Actions, GitLab CI, Woodpecker CI, Forgejo/Gitea Actions, Buildkite,
  etc.) while keeping explicit flags as overrides, and publish tested workflow
  guides for those CI systems alongside the GitHub Actions guides.
- Emit concise Markdown summaries (`$GITHUB_STEP_SUMMARY`) and local flaky-test /
  slowest-file reports from `timings.json` and queue attempt history.
- Go support, where the unit is a package rather than a file.
- Splitting inside a single dominant test file for runners that can select
  individual tests.
- Additional runner guides with golden reports and `guidecheck` coverage:
  `bun test`, Cypress, Minitest, and PHPUnit.

## Name, domain and mascot

**Bunpō** (分蜂) is the Japanese word for a bee colony dividing: part of the
hive leaves to work somewhere else. A test suite divides across machines the
same way. Use `bunpo` without the accent for the repository, the module and
the binary.

### Domain

`bunpo.org` is live on Cloudflare Pages and holds the guides, benchmarks, and
changelog.

### Mascot — Mitsu, the bee

Mitsu is a round honey-brown and cream bee with mint wings, amber pollen
baskets and a divided honeycomb satchel. She hands out honeycomb cells to a
row of smaller bees so that each carries the same weight.

A cell glows only when `verify` has passed. An unverified split shows plain
cells. Use honey brown, cream, mint and amber. The small icon is Mitsu's face
over one hexagon. Results and errors stay in plain text. Commission original
licensed artwork once the tool has steady real-world usage.

## References

- [Knapsack Pro overview](https://docs.knapsackpro.com/overview/) and the [archived Knapsack gem](https://docs.knapsackpro.com/ruby/knapsack)
- [split_tests](https://github.com/leonid-shevtsov/split_tests), [split-test](https://github.com/mtsmfm/split-test), [ci_split_tests](https://github.com/payfazz/ci_split_tests), [pytest-split](https://pypi.org/project/pytest-split/) and [testris](https://pkg.go.dev/github.com/mcncl/testris)
- [Depot test splitting, July 2026](https://depot.dev/changelog/2026-07-15-test-splitting)
- [Go](https://go.dev/doc/), [Go releases](https://go.dev/doc/devel/release), [os.Root](https://pkg.go.dev/os#Root) and [encoding/json/v2](https://pkg.go.dev/encoding/json/v2)
