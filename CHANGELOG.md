# Changelog

All notable changes to Bunpō are recorded here. Release binaries and checksums
start at `v0.0.3` on [GitHub Releases](https://github.com/nuggocto/bunpo/releases).

## [Unreleased]

### Added

- Live-queue design record (`docs/decisions/0003-live-queue.md`) and HTTP protocol and state-machine specification (`docs/queue-protocol.md`).
- Skipped test-case count (`junit.File.Skipped`) in `internal/junit`, keeping `bunpo record` behavior unchanged.
- Pure event-sourced live-queue state machine (`internal/queue`) with shared JSON scenario vectors and event-replay fuzzing (`FuzzEventReplay`).
- Live-queue HTTP wire format, handler, single-shot client, persistent CRC-32C disk event log (`DiskLog`), black-box protocol test suite (`internal/queueapi`), and standalone server (`cmd/bunpo-server`, in-memory or persistent via `--data DIR`).
- Sans-I/O live-queue runner and status state machines (`internal/queuerunner`), `bunpo --queue` CLI mode (worker loop, process-group signaling, token stripping from child commands, `--status`, and `report-0.json` output for `bunpo merge`), deterministic simulation harness (`internal/sim` driving the real `Handler`, `DiskLog`, and runner/status machines on a faulted virtual network and filesystem), and multi-process crash/kill end-to-end tests.
- Cloudflare Worker live-queue backend (`worker/`) with a pure TypeScript port of the state machine (`worker/src/core`), SQLite-backed `BuildDO` (chunked test catalog, append-only event log, throttled active-lease deadline persistence across 10 s DO hibernation, and deadline/+7d cleanup alarms) and `BuildIndex`, constant-time bearer auth, streaming body bounds, differential replay against the Go simulator, seeded DO eviction tests, and local `workerd` black-box and `bunpo --queue` E2E CI job.
- Shared static live-queue dashboard (`internal/dashboard/static/`) served identically by `bunpo-server` (`//go:embed`, `+61.5 KiB`) and the Cloudflare Worker (Workers Static Assets, 0 Worker/DO invocations for static files), with recent builds, per-machine SVG lease timeline, slowest and failed/unresolved/retried views with 200-test pagination, read-token `sessionStorage` auth with sign-out, strict CSP/security headers, and bounded 10 s visibility-aware polling with exponential backoff.
- CLI reference (`docs/cli.md`) with bidirectional flag-drift tests, self-hosting guide (`docs/self-hosting.md`), minimal `scratch` `Dockerfile` for `bunpo-server`, cross-compiled `bunpo-server` release binaries with `scripts/smoke-server.sh` verification, and manual Cloudflare Free-plan trial runbook and scripts (`docs/trials/cloudflare-queue.md`, `worker/scripts/trial.mjs`, `.github/workflows/queue-trial.yml`).

## [v0.0.7] - 2026-11-23

### Changed

- Rename the GitHub Action display name in `action.yml` to `Setup Bunpo` for GitHub Marketplace.

### Fixed

- Publish releases as prereleases in `.github/workflows/release.yml` until cross-OS `check` jobs pass, then promote stable tags (`--prerelease=false --latest`) and move `v0`.
- Forward `github.token` from `action.yml` only on `https://github.com`, and restrict `@v0` major-tag API matching in `scripts/install.sh` to stable `vX.Y.Z` tags.
- Delete any leftover `report-<index>.json` at the start of `bunpo record` so a failed retry leaves no stale report behind for `merge`.
- Format large durations in `formatMS` with integer millisecond math to avoid `time.Duration` nanosecond overflow above ~292 years.
- Gate `actions/cache/save` on passing test jobs in CI and all five runner guides, widen `find` patterns in the pytest and Playwright guides to match runner defaults (including symlinks), and document the complete `bunpo plan` flow for concurrent pushes and re-runs.
- Verify that each shard's `report-<index>.json` covers its assigned files in `guidecheck`, and switch the Playwright guide from `--test-list` to case-sensitive anchored regex arguments (`/^\/abs\/path$/`) so filenames starting with `[` (such as Next.js dynamic routes) work without failing Playwright's test-list parser.

## [v0.0.6] - 2026-11-22

### Added

- Accept `-version` and `--version` flags alongside `bunpo version`.
- Live GitHub Actions trial report on LaTabdhir (`docs/trials/2026-11-22-souhib-github-actions-trial.md`).

### Changed

- Corrected Playwright JUnit retry timing behavior in `docs/guides/playwright.md` (Playwright sums attempt durations by default and records only the final passing attempt when `includeRetries: true`) and documented heterogeneous self-hosted runners and queue delays.
- Updated `README.md`, `docs/book.md`, and guides to reflect two-column timing files, current benchmark timings, and the live GitHub Actions trial.

## [v0.0.5] - 2026-11-22

### Added

- Support `-` as the second column in `--format lines` reports to mark a failed file as seen without recording a timing, matching JUnit report semantics.
- Full-cost inline Vitest reporter in `docs/guides/vitest.md` (recording environment setup, imports, collection, setup, and test duration via `mod.diagnostic()`, and `-` for failed modules) and monorepo, coverage-threshold, sidecar, and filtered-suite guidance across all five runner guides.
- Terminal recording (`docs/demo.gif`, `docs/demo.tape`) in `README.md` and local trial results on LaTabdhir and Majlisna (`docs/trials/2026-11-21-souhib-local-trial.md`).

### Fixed

- Reject `null` elements inside `timings.json` `samples` arrays instead of decoding them as `0 ms`.
- Clamp `junit` report `limitReader` reads at the byte limit so bytes past the cap cannot mask size-limit errors with syntax or line-count errors.
- Avoid creating a missing `--dir` directory when `bunpo split --plan` fails, and pluralize missing-machine messages in `bunpo merge`.
- Paginate the GitHub Releases API (up to 10 pages) and validate matched tags against the release-tag pattern during major-tag (`@v0`) installs in `scripts/install.sh`.
- Emit `-` for failed packages in `scripts/dogfood.sh` so failed packages stay seen without warnings.

### Changed

- Parse versioned JSON in a single pass on the happy path in `internal/jsonfile`, falling back to a header check only on error.
- Add a zero-allocation canonicalization fast path in `internal/split`, reuse the SHA-256 digest buffer, preallocate shards, remove duplicate key sorting in `timing.Decode`, and use stack buffers in `timing.Median` (cutting `BenchmarkSplit` on 50,000 tests from 252k to 403 allocations and ~49 ms to ~31 ms).

## [v0.0.4] - 2026-11-21

### Fixed

- Require nested JSON members in `timings.json` (`samples`, `last_seen`) and run reports (`timed`, `ms`), reject `null` shards in `plan.json`, and return zero values on decode validation errors.
- Reject non-canonical test identifiers in `timings.json` and `merge` reports.
- Flag out-of-range manifest indexes and sort missing test lists deterministically in `manifest.Verify`.
- Validate `--path-prefix` upfront in `bunpo record` (including on empty shards) and sort report map keys so errors are deterministic.
- Delete any leftover `plan.json` at the start of `bunpo plan` so a failed run never leaves a stale plan behind.
- Fix singular grammar (`1 of N tests has ... and counts`) and avoid printing `0ms` predictions when `timings.json` has no matching tests.
- Validate the Action's `version` input and `releases/latest` redirect against a release-tag pattern, and retry the `releases/latest` lookup.
- Check the `v0` major tag using GitHub's exact-match ref endpoint in the release workflow.
- Record passing packages and upload `.bunpo/` artifacts in dogfood CI even when a Go test fails.

### Changed

- Vitest guide now passes the test list through `BUNPO_TESTS_FILE=mine.txt` instead of an environment variable containing every path, avoiding Linux's 128 KiB per-variable limit on large suites.
- Renamed the GitHub Action from `Bunpō` to `Bunpo` in `action.yml` for ASCII searchability, and documented passing `with: version` when pinning the Action to a commit SHA.
- Updated `PROJECT.md` with a verified comparison against `split_tests` and `split-test`, and updated `docs/book.md` to match current behavior.

### Added

- Direct unit tests for `internal/jsonfile`, size and line-limit tests for two-column report reading, and regression tests for nested JSON validation.

## [v0.0.3] - 2026-11-21

First release with cross-compiled binaries, checksums, GitHub Action, runner guides, and benchmarks.

### Added

- Cross-compiled static binaries (`CGO_ENABLED=0`, `-trimpath`) with `SHA256SUMS` for Linux, macOS, and Windows on x86-64 and ARM64 (`scripts/build-release.sh` and `.github/workflows/release.yml`).
- Installing GitHub Action (`action.yml` and `scripts/install.sh`) that resolves exact tags, major tags (`@v0`), or `latest`, verifies `SHA256SUMS`, and puts `bunpo` on `PATH`.
- Complete GitHub Actions guides for pytest, Jest, Vitest, RSpec, and Playwright (`docs/guides/`), verified against real runners with `guidecheck`.
- Terminal label colors (`NO_COLOR`-aware, truecolor and 256-color) and GitHub Actions workflow annotations (`::warning::`, `::error::`, `::notice::`).
- Synthetic pytest benchmark suite (`benchmark/`) and published results (`docs/benchmark.md`), meeting the 1.10 predicted-load target (1.021 on the 40-file suite) alongside tight and indivisible-file cases.
- Dogfood CI (`scripts/dogfood.sh`) splitting Bunpō's own Go test packages across parallel runners and testing `action.yml` across Linux, macOS, and Windows.
- Architectural decision record for releases and the installing Action (`docs/decisions/0002-release-and-action.md`).

### Fixed

- Clear a machine's old `manifest-<index>.json` and `report-<index>.json` at the start of `bunpo split`, and reject empty or malformed test lists.
- Hash the downloaded binary from standard input in `scripts/install.sh` so checksum verification works on Windows paths.
- Test each published release through `action.yml` on Linux, macOS, and Windows before moving the `v0` major tag.

## [v0.0.2] - 2026-11-20

### Added

- Full command-line tool (`cmd/bunpo`): `split`, `plan`, `record`, `merge`, `verify`, and `version` with distinct exit codes (0 success, 1 failed check, 2 bad usage or input).
- Bounded JUnit XML and two-column (`--format lines`) report readers (`internal/junit`) using exact rational sums rounded half up once per file, with golden reports generated from real pytest, Jest, Vitest, RSpec, and Playwright suites plus fuzz testing.
- Versioned manifest, plan, and run report formats and the `manifest.Verify` checker (`internal/manifest`), plus a shared strict JSON helper (`internal/jsonfile`).
- Atomic file writes (temp file, `fsync`, rename) and directory confinement through `os.Root`.
- Multi-process end-to-end tests covering 8 parallel machines, stale timing caches, skipped runners, dominant files, empty shards, and timing carryover.
- Architectural decision record for file paths in test reports (`docs/decisions/0001-file-paths-in-test-reports.md`).

## [v0.0.1] - 2026-11-20

### Added

- Core split engine (`internal/split`): canonical repository-relative identifiers with POSIX/Windows path rules and collision detection, deterministic longest-first integer-only dealing with test-count tie-breaking, and SHA-256 split digests.
- Timing file engine (`internal/timing`): strict versioned JSON (`encoding/json/v2`), integer half-up median over the last 5 samples, order-independent `Merge` that keeps failed files seen without recording their time, and pruning after 30 unseen runs.
- Table-driven tests, 2,000-seed property tests, a brute-force optimal solver oracle verifying the 4/3 makespan bound, fuzzing for timing decoding, and a 50,000-test benchmark.
- Go 1.27 module with pinned `staticcheck` and `govulncheck` tools, GitHub Actions CI, specification (`PROJECT.md`), and developer guide (`docs/book.md`).

[v0.0.7]: https://github.com/nuggocto/bunpo/compare/v0.0.6...v0.0.7
[v0.0.6]: https://github.com/nuggocto/bunpo/compare/v0.0.5...v0.0.6
[v0.0.5]: https://github.com/nuggocto/bunpo/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/nuggocto/bunpo/compare/v0.0.3...v0.0.4
[v0.0.3]: https://github.com/nuggocto/bunpo/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/nuggocto/bunpo/compare/v0.0.1...v0.0.2
[v0.0.1]: https://github.com/nuggocto/bunpo/releases/tag/v0.0.1
