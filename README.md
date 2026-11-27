# Bunpō 分蜂

Bunpō splits a test suite across parallel CI machines by how long each test
file took last time, and checks that every file was assigned exactly once.

Split test files by name and one machine gets the slow ones while the others
sit idle. Bunpō remembers each file's time and deals them out longest first,
so every machine finishes at about the same moment. Static mode is one binary
with no dependencies and no server; timings live in your own CI cache. An
optional self-hosted live queue handles runners that differ in speed, start
late, or get killed.

![Terminal recording of bunpo split, record, merge, and verify catching a stale timing cache](docs/demo.gif)

The test list goes to standard output, ready for your test command; messages
go to standard error. In GitHub Actions, warnings and failures show up as
annotations on the run and the pull request.

## Get started

Follow the guide for your test runner. Each one is a complete GitHub Actions
workflow:

- [pytest](docs/guides/pytest.md)
- [Jest](docs/guides/jest.md)
- [Vitest](docs/guides/vitest.md)
- [RSpec](docs/guides/rspec.md)
- [Playwright](docs/guides/playwright.md)

In GitHub Actions, one step installs Bunpō:

```yaml
- uses: nuggocto/bunpo@v0
```

On Linux and macOS, install with one line:

```sh
curl -fsSL https://bunpo.org/install.sh | sh
```

See [bunpo.org/install](https://bunpo.org/install) for Windows, pinned versions,
and manual download from [releases](https://github.com/nuggocto/bunpo/releases)
with `SHA256SUMS`. Releases start at `v0.0.3` and cover Linux, macOS and Windows
on x86-64 and ARM64.

The CLI has no GitHub Actions dependency and works on GitLab CI, Woodpecker CI,
Forgejo/Gitea Actions, Buildkite, or any CI that runs a static binary: pass
0-based `--index` and `--total` to `bunpo split`, pass `.bunpo/` files to a
final job for `bunpo verify` and `bunpo merge`, and cache `.bunpo/timings.json`
on your default branch.

## How it works

1. **`bunpo split`** on each machine reads the list of test files and prints
   that machine's share. The same list and timings give the same split on
   every machine.
2. Your test runner runs those files and writes a JUnit report (or a
   two-column timing file).
3. **`bunpo record`** turns the report into that machine's timings.
4. A final job runs **`bunpo verify`**, which checks that every machine split
   the same input and that each file was assigned exactly once, and
   **`bunpo merge`**, which folds the timings into the file your CI caches for
   next time.

`verify` checks what each machine was asked to run. Your workflow still checks
that each runner succeeded; the guides do both.

On a declared benchmark of 40 files, the slowest machine's predicted load was
within 2.1% of the fastest; [docs/benchmark.md](docs/benchmark.md) has the
numbers, including a suite where one file is too large to balance. On a 113-file
pytest suite, four machines finished in 2m 08s instead of 4m 15s with a count
split (2m 13s on a second run). That was the best of seven suites; some gained
nothing. [docs/trials/](docs/trials/) has the measured runs.

## Live queue

`bunpo --queue` pulls batches of test files from a self-hosted queue server as
each machine finishes, and reassigns tests when a machine is killed or loses a
report. Two servers speak the same HTTP protocol and serve the same read-only
web dashboard: a Cloudflare Worker (`worker/`, SQLite-backed Durable Objects)
and a standalone Go binary (`bunpo-server`, in memory or persisted with
`--data`).

If your CI machines are identical and start together (or on fork pull requests
without repository secrets), stick with `bunpo split` and `bunpo verify`.
Dynamic batching starts the test runner once per batch instead of once per
machine. Use `--queue` when runners differ in speed (such as x64 alongside
ARM64), start minutes apart behind CI queues, or can be killed mid-run.

See [docs/cli.md](docs/cli.md), [docs/self-hosting.md](docs/self-hosting.md),
and [docs/queue-protocol.md](docs/queue-protocol.md) for setup and limits.

## Documentation

- [bunpo.org](https://bunpo.org): guides, live-queue replay, benchmarks, install instructions, and changelog.
- [docs/cli.md](docs/cli.md): CLI reference for `bunpo` and `bunpo-server`, exit codes, env vars, and `.bunpo/` files.
- [docs/self-hosting.md](docs/self-hosting.md): deploying the Cloudflare Worker or `bunpo-server` behind TLS.
- [docs/queue-protocol.md](docs/queue-protocol.md): live-queue HTTP protocol and state-machine specification.
- [docs/guides/](docs/guides/): setup for each test runner (static and live queue).
- [docs/book.md](docs/book.md): how the code works, every command, file and
  rule.
- [docs/benchmark.md](docs/benchmark.md): synthetic benchmark results and 50,000-test CLI timing.
- [docs/trials/](docs/trials/): local and GitHub Actions trials on real suites.
- [CHANGELOG.md](CHANGELOG.md): release history.
- [SECURITY.md](SECURITY.md): how to report a vulnerability privately.
- [PROJECT.md](PROJECT.md): the specification and the roadmap.

## Development

Bunpō needs Go 1.21 or later installed. Go downloads the exact toolchain named
in `go.mod` on first use.

```sh
gofmt -l .                  # must print nothing
go vet ./...
go tool staticcheck ./...
go test -race ./...
go tool govulncheck ./...
go build -o bin/bunpo ./cmd/bunpo
go build -o bin/bunpo-server ./cmd/bunpo-server
```

The Cloudflare Worker in `worker/` has its own checks:

```sh
(cd worker && npm ci --ignore-scripts && npm run typecheck && npm run test:core && npm run test:do)
./worker/scripts/test-blackbox.sh
```

Licensed under Apache-2.0.
