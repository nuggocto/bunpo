## Skills

- Use the `go` skill for project work.
- Use the `unslop` skill for all writing.
- Use the `tiger-style` skill when designing, implementing, refactoring,
  or reviewing code. Apply it within the requested scope and follow the
  project's language conventions. Do not use it to justify unrelated
  rewrites or speculative complexity.
- Use the `test-quality` skill when writing or reviewing tests.
- Use the `qa` skill when verifying user-visible behavior, packaged artifacts,
  supported platforms, or a release candidate.

## Project

- `PROJECT.md` is the specification. Its "Core rules" are requirements, not
  suggestions. Read the relevant section before changing behavior.
- Limits and safeguards already listed there (report size bounds, `os.Root`,
  path-collision checks) are approved scope, not speculative complexity.
- Go code uses the standard library only. The Cloudflare Worker in `worker/`
  has no runtime dependencies; its dev tools are pinned in
  `worker/package.json` and `worker/package-lock.json`. Do not add a
  dependency without asking.
- Change `PROJECT.md` only when the user agrees to change the plan.

## Branches, commits and PRs

- `shrek` is the permanent default and production branch. Never rename or replace it.
- If no PR is requested, work directly on `shrek`.
- `username` below means the contributor's GitHub username, such as `alice-donkey`.
- If a PR is requested, create a task branch from `username-donkey` (create it
  from `shrek` if it does not exist) and target `shrek`. Check that the PR includes no unrelated commits.
- Use `username-puss-in-boots` only when the user explicitly requests an experiment.
- Do not commit, push, open, or merge a PR unless requested.
- Never refer to work by its roadmap label ("Phase 2", "a 0.2.0 item") in
  commit messages, PR titles or descriptions, or code comments. Roadmap
  labels are planning labels in `PROJECT.md` and mean nothing to someone
  reading the history or the code. Describe the change or the missing
  behavior instead. Release versions are fine in release commits and the
  changelog.
- Use conventional commit messages and PR titles: `fix: summary` or `fix(scope): summary`. Valid types are `feat`, `fix`, `docs`, `chore`, `refactor`, and `test`. Use the affected package or area as the scope when helpful.

## General Principles

- If the repo uses `docs/book.md`, update it when a completed change alters
  behavior, architecture, setup, or operation. Explain significant changes and tradeoffs. Use `explaining-code` when useful and `show-me` when a diagram would help.
- Skip routine tool use, dead ends, and trivial edits in the write-up. Report
  failed checks that affect confidence in the result.
- Add a decision record in `docs/` for significant architectural decisions.
- Preserve user work and avoid unrelated edits.
- Prefer straightforward code and avoid premature abstractions.
- Extract functions when they clarify responsibility, isolate side effects,
  or remove meaningful duplication.
- Do not split code merely to reduce function length.
- Write comments that explain why the code does something non-obvious,
  especially when a constraint or workaround would be easy to miss.
  Don't repeat what the code already shows. Never mention roadmap labels in
  comments.
- Before adding complexity for a speculative or vanishingly unlikely race or security edge case, explain the concrete failure mode, likelihood, and complexity cost to the user and get their buy-in. Do not silently expand scope for theoretical robustness.
- Stop every server, app, and display you start before you finish. Never
  detach them (`setsid`, `nohup`, `disown`) so they outlive your session. If
  one stops while you work, including because the user stopped it, ask before
  starting it again. Use the kick cli for handling all of that.

## Testing

- Only add a test if its failure would tell you something is actually broken.
  Assertions on styling values, colors, or internal structure fail on harmless
  changes and pass on real bugs, so leave them out.
- Test actual implementation, do not duplicate logic into tests.
  An independent reference oracle, such as the brute-force optimal solver
  in PROJECT.md, is not duplication.

## Deterministic Simulation Testing (DST)

- Prefer pure, deterministic core logic.
- Abstract all I/O, time, network, randomness, and scheduling behind interfaces.
- Make the real code runnable inside a seeded simulator.
- Design so any bug found with a seed can be perfectly replayed.

## Project workflow

- Toolchain: Go 1.27, pinned by the `toolchain` directive in `go.mod`.
  Release builds set `GOTOOLCHAIN` to that exact version.
- Install dependencies: `go mod download`
- Format: `gofmt -l .` (must print nothing)
- Lint: `go vet ./...` and `go tool staticcheck ./...`
- Run tests: `go test -race ./...`
- Vulnerabilities: `go tool govulncheck ./...`
- Build: `go build -o bin/bunpo ./cmd/bunpo` and
  `go build -o bin/bunpo-server ./cmd/bunpo-server`
- Worker (`worker/`, Node pinned in CI): `npm ci --ignore-scripts`, then
  `npm run typecheck`, `npm run test:core` and `npm run test:do`;
  `worker/scripts/test-blackbox.sh` runs the protocol and `bunpo --queue`
  end-to-end tests against a local Worker.
- Release build: `scripts/build-release.sh VERSION OUTDIR` (12 binaries and
  `SHA256SUMS`).
- After every stable release, update `src/data/releases.ts` in `bunpo-site`:
  `LATEST_VERSION`, the release entry, and the six `bunpo` CLI hashes copied
  from the published `SHA256SUMS`. `bunpo.org/install.sh` and
  `bunpo.org/install.ps1` pin those hashes, so until the site is updated they
  keep installing the previous release.

- Run the checks relevant to the change.
- Report which checks passed, failed, or were not run.
- Do not claim something works unless it was verified.
