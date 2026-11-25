# 3. Live queue architecture and protocol

Date: 2026-11-23. Status: accepted.

Implementation status: the pure state machine (`internal/queue` and `worker/src/core`), HTTP wire protocol, client, and persistent CRC-32C disk event log (`internal/queueapi`) are implemented. So are the standalone server (`cmd/bunpo-server`, in-memory or persistent via `--data DIR`), `bunpo --queue` worker and `--status` commands, the Cloudflare Worker (`worker/`, SQLite `BuildDO` and `BuildIndex`), and the shared static web dashboard (`internal/dashboard/static/`).

## Context

Static splitting works well when CI machines are identical, start together, and run to completion. Mixed-speed runners (such as x64 alongside ARM64), queued runners that start late, and killed runners leave static shards unbalanced or incomplete.

The live queue hands out batches of tests as runners ask for work and reassigns tests when a runner dies or loses a completion report. Each logical test gets at most one accepted terminal result under its current lease. Duplicate submissions are idempotent, stale results stay visible in attempt history, and bounded retry exhaustion leaves the build unresolved rather than passing.

Two servers speak the same HTTP JSON protocol: a standalone Go binary (`bunpo-server`, in memory or persisted to disk with `--data DIR`) for teams not on Cloudflare, and a Cloudflare Worker with one SQLite-backed Durable Object per build (zero-ops, scaling to zero between CI runs). Both serve the same static dashboard bundle.

## Decision

### Event-sourced state machine

`internal/queue` separates deciding from applying:

```text
Decide(state, now, command) -> (events, response)   // pure; never mutates state
Apply(state, event)         -> error                // pure; never reads a clock
```

Only six event types are persisted (`initialized`, `conflicted`, `leased`, `reported`, `expired`, `finalized`). Replay applies persisted events without evaluating time, so a server restart or Durable Object wake cannot make a different decision than the live server made.

Every mutating handler runs under the build's serialization boundary in strict order: `Decide`, persist events (`fsync` on the Go journal, SQLite transaction in the Durable Object), `Apply`, reply. If persistence fails, the server returns `503` and leaves in-memory state untouched.

Read handlers (`GET`) write nothing unless `Decide` at `now` changes the build verdict from `running` to terminal (via build deadline or final-attempt lease expiry). In that single case the handler persists the finalizing event before returning the terminal verdict, so a terminal verdict never reverts across a restart or clock step.

### Heartbeats and wake deadlines

Heartbeats are not persisted as events in the event log. A healthy heartbeat updates the active lease's in-memory deadline (`deadline = max(deadline, now + ttl)`). Recovery after a restart or hibernation differs by runtime:

- **Go server (`bunpo-server --data DIR`):** healthy heartbeats do zero disk `fsync`s. After a process restart, every active lease resets its in-memory deadline to `max(deadline, now + ttl)`. That can delay reassignment by at most one TTL across a restart and cannot lose or double-accept a result.
- **Cloudflare Worker (`BuildDO`):** Cloudflare hibernates idle Durable Objects after roughly 10 s. Resetting active leases to `now + ttl` on every wake would keep a dead runner's lease alive until `build_timeout_ms` whenever another runner polls more than 10 s apart. Instead, `BuildDO` tracks each active lease's real (ungraced) `base` deadline in a pure `LeaseDeadlineTracker` (`worker/src/core/deadlines.ts`). It writes all active leases' `base` deadlines into one `meta` row (`lease_deadlines`) only when a heartbeat or repeated-request lease replay pushes `base[L]` past `persisted[L] + floor(ttl / 2)`. On wake, each active lease gets `persisted_or_granted[L] + floor(ttl / 2)`, never persisting the graced value. A lease never expires before a promised deadline, a dead runner's lease expires within `floor(ttl / 2)` past its last real deadline regardless of wake count, and a 64-machine 10-minute build triggers 25 to 27 `meta` row updates (1,920 heartbeats in `worker/test/deadlines.test.ts` trigger at most 30).

### First-class leases and bounded state

A lease is a first-class record (`lease_id` monotonic per build, `runner_id`, optional `machine_index`, `deadline_ms`, and its attempt list).

- Attempt IDs are deterministic: `<test_index>.<generation>`.
- `heartbeat` and `report` identify a `lease_id`. Reports are all-or-nothing: a report must name every attempt in its lease exactly once, or the server rejects it with `400`.
- Each runner process generates a random 16-byte hex `runner_id`; `--index` is optional display metadata so a restarted CI job does not collide with a partitioned predecessor.
- The server remembers only the most recent `(request_id, lease_id)` per `runner_id` (and never remembers `wait` or `done`). Repeating `request_id` while its lease is still active extends and returns that lease; repeating it after expiry or completion returns `409 stale_request`. A runner holding an active lease that calls `lease` with a fresh `request_id` receives `409 lease_held`, guaranteeing at most one active lease per runner.
- Lazy expiry scans only active leases (ascending `lease_id`, attempts in ascending `test_index`), never the full test list.

### Init conflicts, bounded events, and verdict precedence

The first `init` fixes the canonical test list, predictions, and config. Later `init` calls check a SHA-256 digest of the test list and config (excluding predictions, so runners with different timing caches agree). A mismatch returns `409` and persists a `conflicted` event in **any** build state (including after the build finished, so a late runner with a different test list still invalidates a `passed` verdict) unless an identical conflict is already stored or the 8-conflict list is full.

To keep the event log strictly bounded under hostile repeats, identical stale/conflict reports and identical `init` conflicts emit no events, and once an 8-entry list is filled its boolean `truncated` flag is set and further entries emit nothing. `reported` events omit `duplicate` results, and `Apply` validates every event completely before mutating state so a damaged journal fails loudly.

Verdict precedence: `running` while any test is `pending` or `leased`; else `failed` if any test failed; else `unresolved` if any test is `unresolved` or the build recorded an `init` conflict; else `passed`. A finished verdict may change in only one way: `passed -> unresolved` when a late `init` conflict arrives; never upward and never otherwise.

### Guided self-scheduling batching

Pending tests are dealt longest predicted duration first, breaking ties by lower `test_index` (byte-order path). Unknown predictions use the integer median of known predictions (or `0` when none are known). First attempts (`generation == 0`) come before retries (`generation > 0`).

Each lease takes tests in deal order until the batch holds at least `ceil(remaining_ms / (2 * machines))` milliseconds (or `ceil(remaining_pending / (2 * machines))` tests when `remaining_ms == 0`), clamped to `[1, max_batch]` (default 200). A test with `generation > 0` is always leased alone and skipped while packing first-attempt batches, so a file that kills its runner cannot take healthy siblings down with it.

### Client outcomes and process control

`internal/junit` adds `Skipped` (cases with a direct `<skipped>` child) to `junit.File` without changing `bunpo record`. In queue mode:

- A file with a failed case reports `failed`.
- On exit 0, a file with cases not all skipped reports `passed` (timed); all cases skipped reports `skipped`; absent reports `error`.
- On a normal (non-signaled) non-zero exit with a JUnit report where another file in the same lease failed, a file with cases and zero skipped cases reports `passed` (untimed, because the runner may have stopped early). Because the failed sibling is in the same all-or-nothing report, this cannot fabricate a passing build, and leaving it untimed keeps truncated durations out of `timings.json`.
- Everything else (signaled exit, missing/invalid report, `--format lines` on non-zero exit) reports `error`.
- Any file in the report outside the lease causes the whole batch to report `error` and the client to exit `1`.

On Unix, the client runs the test command in its own process group and signals the group (`SIGTERM`, then `SIGKILL` after 5 s) on `SIGINT`/`SIGTERM` or when a heartbeat reports the lease is lost.

To keep runner and status behavior deterministic and testable under fault injection, `internal/queuerunner` separates pure sans-I/O state machines (`Machine` and `StatusMachine`, advancing via `Step(nowMS, event) -> []Action` with `OpID`/`TimerGen` fencing and an `Entropy` interface) from a thin OS/HTTP driver, and `internal/queueapi.Client` performs single-shot HTTP requests without internal retry loops. `internal/sim` runs the real machines, wire encoders, `queueapi.Handler`, and `queueapi.DiskLog` in a single-goroutine seeded simulator with an independent oracle.

### Standalone server disk journal (`--data DIR`)

`bunpo-server --data DIR` persists one file per build inside `DIR` (`0700` directory, `0600` files, opened through `os.Root`), named by the lowercase hex SHA-256 of the build ID so case-insensitive filesystems and legal punctuation never collide. The server binds `--addr` before touching `--data`, loads existing logs, and only then serves requests. Operators must run at most one server per directory.

Each file starts with `bunpo-queue-log 1\n`, followed by one line per `Append`: `<8-hex-crc32c> <deterministic-json-array-of-events>\n` (bounded at `8 MiB`). One `Append` is one framed record, so multi-event decisions replay all-or-nothing. On decode, re-encoding the parsed events must reproduce the exact payload bytes.

- **Per-decision `fsync`:** Every `Append` opens the file, checks that its size matches the expected offset (catching a stray second server or outside edit by breaking that build instead of mixing two histories), writes, `fsync`s, and closes (`O_EXCL` plus directory `fsync` on create). A write error truncates back to the old size and `fsync`s (returning `503 storage_error` only if both succeed); a failed `fsync` is always uncertain (`500` until restart). The tradeoff is one `fsync` latency per mutating decision (typically a few milliseconds on local SSDs) in exchange for never acknowledging a decision that power loss can erase.
- **Torn tail vs. damage on load:** A file with zero complete records had its `init` never answered and is removed. An invalid final record at real `EOF` is treated as a torn tail from an uncompleted write: logged, truncated to the last valid offset, and `fsync`ed. Any I/O read error, wrong header version, invalid record followed by more bytes, build-ID mismatch, or `Apply` error is treated as damage: the file is left untouched on disk, the server logs the error, that build answers `500`, and all other builds load normally. Every accepted file and the directory are `fsync`ed once before serving so bytes that survived a process kill in the OS page cache cannot vanish on a later power loss after the server answers from them.
- **Eviction and deletion:** Retention and capacity eviction delete non-broken files before allowing a build ID to be reused; a failed `Remove` keeps the build entry so two histories cannot collide. Evicted broken builds leave their file untouched on disk and remain quarantined (`500`).

### Cloudflare Worker topology and measurements

One SQLite-backed Durable Object (`BuildDO`) owns each build. SQLite tables are created only when `init` actually persists, so `GET` on an unknown build ID writes zero tables and zero rows. `BuildDO` stores the immutable test catalog in 1,000-test chunks (`catalog_chunks`, `INTEGER PRIMARY KEY`, ~68 KB/row, well under SQLite's 100 KB statement and 8 MB row limits). Each `Append` writes one row to `event_log` (`INTEGER PRIMARY KEY`, no `AUTOINCREMENT`, no extra index). Throttled active-lease deadlines (`lease_deadlines`) and the last confirmed index state (`index_synced`) live in `meta`.

The Worker checks bearer tokens in constant time over SHA-256 digests before waking any Durable Object, and streams and bounds request bodies while reading. It logs structured JSON (`build_id`, `method`, error name and message, never tokens or bodies) on DO errors, and forwards raw JSON text so bodies are parsed once inside `BuildDO` using `BuildDO`'s own clock.

Each `BuildDO` runs `transactionSync` per write and aborts via `ctx.abort()` on storage error so memory cannot diverge from SQLite. During wake replay it skips redundant per-path validation and SHA-256 re-hashing on its own SQLite `initialized` row, while always replaying with the protocol 20,000-test limit. `MAX_TESTS` applies only to new `init` calls, so lowering `MAX_TESTS` never breaks replay of existing builds. Lifecycle alarms are scheduled in the same synchronous turn as the SQLite write (`build_deadline` while running, +7 days once terminal to call `ctx.storage.deleteAll()` and reset memory), and any missing alarm is restored on wake.

A separate SQLite-backed `BuildIndex` Durable Object holds up to 200 recent build summaries with `expires_at_ms` and lazy pruning, mutating its in-memory map only after `transactionSync` commits. It receives at most two inline RPCs per build (on `init` and on terminal verdict or first conflict). `BuildDO` records the last confirmed `{ verdict, has_conflicts }` in `meta` (`index_synced`) only after `BuildIndex.upsert` succeeds. If an index update fails, later requests compare current state against `index_synced` and schedule a non-blocking `ctx.waitUntil` repair at most once every 60 seconds (or on wake or alarm), never delaying replies.

The Worker implements the same pure state machine in TypeScript (`worker/src/core`) using native `node:crypto` (`nodejs_compat`, `createHash("sha256")`) for the build digest. It passes the shared JSON test vectors, Go simulator differential traces (well-behaved and hostile), `LeaseDeadlineTracker` property tests, and seeded DO eviction tests (including eviction after every request with dead runners). It also passes the Go black-box HTTP protocol and `bunpo --queue` E2E suites against local `workerd` with real 10 s DO hibernation.

#### Local storage and request measurements (10-minute build, 60-byte test paths, 60 s lease TTL)

Storage rows, `lease_deadlines` meta updates, and database sizes below came from a one-off local measurement inside `workerd` (`cursor.rowsWritten`, `databaseSize`) on the production `BuildDO`; the script used to collect them is not in the repository. Lifecycle rows written include `catalog_chunks`, `event_log`, `meta` (`lease_deadlines` and `index_synced`), schema, `setAlarm`, `BuildIndex`, and 7-day `deleteAll()`.

Per [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), "Durable Objects that are idle and eligible for hibernation are not billed for duration, even before the runtime has hibernated them." The `Active GB-s` column below is therefore a conservative upper bound that counts 128 MB (0.125 GB) across up to 10 s of idle time per inter-request gap plus a 10 s tail (`489 to 531 s`, `61.2 to 66.4 GB-s`, or `610 s x 0.125 GB = 76.25 GB-s` continuous). Actual billed duration for hibernating objects is much smaller: in the real Free-plan trial, all 144 recorded Durable Object invocations totaled 7.9 s of wall time. `BuildIndex` requests and active duration are extra (typically two requests per build, on `init` and finish).

| Tests | Machines | DO Reqs | `lease_deadlines` Writes | Data / Lifecycle Rows Written | SQLite Size | Conservative GB-s Upper Bound | Free Builds/Day (First Limit Hit) |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000 | 8 | 417 | 23 | 259 / 467 | 312 KiB | 64.2 | 214 (rows written/day; 239 DO reqs/day) |
| 1,000 | 64 | 2,934 | 27 | 1,021 / 1,983 | 388 KiB | 64.9 | 34 (DO reqs/day) |
| 5,000 | 8 | 472 | 23 | 319 / 587 | 1.19 MiB | 64.2 | 170 (rows written/day) |
| 5,000 | 64 | 3,251 | 25 | 1,565 / 3,075 | 1.48 MiB | 66.3 | 30 (DO reqs/day) |
| 10,000 | 8 | 509 | 22 | 358 / 667 | 2.28 MiB | 62.1 | 149 (rows written/day) |
| 10,000 | 64 | 3,419 | 25 | 1,784 / 3,513 | 2.78 MiB | 66.3 | 28 (rows written/day) |
| 20,000 | 8 | 573 | 16 | 442 / 847 | 4.48 MiB | 61.2 | 118 (rows written/day) |
| 20,000 | 64 | 3,656 | 25 | 1,996 / 3,937 | 5.13 MiB | 66.4 | 25 (rows written/day) |

- **Worker `MAX_TESTS` default (5,000) and real CPU data:** The protocol and Go server keep 20,000. On the Worker, `MAX_TESTS` in `worker/wrangler.jsonc` defaults to 5,000 (valid range 1 to 20,000; invalid values fail closed with 500 on every request). Earlier local Node CPU estimates for 5,000-test `init` (2.02 to 3.80 ms) were about 5 to 14x lower than real Cloudflare Durable Object CPU and have been removed. Cloudflare's pages conflict on Free-plan Durable Object CPU: the [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) table lists 30 seconds default with no Free value, while its FAQ says "Durable Objects are Worker scripts, and have the same per invocation CPU limits as any Workers do," and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) gives 10 ms on Free. Across two trials on a real Free-plan account ([docs/trials/2026-11-25-cloudflare-free-trial.md](../trials/2026-11-25-cloudflare-free-trial.md)), 21 builds of 20,000 files were created and answered every request and 130 Durable Object invocations exceeded 10 ms (up to 73 ms) with none stopped. A strictly enforced 10 ms DO limit would hit 1,000 files too (`init` took 12 to 14 ms); if enforced, the DO throws, the Worker returns 503 `storage_error`, and runners retry for 2 minutes before exiting 1, never producing a false pass. The 10 ms Workers Free limit does apply to Worker `fetch`, which reached 8 ms with 20,000-file (1.04 MB) `init` bodies (bodies near 4 MiB were not tested). `MAX_TESTS` stays at 5,000 by default for Worker `fetch` CPU margin on larger bodies and daily row writes, not because of an observed Durable Object CPU limit.
- **Observability sampling:** With both logs and traces at 100%, the trial account recorded 1,841 of 200,000 observability events for 183 Worker requests (about 10 events/request, which would hit the 200,000-event daily cap around 20,000 requests, or 35 to 48 8-machine builds/day; from 1 December 2026 Cloudflare replaces the event cap with 0.5 GB/day shared by logs and traces). `worker/wrangler.jsonc` sets `observability.traces.head_sampling_rate` to `0.05` while keeping logs at 100% so `do_rpc_error` entries are never sampled away.
- **64-machine heartbeats:** At 64 machines and the default 60 s `--lease-ttl` (heartbeats every 20 s), heartbeats are roughly half of all Worker and DO requests while causing only 25 to 27 `lease_deadlines` meta updates over a 10-minute build. Passing a longer `--lease-ttl` (up to 10m) cuts heartbeat volume proportionally.

### Shared static dashboard

A single static folder (`internal/dashboard/static/`, plain HTML, CSS, ES modules, and SVG favicon) serves both backends with no framework, build step, external requests, or protocol changes:

- **Go (`bunpo-server`):** `internal/dashboard` embeds the 5 static files (41,474 bytes raw) via `//go:embed` and wraps the queue API handler.
- **Cloudflare Worker (`worker/`):** `worker/wrangler.jsonc` configures Workers Static Assets (`directory: "../internal/dashboard/static"`, `not_found_handling: "none"`, `run_worker_first: ["/api/*", "/healthz"]`). Static asset requests (`/`, `/dashboard/*`) are served directly by Cloudflare's Static Assets layer using `_headers` at 0 Worker requests and 0 Durable Object wakes. Deep links (`/builds/<valid-build-id>`) miss Static Assets and fall through to the Worker, which serves `index.html` through `env.ASSETS.fetch` with matching security headers (1 Worker request, 0 DO wakes) because `_headers` does not apply to Worker-proxied responses. Because Cloudflare `_headers` appends values across matching patterns, `/*` carries only the shared security headers while `/`, `/index.html`, and `/dashboard/*` set `Cache-Control` separately without overlap. Unknown paths, directory requests, `_headers`, and traversal return the API's JSON 404.
- **Auth, timeline scope, and polling cost:** The browser stores only `BUNPO_QUEUE_READ_TOKEN` in `sessionStorage`, sends it via `Authorization: Bearer` (never in cookies or URLs), clears token and DOM on sign-out, and renders all server strings with `textContent` and DOM APIs (zero `innerHTML`, zero inline scripts or styles) under a strict CSP (`default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`). The per-machine timeline covers the tests loaded so far (starting with the first 200 tests, extendable in 200-test pages), not the whole build. A pure polling state machine (`pollStep` in `core.js`) polls only while viewing a `running` build in a visible tab, at most once every 10 s, stopping on terminal verdicts or non-retryable 4xx errors and backing off exponentially on network errors, 429, and 5xx (honoring `Retry-After`) up to 5 failures; a successful manual refresh restarts polling while the build is still running. An open visible tab on a running build sends 360 requests/hour (`GET /api/v1/builds/{build}`) when state is unchanged, up to 1,080 requests/hour when `last_seen_ms` changes on every 10 s tick (`status` plus page 1 of timeline and page 1 of filtered tests; refresh merges page 1 without collapsing extra loaded pages), and 0 requests/hour once finished, hidden, or on the builds list.

## Consequences

- Measured on local `workerd`, a 20,000-test build writes 442 SQLite data rows (847 lifecycle rows including 7-day `deleteAll()`) at 8 machines and 1,996 data rows (3,937 lifecycle rows) at 64 machines, instead of roughly 120,000. Heartbeats share a single `meta` row updated at most once per `floor(ttl / 2)` window (16 to 27 updates per 10-minute build).
- With `--data DIR` (and in the Durable Object backend), restarting `bunpo-server` cannot change past decisions or accept a stale result, at the cost of one `fsync` per mutating decision and delaying expiry of in-flight leases by up to one TTL. Without `--data`, `bunpo-server` stays in memory only: a restart loses running builds, runners receive 404 and exit 1, and nothing can pass across a restart.
- Both Go and TypeScript maintain a copy of the small state machine; shared deterministic JSON vectors catch behavioral drift before deployment.
- Runners using `--bail`, `-x`, or `--max-failures` still fail the build safely (the failing file marks the build `failed`), though unrun files in that batch report as `error` (and may be retried) while clean siblings that finished before stopping report `passed` untimed.
- The standalone Go server caps retained builds (200 total, 64 running) and the client rejects plain `http://` outside loopback unless `--allow-insecure-http` is passed.

## Alternatives considered

- **One SQLite row per test with per-test heartbeat updates and full-table lazy scans.** Breaks Cloudflare Workers Free limits immediately:
  - [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) limits Workers Free to **100,000 SQLite rows written/day**, **5 million rows read/day**, and **5 GB** total storage (deletes and `setAlarm()` calls also count as row writes). One 20,000-test build writes ~120,000 rows (`init` + `lease` + `report` + attempts + cleanup delete). Updating 200 test rows per heartbeat across 256 runners burns 102,400 row writes in two rounds (40 s). Scanning 20,000 test rows per request exhausts 5 million row reads/day in seconds.
  - [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) caps SQL statements at **100 KB** and **100 bound parameters per query**, which prevents single-query updates over `max_batch = 200` and forces chunked `init` inserts anyway.
- **Storing commands (including heartbeats) instead of events.** Forces replay to re-evaluate timestamps against leases (risking divergent decisions if clocks or expiry rules ever shift) and appends hundreds of thousands of heartbeat lines per build to disk and SQLite.
- **No Durable Object alarms.** Violates 7-day retention: idle objects never run `ctx.storage.deleteAll()` and leak toward the 5 GB Free-plan storage cap (and accrue Paid storage charges indefinitely), while builds whose runners all died stay `running` forever in `GET /api/v1/builds`. Per [Durable Objects alarms](https://developers.cloudflare.com/durable-objects/api/alarms/), each object supports one alarm with at-least-once delivery (up to 6 retries, 2 s exponential backoff, 15 min max wall time), `setAlarm()` costs 1 row written, and `deleteAll()` removes any pending alarm on compatibility dates `>= 2026-02-24`.
- **8 MiB `init` body, 1 s minimum TTL, and duplicate-key JSON rejection in the Worker.**
  - An 8 MiB (`8,388,608` B) body exceeds SQLite's **8 MB** (`8,000,000` B) max string/BLOB/row limit ([Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)) if stored intact, and concurrent 8 MiB payloads risk OOMing the shared **128 MB** V8 isolate ([Workers limits](https://developers.cloudflare.com/workers/platform/limits/)). Incoming HTTP requests allow 100 MB on Free/Pro zones, so the bottleneck is isolate memory, SQLite row size, and CPU.
  - Workers Free caps CPU time at **10 ms per request** (Paid allows 30 s default, up to 5 min; [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)). Running a custom JS JSON parser to detect duplicate keys over an 8 MiB payload breaks the 10 ms budget. Capping `init` at 4 MiB, sending a parallel `predictions_ms` array instead of repeating path keys, and keeping duplicate-key rejection on local `.bunpo/*.json` files (via Go `encoding/json/v2`) keeps the wire contract simple and fast.
  - A 1 s minimum TTL with 256 runners sends 768 heartbeats/s (near the **1,000 req/s** soft limit per DO) and exhausts the **100,000 req/day** Free limit in 130 s. Raising the minimum TTL to 10 s (configurable in constructors for fast tests) avoids self-inflicted request storms.
- **One file per lease.** Simpler client outcome logic, but pays 1–10 s of runner startup (`pytest`, Jest, Vitest, Playwright browser launch) on every file and multiplies HTTP requests and storage writes. Users who want one file per lease can set `--max-batch 1`.
- **Retrying `failed` tests in the queue.** Mixes infrastructure retries with test-runner flaky retries, can hide regressions, and triples CI time on broken pull requests. Flaky retries remain the test runner's job.
- **Compiling Go to WebAssembly for Cloudflare Workers.** Adds bundle size, cold-start cost, and JS/SQLite bridge code for a state machine small enough to verify identically with shared JSON vectors.
- **Suite-level `<failure>`/`<error>` XML parsing.** Unnecessary: a suite/teardown failure causes a non-zero command exit, which already marks the batch `error` when no test case failed, or leaves passing siblings untimed alongside the failed sibling.
