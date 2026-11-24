# 3. Live queue architecture and protocol

Date: 2026-11-23. Status: accepted.

Implementation status: the pure state machine (`internal/queue` and `worker/src/core`), HTTP wire protocol, client, and persistent CRC-32C disk event log (`internal/queueapi`), standalone server (`cmd/bunpo-server`, in-memory or persistent via `--data DIR`), `bunpo --queue` worker and `--status` commands, and the Cloudflare Worker (`worker/`, SQLite `BuildDO` and `BuildIndex`) are implemented. Only the dashboard remains planned.

## Context

Static splitting works well when CI machines are identical, start together, and run to completion. Mixed-speed runners (such as x64 alongside ARM64), queued runners that start late, and killed runners leave static shards unbalanced or incomplete.

The live queue hands out batches of tests as runners ask for work, reassigns tests when a runner dies or loses a completion report, and enforces strict correctness: every logical test gets at most one accepted terminal result under the current lease, duplicate submissions are idempotent, stale results stay visible in attempt history, and bounded retry exhaustion leaves the build unresolved rather than passing.

Two servers will speak the same HTTP JSON protocol: a standalone Go binary (`bunpo-server`, running in memory or persisted to disk with `--data DIR`) for teams not on Cloudflare, and a planned Cloudflare Worker with one SQLite-backed Durable Object per build (zero-ops, scales to zero between CI runs). Both are designed to serve the same static dashboard bundle.

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

### Heartbeats in memory only

Heartbeats are not persisted events. A healthy heartbeat updates the active lease's deadline in memory only (`deadline = max(deadline, now + ttl)`), costing zero disk `fsync`s and zero SQLite row writes. After a server restart or Durable Object eviction, every active lease resets its in-memory deadline to `now + ttl`. That can delay reassignment by at most one TTL, and it cannot lose or double-accept a result.

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

Each lease takes tests in deal order until the batch holds at least `ceil(remaining_ms / (2 * machines))` milliseconds—or `ceil(remaining_pending / (2 * machines))` tests when `remaining_ms == 0`—clamped to `[1, max_batch]` (default 200). A test with `generation > 0` is always leased alone and skipped while packing first-attempt batches, so a file that kills its runner cannot take healthy siblings down with it.

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

One SQLite-backed Durable Object (`BuildDO`) owns each build, storing the immutable test catalog in 1,000-test chunks (`catalog_chunks`, `INTEGER PRIMARY KEY`, ~68 KB/row, well under SQLite's 100 KB statement and 8 MB row limits) and appending one row per `Append` (`event_log`, `INTEGER PRIMARY KEY`, no `AUTOINCREMENT`, no extra index). The Worker checks bearer tokens in constant time over SHA-256 digests before waking any Durable Object, streams and bounds request bodies while reading, and forwards raw JSON text so bodies are parsed once inside `BuildDO`. Each `BuildDO` runs `transactionSync` per `Append` (aborting via `ctx.abort()` on storage error so memory cannot diverge), skips redundant per-path validation and SHA-256 re-hashing on its own SQLite `initialized` row during wake replay while still applying every event through `Apply`, and uses two lifecycle alarms: one at `build_deadline` to finalize abandoned builds (rescheduling to `+7 days`), and once terminal, one at `+7 days` to call `ctx.storage.deleteAll()` and reset memory.

A separate SQLite-backed `BuildIndex` Durable Object holds up to 200 recent build summaries with `expires_at_ms` and lazy pruning, receiving at most two RPCs per build (on `init` and on terminal verdict or first conflict). If an index update fails, `BuildDO` keeps `index_dirty=1` in SQLite and repairs the entry on the next wake, request, or alarm.

The Worker implements the same pure state machine in TypeScript (`worker/src/core`), using native `node:crypto` (`nodejs_compat`, `createHash("sha256")`) for the build digest, and passes the shared JSON test vectors, Go simulator differential traces (well-behaved and hostile), seeded DO eviction tests, and the Go black-box HTTP protocol test suite (`BUNPO_TEST_SERVER_URL`).

#### Local measurements (10-minute build, 60-byte test paths, 60 s lease TTL)

Storage rows and database sizes were measured inside local `workerd` (`cursor.rowsRead`, `cursor.rowsWritten`, `databaseSize`). Because timers do not advance inside `workerd` during synchronous CPU work, CPU times were measured on the same TypeScript code under Node 26 via `process.cpuUsage()` (warm p50 / max, reproducible with `node worker/bench/repro.ts`). Lifecycle rows written include `catalog_chunks`, `event_log`, schema, `setAlarm`, `BuildIndex`, and 7-day `deleteAll()`.

| Tests | Machines | DO Reqs | Data / Lifecycle Rows Written | SQLite Size | Warm `init` CPU | Warm `wake` CPU | Free Builds/Day (First Limit Hit) |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000 | 8 | 426 | 202 / 410 | 300 KiB | 0.52 / 2.65 ms | 0.78 / 2.24 ms | 173 (DO GB-s/day) |
| 1,000 | 64 | 2,889 | 956 / 1,918 | 388 KiB | 0.36 / 1.35 ms | 0.68 / 1.72 ms | 34 (DO reqs/day) |
| 5,000 | 8 | 483 | 262 / 530 | 1.18 MiB | 2.40 / 3.78 ms | 2.46 / 5.57 ms | 173 (DO GB-s/day) |
| 5,000 | 64 | 3,315 | 1,504 / 3,014 | 1.47 MiB | 2.14 / 4.03 ms | 2.40 / 5.23 ms | 30 (DO reqs/day) |
| 10,000 | 8 | 517 | 303 / 612 | 2.28 MiB | 3.90 / 12.66 ms | 3.76 / 5.57 ms | 163 (rows written/day) |
| 10,000 | 64 | 3,520 | 1,723 / 3,452 | 2.78 MiB | 3.95 / 8.86 ms | 4.20 / 5.25 ms | 28 (DO reqs & rows written) |
| 20,000 | 8 | 590 | 399 / 804 | 4.47 MiB | 7.52 / 13.17 ms | 9.75 / 38.77 ms | 124 (rows written/day) |
| 20,000 | 64 | 3,691 | 1,935 / 3,876 | 5.12 MiB | 7.55 / 13.96 ms | 15.91 / 25.83 ms | 25 (rows written/day) |

- **Worker `MAX_TESTS` default (5,000):** The protocol and Go server keep `20,000`. On the Worker, `MAX_TESTS` in `worker/wrangler.jsonc` defaults to `5,000` (valid range `1..20,000`; invalid values fail closed with `500` on every request). What cannot be measured locally is the real Free-plan CPU limit inside a Durable Object (Cloudflare documents `10 ms` on Workers Free and `30 s` on Durable Objects without naming Free) and Cloudflare's exact CPU speed, so `5,000` is chosen from local measurements under the conservative `10 ms` assumption with margin and can be raised up to `20,000` after testing on a real account.
- **64-machine heartbeats:** At 64 machines and the default 60 s `--lease-ttl` (heartbeats every 20 s), heartbeats are roughly half of all Worker and DO requests (`1,628–1,679` of `2,886–3,688` over a 10-minute build). Passing a longer `--lease-ttl` (up to `10m`) cuts heartbeat volume proportionally.

## Consequences

- Measured on local `workerd`, a 20,000-test build writes `399` SQLite data rows (`804` lifecycle rows including 7-day `deleteAll()`) at 8 machines and `1,935` data rows (`3,876` lifecycle rows) at 64 machines—instead of ~120,000—and healthy heartbeats write zero rows.
- With `--data DIR` (and in the planned Durable Object backend), restarting `bunpo-server` cannot change past decisions or accept a stale result, at the cost of one `fsync` per mutating decision and delaying expiry of in-flight leases by up to one TTL. Without `--data`, `bunpo-server` stays in memory only: a restart loses running builds, runners receive `404` and exit `1`, and nothing can pass across a restart.
- Both Go and TypeScript will maintain a copy of the small state machine; shared deterministic JSON vectors catch behavioral drift before deployment.
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
