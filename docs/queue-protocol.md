# Bunpō live queue protocol and state machine specification

Protocol version: `1`.

Implementation status: the Go state machine (`internal/queue`), HTTP handler, client, and persistent CRC-32C disk log (`internal/queueapi`), standalone server (`cmd/bunpo-server`, in-memory or persistent via `--data DIR`), `bunpo --queue` worker and `--status` commands, and the Cloudflare Worker (`worker/`, SQLite `BuildDO` and `BuildIndex`) are implemented. Only the dashboard remains planned.

This document specifies the HTTP JSON protocol, the pure queue state machine, the event log, and the client rules for Bunpō's live queue. An independent server or client implementation should be able to interoperate from this document alone.

## 1. Conventions and wire format

- Base path: `/api/v1`.
- Content type for all JSON requests and responses: `application/json; charset=utf-8`.
- Timestamps are signed 64-bit integers in Unix milliseconds (`int64`, safe in IEEE-754 doubles up to `2^53 - 1`).
- Durations are signed 64-bit integers in whole milliseconds (`0` to `2,592,000,000`, i.e., 30 days). No floating-point arithmetic takes part in scheduling, median calculation, or batching.
- Every request and response object includes `"version": 1` (except `GET /healthz`).
- Strict JSON decoding:
  - Input must be valid UTF-8.
  - Unknown object members are rejected (`400 invalid_request`).
  - Missing required members are rejected (`400 invalid_request`). Nullable fields explicitly marked `| null` must still be present as keys.
  - On decode failure, check `"version"` when present so a mismatched protocol version fails with `400 unsupported_version` naming both the received version and `1`.
  - Duplicate object keys are not part of the wire contract; servers may reject them (the Go server does via `encoding/json/v2`).
- Every error response has the shape:

```json
{
  "version": 1,
  "error": {
    "code": "string",
    "message": "human-readable explanation"
  }
}
```

- All `/api/v1/...` responses set `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`.

## 2. Authentication and trust boundary

Two distinct bearer tokens are configured from the environment only, never CLI flags:

- `BUNPO_QUEUE_TOKEN`: write token (grants read and write access).
- `BUNPO_QUEUE_READ_TOKEN`: read token (grants `GET /api/v1/...` access only).

Server requirements:

- Both tokens must be configured, non-empty, at least 16 bytes long, and not equal. The standalone Go server refuses to start if this fails. The planned Cloudflare Worker will fail closed with `503 server_misconfigured` on every `/api/v1/...` request.
- Clients send `Authorization: Bearer <token>`.
- Servers compare the SHA-256 digest of the presented token against the SHA-256 digest of the configured token(s) in constant time.
- Missing or malformed `Authorization` header returns `401 unauthorized`. A valid read token presented to a `POST` endpoint returns `403 forbidden`.
- `GET /healthz` requires no token and returns no build or configuration data.

Trust boundary:

- **Anyone holding `BUNPO_QUEUE_TOKEN` can initialize builds, lease work, and report any outcome for any build.** Treat it as a CI write secret.
- Pull requests from forks do not have repository secrets and must use static mode (`bunpo split` / `verify`).
- The client strips `BUNPO_QUEUE_TOKEN` and `BUNPO_QUEUE_READ_TOKEN` from the test command's environment so untrusted test code cannot trivially call the queue API directly. On some operating systems a same-user child process can still inspect another process's environment, so stripping is a reduction in exposure rather than a hard security boundary.
- The client refuses plain `http://` URLs except loopback (`localhost`, `127.0.0.1`, `[::1]`) unless `--allow-insecure-http` is passed (which prints a warning to stderr). The client never follows HTTP redirects, bounds every response body, and never prints tokens.
- The planned dashboard will use no cookies; it will store the read token in `sessionStorage`, send it in the `Authorization` header, render all server data with `textContent`, and serve a strict Content-Security-Policy.

### When to use the live queue vs. static splitting

Batching costs extra test-runner startups compared to one static invocation per machine. In simulations with a 5 s runner startup overhead across 4 machines:

| Scenario | Static split | Live queue |
| --- | --- | --- |
| 4 equal machines starting together | 827 s | 902 s |
| Mixed speeds (1.8x and 1x) | 1,077 s | 850 s |
| One machine 5 minutes late | 1,127 s | 976 s |

Use static splitting (`bunpo split` / `verify`) when machines are identical and start together. Use the live queue when runners differ in speed, start at different times behind CI queues, or can be preempted or killed mid-run. State-machine `Decide` calls stay under 2.5 ms at 20,000 tests.

## 3. Identifiers and limits

| Parameter | Limit / Rule |
| --- | --- |
| Protocol `version` | `1` |
| `build` ID | `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` and must not contain `..` |
| `tests` per build | `1` to `20,000` distinct canonical POSIX paths, strictly sorted by UTF-8 bytes (Go server keeps `20,000`; Cloudflare Worker `MAX_TESTS` var defaults to `5,000`, configurable `1..20,000`, invalid values refuse every request with `500`) |
| Canonical test ID | Non-empty, valid UTF-8, max `4,096` bytes, `/` separator, no `\`, no leading `/`, no empty/`.`/`..` components, no leading Windows drive (`C:`), no NUL/CR/LF |
| `predictions_ms` | Length equal to `len(tests)`; each element is `null` or `0..2,592,000,000` |
| `machines` (`--total`) | `1` to `256` |
| `runner_id` | `^[A-Za-z0-9._-]{1,64}$` (Bunpō client generates 32 lowercase hex chars per process via `crypto/rand`) |
| `machine_index` | Optional display metadata: `null` or `0..255` |
| `request_id` | `^[A-Za-z0-9._-]{1,128}$` |
| Distinct `runner_id`s per build | Max `1,024` |
| `lease_ttl_ms` | `10,000` to `600,000` (10 s to 10 min; default `60,000`). Core state machine accepts a configurable minimum for tests. On large matrices (e.g. 64 machines), heartbeats at `lease_ttl_ms / 3` are roughly half of all requests; a longer `--lease-ttl` reduces them. |
| `build_timeout_ms` | `lease_ttl_ms` to `86,400,000` (up to 24 h; default `21,600,000` = 6 h) |
| `max_attempts` | `1` to `10` (default `3`) |
| `max_batch` | `1` to `200` (default `200`) |
| Request body size | `4 MiB` for `POST .../init`; `1 MiB` for all other `POST` endpoints |
| Stored `init` conflicts per build | Up to `8` unique `(runner_id, got_digest)` conflicts kept; filling entry 8 sets `conflicts_truncated: true`, and duplicates or further conflicts emit no events |
| Stored stale/conflict attempt records per test | Up to `8` unique records kept; filling entry 8 sets `history_truncated: true`, and duplicates or further records emit no events |
| Retained builds (Go server) | Max `200` builds total, max `64` `running` (`503 at_capacity` when full), deleted `7 days` after finishing. Capacity scans and `GET /api/v1/builds` finalize builds past their `deadline_ms` through the event log; under capacity pressure, broken builds, oldest finished builds, and running builds idle for `> 2 * lease_ttl_ms` (longest idle first) are evicted (returning `404` so no runner can pass). With `--data DIR`, builds persist as per-build CRC-32C-framed event logs (surviving restarts with active lease deadlines reset to `now + lease_ttl_ms`, torn trailing records truncated, damaged logs quarantined untouched returning `500`, and evicted/expired non-broken build files removed before reusing a build ID). Without `--data`, builds stay in memory only and a restart loses them (`404`). |
| Retained builds (Cloudflare Worker) | `BuildIndex` lists at most `200` recent builds (with `expires_at_ms` lazy pruning); each `BuildDO` calls `deleteAll()` `7 days` after finishing |
| `GET /builds/{build}` summary caps | Up to `256` runner summaries, `20` slowest timed tests, `50` problem tests (`failed` then `unresolved`) |
| `GET /builds/{build}/tests` page size | Default `200`, max `1,000` |

## 4. Init digest

The build configuration digest is lowercase hex SHA-256 over the following exact byte stream:

```text
bunpo queue 1\n
machines <machines>\n
lease_ttl_ms <lease_ttl_ms>\n
build_timeout_ms <build_timeout_ms>\n
max_attempts <max_attempts>\n
max_batch <max_batch>\n
<test_0>\n
<test_1>\n
...
<test_{n-1}>\n
```

`<test_i>` are the canonical test paths in byte order. Because paths cannot contain `\n`, the encoding is unambiguous. `predictions_ms` is deliberately excluded so machines with different timing caches agree on the build.

## 5. Pure state machine

The state machine splits deciding from applying:

```text
Decide(state, now_ms, command) -> (events, response)   // pure, never mutates state
Apply(state, event)            -> error                // pure, never reads a clock
```

### 5.1 Clocks and replay

- Every `Decide` call receives `now_ms` (server Unix ms) and clamps it:
  `now = max(now_ms, state.last_seen_ms)`.
- Persisted events carry `at_ms = now`. `Apply` updates `state.last_seen_ms = max(state.last_seen_ms, event.at_ms)`.
- Heartbeats are **not** persisted events. A healthy heartbeat updates only the active lease's in-memory `deadline_ms = max(lease.deadline_ms, now + lease_ttl_ms)`.
- After a server restart or Durable Object eviction, the server replays all persisted events through `Apply` (never evaluating time during replay) and then resets every active lease's in-memory deadline to `max(lease.deadline_ms, now + lease_ttl_ms)`.
- Persistence order under the build lock: `Decide`, persist returned `events`, `Apply` each event to in-memory state, apply any in-memory deadline extension (for `heartbeat` or active `request_id` replay), reply. If persistence fails, return `503 storage_error` and leave in-memory state unchanged.

### 5.2 Test states and attempt IDs

Each test `i` (`0 <= i < len(tests)`) has:

- `state`: one of `pending`, `leased`, `passed`, `failed`, `skipped`, `unresolved`.
- `generation`: integer starting at `0`, incremented by `1` each time the test is leased.
- Attempt ID: `"<i>.<generation>"` (for example, `"42.1"`).
- `effective_ms`: predicted duration from `init` (`predictions_ms[i]` if non-null, else the integer half-up median of all non-null `predictions_ms`, or `0` if none are non-null).
- `active_lease_id`: `0` unless `state == leased`.
- `attempts`: up to `max_attempts` records (one per generation `1..generation`), recording `lease_id`, `runner_id`, `leased_at_ms`, `status` (`leased` | `expired` | `passed` | `failed` | `skipped` | `error`), `timed`, `duration_ms`, `finished_at_ms`.
- `history`: up to `8` unique extra records (`stale` or `conflict`) plus `history_truncated` (`bool`, set when the 8th record is stored).

Terminal test states: `passed`, `failed`, `skipped`, `unresolved`. Once a test reaches a terminal state, its state and accepted result never change.

### 5.3 Verdict precedence

A build's verdict is derived from its state:

1. If any test is `pending` or `leased`: `running`.
2. Else if any test is `failed`: `failed`.
3. Else if any test is `unresolved` or `len(conflicts) > 0`: `unresolved`.
4. Else: `passed`.

`finished_at_ms` is fixed to the `at_ms` of the event that completed the last remaining test (or finalized the build). A finished verdict may change in **exactly one way**: `passed -> unresolved` if a conflicting `init` arrives after the build passed. Never upward, and never otherwise.

### 5.4 Lazy expiry and finalization

Lazy expiry runs at the start of `Decide` using the clamped `now`:

1. If the build is already terminal (`verdict != running`), emit nothing.
2. If `now >= state.deadline_ms`: emit one `finalized` event (`at_ms: now`, `reason: "deadline"`). Applying `finalized` marks all active leases `expired` (in ascending `lease_id` order) and transitions every `pending` and `leased` test to `unresolved`.
3. Otherwise, scan active leases in ascending `lease_id` order. For each active lease with `lease.deadline_ms <= now`, emit one `expired` event (`lease_id`, `at_ms: now`). Applying `expired` marks the lease `expired` and, for each attempt in ascending `test_index` order where the test is still `leased` under this `lease_id`:
   - marks that attempt `expired` at `at_ms`;
   - if `test.generation >= config.max_attempts`, sets test state to `unresolved`;
   - else sets test state back to `pending`.

When each command triggers lazy expiry:

- `Lease` and `Report`: always run lazy expiry first and include any emitted `finalized`/`expired` events.
- `Heartbeat`: if `now >= state.deadline_ms` or the target lease has `deadline_ms <= now`, run full lazy expiry first; if the target lease is still active and `now < state.deadline_ms`, emit zero events.
- `Status` and `ListTests` (`GET`): evaluate lazy expiry without mutating state. If and only if doing so transitions `verdict` from `running` to terminal, emit those `finalized`/`expired` events (so the server persists them before returning the terminal verdict). Otherwise emit zero events and return the current persisted state.

### 5.5 Deal order and batching

Pending tests are ordered by:

1. `generation == 0` before `generation > 0` (first attempts before retries).
2. Higher `effective_ms` first.
3. Lower `test_index` (byte order of path) first.

Batch selection on `Lease`:

- Let `pending_first` be pending tests with `generation == 0` and `pending_retry` be pending tests with `generation > 0`.
- If the first test in deal order has `generation > 0` (meaning `pending_first` is empty and `pending_retry` is non-empty): lease **only that single test**.
- Otherwise (first test has `generation == 0`):
  - Compute `remaining_ms` = sum of `effective_ms` across **all pending tests** (`pending_first` and `pending_retry`).
  - Let `denom = 2 * config.machines`.
  - If `remaining_ms > 0`: `target_ms = (remaining_ms + denom - 1) / denom` (integer ceiling). Take tests from `pending_first` in deal order, skipping `generation > 0`, adding until `batch_ms >= target_ms` or `len(batch) == config.max_batch` or `pending_first` is exhausted. Always take at least 1 test.
  - If `remaining_ms == 0`: `target_count = (total_pending + denom - 1) / denom` (integer ceiling), clamped to `[1, config.max_batch]`. Take up to `target_count` tests from `pending_first` in deal order.
- Sort the chosen batch's attempts in ascending `test_index` order in the `leased` event and response.

When no tests are `pending` but at least one active lease exists, `Lease` returns `status: "wait"` with integer millisecond arithmetic:

```text
expected_end(lease) = lease.leased_at_ms + sum(tests[a.test_index].effective_ms for a in lease.attempts)
latest              = max(expected_end(lease) for active leases)
earliest_deadline   = min(lease.deadline_ms for active leases)

if now < latest:
    wait = latest - now
else:
    wait = (now - latest) / 4

wait           = min(wait, earliest_deadline - now)
max_wait       = max(1, min(config.lease_ttl_ms / 2, 15_000))
min_wait       = min(1_000, max_wait)
retry_after_ms = clamp(wait, min_wait, max_wait)
```

Before active leases reach their predicted end, idle machines sleep until `latest` (capped by `earliest_deadline` and `max_wait`) so builds with predictions finish within roughly 1-2 seconds of the last test instead of sleeping half a lease TTL. Once overdue—or when no predictions exist (`effective_ms == 0`, so `latest == max(leased_at_ms) <= now`)—the wait backs off as `(now - latest) / 4` up to `15_000 ms`, while still capping by `earliest_deadline - now` so idle runners wake promptly if a dead holder's lease expires. With this backoff, 255 idle machines waiting on one test that runs 10 minutes overdue send about 13,000 requests instead of 150,000. (With tiny TTLs in tests where `max_wait < 1_000`, `min_wait` collapses to `max_wait`.)

### 5.6 Result evaluation order (`Report`)

A `Report` names `lease_id`, `runner_id`, and `results[]`.

1. **Lease and shape validation (all-or-nothing, `400`, nothing stored):**
   - `lease_id` must exist and `lease.runner_id == runner_id`.
   - `results` must contain every attempt `(test_index, generation)` of that lease exactly once—no missing attempts, no extra attempts, no duplicates within the same request.
   - Each result's `test_index` must be in `0..len(tests)-1` and `generation` in `1..test.generation`.
   - Each `outcome` must be one of `passed`, `failed`, `skipped`, `error`.
   - If `timed == false`, `duration_ms` must be `0`. If `timed == true`, `outcome` must be `passed` and `duration_ms` must be in `0..2,592,000,000`.
2. **Run lazy expiry at `now`** (see 5.4).
3. **Evaluate each result in the lease's attempt order (ascending `test_index`):**
   1. If attempt `(test_index, generation)` already has a recorded outcome (`passed`, `failed`, `skipped`, or `error`—not `leased` or `expired`):
      - If `(outcome, timed, duration_ms)` matches the recorded attempt -> `duplicate` (no change, never persisted in a `reported` event).
      - Else -> `conflict` (persisted only if `len(history) < 8` and no identical `(generation, lease_id, runner_id, outcome, timed, duration_ms, disposition)` record is already stored; filling the 8th slot sets `history_truncated: true`; no change to test state or accepted outcome).
   2. Else if `generation == test.generation` and `test.state == leased` and `test.active_lease_id == lease_id` (and lease is still active after lazy expiry):
      - `accepted`. Records `(outcome, timed, duration_ms, at_ms)` on the attempt.
      - If `outcome` is `passed`, `failed`, or `skipped`: transitions test to that terminal state and clears `active_lease_id`.
      - If `outcome == error`: clears `active_lease_id`; if `generation >= config.max_attempts`, transitions test to `unresolved`, else returns test to `pending`.
   3. Else (older generation, or this attempt already expired):
      - `stale` (persisted only if `len(history) < 8` and no identical record is already stored; filling the 8th slot sets `history_truncated: true`; never terminal, no change to test state).
4. If the lease was active and all its attempts were `accepted`, mark the lease `reported`.
5. Emit one `reported` event containing **only** the results that actually change state or append a new `stale`/`conflict` history entry (`duplicate`, deduplicated, and post-truncation records are omitted from the event, though every lease attempt still appears in the HTTP response). If no results qualify, emit zero events.

### 5.7 Persisted event schema

Every event has `seq` (1-based monotonic per build, assigned by storage/log), `type`, and `at_ms`:

| `type` | Fields |
| --- | --- |
| `initialized` | `build_id`, `digest`, `config` (`machines`, `lease_ttl_ms`, `build_timeout_ms`, `max_attempts`, `max_batch`), `deadline_ms`, `tests[]`, `predictions_ms[]` (each `int64 \| null`) |
| `conflicted` | `runner_id`, `machine_index`, `got_digest`, `reason` |
| `leased` | `lease_id`, `runner_id`, `machine_index`, `request_id`, `deadline_ms`, `attempts[]` (`{test_index, generation}`) |
| `reported` | `lease_id`, `runner_id`, `close_lease` (`bool`), `results[]` (`{test_index, generation, outcome, timed, duration_ms, disposition}` — `duplicate` results are never stored) |
| `expired` | `lease_id` |
| `finalized` | `reason` (`"deadline"`) |

`Apply` rules:

- `Apply` must validate every precondition of an event before mutating any state, and reject events that do not fit the current state (wrong or unknown `lease_id`, `runner_id` mismatch, unknown or mismatched attempt, `accepted` on a non-leased test, `duplicate` in a `reported` event, `leased` for a runner that already holds an active lease, `at_ms < last_seen_ms`, or appending past the 8-item cap). A damaged journal must fail loudly rather than apply an event partly.
- Because `duplicate` reports, identical stale/conflict records, identical `init` conflicts, and entries past the 8-item caps emit zero events, the total number of persisted events for any build is strictly bounded by `10 + len(tests) * (3 * max_attempts + 8)` regardless of hostile retries.

## 6. HTTP endpoints

### `POST /api/v1/builds/{build}/init`

Auth: write token. Max body: `4 MiB`.

Request:

```json
{
  "version": 1,
  "runner_id": "0123456789abcdef0123456789abcdef",
  "machine_index": 0,
  "config": {
    "machines": 4,
    "lease_ttl_ms": 60000,
    "build_timeout_ms": 21600000,
    "max_attempts": 3,
    "max_batch": 200
  },
  "tests": ["tests/a.py", "tests/b.py"],
  "predictions_ms": [1200, null]
}
```

Responses:

- `200 OK` (created or idempotent match):
  ```json
  {
    "version": 1,
    "build_id": "run-123",
    "created": true,
    "digest": "<64-hex>",
    "verdict": "running",
    "test_count": 2,
    "deadline_ms": 1700021600000
  }
  ```
- `409 Conflict` (`init_conflict`): test list or config digest differs from the build's stored digest. Message names the difference (config mismatch or test count / first differing test). Persists a `conflicted` event in any build state (including finished builds, flipping `passed -> unresolved`) unless an identical `(runner_id, got_digest)` conflict is already stored or `conflicts_truncated` is already `true`.
- `400 Bad Request`: invalid build ID, non-canonical/unsorted/duplicate tests, invalid config or predictions length, unsupported version.
- `413 Payload Too Large`: body > `4 MiB`.
- `503 Service Unavailable` (`at_capacity` or `storage_error`).

### `POST /api/v1/builds/{build}/lease`

Auth: write token. Max body: `1 MiB`.

Request:

```json
{
  "version": 1,
  "runner_id": "0123456789abcdef0123456789abcdef",
  "machine_index": 0,
  "request_id": "a1b2c3d4e5f6"
}
```

Responses (`200 OK`):

- Leased:
  ```json
  {
    "version": 1,
    "status": "leased",
    "lease_id": 1,
    "deadline_ms": 1700000060000,
    "attempts": [
      {"attempt_id": "0.1", "test_index": 0, "generation": 1, "test": "tests/a.py"}
    ]
  }
  ```
- Wait (no pending tests, active leases remain):
  ```json
  {
    "version": 1,
    "status": "wait",
    "retry_after_ms": 15000
  }
  ```
- Done (build is terminal):
  ```json
  {
    "version": 1,
    "status": "done",
    "verdict": "passed"
  }
  ```

Error responses:

- `404 Not Found` (`build_not_found`): build not initialized.
- `409 Conflict` (`stale_request`): `request_id` matches the runner's last remembered lease, which is no longer active.
- `409 Conflict` (`lease_held`): `runner_id` already holds an active lease and sent a different `request_id`. Message names the held `lease_id`.
- `429 Too Many Requests` (`too_many_runners`): `runner_id` is new and build already has `1,024` distinct runners.

### `POST /api/v1/builds/{build}/heartbeat`

Auth: write token. Max body: `1 MiB`.

Request:

```json
{
  "version": 1,
  "runner_id": "0123456789abcdef0123456789abcdef",
  "lease_id": 1
}
```

Response (`200 OK`):

```json
{
  "version": 1,
  "held": true,
  "deadline_ms": 1700000080000,
  "verdict": "running"
}
```

When `held` is `false`, `deadline_ms` is `0` and the runner must stop its child process and request a new lease.

Error responses: `400` (`unknown_lease`, `runner_mismatch`), `404` (`build_not_found`).

### `POST /api/v1/builds/{build}/report`

Auth: write token. Max body: `1 MiB`.

Request:

```json
{
  "version": 1,
  "runner_id": "0123456789abcdef0123456789abcdef",
  "lease_id": 1,
  "results": [
    {
      "attempt_id": "0.1",
      "test_index": 0,
      "generation": 1,
      "outcome": "passed",
      "timed": true,
      "duration_ms": 1240
    }
  ]
}
```

Response (`200 OK`):

```json
{
  "version": 1,
  "verdict": "running",
  "results": [
    {"attempt_id": "0.1", "test_index": 0, "generation": 1, "disposition": "accepted"}
  ]
}
```

`disposition` is one of `accepted`, `duplicate`, `stale`, `conflict`.

Error responses: `400` (missing/extra/duplicate attempt, attempt ID mismatch with `test_index`/`generation`, invalid outcome/timing, unknown lease, runner mismatch), `404` (`build_not_found`).

### `GET /api/v1/builds/{build}`

Auth: read or write token.

Response (`200 OK`):

```json
{
  "version": 1,
  "build_id": "run-123",
  "digest": "<64-hex>",
  "verdict": "running",
  "created_at_ms": 1700000000000,
  "deadline_ms": 1700021600000,
  "finished_at_ms": null,
  "last_seen_ms": 1700000020000,
  "config": {
    "machines": 4,
    "lease_ttl_ms": 60000,
    "build_timeout_ms": 21600000,
    "max_attempts": 3,
    "max_batch": 200
  },
  "counts": {
    "total": 2,
    "pending": 1,
    "leased": 0,
    "passed": 1,
    "failed": 0,
    "skipped": 0,
    "unresolved": 0
  },
  "conflicts_truncated": false,
  "conflicts": [],
  "runner_count": 1,
  "runners": [
    {
      "runner_id": "0123456789abcdef0123456789abcdef",
      "machine_index": 0,
      "first_seen_ms": 1700000000000,
      "leases": 1,
      "accepted": 1,
      "active_lease_id": 0
    }
  ],
  "slowest": [
    {"test_index": 0, "test": "tests/a.py", "duration_ms": 1240}
  ],
  "problem_count": 0,
  "problems": []
}
```

Ordering guarantees: `runners` ordered by `first_seen_ms` asc then `runner_id` asc (capped at `256`); `slowest` ordered by `duration_ms` desc then `test_index` asc (capped at `20`, timed `passed` tests only); `problems` lists `failed` then `unresolved`, each ordered by `test_index` asc (capped at `50`).

### `GET /api/v1/builds/{build}/tests`

Auth: read or write token.

Query parameters:

- `cursor`: non-negative integer `test_index` to start from (default `0`).
- `limit`: `1..1000` (default `200`).
- `state`: optional comma-separated filter (`pending`, `leased`, `passed`, `failed`, `skipped`, `unresolved`).
- `retried`: optional `true` to return only tests with `generation > 1` or non-empty `history` / `history_truncated == true`.

Response (`200 OK`):

```json
{
  "version": 1,
  "build_id": "run-123",
  "verdict": "passed",
  "tests": [
    {
      "test_index": 0,
      "test": "tests/a.py",
      "state": "passed",
      "generation": 1,
      "predicted_ms": 1200,
      "timed": true,
      "duration_ms": 1240,
      "attempts": [
        {
          "attempt_id": "0.1",
          "generation": 1,
          "lease_id": 1,
          "runner_id": "0123456789abcdef0123456789abcdef",
          "status": "passed",
          "timed": true,
          "duration_ms": 1240,
          "leased_at_ms": 1700000000000,
          "finished_at_ms": 1700000001240
        }
      ],
      "history": [],
      "history_truncated": false
    }
  ],
  "next_cursor": null
}
```

### `GET /api/v1/builds`

Auth: read or write token.

Returns up to `200` most recent builds ordered by `created_at_ms` desc, then `build_id` asc.

```json
{
  "version": 1,
  "builds": [
    {
      "build_id": "run-123",
      "verdict": "passed",
      "created_at_ms": 1700000000000,
      "finished_at_ms": 1700000045000,
      "test_count": 2,
      "failed_count": 0,
      "unresolved_count": 0,
      "has_conflicts": false
    }
  ]
}
```

### `GET /healthz`

Auth: none. Returns `200 OK` with `{"ok":true}` and no build, version, or environment details.

## 7. Client execution and `--status` rules

### 7.1 Outcome mapping per file in a lease

Let `exit` be the test command's exit status. "Signaled" means the process did not exit normally.

| Command exit | File in report | Outcome |
| --- | --- | --- |
| Any | Has at least one failed case (`<failure>` or `<error>`) | `failed` (`timed: false`) |
| Exit `0` | Has cases, not all skipped | `passed` (`timed: true`, `duration_ms`) |
| Exit `0` | Has cases, every case skipped | `skipped` (`timed: false`) |
| Exit `0` | Absent from report | `error` (`timed: false`) |
| Normal non-zero, JUnit format, and another file in this lease failed | Has cases, zero skipped cases | `passed` (`timed: false`, `duration_ms: 0`) |
| Anything else | Anything | `error` (`timed: false`) |

Additional client rules:

- Delete every `--report` file before running the command so a prior batch's report can never stand in.
- Missing, empty, or invalid report file: report every file in the lease as `error` and keep looping.
- Report mentions a file outside the lease: report every file in the lease as `error`, print the same diagnostic as `bunpo record`, and exit `1`.
- Heartbeat runs at `lease_ttl_ms / 3`. On `SIGINT`/`SIGTERM` or when `heartbeat` returns `held: false`, signal the child process group on Unix (`SIGTERM`, then `SIGKILL` after 5 s; process kill only on Windows). On `SIGINT`/`SIGTERM`, stop heartbeats and report nothing. On lost lease, discard the run and loop to `lease`.
- On `wait`, sleep `retry_after_ms` plus up to 25% random jitter and ask again.
- Retry transient HTTP/network/5xx failures with exponential backoff and jitter up to 2 minutes, then exit `1`.
- Exit `0` only when the build verdict is `passed`; `1` when `failed`, `unresolved`, or on runtime error; `2` on bad usage or invalid input.

### 7.2 `--status` and `report-0.json`

`bunpo --queue URL --build ID --status [--dir DIR]` reads `BUNPO_QUEUE_READ_TOKEN`, falling back to `BUNPO_QUEUE_TOKEN`:

1. Removes any leftover `report-0.json` in `--dir` first. Fails if any other `report-<N>.json` (`N >= 1`) exists in `--dir`.
2. Fetches `GET /api/v1/builds/{build}`, prints the verdict, counts, failed/unresolved tests, and any `init` conflicts to stderr.
3. If the build is finished with zero unresolved tests and zero `init` conflicts (`verdict` is `passed` or `failed` with `unresolved == 0 && len(conflicts) == 0`), paginates `GET /api/v1/builds/{build}/tests` (`limit=1000`) and writes `report-0.json` (`index: 0`, `machines: 1`): timed `passed` files keep their `duration_ms`; `failed`, `skipped`, and untimed `passed` files are written as `{timed: false, ms: 0}` so `bunpo merge` keeps them seen without distorting timings.
4. Exits `0` if `verdict == passed`, `1` otherwise, `2` on usage or input error.
