# Shared queue state-machine test vectors

Each `*.json` file in this directory is a language-neutral test scenario that
both the Go core (`internal/queue`) and the TypeScript Worker core (`worker/`)
execute against their pure `Decide` and `Apply` functions.

## Format

```json
{
  "name": "short-kebab-name",
  "description": "What rule or failure mode this scenario checks.",
  "min_lease_ttl_ms": 1000,
  "steps": [
    {
      "now_ms": 1000,
      "command": { "type": "init", "...": "..." },
      "expect_status": 200,
      "expect_error_code": "",
      "expect_events": ["initialized"],
      "expect_response": { "...": "..." }
    }
  ],
  "expect_final": {
    "verdict": "passed",
    "counts": {
      "total": 2,
      "pending": 0,
      "leased": 0,
      "passed": 2,
      "failed": 0,
      "skipped": 0,
      "unresolved": 0
    },
    "conflicts_len": 0,
    "conflicts_truncated": false,
    "tests": [
      {
        "test_index": 0,
        "state": "passed",
        "generation": 1,
        "timed": true,
        "duration_ms": 100,
        "history_len": 0,
        "history_truncated": false
      }
    ]
  }
}
```

## Step execution rules

1. Construct an empty `State` with `min_lease_ttl_ms` (default `10000` if omitted or `0`).
2. For each step in `steps`:
   - If `command.type == "restart"`, call `ResetActiveLeaseDeadlines(now_ms)` (simulating a server restart or Durable Object eviction) and continue.
   - Otherwise call the matching `Decide*` function at `now_ms`. `Decide*` must **not** mutate `State`.
   - Assert HTTP status (`200` on nil error, else `err.HTTPStatus`) matches `expect_status`, and `err.Code` matches `expect_error_code`.
   - Assert the emitted event types match `expect_events` in exact order.
   - Apply every emitted event to `State` via `Apply` (even when `expect_status` is `409` or `429` if events were returned, such as `conflicted` or `expired`), and record every emitted event in the scenario's event log.
   - If `command.type == "heartbeat"` and `response.held == true`, call `ExtendLease(command.lease_id, response.deadline_ms)`.
   - If `command.type == "lease"` and `expect_status == 200` and `response.status == "leased"`, call `ExtendLease(response.lease_id, response.deadline_ms)`.
   - If `expect_response` is present, assert every key present in `expect_response` equals the corresponding field in the actual JSON response.
3. After all steps:
   - Assert `State` matches `expect_final` (verdict, counts, conflicts length/truncated flag, and each listed test's state, generation, timing, and history bounds).
   - Replay the recorded event log onto a brand-new `State` via `Apply` only, and assert the replayed state equals the live state's persistent fields.

## Command types

- `"init"`: `build_id`, `runner_id`, `machine_index`, `config`, `tests`, `predictions_ms`.
- `"lease"`: `runner_id`, `machine_index`, `request_id`.
- `"heartbeat"`: `runner_id`, `lease_id`.
- `"report"`: `runner_id`, `lease_id`, `results`.
- `"status"`: no extra fields.
- `"list_tests"`: `cursor`, `limit`, `states`, `retried`.
- `"restart"`: simulates restart/eviction deadline reset at `now_ms`.
