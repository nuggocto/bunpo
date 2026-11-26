# Local trial on LaTabdhir and Majlisna

Date: 2026-11-21. Bunpō built from `shrek` at `975284e`. `cmd/` and `internal/`
have not changed since `v0.0.4`, so this is the `v0.0.4` binary's behavior.

Souhib agreed to the trial on two production repositories:

- [Souhib/LaTabdhir](https://github.com/Souhib/LaTabdhir) at `e7b220a2`
- [Souhib/Majlisna](https://github.com/Souhib/Majlisna) at `b766271`

I ran every major pytest, Vitest, and Playwright suite across both
repositories: four in LaTabdhir (`backend/` SQLite, `backend/` weekly
`postgres-parity.yml`, `front/` Vitest, and `e2e/` Playwright + Docker
Compose) and three in Majlisna (`backend/` `--use-postgres` plus SQLite for
comparison, `e2e/` Playwright + Docker Compose across 17 projects, and
`front/` Vitest).

This was a local run, not GitHub Actions, so the Action, cache, and artifact
steps were not exercised. Nobody outside the project has set Bunpō up from
the guides in GitHub Actions yet.

## How I measured

A script followed each guide's job for every machine: list the files,
`bunpo split`, run the runner on that share, `bunpo record`. After the last
machine it ran `bunpo verify` and `bunpo merge`, like the final job. Each
"machine" ran alone, one after the other, pinned with `taskset` to 4 CPUs to
match a 4-vCPU runner. That keeps machines from slowing each other down.

- **Before** is the suite as the project's CI runs it today, on one 4-CPU
  machine.
- **After** is the slowest of 4 machines, since the job finishes when it does.
  Because Souhib currently runs two self-hosted runners (a VPS and a home
  server), I also include 2-machine splits for the long database and E2E
  workflows.
- Round 1 starts with no timing file, so files are dealt out by count.
  Rounds 2 and 3 use the timings merged from the rounds before.
- The times cover the test command only (plus Playwright's per-run
  `globalSetup`). Each extra CI machine also pays for checkout, installing
  dependencies, and any Docker stack startup, which a local run cannot measure.
  Docker build and startup times are reported separately where applicable.

Every round of every suite ran the exact same number of tests as the
single-machine baseline, with nothing missing or doubled. `verify` and `merge`
exited 0 every time.

## Summary

| Repository & suite | Runner | Files / tests | Before (1 machine) | After, 4 machines (R1 count) | After, 4 machines (R2 & R3 timed) | Change |
| --- | --- | --- | --- | --- | --- | --- |
| LaTabdhir `postgres-parity.yml` | pytest `--use-postgres` (serial, Postgres 18) | 495 / 6,469 (6,444 pass, 25 skip) | 1,529.7 s (25m 30s) | 395.7 s (54.5 s spread) | **375.5 s, 379.3 s** (5.4 s & 10.3 s spread) | **4.0x faster** (2m pred: 11m52s each) |
| LaTabdhir `backend/` CI | pytest `-n 4` (SQLite) | 495 / 6,469 | 221.5 s (3m 42s) | 86.7 s (20.3 s spread) | **60.7 s, 63.0 s** (2.6 s & 5.7 s spread) | **3.6x faster** |
| LaTabdhir `front/` | Vitest (full-cost reporter) | 294 / 2,570 | 123.2 s (2m 03s) | 37.6 s (5.8 s spread) | **34.3 s, 36.6 s** (1.7 s & 2.4 s spread) | **3.4x faster** |
| LaTabdhir `e2e/` | Playwright `workers: 1` (Docker Compose) | 68 / 232 (`--grep-invert @payments`) | 308.7 s (5m 09s) | 188.6 s | **94.3–103.2 s** clean (165.4 s / 144.1 s with flaky retry) | **~3.0x faster** clean, 2.1x with flake |
| Majlisna `backend/` CI | pytest `--use-postgres` (serial, Postgres 16) | 113 / 2,777 (all pass) | 490.0 s (8m 10s) | 254.7 s (179.5 s spread) | **127.6 s, 132.8 s** (7.3 s & 11.1 s spread) | **3.8x faster** (2m measured: 245.5 s / 258.5 s, 1.9x) |
| Majlisna `e2e/` | Playwright `E2E_WORKERS=4` (Docker Compose, 17 projects) | 70 / 288 (all pass) | 374.4 s (6m 14s) | 207.7 s (140.6 s spread) | **207.8 s, 207.9 s** (m1–m3: 112–125 s; floored by 190 s WS test) | **1.8x faster** (2m pred: 12m1s vs 12m2s) |
| Majlisna `front/` | Vitest (inline full-cost reporter) | 151 / 1,584 | 24.7 s | 7.8 s (1.08 s spread) | **7.8 s, 7.7 s** (0.56 s & 0.32 s spread) | **3.2x faster** (too short for 4 CI runners) |

## LaTabdhir

### LaTabdhir `postgres-parity.yml` (pytest `--use-postgres`, serial, Postgres 18)

In GitHub CI, `postgres-parity.yml` runs weekly for **51–69 minutes** (and
previously timed out at 120 minutes) because all 495 files run serially
against one PostgreSQL 18 sidecar (`TRUNCATE` between tests prevents
`pytest-xdist` on a single database). With Bunpō, each machine starts its own
isolated `postgres:18-alpine` sidecar (2.5 s startup locally) and runs its
share with zero table lock contention.

I ran the exact CI command (`pytest --use-postgres -p no:cacheprovider --no-cov -q`)
with the workflow's exact environment variables and `postgres:18-alpine`
sidecar recreated fresh before every machine.

| Round | Machine 0 | Machine 1 | Machine 2 | Machine 3 | Spread | Prediction |
| --- | --- | --- | --- | --- | --- | --- |
| Baseline (1 machine) | 1,529.7 s (25m 30s; 6,444 pass, 25 skip) | | | | | |
| 1, by count | 363.1 s (124) | 395.7 s (124) | 385.2 s (124) | 341.2 s (123) | 54.5 s | |
| 2, timed | 375.5 s (123) | 370.0 s (123) | 370.6 s (127) | 371.6 s (122) | **5.4 s** | 5m59s each |
| 3, timed | 369.9 s (123) | 379.3 s (122) | 369.0 s (127) | 372.0 s (123) | **10.3 s** | 5m59s each |

Bunpō predicted `5m59s` (359 s) on every machine in rounds 2 and 3; pytest
finished in `369.0–379.3 s` (2.8–5.7% above the 359 s prediction), and all four
6-minute shards finished within 5.4 s in round 2 and 10.3 s in round 3.
Speedup was **4.07x** (round 2) and **4.03x** (round 3), slightly over 4x
because each shard gets a fresh Postgres database rather than accumulating
25 minutes of session churn.

On **2 machines** (matching Souhib's two self-hosted runners), `bunpo split`
predicts **11m52s vs 11m52s** (250 vs 245 files), cutting the weekly 51–69 min
CI job in half.

Top slowest files under PostgreSQL 18:
`tests/services/test_ticket_emails.py` (28.5 s),
`tests/integration/test_idor.py` (26.0 s),
`tests/services/test_service_day.py` (20.5 s),
`tests/integration/test_order_amount_bounds.py` (19.6 s),
`tests/controllers/test_store_controller.py` (17.6 s).

### LaTabdhir `backend/` CI (pytest `-n 4`, SQLite)

CI runs `pytest -n auto -q --no-cov` on SQLite inside a Python 3.12 container.
I ran the same command on the host with Python 3.12 and the CI environment
variables, adding `--rootdir=. -o junit_family=xunit1 --junitxml=junit.xml`.

| Round | Machine 0 | Machine 1 | Machine 2 | Machine 3 | Spread |
| --- | --- | --- | --- | --- | --- |
| Baseline (1 machine) | 221.5 s (495 files / 6,469 tests) | | | | |
| 1, by count | 66.3 s | 86.7 s | 77.2 s | 71.5 s | 20.3 s |
| 2, timed | 59.0 s | 60.4 s | 58.1 s | 60.7 s | **2.6 s** |
| 3, timed | 59.4 s | 58.7 s | 57.3 s | 63.0 s | **5.7 s** |

Bunpō predicted equal loads of 4m1s (round 2) and 3m33s (round 3). Those are
summed test seconds across 4 xdist workers, so wall time is about a quarter
of the prediction.

LaTabdhir already has an in-repo split in `tests/conftest.py` using
`crc32(nodeid) % PYTEST_SHARD_TOTAL`. Measured the same way for comparison:

| crc32 shard | 1 | 2 | 3 | 4 |
| --- | --- | --- | --- | --- |
| First run | 58.8 s | 55.0 s | 66.5 s | 112.8 s |
| Rerun | | | 59.1 s | 58.9 s |

Shard 4's 112.8 s was local machine noise (reran in 58.9 s). Hashing 6,469
test IDs gives the same wall time (~59 s) as Bunpō on this suite. Bunpō's
advantages are removing the conftest hook, verifying every file was assigned
exactly once, collecting only each machine's own files, and adapting when a
file grows slow.

Note: `backend/` SQLite and `postgres-parity.yml` **must use separate cache
key prefixes** (`bunpo-backend-sqlite-timings-` vs
`bunpo-backend-postgres-timings-`) because PostgreSQL file durations differ
sharply from SQLite durations.

### LaTabdhir `front/` (Vitest)

CI runs `bun run test:coverage` under Node 26 (294 files / 2,570 tests).

| Round | Timings from | Machines | Spread | Bunpō's prediction |
| --- | --- | --- | --- | --- |
| Baseline (1 machine) | — | 123.2 s | | |
| 1 | none (count) | 34.2, 37.6, 34.2, 31.7 s | 5.8 s | |
| 2 | JUnit | 32.0, 32.1, 37.5, 38.8 s | 6.8 s | 20 s each |
| 1 | none (count) | 32.0, 33.2, 33.7, 31.2 s | 2.4 s | |
| 2 | full cost | 33.5, 34.3, 32.5, 33.5 s | **1.7 s** | 1m29s each |
| 3 | full cost | 36.6, 35.6, 34.2, 34.5 s | **2.4 s** | 1m30s each |

With Vitest's JUnit report, timed round 2 was worse than counting files
because JUnit counts only test bodies (78 s) and leaves out imports (161 s),
setup files (48 s), and environment startup (50 s). Switching to an inline
Vitest reporter that sums `mod.diagnostic()` and feeds
`bunpo record --format lines` cut the spread from 6.8 s to 1.7–2.4 s. That
inline reporter is now the default in `docs/guides/vitest.md`.

### LaTabdhir `e2e/` (Playwright + Docker Compose)

CI (`e2e.yml`, ~20 min in recent runs) runs `--project=chromium --grep-invert @payments`
with `workers: 1` (because specs share store state) against the full Docker
Compose stack (PostgreSQL, SeaweedFS, Mailpit, backend, frontend, waitlist
frontend). Out of 71 spec files on disk, 3 are `@payments`, leaving 68 files
and 232 tests (231 passed, 1 skipped). I ran with `E2E_SENTRY_DSN=""` and
`CI=true`.

Fixed stack overhead measured separately: cold Docker build + stack startup
took 447 s; steady-state stack startup with cached images took 13.7 s.
Playwright's `globalSetup` (database reset and seed) runs once per machine
and takes 21–27 s.

| Round | Machine 0 | Machine 1 | Machine 2 | Machine 3 | Spread | Prediction |
| --- | --- | --- | --- | --- | --- | --- |
| Baseline (1 machine) | 308.7 s (68 files) | | | | | |
| 1, by count | 74.7 s (17) | 89.8 s (17) | 121.3 s (17) | 188.6 s (17, 1 flaky) | 113.9 s | |
| 2, timed | 106.8 s (14) | 72.9 s (15) | 108.3 s (22) | 165.4 s (17, 1 flaky) | 92.4 s | 1m19s each |
| 3, timed | 99.9 s (13) | 94.3 s (16) | 144.1 s (21, 1 flaky) | 103.2 s (18) | 49.9 s (**8.9 s** clean) | 1m16s each |

In round 3, the three clean shards finished in 94.3–103.2 s (8.9 s spread,
~3.0x faster than baseline), matching 76 s predicted test time plus ~22 s
`globalSetup`. Machine 2 took 144.1 s because
`tests/cross-flow/full-journey.spec.ts` flaked and retried twice (~60 s
extra). Top slowest specs: `tests/ui/accessibility.spec.ts` (46.1 s),
`tests/auth/mfa.spec.ts` (25.5 s),
`tests/cross-flow/notification-on-booking.spec.ts` (11.6 s).

Because each machine starts its own Docker Compose stack, Bunpō parallelizes
a `workers: 1` shared-state suite without rewriting tests.

## Majlisna

### Majlisna `backend/` (pytest `--use-postgres` vs SQLite)

CI runs `uv run pytest --tb=short -q --use-postgres --cov=majlisna --cov-branch --cov-report=term --cov-fail-under=88`
(serial, no xdist, spinning up a PostgreSQL 16 testcontainer). Running locally
with `--use-postgres --no-cov` on Python 3.14.10 ran all **2,777 tests**
across 113 files (0 skipped, 0 failed).

| Round | Machine 0 | Machine 1 | Machine 2 | Machine 3 | Spread | Prediction |
| --- | --- | --- | --- | --- | --- | --- |
| Baseline (1 machine, `--use-postgres`) | 490.0 s (8m 10s; 113 files / 2,777 tests) | | | | | |
| 1, by count | 75.8 s (29) | 75.2 s (28) | 254.7 s (28) | 101.9 s (28) | 179.5 s | |
| 2, timed | 127.6 s (1) | 121.9 s (36) | 125.9 s (40) | 120.3 s (36) | **7.3 s** | 2m15s, 2m0s, 2m0s, 2m0s |
| 3, timed | 132.8 s (1) | 121.7 s (36) | 121.9 s (37) | 125.4 s (39) | **11.1 s** | 2m10s, 2m0s, 2m0s, 2m0s |
| **2 machines, timed (measured)** | **258.5 s (57)** | **245.5 s (56)** | — | — | **13.0 s** | **4m6s each** |

Real timings made the difference here:

- **By count (round 1)** put the 131 s WebSocket reconnect file on machine 2
  with 27 other files (**254.7 s**, 179.5 s spread).
- **Timed (rounds 2 and 3)** isolated `tests/api/ws/test_wp04_reconnect_and_detach.py`
  by itself on machine 0 (127.6–132.8 s) and packed the other 112 files onto
  machines 1–3 in 120.3–125.9 s, cutting the 8m 10s suite to **2m 08s
  (3.84x faster)** with a **7.3 s spread** across all four machines.
- **On 2 machines** (matching Souhib's two self-hosted runners), the measured
  run cut 490.0 s to **258.5 s / 245.5 s (1.90x faster)**.
- **PostgreSQL vs SQLite weights differ completely:** On SQLite (234.7 s
  baseline, 63 Postgres tests skipped), `test_undercover_game.py` took 5.8 s,
  `test_codenames_game.py` took 4.8 s, and `test_game_pause.py` took 4.0 s,
  leaving `test_wp04_reconnect_and_detach.py` (118 s) as half the suite (2.1x
  ceiling on 4 machines). Under `--use-postgres`, those controller suites jump
  to **40.9 s**, **25.5 s**, and **24.4 s** (plus `test_migrations.py` at
  16.5 s and `test_disconnect_lock_postgres.py` at 15.3 s), so the other 112
  files balance right alongside the 131 s file on 4 machines.

During the SQLite run, round 3 also caught a real random test bug in
`tests/api/controllers/test_room_invites.py:116` (see Side findings below)
and verified Bunpō's failure path live: `record` kept the failed file's
earlier timing, and `merge` reported `113 files seen, 112 timed`.

### Majlisna `e2e/` (Playwright + Docker Compose, 17 projects)

CI (`pipeline.yml`, 10–34 min depending on runner, 55-min timeout) runs 70
spec files / 288 tests across 17 Playwright projects, including 4 mobile
viewport projects (`mobile-smoke`, `mobile-rooms`, `mobile-wordquiz`,
`mobile-daily-word`) that re-run the same spec files a second time. Retries
are disabled (`retries: 0`).

Fixed stack overhead (`db`, `pgbouncer`, `redis`, `backend`, `frontend`):
cold Docker build + stack startup took **99.6 s**; steady-state stack startup
with cached images took **33.7 s**. I ran with `CI=true E2E_WORKERS=4` on 4
pinned CPUs.

| Round | Machine 0 | Machine 1 | Machine 2 | Machine 3 | Spread | Prediction (summed worker time) |
| --- | --- | --- | --- | --- | --- | --- |
| Baseline (1 machine, `E2E_WORKERS=4`) | 374.4 s (6m 14s; 70 files / 288 passed) | | | | | |
| 1, by count | 207.7 s (18) | 90.2 s (18) | 67.1 s (17) | 119.7 s (17) | 140.6 s | |
| 2, timed | 207.8 s (1) | 113.7 s (23) | 112.1 s (23) | 125.4 s (23) | 95.7 s (13.3 s m1–m3) | 6m26s, 5m35s, 5m35s, 5m35s |
| 3, timed | 207.9 s (1) | 116.9 s (23) | 121.3 s (23) | 115.0 s (23) | 92.9 s (**6.3 s** m1–m3) | 6m27s, 5m45s, 5m45s, 5m46s |

Every round ran all 288 tests across desktop and mobile projects
(`verify` and `merge` exited 0; `record` summed each file's desktop + mobile
project times automatically).

Why 4 machines hit a **207.9 s (1.80x)** floor here:
`tests/websocket/disconnect-during-game.spec.ts` contains two tests that each
wait out a real ~190 s in-game WebSocket disconnect grace period (386.8 s
summed case time, 207.8 s wall time running side by side under
`fullyParallel: true`). On 4 machines, Bunpō warned that this single file
exceeds the average load, isolated it on machine 0, and balanced the other 69
files across machines 1–3 within 6.3 s (`115.0–121.3 s`).

On **2 machines** (matching Souhib's two self-hosted runners), `bunpo split`
predicts **12m1s vs 12m2s** (31 vs 39 files, 1 s predicted spread, no
dominant-file warning). Because `disconnect-during-game.spec.ts` runs beside
other specs on machine 0's remaining workers, **2 machines capture almost
the full speedup without wasting machines 3 and 4** (and on the slower
2-worker VPS runner, where the suite runs 32–34 min, 2 machines would cut it
roughly in half).

Top slowest specs (summed across projects):
`tests/websocket/disconnect-during-game.spec.ts` (386.8 s),
`tests/codenames/game-voting.spec.ts` (66.5 s),
`tests/codenames/game-gameplay.spec.ts` (59.9 s),
`tests/wordquiz/game-flow.spec.ts` (58.3 s),
`tests/undercover/ready-quorum.spec.ts` (53.4 s).

### Majlisna `front/` (Vitest)

CI runs `bun run test:coverage` with thresholds (`statements: 77, branches: 73,
functions: 76, lines: 78`). I ran `vitest run` using the exact inline
`bunpoReporter` now shipped in `docs/guides/vitest.md` (151 files / 1,584
tests, `happy-dom`).

| Round | Machine 0 | Machine 1 | Machine 2 | Machine 3 | Spread | Prediction |
| --- | --- | --- | --- | --- | --- | --- |
| Baseline (1 machine) | 24.7 s (151 files) | | | | | |
| 1, by count | 7.7 s (38) | 6.7 s (38) | 7.8 s (38) | 6.7 s (37) | 1.08 s | |
| 2, timed | 7.3 s (37) | 7.4 s (38) | 7.8 s (38) | 7.6 s (38) | 0.56 s | 18 s each |
| 3, timed | 7.5 s (37) | 7.5 s (38) | 7.4 s (38) | 7.7 s (38) | **0.32 s** | 18 s each |

The new inline reporter worked on the first try and balanced all four
machines within **0.32 s**. At 24.7 s total, the suite is fast enough to stay
on one runner in CI.

## Guide friction found and fixed

Every gap uncovered across both repositories has been fixed in `docs/guides/`
and `docs/book.md` (and verified by `guidecheck`):

1. **Tests in a subdirectory.** All seven suites live in subdirectories
   (`backend/`, `front/`, `e2e/`). GitHub's `defaults.run.working-directory`
   does not apply to `uses:` steps (`actions/cache`, `upload-artifact`,
   `download-artifact`). Added subdirectory instructions and suite-scoped cache
   and artifact prefixes to all five runner guides.
2. **Separate workflow lanes for the same directory (SQLite vs `--use-postgres`).**
   PostgreSQL and SQLite file durations differ sharply, so two workflows
   targeting `backend/` must use separate cache prefixes (added to
   `docs/guides/pytest.md`, along with per-machine database sidecar guidance).
3. **Coverage thresholds fail every machine.** LaTabdhir `front/` and Majlisna
   `front/` and `backend/` enforce coverage floors, causing passing shards to
   exit 1. Added coverage threshold guidance to pytest, Vitest, Jest, and
   RSpec guides.
4. **Vitest JUnit times leave out most of the cost.** Replaced JUnit in
   `docs/guides/vitest.md` with the inline `bunpoReporter` using
   `mod.diagnostic()` and `bunpo record --format lines` (skipping failed
   modules so failed runs keep prior timings). Verified on Majlisna `front/`
   (0.32 s spread) and in `guidecheck`.
5. **`(path) =>` shadowed `import path from "node:path"` in Vitest configs.**
   Renamed the parameter in `docs/guides/vitest.md` to `(file) =>`.
6. **`pytest-xdist` (`-n auto`) and Playwright `workers > 1` / multi-project
   suites.** Documented summed-seconds predictions and multi-project behavior
   in `docs/guides/pytest.md` and `docs/guides/playwright.md`.
7. **Existing in-repo splits must be removed.** Added a warning in all five
   guides to remove existing `--shard` flags or `conftest.py` shard hooks.
8. **Playwright filtered suites (`--project`, `--grep-invert`) and `dotenv`
   stdout banners.** Added `PLAYWRIGHT_JSON_OUTPUT_NAME=list.json` + `jq`
   discovery, Docker Compose/`workers: 1` overhead notes, and Playwright retry
   timing behavior to `docs/guides/playwright.md`.

## Side findings for Souhib

- **Majlisna `backend/` probabilistic test bug:**
  `tests/api/controllers/test_room_invites.py:116` asserts
  `assert ids["pin"] not in invite.model_dump_json()`. In one run, random
  4-digit PIN `"0423"` appeared inside room UUID `da07a042393d` and failed the
  test (~1-in-1,000 random flake). Checking `"pin" not in invite.model_dump()`
  fixes it permanently.
- **Majlisna `backend/` single 2-minute file:**
  `tests/api/ws/test_wp04_reconnect_and_detach.py` (11 tests) takes 131 s
  under `--use-postgres` (and 118 s on SQLite). On SQLite it is half the
  suite; on Postgres it caps 4-machine scaling at ~2m 10s.
- **Majlisna `e2e/` 190 s WebSocket grace floor:**
  `tests/websocket/disconnect-during-game.spec.ts` runs two ~190 s real-time
  disconnect grace tests, setting a ~3.5 min wall-time floor regardless of
  machine count. Because of that floor, 2 runners give almost the same speedup
  as 4 runners unless that spec's wait timer is made configurable in E2E.
- **LaTabdhir `e2e/` flakes:**
  `tests/cross-flow/full-journey.spec.ts` (`user: signup -> verify -> login ->
  marketplace`) flaked on 2 of 3 rounds and passed on retry #2 both times
  (~60 s penalty each time). `tests/marketplace/store-detail.spec.ts` flaked
  once in round 1 (~34 s).
- **LaTabdhir `backend/` frozen-clock timer quirk:**
  Frozen-clock tests in `tests/crons/` and `tests/integration/test_mfa.py`
  make `pytest --durations` report ~1.79 billion seconds (JUnit XML is
  unaffected). Also `pydantic-core` 2.33.2 requires Python 3.13 or older (CI
  pins 3.12).

## Not done

- No live GitHub Actions workflow run yet (Action install, cache restore/save,
  and artifact upload/download were not exercised on these repositories).
- `e2e-payments.yml` (3 manual specs requiring live Tap sandbox keys) and
  LaTabdhir `mobile/` (`flutter test`, outside Bunpō's five guided runners)
  were not tested.
- Someone outside the project still needs to wire up Bunpō in GitHub Actions
  from the guides alone.
