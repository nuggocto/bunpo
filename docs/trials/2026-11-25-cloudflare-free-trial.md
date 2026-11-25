# Cloudflare Workers Free-plan live-queue trial

Date: 2026-11-25. Two trial sessions deployed from `worker/` (`wrangler 4.167.0`) to a real Cloudflare Workers Free account, each torn down afterward (both Durable Object namespaces removed and Wrangler logged out).

## Run 1: deployment check and GitHub Actions kill runs

- Deploying before uploading secrets returned 200 on `/healthz` and 503 (`server_misconfigured`) on `/api/v1/builds`. After uploading secrets, a wrong token returned 401 and the read token on `POST` returned 403.
- `worker/scripts/trial.mjs` passed all checks under default `MAX_TESTS=5000` (38 requests) and under `MAX_TESTS=20000` (48 requests).
- `wrangler tail` recorded 301 invocations total, all `outcome: "ok"`: 144 Durable Object invocations (113 `BuildDO`, 31 `BuildIndex`, totaling 7.9 s of recorded Durable Object wall time) plus Worker `fetch` invocations. 12 Durable Object invocations used more than 10 ms CPU (up to 35 ms on 20,000-test `init`) and none was stopped.
- The Cloudflare account page after Run 1 showed 183 of 100,000 Worker requests and 1,841 of 200,000 observability events (about 10 events per request with both logs and traces at 100%).
- Dead-runner reassignment (10 s lease TTL across DO hibernation) completed in 15,463 ms and 15,325 ms server time (client polls once per second after 11 s).
- Two 3-machine GitHub Actions kill runs (15 s lease TTL) finished all 9 tests with one accepted result each, the killed machine's expired attempt visible, and `report-0.json` written. The killed machine's test was leased again after 15,513 ms when killed before running tests and after 15,039 ms when killed after running tests and before reporting.
- Teardown: `wrangler delete` removed both Durable Object namespaces (`BuildDO` and `BuildIndex`: 2 before, 0 after, checked via the Cloudflare API). It prints a warning about Container applications that does not apply. `wrangler secret put` left every deployment's source as `"wrangler"`, and no script API override prompt appeared on the two later deploys.
- Earlier local Node CPU estimates for `init` at 5,000 tests were 2.02 to 3.80 ms, while real Durable Object CPU in Run 1 was 17 to 28 ms (about 5 to 14x higher). Local Node CPU timings do not predict Cloudflare Durable Object CPU.

### Run 1 Durable Object CPU time (`wrangler tail`)

Two runs of `trial.mjs`: first at `MAX_TESTS=5000`, second at `MAX_TESTS=20000`. All other `BuildDO` calls used at most 6 ms CPU, `BuildIndex` used 0 ms, and Worker `fetch` used at most 5 ms.

| Test files | `init` CPU (5k / 20k run) | Wake `status` CPU (5k / 20k run) | Deadline `alarm` CPU (5k / 20k run) |
| --- | ---: | ---: | ---: |
| 1,000 | 14 ms / 12 ms | 3 ms / 3 ms | 9 ms / 10 ms |
| 5,000 | 28 ms / 17 ms | 11 ms / 6 ms | 24 ms / 12 ms |
| 10,000 | — / 20 ms | — / 9 ms | — / 20 ms |
| 20,000 | — / 35 ms | — / 16 ms | — / 23 ms |

### Run 1 client wall time (`trial.mjs`)

| Test files | `MAX_TESTS=5000` `init` / `wake` (15 s idle) | `MAX_TESTS=20000` `init` / `wake` (15 s idle) |
| --- | ---: | ---: |
| 1,000 | 316 ms / 99 ms | 329 ms / 127 ms |
| 5,000 | 479 ms / 163 ms | 398 ms / 105 ms |
| 10,000 | — | 505 ms / 101 ms |
| 20,000 | — | 512 ms / 121 ms |

### Run 1 dead-runner reassignment

| Scenario | Lease TTL | Server reassignment time | Notes |
| --- | ---: | ---: | --- |
| `trial.mjs` dead runner (5k run) | 10 s | 15,463 ms | 11 s sleep + 1/s polling across DO hibernation |
| `trial.mjs` dead runner (20k run) | 10 s | 15,325 ms | 11 s sleep + 1/s polling across DO hibernation |
| 3 GitHub runners, kill before run | 15 s | 15,513 ms | 9/9 passed, 1 accepted each, `report-0.json` written |
| 3 GitHub runners, kill after run, before report | 15 s | 15,039 ms | 9/9 passed, 1 accepted each, `report-0.json` written |

## Run 2: heartbeats and sustained load

Deployed from commit `6e4e00b` with `MAX_TESTS=20000` via `--var` and `observability.traces.head_sampling_rate` set to `0.05`. Driven by a real `bunpo` binary built from `6e4e00b` on one machine plus a one-off load script (not in the repository), then torn down again (both namespaces removed, Wrangler logged out).

### Heartbeats

- **8 machines, 400 files of 3 s (`--lease-ttl 15s`, `--max-batch 10`):** Passed with all 400 files accepted once. Leases lasted up to 30,132 ms against the 15 s TTL and none expired except the killed machine's. Machine 7 was `SIGKILL`ed about 20 s in; its 10 files expired and were leased again alone after first attempts finished (146,296 to 152,259 ms after the kill), matching the deal order where first attempts come before retries.
- **1 machine, one 150 s file (`--lease-ttl 60s`):** One attempt, passed, lease held for 150,089 ms with heartbeats 20 s apart (past the ~10 s Durable Object hibernation delay).
- **Heartbeat CPU:** 114 heartbeat calls recorded, at most 1 ms CPU each (a lower bound because `wrangler tail` missed events under load).

### Sustained load

40 builds started over 50 s: 20 builds of 5,000 files 1.5 s apart, then 10 builds of 20,000 files 2 s apart, then 10 builds of 20,000 files at once. Each build ran `init`, 3 leases, 3 reports, and 2 `status` reads after 16 s idle: 360 requests total, all HTTP 200. All 40 builds reached their 5-minute deadline and became `unresolved`, as intended.

- **Client wall time:** Longest `init` 651 ms (5,000 files) and 1,936 ms (20,000 files); longest wake `status` 223 ms (5,000) and 217 ms (20,000).
- **Recorded CPU (`wrangler tail`):**
  - `BuildDO` `init` at 5,000 files: 14 to 46 ms, median 24 ms (18 recorded).
  - `BuildDO` `init` at 20,000 files: 31 to 73 ms, median 49 ms (16 recorded).
  - `BuildDO` `status`: up to 54 ms; deadline `alarm`: 7 to 43 ms (39 recorded); `lease`: up to 11 ms; `report`: up to 7 ms.
  - Worker `fetch`: up to 8 ms while the 20,000-file builds were created (1.04 MB `init` bodies) and up to 6 ms for the 5,000-file builds (0.26 MB). One small request earlier in Run 2 also took 7 ms, so body size does not explain every peak. The 10 ms Workers Free CPU limit applies to `fetch`, and a body near the 4 MiB limit could exceed it (not tested).
- **Recording totals and tail loss:** The whole Run 2 `wrangler tail` recording captured 927 invocations, all `outcome: "ok"`, 118 above 10 ms CPU (73 ms largest), none stopped. Under this load `wrangler tail` dropped events (216 of the 360 `BuildDO` calls in the sustained part were recorded); the client received every response.
- **Full-day analytics:** Cloudflare analytics showed 929 Worker requests and 0 errors for the whole day across both runs. Durable Object analytics figures were incomplete and changed between queries, so none are quoted.

## Limits of this evidence

- Cloudflare's Durable Objects CPU documentation conflicts on Free: the [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) table lists 30 seconds default with no Free value, while its FAQ says "Durable Objects are Worker scripts, and have the same per invocation CPU limits as any Workers do," and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) gives 10 ms on Free. Across both runs, 21 builds of 20,000 files passed and 130 Durable Object calls exceeded 10 ms (up to 73 ms) with none stopped. A strictly enforced 10 ms DO limit would hit 1,000 files too (`init` used 12 to 14 ms); if enforced, the Durable Object throws, the Worker returns 503 `storage_error`, and runners retry for 2 minutes before exiting 1 (never a false pass).
- `MAX_TESTS` stays at 5,000 by default for Worker `fetch` CPU margin on larger request bodies (up to 8 ms with 1.04 MB bodies against the 10 ms Workers Free limit) and daily SQLite row writes, rather than a Durable Object limit observed in these runs.
- Still untested on the real account: daily quota exhaustion, the 7-day `deleteAll()` deletion, more than 8 machines, builds longer than 3 minutes of work, and request bodies near 4 MiB.

## Repeating the deployment check

After deploying `worker/` and uploading `BUNPO_QUEUE_TOKEN` and `BUNPO_QUEUE_READ_TOKEN` (see [docs/self-hosting.md](../self-hosting.md)), start a `wrangler tail` capture in a second terminal to record CPU times without logging headers or tokens:

```bash
(cd worker && npx wrangler tail --format json) | jq --unbuffered -c '{
  outcome,
  executionModel,
  entrypoint: (.entrypoint // "fetch"),
  rpcMethod: (.event.rpcMethod // null),
  cpuTime,
  wallTime,
  exceptions: [(.exceptions // [])[] | {name, message}]
}' > /tmp/bunpo-tail.jsonl
```

In the first terminal, preview the request plan and run `worker/scripts/trial.mjs`:

```bash
export BUNPO_QUEUE_URL="https://bunpo-queue.<subdomain>.workers.dev"
node worker/scripts/trial.mjs        # dry run, exits 2
node worker/scripts/trial.mjs --yes  # 38 requests at MAX_TESTS=5000, 48 at 20000
```

To test 10,000 and 20,000 tests temporarily, deploy with `--var MAX_TESTS:20000`, re-run `trial.mjs --yes`, then restore defaults with `npx wrangler deploy`. Stop `wrangler tail` with `Ctrl-C` and summarize the captured invocations:

```bash
jq -s '
  group_by([.entrypoint, (.rpcMethod // "-")]) |
  map({
    entrypoint: .[0].entrypoint,
    method: (.[0].rpcMethod // "-"),
    count: length,
    maxCpuMs: (map(.cpuTime // 0) | max),
    maxWallMs: (map(.wallTime // 0) | max),
    nonOk: map(select(.outcome != "ok") | {outcome, exceptions})
  })
' /tmp/bunpo-tail.jsonl
```
