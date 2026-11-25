# Cloudflare Workers Free-plan live-queue trial

Date: 2026-11-25. Deployed from `worker/` (`wrangler 4.167.0`) to a real Cloudflare Workers Free account, then torn down afterward.

## Summary

- Deploying before uploading secrets returned 200 on `/healthz` and 503 (`server_misconfigured`) on `/api/v1/builds`. After uploading secrets, a wrong token returned 401 and the read token on `POST` returned 403.
- `worker/scripts/trial.mjs` passed all checks under default `MAX_TESTS=5000` (38 requests) and under `MAX_TESTS=20000` (48 requests).
- `wrangler tail` recorded 301 invocations total, all `outcome: "ok"`: 144 Durable Object invocations (113 `BuildDO`, 31 `BuildIndex`, totaling 7.9 s of recorded Durable Object wall time) plus Worker `fetch` invocations. 12 Durable Object invocations used more than 10 ms CPU (up to 35 ms on 20,000-test `init`) and none was stopped.
- The Cloudflare account page for the trial day showed 183 of 100,000 Worker requests and 1,841 of 200,000 observability events (about 10 events per request with both logs and traces at 100%).
- Dead-runner reassignment (10 s lease TTL across DO hibernation) completed in 15,463 ms and 15,325 ms server time (client polls once per second after 11 s).
- Two 3-machine GitHub Actions kill runs (15 s lease TTL) finished all 9 tests with one accepted result each, the killed machine's expired attempt visible, and `report-0.json` written. The killed machine's test was leased again after 15,513 ms when killed before running tests and after 15,039 ms when killed after running tests and before reporting.
- Teardown: `wrangler delete` removed both Durable Object namespaces (`BuildDO` and `BuildIndex`: 2 before, 0 after, checked via the Cloudflare API). It prints a warning about Container applications that does not apply. `wrangler secret put` left every deployment's source as `"wrangler"`, and no script API override prompt appeared on the two later deploys.
- Earlier local Node CPU estimates for `init` at 5,000 tests were 2.02 to 3.80 ms, while real Durable Object CPU was 17 to 28 ms (about 5 to 14x higher). Local Node CPU timings do not predict Cloudflare Durable Object CPU.

## Measurements

### Durable Object CPU time (`wrangler tail`)

Two runs of `trial.mjs`: run 1 at `MAX_TESTS=5000`, run 2 at `MAX_TESTS=20000`. All other `BuildDO` calls used at most 6 ms CPU, `BuildIndex` used 0 ms, and Worker `fetch` used at most 5 ms.

| Test files | `init` CPU (run 1 / run 2) | Wake `status` CPU (run 1 / run 2) | Deadline `alarm` CPU (run 1 / run 2) |
| --- | ---: | ---: | ---: |
| 1,000 | 14 ms / 12 ms | 3 ms / 3 ms | 9 ms / 10 ms |
| 5,000 | 28 ms / 17 ms | 11 ms / 6 ms | 24 ms / 12 ms |
| 10,000 | — / 20 ms | — / 9 ms | — / 20 ms |
| 20,000 | — / 35 ms | — / 16 ms | — / 23 ms |

### Client wall time (`trial.mjs`)

| Test files | Run 1 `init` / `wake` (15 s idle) | Run 2 `init` / `wake` (15 s idle) |
| --- | ---: | ---: |
| 1,000 | 316 ms / 99 ms | 329 ms / 127 ms |
| 5,000 | 479 ms / 163 ms | 398 ms / 105 ms |
| 10,000 | — | 505 ms / 101 ms |
| 20,000 | — | 512 ms / 121 ms |

### Dead-runner reassignment

| Scenario | Lease TTL | Server reassignment time | Notes |
| --- | ---: | ---: | --- |
| `trial.mjs` dead runner (run 1) | 10 s | 15,463 ms | 11 s sleep + 1/s polling across DO hibernation |
| `trial.mjs` dead runner (run 2) | 10 s | 15,325 ms | 11 s sleep + 1/s polling across DO hibernation |
| 3 GitHub runners, kill before run | 15 s | 15,513 ms | 9/9 passed, 1 accepted each, `report-0.json` written |
| 3 GitHub runners, kill after run, before report | 15 s | 15,039 ms | 9/9 passed, 1 accepted each, `report-0.json` written |

## Limits of this evidence

- Only one run tested 10,000 and 20,000 tests, with no sustained load.
- Cloudflare's Durable Objects CPU documentation conflicts on Free: the [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) table lists 30 seconds default with no Free value, while its FAQ says "Durable Objects are Worker scripts, and have the same per invocation CPU limits as any Workers do," and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) gives 10 ms on Free. In this trial 12 Durable Object invocations exceeded 10 ms under light use without being stopped, but if a 10 ms limit were enforced it would hit 1,000 files too (`init` used 12 to 14 ms). If enforced, the Durable Object throws, the Worker returns 503 `storage_error`, and runners retry for 2 minutes before exiting 1 (never a false pass). `MAX_TESTS` stays at 5,000 by default and can be raised up to 20,000.
- Zero heartbeat calls reached the real account because every batch finished before `TTL / 3`. Heartbeats on Cloudflare are covered only by local `workerd` tests.
- Also untested on the real account: more than 3 machines, long builds, the 7-day `deleteAll()` cleanup alarm, and daily quota exhaustion.

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
