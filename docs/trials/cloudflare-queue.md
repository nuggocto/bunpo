# Cloudflare Workers Free-plan trial runbook

Manual checklist for verifying Bunpō's live queue on your own Cloudflare account (Workers Free) before tagging `v0.1.0`. Nothing in this repository touches your Cloudflare account automatically.

**Run `bash` first.** Every command below is written for `bash` (if your default shell is `fish`, start `bash` first) and assumes your working directory is the **repository root**.

## Cost and time

- **Cost:** Free. The Workers Free plan cannot bill you. The whole trial sends roughly `150–200` requests total out of `100,000`/day (`38` requests for `trial.mjs` at `MAX_TESTS=5000`, `48` at `MAX_TESTS=20000`, `57` planned max, plus ~`50–80` across the two 3-machine workflow runs).
- **Time:** about `10–15 minutes` end to end (`trial.mjs` takes ~`45 s` at `MAX_TESTS=5000` or ~`75 s` at `MAX_TESTS=20000`; each GitHub Actions workflow run takes ~`1–2 min`).

## Prerequisites

- `bash`, Node.js 26+, `jq`, `curl`, `openssl`, and a clean checkout of this repository.
- A Cloudflare account (Workers Free).
- `gh` CLI logged in to your GitHub repository (or use the GitHub Settings web UI).

## Step 1: Install Worker dependencies (scripts disabled)

```bash
(cd worker && npm ci --ignore-scripts)
```

**Expected:** clean install with zero lifecycle scripts run.

## Step 2: Log in to Cloudflare

```bash
(cd worker && npx wrangler login)
```

**Expected:** browser opens Cloudflare OAuth and prints `Successfully logged in.`

## Step 3: Deploy the Worker first (closed by default without secrets)

Deploying before setting secrets avoids creating a draft Worker. Until both tokens are uploaded, the Worker refuses every `/api/v1/*` request with `503`.

```bash
(cd worker && npx wrangler deploy)
```

**Expected:** If your account has no `workers.dev` subdomain yet, Wrangler warns and asks `Would you like to register a workers.dev subdomain now?` (answer `y` to register one, or decline and configure a custom route in `worker/wrangler.jsonc`). Wrangler deploys `bunpo-queue`, creates SQLite Durable Object classes `BuildDO` and `BuildIndex`, uploads the static dashboard assets, and prints your URL, e.g. `https://bunpo-queue.<subdomain>.workers.dev`.

Export your deployed URL and verify it refuses API calls before secrets exist:

```bash
export BUNPO_QUEUE_URL="https://bunpo-queue.<subdomain>.workers.dev"
curl -sS -w "\nHTTP %{http_code}\n" "$BUNPO_QUEUE_URL/healthz"
curl -sS -w "\nHTTP %{http_code}\n" "$BUNPO_QUEUE_URL/api/v1/builds"
```

**Expected:**
- `/healthz`: `{"ok":true}` and `HTTP 200`.
- `/api/v1/builds`: `{"version":1,"error":{"code":"server_misconfigured","message":"BUNPO_QUEUE_TOKEN must be set and at least 16 bytes"}}` and `HTTP 503`.

## Step 4: Generate and upload both bearer tokens

```bash
export BUNPO_QUEUE_TOKEN=$(openssl rand -hex 32)
export BUNPO_QUEUE_READ_TOKEN=$(openssl rand -hex 32)
printf '%s' "$BUNPO_QUEUE_TOKEN" | (cd worker && npx wrangler secret put BUNPO_QUEUE_TOKEN)
printf '%s' "$BUNPO_QUEUE_READ_TOKEN" | (cd worker && npx wrangler secret put BUNPO_QUEUE_READ_TOKEN)
```

**Expected:** Wrangler prints `Success! Uploaded secret ...` for each token.

Verify the read token now works:

```bash
curl -fsS -H "Authorization: Bearer $BUNPO_QUEUE_READ_TOKEN" "$BUNPO_QUEUE_URL/api/v1/builds"
```

**Expected:** `{"version":1,"builds":[]}`.

## Step 5: Open the dashboard in a browser

Print the read token to copy into the browser sign-in form:

```bash
printf '%s\n' "$BUNPO_QUEUE_READ_TOKEN"
```

Open `$BUNPO_QUEUE_URL` in your browser, paste the token, and confirm the empty **Recent builds** view loads with zero console errors and zero external network requests.

## Step 6: Start CPU/outcome tail capture in a second terminal

`trial.mjs` measures wall latency; Cloudflare's trace stream records actual `cpuTime` and `outcome` per entrypoint and Durable Object RPC method. Open a **second terminal**, start `bash` in the repository root, and run:

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

This keeps **only** `outcome`, `executionModel`, `entrypoint`, `rpcMethod`, `cpuTime`, `wallTime`, and exception names/messages (no request headers, URLs, IPs, or `cf` metadata). Leave it running during Step 7.

## Step 7: Run `trial.mjs`

Back in your **first terminal** (repository root), preview the request plan first (dry run):

```bash
node worker/scripts/trial.mjs
```

**Expected:** prints `Planned requests: up to 57 (hard client cap: 1500)` and exits `2` without sending requests.

Run the trial under the default `MAX_TESTS=5000` (~`45 s`, `38` requests sent):

```bash
node worker/scripts/trial.mjs --yes
```

**Expected:**
- `healthz`, `auth-wrong-token-401`, `auth-read-post-403`: `PASS`.
- Size sweep: `1000` and `5000` `PASS` with `init`, `lease`, `report`, `status`, `list`, and `wake(15s)` timings; `10000` and `20000` show `SKIP (MAX_TESTS)`.
- `retry-behavior`: `PASS`.
- `dead-runner-1.5-ttl`: `PASS` (reassigned within `1.5 * TTL` across 10 s DO hibernation).
- `dashboard-headers`: `PASS`.

### Optional: measure `10,000` and `20,000` tests (`MAX_TESTS=20000`)

Temporarily raise `MAX_TESTS` without editing `worker/wrangler.jsonc` (~`75 s`, `48` requests sent):

```bash
(cd worker && npx wrangler deploy --var MAX_TESTS:20000)
node worker/scripts/trial.mjs --yes
```

Because `wrangler secret put` updated the Worker via the script API in Step 4, Wrangler may warn:
`You are about to upload a Worker that was last updated via the script API. Edits that have been made via the script API will be overridden by your local code and config. Would you like to continue?`
Answer `y` — secrets uploaded with `wrangler secret put` stay intact.

Restore the default `MAX_TESTS=5000` afterwards:

```bash
(cd worker && npx wrangler deploy)
```

### Summarize captured CPU times

Switch to your **second terminal**, press `Ctrl-C` to stop `wrangler tail`, and run:

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

**Expected:** every `(entrypoint, method)` group has `nonOk: []`, and `maxCpuMs` shows actual Worker and Durable Object CPU time for `init`, `lease`, `report`, `status`, and `list`.

## Step 8: Run the 3-machine GitHub Actions kill trial

Set the GitHub Actions variable and secrets with `gh` (or in **Settings -> Secrets and variables -> Actions**):

```bash
gh variable set BUNPO_QUEUE_URL --body "$BUNPO_QUEUE_URL"
printf '%s' "$BUNPO_QUEUE_TOKEN" | gh secret set BUNPO_QUEUE_TOKEN
printf '%s' "$BUNPO_QUEUE_READ_TOKEN" | gh secret set BUNPO_QUEUE_READ_TOKEN
```

Trigger **Queue Trial (Manual)** (`.github/workflows/queue-trial.yml`) for both scenarios:

```bash
gh workflow run queue-trial.yml -f scenario=kill-before-run
gh workflow run queue-trial.yml -f scenario=kill-after-run
```

**Expected:** both runs pass (~`1–2 min` each). Machines `1` and `2` wait until machine `0` joins and leases a batch; machine `0` is killed as configured; survivors finish all `9` tests; in `verify`, `bunpo --status` prints `verdict passed` and writes `report-0.json`, and `assert-queue-trial.mjs` confirms `9` tests passed with exactly one accepted result each and machine `0`'s expired attempt visible at generation >= 2. Open `$BUNPO_QUEUE_URL` in your browser to inspect both builds.

## Step 9: Undo / teardown

1. Delete the Worker and log out of Wrangler:

   ```bash
   (cd worker && npx wrangler delete && npx wrangler logout)
   rm -f /tmp/bunpo-tail.jsonl
   ```

2. Open **Cloudflare Dashboard -> Workers & Pages -> Durable Objects** and confirm `BuildDO` and `BuildIndex` are gone. If either namespace still appears, delete it in the dashboard (or re-deploy with a `deleted_classes` migration for `["BuildDO", "BuildIndex"]` before running `wrangler delete`).

3. Remove the temporary GitHub Actions variable and secrets (or delete them in **Settings -> Secrets and variables -> Actions**):

   ```bash
   gh variable delete BUNPO_QUEUE_URL
   gh secret delete BUNPO_QUEUE_TOKEN
   gh secret delete BUNPO_QUEUE_READ_TOKEN
   ```

## What to send back

1. Full output of `node worker/scripts/trial.mjs --yes` (default `5000`, and `20000` if run).
2. Output of the `jq -s` CPU summary from `/tmp/bunpo-tail.jsonl`.
3. Links to both `Queue Trial (Manual)` GitHub Actions runs (`kill-before-run` and `kill-after-run`).
4. Anything unexpected in the Cloudflare dashboard (Metrics, Logs, or Durable Objects) or in the Bunpō web dashboard.
