# Cloudflare Workers Free-plan trial runbook

Manual checklist for verifying Bunpō's live queue on your own Cloudflare account (Workers Free) before tagging `v0.1.0`. Nothing in this repository touches your Cloudflare account automatically.

## Prerequisites

- Node.js 26+ and a clean checkout of this repository.
- A Cloudflare account (Free plan is fine; Workers Paid is not required).
- `gh` CLI (optional, for triggering the GitHub Actions workflow from the terminal).

## Step 1: Install Worker dependencies (scripts disabled)

```sh
cd worker
npm ci --ignore-scripts
```

**Expected:** clean install with zero lifecycle scripts run.

## Step 2: Log in to Cloudflare

```sh
npx wrangler login
```

**Expected:** browser opens Cloudflare OAuth and prints `Successfully logged in.`

## Step 3: Generate and set bearer tokens

Generate two distinct 32-byte random hex tokens and keep them in your current shell (do not commit them):

```sh
export BUNPO_QUEUE_TOKEN=$(openssl rand -hex 32)
export BUNPO_QUEUE_READ_TOKEN=$(openssl rand -hex 32)
```

Upload both as Worker secrets:

```sh
printf '%s' "$BUNPO_QUEUE_TOKEN" | npx wrangler secret put BUNPO_QUEUE_TOKEN
printf '%s' "$BUNPO_QUEUE_READ_TOKEN" | npx wrangler secret put BUNPO_QUEUE_READ_TOKEN
```

**Expected:** If `bunpo-queue` does not exist yet, Wrangler creates a draft Worker automatically when stdin is piped (interactively it asks `There doesn't seem to be a Worker called "bunpo-queue". Do you want to create a new Worker with that name and add secrets to it?` — answer yes) and prints `Success! Uploaded secret ...` for each token.

## Step 4: Deploy the Worker and dashboard assets

```sh
npx wrangler deploy
```

**Expected:** If your account has no `workers.dev` subdomain yet, Wrangler warns and asks `Would you like to register a workers.dev subdomain now?` (answer yes to register one, or decline and configure a custom route in `wrangler.jsonc`). Wrangler then deploys `bunpo-queue`, creates SQLite Durable Object classes `BuildDO` and `BuildIndex`, uploads the static dashboard assets, and prints your URL, e.g. `https://bunpo-queue.<subdomain>.workers.dev`. Export it:

```sh
export BUNPO_QUEUE_URL="https://bunpo-queue.<subdomain>.workers.dev"
```

## Step 5: Smoke-check `/healthz` and the dashboard

```sh
curl -fsS "$BUNPO_QUEUE_URL/healthz"
```

**Expected:** `{"ok":true}`.

Open `$BUNPO_QUEUE_URL` in a browser, paste `$BUNPO_QUEUE_READ_TOKEN`, and confirm the empty **Recent builds** view loads with zero console errors or external requests.

## Step 6: Run the bounded trial script (~60 requests)

From the repository root, preview the request plan first (dry run):

```sh
node worker/scripts/trial.mjs
```

**Expected:** prints target URL, planned request count (~60, hard cap 1,500), and exits `2` without sending requests.

Now run the trial:

```sh
node worker/scripts/trial.mjs --yes
```

**Expected:**
- `healthz`, `auth-wrong-token-401`, `auth-read-post-403`: `PASS`.
- Size sweep (`1000`, `5000`): `PASS` with `init`, `lease`, `report`, `status`, `list`, and `wake(15s)` timings; `10000` and `20000` show `SKIP (MAX_TESTS)` under the default `MAX_TESTS=5000`.
- `retry-behavior`: `PASS` (attempt 1 `error` -> attempt 2 `passed`).
- `dead-runner-1.5-ttl`: `PASS` (lease reassigned across 10 s DO hibernation within `1.5 * TTL`).
- `dashboard-headers`: `PASS` (strict CSP, `nosniff`, `no-referrer`, `Cache-Control`, and JSON `404` on `/_headers`).

### Optional: test `MAX_TESTS=20000`

To see whether `10,000` and `20,000` tests stay under Cloudflare's Free-plan CPU limit on your account, temporarily deploy with `--var MAX_TESTS:20000` (without editing `wrangler.jsonc`), re-run `node worker/scripts/trial.mjs --yes`, and inspect the Cloudflare dashboard:

```sh
(cd worker && npx wrangler deploy --var MAX_TESTS:20000)
node worker/scripts/trial.mjs --yes
```

Check **Workers & Pages -> bunpo-queue -> Metrics & Logs** in the Cloudflare dashboard:
- **CPU time (p50 and p99):** confirm p99 stays under `10 ms` (or note the largest size that succeeds without error `1102` / "exceeded CPU").
- **Errors:** confirm zero `1102` CPU errors and zero quota errors.
- **Durable Objects:** check `BuildDO` / `BuildIndex` requests and SQLite rows written.

Restore the default `MAX_TESTS=5000` afterwards:

```sh
(cd worker && npx wrangler deploy)
```

## Step 7: Run the real GitHub Actions matrix trial (3 machines)

1. In your GitHub repository (**Settings -> Secrets and variables -> Actions**):
   - Add **Variable** `BUNPO_QUEUE_URL` = your deployed `https://...` URL.
   - Add **Secret** `BUNPO_QUEUE_TOKEN` = your write token.
   - Add **Secret** `BUNPO_QUEUE_READ_TOKEN` = your read token.
2. Trigger **Actions -> Queue Trial (Manual)** (`.github/workflows/queue-trial.yml`) twice:
   - First with `scenario = kill-before-run`
   - Second with `scenario = kill-after-run`
   Or via `gh`:
   ```sh
   gh workflow run queue-trial.yml -f scenario=kill-before-run
   gh workflow run queue-trial.yml -f scenario=kill-after-run
   ```

**Expected:** both runs finish green; in each `verify` job, `bunpo --status` prints `verdict passed` and writes `report-0.json`, and `assert-queue-trial.mjs` prints `PASS` confirming 9/9 tests passed with exactly one accepted result each and machine 0's expired attempt visible at generation >= 2. Open `$BUNPO_QUEUE_URL` in your browser to inspect both builds' timelines and attempt history.

## Step 8: Undo / teardown

Remove the Worker, its Durable Objects, and all stored SQLite data:

```sh
cd worker
npx wrangler delete
```

**Expected:** Wrangler prompts for confirmation and deletes `bunpo-queue` and its Durable Object storage.

Remove the temporary GitHub Actions configuration from **Settings -> Secrets and variables -> Actions** (`BUNPO_QUEUE_URL` variable, `BUNPO_QUEUE_TOKEN` and `BUNPO_QUEUE_READ_TOKEN` secrets), and optionally log out of Wrangler:

```sh
npx wrangler logout
```
