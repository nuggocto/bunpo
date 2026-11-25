# Self-hosting the live queue

Bunpō's live queue runs on either of two servers speaking the same HTTP JSON protocol ([docs/queue-protocol.md](queue-protocol.md)) and serving the same read-only web dashboard:

- **Cloudflare Worker (`worker/`):** zero-ops scale-to-zero on SQLite-backed Durable Objects and free Workers Static Assets.
- **Standalone Go server (`bunpo-server`):** one static binary on a VPS, VM, or container, in memory or persisted to disk with `--data DIR`.

If your CI machines are identical and start together, static splitting (`bunpo split` / `verify`) needs no server and avoids extra runner startups.

## 1. Generate bearer tokens

Both servers require two distinct tokens, each at least 16 bytes long:

- `BUNPO_QUEUE_TOKEN`: write token for runners (`bunpo --queue ...`).
- `BUNPO_QUEUE_READ_TOKEN`: read-only token for `--status` and the web dashboard.

Generate two 32-byte random hex tokens and hold them in your current shell:

```sh
export BUNPO_QUEUE_TOKEN=$(openssl rand -hex 32)
export BUNPO_QUEUE_READ_TOKEN=$(openssl rand -hex 32)
```

Never pass tokens on the command line or commit them to `worker/wrangler.jsonc`. Both servers refuse to start or fail closed if tokens are missing, shorter than 16 bytes, or equal.

## 2. Cloudflare Worker

Every command below starts from a clean checkout of this repository, after exporting `BUNPO_QUEUE_TOKEN` and `BUNPO_QUEUE_READ_TOKEN` from step 1.

### First-time deploy

1. Install pinned dev dependencies with lifecycle scripts disabled:

   ```sh
   cd worker
   npm ci --ignore-scripts
   ```

2. Authenticate Wrangler with your Cloudflare account:

   ```sh
   npx wrangler login
   ```

3. Deploy the Worker, Durable Object migrations, and dashboard static assets first:

   ```sh
   npx wrangler deploy
   ```

   Deploying before setting secrets avoids creating a draft Worker; until both tokens are uploaded, the Worker answers `/healthz` with `200` and refuses every `/api/v1/*` request with `503 server_misconfigured`. If your account has no `workers.dev` subdomain yet, Wrangler warns and asks `Would you like to register a workers.dev subdomain now?` (default no). Answer yes to register one, or decline and configure a custom route in `worker/wrangler.jsonc`. Once deployed, Wrangler prints the URL (for example `https://bunpo-queue.<subdomain>.workers.dev`).

4. Upload both bearer tokens as Worker secrets:

   ```sh
   printf '%s' "$BUNPO_QUEUE_TOKEN" | npx wrangler secret put BUNPO_QUEUE_TOKEN
   printf '%s' "$BUNPO_QUEUE_READ_TOKEN" | npx wrangler secret put BUNPO_QUEUE_READ_TOKEN
   ```

5. Verify `/healthz` and authentication:

   ```sh
   curl -fsS "https://bunpo-queue.<subdomain>.workers.dev/healthz"
   # {"ok":true}

   curl -fsS -H "Authorization: Bearer $BUNPO_QUEUE_READ_TOKEN" \
     "https://bunpo-queue.<subdomain>.workers.dev/api/v1/builds"
   # {"version":1,"builds":[]}
   ```

6. Open the URL in a browser and paste `$BUNPO_QUEUE_READ_TOKEN` to view the dashboard.

### Wire GitHub Actions

In your repository's **Settings -> Secrets and variables -> Actions**:

- **Variables:** set `BUNPO_QUEUE_URL` to the deployed `https://...` URL (no trailing slash). In a public repository, repository variables appear in public workflow logs; the URL is not a secret because every `/api/v1/*` request requires a bearer token.
- **Secrets:** set `BUNPO_QUEUE_TOKEN` (write) and `BUNPO_QUEUE_READ_TOKEN` (read).

Fork pull requests do not receive repository secrets; keep static splitting for fork PRs.

### Update and teardown

To deploy an update:

```sh
git pull
cd worker
npm ci --ignore-scripts
npx wrangler deploy
```

To remove the Worker and all Durable Object data:

```sh
cd worker
npx wrangler delete
npx wrangler logout
```

`wrangler delete` asks for confirmation and removes the Worker together with both Durable Object namespaces (`BuildDO` and `BuildIndex`). It prints a warning about Container applications that does not apply. Remove `BUNPO_QUEUE_URL`, `BUNPO_QUEUE_TOKEN`, and `BUNPO_QUEUE_READ_TOKEN` from GitHub Actions settings if you are decommissioning the server.

### Cloudflare Free-plan limits and failure modes

Free-plan limits are per account (shared with all other Workers on the account) and reset at 00:00 UTC. See [docs/decisions/0003-live-queue.md](decisions/0003-live-queue.md) for the local `workerd` row, request, and duration table, and [docs/trials/2026-11-25-cloudflare-free-trial.md](trials/2026-11-25-cloudflare-free-trial.md) for real Free-plan CPU measurements and how to run `worker/scripts/trial.mjs`.

| Resource | Cloudflare Free limit | Bunpō behavior |
| --- | --- | --- |
| Worker requests | 100,000 / day (Error 1027 past limit) | Static dashboard assets (`/`, `/dashboard/*`) bypass the Worker and cost 0 Worker requests. |
| Durable Object requests | 100,000 / day | First limit reached on large matrices (64 machines); increase `--lease-ttl` up to 10m to cut heartbeats. |
| Durable Object SQLite rows written | 100,000 / day | Event-sourced log plus throttled `lease_deadlines` meta row keeps writes in the hundreds to low thousands per build. |
| Durable Object SQLite rows read | 5,000,000 / day | Catalog and event log stay compact; builds replay on wake only. |
| Durable Object duration | 13,000 GB-s / day | Per [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), idle hibernation-eligible DOs are not billed for duration even before hibernating (the 76.25 GB-s continuous 10-minute figure in ADR 0003 is a conservative upper bound; the trial recorded 7.9 s total DO wall time). |
| Durable Object storage | 5 GB per account; per object, the FAQ states 1 GB on Free while the limits table lists 10 GB | Finished builds call `deleteAll()` automatically 7 days after completion. |
| Observability (logs and traces) | 200,000 events / day; replaced from 1 December 2026 by 0.5 GB / day shared by logs and traces (Cloudflare's pricing page states the 0.5 GB/day quota stops ingesting until 00:00 UTC) | At 100% logs and traces, the first trial used about 10 events/request (~20,000 requests, or 35 to 48 8-machine builds/day). `worker/wrangler.jsonc` sets `observability.traces.head_sampling_rate` to `0.05` and keeps logs at 100% so `do_rpc_error` is not sampled away. Adjust `head_sampling_rate` or set `observability.traces.enabled` to `false` in `worker/wrangler.jsonc` and redeploy. Cloudflare's docs do not say reaching the observability limit affects serving requests. |
| CPU time | Conflicting docs: [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) table lists 30 s default with no Free value, its FAQ says DOs share Worker CPU limits, and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) gives 10 ms on Free | `MAX_TESTS` defaults to 5,000 in `worker/wrangler.jsonc` and goes up to 20,000. Across two Free-plan trials, 21 builds of 20,000 files passed and 130 DO calls exceeded 10 ms (up to 73 ms) with none stopped, while Worker `fetch` reached 8 ms on 20,000-file (~1.3 MB) bodies (bodies near 4 MiB were not tested). `MAX_TESTS` stays at 5,000 for Worker `fetch` CPU margin on larger bodies and daily row writes. A strictly enforced 10 ms DO limit would hit 1,000 files too (`init` used 12 to 14 ms); run `worker/scripts/trial.mjs` on your own deployment before raising `MAX_TESTS`. |

What happens on failures and limits:

- **Durable Object limit or stub error (storage failure, CPU limit, DO overload, or DO daily quota):** Per [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), past any Durable Object Free limit "further operations of that type will fail with an error." `worker/src/index.ts` catches exceptions from `BuildDO` and `BuildIndex` stubs, logs a structured JSON `do_rpc_error` entry (never tokens or bodies), and returns 503 with `{"version":1,"error":{"code":"storage_error","message":"durable object request failed"}}`.
- **Edge or Worker rejection (Worker daily quota Error 1027, rate limit, or Worker CPU limit):** Cloudflare returns 429 or 5xx outside the Worker's JSON envelope; the Go client maps non-JSON errors to `http_error`.
- **Client retry and fail-safe exit:** `bunpo --queue` treats 429, all 5xx responses (including 503 `storage_error`), and network errors as transient, retrying with exponential backoff and jitter up to 2 minutes before exiting 1. A build never reports a false pass when the server hits a limit.
- **Large matrices (64+ machines):** Heartbeats at `lease_ttl / 3` are roughly half of all requests; increase `--lease-ttl` (up to 10m) to cut request volume proportionally.
- **Open dashboard tabs:** One visible tab viewing a running build makes 360 req/hr when idle, up to 1,080 req/hr while state changes every 10 s, and 0 req/hr once finished or hidden.

## 3. Standalone Go server (`bunpo-server`)

Download `bunpo-server-<os>-<arch>` and `SHA256SUMS` from GitHub Releases (or build with `go build -o bunpo-server ./cmd/bunpo-server`), verify the checksum, and place the binary at `/usr/local/bin/bunpo-server`.

### TLS reverse proxy

`bunpo-server` speaks plain HTTP and binds `127.0.0.1:8080` by default. Put TLS in front of it (for example with Caddy):

```caddyfile
queue.example.com {
    reverse_proxy 127.0.0.1:8080
}
```

### systemd service

Create a dedicated user and a `0640` environment file owned by `root:bunpo` at `/etc/bunpo/env` (readable by `bunpo`, writable only by `root`):

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin bunpo
sudo install -d -m 0750 -o root -g bunpo /etc/bunpo
sudo tee /etc/bunpo/env >/dev/null <<'EOF'
BUNPO_QUEUE_TOKEN=replace-with-64-hex-chars
BUNPO_QUEUE_READ_TOKEN=replace-with-different-64-hex-chars
EOF
sudo chmod 0640 /etc/bunpo/env
sudo chown root:bunpo /etc/bunpo/env
```

Install `/etc/systemd/system/bunpo-server.service`:

```ini
[Unit]
Description=Bunpo live-queue server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=bunpo
Group=bunpo
EnvironmentFile=/etc/bunpo/env
StateDirectory=bunpo
StateDirectoryMode=0700
ExecStart=/usr/local/bin/bunpo-server --addr 127.0.0.1:8080 --data /var/lib/bunpo
Restart=on-failure
RestartSec=3s
TimeoutStopSec=15s
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/bunpo

[Install]
WantedBy=multi-user.target
```

Enable and start:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now bunpo-server
curl -fsS http://127.0.0.1:8080/healthz
```

### Persistence (`--data`), backups, and limits

- **One server per directory:** Never point two `bunpo-server` processes at the same `--data` directory (including over shared NFS). Each process opens `--data` through `os.Root`, checks expected file sizes before appending, and `fsync`s every decision.
- **In-memory mode:** Omitting `--data` keeps builds in memory only. A restart loses active builds (404); runners exit 1 and nothing can pass across a restart.
- **Capacity and retention:** `bunpo-server` retains up to 200 builds total and 64 running builds (`503 at_capacity` when full), removing finished builds 7 days after completion.
- **Backups:** Stop `bunpo-server` (or take an atomic filesystem snapshot) before copying `/var/lib/bunpo`. Each build is one `0600` CRC-32C-framed event log named by the SHA-256 of its build ID. On startup, a torn final record at EOF is truncated and logged; any mid-file corruption is quarantined untouched (500 for that build only) without blocking other builds.

### Token rotation

1. Pick a quiet window between CI builds (or pause queued workflows).
2. Generate new tokens with `openssl rand -hex 32`.
3. Update `/etc/bunpo/env` and restart: `sudo systemctl restart bunpo-server`.
4. Update `BUNPO_QUEUE_TOKEN` and `BUNPO_QUEUE_READ_TOKEN` in your CI secrets. Any in-flight runner with the old token receives 401 and exits 1 (re-run the workflow after updating secrets).

### Container image (optional)

Build the minimal `scratch` container image locally from the repository root (non-root UID 65532, static binary, `/data` volume):

```sh
docker build -t bunpo-server:local .
docker run --rm -p 127.0.0.1:8080:8080 \
  -v bunpo-data:/data \
  -e BUNPO_QUEUE_TOKEN="$BUNPO_QUEUE_TOKEN" \
  -e BUNPO_QUEUE_READ_TOKEN="$BUNPO_QUEUE_READ_TOKEN" \
  bunpo-server:local
```

Put TLS in front of the container port in production.
