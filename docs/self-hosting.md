# Self-hosting the live queue

Bunpō's live queue runs on either of two servers speaking the same HTTP JSON protocol ([docs/queue-protocol.md](queue-protocol.md)) and serving the same read-only web dashboard:

- **Cloudflare Worker (`worker/`):** zero-ops scale-to-zero on SQLite-backed Durable Objects and free Workers Static Assets.
- **Standalone Go server (`bunpo-server`):** one static binary on a VPS, VM, or container, in memory or persisted to disk with `--data DIR`.

If your CI machines are identical and start together, static splitting (`bunpo split` / `verify`) needs no server and avoids extra runner startups.

## 1. Generate bearer tokens

Both servers require two distinct tokens, each at least 16 bytes long:

- `BUNPO_QUEUE_TOKEN`: write token for runners (`bunpo --queue ...`).
- `BUNPO_QUEUE_READ_TOKEN`: read-only token for `--status` and the web dashboard.

Generate 32-byte random hex tokens:

```sh
openssl rand -hex 32   # BUNPO_QUEUE_TOKEN
openssl rand -hex 32   # BUNPO_QUEUE_READ_TOKEN
```

Never pass tokens on the command line or commit them to `worker/wrangler.jsonc`. Both servers refuse to start or fail closed if tokens are missing, shorter than 16 bytes, or equal.

## 2. Cloudflare Worker

Every command below starts from a clean checkout of this repository.

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

3. Store both bearer tokens as Worker secrets (each command prompts for the value):

   ```sh
   npx wrangler secret put BUNPO_QUEUE_TOKEN
   npx wrangler secret put BUNPO_QUEUE_READ_TOKEN
   ```

4. Deploy the Worker, Durable Object migrations, and dashboard static assets:

   ```sh
   npx wrangler deploy
   ```

   Wrangler prints the deployed URL (for example `https://bunpo-queue.<subdomain>.workers.dev`).

5. Verify `/healthz` and authentication:

   ```sh
   curl -fsS "https://bunpo-queue.<subdomain>.workers.dev/healthz"
   # {"ok":true}

   curl -fsS -H "Authorization: Bearer $BUNPO_QUEUE_READ_TOKEN" \
     "https://bunpo-queue.<subdomain>.workers.dev/api/v1/builds"
   # {"version":1,"builds":[]}
   ```

6. Open the URL in a browser and paste `BUNPO_QUEUE_READ_TOKEN` to view the dashboard.

### Wire GitHub Actions

In your repository's **Settings -> Secrets and variables -> Actions**:

- **Variables:** set `BUNPO_QUEUE_URL` to the deployed `https://...` URL (no trailing slash).
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

To remove the Worker and its Durable Objects completely:

```sh
cd worker
npx wrangler delete
```

### Cloudflare Free-plan limits and failure modes

Cloudflare Workers Free limits (verify current limits in the Cloudflare dashboard):

| Resource | Free-plan limit | Bunpō behavior |
| --- | --- | --- |
| Worker requests | 100,000 / day | Static dashboard assets (`/`, `/dashboard/*`) bypass the Worker and cost 0 Worker requests. |
| Durable Object requests | 100,000 / day | 8 machines / 10-min build: ~417–573 DO reqs; 64 machines: ~2,934–3,656 DO reqs. |
| Durable Object SQLite rows written | 100,000 / day | 8 machines: ~467–847 lifecycle rows/build; 64 machines: ~1,983–3,937 rows/build. |
| Durable Object SQLite rows read | 5,000,000 / day | Catalog and event log stay compact; builds replay on wake only. |
| Durable Object duration | 13,000 GB-s / day | 10-min build: ~61–76 GB-s (~170–212 builds/day). |
| Durable Object storage | 5 GB total | Finished builds delete all SQLite state automatically 7 days after completion. |
| Worker CPU time | 10 ms / request (Free) | `MAX_TESTS` defaults to `5,000` in `worker/wrangler.jsonc` (configurable `1..20000`). |

What happens past a limit:

- **Daily request, row-write, or GB-s quota hit:** Cloudflare rejects requests with `429` or `503`. Runners retry with exponential backoff up to 2 minutes and then exit `1`; the build fails safely and never reports a false pass.
- **CPU limit exceeded:** Cloudflare returns `503` (error `1102` / "exceeded CPU"). Lower `MAX_TESTS` in `worker/wrangler.jsonc` or move to Workers Paid if your suite exceeds what fits under 10 ms.
- **Large matrices (64+ machines):** Heartbeats at `lease_ttl / 3` are roughly half of all requests; increase `--lease-ttl` (up to `10m`) to cut request volume proportionally.
- **Open dashboard tabs:** One visible tab viewing a running build makes `360` req/hr when idle, up to `1,080` req/hr while state changes every 10 s, and `0` req/hr once the build finishes or the tab is hidden.

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

Create a dedicated user and a `0600` environment file at `/etc/bunpo/env`:

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
- **In-memory mode:** Omitting `--data` keeps builds in memory only. A restart loses active builds (`404`); runners exit `1` and nothing can pass across a restart.
- **Capacity and retention:** `bunpo-server` retains up to `200` builds total and `64` running builds (`503 at_capacity` when full), removing finished builds 7 days after completion.
- **Backups:** Stop `bunpo-server` (or take an atomic filesystem snapshot) before copying `/var/lib/bunpo`. Each build is one `0600` CRC-32C-framed event log named by the SHA-256 of its build ID. On startup, a torn final record at EOF is truncated and logged; any mid-file corruption is quarantined untouched (`500` for that build only) without blocking other builds.

### Token rotation

1. Pick a quiet window between CI builds (or pause queued workflows).
2. Generate new tokens with `openssl rand -hex 32`.
3. Update `/etc/bunpo/env` and restart: `sudo systemctl restart bunpo-server`.
4. Update `BUNPO_QUEUE_TOKEN` and `BUNPO_QUEUE_READ_TOKEN` in your CI secrets. Any in-flight runner with the old token receives `401` and exits `1` (re-run the workflow after updating secrets).

### Container image (optional)

Build the minimal `scratch` container image locally from the repository root (non-root UID `65532`, static binary, `/data` volume):

```sh
docker build -t bunpo-server:local .
docker run --rm -p 127.0.0.1:8080:8080 \
  -v bunpo-data:/data \
  -e BUNPO_QUEUE_TOKEN="$BUNPO_QUEUE_TOKEN" \
  -e BUNPO_QUEUE_READ_TOKEN="$BUNPO_QUEUE_READ_TOKEN" \
  bunpo-server:local
```

Put TLS in front of the container port in production.
