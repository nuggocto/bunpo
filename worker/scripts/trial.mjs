#!/usr/bin/env node
// Manual trial runner for verifying a deployed Cloudflare Worker (or local
// bunpo-server) before relying on it in CI. Plain Node, zero dependencies.
//
// Required environment variables:
//   BUNPO_QUEUE_URL         https://... (plain http:// allowed only on loopback)
//   BUNPO_QUEUE_TOKEN       write bearer token
//   BUNPO_QUEUE_READ_TOKEN  read-only bearer token
//
// Pass --yes to run; without --yes it prints the request plan and exits 2.

import { performance } from "node:perf_hooks";

const MAX_REQUESTS = 1500;
const SIZES = [1000, 5000, 10000, 20000];
const IDLE_WAKE_MS = 15_000;
const DEAD_TTL_MS = 10_000;
const EXPECTED_CSP =
  "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

const args = new Set(process.argv.slice(2));
const confirmed = args.has("--yes");

const rawURL = (process.env.BUNPO_QUEUE_URL || "").trim().replace(/\/+$/, "");
const writeToken = process.env.BUNPO_QUEUE_TOKEN || "";
const readToken = process.env.BUNPO_QUEUE_READ_TOKEN || "";

function die(msg, code = 2) {
  console.error(`trial: ${msg}`);
  process.exit(code);
}

function validateBaseURL(u) {
  let parsed;
  try {
    parsed = new URL(u);
  } catch {
    die(`invalid BUNPO_QUEUE_URL: ${JSON.stringify(u)}`);
  }
  if (parsed.protocol === "https:") return parsed.origin + parsed.pathname.replace(/\/+$/, "");
  if (parsed.protocol === "http:") {
    const h = parsed.hostname;
    if (h === "localhost" || h === "127.0.0.1" || h === "::1" || h === "[::1]") {
      return parsed.origin + parsed.pathname.replace(/\/+$/, "");
    }
    die(`refusing non-https BUNPO_QUEUE_URL outside loopback: ${u}`);
  }
  die(`unsupported URL scheme in BUNPO_QUEUE_URL: ${parsed.protocol}`);
}

const plannedMaxRequests = 1 + 2 + SIZES.length * 6 + 7 + 16 + 7; // ~60 requests, hard cap 1500

if (!rawURL || !writeToken || !readToken) {
  console.error("Usage:");
  console.error("  BUNPO_QUEUE_URL=https://... BUNPO_QUEUE_TOKEN=... BUNPO_QUEUE_READ_TOKEN=... node worker/scripts/trial.mjs --yes");
  die("BUNPO_QUEUE_URL, BUNPO_QUEUE_TOKEN, and BUNPO_QUEUE_READ_TOKEN must be set");
}

const baseURL = validateBaseURL(rawURL);

console.log(`Target: ${baseURL}`);
console.log(`Planned requests: ~${plannedMaxRequests} (hard client cap: ${MAX_REQUESTS})`);
console.log("Checks: /healthz, auth (401/403), sizes 1k/5k/10k/20k + 15s idle wake, retry, dead-runner expiry (<=1.5 TTL), dashboard headers.");

if (!confirmed) {
  console.error("Dry run only. Re-run with --yes to send requests.");
  process.exit(2);
}

let requestCount = 0;

function classifyError(status, text, json, err) {
  if (err) {
    return { kind: "network_error", detail: err.cause?.code || err.message || String(err) };
  }
  const lower = (text || "").toLowerCase();
  if (
    status === 503 ||
    status === 500 ||
    status === 524
  ) {
    if (lower.includes("1102") || lower.includes("exceeded cpu") || lower.includes("cpu time") || lower.includes("worker exceeded resource")) {
      return { kind: "cpu_limit", detail: `HTTP ${status}: ${text.slice(0, 160)}` };
    }
  }
  if (
    status === 429 ||
    lower.includes("1027") ||
    lower.includes("quota") ||
    lower.includes("daily limit") ||
    lower.includes("rate limit")
  ) {
    return { kind: "quota_limit", detail: `HTTP ${status}: ${text.slice(0, 160)}` };
  }
  if (json && typeof json === "object" && json.error && typeof json.error.code === "string") {
    return { kind: "api_error", code: json.error.code, detail: `${json.error.code}: ${json.error.message || ""}` };
  }
  return { kind: "http_error", detail: `HTTP ${status}: ${text.slice(0, 160)}` };
}

async function req(method, path, { token = "", body = undefined } = {}) {
  if (requestCount >= MAX_REQUESTS) {
    throw new Error(`request cap (${MAX_REQUESTS}) reached`);
  }
  requestCount++;
  const headers = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;
  if (body !== undefined) headers["Content-Type"] = "application/json; charset=utf-8";

  const t0 = performance.now();
  let resp;
  try {
    resp = await fetch(`${baseURL}${path}`, {
      method,
      headers,
      body: body !== undefined ? (typeof body === "string" ? body : JSON.stringify(body)) : undefined,
      redirect: "manual",
    });
  } catch (err) {
    const ms = Math.round(performance.now() - t0);
    return { ok: false, status: 0, ms, headers: new Headers(), text: "", json: null, failure: classifyError(0, "", null, err) };
  }
  const text = await resp.text();
  const ms = Math.round(performance.now() - t0);
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  const failure = resp.ok ? null : classifyError(resp.status, text, json, null);
  return { ok: resp.ok, status: resp.status, ms, headers: resp.headers, text, json, failure };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function makeTests(n) {
  const tests = new Array(n);
  const preds = new Array(n);
  for (let i = 0; i < n; i++) {
    tests[i] = `tests/suite_${String(Math.floor(i / 100)).padStart(3, "0")}/test_${String(i).padStart(5, "0")}.py`;
    preds[i] = 100 + (i % 50) * 10;
  }
  return { tests, preds };
}

const runTag = `${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`;
const results = [];
let overallFailed = false;

function record(name, pass, detail) {
  results.push({ name, pass, detail });
  if (!pass) overallFailed = true;
  console.log(`[${pass ? "PASS" : "FAIL"}] ${name}: ${detail}`);
}

// 1. /healthz
{
  const r = await req("GET", "/healthz");
  const ok = r.status === 200 && r.json?.ok === true;
  record("healthz", ok, ok ? `200 ok (${r.ms} ms)` : r.failure?.detail || `status ${r.status}`);
}

// 2. Auth: wrong token -> 401, read token on POST -> 403
{
  const r401 = await req("GET", "/api/v1/builds", { token: "wrong-token-0123456789abcdef" });
  const ok401 = r401.status === 401 && r401.json?.error?.code === "unauthorized";
  record("auth-wrong-token-401", ok401, ok401 ? `401 unauthorized (${r401.ms} ms)` : r401.failure?.detail || `status ${r401.status}`);

  const r403 = await req("POST", `/api/v1/builds/trial-auth-${runTag}/init`, {
    token: readToken,
    body: { version: 1 },
  });
  const ok403 = r403.status === 403 && r403.json?.error?.code === "forbidden";
  record("auth-read-post-403", ok403, ok403 ? `403 forbidden (${r403.ms} ms)` : r403.failure?.detail || `status ${r403.status}`);
}

// 3. Size sweep: 1k, 5k, 10k, 20k (skip sizes above MAX_TESTS)
const sizeRows = [];
for (const n of SIZES) {
  const bid = `trial-${n}-${runTag}`;
  const row = { size: n, init: "-", lease: "-", report: "-", status: "-", list: "-", wake: "-", verdict: "PASS" };
  const { tests, preds } = makeTests(n);

  const initR = await req("POST", `/api/v1/builds/${bid}/init`, {
    token: writeToken,
    body: {
      version: 1,
      runner_id: "trial-runner-0",
      machine_index: 0,
      config: {
        machines: 4,
        lease_ttl_ms: 30_000,
        build_timeout_ms: 120_000,
        max_attempts: 2,
        max_batch: 200,
      },
      tests,
      predictions_ms: preds,
    },
  });

  if (initR.status === 400 && initR.json?.error?.code === "invalid_request") {
    row.init = `${initR.ms}ms`;
    row.verdict = "SKIP (MAX_TESTS)";
    sizeRows.push(row);
    console.log(`[SKIP] size-${n}: server MAX_TESTS rejected ${n} tests (${initR.json.error.message})`);
    continue;
  }
  if (initR.status !== 200) {
    row.init = `${initR.ms}ms`;
    row.verdict = `FAIL (${initR.failure?.kind || initR.status})`;
    sizeRows.push(row);
    record(`size-${n}`, false, `init failed: ${initR.failure?.kind} - ${initR.failure?.detail}`);
    continue;
  }
  row.init = `${initR.ms}ms`;

  const leaseR = await req("POST", `/api/v1/builds/${bid}/lease`, {
    token: writeToken,
    body: { version: 1, runner_id: "trial-runner-0", machine_index: 0, request_id: "q1" },
  });
  if (leaseR.status !== 200 || leaseR.json?.status !== "leased") {
    row.lease = `${leaseR.ms}ms`;
    row.verdict = `FAIL (${leaseR.failure?.kind || leaseR.status})`;
    sizeRows.push(row);
    record(`size-${n}`, false, `lease failed: ${leaseR.failure?.kind} - ${leaseR.failure?.detail}`);
    continue;
  }
  row.lease = `${leaseR.ms}ms`;

  const repResults = leaseR.json.attempts.map((a) => ({
    attempt_id: a.attempt_id,
    test_index: a.test_index,
    generation: a.generation,
    outcome: "passed",
    timed: true,
    duration_ms: 50,
  }));
  const repR = await req("POST", `/api/v1/builds/${bid}/report`, {
    token: writeToken,
    body: { version: 1, runner_id: "trial-runner-0", lease_id: leaseR.json.lease_id, results: repResults },
  });
  if (repR.status !== 200) {
    row.report = `${repR.ms}ms`;
    row.verdict = `FAIL (${repR.failure?.kind || repR.status})`;
    sizeRows.push(row);
    record(`size-${n}`, false, `report failed: ${repR.failure?.kind} - ${repR.failure?.detail}`);
    continue;
  }
  row.report = `${repR.ms}ms`;

  const stR = await req("GET", `/api/v1/builds/${bid}`, { token: readToken });
  if (stR.status !== 200 || stR.json?.counts?.total !== n || stR.json?.counts?.passed !== repResults.length) {
    row.status = `${stR.ms}ms`;
    row.verdict = `FAIL (${stR.failure?.kind || stR.status})`;
    sizeRows.push(row);
    record(`size-${n}`, false, `status failed: ${stR.failure?.kind || stR.status}`);
    continue;
  }
  row.status = `${stR.ms}ms`;

  const listR = await req("GET", "/api/v1/builds", { token: readToken });
  const listed = listR.status === 200 && Array.isArray(listR.json?.builds) && listR.json.builds.some((b) => b.build_id === bid);
  if (!listed) {
    row.list = `${listR.ms}ms`;
    row.verdict = `FAIL (${listR.failure?.kind || "not listed"})`;
    sizeRows.push(row);
    record(`size-${n}`, false, `list failed: build ${bid} not in /api/v1/builds`);
    continue;
  }
  row.list = `${listR.ms}ms`;

  // Idle >10s so Cloudflare hibernates the DO, then measure cold wake replay.
  await sleep(IDLE_WAKE_MS);
  const wakeR = await req("GET", `/api/v1/builds/${bid}`, { token: readToken });
  if (wakeR.status !== 200 || wakeR.json?.counts?.passed !== repResults.length) {
    row.wake = `${wakeR.ms}ms`;
    row.verdict = `FAIL (${wakeR.failure?.kind || wakeR.status})`;
    sizeRows.push(row);
    record(`size-${n}`, false, `wake failed: ${wakeR.failure?.kind} - ${wakeR.failure?.detail}`);
    continue;
  }
  row.wake = `${wakeR.ms}ms`;
  sizeRows.push(row);
  record(`size-${n}`, true, `init=${row.init} lease=${row.lease} report=${row.report} status=${row.status} list=${row.list} wake=${row.wake}`);
}

// 4. Retry behavior: error on attempt 1 -> retried at generation 2 -> passed
{
  const bid = `trial-retry-${runTag}`;
  const initR = await req("POST", `/api/v1/builds/${bid}/init`, {
    token: writeToken,
    body: {
      version: 1,
      runner_id: "retry-r0",
      machine_index: 0,
      config: { machines: 1, lease_ttl_ms: 15_000, build_timeout_ms: 120_000, max_attempts: 2, max_batch: 1 },
      tests: ["tests/retry_me.py"],
      predictions_ms: [100],
    },
  });
  let ok = initR.status === 200;
  let detail = ok ? "" : `init ${initR.failure?.detail}`;
  if (ok) {
    const l1 = await req("POST", `/api/v1/builds/${bid}/lease`, {
      token: writeToken,
      body: { version: 1, runner_id: "retry-r0", machine_index: 0, request_id: "q1" },
    });
    ok = l1.status === 200 && l1.json?.attempts?.[0]?.generation === 1;
    if (!ok) detail = `lease1 ${l1.failure?.detail || l1.status}`;
    if (ok) {
      const a1 = l1.json.attempts[0];
      const r1 = await req("POST", `/api/v1/builds/${bid}/report`, {
        token: writeToken,
        body: {
          version: 1,
          runner_id: "retry-r0",
          lease_id: l1.json.lease_id,
          results: [{ attempt_id: a1.attempt_id, test_index: a1.test_index, generation: a1.generation, outcome: "error", timed: false, duration_ms: 0 }],
        },
      });
      ok = r1.status === 200 && r1.json?.verdict === "running";
      if (!ok) detail = `report1 ${r1.failure?.detail || r1.text}`;
    }
    if (ok) {
      const l2 = await req("POST", `/api/v1/builds/${bid}/lease`, {
        token: writeToken,
        body: { version: 1, runner_id: "retry-r0", machine_index: 0, request_id: "q2" },
      });
      ok = l2.status === 200 && l2.json?.attempts?.[0]?.generation === 2;
      if (!ok) detail = `lease2 ${l2.failure?.detail || l2.text}`;
      if (ok) {
        const a2 = l2.json.attempts[0];
        const r2 = await req("POST", `/api/v1/builds/${bid}/report`, {
          token: writeToken,
          body: {
            version: 1,
            runner_id: "retry-r0",
            lease_id: l2.json.lease_id,
            results: [{ attempt_id: a2.attempt_id, test_index: a2.test_index, generation: a2.generation, outcome: "passed", timed: true, duration_ms: 40 }],
          },
        });
        ok = r2.status === 200 && r2.json?.verdict === "passed";
        detail = ok ? "attempt 1 error -> attempt 2 passed -> verdict passed" : `report2 ${r2.failure?.detail || r2.text}`;
      }
    }
  }
  record("retry-behavior", ok, detail);
}

// 5. Dead runner lease moves within 1.5 * TTL (even across 10s DO hibernation)
{
  const bid = `trial-dead-${runTag}`;
  const initR = await req("POST", `/api/v1/builds/${bid}/init`, {
    token: writeToken,
    body: {
      version: 1,
      runner_id: "dead-r0",
      machine_index: 0,
      config: { machines: 2, lease_ttl_ms: DEAD_TTL_MS, build_timeout_ms: 120_000, max_attempts: 2, max_batch: 1 },
      tests: ["tests/dead_target.py"],
      predictions_ms: [100],
    },
  });
  let ok = initR.status === 200;
  let detail = ok ? "" : `init ${initR.failure?.detail}`;
  if (ok) {
    const tLease = performance.now();
    const deadLease = await req("POST", `/api/v1/builds/${bid}/lease`, {
      token: writeToken,
      body: { version: 1, runner_id: "dead-r0", machine_index: 0, request_id: "dead-q1" },
    });
    ok = deadLease.status === 200 && deadLease.json?.status === "leased";
    if (!ok) detail = `dead lease failed: ${deadLease.failure?.detail || deadLease.text}`;
    if (ok) {
      // Wait 11s so a Cloudflare DO hibernates (>10s) and the 10s lease TTL elapses;
      // with floor(ttl/2)=5s wake grace, reassignment must happen within 1.5*TTL (15s).
      await sleep(DEAD_TTL_MS + 1000);
      let reassigned = null;
      let elapsedMS = 0;
      for (let i = 0; i < 8; i++) {
        const pollR = await req("POST", `/api/v1/builds/${bid}/lease`, {
          token: writeToken,
          body: { version: 1, runner_id: "survivor-r1", machine_index: 1, request_id: `surv-${i}` },
        });
        elapsedMS = Math.round(performance.now() - tLease);
        if (pollR.status === 200 && pollR.json?.status === "leased" && pollR.json?.attempts?.[0]?.generation === 2) {
          reassigned = pollR.json;
          break;
        }
        await sleep(1000);
      }
      if (!reassigned) {
        ok = false;
        detail = `lease not reassigned after ${elapsedMS} ms`;
      } else {
        const a = reassigned.attempts[0];
        const rep = await req("POST", `/api/v1/builds/${bid}/report`, {
          token: writeToken,
          body: {
            version: 1,
            runner_id: "survivor-r1",
            lease_id: reassigned.lease_id,
            results: [{ attempt_id: a.attempt_id, test_index: a.test_index, generation: a.generation, outcome: "passed", timed: true, duration_ms: 30 }],
          },
        });
        const testsR = await req("GET", `/api/v1/builds/${bid}/tests?limit=10`, { token: readToken });
        const attempts = testsR.json?.tests?.[0]?.attempts || [];
        const serverDeltaMS =
          attempts.length >= 2 ? attempts[1].leased_at_ms - attempts[0].leased_at_ms : elapsedMS;
        // Allow up to 1.5*TTL + 1.5s poll/RTT margin on wall time, and check server timestamps.
        const withinBudget = serverDeltaMS <= DEAD_TTL_MS * 1.5 + 1500 && rep.status === 200 && rep.json?.verdict === "passed";
        ok = withinBudget;
        detail = `reassigned in ${serverDeltaMS} ms server time (${elapsedMS} ms wall, budget <= ${DEAD_TTL_MS * 1.5} ms + poll margin)`;
      }
    }
  }
  record("dead-runner-1.5-ttl", ok, detail);
}

// 6. Dashboard headers and 404 envelope
{
  const checks = [
    ["/", "no-cache"],
    ["/builds/trial-1.2.3", "no-cache"],
    ["/dashboard/app.js", "public, max-age=300"],
    ["/dashboard/core.js", "public, max-age=300"],
    ["/dashboard/style.css", "public, max-age=300"],
    ["/dashboard/favicon.svg", "public, max-age=300"],
  ];
  let bad = [];
  for (const [p, wantCache] of checks) {
    const r = await req("GET", p);
    if (r.status !== 200) bad.push(`${p}: status ${r.status}`);
    else if (r.headers.get("content-security-policy") !== EXPECTED_CSP) bad.push(`${p}: CSP mismatch`);
    else if (r.headers.get("x-content-type-options") !== "nosniff") bad.push(`${p}: missing nosniff`);
    else if (r.headers.get("referrer-policy") !== "no-referrer") bad.push(`${p}: missing no-referrer`);
    else if (r.headers.get("cache-control") !== wantCache) bad.push(`${p}: cache-control=${r.headers.get("cache-control")}`);
  }
  const r404 = await req("GET", "/_headers");
  if (r404.status !== 404 || r404.json?.error?.code !== "not_found") {
    bad.push(`/_headers: expected JSON 404, got ${r404.status}`);
  }
  record("dashboard-headers", bad.length === 0, bad.length === 0 ? "all 6 assets + /_headers 404 verified" : bad.join("; "));
}

console.log("\n=== Size sweep summary ===");
console.log("Size   | init    | lease   | report  | status  | list    | wake(15s) | Verdict");
console.log("-------+---------+---------+---------+---------+---------+-----------+------------------");
for (const r of sizeRows) {
  console.log(
    `${String(r.size).padEnd(6)} | ${r.init.padEnd(7)} | ${r.lease.padEnd(7)} | ${r.report.padEnd(7)} | ${r.status.padEnd(7)} | ${r.list.padEnd(7)} | ${r.wake.padEnd(9)} | ${r.verdict}`,
  );
}

console.log(`\nTotal requests sent: ${requestCount} / ${MAX_REQUESTS}`);
console.log("\nWhat to check in the Cloudflare dashboard (Workers & Pages -> bunpo-queue):");
console.log("  1. Metrics -> CPU time (p50 and p99): note p50/p99 per request size.");
console.log("  2. Metrics / Logs -> Errors: confirm zero 'exceeded CPU' (error 1102) or quota errors.");
console.log("  3. Durable Objects -> BuildDO / BuildIndex: check requests and SQLite rows written.");

process.exit(overallFailed ? 1 : 0);
