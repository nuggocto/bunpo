// Measure exact CPU costs of concrete alternatives for fitting large builds
// under the Cloudflare Workers Free 10 ms CPU budget (5 ms with 2x margin).
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  type Config,
  type InitCommand,
  type QueueEvent,
  type ReportCommand,
  ProtocolVersion,
  applyEvent,
  computeEffectivePredictions,
  decideInit,
  decideLease,
  decideReport,
  defaultLimits,
  extendLease,
  newState,
  resetActiveLeaseDeadlines,
  validateTestList,
} from "../src/core/index.ts";

function generateTests(count: number): { tests: string[]; predictions: (number | null)[] } {
  const tests = new Array<string>(count);
  const predictions = new Array<number | null>(count);
  for (let i = 0; i < count; i++) {
    const pkg = String(i % 40).padStart(2, "0");
    const mod = String(Math.floor(i / 40) % 25).padStart(2, "0");
    const num = String(i).padStart(5, "0");
    tests[i] = `services/pkg_${pkg}/internal/mod_${mod}/handlers/order_${num}.spec.ts`;
    if (i % 10 === 0) {
      predictions[i] = null;
    } else {
      const bucket = i % 20;
      predictions[i] = bucket === 0 ? 12_000 + (i % 6_000) : 300 + ((i * 7919) % 4_000);
    }
  }
  const pairs = tests.map((t, idx) => ({ t, p: predictions[idx]! }));
  pairs.sort((a, b) => (a.t < b.t ? -1 : a.t > b.t ? 1 : 0));
  for (let i = 0; i < count; i++) {
    tests[i] = pairs[i]!.t;
    predictions[i] = pairs[i]!.p;
  }
  return { tests, predictions };
}

function fastConfigDigest(c: Config, tests: readonly string[]): string {
  const h = createHash("sha256");
  h.update(
    `bunpo queue ${ProtocolVersion}\nmachines ${c.machines}\nlease_ttl_ms ${c.lease_ttl_ms}\nbuild_timeout_ms ${c.build_timeout_ms}\nmax_attempts ${c.max_attempts}\nmax_batch ${c.max_batch}\n`,
  );
  h.update(tests.join("\n"));
  h.update("\n");
  return h.digest("hex");
}

function measureCpuMS(fn: () => void): number {
  const start = process.cpuUsage();
  fn();
  const diff = process.cpuUsage(start);
  return (diff.user + diff.system) / 1000;
}

function stats(samples: number[]): { p50: number; max: number } {
  const s = samples.slice().sort((a, b) => a - b);
  return {
    p50: Number(s[Math.floor(s.length / 2)]!.toFixed(3)),
    max: Number(s[s.length - 1]!.toFixed(3)),
  };
}

function buildMidEvents(testCount: number, machines: number) {
  const { tests, predictions } = generateTests(testCount);
  const cfg: Config = {
    machines,
    lease_ttl_ms: 60_000,
    build_timeout_ms: 6 * 60 * 60 * 1000,
    max_attempts: 3,
    max_batch: 200,
  };
  const now = 1_700_000_000_000;
  const initCmd: InitCommand = {
    build_id: `alt-${testCount}-${machines}`,
    runner_id: "runner-0",
    machine_index: 0,
    config: cfg,
    tests,
    predictions_ms: predictions,
  };
  const initReqJSON = JSON.stringify({ version: 1, ...initCmd });
  const lim = defaultLimits();
  const st = newState(lim);
  const initOut = decideInit(st, now, initCmd);
  const initEv = initOut.events[0]!;
  applyEvent(st, initEv);

  const catalogChunksJSON: string[] = [];
  for (let off = 0; off < testCount; off += 1000) {
    const end = Math.min(off + 1000, testCount);
    catalogChunksJSON.push(
      JSON.stringify({ tests: tests.slice(off, end), predictions_ms: predictions.slice(off, end) }),
    );
  }
  const eventRowsJSON: string[] = [
    JSON.stringify([{ type: "initialized", at_ms: initEv.at_ms, initialized: { ...initEv.initialized!, tests: [], predictions_ms: [] } }]),
  ];

  let t = now + 1000;
  const active: { runner: string; machine: number; leaseID: number; attempts: { attempt_id: string; test_index: number; generation: number }[] }[] = [];
  const reqSeq = new Array<number>(machines).fill(0);
  let completed = 0;
  const half = Math.floor(testCount / 2);

  for (let m = 0; m < machines; m++) {
    reqSeq[m]!++;
    const l = decideLease(st, t, { runner_id: `runner-${m}`, machine_index: m, request_id: `r-${m}-${reqSeq[m]}` });
    if (l.error || !l.response || l.response.status !== "leased") break;
    eventRowsJSON.push(JSON.stringify(l.events));
    for (const ev of l.events) applyEvent(st, ev);
    extendLease(st, l.response.lease_id, l.response.deadline_ms);
    active.push({ runner: `runner-${m}`, machine: m, leaseID: l.response.lease_id, attempts: l.response.attempts ?? [] });
    t += 50;
  }

  let cursor = 0;
  while (completed < half && active.length > 0) {
    const slot = active[cursor % active.length]!;
    t += 200;
    const repCmd: ReportCommand = {
      runner_id: slot.runner,
      lease_id: slot.leaseID,
      results: slot.attempts.map((a) => ({
        attempt_id: a.attempt_id,
        test_index: a.test_index,
        generation: a.generation,
        outcome: "passed",
        timed: true,
        duration_ms: predictions[a.test_index] ?? 1500,
      })),
    };
    const r = decideReport(st, t, repCmd);
    if (r.events.length > 0) {
      eventRowsJSON.push(JSON.stringify(r.events));
      for (const ev of r.events) applyEvent(st, ev);
    }
    completed += slot.attempts.length;
    if (completed >= half) break;
    reqSeq[slot.machine]!++;
    t += 50;
    const l = decideLease(st, t, { runner_id: slot.runner, machine_index: slot.machine, request_id: `r-${slot.machine}-${reqSeq[slot.machine]}` });
    if (l.error || !l.response || l.response.status !== "leased") {
      active.splice(cursor % active.length, 1);
      continue;
    }
    eventRowsJSON.push(JSON.stringify(l.events));
    for (const ev of l.events) applyEvent(st, ev);
    extendLease(st, l.response.lease_id, l.response.deadline_ms);
    slot.leaseID = l.response.lease_id;
    slot.attempts = l.response.attempts ?? [];
    cursor++;
  }

  const eff = computeEffectivePredictions(predictions);
  const dealOrder = Array.from({ length: testCount }, (_, i) => i).sort((a, b) => eff[b]! - eff[a]! || a - b);
  const initMetaWithOrderJSON = JSON.stringify({
    build_id: initCmd.build_id,
    digest: st.digest,
    config: cfg,
    created_at_ms: now,
    deadline_ms: st.deadlineMS,
    eff,
    dealOrder,
  });

  // Compact snapshot JSON at mid-build (stores dynamic per-test fields as flat arrays,
  // avoiding per-test object/array allocations during JSON.parse).
  const snapshotJSON = JSON.stringify({
    seq: eventRowsJSON.length,
    lastSeenMS: st.lastSeenMS,
    finishedAtMS: st.finishedAtMS,
    counts: st.counts,
    status: st.tests.map((x) => x.status),
    gen: st.tests.map((x) => x.generation),
    activeLease: st.tests.map((x) => x.activeLeaseID),
    timed: st.tests.map((x) => (x.timed ? 1 : 0)),
    dur: st.tests.map((x) => x.durationMS),
    leases: st.leases,
    activeLeases: st.activeLeases,
    runnerOrder: st.runnerOrder,
    runners: st.runnerOrder.map((id) => st.runners.get(id)),
    conflicts: st.conflicts,
    conflictsTruncated: st.conflictsTruncated,
  });

  return {
    testCount,
    machines,
    cfg,
    now,
    midNow: t + 100,
    tests,
    predictions,
    initReqJSON,
    catalogChunksJSON,
    eventRowsJSON,
    initMetaWithOrderJSON,
    snapshotJSON,
  };
}

// Alternative A: node:crypto joined + single-pass init (validate & hash once, not twice)
function runAltInit(initReqJSON: string, cfg: Config, now: number) {
  const parsed = JSON.parse(initReqJSON) as InitCommand;
  const err = validateTestList(parsed.tests, parsed.predictions_ms, 20_000);
  if (err) throw err;
  const digest = fastConfigDigest(cfg, parsed.tests);
  const eff = computeEffectivePredictions(parsed.predictions_ms);
  const n = parsed.tests.length;
  const order = new Int32Array(n);
  for (let i = 0; i < n; i++) order[i] = i;
  order.sort((a, b) => eff[b]! - eff[a]! || a - b);
  for (let off = 0; off < n; off += 1000) {
    const end = Math.min(off + 1000, n);
    JSON.stringify({ tests: parsed.tests.slice(off, end), predictions_ms: parsed.predictions_ms.slice(off, end) });
  }
  JSON.stringify({ version: 1, build_id: parsed.build_id, created: true, digest, verdict: "running", test_count: n, deadline_ms: now + cfg.build_timeout_ms });
}

// Alternative B: Cheaper replay (trusted SQLite rows -> skip re-validate & re-hash, reuse persisted dealOrder + eff)
function runAltCheaperReplay(
  catalogChunksJSON: string[],
  initMetaWithOrderJSON: string,
  eventRowsJSON: string[],
  midNow: number,
) {
  const meta = JSON.parse(initMetaWithOrderJSON) as {
    build_id: string;
    digest: string;
    config: Config;
    created_at_ms: number;
    deadline_ms: number;
    eff: number[];
    dealOrder: number[];
  };
  const paths: string[] = [];
  const preds: (number | null)[] = [];
  for (let i = 0; i < catalogChunksJSON.length; i++) {
    const c = JSON.parse(catalogChunksJSON[i]!) as { tests: string[]; predictions_ms: (number | null)[] };
    for (let j = 0; j < c.tests.length; j++) {
      paths.push(c.tests[j]!);
      preds.push(c.predictions_ms[j]!);
    }
  }
  const s = newState(defaultLimits());
  const n = paths.length;
  const tests = new Array(n);
  for (let i = 0; i < n; i++) {
    tests[i] = {
      index: i,
      path: paths[i]!,
      predictedMS: preds[i]!,
      effectiveMS: meta.eff[i]!,
      status: "pending",
      generation: 0,
      activeLeaseID: 0,
      timed: false,
      durationMS: 0,
      attempts: [],
      history: [],
      historyTruncated: false,
    };
  }
  s.initialized = true;
  s.buildID = meta.build_id;
  s.digest = meta.digest;
  s.config = meta.config;
  s.createdAtMS = meta.created_at_ms;
  s.deadlineMS = meta.deadline_ms;
  s.lastSeenMS = meta.created_at_ms;
  s.tests = tests;
  s.dealOrder = meta.dealOrder;
  s.counts = { total: n, pending: n, leased: 0, passed: 0, failed: 0, skipped: 0, unresolved: 0 };

  for (let i = 1; i < eventRowsJSON.length; i++) {
    const evs = JSON.parse(eventRowsJSON[i]!) as QueueEvent[];
    for (let j = 0; j < evs.length; j++) applyEvent(s, evs[j]!);
  }
  resetActiveLeaseDeadlines(s, midNow);
}

// Alternative C: Snapshot restore (parse catalog + meta + compact snapshot, 0 event replay)
function runAltSnapshotRestore(
  catalogChunksJSON: string[],
  initMetaWithOrderJSON: string,
  snapshotJSON: string,
) {
  JSON.parse(initMetaWithOrderJSON);
  for (let i = 0; i < catalogChunksJSON.length; i++) {
    JSON.parse(catalogChunksJSON[i]!);
  }
  JSON.parse(snapshotJSON);
}

// Alternative D: Chunked init (validate + store 2,000 tests per request, 10 requests for 20k)
function runAltChunkedInitStep(chunkJSON: string) {
  const c = JSON.parse(chunkJSON) as { tests: string[]; predictions_ms: (number | null)[] };
  const err = validateTestList(c.tests, c.predictions_ms, 20_000);
  if (err) throw err;
  JSON.stringify(c);
}

const args = process.argv.slice(2);
if (args[0] === "--cold") {
  const n = Number(args[1]);
  const mode = args[2]!;
  if (mode === "alt_init") {
    const { tests, predictions } = generateTests(n);
    const cfg: Config = { machines: 64, lease_ttl_ms: 60_000, build_timeout_ms: 21_600_000, max_attempts: 3, max_batch: 200 };
    const req = JSON.stringify({ version: 1, build_id: "b", runner_id: "r0", machine_index: 0, config: cfg, tests, predictions_ms: predictions });
    process.stdout.write(String(measureCpuMS(() => runAltInit(req, cfg, 1_700_000_000_000))));
    process.exit(0);
  }
  const chunks: Buffer[] = [];
  for await (const c of process.stdin) chunks.push(c as Buffer);
  const p = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (mode === "alt_replay") {
    process.stdout.write(String(measureCpuMS(() => runAltCheaperReplay(p.catalogChunksJSON, p.initMetaWithOrderJSON, p.eventRowsJSON, p.midNow))));
  } else if (mode === "alt_snapshot") {
    process.stdout.write(String(measureCpuMS(() => runAltSnapshotRestore(p.catalogChunksJSON, p.initMetaWithOrderJSON, p.snapshotJSON))));
  } else if (mode === "alt_chunk_2000") {
    process.stdout.write(String(measureCpuMS(() => runAltChunkedInitStep(p.chunk2000JSON))));
  }
  process.exit(0);
}

const sizes = [1_000, 2_000, 3_000, 5_000, 10_000, 20_000];
const results: unknown[] = [];

for (const n of sizes) {
  const fx = buildMidEvents(n, 64);
  const chunk2000JSON = JSON.stringify({
    tests: fx.tests.slice(0, Math.min(2000, n)),
    predictions_ms: fx.predictions.slice(0, Math.min(2000, n)),
  });

  const warm = (fn: () => void) => {
    for (let i = 0; i < 5; i++) fn();
    const s: number[] = [];
    for (let i = 0; i < 20; i++) s.push(measureCpuMS(fn));
    return stats(s);
  };
  const cold = (mode: string, stdinObj?: unknown) => {
    const s: number[] = [];
    const input = stdinObj ? JSON.stringify(stdinObj) : undefined;
    for (let i = 0; i < 5; i++) {
      const out = execFileSync(process.execPath, [import.meta.filename, "--cold", String(n), mode], {
        input,
        encoding: "utf8",
        maxBuffer: 64 * 1024 * 1024,
      });
      s.push(Number(out.trim()));
    }
    return stats(s);
  };

  results.push({
    tests: n,
    midEventRows: fx.eventRowsJSON.length,
    snapshotBytes: fx.snapshotJSON.length,
    warm: {
      alt_init_node_crypto_single_pass: warm(() => runAltInit(fx.initReqJSON, fx.cfg, fx.now)),
      alt_cheaper_replay: warm(() =>
        runAltCheaperReplay(fx.catalogChunksJSON, fx.initMetaWithOrderJSON, fx.eventRowsJSON, fx.midNow),
      ),
      alt_snapshot_restore: warm(() =>
        runAltSnapshotRestore(fx.catalogChunksJSON, fx.initMetaWithOrderJSON, fx.snapshotJSON),
      ),
      alt_chunked_init_2000: warm(() => runAltChunkedInitStep(chunk2000JSON)),
    },
    cold: {
      alt_init_node_crypto_single_pass: cold("alt_init"),
      alt_cheaper_replay: cold("alt_replay", {
        catalogChunksJSON: fx.catalogChunksJSON,
        initMetaWithOrderJSON: fx.initMetaWithOrderJSON,
        eventRowsJSON: fx.eventRowsJSON,
        midNow: fx.midNow,
      }),
      alt_snapshot_restore: cold("alt_snapshot", {
        catalogChunksJSON: fx.catalogChunksJSON,
        initMetaWithOrderJSON: fx.initMetaWithOrderJSON,
        snapshotJSON: fx.snapshotJSON,
      }),
      alt_chunked_init_2000: cold("alt_chunk_2000", { chunk2000JSON }),
    },
  });
}

console.log(JSON.stringify(results, null, 2));
