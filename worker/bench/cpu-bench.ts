// Local Node CPU benchmark using process.cpuUsage() (user + system microseconds).
// Measures BOTH cold (fresh V8 isolate per run) and warm (JIT-warmed) p50 & max
// CPU time on the exact same pure TypeScript core that runs in workerd.
//
// Usage:
//   node bench/cpu-bench.ts                  (runs full warm + cold suite)
//   node bench/cpu-bench.ts --worker <spec>  (internal single cold run)

import { execFileSync } from "node:child_process";
import {
  type Config,
  type InitCommand,
  type LeaseCommand,
  type QueueEvent,
  type ReportCommand,
  type State,
  applyEvent,
  computeEffectivePredictions,
  configDigest,
  decideInit,
  decideLease,
  decideListTests,
  decideReport,
  decideStatus,
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

function measureCpuMS(fn: () => void): number {
  const start = process.cpuUsage();
  fn();
  const diff = process.cpuUsage(start);
  return (diff.user + diff.system) / 1000;
}

function stats(samples: number[]): { p50: number; max: number } {
  const sorted = samples.slice().sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length / 2)]!;
  const max = sorted[sorted.length - 1]!;
  return { p50: Number(p50.toFixed(3)), max: Number(max.toFixed(3)) };
}

interface ScenarioFixtures {
  testCount: number;
  machines: number;
  cfg: Config;
  now: number;
  initCmd: InitCommand;
  initReqJSON: string;
  stateAfterInit: State;
  stateAtMid: State;
  midNow: number;
  midLeaseCmd: LeaseCommand;
  midReportCmd: ReportCommand;
  midReportReqJSON: string;
  catalogChunksJSON: string[];
  midEventRowsJSON: string[];
  midEventCount: number;
}

function buildFixtures(testCount: number, machines: number, chunkSize = 1000): ScenarioFixtures {
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
    build_id: `bench-${testCount}-${machines}`,
    runner_id: "runner-0",
    machine_index: 0,
    config: cfg,
    tests,
    predictions_ms: predictions,
  };
  const initReqJSON = JSON.stringify({ version: 1, ...initCmd });

  const lim = defaultLimits();
  const stateAfterInit = newState(lim);
  const initOut = decideInit(stateAfterInit, now, initCmd);
  if (initOut.error) throw initOut.error;

  const catalogChunksJSON: string[] = [];
  for (let offset = 0; offset < testCount; offset += chunkSize) {
    const end = Math.min(offset + chunkSize, testCount);
    catalogChunksJSON.push(
      JSON.stringify({
        tests: tests.slice(offset, end),
        predictions_ms: predictions.slice(offset, end),
      }),
    );
  }
  const initEv = initOut.events[0]!;
  const initHeaderRowJSON = JSON.stringify([
    {
      type: "initialized",
      at_ms: initEv.at_ms,
      initialized: {
        ...initEv.initialized!,
        tests: [],
        predictions_ms: [],
      },
    },
  ]);

  applyEvent(stateAfterInit, initEv);

  // Clone state by replaying initEv so we have both post-init and mid-build states.
  const stateAtMid = newState(lim);
  applyEvent(stateAtMid, initEv);
  const midEventRowsJSON: string[] = [initHeaderRowJSON];

  // Advance to ~50% completed tests with `machines` active runners.
  let t = now + 1000;
  const active: { runner: string; machine: number; leaseID: number; attempts: { attempt_id: string; test_index: number; generation: number }[] }[] = [];
  const reqSeq = new Array<number>(machines).fill(0);
  let completed = 0;
  const half = Math.floor(testCount / 2);

  // First round: all machines lease.
  for (let m = 0; m < machines; m++) {
    reqSeq[m]!++;
    const l = decideLease(stateAtMid, t, {
      runner_id: `runner-${m}`,
      machine_index: m,
      request_id: `req-${m}-${reqSeq[m]}`,
    });
    if (l.error || !l.response || l.response.status !== "leased") break;
    midEventRowsJSON.push(JSON.stringify(l.events));
    for (const ev of l.events) applyEvent(stateAtMid, ev);
    extendLease(stateAtMid, l.response.lease_id, l.response.deadline_ms);
    active.push({
      runner: `runner-${m}`,
      machine: m,
      leaseID: l.response.lease_id,
      attempts: l.response.attempts ?? [],
    });
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
    const r = decideReport(stateAtMid, t, repCmd);
    if (r.error) throw r.error;
    if (r.events.length > 0) {
      midEventRowsJSON.push(JSON.stringify(r.events));
      for (const ev of r.events) applyEvent(stateAtMid, ev);
    }
    completed += slot.attempts.length;

    if (completed >= half) {
      // Leave slot idle (ready for midLeaseCmd) and keep another slot active (for midReportCmd).
      active.splice(cursor % active.length, 1);
      break;
    }

    reqSeq[slot.machine]!++;
    t += 50;
    const l = decideLease(stateAtMid, t, {
      runner_id: slot.runner,
      machine_index: slot.machine,
      request_id: `req-${slot.machine}-${reqSeq[slot.machine]}`,
    });
    if (l.error || !l.response || l.response.status !== "leased") {
      active.splice(cursor % active.length, 1);
      continue;
    }
    midEventRowsJSON.push(JSON.stringify(l.events));
    for (const ev of l.events) applyEvent(stateAtMid, ev);
    extendLease(stateAtMid, l.response.lease_id, l.response.deadline_ms);
    slot.leaseID = l.response.lease_id;
    slot.attempts = l.response.attempts ?? [];
    cursor++;
  }

  // Pick a runner without an active lease for midLeaseCmd.
  const idleMachine = 0;
  // Ensure runner-idle has no active lease (use runner-extra if needed).
  const midLeaseCmd: LeaseCommand = {
    runner_id: `runner-bench-idle`,
    machine_index: idleMachine,
    request_id: "req-bench-mid",
  };

  // Pick an active lease for midReportCmd.
  const repSlot = active[0]!;
  const midReportCmd: ReportCommand = {
    runner_id: repSlot.runner,
    lease_id: repSlot.leaseID,
    results: repSlot.attempts.map((a) => ({
      attempt_id: a.attempt_id,
      test_index: a.test_index,
      generation: a.generation,
      outcome: "passed",
      timed: true,
      duration_ms: predictions[a.test_index] ?? 1500,
    })),
  };
  const midReportReqJSON = JSON.stringify({ version: 1, ...midReportCmd });

  return {
    testCount,
    machines,
    cfg,
    now,
    initCmd,
    initReqJSON,
    stateAfterInit,
    stateAtMid,
    midNow: t + 100,
    midLeaseCmd,
    midReportCmd,
    midReportReqJSON,
    catalogChunksJSON,
    midEventRowsJSON,
    midEventCount: midEventRowsJSON.length,
  };
}

type OpName =
  | "init_core"
  | "init_full"
  | "first_lease"
  | "mid_lease"
  | "report"
  | "status"
  | "list_200"
  | "list_1000"
  | "wake_replay";

function runOpOnce(fx: ScenarioFixtures, op: OpName): number {
  const lim = defaultLimits();
  switch (op) {
    case "init_core":
      return measureCpuMS(() => {
        const s = newState(lim);
        const out = decideInit(s, fx.now, fx.initCmd);
        for (const ev of out.events) applyEvent(s, ev);
      });
    case "init_full":
      return measureCpuMS(() => {
        const parsed = JSON.parse(fx.initReqJSON) as InitCommand;
        const s = newState(lim);
        const out = decideInit(s, fx.now, parsed);
        // Serialize catalog chunks + init header row + HTTP response (matches DO work).
        const ev = out.events[0]!;
        const init = ev.initialized!;
        for (let off = 0; off < init.tests.length; off += 1000) {
          const end = Math.min(off + 1000, init.tests.length);
          JSON.stringify({
            tests: init.tests.slice(off, end),
            predictions_ms: init.predictions_ms.slice(off, end),
          });
        }
        JSON.stringify([{ type: "initialized", at_ms: ev.at_ms, initialized: { ...init, tests: [], predictions_ms: [] } }]);
        applyEvent(s, ev);
        JSON.stringify({ version: 1, ...out.response });
      });
    case "first_lease":
      return measureCpuMS(() => {
        const out = decideLease(fx.stateAfterInit, fx.now + 500, {
          runner_id: "runner-first",
          machine_index: 0,
          request_id: "req-first",
        });
        JSON.stringify(out.events);
        JSON.stringify({ version: 1, ...out.response });
      });
    case "mid_lease":
      return measureCpuMS(() => {
        const out = decideLease(fx.stateAtMid, fx.midNow, fx.midLeaseCmd);
        JSON.stringify(out.events);
        JSON.stringify({ version: 1, ...out.response });
      });
    case "report":
      return measureCpuMS(() => {
        const parsed = JSON.parse(fx.midReportReqJSON) as ReportCommand;
        const out = decideReport(fx.stateAtMid, fx.midNow, parsed);
        JSON.stringify(out.events);
        JSON.stringify({ version: 1, ...out.response });
      });
    case "status":
      return measureCpuMS(() => {
        const out = decideStatus(fx.stateAtMid, fx.midNow);
        JSON.stringify({ version: 1, ...out.response });
      });
    case "list_200":
      return measureCpuMS(() => {
        const out = decideListTests(fx.stateAtMid, fx.midNow, { cursor: 0, limit: 200 });
        JSON.stringify({ version: 1, ...out.response });
      });
    case "list_1000":
      return measureCpuMS(() => {
        const out = decideListTests(fx.stateAtMid, fx.midNow, { cursor: 0, limit: 1000 });
        JSON.stringify({ version: 1, ...out.response });
      });
    case "wake_replay":
      return measureCpuMS(() => {
        const s = newState(lim);
        const tests: string[] = [];
        const predictions: (number | null)[] = [];
        for (let i = 0; i < fx.catalogChunksJSON.length; i++) {
          const chunk = JSON.parse(fx.catalogChunksJSON[i]!) as { tests: string[]; predictions_ms: (number | null)[] };
          for (let j = 0; j < chunk.tests.length; j++) {
            tests.push(chunk.tests[j]!);
            predictions.push(chunk.predictions_ms[j]!);
          }
        }
        for (let i = 0; i < fx.midEventRowsJSON.length; i++) {
          const events = JSON.parse(fx.midEventRowsJSON[i]!) as QueueEvent[];
          for (let j = 0; j < events.length; j++) {
            const ev = events[j]!;
            if (ev.type === "initialized" && ev.initialized) {
              ev.initialized.tests = tests;
              ev.initialized.predictions_ms = predictions;
            }
            applyEvent(s, ev);
          }
        }
        resetActiveLeaseDeadlines(s, fx.midNow);
      });
  }
}

// Breakdown of init & wake_replay costs at 20,000 tests to guide optimization recommendations.
function measureBreakdown(testCount: number, machines: number) {
  const fx = buildFixtures(testCount, machines);
  const runs = 15;
  const runWarm = (fn: () => void) => {
    for (let i = 0; i < 3; i++) fn();
    const s: number[] = [];
    for (let i = 0; i < runs; i++) s.push(measureCpuMS(fn));
    return stats(s);
  };

  const jsonParseInit = runWarm(() => {
    JSON.parse(fx.initReqJSON);
  });
  const validate20k = runWarm(() => {
    validateTestList(fx.initCmd.tests, fx.initCmd.predictions_ms, 20_000);
  });
  const sha256_20k = runWarm(() => {
    configDigest(fx.cfg, fx.initCmd.tests);
  });
  const effAndSort = runWarm(() => {
    const eff = computeEffectivePredictions(fx.initCmd.predictions_ms);
    const order = new Array<number>(testCount);
    for (let i = 0; i < testCount; i++) order[i] = i;
    order.sort((a, b) => {
      const d = eff[b]! - eff[a]!;
      return d !== 0 ? d : a - b;
    });
  });
  const chunkSerialize = runWarm(() => {
    for (let off = 0; off < testCount; off += 1000) {
      const end = Math.min(off + 1000, testCount);
      JSON.stringify({
        tests: fx.initCmd.tests.slice(off, end),
        predictions_ms: fx.initCmd.predictions_ms.slice(off, end),
      });
    }
  });
  const chunkParse = runWarm(() => {
    const tests: string[] = [];
    const preds: (number | null)[] = [];
    for (let i = 0; i < fx.catalogChunksJSON.length; i++) {
      const c = JSON.parse(fx.catalogChunksJSON[i]!) as { tests: string[]; predictions_ms: (number | null)[] };
      for (let j = 0; j < c.tests.length; j++) {
        tests.push(c.tests[j]!);
        preds.push(c.predictions_ms[j]!);
      }
    }
  });
  const eventsReplayOnly = runWarm(() => {
    const s = newState(defaultLimits());
    // Trusted fast init (skipping redundant validateTestList + sha256 on replay)
    const initEv = JSON.parse(fx.midEventRowsJSON[0]!) as QueueEvent[];
    const d = initEv[0]!.initialized!;
    const eff = computeEffectivePredictions(fx.initCmd.predictions_ms);
    const tests = new Array(testCount);
    const order = new Array<number>(testCount);
    for (let i = 0; i < testCount; i++) {
      tests[i] = {
        index: i,
        path: fx.initCmd.tests[i]!,
        predictedMS: fx.initCmd.predictions_ms[i]!,
        effectiveMS: eff[i]!,
        status: "pending",
        generation: 0,
        activeLeaseID: 0,
        timed: false,
        durationMS: 0,
        attempts: [],
        history: [],
        historyTruncated: false,
      };
      order[i] = i;
    }
    order.sort((a, b) => {
      const diff = eff[b]! - eff[a]!;
      return diff !== 0 ? diff : a - b;
    });
    s.initialized = true;
    s.buildID = d.build_id;
    s.digest = d.digest;
    s.config = { ...d.config };
    s.createdAtMS = initEv[0]!.at_ms;
    s.deadlineMS = d.deadline_ms;
    s.lastSeenMS = initEv[0]!.at_ms;
    s.tests = tests;
    s.dealOrder = order;
    s.counts = { total: testCount, pending: testCount, leased: 0, passed: 0, failed: 0, skipped: 0, unresolved: 0 };

    for (let i = 1; i < fx.midEventRowsJSON.length; i++) {
      const events = JSON.parse(fx.midEventRowsJSON[i]!) as QueueEvent[];
      for (const ev of events) applyEvent(s, ev);
    }
    resetActiveLeaseDeadlines(s, fx.midNow);
  });

  // Compact snapshot restore cost (precomputed state JSON parsed directly).
  const compactSnapshot = JSON.stringify({
    buildID: fx.stateAtMid.buildID,
    digest: fx.stateAtMid.digest,
    config: fx.stateAtMid.config,
    createdAtMS: fx.stateAtMid.createdAtMS,
    deadlineMS: fx.stateAtMid.deadlineMS,
    finishedAtMS: fx.stateAtMid.finishedAtMS,
    lastSeenMS: fx.stateAtMid.lastSeenMS,
    counts: fx.stateAtMid.counts,
    dealOrder: fx.stateAtMid.dealOrder,
    // Compact per-test dynamic state: [statusCode, generation, activeLeaseID, timed, durationMS]
    testDyn: fx.stateAtMid.tests.map((t) => [t.status, t.generation, t.activeLeaseID, t.timed ? 1 : 0, t.durationMS]),
    leases: fx.stateAtMid.leases,
    activeLeases: fx.stateAtMid.activeLeases,
    runnerOrder: fx.stateAtMid.runnerOrder,
    runners: fx.stateAtMid.runnerOrder.map((id) => fx.stateAtMid.runners.get(id)),
  });
  const snapshotParseBytes = compactSnapshot.length;
  const snapshotSerialize = runWarm(() => {
    JSON.stringify({
      counts: fx.stateAtMid.counts,
      testDyn: fx.stateAtMid.tests.map((t) => [t.status, t.generation, t.activeLeaseID, t.timed ? 1 : 0, t.durationMS]),
      leases: fx.stateAtMid.leases,
      activeLeases: fx.stateAtMid.activeLeases,
    });
  });
  const snapshotParse = runWarm(() => {
    JSON.parse(compactSnapshot);
  });

  return {
    testCount,
    machines,
    midEventRows: fx.midEventCount,
    jsonParseInit,
    validate20k,
    sha256_20k,
    effAndSort,
    chunkSerialize,
    chunkParse,
    trustedReplayNoRevalidateNoSha: eventsReplayOnly,
    snapshotBytes: snapshotParseBytes,
    snapshotSerialize,
    snapshotParse,
  };
}

// Worker subprocess mode for true cold-start CPU measurement.
const args = process.argv.slice(2);
if (args[0] === "--cold") {
  const testCount = Number(args[1]);
  const machines = Number(args[2]);
  const op = args[3] as OpName;
  if (op === "init_core" || op === "init_full") {
    // Prepare inputs without warming up decideInit/applyEvent/Sha256!
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
      build_id: `bench-${testCount}-${machines}`,
      runner_id: "runner-0",
      machine_index: 0,
      config: cfg,
      tests,
      predictions_ms: predictions,
    };
    const initReqJSON = JSON.stringify({ version: 1, ...initCmd });
    const lim = defaultLimits();
    const ms =
      op === "init_core"
        ? measureCpuMS(() => {
            const s = newState(lim);
            const out = decideInit(s, now, initCmd);
            for (const ev of out.events) applyEvent(s, ev);
          })
        : measureCpuMS(() => {
            const parsed = JSON.parse(initReqJSON) as InitCommand;
            const s = newState(lim);
            const out = decideInit(s, now, parsed);
            const ev = out.events[0]!;
            const init = ev.initialized!;
            for (let off = 0; off < init.tests.length; off += 1000) {
              const end = Math.min(off + 1000, init.tests.length);
              JSON.stringify({
                tests: init.tests.slice(off, end),
                predictions_ms: init.predictions_ms.slice(off, end),
              });
            }
            JSON.stringify([
              { type: "initialized", at_ms: ev.at_ms, initialized: { ...init, tests: [], predictions_ms: [] } },
            ]);
            applyEvent(s, ev);
            JSON.stringify({ version: 1, ...out.response });
          });
    process.stdout.write(String(ms));
    process.exit(0);
  }

  // For other ops, build fixtures (note: building fixtures warms up init/lease/report slightly,
  // so for wake_replay we deserialize pre-built fixture data passed on stdin for a TRUE cold wake!).
  if (op === "wake_replay_cold") {
    const chunks: Buffer[] = [];
    for await (const c of process.stdin) chunks.push(c as Buffer);
    const payload = JSON.parse(Buffer.concat(chunks).toString("utf8")) as {
      catalogChunksJSON: string[];
      midEventRowsJSON: string[];
      midNow: number;
    };
    const lim = defaultLimits();
    const ms = measureCpuMS(() => {
      const s = newState(lim);
      const tests: string[] = [];
      const predictions: (number | null)[] = [];
      for (let i = 0; i < payload.catalogChunksJSON.length; i++) {
        const chunk = JSON.parse(payload.catalogChunksJSON[i]!) as {
          tests: string[];
          predictions_ms: (number | null)[];
        };
        for (let j = 0; j < chunk.tests.length; j++) {
          tests.push(chunk.tests[j]!);
          predictions.push(chunk.predictions_ms[j]!);
        }
      }
      for (let i = 0; i < payload.midEventRowsJSON.length; i++) {
        const events = JSON.parse(payload.midEventRowsJSON[i]!) as QueueEvent[];
        for (let j = 0; j < events.length; j++) {
          const ev = events[j]!;
          if (ev.type === "initialized" && ev.initialized) {
            ev.initialized.tests = tests;
            ev.initialized.predictions_ms = predictions;
          }
          applyEvent(s, ev);
        }
      }
      resetActiveLeaseDeadlines(s, payload.midNow);
    });
    process.stdout.write(String(ms));
    process.exit(0);
  }

  const fx = buildFixtures(testCount, machines);
  const ms = runOpOnce(fx, op);
  process.stdout.write(String(ms));
  process.exit(0);
}

// Main coordinator mode.
const sizes = [1_000, 5_000, 10_000, 20_000];
const machineCounts = [8, 64];
const ops: OpName[] = [
  "init_core",
  "init_full",
  "first_lease",
  "mid_lease",
  "report",
  "status",
  "list_200",
  "list_1000",
  "wake_replay",
];
const WARM_ITERS = 25;
const COLD_ITERS = 7;

const allResults: unknown[] = [];

for (const n of sizes) {
  for (const m of machineCounts) {
    const fx = buildFixtures(n, m);
    const warm: Record<string, { p50: number; max: number }> = {};
    const cold: Record<string, { p50: number; max: number }> = {};

    for (const op of ops) {
      // Warmup
      for (let w = 0; w < 5; w++) runOpOnce(fx, op);
      const samples: number[] = [];
      for (let i = 0; i < WARM_ITERS; i++) {
        samples.push(runOpOnce(fx, op));
      }
      warm[op] = stats(samples);
    }

    // Cold measurements (fresh Node process per iteration) for key ops.
    const coldOps: OpName[] = ["init_core", "init_full", "first_lease", "mid_lease", "report", "status", "list_200", "list_1000", "wake_replay"];
    const wakeStdin = JSON.stringify({
      catalogChunksJSON: fx.catalogChunksJSON,
      midEventRowsJSON: fx.midEventRowsJSON,
      midNow: fx.midNow,
    });
    for (const op of coldOps) {
      const samples: number[] = [];
      for (let i = 0; i < COLD_ITERS; i++) {
        const argOp = op === "wake_replay" ? "wake_replay_cold" : op;
        const out = execFileSync(process.execPath, [import.meta.filename, "--cold", String(n), String(m), argOp], {
          input: op === "wake_replay" ? wakeStdin : undefined,
          encoding: "utf8",
          maxBuffer: 64 * 1024 * 1024,
        });
        samples.push(Number(out.trim()));
      }
      cold[op] = stats(samples);
    }

    allResults.push({
      tests: n,
      machines: m,
      midEventRows: fx.midEventCount,
      warm,
      cold,
    });
  }
}

const breakdown20k = [measureBreakdown(20_000, 8), measureBreakdown(20_000, 64), measureBreakdown(10_000, 64)];

console.log(JSON.stringify({ cpu: allResults, breakdown: breakdown20k }, null, 2));
