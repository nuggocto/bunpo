// Reproduces the Worker core CPU and payload-size table published in
// docs/decisions/0003-live-queue.md using process.cpuUsage() under Node
// (timers do not advance during synchronous CPU work inside workerd).
//
// Usage: node bench/repro.ts

import {
  type ActiveLeaseSeed,
  type Config,
  type InitCommand,
  type QueueEvent,
  type ReportCommand,
  LeaseDeadlineTracker,
  applyEvent,
  decideInit,
  decideLease,
  decideListTests,
  decideReport,
  decideStatus,
  defaultLimits,
  extendLease,
  newState,
} from "../src/core/index.ts";

function generateTests(count: number): { tests: string[]; predictions: (number | null)[] } {
  const tests = new Array<string>(count);
  const predictions = new Array<number | null>(count);
  for (let i = 0; i < count; i++) {
    const pkg = String(i % 40).padStart(2, "0");
    const mod = String(Math.floor(i / 40) % 25).padStart(2, "0");
    const num = String(i).padStart(5, "0");
    tests[i] = `services/pkg_${pkg}/internal/mod_${mod}/handlers/order_${num}.spec.ts`;
    predictions[i] = i % 10 === 0 ? null : i % 20 === 0 ? 12_000 + (i % 6_000) : 300 + ((i * 7919) % 4_000);
  }
  const pairs = tests.map((t, idx) => ({ t, p: predictions[idx]! }));
  pairs.sort((a, b) => (a.t < b.t ? -1 : a.t > b.t ? 1 : 0));
  for (let i = 0; i < count; i++) {
    tests[i] = pairs[i]!.t;
    predictions[i] = pairs[i]!.p;
  }
  return { tests, predictions };
}

function cpuMS(fn: () => void): number {
  const start = process.cpuUsage();
  fn();
  const diff = process.cpuUsage(start);
  return (diff.user + diff.system) / 1000;
}

function warmStats(fn: () => void, iters = 20): { p50: number; max: number } {
  for (let i = 0; i < 5; i++) fn();
  const samples: number[] = [];
  for (let i = 0; i < iters; i++) samples.push(cpuMS(fn));
  samples.sort((a, b) => a - b);
  return {
    p50: Number(samples[Math.floor(samples.length / 2)]!.toFixed(2)),
    max: Number(samples[samples.length - 1]!.toFixed(2)),
  };
}

const rows: unknown[] = [];
for (const testCount of [1_000, 5_000, 10_000, 20_000]) {
  for (const machines of [8, 64]) {
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
      build_id: `repro-${testCount}-${machines}`,
      runner_id: "runner-0",
      machine_index: 0,
      config: cfg,
      tests,
      predictions_ms: predictions,
    };
    const initJSON = JSON.stringify({ version: 1, ...initCmd });

    const lim = defaultLimits();
    const mid = newState(lim);
    const initOut = decideInit(mid, now, initCmd);
    const initEv = initOut.events[0]!;
    applyEvent(mid, initEv, { trustedInit: true });

    const chunks: string[] = [];
    for (let off = 0; off < testCount; off += 1000) {
      chunks.push(JSON.stringify({ tests: tests.slice(off, off + 1000), predictions_ms: predictions.slice(off, off + 1000) }));
    }
    const evRows: string[] = [
      JSON.stringify([{ type: "initialized", at_ms: initEv.at_ms, initialized: { ...initEv.initialized!, tests: [], predictions_ms: [] } }]),
    ];

    let t = now + 1000;
    const active: { runner: string; machine: number; leaseID: number; attempts: { attempt_id: string; test_index: number; generation: number }[] }[] = [];
    const reqSeq = new Array<number>(machines).fill(0);
    let done = 0;
    for (let m = 0; m < machines; m++) {
      reqSeq[m]!++;
      const l = decideLease(mid, t, { runner_id: `r-${m}`, machine_index: m, request_id: `q-${m}-${reqSeq[m]}` });
      if (!l.response || l.response.status !== "leased") break;
      evRows.push(JSON.stringify(l.events));
      for (const ev of l.events) applyEvent(mid, ev);
      extendLease(mid, l.response.lease_id, l.response.deadline_ms);
      active.push({ runner: `r-${m}`, machine: m, leaseID: l.response.lease_id, attempts: l.response.attempts ?? [] });
      t += 50;
    }
    let idx = 0;
    while (done < Math.floor(testCount / 2) && active.length > 0) {
      const slot = active[idx % active.length]!;
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
      const r = decideReport(mid, t, repCmd);
      evRows.push(JSON.stringify(r.events));
      for (const ev of r.events) applyEvent(mid, ev);
      done += slot.attempts.length;
      if (done >= Math.floor(testCount / 2)) {
        active.splice(idx % active.length, 1);
        break;
      }
      reqSeq[slot.machine]!++;
      t += 50;
      const l = decideLease(mid, t, { runner_id: slot.runner, machine_index: slot.machine, request_id: `q-${slot.machine}-${reqSeq[slot.machine]}` });
      if (!l.response || l.response.status !== "leased") {
        active.splice(idx % active.length, 1);
        continue;
      }
      evRows.push(JSON.stringify(l.events));
      for (const ev of l.events) applyEvent(mid, ev);
      extendLease(mid, l.response.lease_id, l.response.deadline_ms);
      slot.leaseID = l.response.lease_id;
      slot.attempts = l.response.attempts ?? [];
      idx++;
    }

    const repSlot = active[0]!;
    const repCmd: ReportCommand = {
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
    const repJSON = JSON.stringify({ version: 1, ...repCmd });

    const initStat = warmStats(() => {
      const parsed = JSON.parse(initJSON) as InitCommand;
      const s = newState(lim);
      const out = decideInit(s, now, parsed);
      const ev = out.events[0]!;
      applyEvent(s, ev, { trustedInit: true });
      JSON.stringify({ version: 1, ...out.response });
    });
    const leaseStat = warmStats(() => {
      const out = decideLease(mid, t + 100, { runner_id: "r-idle", machine_index: 0, request_id: "q-mid" });
      JSON.stringify(out.events);
      JSON.stringify({ version: 1, ...out.response });
    });
    const reportStat = warmStats(() => {
      const parsed = JSON.parse(repJSON) as ReportCommand;
      const out = decideReport(mid, t + 100, parsed);
      JSON.stringify(out.events);
      JSON.stringify({ version: 1, ...out.response });
    });
    const statusStat = warmStats(() => {
      const out = decideStatus(mid, t + 100);
      JSON.stringify({ version: 1, ...out.response });
    });
    const listStat = warmStats(() => {
      const out = decideListTests(mid, t + 100, { cursor: 0, limit: 200 });
      JSON.stringify({ version: 1, ...out.response });
    });
    const wakeStat = warmStats(() => {
      const s = newState(lim);
      const pTests: string[] = [];
      const pPreds: (number | null)[] = [];
      for (const raw of chunks) {
        const c = JSON.parse(raw) as { tests: string[]; predictions_ms: (number | null)[] };
        for (let i = 0; i < c.tests.length; i++) {
          pTests.push(c.tests[i]!);
          pPreds.push(c.predictions_ms[i]!);
        }
      }
      for (const raw of evRows) {
        const evs = JSON.parse(raw) as QueueEvent[];
        for (const ev of evs) {
          if (ev.type === "initialized" && ev.initialized) {
            ev.initialized.tests = pTests;
            ev.initialized.predictions_ms = pPreds;
            applyEvent(s, ev, { trustedInit: true });
          } else {
            applyEvent(s, ev);
          }
        }
      }
      const tracker = new LeaseDeadlineTracker();
      const activeSeeds: ActiveLeaseSeed[] = [];
      for (let i = 0; i < s.leases.length; i++) {
        const l = s.leases[i]!;
        if (l.status === "active") {
          activeSeeds.push({ leaseID: l.lease_id, grantedDeadlineMS: l.deadline_ms });
        }
      }
      const eff = tracker.restoreOnWake(activeSeeds, null, s.config.lease_ttl_ms);
      for (const [id, dl] of eff) {
        extendLease(s, id, dl);
      }
    });

    rows.push({
      tests: testCount,
      machines,
      initBytes: initJSON.length,
      midEventRows: evRows.length,
      init_ms: initStat,
      lease_ms: leaseStat,
      report_ms: reportStat,
      status_ms: statusStat,
      list200_ms: listStat,
      wake_ms: wakeStat,
    });
  }
}

console.log(JSON.stringify(rows, null, 2));
