// Seeded Deterministic Simulation Test driving the REAL SQLite BuildDO inside
// local workerd with evictDurableObject() injected at seeded points, asserting
// every single response matches the pure TypeScript core byte-for-byte.
//
// Replay a single failing seed:
//   BUNPO_DO_DST_SEED=<seed> npm run test:do

import { env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  type Config,
  type InitCommand,
  type LeaseCommand,
  type Outcome,
  type ReportCommand,
  ProtocolVersion,
  applyEvent,
  decideHeartbeat,
  decideInit,
  decideLease,
  decideListTests,
  decideReport,
  decideStatus,
  defaultLimits,
  extendLease,
  newState,
  resetActiveLeaseDeadlines,
  stateVerdict,
} from "../src/core/index.ts";

// Deterministic xorshift64* PRNG (BigInt-free 32-bit mulberry32 for speed).
function makeRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randInt(rng: () => number, maxExclusive: number): number {
  return Math.floor(rng() * maxExclusive);
}

function canonicalJSON(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value as Record<string, unknown>).sort()) {
      const v = (value as Record<string, unknown>)[k];
      if (v !== undefined) out[k] = sortKeys(v);
    }
    return out;
  }
  return value;
}

async function runSeedThroughRealDO(seed: number): Promise<void> {
  const rng = makeRng(seed);
  const bid = `dst-seed-${seed}`;
  const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

  const lim = defaultLimits();
  lim.maxTests = 5_000;
  const oracle = newState(lim);

  const numTests = 3 + randInt(rng, 7);
  const numRunners = 2 + randInt(rng, 3);
  const ttlMS = 10_000;
  const maxAttempts = 2 + randInt(rng, 2);
  const maxBatch = 1 + randInt(rng, 3);
  const shortTimeout = rng() < 0.15;
  const cfg: Config = {
    machines: numRunners,
    lease_ttl_ms: ttlMS,
    build_timeout_ms: shortTimeout ? ttlMS * 2 : ttlMS * 20,
    max_attempts: maxAttempts,
    max_batch: maxBatch,
  };

  const tests = Array.from({ length: numTests }, (_, i) => `tests/t${String(i).padStart(2, "0")}.py`);
  const preds = tests.map(() => (rng() < 0.25 ? null : 100 + randInt(rng, 1500)));

  let now = 1_700_000_000_000;
  let pendingWakeReset = false;
  let evictedCount = 0;

  const maybeEvict = async (stepLabel: string) => {
    // Evict with ~25% probability on every step so evictions happen across every state transition.
    if (rng() < 0.25) {
      await evictDurableObject(stub);
      pendingWakeReset = true;
      evictedCount++;
    }
    void stepLabel;
  };

  const syncOracleWake = (atMS: number) => {
    if (pendingWakeReset) {
      pendingWakeReset = false;
      if (oracle.initialized && stateVerdict(oracle) === "running") {
        resetActiveLeaseDeadlines(oracle, atMS);
      }
    }
  };

  const assertMatch = (
    stepLabel: string,
    doReply: { status: number; body: string },
    expectedStatus: number,
    expectedBodyObj: unknown,
  ) => {
    if (doReply.status !== expectedStatus) {
      throw new Error(
        `seed ${seed} (evictions=${evictedCount}) @ ${stepLabel}: status=${doReply.status}, want ${expectedStatus}\nDO body: ${doReply.body}`,
      );
    }
    const gotCan = canonicalJSON(JSON.parse(doReply.body));
    const wantCan = canonicalJSON(expectedBodyObj);
    if (gotCan !== wantCan) {
      throw new Error(
        `seed ${seed} (evictions=${evictedCount}) @ ${stepLabel}: body mismatch\ngot:  ${gotCan}\nwant: ${wantCan}`,
      );
    }
  };

  // Step 0: init from runner-0 (and optional duplicate/conflicting init).
  const initCmd: InitCommand = {
    build_id: bid,
    runner_id: "r0",
    machine_index: 0,
    config: cfg,
    tests,
    predictions_ms: preds,
  };
  {
    const raw = JSON.stringify({ version: ProtocolVersion, ...initCmd, build_id: undefined });
    syncOracleWake(now);
    const out = decideInit(oracle, now, initCmd);
    for (const ev of out.events) applyEvent(oracle, ev, { trustedInit: true });
    const rep = await stub.init(now, bid, raw);
    assertMatch("init", rep, 200, { version: ProtocolVersion, ...out.response });
    await maybeEvict("after-init");
  }

  // Track active leases per runner for realistic interleavings (heartbeats, expiries, stale/dup reports, conflicts).
  const runnerLeases = new Map<
    string,
    { leaseID: number; reqID: string; attempts: { attempt_id: string; test_index: number; generation: number }[] }
  >();
  let reqSeq = 1;

  for (let step = 0; step < 45; step++) {
    now += 500 + randInt(rng, 4_500); // occasionally jumps past TTL (10_000 ms)
    if (rng() < 0.12) {
      now += ttlMS + 500; // force lease expiry
    }

    const rIdx = randInt(rng, numRunners);
    const runnerID = `r${rIdx}`;
    const actionRoll = rng();
    const held = runnerLeases.get(runnerID);

    if (actionRoll < 0.12) {
      // Status check
      syncOracleWake(now);
      const out = decideStatus(oracle, now);
      for (const ev of out.events) applyEvent(oracle, ev);
      const rep = await stub.status(now);
      assertMatch(`step-${step}-status`, rep, 200, { version: ProtocolVersion, ...out.response });
    } else if (actionRoll < 0.22) {
      // ListTests check
      const listCmd = { cursor: 0, limit: 3, retried: rng() < 0.5 };
      syncOracleWake(now);
      const out = decideListTests(oracle, now, listCmd);
      for (const ev of out.events) applyEvent(oracle, ev);
      const rep = await stub.listTests(now, listCmd);
      assertMatch(`step-${step}-list`, rep, 200, { version: ProtocolVersion, ...out.response });
    } else if (actionRoll < 0.28) {
      // Divergent or duplicate init
      const div = rng() < 0.5;
      const c2: InitCommand = {
        build_id: bid,
        runner_id: div ? `div-${step}` : runnerID,
        machine_index: rIdx,
        config: cfg,
        tests: div ? [...tests, "tests/zz_extra.py"] : tests,
        predictions_ms: div ? [...preds, null] : preds,
      };
      const raw = JSON.stringify({
        version: ProtocolVersion,
        runner_id: c2.runner_id,
        machine_index: c2.machine_index,
        config: c2.config,
        tests: c2.tests,
        predictions_ms: c2.predictions_ms,
      });
      syncOracleWake(now);
      const out = decideInit(oracle, now, c2);
      for (const ev of out.events) applyEvent(oracle, ev);
      const rep = await stub.init(now, bid, raw);
      if (out.error) {
        assertMatch(`step-${step}-init-err`, rep, out.error.httpStatus, {
          version: ProtocolVersion,
          error: { code: out.error.code, message: out.error.message },
        });
      } else {
        assertMatch(`step-${step}-init-dup`, rep, 200, { version: ProtocolVersion, ...out.response });
      }
    } else if (held && actionRoll < 0.52) {
      // Heartbeat
      const hbCmd = { runner_id: runnerID, lease_id: held.leaseID };
      const raw = JSON.stringify({ version: ProtocolVersion, ...hbCmd });
      syncOracleWake(now);
      const out = decideHeartbeat(oracle, now, hbCmd);
      for (const ev of out.events) applyEvent(oracle, ev);
      if (!out.error && out.response?.held) {
        extendLease(oracle, hbCmd.lease_id, out.response.deadline_ms);
      }
      const rep = await stub.heartbeat(now, raw);
      assertMatch(`step-${step}-hb`, rep, 200, { version: ProtocolVersion, ...out.response });
      if (!out.response?.held) {
        runnerLeases.delete(runnerID);
      }
    } else if (held && actionRoll < 0.85) {
      // Report (normal, duplicate, or stale if expired)
      const outcomes: Outcome[] = ["passed", "passed", "passed", "skipped", "error", "failed"];
      const repCmd: ReportCommand = {
        runner_id: runnerID,
        lease_id: held.leaseID,
        results: held.attempts.map((a) => {
          const outcome = outcomes[randInt(rng, outcomes.length)]!;
          const timed = outcome === "passed";
          return {
            attempt_id: a.attempt_id,
            test_index: a.test_index,
            generation: a.generation,
            outcome,
            timed,
            duration_ms: timed ? 50 + randInt(rng, 500) : 0,
          };
        }),
      };
      const raw = JSON.stringify({ version: ProtocolVersion, ...repCmd });
      syncOracleWake(now);
      const out = decideReport(oracle, now, repCmd);
      for (const ev of out.events) applyEvent(oracle, ev);
      const rep = await stub.report(now, raw);
      assertMatch(`step-${step}-report`, rep, 200, { version: ProtocolVersion, ...out.response });
      // Keep held 20% of the time to trigger duplicate/conflict report on a later step!
      if (rng() >= 0.2) {
        runnerLeases.delete(runnerID);
      }
    } else {
      // Lease (fresh or repeated request_id)
      const reuseReq = held && rng() < 0.4;
      const leaseCmd: LeaseCommand = {
        runner_id: runnerID,
        machine_index: rIdx,
        request_id: reuseReq ? held.reqID : `req-${reqSeq++}`,
      };
      const raw = JSON.stringify({ version: ProtocolVersion, ...leaseCmd });
      syncOracleWake(now);
      const out = decideLease(oracle, now, leaseCmd);
      for (const ev of out.events) applyEvent(oracle, ev);
      if (!out.error && out.response?.status === "leased") {
        extendLease(oracle, out.response.lease_id, out.response.deadline_ms);
      }
      const rep = await stub.lease(now, raw);
      if (out.error) {
        assertMatch(`step-${step}-lease-err`, rep, out.error.httpStatus, {
          version: ProtocolVersion,
          error: { code: out.error.code, message: out.error.message },
        });
      } else {
        assertMatch(`step-${step}-lease`, rep, 200, { version: ProtocolVersion, ...out.response });
        if (out.response.status === "leased") {
          runnerLeases.set(runnerID, {
            leaseID: out.response.lease_id,
            reqID: leaseCmd.request_id,
            attempts: out.response.attempts ?? [],
          });
        }
      }
    }

    await maybeEvict(`after-step-${step}`);

    if (stateVerdict(oracle) !== "running" && step > 10 && rng() < 0.4) {
      break;
    }
  }

  // Final eviction + status check verifying full SQLite replay parity.
  if (!pendingWakeReset) {
    await evictDurableObject(stub);
    pendingWakeReset = true;
  }
  now += 100;
  syncOracleWake(now);
  const finalOut = decideStatus(oracle, now);
  for (const ev of finalOut.events) applyEvent(oracle, ev);
  const finalRep = await stub.status(now);
  assertMatch("final-status", finalRep, 200, { version: ProtocolVersion, ...finalOut.response });
}

describe("Durable Object seeded eviction DST", () => {
  it("matches pure core across 200 seeded builds with random DO evictions", async () => {
    const single = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.[
      "BUNPO_DO_DST_SEED"
    ];
    if (single) {
      await runSeedThroughRealDO(Number(single));
      return;
    }

    const totalSeeds = 200;
    for (let seed = 1; seed <= totalSeeds; seed++) {
      try {
        await runSeedThroughRealDO(seed);
      } catch (err) {
        throw new Error(
          `${err instanceof Error ? err.message : String(err)}\n\nReplay:\n  BUNPO_DO_DST_SEED=${seed} npm run test:do`,
        );
      }
    }
  });
});
