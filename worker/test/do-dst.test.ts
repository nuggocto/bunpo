// Seeded deterministic simulation test driving the real SQLite BuildDO inside
// local workerd with evictDurableObject() injected at seeded points (including
// after every request with a dead runner), asserting every response matches the
// pure TypeScript core and LeaseDeadlineTracker byte-for-byte.
//
// Replay a single failing seed:
//   BUNPO_DO_DST_SEED=<seed> npm run test:do

import { env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  type ActiveLeaseSeed,
  type Config,
  type InitCommand,
  type LeaseCommand,
  type Outcome,
  type QueueEvent,
  type ReportCommand,
  type SerializedDeadlines,
  LeaseDeadlineTracker,
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
  stateVerdict,
} from "../src/core/index.ts";

// Deterministic 32-bit mulberry32 PRNG.
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

interface DstOptions {
  evictEveryRequest?: boolean;
  deadRunner?: boolean;
  prefix: string;
}

async function runSeedThroughRealDO(seed: number, opts: DstOptions): Promise<void> {
  const rng = makeRng(seed);
  const bid = `${opts.prefix}-${seed}`;
  const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

  const oracle = newState(defaultLimits());
  const tracker = new LeaseDeadlineTracker();
  let storedDeadlines: SerializedDeadlines | null = null;
  const grantedDeadlines = new Map<number, number>();
  const lastRealDeadlines = new Map<number, number>();

  const numTests = 3 + randInt(rng, 7);
  const numRunners = 2 + randInt(rng, 3);
  const ttlMS = 10_000;
  const graceMS = Math.floor(ttlMS / 2);
  const maxAttempts = 2 + randInt(rng, 2);
  const maxBatch = 1 + randInt(rng, 3);
  const shortTimeout = !opts.deadRunner && rng() < 0.15;
  const cfg: Config = {
    machines: numRunners,
    lease_ttl_ms: ttlMS,
    build_timeout_ms: shortTimeout ? ttlMS * 2 : ttlMS * 30,
    max_attempts: maxAttempts,
    max_batch: maxBatch,
  };

  const tests = Array.from({ length: numTests }, (_, i) => `tests/t${String(i).padStart(2, "0")}.py`);
  const preds = tests.map(() => (rng() < 0.25 ? null : 100 + randInt(rng, 1500)));

  let now = 1_700_000_000_000;
  let evictedCount = 0;
  let currentlyEvicted = false;

  const applyOracleEvents = (events: QueueEvent[], trustedInit = false) => {
    for (const ev of events) {
      applyEvent(oracle, ev, trustedInit && ev.type === "initialized" ? { trustedInit: true } : undefined);
      if (ev.type === "leased" && ev.leased) {
        tracker.onGrant(ev.leased.lease_id, ev.leased.deadline_ms);
        grantedDeadlines.set(ev.leased.lease_id, ev.leased.deadline_ms);
        lastRealDeadlines.set(ev.leased.lease_id, ev.leased.deadline_ms);
      } else if (ev.type === "reported" && ev.reported?.close_lease) {
        tracker.onClose(ev.reported.lease_id);
      } else if (ev.type === "expired" && ev.expired) {
        tracker.onClose(ev.expired.lease_id);
      } else if (ev.type === "finalized") {
        tracker.clear();
      }
    }
  };

  const extendOracleLease = (leaseID: number, promisedDeadlineMS: number, atMS: number) => {
    const snap = tracker.planExtend(leaseID, atMS, oracle.config.lease_ttl_ms);
    if (snap !== null) {
      storedDeadlines = snap;
      tracker.commitPersist(snap);
    }
    const prevReal = lastRealDeadlines.get(leaseID) ?? 0;
    if (atMS + oracle.config.lease_ttl_ms > prevReal) {
      lastRealDeadlines.set(leaseID, atMS + oracle.config.lease_ttl_ms);
    }
    extendLease(oracle, leaseID, promisedDeadlineMS);
  };

  const doEvictAndSyncWake = async () => {
    if (currentlyEvicted) {
      return;
    }
    await evictDurableObject(stub);
    currentlyEvicted = true;
    evictedCount++;

    if (oracle.initialized) {
      const activeSeeds: ActiveLeaseSeed[] = [];
      for (const l of oracle.leases) {
        if (l.status === "active") {
          activeSeeds.push({
            leaseID: l.lease_id,
            grantedDeadlineMS: grantedDeadlines.get(l.lease_id)!,
          });
        }
      }
      const effective = tracker.restoreOnWake(activeSeeds, storedDeadlines, oracle.config.lease_ttl_ms);
      for (const [id, eff] of effective) {
        // Wake grace must never exceed lastRealDeadline + floor(ttl/2).
        const maxAllowed = (lastRealDeadlines.get(id) ?? 0) + graceMS;
        expect(eff, `seed=${seed} lease=${id} exceeded wake grace bound`).toBeLessThanOrEqual(maxAllowed);
        extendLease(oracle, id, eff);
      }
    }
  };

  const maybeEvict = async () => {
    if (opts.evictEveryRequest || rng() < 0.25) {
      await doEvictAndSyncWake();
    }
  };

  const assertMatch = (
    stepLabel: string,
    doReply: { status: number; body: string },
    expectedStatus: number,
    expectedBodyObj: unknown,
  ) => {
    currentlyEvicted = false;
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

  // Step 0: init from r0.
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
    const out = decideInit(oracle, now, initCmd);
    applyOracleEvents(out.events, true);
    const rep = await stub.init(bid, raw, now);
    assertMatch("init", rep, 200, { version: ProtocolVersion, ...out.response });
    await maybeEvict();
  }

  const runnerLeases = new Map<
    string,
    { leaseID: number; reqID: string; attempts: { attempt_id: string; test_index: number; generation: number }[] }
  >();
  let reqSeq = 1;
  let deadLeaseID: number | null = null;

  // In deadRunner mode, r0 takes one lease and then dies forever while other runners continue
  // working and the DO is evicted after every single request.
  if (opts.deadRunner) {
    const leaseCmd: LeaseCommand = { runner_id: "r0", machine_index: 0, request_id: `req-${reqSeq++}` };
    const raw = JSON.stringify({ version: ProtocolVersion, ...leaseCmd });
    const out = decideLease(oracle, now, leaseCmd);
    applyOracleEvents(out.events);
    if (!out.error && out.response?.status === "leased") {
      extendOracleLease(out.response.lease_id, out.response.deadline_ms, now);
      deadLeaseID = out.response.lease_id;
    }
    const rep = await stub.lease(raw, now);
    assertMatch("dead-r0-lease", rep, 200, { version: ProtocolVersion, ...out.response });
    await maybeEvict();
  }

  const totalSteps = opts.deadRunner ? 35 : 45;
  for (let step = 0; step < totalSteps; step++) {
    now += 1_000 + randInt(rng, 4_500);
    if (!opts.deadRunner && rng() < 0.12) {
      now += ttlMS + graceMS + 500;
    }

    // In deadRunner mode, r0 never sends another request after leasing.
    const rIdx = opts.deadRunner ? 1 + randInt(rng, numRunners - 1) : randInt(rng, numRunners);
    const runnerID = `r${rIdx}`;
    const actionRoll = rng();
    const held = runnerLeases.get(runnerID);

    if (actionRoll < 0.12) {
      const out = decideStatus(oracle, now);
      applyOracleEvents(out.events);
      const rep = await stub.status(now);
      assertMatch(`step-${step}-status`, rep, 200, { version: ProtocolVersion, ...out.response });
    } else if (actionRoll < 0.22) {
      const listCmd = { cursor: 0, limit: 3, retried: rng() < 0.5 };
      const out = decideListTests(oracle, now, listCmd);
      applyOracleEvents(out.events);
      const rep = await stub.listTests(listCmd, now);
      assertMatch(`step-${step}-list`, rep, 200, { version: ProtocolVersion, ...out.response });
    } else if (actionRoll < 0.28) {
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
      const out = decideInit(oracle, now, c2);
      applyOracleEvents(out.events);
      const rep = await stub.init(bid, raw, now);
      if (out.error) {
        assertMatch(`step-${step}-init-err`, rep, out.error.httpStatus, {
          version: ProtocolVersion,
          error: { code: out.error.code, message: out.error.message },
        });
      } else {
        assertMatch(`step-${step}-init-dup`, rep, 200, { version: ProtocolVersion, ...out.response });
      }
    } else if (held && actionRoll < 0.52) {
      const hbCmd = { runner_id: runnerID, lease_id: held.leaseID };
      const raw = JSON.stringify({ version: ProtocolVersion, ...hbCmd });
      const out = decideHeartbeat(oracle, now, hbCmd);
      applyOracleEvents(out.events);
      if (!out.error && out.response?.held) {
        extendOracleLease(hbCmd.lease_id, out.response.deadline_ms, now);
      }
      const rep = await stub.heartbeat(raw, now);
      assertMatch(`step-${step}-hb`, rep, 200, { version: ProtocolVersion, ...out.response });
      if (!out.response?.held) {
        runnerLeases.delete(runnerID);
      }
    } else if (held && actionRoll < 0.85) {
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
      const out = decideReport(oracle, now, repCmd);
      applyOracleEvents(out.events);
      const rep = await stub.report(raw, now);
      assertMatch(`step-${step}-report`, rep, 200, { version: ProtocolVersion, ...out.response });
      if (rng() >= 0.2) {
        runnerLeases.delete(runnerID);
      }
    } else {
      const reuseReq = held && rng() < 0.4;
      const leaseCmd: LeaseCommand = {
        runner_id: runnerID,
        machine_index: rIdx,
        request_id: reuseReq ? held.reqID : `req-${reqSeq++}`,
      };
      const raw = JSON.stringify({ version: ProtocolVersion, ...leaseCmd });
      const out = decideLease(oracle, now, leaseCmd);
      applyOracleEvents(out.events);
      if (!out.error && out.response?.status === "leased") {
        extendOracleLease(out.response.lease_id, out.response.deadline_ms, now);
      }
      const rep = await stub.lease(raw, now);
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

    await maybeEvict();

    if (!opts.deadRunner && stateVerdict(oracle) !== "running" && step > 10 && rng() < 0.4) {
      break;
    }
  }

  // In deadRunner mode, verify r0's abandoned lease expired well before build timeout.
  if (opts.deadRunner && deadLeaseID !== null) {
    const deadLease = oracle.leases[deadLeaseID - 1]!;
    expect(deadLease.status, `seed=${seed}: dead runner lease ${deadLeaseID} never expired`).not.toBe("active");
  }

  // Final eviction + status check verifying full SQLite replay parity.
  await doEvictAndSyncWake();
  now += 100;
  const finalOut = decideStatus(oracle, now);
  applyOracleEvents(finalOut.events);
  const finalRep = await stub.status(now);
  assertMatch("final-status", finalRep, 200, { version: ProtocolVersion, ...finalOut.response });
}

describe("Durable Object seeded eviction DST", () => {
  it("matches pure core across 200 seeded builds with random DO evictions", async () => {
    const single = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.[
      "BUNPO_DO_DST_SEED"
    ];
    if (single) {
      await runSeedThroughRealDO(Number(single), { prefix: "dst-seed" });
      return;
    }

    const totalSeeds = 200;
    for (let seed = 1; seed <= totalSeeds; seed++) {
      try {
        await runSeedThroughRealDO(seed, { prefix: "dst-seed" });
      } catch (err) {
        throw new Error(
          `${err instanceof Error ? err.message : String(err)}\n\nReplay:\n  BUNPO_DO_DST_SEED=${seed} npm run test:do`,
        );
      }
    }
  });

  it("matches pure core with eviction after EVERY request and expires dead runners within ttl/2", async () => {
    const single = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.[
      "BUNPO_DO_DST_SEED"
    ];
    if (single) {
      await runSeedThroughRealDO(Number(single), {
        prefix: "dst-every",
        evictEveryRequest: true,
        deadRunner: true,
      });
      return;
    }

    for (let seed = 1; seed <= 50; seed++) {
      try {
        await runSeedThroughRealDO(seed, {
          prefix: "dst-every",
          evictEveryRequest: true,
          deadRunner: true,
        });
      } catch (err) {
        throw new Error(
          `${err instanceof Error ? err.message : String(err)}\n\nReplay:\n  BUNPO_DO_DST_SEED=${seed} npm run test:do`,
        );
      }
    }
  });
});
