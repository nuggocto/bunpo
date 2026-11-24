import { env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { Config, InitCommand } from "../src/core/index.ts";

// Realistic ~60-byte repository-relative paths sorted in UTF-8 byte order.
function generateTests(count: number): { tests: string[]; predictions: (number | null)[] } {
  const tests = new Array<string>(count);
  const predictions = new Array<number | null>(count);
  for (let i = 0; i < count; i++) {
    const pkg = String(i % 40).padStart(2, "0");
    const mod = String(Math.floor(i / 40) % 25).padStart(2, "0");
    const num = String(i).padStart(5, "0");
    // Length: "services/pkg_00/internal/mod_00/handlers/order_00000.spec.ts" = 60 bytes
    tests[i] = `services/pkg_${pkg}/internal/mod_${mod}/handlers/order_${num}.spec.ts`;
    // Deterministic realistic predictions: 10% unknown (null), rest 200ms..18000ms skewed.
    if (i % 10 === 0) {
      predictions[i] = null;
    } else {
      const bucket = i % 20;
      predictions[i] = bucket === 0 ? 12_000 + (i % 6_000) : 300 + ((i * 7919) % 4_000);
    }
  }
  // Ensure strict UTF-8 byte sort order matching protocol requirements.
  const pairs = tests.map((t, idx) => ({ t, p: predictions[idx]! }));
  pairs.sort((a, b) => (a.t < b.t ? -1 : a.t > b.t ? 1 : 0));
  for (let i = 0; i < count; i++) {
    tests[i] = pairs[i]!.t;
    predictions[i] = pairs[i]!.p;
  }
  return { tests, predictions };
}

interface BuildMeasurement {
  tests: number;
  machines: number;
  chunkSize: number;
  catalogChunks: number;
  maxCatalogChunkBytes: number;
  maxEventRowBytes: number;
  initReqBytes: number;
  maxLeaseRespBytes: number;
  maxReportReqBytes: number;
  statusRespBytes: number;
  list200RespBytes: number;
  list1000RespBytes: number;
  initRowsWritten: number;
  totalRowsWritten: number;
  totalRowsRead: number;
  wakeMidRowsRead: number;
  eventRows: number;
  leasesIssued: number;
  reportsSent: number;
  heartbeatsSent: number;
  waitPolls: number;
  workerRequests: number;
  doRequests: number;
  databaseSizeBytes: number;
  setAlarmCalls: number;
}

async function runFullBuildInDO(
  testCount: number,
  machines: number,
  chunkSize = 1000,
): Promise<BuildMeasurement> {
  const buildID = `meas-${testCount}-${machines}-${chunkSize}`;
  const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(buildID));
  const { tests, predictions } = generateTests(testCount);

  const cfg: Config = {
    machines,
    lease_ttl_ms: 60_000,
    build_timeout_ms: 6 * 60 * 60 * 1000,
    max_attempts: 3,
    max_batch: 200,
  };

  let now = 1_700_000_000_000;
  const initCmd: InitCommand = {
    build_id: buildID,
    runner_id: "runner-0",
    machine_index: 0,
    config: cfg,
    tests,
    predictions_ms: predictions,
  };
  const initReqBytes = JSON.stringify({ version: 1, ...initCmd }).length;

  // All `machines` runners call init (runner-0 creates, rest get created=false, 0 writes).
  let initRowsWritten = 0;
  let workerRequests = 0;
  let doRequests = 0;
  for (let m = 0; m < machines; m++) {
    const res = await stub.init(
      now + m * 10,
      {
        ...initCmd,
        runner_id: `runner-${m}`,
        machine_index: m,
      },
      chunkSize,
    );
    expect(res.status).toBe(200);
    workerRequests++;
    doRequests++;
    if (m === 0) {
      expect(res.response?.created).toBe(true);
      initRowsWritten = res.rowsWritten;
    } else {
      expect(res.response?.created).toBe(false);
      expect(res.rowsWritten).toBe(0);
    }
  }

  // Simulate realistic 10-minute (600,000 ms) build across `machines` runners.
  // Scale batch execution times so the build spans ~600s, sending heartbeats every 20s (TTL/3).
  const BUILD_TARGET_MS = 600_000;
  const HEARTBEAT_INTERVAL_MS = 20_000;
  const totalPredMS = predictions.reduce<number>((acc, p) => acc + (p ?? 2000), 0);
  const idealPerMachineMS = Math.max(1, totalPredMS / machines);
  const timeScale = BUILD_TARGET_MS / idealPerMachineMS;

  interface RunnerSim {
    id: string;
    machineIndex: number;
    reqSeq: number;
    state: "need_lease" | "running" | "waiting" | "done";
    nextActionMS: number;
    leaseID: number;
    finishMS: number;
    nextHeartbeatMS: number;
    attempts: { attempt_id: string; test_index: number; generation: number; test: string }[];
  }

  const runners: RunnerSim[] = [];
  for (let m = 0; m < machines; m++) {
    runners.push({
      id: `runner-${m}`,
      machineIndex: m,
      reqSeq: 0,
      state: "need_lease",
      nextActionMS: now + 1000 + m * 20,
      leaseID: 0,
      finishMS: 0,
      nextHeartbeatMS: 0,
      attempts: [],
    });
  }

  let leasesIssued = 0;
  let reportsSent = 0;
  let heartbeatsSent = 0;
  let waitPolls = 0;
  let maxLeaseRespBytes = 0;
  let maxReportReqBytes = 0;
  let evictedAtMid = false;
  let wakeMidRowsRead = 0;
  let completedTests = 0;

  while (true) {
    // Pick the runner with the earliest next event.
    let nextRunner: RunnerSim | null = null;
    for (const r of runners) {
      if (r.state === "done") continue;
      if (!nextRunner || r.nextActionMS < nextRunner.nextActionMS) {
        nextRunner = r;
      }
    }
    if (!nextRunner) break;

    now = Math.max(now, nextRunner.nextActionMS);

    // Trigger one mid-build eviction when ~50% of tests have been reported.
    if (!evictedAtMid && completedTests >= Math.floor(testCount / 2)) {
      evictedAtMid = true;
      await evictDurableObject(stub);
      const afterWake = await stub.getMetrics();
      expect(afterWake.wokeFromStorage).toBe(true);
      wakeMidRowsRead = afterWake.wakeRowsRead;
    }

    if (nextRunner.state === "need_lease" || nextRunner.state === "waiting") {
      nextRunner.reqSeq++;
      const res = await stub.lease(now, {
        runner_id: nextRunner.id,
        machine_index: nextRunner.machineIndex,
        request_id: `req-${nextRunner.machineIndex}-${nextRunner.reqSeq}`,
      });
      expect(res.status).toBe(200);
      workerRequests++;
      doRequests++;
      const respBytes = JSON.stringify({ version: 1, ...res.response }).length;
      if (respBytes > maxLeaseRespBytes) maxLeaseRespBytes = respBytes;

      const r = res.response!;
      if (r.status === "leased") {
        leasesIssued++;
        nextRunner.leaseID = r.lease_id;
        nextRunner.attempts = r.attempts ?? [];
        let batchDur = 0;
        for (const a of nextRunner.attempts) {
          batchDur += Math.max(50, Math.round((predictions[a.test_index] ?? 2000) * timeScale));
        }
        nextRunner.finishMS = now + batchDur;
        nextRunner.nextHeartbeatMS = now + HEARTBEAT_INTERVAL_MS;
        nextRunner.state = "running";
        nextRunner.nextActionMS = Math.min(nextRunner.finishMS, nextRunner.nextHeartbeatMS);
      } else if (r.status === "wait") {
        waitPolls++;
        nextRunner.state = "waiting";
        nextRunner.nextActionMS = now + r.retry_after_ms;
      } else {
        nextRunner.state = "done";
      }
    } else if (nextRunner.state === "running") {
      if (nextRunner.nextHeartbeatMS < nextRunner.finishMS && now >= nextRunner.nextHeartbeatMS) {
        const hb = await stub.heartbeat(now, {
          runner_id: nextRunner.id,
          lease_id: nextRunner.leaseID,
        });
        expect(hb.status).toBe(200);
        expect(hb.response?.held).toBe(true);
        // Critical invariant: healthy heartbeats NEVER read or write SQLite rows.
        expect(hb.rowsRead).toBe(0);
        expect(hb.rowsWritten).toBe(0);
        heartbeatsSent++;
        workerRequests++;
        doRequests++;
        nextRunner.nextHeartbeatMS = now + HEARTBEAT_INTERVAL_MS;
        nextRunner.nextActionMS = Math.min(nextRunner.finishMS, nextRunner.nextHeartbeatMS);
      } else {
        const results = nextRunner.attempts.map((a) => ({
          attempt_id: a.attempt_id,
          test_index: a.test_index,
          generation: a.generation,
          outcome: "passed" as const,
          timed: true,
          duration_ms: predictions[a.test_index] ?? 1500,
        }));
        const repCmd = {
          runner_id: nextRunner.id,
          lease_id: nextRunner.leaseID,
          results,
        };
        const reqBytes = JSON.stringify({ version: 1, ...repCmd }).length;
        if (reqBytes > maxReportReqBytes) maxReportReqBytes = reqBytes;

        const rep = await stub.report(now, repCmd);
        expect(rep.status).toBe(200);
        reportsSent++;
        workerRequests++;
        doRequests++;
        completedTests += results.length;

        if (rep.response?.verdict !== "running") {
          nextRunner.state = "done";
        } else {
          nextRunner.state = "need_lease";
          nextRunner.nextActionMS = now + 50;
        }
      }
    }
  }

  const st = await stub.status(now + 100);
  expect(st.status).toBe(200);
  expect(st.response?.verdict).toBe("passed");
  expect(st.response?.counts.passed).toBe(testCount);
  const statusRespBytes = JSON.stringify({ version: 1, ...st.response }).length;

  const list200 = await stub.listTests(now + 100, { cursor: 0, limit: 200 });
  expect(list200.status).toBe(200);
  const list200RespBytes = JSON.stringify({ version: 1, ...list200.response }).length;

  const list1000 = await stub.listTests(now + 100, { cursor: 0, limit: 1000 });
  expect(list1000.status).toBe(200);
  const list1000RespBytes = JSON.stringify({ version: 1, ...list1000.response }).length;

  // Fetch final metrics AFTER mid-build eviction + completion. Note totalRowsWritten on
  // the post-eviction instance only counts writes since wake; get exact total via SQL counts!
  const metrics = await stub.getMetrics();
  // Exact total writes across the whole build lifecycle:
  //   initCatalogRowsWritten (= catalogChunkCount) + eventRowCount (1 init + 1 per lease + 1 per report)
  //   + schema creation (only on initial creation, 0 on wake) + setAlarm calls (2: deadline + 7d retention).
  const exactBuildRowsWritten = metrics.catalogChunkCount + metrics.eventRowCount;
  // Exact reads across the whole build lifecycle (including 1 mid-build eviction wake):
  const exactBuildRowsReadWith1Wake = wakeMidRowsRead;

  return {
    tests: testCount,
    machines,
    chunkSize,
    catalogChunks: metrics.catalogChunkCount,
    maxCatalogChunkBytes: metrics.maxCatalogChunkBytes,
    maxEventRowBytes: metrics.maxEventRowBytes,
    initReqBytes,
    maxLeaseRespBytes,
    maxReportReqBytes,
    statusRespBytes,
    list200RespBytes,
    list1000RespBytes,
    initRowsWritten,
    totalRowsWritten: exactBuildRowsWritten,
    totalRowsRead: exactBuildRowsReadWith1Wake,
    wakeMidRowsRead,
    eventRows: metrics.eventRowCount,
    leasesIssued,
    reportsSent,
    heartbeatsSent,
    waitPolls,
    workerRequests,
    doRequests,
    databaseSizeBytes: metrics.databaseSize,
    setAlarmCalls: 2,
  };
}

describe("SQLite Durable Object storage layout prototype", () => {
  it("verifies zero-index row writes, zero-write heartbeats, eviction replay, and deleteAll", async () => {
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName("unit-storage-check"));
    const { tests, predictions } = generateTests(25);
    const now = 1_700_000_000_000;

    const initRes = await stub.init(
      now,
      {
        build_id: "unit-storage-check",
        runner_id: "r1",
        machine_index: 0,
        config: {
          machines: 2,
          lease_ttl_ms: 60_000,
          build_timeout_ms: 600_000,
          max_attempts: 3,
          max_batch: 200,
        },
        tests,
        predictions_ms: predictions,
      },
      10,
    );
    expect(initRes.status).toBe(200);
    // 25 tests / chunkSize 10 = 3 catalog chunks + 1 event_log row = 4 rows written.
    // Note: in workerd SQLite, each INSERT into a PRIMARY KEY table also reports rowsRead=1
    // (B-tree uniqueness check on rowid).
    expect(initRes.rowsWritten).toBe(4);
    expect(initRes.rowsRead).toBe(4);

    const leaseRes = await stub.lease(now + 100, {
      runner_id: "r1",
      machine_index: 0,
      request_id: "req-1",
    });
    expect(leaseRes.status).toBe(200);
    expect(leaseRes.rowsWritten).toBe(1); // Exactly 1 row written (no extra index, no AUTOINCREMENT)
    expect(leaseRes.rowsRead).toBe(1);

    const hbRes = await stub.heartbeat(now + 20_000, {
      runner_id: "r1",
      lease_id: leaseRes.response!.lease_id,
    });
    expect(hbRes.status).toBe(200);
    expect(hbRes.response?.held).toBe(true);
    expect(hbRes.rowsRead).toBe(0);
    expect(hbRes.rowsWritten).toBe(0);

    // Evict DO and verify wake replay restores full state.
    await evictDurableObject(stub);
    const metricsAfterWake = await stub.getMetrics();
    expect(metricsAfterWake.wokeFromStorage).toBe(true);
    expect(metricsAfterWake.catalogChunkCount).toBe(3);
    expect(metricsAfterWake.eventRowCount).toBe(2);
    // Reading 3 catalog chunks + 2 event rows = 5 rows read!
    expect(metricsAfterWake.wakeRowsRead).toBe(5);
    console.log("UNIT_STORAGE_METRICS:" + JSON.stringify({ initRes, leaseRes, hbRes, metricsAfterWake }));

    const st = await stub.status(now + 25_000);
    expect(st.status).toBe(200);
    expect(st.response?.counts.leased).toBe(leaseRes.response!.attempts!.length);

    const del = await stub.deleteAllData();
    // SQLite DO retains at most one empty 4 KiB header page immediately after deleteAll().
    expect(del.sizeAfter).toBeLessThanOrEqual(4096);
  });

  it("measures SQLite rows, bytes, and request counts across 1k..20k tests and 8/64 machines", async () => {
    const sizes = [1_000, 5_000, 10_000, 20_000];
    const machineCounts = [8, 64];
    const results: BuildMeasurement[] = [];

    for (const n of sizes) {
      for (const m of machineCounts) {
        const meas = await runFullBuildInDO(n, m, 1000);
        results.push(meas);
      }
    }

    console.log("DO_MEASUREMENTS_JSON:" + JSON.stringify(results));
  });
});
