import { env, exports } from "cloudflare:workers";
import { evictDurableObject, listDurableObjectIds, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { BuildDO, INDEX_RETRY_INTERVAL_MS, RETENTION_MS } from "../src/build-do.ts";
import workerEntry from "../src/index.ts";

const WRITE_TOKEN = "test-write-token-0123456789abcdef";
const READ_TOKEN = "test-read-token-fedcba9876543210";

function authHeaders(token: string, json = false): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    ...(json ? { "Content-Type": "application/json; charset=utf-8" } : {}),
  };
}

function makeInitBody(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    version: 1,
    runner_id: "runner-a",
    machine_index: 0,
    config: {
      machines: 2,
      lease_ttl_ms: 10_000,
      build_timeout_ms: 60_000,
      max_attempts: 3,
      max_batch: 2,
    },
    tests: ["tests/a.py", "tests/b.py", "tests/c.py", "tests/d.py"],
    predictions_ms: [100, 200, 300, 400],
    ...overrides,
  });
}

async function req(
  method: string,
  path: string,
  token?: string,
  body?: string | ReadableStream<Uint8Array>,
): Promise<{ status: number; json: Record<string, unknown>; headers: Headers }> {
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;
  if (body !== undefined) headers["Content-Type"] = "application/json; charset=utf-8";
  const resp = await exports.default.fetch(`http://localhost${path}`, {
    method,
    headers,
    ...(body !== undefined ? { body } : {}),
  });
  const json = (await resp.json()) as Record<string, unknown>;
  return { status: resp.status, json, headers: resp.headers };
}

describe("Cloudflare Worker auth, config, streaming bounds, and Durable Object lifecycle", () => {
  it("refuses every request with 500 when MAX_TESTS is invalid, and 503 when tokens are misconfigured", async () => {
    for (const badMax of ["0", "-5", "20001", "abc", "", "5000.5", "0500"]) {
      const badEnv = { ...env, MAX_TESTS: badMax } as unknown as Env;
      const rHealth = await workerEntry.fetch(new Request("http://localhost/healthz"), badEnv);
      expect(rHealth.status, `MAX_TESTS=${badMax} on /healthz`).toBe(500);
      const body = (await rHealth.json()) as { error: { code: string } };
      expect(body.error.code).toBe("server_misconfigured");

      const rApi = await workerEntry.fetch(
        new Request("http://localhost/api/v1/builds", { headers: authHeaders(READ_TOKEN) }),
        badEnv,
      );
      expect(rApi.status, `MAX_TESTS=${badMax} on /api/v1/builds`).toBe(500);
    }

    // Missing, short (<16 bytes), and equal tokens fail closed with 503 on /api/v1/...
    const badTokenEnvs: Partial<Env>[] = [
      { BUNPO_QUEUE_TOKEN: "", BUNPO_QUEUE_READ_TOKEN: READ_TOKEN },
      { BUNPO_QUEUE_TOKEN: WRITE_TOKEN, BUNPO_QUEUE_READ_TOKEN: "short-token" },
      { BUNPO_QUEUE_TOKEN: WRITE_TOKEN, BUNPO_QUEUE_READ_TOKEN: WRITE_TOKEN },
    ];
    for (const override of badTokenEnvs) {
      const badEnv = { ...env, ...override } as unknown as Env;
      const rHealth = await workerEntry.fetch(new Request("http://localhost/healthz"), badEnv);
      expect(rHealth.status).toBe(200);
      const rApi = await workerEntry.fetch(
        new Request("http://localhost/api/v1/builds", { headers: authHeaders(WRITE_TOKEN) }),
        badEnv,
      );
      expect(rApi.status).toBe(503);
      const body = (await rApi.json()) as { error: { code: string } };
      expect(body.error.code).toBe("server_misconfigured");
    }
  });

  it("checks auth in Worker before waking any DO and enforces MAX_TESTS", async () => {
    const beforeIds = await listDurableObjectIds(env.BUILD_DO);
    const badAuth = await req("GET", "/api/v1/builds/uncreated-auth-check", "wrong-token-0123456789");
    expect(badAuth.status).toBe(401);
    const afterIds = await listDurableObjectIds(env.BUILD_DO);
    expect(afterIds.length).toBe(beforeIds.length);

    // Over MAX_TESTS (5000 default) returns 400 invalid_request.
    const tooMany = Array.from({ length: 5001 }, (_, i) => `t/${String(i).padStart(5, "0")}.py`);
    const preds = new Array<null>(5001).fill(null);
    const r = await req(
      "POST",
      "/api/v1/builds/over-max-tests/init",
      WRITE_TOKEN,
      makeInitBody({ tests: tooMany, predictions_ms: preds }),
    );
    expect(r.status).toBe(400);
    expect((r.json["error"] as { code: string }).code).toBe("invalid_request");
  });

  it("leaves SQLite storage empty when GET hits an unknown build ID", async () => {
    const bid = "unknown-empty-db-check";
    const r = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(r.status).toBe(404);
    expect((r.json["error"] as { code: string }).code).toBe("build_not_found");

    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));
    const inspection = await runInDurableObject(stub, async (_inst, state) => {
      const dbSize = state.storage.sql.databaseSize;
      const alarm = await state.storage.getAlarm();
      const tables = state.storage.sql
        .exec<{ name: string }>(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'",
        )
        .toArray();
      return { tableCount: tables.length, dbSize, alarm };
    });
    expect(inspection.tableCount).toBe(0);
    // An untouched SQLite DO in workerd occupies at most one empty 4 KiB header page.
    expect(inspection.dbSize).toBeLessThanOrEqual(4096);
    expect(inspection.alarm).toBeNull();
  });

  it("enforces body size while streaming without Content-Length", async () => {
    // Stream > 1 MiB to /lease without Content-Length header.
    const chunk = new Uint8Array(64 * 1024).fill(0x20);
    let sent = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (sent >= 17) {
          controller.close();
          return;
        }
        sent++;
        controller.enqueue(chunk);
      },
    });
    const r = await req("POST", "/api/v1/builds/stream-bound/lease", WRITE_TOKEN, stream);
    expect(r.status).toBe(413);
    expect((r.json["error"] as { code: string }).code).toBe("payload_too_large");
  });

  it("survives eviction at mid-build and eviction between every request of a full build", async () => {
    const bid = "evict-every-step";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    const initR = await req("POST", `/api/v1/builds/${bid}/init`, WRITE_TOKEN, makeInitBody());
    expect(initR.status).toBe(200);
    expect(initR.json["created"]).toBe(true);
    await evictDurableObject(stub);

    const accepted = new Set<number>();
    for (let step = 1; step <= 10; step++) {
      const leaseR = await req(
        "POST",
        `/api/v1/builds/${bid}/lease`,
        WRITE_TOKEN,
        JSON.stringify({ version: 1, runner_id: "runner-a", machine_index: 0, request_id: `q-${step}` }),
      );
      expect(leaseR.status).toBe(200);
      await evictDurableObject(stub);

      if (leaseR.json["status"] === "done") {
        expect(leaseR.json["verdict"]).toBe("passed");
        break;
      }
      expect(leaseR.json["status"]).toBe("leased");
      const leaseID = leaseR.json["lease_id"] as number;
      const attempts = leaseR.json["attempts"] as { attempt_id: string; test_index: number; generation: number }[];

      const hbR = await req(
        "POST",
        `/api/v1/builds/${bid}/heartbeat`,
        WRITE_TOKEN,
        JSON.stringify({ version: 1, runner_id: "runner-a", lease_id: leaseID }),
      );
      expect(hbR.status).toBe(200);
      expect(hbR.json["held"]).toBe(true);
      await evictDurableObject(stub);

      const repR = await req(
        "POST",
        `/api/v1/builds/${bid}/report`,
        WRITE_TOKEN,
        JSON.stringify({
          version: 1,
          runner_id: "runner-a",
          lease_id: leaseID,
          results: attempts.map((a) => ({
            attempt_id: a.attempt_id,
            test_index: a.test_index,
            generation: a.generation,
            outcome: "passed",
            timed: true,
            duration_ms: 120,
          })),
        }),
      );
      expect(repR.status).toBe(200);
      for (const a of attempts) accepted.add(a.test_index);
      await evictDurableObject(stub);

      if (repR.json["verdict"] === "passed") {
        break;
      }
    }

    expect(accepted.size).toBe(4);
    const st = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(st.status).toBe(200);
    expect(st.json["verdict"]).toBe("passed");
  });

  it("expires a dead runner lease within ttl/2 even when DO hibernates before every poll", async () => {
    const bid = "dead-runner-hibernation";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));
    const ttlMS = 15_000;
    const graceMS = Math.floor(ttlMS / 2); // 7,500 ms
    let now = Date.now();

    const initRaw = makeInitBody({
      config: {
        machines: 2,
        lease_ttl_ms: ttlMS,
        build_timeout_ms: 300_000,
        max_attempts: 3,
        max_batch: 1,
      },
      tests: ["tests/a.py", "tests/b.py"],
      predictions_ms: [200, 100],
    });
    const initR = await stub.init(bid, initRaw, now);
    expect(initR.status).toBe(200);

    // Runner A leases tests/a.py and heartbeats twice (forcing a lease_deadlines meta write), then dies.
    const lA = await stub.lease(
      JSON.stringify({ version: 1, runner_id: "runner-a", machine_index: 0, request_id: "a-1" }),
      now,
    );
    expect(lA.status).toBe(200);
    const leaseA = JSON.parse(lA.body) as { lease_id: number; attempts: { test: string }[] };
    expect(leaseA.attempts[0]!.test).toBe("tests/a.py");

    // Heartbeat past grace triggers one meta write; then runner A stops forever.
    now += 10_000;
    const hbA = await stub.heartbeat(
      JSON.stringify({ version: 1, runner_id: "runner-a", lease_id: leaseA.lease_id }),
      now,
    );
    expect(hbA.status).toBe(200);
    expect((JSON.parse(hbA.body) as { held: boolean }).held).toBe(true);
    const lastRealDeadlineA = now + ttlMS;

    // Runner B finishes tests/b.py.
    const lB = await stub.lease(
      JSON.stringify({ version: 1, runner_id: "runner-b", machine_index: 1, request_id: "b-1" }),
      now,
    );
    const leaseB = JSON.parse(lB.body) as {
      lease_id: number;
      attempts: { attempt_id: string; test_index: number; generation: number }[];
    };
    await stub.report(
      JSON.stringify({
        version: 1,
        runner_id: "runner-b",
        lease_id: leaseB.lease_id,
        results: [
          {
            attempt_id: leaseB.attempts[0]!.attempt_id,
            test_index: leaseB.attempts[0]!.test_index,
            generation: leaseB.attempts[0]!.generation,
            outcome: "passed",
            timed: true,
            duration_ms: 80,
          },
        ],
      }),
      now + 100,
    );

    // Now Runner B polls every 12 s with a full DO eviction before EVERY poll.
    // Under the old reset-on-wake rule this stayed "wait" forever; under LeaseDeadlineTracker
    // runner-a's lease MUST expire by lastRealDeadlineA + graceMS.
    let reapedAt: number | null = null;
    for (let poll = 1; poll <= 5; poll++) {
      now += 12_000;
      await evictDurableObject(stub);
      const r = await stub.lease(
        JSON.stringify({ version: 1, runner_id: "runner-b", machine_index: 1, request_id: `b-poll-${poll}` }),
        now,
      );
      expect(r.status).toBe(200);
      const parsed = JSON.parse(r.body) as {
        status: string;
        attempts?: { test: string; generation: number }[];
      };
      if (parsed.status === "leased") {
        expect(parsed.attempts![0]!.test).toBe("tests/a.py");
        expect(parsed.attempts![0]!.generation).toBe(2);
        reapedAt = now;
        break;
      }
      expect(parsed.status).toBe("wait");
      // Never still waiting once strictly past lastRealDeadlineA + graceMS.
      expect(now).toBeLessThan(lastRealDeadlineA + graceMS);
    }
    expect(reapedAt).not.toBeNull();
    expect(reapedAt!).toBeGreaterThanOrEqual(lastRealDeadlineA);
    expect(reapedAt!).toBeLessThanOrEqual(lastRealDeadlineA + graceMS + 12_000);
  });

  it("handles deadline alarm, +7d cleanup alarm, duplicate alarm delivery, and re-init after deleteAll", async () => {
    const bid = "alarm-lifecycle";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    const initR = await req("POST", `/api/v1/builds/${bid}/init`, WRITE_TOKEN, makeInitBody());
    expect(initR.status).toBe(200);
    const deadlineMS = initR.json["deadline_ms"] as number;

    // Lease one batch and abandon it.
    const leaseR = await req(
      "POST",
      `/api/v1/builds/${bid}/lease`,
      WRITE_TOKEN,
      JSON.stringify({ version: 1, runner_id: "runner-a", machine_index: 0, request_id: "q-1" }),
    );
    expect(leaseR.status).toBe(200);

    // Trigger deadline alarm twice (idempotent duplicate delivery).
    await stub.runAlarmAt(deadlineMS);
    await stub.runAlarmAt(deadlineMS + 100);

    const st = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(st.status).toBe(200);
    expect(st.json["verdict"]).toBe("unresolved");
    const finishedAt = st.json["finished_at_ms"] as number;
    expect(finishedAt).toBe(deadlineMS);

    // Verify alarm was rescheduled to finishedAt + 7 days and BuildIndex updated to unresolved.
    const scheduledAlarm = await runInDurableObject(stub, async (_inst, state) => state.storage.getAlarm());
    expect(scheduledAlarm).toBe(finishedAt + RETENTION_MS);

    const idxStub = env.BUILD_INDEX.get(env.BUILD_INDEX.idFromName("global"));
    const listedBefore = await idxStub.list(finishedAt + 1000);
    const entryBefore = listedBefore.find((b) => b.build_id === bid);
    expect(entryBefore?.verdict).toBe("unresolved");

    // Trigger +7d retention alarm twice (idempotent duplicate delivery -> deleteAll + reset memory).
    await stub.runAlarmAt(finishedAt + RETENTION_MS);
    await stub.runAlarmAt(finishedAt + RETENTION_MS + 1000);

    // Without evicting the DO, GET must return 404 build_not_found.
    const gone = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(gone.status).toBe(404);
    expect((gone.json["error"] as { code: string }).code).toBe("build_not_found");

    // BuildIndex lazily prunes expired build past finishedAt + 7 days.
    const listedAfter = await idxStub.list(finishedAt + RETENTION_MS + 1);
    expect(listedAfter.find((b) => b.build_id === bid)).toBeUndefined();

    // Immediately re-init the same build ID after deleteAll (no eviction in between!).
    const reInit = await req("POST", `/api/v1/builds/${bid}/init`, WRITE_TOKEN, makeInitBody());
    expect(reInit.status).toBe(200);
    expect(reInit.json["created"]).toBe(true);
    expect(reInit.json["verdict"]).toBe("running");

    // Also confirm workerd's native runDurableObjectAlarm executes alarm() without throwing.
    const ran = await runDurableObjectAlarm(stub);
    expect(ran).toBe(true);
  });

  it("restores a missing alarm on wake for both running and terminal builds", async () => {
    const bid = "alarm-wake-restore";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    const initR = await req(
      "POST",
      `/api/v1/builds/${bid}/init`,
      WRITE_TOKEN,
      makeInitBody({ tests: ["only.py"], predictions_ms: [100] }),
    );
    expect(initR.status).toBe(200);
    const deadlineMS = initR.json["deadline_ms"] as number;

    // Delete alarm while running, evict, wake -> alarm restored to deadlineMS.
    await runInDurableObject(stub, async (_inst, state) => {
      await state.storage.deleteAlarm();
      expect(await state.storage.getAlarm()).toBeNull();
    });
    await evictDurableObject(stub);

    const stRunning = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(stRunning.status).toBe(200);
    const restoredRunning = await runInDurableObject(stub, async (_inst, state) => state.storage.getAlarm());
    expect(restoredRunning).toBe(deadlineMS);

    // Finish the build: verify terminal +7d cleanup alarm is scheduled immediately,
    // then delete it, evict, wake -> alarm restored to finishedAt + RETENTION_MS.
    const l = await req(
      "POST",
      `/api/v1/builds/${bid}/lease`,
      WRITE_TOKEN,
      JSON.stringify({ version: 1, runner_id: "r1", machine_index: 0, request_id: "q1" }),
    );
    const leaseID = l.json["lease_id"] as number;
    const rep = await req(
      "POST",
      `/api/v1/builds/${bid}/report`,
      WRITE_TOKEN,
      JSON.stringify({
        version: 1,
        runner_id: "r1",
        lease_id: leaseID,
        results: [
          { attempt_id: "0.1", test_index: 0, generation: 1, outcome: "passed", timed: true, duration_ms: 50 },
        ],
      }),
    );
    expect(rep.status).toBe(200);
    const stDone = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    const finishedAt = stDone.json["finished_at_ms"] as number;

    const scheduledTerminal = await runInDurableObject(stub, async (_inst, state) => state.storage.getAlarm());
    expect(scheduledTerminal).toBe(finishedAt + RETENTION_MS);

    await runInDurableObject(stub, async (_inst, state) => {
      await state.storage.deleteAlarm();
      expect(await state.storage.getAlarm()).toBeNull();
    });
    await evictDurableObject(stub);

    const stAfter = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(stAfter.status).toBe(200);
    expect(stAfter.json["verdict"]).toBe("passed");
    const restoredTerminal = await runInDurableObject(stub, async (_inst, state) => state.storage.getAlarm());
    expect(restoredTerminal).toBe(finishedAt + RETENTION_MS);
  });

  it("replays existing builds even if MAX_TESTS is lowered below their test count", async () => {
    const bid = "lowered-max-tests-replay";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    const body5 = makeInitBody({
      tests: ["t/1.py", "t/2.py", "t/3.py", "t/4.py", "t/5.py"],
      predictions_ms: [10, 20, 30, 40, 50],
    });
    const initR = await req("POST", `/api/v1/builds/${bid}/init`, WRITE_TOKEN, body5);
    expect(initR.status).toBe(200);

    // Construct a fresh BuildDO against the same SQLite storage with MAX_TESTS lowered to "2".
    const replayRes = await runInDurableObject(stub, async (_inst, state) => {
      const loweredEnv = { ...env, MAX_TESTS: "2" } as unknown as Env;
      const loweredDO = new BuildDO(state, loweredEnv);
      await state.blockConcurrencyWhile(async () => {});
      const st = await loweredDO.status();
      const dupInit = await loweredDO.init(bid, body5);
      return { st, dupInit };
    });
    expect(replayRes.st.status).toBe(200);
    expect((JSON.parse(replayRes.st.body) as { counts: { total: number } }).counts.total).toBe(5);
    expect(replayRes.dupInit.status).toBe(200);
    expect((JSON.parse(replayRes.dupInit.body) as { created: boolean }).created).toBe(false);

    // A NEW uninitialized build under MAX_TESTS="2" still rejects >2 tests with 400 invalid_request.
    const freshStub = env.BUILD_DO.get(env.BUILD_DO.idFromName("lowered-max-tests-new"));
    const newRes = await runInDurableObject(freshStub, async (_inst, state) => {
      const loweredEnv = { ...env, MAX_TESTS: "2" } as unknown as Env;
      const loweredDO = new BuildDO(state, loweredEnv);
      await state.blockConcurrencyWhile(async () => {});
      return loweredDO.init("lowered-max-tests-new", body5);
    });
    expect(newRes.status).toBe(400);
    expect((JSON.parse(newRes.body) as { error: { code: string } }).error.code).toBe("invalid_request");
  });

  it("aborts DO on storage failure without diverging state and logs structured JSON without secrets", async () => {
    const bid = "storage-fail-abort";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    const initR = await req("POST", `/api/v1/builds/${bid}/init`, WRITE_TOKEN, makeInitBody());
    expect(initR.status).toBe(200);

    // Inject a one-shot SQLite transaction failure and tag the live DO instance so we can
    // confirm ctx.abort() actually tore down the isolate instead of keeping it alive in memory.
    await runInDurableObject(stub, async (inst, state) => {
      (inst as unknown as { __preAbortTag?: boolean }).__preAbortTag = true;
      state.storage.sql.exec(
        "CREATE TRIGGER fail_once BEFORE INSERT ON event_log BEGIN SELECT RAISE(ABORT, 'injected disk failure'); END",
      );
    });

    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const failedLease = await req(
        "POST",
        `/api/v1/builds/${bid}/lease`,
        WRITE_TOKEN,
        JSON.stringify({ version: 1, runner_id: "runner-a", machine_index: 0, request_id: "q-fail" }),
      );
      expect(failedLease.status).toBe(503);
      expect((failedLease.json["error"] as { code: string }).code).toBe("storage_error");

      expect(errSpy).toHaveBeenCalled();
      const rawLog = String(errSpy.mock.calls[0]![0]);
      expect(rawLog).not.toContain(WRITE_TOKEN);
      expect(rawLog).not.toContain("q-fail");
      const parsedLog = JSON.parse(rawLog) as Record<string, unknown>;
      expect(parsedLog["event"]).toBe("do_rpc_error");
      expect(parsedLog["build_id"]).toBe(bid);
      expect(parsedLog["method"]).toBe("lease");
      expect(typeof parsedLog["error_name"]).toBe("string");
      expect(typeof parsedLog["error_message"]).toBe("string");
    } finally {
      errSpy.mockRestore();
    }

    // Acquire a fresh stub because ctx.abort() permanently invalidates the aborted stub.
    const freshStub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));
    const tagAfterAbort = await runInDurableObject(freshStub, async (inst, state) => {
      state.storage.sql.exec("DROP TRIGGER IF EXISTS fail_once");
      return (inst as unknown as { __preAbortTag?: boolean }).__preAbortTag;
    });
    expect(tagAfterAbort).toBeUndefined();
    const st = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(st.status).toBe(200);
    const counts = st.json["counts"] as { pending: number; leased: number };
    expect(counts.pending).toBe(4);
    expect(counts.leased).toBe(0);
  });

  it("isolates two builds cleanly", async () => {
    const rA = await req(
      "POST",
      "/api/v1/builds/iso-a/init",
      WRITE_TOKEN,
      makeInitBody({ tests: ["a/1.py"], predictions_ms: [100] }),
    );
    const rB = await req(
      "POST",
      "/api/v1/builds/iso-b/init",
      WRITE_TOKEN,
      makeInitBody({ tests: ["b/1.py", "b/2.py"], predictions_ms: [100, 200] }),
    );
    expect(rA.status).toBe(200);
    expect(rB.status).toBe(200);

    const lA = await req(
      "POST",
      "/api/v1/builds/iso-a/lease",
      WRITE_TOKEN,
      JSON.stringify({ version: 1, runner_id: "r1", machine_index: 0, request_id: "qa" }),
    );
    expect(lA.status).toBe(200);
    const attA = (lA.json["attempts"] as { test: string }[])[0]!;
    expect(attA.test).toBe("a/1.py");

    const stB = await req("GET", "/api/v1/builds/iso-b", READ_TOKEN);
    expect((stB.json["counts"] as { total: number; pending: number }).total).toBe(2);
    expect((stB.json["counts"] as { total: number; pending: number }).pending).toBe(2);
  });

  it("throttles failed BuildIndex retries to once per minute and repairs on wake without blocking replies", async () => {
    const bid = "index-repair-test";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    let now = Date.now();
    const initR = await stub.init(
      bid,
      makeInitBody({ tests: ["only.py"], predictions_ms: [100], config: { machines: 1, lease_ttl_ms: 30_000, build_timeout_ms: 300_000, max_attempts: 3, max_batch: 1 } }),
      now,
    );
    expect(initR.status).toBe(200);

    const leaseR = await stub.lease(
      JSON.stringify({ version: 1, runner_id: "r1", machine_index: 0, request_id: "q1" }),
      now,
    );
    expect(leaseR.status).toBe(200);
    const leaseID = (JSON.parse(leaseR.body) as { lease_id: number }).lease_id;

    // Force BuildIndex RPC to fail and count how many times it is called.
    await runInDurableObject(stub, async (inst: BuildDO) => {
      const target = inst as unknown as { env: { BUILD_INDEX: unknown }; __failCount?: number };
      target.__failCount = 0;
      target.env = {
        ...target.env,
        BUILD_INDEX: {
          idFromName: () => "broken",
          get: () => ({
            upsert: async () => {
              target.__failCount = (target.__failCount ?? 0) + 1;
              throw new Error("simulated BuildIndex outage");
            },
          }),
        },
      };
    });

    // Trigger an init conflict so BuildIndex sync is attempted and fails while build is still running.
    now += 1_000;
    const confR = await stub.init(
      bid,
      makeInitBody({
        runner_id: "r2",
        tests: ["extra.py", "only.py"],
        predictions_ms: [100, 100],
      }),
      now,
    );
    expect(confR.status).toBe(409);

    const callsAfterConflict = await runInDurableObject(
      stub,
      async (inst: BuildDO) => (inst as unknown as { __failCount: number }).__failCount,
    );
    expect(callsAfterConflict).toBe(1);

    // Send 4 heartbeats within 60 s (at +10s, +20s, +30s, +40s):
    // BuildIndex must NOT be retried on every heartbeat.
    for (let i = 1; i <= 4; i++) {
      now += 10_000;
      const hb = await stub.heartbeat(
        JSON.stringify({ version: 1, runner_id: "r1", lease_id: leaseID }),
        now,
      );
      expect(hb.status).toBe(200);
      expect((JSON.parse(hb.body) as { held: boolean }).held).toBe(true);
    }
    const callsWithinMinute = await runInDurableObject(
      stub,
      async (inst: BuildDO) => (inst as unknown as { __failCount: number }).__failCount,
    );
    expect(callsWithinMinute).toBe(1);

    // Advance another 20 s (total 60 s since failed attempt, still within 30 s lease TTL):
    // exactly one background retry fires via waitUntil.
    now += INDEX_RETRY_INTERVAL_MS - 40_000;
    const hbAfterMin = await stub.heartbeat(
      JSON.stringify({ version: 1, runner_id: "r1", lease_id: leaseID }),
      now,
    );
    expect(hbAfterMin.status).toBe(200);
    expect((JSON.parse(hbAfterMin.body) as { held: boolean }).held).toBe(true);
    await expect
      .poll(async () =>
        runInDurableObject(stub, async (inst: BuildDO) => (inst as unknown as { __failCount: number }).__failCount),
      )
      .toBe(2);

    // Now finish the build while BuildIndex is still broken; client's report still succeeds.
    now += 1_000;
    const repR = await stub.report(
      JSON.stringify({
        version: 1,
        runner_id: "r1",
        lease_id: leaseID,
        results: [
          {
            attempt_id: "0.1",
            test_index: 0,
            generation: 1,
            outcome: "passed",
            timed: true,
            duration_ms: 90,
          },
        ],
      }),
      now,
    );
    expect(repR.status).toBe(200);
    // An init conflict was recorded earlier, so the final verdict is "unresolved" even though all tests passed.
    expect((JSON.parse(repR.body) as { verdict: string }).verdict).toBe("unresolved");

    // BuildIndex still says "running" because the update failed.
    const listBefore = await req("GET", "/api/v1/builds", READ_TOKEN);
    const bBefore = (listBefore.json["builds"] as { build_id: string; verdict: string }[]).find(
      (b) => b.build_id === bid,
    );
    expect(bBefore?.verdict).toBe("running");

    // Evict DO so it wakes with real env.BUILD_INDEX, compares index_synced meta row against current state,
    // and repairs BuildIndex in the background without blocking GET.
    await evictDurableObject(stub);
    const st = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(st.status).toBe(200);
    expect(st.json["verdict"]).toBe("unresolved");

    // BuildIndex is repaired to "unresolved" with has_conflicts=true.
    await expect
      .poll(async () => {
        const listAfter = await req("GET", "/api/v1/builds", READ_TOKEN);
        const bAfter = (
          listAfter.json["builds"] as { build_id: string; verdict: string; has_conflicts: boolean }[]
        ).find((b) => b.build_id === bid);
        return bAfter ? `${bAfter.verdict}:${bAfter.has_conflicts}` : "missing";
      })
      .toBe("unresolved:true");

    // Once repaired and persisted to meta.index_synced, neither the live DO (>60 s later)
    // nor a freshly woken DO should call BuildIndex.upsert again.
    await runInDurableObject(stub, async (inst: BuildDO) => {
      const target = inst as unknown as { env: { BUILD_INDEX: unknown }; __spuriousCalls?: number };
      target.__spuriousCalls = 0;
      target.env = {
        ...target.env,
        BUILD_INDEX: {
          idFromName: () => "spy",
          get: () => ({
            upsert: async () => {
              target.__spuriousCalls = (target.__spuriousCalls ?? 0) + 1;
            },
          }),
        },
      };
    });
    const stLive = await stub.status(now + INDEX_RETRY_INTERVAL_MS + 5_000);
    expect(stLive.status).toBe(200);
    const liveSpurious = await runInDurableObject(
      stub,
      async (inst: BuildDO) => (inst as unknown as { __spuriousCalls: number }).__spuriousCalls,
    );
    expect(liveSpurious).toBe(0);

    await evictDurableObject(stub);
    await runInDurableObject(stub, async (inst: BuildDO) => {
      const target = inst as unknown as { env: { BUILD_INDEX: unknown }; __spuriousCalls?: number };
      target.__spuriousCalls = 0;
      target.env = {
        ...target.env,
        BUILD_INDEX: {
          idFromName: () => "spy",
          get: () => ({
            upsert: async () => {
              target.__spuriousCalls = (target.__spuriousCalls ?? 0) + 1;
            },
          }),
        },
      };
    });
    const stSynced = await stub.status(now + INDEX_RETRY_INTERVAL_MS + 10_000);
    expect(stSynced.status).toBe(200);
    const spuriousCalls = await runInDurableObject(
      stub,
      async (inst: BuildDO) => (inst as unknown as { __spuriousCalls: number }).__spuriousCalls,
    );
    expect(spuriousCalls).toBe(0);
  });

  it("repairs verdict-only BuildIndex mismatch on wake and prunes closed leases from lease_deadlines", async () => {
    const bid = "verdict-repair-and-prune";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));
    let now = Date.now();
    const ttlMS = 10_000;

    await stub.init(
      bid,
      makeInitBody({
        config: { machines: 3, lease_ttl_ms: ttlMS, build_timeout_ms: 300_000, max_attempts: 3, max_batch: 1 },
        tests: ["t/1.py", "t/2.py", "t/3.py"],
        predictions_ms: [100, 100, 100],
      }),
      now,
    );

    // Three leases: l1 reports (closed by report), l2 expires (closed by expiry), l3 heartbeats past grace.
    const l1 = JSON.parse(
      (await stub.lease(JSON.stringify({ version: 1, runner_id: "r1", machine_index: 0, request_id: "q1" }), now)).body,
    ) as { lease_id: number; attempts: { attempt_id: string; test_index: number; generation: number }[] };
    const l2 = JSON.parse(
      (await stub.lease(JSON.stringify({ version: 1, runner_id: "r2", machine_index: 1, request_id: "q2" }), now)).body,
    ) as { lease_id: number };
    const l3 = JSON.parse(
      (await stub.lease(JSON.stringify({ version: 1, runner_id: "r3", machine_index: 2, request_id: "q3" }), now)).body,
    ) as { lease_id: number; attempts: { attempt_id: string; test_index: number; generation: number }[] };

    // Close l1 via report at t+1s.
    now += 1_000;
    await stub.report(
      JSON.stringify({
        version: 1,
        runner_id: "r1",
        lease_id: l1.lease_id,
        results: [
          {
            attempt_id: l1.attempts[0]!.attempt_id,
            test_index: l1.attempts[0]!.test_index,
            generation: l1.attempts[0]!.generation,
            outcome: "passed",
            timed: true,
            duration_ms: 50,
          },
        ],
      }),
      now,
    );

    // Heartbeat l3 at t+6s (> ttl/2 -> writes lease_deadlines); l1 must be pruned, l2+l3 still active.
    now += 5_000;
    await stub.heartbeat(JSON.stringify({ version: 1, runner_id: "r3", lease_id: l3.lease_id }), now);
    const metaAfterReport = await runInDurableObject(stub, async (_inst, state) => {
      const row = state.storage.sql
        .exec<{ v: string }>("SELECT v FROM meta WHERE k = 'lease_deadlines'")
        .toArray()[0]!;
      return JSON.parse(row.v) as Record<string, number>;
    });
    expect(metaAfterReport[String(l1.lease_id)]).toBeUndefined();
    expect(metaAfterReport[String(l3.lease_id)]).toBeDefined();

    // Advance past l2's expiry (t+12s); l1's runner requests a lease which expires l2, then l3 heartbeats past grace.
    now += 6_000;
    const l4 = JSON.parse(
      (await stub.lease(JSON.stringify({ version: 1, runner_id: "r1", machine_index: 0, request_id: "q4" }), now)).body,
    ) as { lease_id: number; attempts: { attempt_id: string; test_index: number; generation: number }[] };
    await stub.heartbeat(JSON.stringify({ version: 1, runner_id: "r3", lease_id: l3.lease_id }), now);
    const metaAfterExpire = await runInDurableObject(stub, async (_inst, state) => {
      const row = state.storage.sql
        .exec<{ v: string }>("SELECT v FROM meta WHERE k = 'lease_deadlines'")
        .toArray()[0]!;
      return JSON.parse(row.v) as Record<string, number>;
    });
    expect(metaAfterExpire[String(l2.lease_id)]).toBeUndefined();
    expect(metaAfterExpire[String(l3.lease_id)]).toBeDefined();

    // Now break BuildIndex and finish remaining tests (verdict running -> passed, has_conflicts stays false).
    await runInDurableObject(stub, async (inst: BuildDO) => {
      const target = inst as unknown as { env: { BUILD_INDEX: unknown } };
      target.env = {
        ...target.env,
        BUILD_INDEX: {
          idFromName: () => "broken",
          get: () => ({
            upsert: async () => {
              throw new Error("outage");
            },
          }),
        },
      };
    });
    now += 500;
    await stub.report(
      JSON.stringify({
        version: 1,
        runner_id: "r3",
        lease_id: l3.lease_id,
        results: [
          {
            attempt_id: l3.attempts[0]!.attempt_id,
            test_index: l3.attempts[0]!.test_index,
            generation: l3.attempts[0]!.generation,
            outcome: "passed",
            timed: true,
            duration_ms: 50,
          },
        ],
      }),
      now,
    );
    await stub.report(
      JSON.stringify({
        version: 1,
        runner_id: "r1",
        lease_id: l4.lease_id,
        results: [
          {
            attempt_id: l4.attempts[0]!.attempt_id,
            test_index: l4.attempts[0]!.test_index,
            generation: l4.attempts[0]!.generation,
            outcome: "passed",
            timed: true,
            duration_ms: 50,
          },
        ],
      }),
      now + 100,
    );

    // Evict and wake -> verdict-only mismatch (running vs passed, has_conflicts=false) MUST repair BuildIndex!
    await evictDurableObject(stub);
    const st = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(st.status).toBe(200);
    expect(st.json["verdict"]).toBe("passed");
    await expect
      .poll(async () => {
        const list = await req("GET", "/api/v1/builds", READ_TOKEN);
        return (list.json["builds"] as { build_id: string; verdict: string }[]).find((b) => b.build_id === bid)?.verdict;
      })
      .toBe("passed");
  });
});
