import { env, exports } from "cloudflare:workers";
import { evictDurableObject, listDurableObjectIds, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { type BuildDO, RETENTION_MS } from "../src/build-do.ts";
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

  it("aborts DO on storage failure without diverging in-memory state", async () => {
    const bid = "storage-fail-abort";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    const initR = await req("POST", `/api/v1/builds/${bid}/init`, WRITE_TOKEN, makeInitBody());
    expect(initR.status).toBe(200);

    // Inject a one-shot SQLite transaction failure by creating a failing trigger on event_log.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "CREATE TRIGGER fail_once BEFORE INSERT ON event_log BEGIN SELECT RAISE(ABORT, 'injected disk failure'); END",
      );
    });

    // Note: ctx.abort() tears down the DO cleanly; calling the Worker returns 503 storage_error.
    const failedLease = await req(
      "POST",
      `/api/v1/builds/${bid}/lease`,
      WRITE_TOKEN,
      JSON.stringify({ version: 1, runner_id: "runner-a", machine_index: 0, request_id: "q-fail" }),
    );
    expect(failedLease.status).toBe(503);
    expect((failedLease.json["error"] as { code: string }).code).toBe("storage_error");

    // Acquire a fresh stub because ctx.abort() permanently invalidates the aborted stub.
    const freshStub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));
    await runInDurableObject(freshStub, async (_inst, state) => {
      state.storage.sql.exec("DROP TRIGGER IF EXISTS fail_once");
    });
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

  it("repairs stale BuildIndex entry on next wake/request if final index update failed", async () => {
    const bid = "index-repair-test";
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(bid));

    const initR = await req(
      "POST",
      `/api/v1/builds/${bid}/init`,
      WRITE_TOKEN,
      makeInitBody({ tests: ["only.py"], predictions_ms: [100] }),
    );
    expect(initR.status).toBe(200);

    const leaseR = await req(
      "POST",
      `/api/v1/builds/${bid}/lease`,
      WRITE_TOKEN,
      JSON.stringify({ version: 1, runner_id: "r1", machine_index: 0, request_id: "q1" }),
    );
    expect(leaseR.status).toBe(200);
    const leaseID = leaseR.json["lease_id"] as number;

    // Force the BuildIndex RPC to fail during the final report by sabotaging env.BUILD_INDEX inside the DO.
    await runInDurableObject(stub, async (inst: BuildDO) => {
      const target = inst as unknown as { env: { BUILD_INDEX: unknown } };
      const realIdx = target.env.BUILD_INDEX;
      target.env = {
        ...target.env,
        BUILD_INDEX: {
          idFromName: () => "broken",
          get: () => ({
            upsert: async () => {
              throw new Error("simulated BuildIndex outage");
            },
          }),
        },
      };
      // Keep reference just to avoid unused var lint.
      void realIdx;
    });

    const repR = await req(
      "POST",
      `/api/v1/builds/${bid}/report`,
      WRITE_TOKEN,
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
    );
    // Client's committed report still succeeds!
    expect(repR.status).toBe(200);
    expect(repR.json["verdict"]).toBe("passed");

    // BuildIndex still says "running" because the update failed.
    const listBefore = await req("GET", "/api/v1/builds", READ_TOKEN);
    const bBefore = (listBefore.json["builds"] as { build_id: string; verdict: string }[]).find(
      (b) => b.build_id === bid,
    );
    expect(bBefore?.verdict).toBe("running");

    // Evict DO so it wakes with real env.BUILD_INDEX and reads index_dirty='1' from SQLite.
    await evictDurableObject(stub);
    const st = await req("GET", `/api/v1/builds/${bid}`, READ_TOKEN);
    expect(st.status).toBe(200);
    expect(st.json["verdict"]).toBe("passed");

    // BuildIndex is now repaired to "passed"!
    const listAfter = await req("GET", "/api/v1/builds", READ_TOKEN);
    const bAfter = (listAfter.json["builds"] as { build_id: string; verdict: string }[]).find(
      (b) => b.build_id === bid,
    );
    expect(bAfter?.verdict).toBe("passed");
  });
});
