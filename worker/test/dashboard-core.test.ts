import { describe, expect, it } from "vitest";
import {
  BASE_POLL_MS,
  MAX_POLL_FAILURES,
  MAX_POLL_MS,
  buildPath,
  buildTimeline,
  createGuardedLoader,
  createPollState,
  fetchBuildInitial,
  fetchRefreshedPages,
  formatAttemptDuration,
  formatDuration,
  formatTimeLeft,
  parseRoute,
  pollStep,
  sortBuilds,
} from "../../internal/dashboard/static/dashboard/core.js";

function makeRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("dashboard pure core", () => {
  it("parses routes safely including build IDs with dots and rejects traversal", () => {
    expect(parseRoute("/")).toEqual({ view: "builds" });
    expect(parseRoute("/index.html")).toEqual({ view: "builds" });
    expect(parseRoute("/builds/release-1.2.3")).toEqual({ view: "build", buildID: "release-1.2.3" });
    expect(parseRoute("/builds/app.js")).toEqual({ view: "build", buildID: "app.js" });
    expect(buildPath("release-1.2.3")).toBe("/builds/release-1.2.3");

    expect(parseRoute("/builds/a..b")).toEqual({ view: "not_found" });
    expect(parseRoute("/builds/a/b")).toEqual({ view: "not_found" });
    expect(parseRoute("/builds/")).toEqual({ view: "not_found" });
    expect(parseRoute("/dashboard/app.js")).toEqual({ view: "not_found" });
    expect(parseRoute("/unknown")).toEqual({ view: "not_found" });
  });

  it("formats durations, attempt wall time, time left, and sorts builds newest first", () => {
    expect(formatDuration(0)).toBe("0 ms");
    expect(formatDuration(450)).toBe("450 ms");
    expect(formatDuration(1_500)).toBe("1.5 s");
    expect(formatDuration(135_000)).toBe("2m 15s");
    expect(formatDuration(3_840_000)).toBe("1h 04m");
    expect(formatDuration(-1)).toBe("—");

    // Timed passed keeps JUnit duration; untimed failed/error uses wall time or omits.
    expect(formatAttemptDuration({ timed: true, duration_ms: 420, leased_at_ms: 1_000, finished_at_ms: 2_000 })).toBe("420 ms");
    expect(formatAttemptDuration({ timed: false, duration_ms: 0, leased_at_ms: 1_000, finished_at_ms: 2_500 })).toBe("1.5 s");
    expect(formatAttemptDuration({ timed: false, duration_ms: 0, leased_at_ms: 1_000, finished_at_ms: 1_000 })).toBe("");
    expect(formatAttemptDuration({ timed: false, duration_ms: 0, leased_at_ms: 1_000, finished_at_ms: 0 })).toBe("");

    expect(formatTimeLeft(10_000, 4_000, "running")).toBe("6.0 s left");
    expect(formatTimeLeft(10_000, 12_000, "running")).toBe("Deadline reached");
    expect(formatTimeLeft(10_000, 4_000, "passed")).toBe("Finished");

    const sorted = sortBuilds([
      { build_id: "b", created_at_ms: 100 },
      { build_id: "a", created_at_ms: 100 },
      { build_id: "c", created_at_ms: 200 },
    ]);
    expect(sorted.map((b) => b.build_id)).toEqual(["c", "a", "b"]);
  });

  it("drops stale responses and stale errors after navigation or sign-out", async () => {
    const inflight = new Map<string, ReturnType<typeof deferred<unknown>>>();
    const loader = createGuardedLoader((path) => {
      const d = deferred<unknown>();
      inflight.set(path, d);
      return d.promise;
    });

    // Case 1: loadBuilds in flight when user signs out -> stale response must be ignored.
    const gen1 = loader.invalidate();
    const buildsPromise = loader.fetch("/api/v1/builds", gen1);
    loader.invalidate(); // sign-out
    inflight.get("/api/v1/builds")!.resolve({ builds: [{ build_id: "secret" }] });
    expect(await buildsPromise).toEqual({ stale: true });

    // Case 2: loadBuild status resolves after user already navigated away -> onStatus never fires.
    const gen2 = loader.invalidate();
    let statusRendered = 0;
    const buildA = fetchBuildInitial(loader, gen2, "build-a", "cursor=0&limit=200", () => {
      statusRendered++;
    });
    loader.invalidate(); // navigated to another build
    inflight.get("/api/v1/builds/build-a")!.resolve({ verdict: "running" });
    expect(await buildA).toEqual({ stale: true });
    expect(statusRendered).toBe(0);

    // Case 3: loadBuild status resolves while on build-b, then user signs out before /tests settles.
    const gen3 = loader.invalidate();
    const buildB = fetchBuildInitial(loader, gen3, "build-b", "cursor=0&limit=200&state=failed,unresolved", () => {
      statusRendered++;
    });
    inflight.get("/api/v1/builds/build-b")!.resolve({ verdict: "running" });
    await Promise.resolve();
    await Promise.resolve();
    expect(statusRendered).toBe(1);
    loader.invalidate(); // sign-out while /tests is in flight
    inflight.get("/api/v1/builds/build-b/tests?cursor=0&limit=200")!.resolve({ tests: [], next_cursor: null });
    inflight.get("/api/v1/builds/build-b/tests?cursor=0&limit=200&state=failed,unresolved")!.reject(new Error("aborted"));
    expect(await buildB).toEqual({ stale: true });
  });

  it("reloads the same page count along the next_cursor chain, captures newly matching tests, stops early on shrink, and drops stale generations", async () => {
    type Page = { tests: { test_index: number; state: string }[]; next_cursor: number | null };

    // (a) A newly matching test between old page boundaries now appears across the reloaded pages.
    {
      const seen: string[] = [];
      const pages: Record<number, Page> = {
        0: {
          tests: [
            { test_index: 0, state: "failed" },
            { test_index: 5, state: "failed" },
          ],
          next_cursor: 6,
        },
        6: {
          tests: [
            { test_index: 10, state: "failed" },
            { test_index: 15, state: "failed" },
          ],
          next_cursor: 16,
        },
      };
      const loader = createGuardedLoader<Page>(async (path) => {
        seen.push(path);
        const cursor = Number(new URL(path, "http://x").searchParams.get("cursor"));
        return pages[cursor]!;
      });
      const gen = loader.invalidate();
      const res = await fetchRefreshedPages(loader, gen, (c) => `/api/v1/builds/b/tests?cursor=${c}&state=failed`, 2);
      expect(seen).toEqual([
        "/api/v1/builds/b/tests?cursor=0&state=failed",
        "/api/v1/builds/b/tests?cursor=6&state=failed",
      ]);
      expect(res).toEqual({
        stale: false,
        ok: true,
        value: {
          tests: [
            { test_index: 0, state: "failed" },
            { test_index: 5, state: "failed" },
            { test_index: 10, state: "failed" },
            { test_index: 15, state: "failed" },
          ],
          nextCursor: 16,
          pagesLoaded: 2,
        },
      });
    }

    // (b) With 3 pages loaded, refresh fetches exactly 3 pages in order and keeps them without fetching page 4.
    {
      const seen: string[] = [];
      const pages: Record<number, Page> = {
        0: { tests: [{ test_index: 0, state: "passed" }], next_cursor: 200 },
        200: { tests: [{ test_index: 200, state: "passed" }], next_cursor: 400 },
        400: { tests: [{ test_index: 400, state: "leased" }], next_cursor: 600 },
        600: { tests: [{ test_index: 600, state: "pending" }], next_cursor: null },
      };
      const loader = createGuardedLoader<Page>(async (path) => {
        seen.push(path);
        const cursor = Number(new URL(path, "http://x").searchParams.get("cursor"));
        return pages[cursor]!;
      });
      const gen = loader.invalidate();
      const res = await fetchRefreshedPages(loader, gen, (c) => `/api/v1/builds/b/tests?cursor=${c}`, 3);
      expect(seen).toEqual([
        "/api/v1/builds/b/tests?cursor=0",
        "/api/v1/builds/b/tests?cursor=200",
        "/api/v1/builds/b/tests?cursor=400",
      ]);
      expect(res).toEqual({
        stale: false,
        ok: true,
        value: {
          tests: [
            { test_index: 0, state: "passed" },
            { test_index: 200, state: "passed" },
            { test_index: 400, state: "leased" },
          ],
          nextCursor: 600,
          pagesLoaded: 3,
        },
      });
    }

    // (c) A list that shrinks so next_cursor becomes null stops early.
    {
      const seen: string[] = [];
      const loader = createGuardedLoader<Page>(async (path) => {
        seen.push(path);
        const cursor = Number(new URL(path, "http://x").searchParams.get("cursor"));
        if (cursor === 0) return { tests: [{ test_index: 0, state: "failed" }], next_cursor: 10 };
        return { tests: [{ test_index: 10, state: "failed" }], next_cursor: null };
      });
      const gen = loader.invalidate();
      const res = await fetchRefreshedPages(loader, gen, (c) => `/api/v1/builds/b/tests?cursor=${c}`, 4);
      expect(seen).toEqual([
        "/api/v1/builds/b/tests?cursor=0",
        "/api/v1/builds/b/tests?cursor=10",
      ]);
      expect(res).toEqual({
        stale: false,
        ok: true,
        value: {
          tests: [
            { test_index: 0, state: "failed" },
            { test_index: 10, state: "failed" },
          ],
          nextCursor: null,
          pagesLoaded: 2,
        },
      });
    }

    // (d) A stale generation discards the result and stops further page fetches.
    {
      const seen: string[] = [];
      const inflight = new Map<string, ReturnType<typeof deferred<Page>>>();
      const loader = createGuardedLoader<Page>((path) => {
        seen.push(path);
        const d = deferred<Page>();
        inflight.set(path, d);
        return d.promise;
      });
      const gen = loader.invalidate();
      const pending = fetchRefreshedPages(loader, gen, (c) => `/api/v1/builds/b/tests?cursor=${c}`, 3);
      inflight.get("/api/v1/builds/b/tests?cursor=0")!.resolve({
        tests: [{ test_index: 0, state: "passed" }],
        next_cursor: 200,
      });
      await Promise.resolve();
      await Promise.resolve();
      expect(seen).toEqual(["/api/v1/builds/b/tests?cursor=0", "/api/v1/builds/b/tests?cursor=200"]);

      // User navigates away / signs out while page 2 is in flight.
      loader.invalidate();
      inflight.get("/api/v1/builds/b/tests?cursor=200")!.resolve({
        tests: [{ test_index: 200, state: "passed" }],
        next_cursor: 400,
      });
      expect(await pending).toEqual({ stale: true });
      // Must not have requested page 3.
      expect(seen).toEqual(["/api/v1/builds/b/tests?cursor=0", "/api/v1/builds/b/tests?cursor=200"]);
    }
  });

  it("lays out per-machine timeline bars with non-color status symbols, wall-time durations, and active lease clamping", () => {
    const status = {
      created_at_ms: 1_000,
      last_seen_ms: 4_000,
      finished_at_ms: null,
      runners: [
        { runner_id: "r-b", machine_index: 1, first_seen_ms: 1_100 },
        { runner_id: "r-a", machine_index: 0, first_seen_ms: 1_000 },
      ],
    };
    const tests = [
      {
        test: "tests/a.py",
        attempts: [
          { lease_id: 1, runner_id: "r-a", generation: 1, status: "passed", timed: true, leased_at_ms: 1_000, finished_at_ms: 2_000, duration_ms: 900 },
        ],
      },
      {
        test: "tests/b.py",
        attempts: [
          { lease_id: 1, runner_id: "r-a", generation: 1, status: "failed", timed: false, leased_at_ms: 1_000, finished_at_ms: 2_500, duration_ms: 0 },
        ],
      },
      {
        test: "tests/c.py",
        attempts: [
          { lease_id: 2, runner_id: "r-b", generation: 1, status: "leased", timed: false, leased_at_ms: 2_000, finished_at_ms: 0, duration_ms: 0 },
        ],
      },
    ];

    const tl = buildTimeline(status, tests, 5_000);
    expect(tl.lanes.map((l) => l.runnerID)).toEqual(["r-a", "r-b"]);
    expect(tl.lanes[0]!.bars).toHaveLength(1);
    const bar1 = tl.lanes[0]!.bars[0]!;
    expect(bar1.kind).toBe("failed");
    expect(bar1.meta.symbol).toBe("✗");
    expect(bar1.tests).toHaveLength(2);
    expect(bar1.tests[0]!.durationText).toBe("900 ms");
    expect(bar1.tests[1]!.durationText).toBe("1.5 s");
    expect(bar1.xPct).toBeCloseTo(0, 2);

    const bar2 = tl.lanes[1]!.bars[0]!;
    expect(bar2.kind).toBe("leased");
    expect(bar2.meta.symbol).toBe("…");
    expect(bar2.tests[0]!.durationText).toBe("");
    expect(bar2.endMS).toBe(5_000);
    expect(bar2.xPct + bar2.wPct).toBeLessThanOrEqual(100.01);
  });

  it("retries network/5xx/429 errors up to cap and restarts polling on manual refresh", () => {
    let now = 50_000;
    let s = pollStep(createPollState(), { type: "watch", verdict: "running", nowMS: now }).state;

    // Network (0) and 5xx (500, 502, 503) and 429 all back off until MAX_POLL_FAILURES stops polling.
    const retryCodes = [0, 500, 502, 503, 429];
    for (let i = 0; i < MAX_POLL_FAILURES; i++) {
      now = s.nextAtMS!;
      s = pollStep(s, { type: "tick", nowMS: now }).state;
      now += 100;
      const res = pollStep(s, { type: "response_error", httpStatus: retryCodes[i]!, nowMS: now });
      s = res.state;
      if (i + 1 < MAX_POLL_FAILURES) {
        expect(s.active).toBe(true);
        expect(s.failures).toBe(i + 1);
      } else {
        expect(s.active).toBe(false);
        expect(s.stoppedReason).toBe("max_retries");
        expect(s.nextAtMS).toBeNull();
      }
    }

    // Manual refresh that succeeds while the build is still running must restart polling.
    now += 5_000;
    const manualRes = pollStep(s, { type: "response_ok", verdict: "running", manual: true, nowMS: now });
    expect(manualRes.state.active).toBe(true);
    expect(manualRes.state.failures).toBe(0);
    expect(manualRes.state.stoppedReason).toBeNull();
    expect(manualRes.state.nextAtMS).toBe(now + BASE_POLL_MS);
    expect(manualRes.actions.some((a) => a.type === "schedule")).toBe(true);

    // Non-retryable 404 stops immediately.
    const err404 = pollStep(manualRes.state, { type: "response_error", httpStatus: 404, nowMS: now + 100 }).state;
    expect(err404.active).toBe(false);
    expect(err404.stoppedReason).toBe("error");
  });

  it("enforces polling invariants across 200 seeded DST runs (min 10s interval, visibility pause, terminal stop, backoff)", () => {
    for (let seed = 1; seed <= 200; seed++) {
      const rng = makeRng(seed);
      let s = createPollState();
      let now = 100_000;
      let lastFetchAt: number | null = now;

      let res = pollStep(s, { type: "watch", verdict: "running", nowMS: now });
      s = res.state;
      expect(s.nextAtMS).toBe(now + BASE_POLL_MS);

      for (let step = 0; step < 60; step++) {
        const roll = rng();
        if (roll < 0.25) {
          now += 500 + Math.floor(rng() * 5_000);
          res = pollStep(s, { type: "visibility", visible: !s.visible, nowMS: now });
          s = res.state;
          if (!s.visible) expect(s.nextAtMS).toBeNull();
          continue;
        }

        if (s.nextAtMS !== null && !s.inFlight) {
          now = Math.max(now + 100, s.nextAtMS);
          res = pollStep(s, { type: "tick", nowMS: now });
          s = res.state;
          const fetched = res.actions.some((a) => a.type === "fetch");
          if (fetched) {
            expect(s.visible).toBe(true);
            expect(s.verdict).toBe("running");
            if (lastFetchAt !== null) {
              expect(now - lastFetchAt, `seed=${seed} polled too fast`).toBeGreaterThanOrEqual(BASE_POLL_MS);
            }
            lastFetchAt = now;

            now += 100 + Math.floor(rng() * 400);
            const rRoll = rng();
            if (rRoll < 0.55) {
              res = pollStep(s, { type: "response_ok", verdict: "running", nowMS: now });
              s = res.state;
              expect(s.failures).toBe(0);
            } else if (rRoll < 0.72) {
              res = pollStep(s, { type: "response_ok", verdict: "passed", nowMS: now });
              s = res.state;
              expect(s.active).toBe(false);
              expect(s.nextAtMS).toBeNull();
              expect(s.stoppedReason).toBe("finished");
              break;
            } else if (rRoll < 0.92) {
              const prevFailures = s.failures;
              const retryableCodes = [0, 429, 500, 502, 503];
              const code = retryableCodes[Math.floor(rng() * retryableCodes.length)]!;
              const retryAfterMS = rng() < 0.3 ? 25_000 : 0;
              res = pollStep(s, { type: "response_error", httpStatus: code, retryAfterMS, nowMS: now });
              s = res.state;
              if (prevFailures + 1 >= MAX_POLL_FAILURES) {
                expect(s.active).toBe(false);
                expect(s.stoppedReason).toBe("max_retries");
                break;
              }
              expect(s.failures).toBe(prevFailures + 1);
              expect(s.nextAtMS).not.toBeNull();
              expect(s.nextAtMS! - now).toBeLessThanOrEqual(MAX_POLL_MS);
              expect(s.nextAtMS! - now).toBeGreaterThanOrEqual(BASE_POLL_MS * 2);
            } else {
              res = pollStep(s, { type: "response_error", httpStatus: 403, nowMS: now });
              s = res.state;
              expect(s.active).toBe(false);
              expect(s.nextAtMS).toBeNull();
              expect(s.stoppedReason).toBe("error");
              break;
            }
          }
        } else {
          now += 2_000;
        }
      }
    }
  });
});
