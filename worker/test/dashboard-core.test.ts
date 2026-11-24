import { describe, expect, it } from "vitest";
import {
  BASE_POLL_MS,
  MAX_POLL_FAILURES,
  MAX_POLL_MS,
  buildPath,
  buildTimeline,
  createPollState,
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

  it("formats durations, time left, and sorts builds newest first", () => {
    expect(formatDuration(0)).toBe("0 ms");
    expect(formatDuration(450)).toBe("450 ms");
    expect(formatDuration(1_500)).toBe("1.5 s");
    expect(formatDuration(135_000)).toBe("2m 15s");
    expect(formatDuration(3_840_000)).toBe("1h 04m");
    expect(formatDuration(-1)).toBe("—");

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

  it("lays out per-machine timeline bars with non-color status symbols and active lease clamping", () => {
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
          { lease_id: 1, runner_id: "r-a", generation: 1, status: "passed", leased_at_ms: 1_000, finished_at_ms: 2_000, duration_ms: 900 },
        ],
      },
      {
        test: "tests/b.py",
        attempts: [
          { lease_id: 1, runner_id: "r-a", generation: 1, status: "failed", leased_at_ms: 1_000, finished_at_ms: 2_500, duration_ms: 0 },
        ],
      },
      {
        test: "tests/c.py",
        attempts: [
          { lease_id: 2, runner_id: "r-b", generation: 1, status: "leased", leased_at_ms: 2_000, finished_at_ms: 0, duration_ms: 0 },
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
    expect(bar1.xPct).toBeCloseTo(0, 2);

    const bar2 = tl.lanes[1]!.bars[0]!;
    expect(bar2.kind).toBe("leased");
    expect(bar2.meta.symbol).toBe("…");
    expect(bar2.endMS).toBe(5_000);
    expect(bar2.xPct + bar2.wPct).toBeLessThanOrEqual(100.01);
  });

  it("enforces polling invariants across 200 seeded DST runs (min 10s interval, visibility pause, terminal stop, 429/503 backoff)", () => {
    for (let seed = 1; seed <= 200; seed++) {
      const rng = makeRng(seed);
      let s = createPollState();
      let now = 100_000;
      let lastFetchAt: number | null = now; // initial load at watch

      let res = pollStep(s, { type: "watch", verdict: "running", nowMS: now });
      s = res.state;
      expect(s.nextAtMS).toBe(now + BASE_POLL_MS);

      for (let step = 0; step < 60; step++) {
        const roll = rng();
        if (roll < 0.25) {
          // Flip tab visibility rapidly — must NEVER cause fetches faster than BASE_POLL_MS.
          now += 500 + Math.floor(rng() * 5_000);
          res = pollStep(s, { type: "visibility", visible: !s.visible, nowMS: now });
          s = res.state;
          if (!s.visible) {
            expect(s.nextAtMS).toBeNull();
          }
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

            // Simulate response: ok running, ok finished, 429/503 backoff, or 401/500 hard stop.
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
              const code = rng() < 0.5 ? 429 : 503;
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
              res = pollStep(s, { type: "response_error", httpStatus: 500, nowMS: now });
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
