import { describe, expect, it } from "vitest";
import {
  type ActiveLeaseSeed,
  type SerializedDeadlines,
  LeaseDeadlineTracker,
  parseSerializedDeadlines,
} from "../src/core/deadlines.ts";

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

describe("LeaseDeadlineTracker (hibernation wake deadline rule)", () => {
  it("never expires before promised deadline, expires within ttl/2 after last real deadline across arbitrary wakes, and bounds writes", () => {
    // Test both even and odd TTLs to catch any integer-division off-by-one.
    const ttls = [10_000, 10_001, 15_000, 60_000, 60_001];

    for (let seed = 1; seed <= 500; seed++) {
      const rng = makeRng(seed);
      const ttl = ttls[seed % ttls.length]!;
      const grace = Math.floor(ttl / 2);

      const tracker = new LeaseDeadlineTracker();
      let storedMeta: SerializedDeadlines | null = null;

      interface LeaseModel {
        id: number;
        granted: number;
        lastRealDeadline: number; // max(granted, now + ttl) on heartbeats/replays
        lastPromised: number;     // max deadline ever returned to client
        effective: number;        // current in-memory deadline
        alive: boolean;
      }

      const leases = new Map<number, LeaseModel>();
      let nextID = 1;
      let now = 1_000_000;
      let writes = 0;

      const numInitial = 1 + randInt(rng, 8);
      for (let i = 0; i < numInitial; i++) {
        const id = nextID++;
        const dl = now + ttl;
        tracker.onGrant(id, dl);
        leases.set(id, {
          id,
          granted: dl,
          lastRealDeadline: dl,
          lastPromised: dl,
          effective: dl,
          alive: true,
        });
        now += randInt(rng, 500);
      }

      const startMS = now;
      const steps = 120;
      for (let step = 0; step < steps; step++) {
        now += 1 + randInt(rng, Math.floor(ttl * 0.45));
        const roll = rng();

        if (roll < 0.3) {
          // Simulate DO hibernation + wake (can happen repeatedly back-to-back!).
          const seeds: ActiveLeaseSeed[] = [];
          for (const l of leases.values()) {
            if (l.alive) {
              seeds.push({ leaseID: l.id, grantedDeadlineMS: l.granted });
            }
          }
          const restored = tracker.restoreOnWake(
            seeds,
            storedMeta ? parseSerializedDeadlines(JSON.stringify(storedMeta)) : null,
            ttl,
          );
          for (const l of leases.values()) {
            if (!l.alive) continue;
            const eff = restored.get(l.id)!;
            // Invariant 1: never expires before any previously promised deadline!
            expect(eff, `seed=${seed} step=${step} lease=${l.id} ttl=${ttl}: wake expired before promise`).toBeGreaterThanOrEqual(
              l.lastPromised,
            );
            // Invariant 2: never exceeds last real deadline + floor(ttl/2), even after many wakes!
            expect(eff, `seed=${seed} step=${step} lease=${l.id} ttl=${ttl}: wake grace compounded`).toBeLessThanOrEqual(
              l.lastRealDeadline + grace,
            );
            l.effective = eff;
          }
        } else if (roll < 0.85) {
          // Pick a random active lease and heartbeat it if still held.
          const active = Array.from(leases.values()).filter((l) => l.alive);
          if (active.length === 0) break;
          const target = active[randInt(rng, active.length)]!;
          if (now >= target.effective) {
            // Lease expired! Check it did not expire before lastPromised and did expire <= lastRealDeadline + grace.
            expect(target.effective).toBeGreaterThanOrEqual(target.lastPromised);
            expect(target.effective).toBeLessThanOrEqual(target.lastRealDeadline + grace);
            target.alive = false;
            tracker.onClose(target.id);
            continue;
          }

          // Simulate healthy heartbeat (matches decideHeartbeat: max(effective, now + ttl)).
          const promised = Math.max(target.effective, now + ttl);
          const real = now + ttl;

          const snap = tracker.planExtend(target.id, now, ttl);
          if (snap !== null) {
            // Simulate occasional failed write (5%) -> DO aborts and returns 503,
            // so the runner never receives a new promise and storage is unchanged.
            if (rng() < 0.05) {
              const seeds: ActiveLeaseSeed[] = [];
              for (const l of leases.values()) {
                if (l.alive) seeds.push({ leaseID: l.id, grantedDeadlineMS: l.granted });
              }
              const restored = tracker.restoreOnWake(seeds, storedMeta, ttl);
              for (const l of leases.values()) {
                if (l.alive) l.effective = restored.get(l.id)!;
              }
              continue;
            }
            storedMeta = snap;
            tracker.commitPersist(snap);
            writes++;
          }

          if (real > target.lastRealDeadline) {
            target.lastRealDeadline = real;
          }
          if (promised > target.lastPromised) {
            target.lastPromised = promised;
          }
          target.effective = promised;
        } else {
          // Grant a new lease to test mixed-age active leases sharing one meta row.
          const id = nextID++;
          const dl = now + ttl;
          tracker.onGrant(id, dl);
          leases.set(id, {
            id,
            granted: dl,
            lastRealDeadline: dl,
            lastPromised: dl,
            effective: dl,
            alive: true,
          });
        }
      }

      // Verify dead-runner bound: advance time to max(lastRealDeadline) + grace with
      // repeated wakes every 10s, and assert EVERY lease expires.
      const maxReal = Math.max(...Array.from(leases.values()).map((l) => l.lastRealDeadline));
      const targetTime = Math.max(now, maxReal + grace);
      let t = now;
      while (true) {
        const seeds: ActiveLeaseSeed[] = [];
        for (const l of leases.values()) {
          if (l.alive) seeds.push({ leaseID: l.id, grantedDeadlineMS: l.granted });
        }
        const restored = tracker.restoreOnWake(seeds, storedMeta, ttl);
        for (const l of leases.values()) {
          if (!l.alive) continue;
          const eff = restored.get(l.id)!;
          expect(eff).toBeLessThanOrEqual(l.lastRealDeadline + grace);
          if (t >= eff) {
            l.alive = false;
            tracker.onClose(l.id);
          }
        }
        if (t >= targetTime) break;
        t = Math.min(targetTime, t + 10_000);
      }
      for (const l of leases.values()) {
        expect(l.alive, `seed=${seed} lease=${l.id} failed to expire by lastRealDeadline + ttl/2`).toBe(false);
      }

      // Invariant 3: write count stays strictly bounded by elapsed time / grace (plus active leases
      // staggered across the initial window), never proportional to raw heartbeat count.
      const elapsedMS = Math.max(1, now - startMS);
      const maxExpectedWrites = Math.ceil(elapsedMS / grace) * leases.size + 1;
      expect(writes).toBeLessThanOrEqual(maxExpectedWrites);
    }
  });

  it("bounds meta writes for 64 concurrent machines over a 10-minute build", () => {
    const ttl = 60_000;
    const hbInterval = 20_000;
    const tracker = new LeaseDeadlineTracker();
    let storedMeta: SerializedDeadlines | null = null;
    let writes = 0;
    let heartbeats = 0;

    // 64 machines staggered across 0..640ms
    for (let m = 1; m <= 64; m++) {
      tracker.onGrant(m, m * 10 + ttl);
    }
    for (let t = hbInterval; t <= 600_000; t += hbInterval) {
      for (let m = 1; m <= 64; m++) {
        heartbeats++;
        const snap = tracker.planExtend(m, t + m * 10, ttl);
        if (snap !== null) {
          storedMeta = snap;
          tracker.commitPersist(snap);
          writes++;
        }
      }
    }

    expect(heartbeats).toBe(1920);
    // All 64 machines share one meta row: <= 30 writes across 30 heartbeat rounds (~1.5% of heartbeats).
    expect(writes).toBeLessThanOrEqual(30);
    expect(writes).toBeGreaterThanOrEqual(15);
    expect(storedMeta).not.toBeNull();
  });

  it("handles boundary conditions, unknown leases, and corrupt meta rows", () => {
    const tracker = new LeaseDeadlineTracker();
    // Exact boundary: ttl=10001 -> grace=5000. Granted at t=0 -> dl=10001.
    tracker.onGrant(1, 10_001);
    // At t=5000, real=15001 = persisted + grace -> no write needed.
    expect(tracker.planExtend(1, 5_000, 10_001)).toBeNull();
    // At t=5001, real=15002 > persisted + grace (15001) -> write required.
    const snap = tracker.planExtend(1, 5_001, 10_001);
    expect(snap).toEqual({ "1": 15_002 });
    // Before commitPersist, a second planExtend still sees old persisted value.
    expect(tracker.planExtend(1, 5_001, 10_001)).toEqual({ "1": 15_002 });
    tracker.commitPersist(snap!);
    // After commitPersist, no write needed at same timestamp.
    expect(tracker.planExtend(1, 5_001, 10_001)).toBeNull();
    // Closed lease cannot be extended.
    tracker.onClose(1);
    expect(() => tracker.planExtend(1, 6_000, 10_001)).toThrow(/not active/);

    // restoreOnWake ignores stale storedMeta entries <= grantedDeadlineMS and closed lease IDs.
    const restored = tracker.restoreOnWake(
      [{ leaseID: 2, grantedDeadlineMS: 20_000 }],
      { "1": 99_999, "2": 15_000 },
      10_001,
    );
    expect(restored.get(2)).toBe(25_000);
    expect(restored.has(1)).toBe(false);

    // parseSerializedDeadlines validation.
    expect(parseSerializedDeadlines(null)).toBeNull();
    expect(parseSerializedDeadlines("")).toBeNull();
    expect(() => parseSerializedDeadlines("[]")).toThrow();
    expect(() => parseSerializedDeadlines("null")).toThrow();
    expect(() => parseSerializedDeadlines("123")).toThrow();
    expect(() => parseSerializedDeadlines('{"1": -5}')).toThrow();
    expect(() => parseSerializedDeadlines('{"1": 1.5}')).toThrow();
    expect(() => parseSerializedDeadlines('{"1": "bad"}')).toThrow();
    expect(parseSerializedDeadlines('{"1": 12345}')).toEqual({ "1": 12345 });
  });
});
