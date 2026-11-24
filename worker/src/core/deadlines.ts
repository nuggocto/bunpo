// Pure active-lease deadline tracker for hibernating Durable Objects.
//
// Cloudflare hibernates idle Durable Objects after ~10 s, discarding in-memory
// state. Resetting active leases to `now + ttl` on wake would keep a dead
// runner's lease alive forever whenever another runner polls > 10 s apart.
//
// Instead, we track three values per active lease L:
//   - base[L]:      last real deadline (grant, or heartbeat/replay: now + ttl)
//   - persisted[L]: deadline durable in SQLite (event_log grant or meta row)
//   - effective[L]: deadline applied in memory (equals persisted[L] + grace on wake)
//
// Where grace = floor(ttl / 2). A heartbeat/replay triggers ONE meta-row write
// storing `base` for all active leases only when `persisted[L] + grace < base[L]`.
// We NEVER persist the graced effective value, so grace never compounds across wakes.
//
// Guarantees:
//   1. A lease never expires earlier than any deadline promised to its runner.
//   2. A dead runner's lease expires at most floor(ttl / 2) past its last real deadline,
//      regardless of how many times the DO hibernates and wakes.
//   3. Meta writes are throttled to at most ~1 write per floor(ttl / 2) across all
//      active leases combined, independent of machine count.

export type SerializedDeadlines = Record<string, number>;

export interface ActiveLeaseSeed {
  leaseID: number;
  grantedDeadlineMS: number;
}

export class LeaseDeadlineTracker {
  private readonly base = new Map<number, number>();
  private readonly persisted = new Map<number, number>();

  clear(): void {
    this.base.clear();
    this.persisted.clear();
  }

  // Record a newly granted lease whose grantedDeadlineMS is already persisted in event_log.
  onGrant(leaseID: number, grantedDeadlineMS: number): void {
    this.base.set(leaseID, grantedDeadlineMS);
    this.persisted.set(leaseID, grantedDeadlineMS);
  }

  // Remove leases that are no longer active (reported, expired, or finalized).
  onClose(leaseID: number): void {
    this.base.delete(leaseID);
    this.persisted.delete(leaseID);
  }

  // Restore tracker state on DO wake from event_log active leases plus the
  // single persisted meta row. Returns the graced effective deadlines to apply
  // to State.leases (persistedOrGranted + floor(ttl / 2)).
  restoreOnWake(
    activeLeases: readonly ActiveLeaseSeed[],
    storedMeta: SerializedDeadlines | null,
    ttlMS: number,
  ): Map<number, number> {
    this.clear();
    const grace = Math.floor(ttlMS / 2);
    const effective = new Map<number, number>();

    for (let i = 0; i < activeLeases.length; i++) {
      const a = activeLeases[i]!;
      const stored = storedMeta ? storedMeta[String(a.leaseID)] : undefined;
      const dur =
        typeof stored === "number" && Number.isSafeInteger(stored) && stored > a.grantedDeadlineMS
          ? stored
          : a.grantedDeadlineMS;
      this.base.set(a.leaseID, dur);
      this.persisted.set(a.leaseID, dur);
      effective.set(a.leaseID, dur + grace);
    }

    return effective;
  }

  // Evaluate a heartbeat or repeated-request lease extension at `nowMS`.
  // Returns the snapshot of ALL active leases' `base` deadlines if SQLite must
  // be updated before replying, or null if the existing persisted deadline
  // already covers `nowMS + ttlMS` within wake grace.
  //
  // When returning a non-null snapshot, neither `base` nor `persisted` is mutated
  // until commitPersist() is called after SQLite commits.
  planExtend(leaseID: number, nowMS: number, ttlMS: number): SerializedDeadlines | null {
    const prevBase = this.base.get(leaseID);
    if (prevBase === undefined) {
      throw new Error(`lease ${leaseID} is not active in LeaseDeadlineTracker`);
    }
    const realDeadline = nowMS + ttlMS;
    const nextBase = realDeadline > prevBase ? realDeadline : prevBase;

    const grace = Math.floor(ttlMS / 2);
    const prevPersisted = this.persisted.get(leaseID) ?? 0;
    if (prevPersisted + grace >= nextBase) {
      this.base.set(leaseID, nextBase);
      return null;
    }

    // One meta write persists the real (ungraced) base deadline of every active lease.
    const snapshot: SerializedDeadlines = {};
    for (const [id, dl] of this.base) {
      snapshot[String(id)] = id === leaseID ? nextBase : dl;
    }
    return snapshot;
  }

  // Mark a planned snapshot as durably persisted after SQLite commits.
  commitPersist(snapshot: SerializedDeadlines): void {
    for (const [key, dl] of Object.entries(snapshot)) {
      const id = Number(key);
      if (this.base.has(id)) {
        const curBase = this.base.get(id) ?? 0;
        if (dl > curBase) {
          this.base.set(id, dl);
        }
        const curPersisted = this.persisted.get(id) ?? 0;
        if (dl > curPersisted) {
          this.persisted.set(id, dl);
        }
      }
    }
  }
}

export function parseSerializedDeadlines(raw: string | null): SerializedDeadlines | null {
  if (!raw) {
    return null;
  }
  const parsed: unknown = JSON.parse(raw);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("corrupt lease_deadlines meta row: expected object");
  }
  const out: SerializedDeadlines = {};
  for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof v !== "number" || !Number.isSafeInteger(v) || v < 0) {
      throw new Error(`corrupt lease_deadlines entry ${JSON.stringify(k)}`);
    }
    out[k] = v;
  }
  return out;
}
