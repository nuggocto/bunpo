import type { State } from "../src/core/index.ts";

// Simulates the standalone Go server's restart rule for shared Go vector and
// differential tests (the Worker uses LeaseDeadlineTracker instead).
export function resetActiveLeaseDeadlinesForGoRestart(s: State, nowMS: number): void {
  if (!s.initialized) {
    return;
  }

  const now = Math.max(nowMS, s.lastSeenMS);
  const newDeadline = now + s.config.lease_ttl_ms;
  for (let i = 0; i < s.activeLeases.length; i++) {
    const id = s.activeLeases[i]!;
    const lease = s.leases[id - 1]!;
    if (newDeadline > lease.deadline_ms) {
      lease.deadline_ms = newDeadline;
    }
  }
}
