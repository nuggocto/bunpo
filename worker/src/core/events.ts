import { compareUTF8 } from "./canonical.ts";
import {
  type AttemptStatus,
  type Config,
  type Disposition,
  type HistoryRecord,
  type InitConflict,
  type LeaseAttempt,
  type Outcome,
  type RunnerState,
  type State,
  type TestState,
  MaxDurationMS,
  MaxInitConflicts,
  MaxTestHistory,
  computeEffectivePredictions,
  configDigest,
  effectiveLimits,
  errInvalid,
  formatAttemptID,
  isValidOutcome,
  stateVerdict,
  validBuildID,
  validRequestID,
  validRunnerID,
  validateConfig,
  validateMachineIndex,
  validateTestList,
} from "./types.ts";

export type EventType = "initialized" | "conflicted" | "leased" | "reported" | "expired" | "finalized";

export interface InitializedData {
  build_id: string;
  digest: string;
  config: Config;
  deadline_ms: number;
  tests: string[];
  predictions_ms: (number | null)[];
}

export interface ConflictedData {
  runner_id: string;
  machine_index: number | null;
  got_digest: string;
  reason: string;
}

export interface LeasedData {
  lease_id: number;
  runner_id: string;
  machine_index: number | null;
  request_id: string;
  deadline_ms: number;
  attempts: LeaseAttempt[];
}

export interface ReportedAttempt {
  test_index: number;
  generation: number;
  outcome: Outcome;
  timed: boolean;
  duration_ms: number;
  disposition: Disposition;
}

export interface ReportedData {
  lease_id: number;
  runner_id: string;
  close_lease: boolean;
  results: ReportedAttempt[];
}

export interface ExpiredData {
  lease_id: number;
}

export interface FinalizedData {
  reason: string;
}

export interface QueueEvent {
  type: EventType;
  at_ms: number;
  initialized?: InitializedData;
  conflicted?: ConflictedData;
  leased?: LeasedData;
  reported?: ReportedData;
  expired?: ExpiredData;
  finalized?: FinalizedData;
}

export function hasInitConflict(conflicts: readonly InitConflict[], runnerID: string, gotDigest: string): boolean {
  for (let i = 0; i < conflicts.length; i++) {
    const c = conflicts[i]!;
    if (c.runner_id === runnerID && c.got_digest === gotDigest) {
      return true;
    }
  }
  return false;
}

export function hasHistoryRecord(
  history: readonly HistoryRecord[],
  rec: Omit<HistoryRecord, "at_ms">,
): boolean {
  for (let i = 0; i < history.length; i++) {
    const h = history[i]!;
    if (
      h.generation === rec.generation &&
      h.lease_id === rec.lease_id &&
      h.runner_id === rec.runner_id &&
      h.outcome === rec.outcome &&
      h.timed === rec.timed &&
      h.duration_ms === rec.duration_ms &&
      h.disposition === rec.disposition
    ) {
      return true;
    }
  }
  return false;
}

export function validateReportValues(outcome: Outcome, timed: boolean, durationMS: number) {
  if (!isValidOutcome(outcome)) {
    return errInvalid(`invalid outcome ${JSON.stringify(outcome)}`);
  }
  if (!timed) {
    if (durationMS !== 0) {
      return errInvalid(`untimed result must have duration_ms 0, got ${durationMS}`);
    }
    return null;
  }
  if (outcome !== "passed") {
    return errInvalid(`only passed outcomes may be timed, got ${JSON.stringify(outcome)}`);
  }
  if (!Number.isInteger(durationMS) || durationMS < 0 || durationMS > MaxDurationMS) {
    return errInvalid(`duration_ms ${durationMS} is outside 0..${MaxDurationMS}`);
  }
  return null;
}

export function outcomeToAttemptStatus(o: Outcome): AttemptStatus {
  switch (o) {
    case "passed":
      return "passed";
    case "failed":
      return "failed";
    case "skipped":
      return "skipped";
    default:
      return "error";
  }
}

export interface ApplyOptions {
  // Skip redundant per-path validation and SHA-256 re-hashing on an initialized
  // event that was already validated by decideInit or persisted to our own SQLite.
  // All structural preconditions and every other event type still validate in full.
  trustedInit?: boolean;
}

export function applyEvent(s: State, ev: QueueEvent, opts?: ApplyOptions): void {
  if (ev.at_ms < 0) {
    throw new Error("event at_ms must not be negative");
  }
  if (s.initialized && ev.at_ms < s.lastSeenMS) {
    throw new Error(`event at_ms ${ev.at_ms} precedes last_seen_ms ${s.lastSeenMS}`);
  }

  switch (ev.type) {
    case "initialized":
      applyInitialized(s, ev.at_ms, ev.initialized, opts?.trustedInit === true);
      return;
    case "conflicted":
      applyConflicted(s, ev.at_ms, ev.conflicted);
      return;
    case "leased":
      applyLeased(s, ev.at_ms, ev.leased);
      return;
    case "reported":
      applyReported(s, ev.at_ms, ev.reported);
      return;
    case "expired":
      applyExpired(s, ev.at_ms, ev.expired);
      return;
    case "finalized":
      applyFinalized(s, ev.at_ms, ev.finalized);
      return;
    default:
      throw new Error(`unknown event type ${JSON.stringify(ev.type)}`);
  }
}

function applyInitialized(s: State, atMS: number, d: InitializedData | undefined, trustedInit: boolean): void {
  if (s.initialized) {
    throw new Error("build already initialized");
  }
  if (!d) {
    throw new Error("initialized event missing payload");
  }
  if (!validBuildID(d.build_id)) {
    throw new Error(`invalid build_id ${JSON.stringify(d.build_id)}`);
  }
  const cfgErr = validateConfig(d.config, s.limits);
  if (cfgErr) {
    throw cfgErr;
  }
  const lim = effectiveLimits(s.limits);
  if (trustedInit) {
    if (d.tests.length === 0 || d.tests.length > lim.maxTests) {
      throw new Error(`initialized tests count ${d.tests.length} outside 1..${lim.maxTests}`);
    }
    if (d.predictions_ms.length !== d.tests.length) {
      throw new Error(
        `initialized predictions_ms length ${d.predictions_ms.length} does not match tests length ${d.tests.length}`,
      );
    }
    if (!/^[0-9a-f]{64}$/.test(d.digest)) {
      throw new Error(`initialized digest ${JSON.stringify(d.digest)} is not a 64-hex SHA-256`);
    }
  } else {
    const testsErr = validateTestList(d.tests, d.predictions_ms, lim.maxTests);
    if (testsErr) {
      throw testsErr;
    }
    const wantDigest = configDigest(d.config, d.tests);
    if (d.digest !== wantDigest) {
      throw new Error(`initialized digest ${JSON.stringify(d.digest)} does not match computed ${JSON.stringify(wantDigest)}`);
    }
  }
  if (d.deadline_ms < atMS) {
    throw new Error(`initialized deadline_ms ${d.deadline_ms} precedes at_ms ${atMS}`);
  }

  const eff = computeEffectivePredictions(d.predictions_ms);
  const tests = new Array<TestState>(d.tests.length);
  const order = new Array<number>(d.tests.length);
  for (let i = 0; i < d.tests.length; i++) {
    tests[i] = {
      index: i,
      path: d.tests[i]!,
      predictedMS: d.predictions_ms[i]!,
      effectiveMS: eff[i]!,
      status: "pending",
      generation: 0,
      activeLeaseID: 0,
      timed: false,
      durationMS: 0,
      attempts: [],
      history: [],
      historyTruncated: false,
    };
    order[i] = i;
  }
  order.sort((a, b) => {
    const diff = eff[b]! - eff[a]!;
    return diff !== 0 ? diff : a - b;
  });

  s.initialized = true;
  s.buildID = d.build_id;
  s.digest = d.digest;
  s.config = { ...d.config };
  s.createdAtMS = atMS;
  s.deadlineMS = d.deadline_ms;
  s.lastSeenMS = atMS;
  s.tests = tests;
  s.dealOrder = order;
  s.counts = {
    total: tests.length,
    pending: tests.length,
    leased: 0,
    passed: 0,
    failed: 0,
    skipped: 0,
    unresolved: 0,
  };
}

function applyConflicted(s: State, atMS: number, d: ConflictedData | undefined): void {
  if (!s.initialized) {
    throw new Error("conflicted event before initialized");
  }
  if (!d) {
    throw new Error("conflicted event missing payload");
  }
  if (!validRunnerID(d.runner_id)) {
    throw new Error(`invalid runner_id ${JSON.stringify(d.runner_id)}`);
  }
  const miErr = validateMachineIndex(d.machine_index);
  if (miErr) {
    throw miErr;
  }
  if (d.got_digest === "" || d.got_digest === s.digest) {
    throw new Error("conflicted event must carry a differing got_digest");
  }
  if (s.conflicts.length >= MaxInitConflicts) {
    throw new Error("conflicted event exceeds max stored init conflicts");
  }
  if (hasInitConflict(s.conflicts, d.runner_id, d.got_digest)) {
    throw new Error("duplicate conflicted event");
  }

  s.lastSeenMS = Math.max(s.lastSeenMS, atMS);
  s.conflicts.push({
    runner_id: d.runner_id,
    machine_index: d.machine_index,
    got_digest: d.got_digest,
    reason: d.reason,
    at_ms: atMS,
  });
  if (s.conflicts.length === MaxInitConflicts) {
    s.conflictsTruncated = true;
  }
}

function applyLeased(s: State, atMS: number, d: LeasedData | undefined): void {
  if (!s.initialized) {
    throw new Error("leased event before initialized");
  }
  if (!d) {
    throw new Error("leased event missing payload");
  }
  const wantID = s.leases.length + 1;
  if (d.lease_id !== wantID) {
    throw new Error(`leased event lease_id ${d.lease_id}, want ${wantID}`);
  }
  if (!validRunnerID(d.runner_id) || !validRequestID(d.request_id)) {
    throw new Error("leased event has invalid runner_id or request_id");
  }
  const miErr = validateMachineIndex(d.machine_index);
  if (miErr) {
    throw miErr;
  }
  if (d.deadline_ms < atMS) {
    throw new Error("leased deadline_ms precedes at_ms");
  }
  if (d.attempts.length === 0 || d.attempts.length > s.config.max_batch) {
    throw new Error(`leased attempts count ${d.attempts.length} outside 1..${s.config.max_batch}`);
  }
  const existingRunner = s.runners.get(d.runner_id);
  const lim = effectiveLimits(s.limits);
  if (!existingRunner && s.runners.size >= lim.maxRunners) {
    throw new Error("runner limit exceeded");
  }
  if (existingRunner && existingRunner.activeLeaseID !== 0) {
    throw new Error(`runner ${JSON.stringify(d.runner_id)} already holds active lease ${existingRunner.activeLeaseID}`);
  }

  // Validate every attempt before mutating state.
  for (let i = 0; i < d.attempts.length; i++) {
    const a = d.attempts[i]!;
    if (a.test_index < 0 || a.test_index >= s.tests.length) {
      throw new Error(`leased test_index ${a.test_index} out of range`);
    }
    if (i > 0 && d.attempts[i - 1]!.test_index >= a.test_index) {
      throw new Error("leased attempts must be unique and sorted by test_index");
    }
    const t = s.tests[a.test_index]!;
    if (t.status !== "pending") {
      throw new Error(`test ${a.test_index} is ${t.status}, not pending`);
    }
    if (a.generation !== t.generation + 1 || a.generation > s.config.max_attempts) {
      throw new Error(
        `test ${a.test_index} generation ${a.generation} invalid (current ${t.generation}, max ${s.config.max_attempts})`,
      );
    }
  }

  s.lastSeenMS = Math.max(s.lastSeenMS, atMS);
  const attempts = d.attempts.map((a) => ({ test_index: a.test_index, generation: a.generation }));
  s.leases.push({
    lease_id: d.lease_id,
    runner_id: d.runner_id,
    machine_index: d.machine_index,
    request_id: d.request_id,
    status: "active",
    leased_at_ms: atMS,
    deadline_ms: d.deadline_ms,
    finished_at_ms: 0,
    attempts,
  });
  s.activeLeases.push(d.lease_id);

  const r = ensureRunner(s, d.runner_id, d.machine_index, atMS);
  r.leases++;
  r.activeLeaseID = d.lease_id;
  r.lastRequestID = d.request_id;
  r.lastLeaseID = d.lease_id;

  for (let i = 0; i < attempts.length; i++) {
    const a = attempts[i]!;
    const t = s.tests[a.test_index]!;
    t.status = "leased";
    t.generation = a.generation;
    t.activeLeaseID = d.lease_id;
    t.attempts.push({
      attempt_id: formatAttemptID(a.test_index, a.generation),
      generation: a.generation,
      lease_id: d.lease_id,
      runner_id: d.runner_id,
      status: "leased",
      timed: false,
      duration_ms: 0,
      leased_at_ms: atMS,
      finished_at_ms: 0,
    });
    s.counts.pending--;
    s.counts.leased++;
  }
}

function applyReported(s: State, atMS: number, d: ReportedData | undefined): void {
  if (!s.initialized) {
    throw new Error("reported event before initialized");
  }
  if (!d) {
    throw new Error("reported event missing payload");
  }
  if (d.lease_id < 1 || d.lease_id > s.leases.length) {
    throw new Error(`reported unknown lease_id ${d.lease_id}`);
  }
  const lease = s.leases[d.lease_id - 1]!;
  if (lease.runner_id !== d.runner_id) {
    throw new Error("reported runner_id mismatch");
  }
  if (d.results.length === 0 || d.results.length > lease.attempts.length) {
    throw new Error("reported results length is invalid for lease");
  }
  if (d.close_lease && (lease.status !== "active" || d.results.length !== lease.attempts.length)) {
    throw new Error(`cannot close lease ${d.lease_id} with status ${lease.status} and ${d.results.length} results`);
  }
  if (!d.close_lease && lease.status === "active") {
    throw new Error(`reported event for active lease ${d.lease_id} must set close_lease`);
  }

  const leaseAttempts = new Map<number, number>();
  for (let i = 0; i < lease.attempts.length; i++) {
    const a = lease.attempts[i]!;
    leaseAttempts.set(a.test_index, a.generation);
  }

  for (let i = 0; i < d.results.length; i++) {
    const res = d.results[i]!;
    if (i > 0 && d.results[i - 1]!.test_index >= res.test_index) {
      throw new Error("reported results must be unique and sorted by test_index");
    }
    const wantGen = leaseAttempts.get(res.test_index);
    if (wantGen === undefined || res.generation !== wantGen) {
      throw new Error(`reported result ${i} does not match lease attempt`);
    }
    const valErr = validateReportValues(res.outcome, res.timed, res.duration_ms);
    if (valErr) {
      throw valErr;
    }
    const t = s.tests[res.test_index]!;
    if (res.generation < 1 || res.generation > t.attempts.length) {
      throw new Error(`reported generation ${res.generation} out of range`);
    }
    if (res.disposition === "accepted") {
      if (!d.close_lease || t.status !== "leased" || t.activeLeaseID !== d.lease_id || t.generation !== res.generation) {
        throw new Error(`cannot accept result for test ${res.test_index}`);
      }
    } else if (res.disposition === "stale" || res.disposition === "conflict") {
      if (d.close_lease) {
        throw new Error(`closing reported event cannot contain ${res.disposition} result`);
      }
      if (t.history.length >= MaxTestHistory) {
        throw new Error(`test ${res.test_index} history is already full`);
      }
      const rec = {
        attempt_id: formatAttemptID(res.test_index, res.generation),
        generation: res.generation,
        lease_id: d.lease_id,
        runner_id: d.runner_id,
        outcome: res.outcome,
        timed: res.timed,
        duration_ms: res.duration_ms,
        disposition: res.disposition,
      };
      if (hasHistoryRecord(t.history, rec)) {
        throw new Error(`duplicate ${res.disposition} history record for test ${res.test_index}`);
      }
    } else {
      throw new Error(`invalid persisted disposition ${JSON.stringify(res.disposition)}`);
    }
  }

  const wasRunning = stateVerdict(s) === "running";
  s.lastSeenMS = Math.max(s.lastSeenMS, atMS);
  const r = s.runners.get(d.runner_id);

  for (let i = 0; i < d.results.length; i++) {
    const res = d.results[i]!;
    const t = s.tests[res.test_index]!;
    if (res.disposition === "accepted") {
      const att = t.attempts[res.generation - 1]!;
      att.status = outcomeToAttemptStatus(res.outcome);
      att.timed = res.timed;
      att.duration_ms = res.duration_ms;
      att.finished_at_ms = atMS;

      t.activeLeaseID = 0;
      s.counts.leased--;
      if (r) {
        r.accepted++;
      }
      switch (res.outcome) {
        case "passed":
          t.status = "passed";
          t.timed = res.timed;
          t.durationMS = res.duration_ms;
          s.counts.passed++;
          break;
        case "failed":
          t.status = "failed";
          s.counts.failed++;
          break;
        case "skipped":
          t.status = "skipped";
          s.counts.skipped++;
          break;
        case "error":
          if (t.generation >= s.config.max_attempts) {
            t.status = "unresolved";
            s.counts.unresolved++;
          } else {
            t.status = "pending";
            s.counts.pending++;
          }
          break;
      }
    } else {
      t.history.push({
        attempt_id: formatAttemptID(res.test_index, res.generation),
        generation: res.generation,
        lease_id: d.lease_id,
        runner_id: d.runner_id,
        outcome: res.outcome,
        timed: res.timed,
        duration_ms: res.duration_ms,
        disposition: res.disposition,
        at_ms: atMS,
      });
      if (t.history.length === MaxTestHistory) {
        t.historyTruncated = true;
      }
    }
  }

  if (d.close_lease) {
    lease.status = "reported";
    lease.finished_at_ms = atMS;
    removeActiveLease(s, d.lease_id);
    if (r && r.activeLeaseID === d.lease_id) {
      r.activeLeaseID = 0;
    }
  }

  if (wasRunning && stateVerdict(s) !== "running") {
    s.finishedAtMS = atMS;
  }
}

function applyExpired(s: State, atMS: number, d: ExpiredData | undefined): void {
  if (!s.initialized) {
    throw new Error("expired event before initialized");
  }
  if (!d) {
    throw new Error("expired event missing payload");
  }
  if (d.lease_id < 1 || d.lease_id > s.leases.length) {
    throw new Error(`expired unknown lease_id ${d.lease_id}`);
  }
  const lease = s.leases[d.lease_id - 1]!;
  if (lease.status !== "active") {
    throw new Error(`expired lease ${d.lease_id} is ${lease.status}, not active`);
  }

  const wasRunning = stateVerdict(s) === "running";
  s.lastSeenMS = Math.max(s.lastSeenMS, atMS);
  lease.status = "expired";
  lease.finished_at_ms = atMS;
  removeActiveLease(s, d.lease_id);

  const r = s.runners.get(lease.runner_id);
  if (r && r.activeLeaseID === d.lease_id) {
    r.activeLeaseID = 0;
  }

  for (let i = 0; i < lease.attempts.length; i++) {
    const a = lease.attempts[i]!;
    const t = s.tests[a.test_index]!;
    if (t.status !== "leased" || t.activeLeaseID !== d.lease_id) {
      continue;
    }
    const att = t.attempts[a.generation - 1]!;
    att.status = "expired";
    att.finished_at_ms = atMS;

    t.activeLeaseID = 0;
    s.counts.leased--;
    if (t.generation >= s.config.max_attempts) {
      t.status = "unresolved";
      s.counts.unresolved++;
    } else {
      t.status = "pending";
      s.counts.pending++;
    }
  }

  if (wasRunning && stateVerdict(s) !== "running") {
    s.finishedAtMS = atMS;
  }
}

function applyFinalized(s: State, atMS: number, d: FinalizedData | undefined): void {
  if (!s.initialized) {
    throw new Error("finalized event before initialized");
  }
  if (!d || d.reason !== "deadline") {
    throw new Error('finalized event requires reason "deadline"');
  }
  if (stateVerdict(s) !== "running") {
    throw new Error("finalized event after build finished");
  }

  s.lastSeenMS = Math.max(s.lastSeenMS, atMS);
  for (let i = 0; i < s.activeLeases.length; i++) {
    const leaseID = s.activeLeases[i]!;
    const lease = s.leases[leaseID - 1]!;
    lease.status = "expired";
    lease.finished_at_ms = atMS;
    const r = s.runners.get(lease.runner_id);
    if (r && r.activeLeaseID === leaseID) {
      r.activeLeaseID = 0;
    }
    for (let j = 0; j < lease.attempts.length; j++) {
      const a = lease.attempts[j]!;
      const t = s.tests[a.test_index]!;
      if (t.status === "leased" && t.activeLeaseID === leaseID) {
        const att = t.attempts[a.generation - 1]!;
        att.status = "expired";
        att.finished_at_ms = atMS;
      }
    }
  }
  s.activeLeases.length = 0;

  for (let i = 0; i < s.tests.length; i++) {
    const t = s.tests[i]!;
    if (t.status === "pending") {
      t.status = "unresolved";
      s.counts.pending--;
      s.counts.unresolved++;
    } else if (t.status === "leased") {
      t.status = "unresolved";
      t.activeLeaseID = 0;
      s.counts.leased--;
      s.counts.unresolved++;
    }
  }
  s.finishedAtMS = atMS;
}

export function extendLease(s: State, leaseID: number, deadlineMS: number): void {
  if (leaseID < 1 || leaseID > s.leases.length) {
    return;
  }
  const lease = s.leases[leaseID - 1]!;
  if (lease.status !== "active") {
    return;
  }
  if (deadlineMS > lease.deadline_ms) {
    lease.deadline_ms = deadlineMS;
  }
}

function ensureRunner(s: State, runnerID: string, machineIndex: number | null, atMS: number): RunnerState {
  const existing = s.runners.get(runnerID);
  if (existing) {
    return existing;
  }
  const r: RunnerState = {
    runnerID,
    machineIndex,
    firstSeenMS: atMS,
    leases: 0,
    accepted: 0,
    activeLeaseID: 0,
    lastRequestID: "",
    lastLeaseID: 0,
  };
  s.runners.set(runnerID, r);
  s.runnerOrder.push(runnerID);
  s.runnerOrder.sort((a, b) => {
    const ra = s.runners.get(a)!;
    const rb = s.runners.get(b)!;
    if (ra.firstSeenMS !== rb.firstSeenMS) {
      return ra.firstSeenMS - rb.firstSeenMS;
    }
    return compareUTF8(a, b);
  });
  return r;
}

function removeActiveLease(s: State, leaseID: number): void {
  let lo = 0;
  let hi = s.activeLeases.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const v = s.activeLeases[mid]!;
    if (v === leaseID) {
      s.activeLeases.splice(mid, 1);
      return;
    }
    if (v < leaseID) {
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
}
