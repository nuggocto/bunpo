import {
  type QueueEvent,
  type ReportedAttempt,
  hasHistoryRecord,
  hasInitConflict,
  outcomeToAttemptStatus,
  validateReportValues,
} from "./events.ts";
import {
  type AttemptRecord,
  type Config,
  type Counts,
  type Disposition,
  type HistoryRecord,
  type InitConflict,
  type Lease,
  type LeaseAttempt,
  type LeaseResponseStatus,
  type Outcome,
  type State,
  type TestStatus,
  type Verdict,
  DefaultTestsPageLimit,
  ErrCodeBuildNotFound,
  ErrCodeInitConflict,
  ErrCodeLeaseHeld,
  ErrCodeRunnerMismatch,
  ErrCodeStaleRequest,
  ErrCodeTooManyRunners,
  ErrCodeUnknownLease,
  MaxInitConflicts,
  MaxStatusProblems,
  MaxStatusRunners,
  MaxStatusSlowest,
  MaxTestHistory,
  MaxTestsPageLimit,
  MaxWaitMS,
  MinWaitMS,
  QueueError,
  computeVerdict,
  configDigest,
  effectiveLimits,
  errInvalid,
  formatAttemptID,
  hasReportedOutcome,
  stateVerdict,
  validBuildID,
  validRequestID,
  validRunnerID,
  validateConfig,
  validateMachineIndex,
  validateTestList,
} from "./types.ts";

export interface InitCommand {
  build_id: string;
  runner_id: string;
  machine_index: number | null;
  config: Config;
  tests: string[];
  predictions_ms: (number | null)[];
}

export interface InitResponse {
  build_id: string;
  created: boolean;
  digest: string;
  verdict: Verdict;
  test_count: number;
  deadline_ms: number;
}

export interface LeaseCommand {
  runner_id: string;
  machine_index: number | null;
  request_id: string;
}

export interface LeasedAttemptView {
  attempt_id: string;
  test_index: number;
  generation: number;
  test: string;
}

export interface LeaseResponse {
  status: LeaseResponseStatus;
  lease_id: number;
  deadline_ms: number;
  attempts?: LeasedAttemptView[];
  retry_after_ms: number;
  verdict?: Verdict;
}

export interface HeartbeatCommand {
  runner_id: string;
  lease_id: number;
}

export interface HeartbeatResponse {
  held: boolean;
  deadline_ms: number;
  verdict: Verdict;
}

export interface ReportResult {
  attempt_id: string;
  test_index: number;
  generation: number;
  outcome: Outcome;
  timed: boolean;
  duration_ms: number;
}

export interface ReportCommand {
  runner_id: string;
  lease_id: number;
  results: ReportResult[];
}

export interface ReportDisposition {
  attempt_id: string;
  test_index: number;
  generation: number;
  disposition: Disposition;
}

export interface ReportResponse {
  verdict: Verdict;
  results: ReportDisposition[];
}

export interface RunnerSummary {
  runner_id: string;
  machine_index: number | null;
  first_seen_ms: number;
  leases: number;
  accepted: number;
  active_lease_id: number;
}

export interface SlowestTest {
  test_index: number;
  test: string;
  duration_ms: number;
}

export interface ProblemTest {
  test_index: number;
  test: string;
  state: TestStatus;
  generation: number;
}

export interface StatusResponse {
  build_id: string;
  digest: string;
  verdict: Verdict;
  created_at_ms: number;
  deadline_ms: number;
  finished_at_ms: number | null;
  last_seen_ms: number;
  config: Config;
  counts: Counts;
  conflicts_truncated: boolean;
  conflicts: InitConflict[];
  runner_count: number;
  runners: RunnerSummary[];
  slowest: SlowestTest[];
  problem_count: number;
  problems: ProblemTest[];
}

export interface ListTestsCommand {
  cursor: number;
  limit: number;
  states?: TestStatus[];
  retried?: boolean;
}

export interface TestView {
  test_index: number;
  test: string;
  state: TestStatus;
  generation: number;
  predicted_ms: number;
  timed: boolean;
  duration_ms: number;
  attempts: AttemptRecord[];
  history: HistoryRecord[];
  history_truncated: boolean;
}

export interface ListTestsResponse {
  build_id: string;
  verdict: Verdict;
  tests: TestView[];
  next_cursor: number | null;
}

export type DecideOutcome<T> =
  | { events: QueueEvent[]; response: T; error: null }
  | { events: QueueEvent[]; response: null; error: QueueError };

interface ExpiryOverlay {
  nowMS: number;
  finalized: boolean;
  expiredLeases: Set<number> | null;
  expiredTests: Map<number, TestStatus> | null;
  counts: Counts;
  events: QueueEvent[];
}

function evaluateExpiry(s: State, nowMS: number): ExpiryOverlay {
  const now = Math.max(nowMS, s.lastSeenMS);
  const ov: ExpiryOverlay = {
    nowMS: now,
    finalized: false,
    expiredLeases: null,
    expiredTests: null,
    counts: { ...s.counts },
    events: [],
  };
  if (!s.initialized || stateVerdict(s) !== "running") {
    return ov;
  }

  if (now >= s.deadlineMS) {
    ov.finalized = true;
    ov.counts.unresolved += ov.counts.pending + ov.counts.leased;
    ov.counts.pending = 0;
    ov.counts.leased = 0;
    ov.events.push({
      type: "finalized",
      at_ms: now,
      finalized: { reason: "deadline" },
    });
    return ov;
  }

  for (let i = 0; i < s.activeLeases.length; i++) {
    const leaseID = s.activeLeases[i]!;
    const lease = s.leases[leaseID - 1]!;
    if (lease.deadline_ms > now) {
      continue;
    }
    if (!ov.expiredLeases) {
      ov.expiredLeases = new Set();
      ov.expiredTests = new Map();
    }
    ov.expiredLeases.add(leaseID);
    for (let j = 0; j < lease.attempts.length; j++) {
      const a = lease.attempts[j]!;
      const t = s.tests[a.test_index]!;
      if (t.status !== "leased" || t.activeLeaseID !== leaseID) {
        continue;
      }
      ov.counts.leased--;
      if (t.generation >= s.config.max_attempts) {
        ov.expiredTests!.set(a.test_index, "unresolved");
        ov.counts.unresolved++;
      } else {
        ov.expiredTests!.set(a.test_index, "pending");
        ov.counts.pending++;
      }
    }
    ov.events.push({
      type: "expired",
      at_ms: now,
      expired: { lease_id: leaseID },
    });
  }
  return ov;
}

function overlayTestStatus(ov: ExpiryOverlay, s: State, idx: number): TestStatus {
  if (ov.finalized) {
    const st = s.tests[idx]!.status;
    if (st === "pending" || st === "leased") {
      return "unresolved";
    }
    return st;
  }
  const exp = ov.expiredTests?.get(idx);
  if (exp !== undefined) {
    return exp;
  }
  return s.tests[idx]!.status;
}

function overlayLeaseActive(ov: ExpiryOverlay, s: State, leaseID: number): boolean {
  if (leaseID < 1 || leaseID > s.leases.length) {
    return false;
  }
  if (s.leases[leaseID - 1]!.status !== "active") {
    return false;
  }
  if (ov.finalized || ov.expiredLeases?.has(leaseID)) {
    return false;
  }
  return true;
}

function overlayVerdict(ov: ExpiryOverlay, s: State): Verdict {
  return computeVerdict(ov.counts, s.conflicts.length > 0);
}

export function decideInit(s: State, nowMS: number, cmd: InitCommand): DecideOutcome<InitResponse> {
  if (!Number.isInteger(nowMS) || nowMS < 0) {
    return { events: [], response: null, error: errInvalid("now_ms must not be negative") };
  }
  if (!validBuildID(cmd.build_id)) {
    return { events: [], response: null, error: errInvalid(`invalid build_id ${JSON.stringify(cmd.build_id)}`) };
  }
  if (!validRunnerID(cmd.runner_id)) {
    return { events: [], response: null, error: errInvalid(`invalid runner_id ${JSON.stringify(cmd.runner_id)}`) };
  }
  const miErr = validateMachineIndex(cmd.machine_index);
  if (miErr) {
    return { events: [], response: null, error: miErr };
  }
  const cfgErr = validateConfig(cmd.config, s.limits);
  if (cfgErr) {
    return { events: [], response: null, error: cfgErr };
  }
  const lim = effectiveLimits(s.limits);
  const testsErr = validateTestList(cmd.tests, cmd.predictions_ms, lim.maxTests);
  if (testsErr) {
    return { events: [], response: null, error: testsErr };
  }

  const digest = configDigest(cmd.config, cmd.tests);

  if (!s.initialized) {
    const deadlineMS = nowMS + cmd.config.build_timeout_ms;
    const ev: QueueEvent = {
      type: "initialized",
      at_ms: nowMS,
      initialized: {
        build_id: cmd.build_id,
        digest,
        config: { ...cmd.config },
        deadline_ms: deadlineMS,
        tests: cmd.tests.slice(),
        predictions_ms: cmd.predictions_ms.slice(),
      },
    };
    return {
      events: [ev],
      response: {
        build_id: cmd.build_id,
        created: true,
        digest,
        verdict: "running",
        test_count: cmd.tests.length,
        deadline_ms: deadlineMS,
      },
      error: null,
    };
  }

  if (s.buildID !== cmd.build_id) {
    return {
      events: [],
      response: null,
      error: errInvalid(`state belongs to build ${JSON.stringify(s.buildID)}, not ${JSON.stringify(cmd.build_id)}`),
    };
  }

  if (digest === s.digest) {
    return {
      events: [],
      response: {
        build_id: s.buildID,
        created: false,
        digest: s.digest,
        verdict: stateVerdict(s),
        test_count: s.tests.length,
        deadline_ms: s.deadlineMS,
      },
      error: null,
    };
  }

  const reason = describeInitDiff(s, cmd);
  const err = new QueueError(409, ErrCodeInitConflict, reason);
  if (s.conflicts.length >= MaxInitConflicts || hasInitConflict(s.conflicts, cmd.runner_id, digest)) {
    return { events: [], response: null, error: err };
  }

  const now = Math.max(nowMS, s.lastSeenMS);
  const ev: QueueEvent = {
    type: "conflicted",
    at_ms: now,
    conflicted: {
      runner_id: cmd.runner_id,
      machine_index: cmd.machine_index,
      got_digest: digest,
      reason,
    },
  };
  return { events: [ev], response: null, error: err };
}

function describeInitDiff(s: State, cmd: InitCommand): string {
  const sc = s.config;
  const cc = cmd.config;
  if (
    sc.machines !== cc.machines ||
    sc.lease_ttl_ms !== cc.lease_ttl_ms ||
    sc.build_timeout_ms !== cc.build_timeout_ms ||
    sc.max_attempts !== cc.max_attempts ||
    sc.max_batch !== cc.max_batch
  ) {
    return `config differs from initialized build (got machines=${cc.machines} ttl=${cc.lease_ttl_ms} timeout=${cc.build_timeout_ms} attempts=${cc.max_attempts} batch=${cc.max_batch}, want machines=${sc.machines} ttl=${sc.lease_ttl_ms} timeout=${sc.build_timeout_ms} attempts=${sc.max_attempts} batch=${sc.max_batch})`;
  }
  if (cmd.tests.length !== s.tests.length) {
    return `test list differs: got ${cmd.tests.length} tests, initialized build has ${s.tests.length}`;
  }
  for (let i = 0; i < cmd.tests.length; i++) {
    const p = cmd.tests[i]!;
    if (p !== s.tests[i]!.path) {
      return `test list differs at index ${i}: got ${JSON.stringify(p)}, initialized build has ${JSON.stringify(s.tests[i]!.path)}`;
    }
  }
  return "init digest differs from initialized build";
}

export function decideLease(s: State, nowMS: number, cmd: LeaseCommand): DecideOutcome<LeaseResponse> {
  if (!s.initialized) {
    return {
      events: [],
      response: null,
      error: new QueueError(404, ErrCodeBuildNotFound, "build not initialized"),
    };
  }
  if (!validRunnerID(cmd.runner_id)) {
    return { events: [], response: null, error: errInvalid(`invalid runner_id ${JSON.stringify(cmd.runner_id)}`) };
  }
  if (!validRequestID(cmd.request_id)) {
    return { events: [], response: null, error: errInvalid(`invalid request_id ${JSON.stringify(cmd.request_id)}`) };
  }
  const miErr = validateMachineIndex(cmd.machine_index);
  if (miErr) {
    return { events: [], response: null, error: miErr };
  }

  const ov = evaluateExpiry(s, nowMS);
  const now = ov.nowMS;

  const r = s.runners.get(cmd.runner_id);
  if (r) {
    if (r.lastRequestID === cmd.request_id && r.lastLeaseID > 0) {
      if (overlayLeaseActive(ov, s, r.lastLeaseID)) {
        const lease = s.leases[r.lastLeaseID - 1]!;
        const newDeadline = Math.max(lease.deadline_ms, now + s.config.lease_ttl_ms);
        return {
          events: ov.events,
          response: leaseResponseFromLease(s, lease, newDeadline),
          error: null,
        };
      }
      return {
        events: ov.events,
        response: null,
        error: new QueueError(
          409,
          ErrCodeStaleRequest,
          `request_id ${JSON.stringify(cmd.request_id)} refers to lease ${r.lastLeaseID}, which is no longer active`,
        ),
      };
    }
    if (r.activeLeaseID > 0 && overlayLeaseActive(ov, s, r.activeLeaseID)) {
      return {
        events: ov.events,
        response: null,
        error: new QueueError(
          409,
          ErrCodeLeaseHeld,
          `runner ${JSON.stringify(cmd.runner_id)} already holds active lease ${r.activeLeaseID}`,
        ),
      };
    }
  }

  const v = overlayVerdict(ov, s);
  if (v !== "running") {
    return {
      events: ov.events,
      response: { status: "done", lease_id: 0, deadline_ms: 0, retry_after_ms: 0, verdict: v },
      error: null,
    };
  }

  const lim = effectiveLimits(s.limits);
  if (!r && s.runners.size >= lim.maxRunners) {
    return {
      events: ov.events,
      response: null,
      error: new QueueError(429, ErrCodeTooManyRunners, `build already has ${s.runners.size} distinct runners`),
    };
  }

  if (ov.counts.pending === 0) {
    const retryAfter = computeWaitMS(s, ov, now);
    return {
      events: ov.events,
      response: { status: "wait", lease_id: 0, deadline_ms: 0, retry_after_ms: retryAfter },
      error: null,
    };
  }

  const chosen = selectBatch(s, ov);
  chosen.sort((a, b) => a - b);

  const leaseID = s.leases.length + 1;
  const deadlineMS = now + s.config.lease_ttl_ms;
  const attempts = new Array<LeaseAttempt>(chosen.length);
  const views = new Array<LeasedAttemptView>(chosen.length);
  for (let i = 0; i < chosen.length; i++) {
    const idx = chosen[i]!;
    const gen = s.tests[idx]!.generation + 1;
    attempts[i] = { test_index: idx, generation: gen };
    views[i] = {
      attempt_id: formatAttemptID(idx, gen),
      test_index: idx,
      generation: gen,
      test: s.tests[idx]!.path,
    };
  }

  const events = ov.events.slice();
  events.push({
    type: "leased",
    at_ms: now,
    leased: {
      lease_id: leaseID,
      runner_id: cmd.runner_id,
      machine_index: cmd.machine_index,
      request_id: cmd.request_id,
      deadline_ms: deadlineMS,
      attempts,
    },
  });

  return {
    events,
    response: {
      status: "leased",
      lease_id: leaseID,
      deadline_ms: deadlineMS,
      attempts: views,
      retry_after_ms: 0,
    },
    error: null,
  };
}

function leaseResponseFromLease(s: State, lease: Lease, deadlineMS: number): LeaseResponse {
  const views = new Array<LeasedAttemptView>(lease.attempts.length);
  for (let i = 0; i < lease.attempts.length; i++) {
    const a = lease.attempts[i]!;
    views[i] = {
      attempt_id: formatAttemptID(a.test_index, a.generation),
      test_index: a.test_index,
      generation: a.generation,
      test: s.tests[a.test_index]!.path,
    };
  }
  return {
    status: "leased",
    lease_id: lease.lease_id,
    deadline_ms: deadlineMS,
    attempts: views,
    retry_after_ms: 0,
  };
}

function selectBatch(s: State, ov: ExpiryOverlay): number[] {
  const firstPending: number[] = [];
  let firstRetry = -1;
  let remainingMS = 0;
  let totalPending = 0;

  for (let i = 0; i < s.dealOrder.length; i++) {
    const idx = s.dealOrder[i]!;
    if (overlayTestStatus(ov, s, idx) !== "pending") {
      continue;
    }
    totalPending++;
    const t = s.tests[idx]!;
    remainingMS += t.effectiveMS;
    if (t.generation === 0) {
      firstPending.push(idx);
    } else if (firstRetry === -1) {
      firstRetry = idx;
    }
  }

  if (firstPending.length === 0) {
    return [firstRetry];
  }

  const denom = 2 * s.config.machines;
  const maxBatch = s.config.max_batch;

  if (remainingMS === 0) {
    let targetCount = Math.floor((totalPending + denom - 1) / denom);
    targetCount = Math.max(1, Math.min(targetCount, maxBatch, firstPending.length));
    return firstPending.slice(0, targetCount);
  }

  const targetMS = Math.floor((remainingMS + denom - 1) / denom);
  const batch: number[] = [];
  let batchMS = 0;
  for (let i = 0; i < firstPending.length; i++) {
    const idx = firstPending[i]!;
    batch.push(idx);
    batchMS += s.tests[idx]!.effectiveMS;
    if (batchMS >= targetMS || batch.length >= maxBatch) {
      break;
    }
  }
  return batch;
}

function computeWaitMS(s: State, ov: ExpiryOverlay, now: number): number {
  let earliestDeadline = 0;
  let latestExpectedEnd = 0;
  let found = false;
  for (let i = 0; i < s.activeLeases.length; i++) {
    const id = s.activeLeases[i]!;
    if (!overlayLeaseActive(ov, s, id)) {
      continue;
    }
    const l = s.leases[id - 1]!;
    let expectedEnd = l.leased_at_ms;
    for (let j = 0; j < l.attempts.length; j++) {
      expectedEnd += s.tests[l.attempts[j]!.test_index]!.effectiveMS;
    }
    if (!found || l.deadline_ms < earliestDeadline) {
      earliestDeadline = l.deadline_ms;
    }
    if (!found || expectedEnd > latestExpectedEnd) {
      latestExpectedEnd = expectedEnd;
    }
    found = true;
  }

  let maxWait = Math.min(Math.floor(s.config.lease_ttl_ms / 2), MaxWaitMS);
  if (maxWait < 1) {
    maxWait = 1;
  }
  const minWait = Math.min(MinWaitMS, maxWait);
  if (!found) {
    return minWait;
  }
  let wait: number;
  if (now < latestExpectedEnd) {
    wait = latestExpectedEnd - now;
  } else {
    wait = Math.floor((now - latestExpectedEnd) / 4);
  }
  wait = Math.min(wait, earliestDeadline - now);
  return Math.max(minWait, Math.min(wait, maxWait));
}

export function decideHeartbeat(s: State, nowMS: number, cmd: HeartbeatCommand): DecideOutcome<HeartbeatResponse> {
  if (!s.initialized) {
    return {
      events: [],
      response: null,
      error: new QueueError(404, ErrCodeBuildNotFound, "build not initialized"),
    };
  }
  if (!validRunnerID(cmd.runner_id)) {
    return { events: [], response: null, error: errInvalid(`invalid runner_id ${JSON.stringify(cmd.runner_id)}`) };
  }
  if (cmd.lease_id < 1 || cmd.lease_id > s.leases.length) {
    return {
      events: [],
      response: null,
      error: new QueueError(400, ErrCodeUnknownLease, `unknown lease_id ${cmd.lease_id}`),
    };
  }
  const lease = s.leases[cmd.lease_id - 1]!;
  if (lease.runner_id !== cmd.runner_id) {
    return {
      events: [],
      response: null,
      error: new QueueError(400, ErrCodeRunnerMismatch, `lease ${cmd.lease_id} belongs to another runner`),
    };
  }

  const now = Math.max(nowMS, s.lastSeenMS);
  if (stateVerdict(s) === "running" && now < s.deadlineMS && lease.status === "active" && lease.deadline_ms > now) {
    const newDeadline = Math.max(lease.deadline_ms, now + s.config.lease_ttl_ms);
    return {
      events: [],
      response: {
        held: true,
        deadline_ms: newDeadline,
        verdict: "running",
      },
      error: null,
    };
  }

  const ov = evaluateExpiry(s, nowMS);
  return {
    events: ov.events,
    response: {
      held: false,
      deadline_ms: 0,
      verdict: overlayVerdict(ov, s),
    },
    error: null,
  };
}

export function decideReport(s: State, nowMS: number, cmd: ReportCommand): DecideOutcome<ReportResponse> {
  if (!s.initialized) {
    return {
      events: [],
      response: null,
      error: new QueueError(404, ErrCodeBuildNotFound, "build not initialized"),
    };
  }
  if (!validRunnerID(cmd.runner_id)) {
    return { events: [], response: null, error: errInvalid(`invalid runner_id ${JSON.stringify(cmd.runner_id)}`) };
  }
  if (cmd.lease_id < 1 || cmd.lease_id > s.leases.length) {
    return {
      events: [],
      response: null,
      error: new QueueError(400, ErrCodeUnknownLease, `unknown lease_id ${cmd.lease_id}`),
    };
  }
  const lease = s.leases[cmd.lease_id - 1]!;
  if (lease.runner_id !== cmd.runner_id) {
    return {
      events: [],
      response: null,
      error: new QueueError(400, ErrCodeRunnerMismatch, `lease ${cmd.lease_id} belongs to another runner`),
    };
  }
  if (cmd.results.length !== lease.attempts.length) {
    return {
      events: [],
      response: null,
      error: errInvalid(
        `report has ${cmd.results.length} results, lease ${cmd.lease_id} has ${lease.attempts.length} attempts`,
      ),
    };
  }

  const byIndex = new Map<number, ReportResult>();
  for (let i = 0; i < cmd.results.length; i++) {
    const res = cmd.results[i]!;
    if (res.test_index < 0 || res.test_index >= s.tests.length) {
      return { events: [], response: null, error: errInvalid(`test_index ${res.test_index} out of range`) };
    }
    const t = s.tests[res.test_index]!;
    if (res.generation < 1 || res.generation > t.generation) {
      return {
        events: [],
        response: null,
        error: errInvalid(`test ${res.test_index} generation ${res.generation} outside 1..${t.generation}`),
      };
    }
    const wantAttemptID = formatAttemptID(res.test_index, res.generation);
    if (res.attempt_id !== "" && res.attempt_id !== wantAttemptID) {
      return {
        events: [],
        response: null,
        error: errInvalid(
          `attempt_id ${JSON.stringify(res.attempt_id)} does not match ${JSON.stringify(wantAttemptID)}`,
        ),
      };
    }
    const valErr = validateReportValues(res.outcome, res.timed, res.duration_ms);
    if (valErr) {
      return { events: [], response: null, error: valErr };
    }
    if (byIndex.has(res.test_index)) {
      return { events: [], response: null, error: errInvalid(`duplicate result for test_index ${res.test_index}`) };
    }
    byIndex.set(res.test_index, res);
  }

  const ordered = new Array<ReportResult>(lease.attempts.length);
  for (let i = 0; i < lease.attempts.length; i++) {
    const want = lease.attempts[i]!;
    const got = byIndex.get(want.test_index);
    if (!got || got.generation !== want.generation) {
      return {
        events: [],
        response: null,
        error: errInvalid(
          `report must contain attempt ${formatAttemptID(want.test_index, want.generation)} of lease ${cmd.lease_id}`,
        ),
      };
    }
    ordered[i] = got;
  }

  const ov = evaluateExpiry(s, nowMS);
  const now = ov.nowMS;
  const leaseActive = overlayLeaseActive(ov, s, cmd.lease_id);

  const stored: ReportedAttempt[] = [];
  const disps = new Array<ReportDisposition>(ordered.length);
  const counts: Counts = { ...ov.counts };
  let allAccepted = leaseActive;

  for (let i = 0; i < ordered.length; i++) {
    const res = ordered[i]!;
    const t = s.tests[res.test_index]!;
    const att = t.attempts[res.generation - 1]!;
    let disp: Disposition;
    let persist = false;

    if (hasReportedOutcome(att.status)) {
      if (
        att.status === outcomeToAttemptStatus(res.outcome) &&
        att.timed === res.timed &&
        att.duration_ms === res.duration_ms
      ) {
        disp = "duplicate";
      } else {
        disp = "conflict";
        const rec = {
          attempt_id: formatAttemptID(res.test_index, res.generation),
          generation: res.generation,
          lease_id: cmd.lease_id,
          runner_id: cmd.runner_id,
          outcome: res.outcome,
          timed: res.timed,
          duration_ms: res.duration_ms,
          disposition: disp,
        };
        if (t.history.length < MaxTestHistory && !hasHistoryRecord(t.history, rec)) {
          persist = true;
        }
      }
      allAccepted = false;
    } else if (
      leaseActive &&
      res.generation === t.generation &&
      overlayTestStatus(ov, s, res.test_index) === "leased" &&
      t.activeLeaseID === cmd.lease_id
    ) {
      disp = "accepted";
      persist = true;
      counts.leased--;
      switch (res.outcome) {
        case "passed":
          counts.passed++;
          break;
        case "failed":
          counts.failed++;
          break;
        case "skipped":
          counts.skipped++;
          break;
        case "error":
          if (res.generation >= s.config.max_attempts) {
            counts.unresolved++;
          } else {
            counts.pending++;
          }
          break;
      }
    } else {
      disp = "stale";
      const rec = {
        attempt_id: formatAttemptID(res.test_index, res.generation),
        generation: res.generation,
        lease_id: cmd.lease_id,
        runner_id: cmd.runner_id,
        outcome: res.outcome,
        timed: res.timed,
        duration_ms: res.duration_ms,
        disposition: disp,
      };
      if (t.history.length < MaxTestHistory && !hasHistoryRecord(t.history, rec)) {
        persist = true;
      }
      allAccepted = false;
    }

    if (persist) {
      stored.push({
        test_index: res.test_index,
        generation: res.generation,
        outcome: res.outcome,
        timed: res.timed,
        duration_ms: res.duration_ms,
        disposition: disp,
      });
    }
    disps[i] = {
      attempt_id: formatAttemptID(res.test_index, res.generation),
      test_index: res.test_index,
      generation: res.generation,
      disposition: disp,
    };
  }

  const events = ov.events.slice();
  if (stored.length > 0) {
    events.push({
      type: "reported",
      at_ms: now,
      reported: {
        lease_id: cmd.lease_id,
        runner_id: cmd.runner_id,
        close_lease: allAccepted,
        results: stored,
      },
    });
  }

  return {
    events,
    response: {
      verdict: computeVerdict(counts, s.conflicts.length > 0),
      results: disps,
    },
    error: null,
  };
}

export function decideStatus(s: State, nowMS: number): DecideOutcome<StatusResponse> {
  if (!s.initialized) {
    return {
      events: [],
      response: null,
      error: new QueueError(404, ErrCodeBuildNotFound, "build not initialized"),
    };
  }

  const ov = evaluateExpiry(s, nowMS);
  const verdictChanged = stateVerdict(s) === "running" && overlayVerdict(ov, s) !== "running";

  const events = verdictChanged ? ov.events : [];
  const useOverlay = verdictChanged;

  const counts = useOverlay ? { ...ov.counts } : { ...s.counts };
  const verdict = useOverlay ? overlayVerdict(ov, s) : stateVerdict(s);
  const lastSeen = useOverlay ? ov.nowMS : s.lastSeenMS;
  const finishedAt = useOverlay ? ov.nowMS : s.finishedAtMS > 0 ? s.finishedAtMS : null;

  const conflicts = s.conflicts.map((c) => ({ ...c }));

  const runnerCap = Math.min(s.runnerOrder.length, MaxStatusRunners);
  const runners = new Array<RunnerSummary>(runnerCap);
  for (let i = 0; i < runnerCap; i++) {
    const r = s.runners.get(s.runnerOrder[i]!)!;
    let activeID = r.activeLeaseID;
    if (useOverlay && activeID > 0 && !overlayLeaseActive(ov, s, activeID)) {
      activeID = 0;
    }
    runners[i] = {
      runner_id: r.runnerID,
      machine_index: r.machineIndex,
      first_seen_ms: r.firstSeenMS,
      leases: r.leases,
      accepted: r.accepted,
      active_lease_id: activeID,
    };
  }

  const slowest: SlowestTest[] = [];
  const failedProblems: ProblemTest[] = [];
  const unresolvedProblems: ProblemTest[] = [];
  for (let i = 0; i < s.tests.length; i++) {
    const t = s.tests[i]!;
    const st = useOverlay ? overlayTestStatus(ov, s, i) : t.status;
    if (st === "passed" && t.timed) {
      // Keep only the top MaxStatusSlowest (20) items sorted by duration_ms desc,
      // test_index asc, avoiding O(N log N) allocations across all passed tests.
      if (slowest.length < MaxStatusSlowest) {
        insertSlowest(slowest, { test_index: t.index, test: t.path, duration_ms: t.durationMS });
      } else {
        const last = slowest[MaxStatusSlowest - 1]!;
        if (t.durationMS > last.duration_ms || (t.durationMS === last.duration_ms && t.index < last.test_index)) {
          slowest.pop();
          insertSlowest(slowest, { test_index: t.index, test: t.path, duration_ms: t.durationMS });
        }
      }
    }
    if (st === "failed") {
      failedProblems.push({
        test_index: t.index,
        test: t.path,
        state: st,
        generation: t.generation,
      });
    } else if (st === "unresolved") {
      unresolvedProblems.push({
        test_index: t.index,
        test: t.path,
        state: st,
        generation: t.generation,
      });
    }
  }

  const problemCount = failedProblems.length + unresolvedProblems.length;
  const problems: ProblemTest[] = [];
  for (let i = 0; i < failedProblems.length && problems.length < MaxStatusProblems; i++) {
    problems.push(failedProblems[i]!);
  }
  for (let i = 0; i < unresolvedProblems.length && problems.length < MaxStatusProblems; i++) {
    problems.push(unresolvedProblems[i]!);
  }

  return {
    events,
    response: {
      build_id: s.buildID,
      digest: s.digest,
      verdict,
      created_at_ms: s.createdAtMS,
      deadline_ms: s.deadlineMS,
      finished_at_ms: finishedAt,
      last_seen_ms: lastSeen,
      config: { ...s.config },
      counts,
      conflicts_truncated: s.conflictsTruncated,
      conflicts,
      runner_count: s.runners.size,
      runners,
      slowest,
      problem_count: problemCount,
      problems,
    },
    error: null,
  };
}

export function decideListTests(s: State, nowMS: number, cmd: ListTestsCommand): DecideOutcome<ListTestsResponse> {
  if (!s.initialized) {
    return {
      events: [],
      response: null,
      error: new QueueError(404, ErrCodeBuildNotFound, "build not initialized"),
    };
  }
  if (!Number.isInteger(cmd.cursor) || cmd.cursor < 0) {
    return { events: [], response: null, error: errInvalid(`cursor ${cmd.cursor} must not be negative`) };
  }
  let limit = cmd.limit;
  if (limit === 0) {
    limit = DefaultTestsPageLimit;
  }
  if (!Number.isInteger(limit) || limit < 1 || limit > MaxTestsPageLimit) {
    return {
      events: [],
      response: null,
      error: errInvalid(`limit ${limit} is outside 1..${MaxTestsPageLimit}`),
    };
  }

  let stateFilter: Set<TestStatus> | null = null;
  if (cmd.states && cmd.states.length > 0) {
    stateFilter = new Set();
    for (let i = 0; i < cmd.states.length; i++) {
      const st = cmd.states[i]!;
      if (
        st === "pending" ||
        st === "leased" ||
        st === "passed" ||
        st === "failed" ||
        st === "skipped" ||
        st === "unresolved"
      ) {
        stateFilter.add(st);
      } else {
        return { events: [], response: null, error: errInvalid(`invalid state filter ${JSON.stringify(st)}`) };
      }
    }
  }

  const ov = evaluateExpiry(s, nowMS);
  const useOverlay = stateVerdict(s) === "running" && overlayVerdict(ov, s) !== "running";
  const events = useOverlay ? ov.events : [];
  const verdict = useOverlay ? overlayVerdict(ov, s) : stateVerdict(s);

  const tests: TestView[] = [];
  let nextCursor: number | null = null;

  for (let i = cmd.cursor; i < s.tests.length; i++) {
    const t = s.tests[i]!;
    const st = useOverlay ? overlayTestStatus(ov, s, i) : t.status;
    if (stateFilter && !stateFilter.has(st)) {
      continue;
    }
    if (cmd.retried && t.generation <= 1 && t.history.length === 0 && !t.historyTruncated) {
      continue;
    }
    if (tests.length === limit) {
      nextCursor = i;
      break;
    }

    const attempts = t.attempts.map((a) => ({ ...a }));
    if (useOverlay && t.status === "leased" && st === "unresolved" && attempts.length > 0) {
      const last = attempts[attempts.length - 1]!;
      if (last.status === "leased") {
        last.status = "expired";
        last.finished_at_ms = ov.nowMS;
      }
    }
    const history = t.history.map((h) => ({ ...h }));
    tests.push({
      test_index: t.index,
      test: t.path,
      state: st,
      generation: t.generation,
      predicted_ms: t.effectiveMS,
      timed: t.timed,
      duration_ms: t.durationMS,
      attempts,
      history,
      history_truncated: t.historyTruncated,
    });
  }

  return {
    events,
    response: {
      build_id: s.buildID,
      verdict,
      tests,
      next_cursor: nextCursor,
    },
    error: null,
  };
}

function insertSlowest(list: SlowestTest[], item: SlowestTest): void {
  let pos = list.length;
  while (pos > 0) {
    const prev = list[pos - 1]!;
    if (prev.duration_ms > item.duration_ms || (prev.duration_ms === item.duration_ms && prev.test_index < item.test_index)) {
      break;
    }
    pos--;
  }
  list.splice(pos, 0, item);
}
