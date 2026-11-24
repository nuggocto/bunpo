import { createHash } from "node:crypto";
import { isCanonicalPOSIXTestID, compareUTF8, median } from "./canonical.ts";

export const ProtocolVersion = 1;

export const MaxTests = 20_000;
export const MaxTestIDBytes = 4_096;
export const MaxMachines = 256;
export const MaxRunners = 1_024;
export const MaxDurationMS = 30 * 24 * 60 * 60 * 1000;

export const MinLeaseTTLMS = 10_000;
export const MaxLeaseTTLMS = 600_000;
export const DefaultLeaseTTLMS = 60_000;

export const MaxBuildTimeoutMS = 24 * 60 * 60 * 1000;
export const DefaultBuildTimeoutMS = 6 * 60 * 60 * 1000;

export const MaxAttemptsLimit = 10;
export const DefaultMaxAttempts = 3;

export const MaxBatchLimit = 200;
export const DefaultMaxBatch = 200;

export const MaxInitConflicts = 8;
export const MaxTestHistory = 8;

export const MaxStatusRunners = 256;
export const MaxStatusSlowest = 20;
export const MaxStatusProblems = 50;

export const DefaultTestsPageLimit = 200;
export const MaxTestsPageLimit = 1_000;

export const MinWaitMS = 1_000;
export const MaxWaitMS = 15_000;

export const ErrCodeInvalidRequest = "invalid_request";
export const ErrCodeBuildNotFound = "build_not_found";
export const ErrCodeInitConflict = "init_conflict";
export const ErrCodeLeaseHeld = "lease_held";
export const ErrCodeStaleRequest = "stale_request";
export const ErrCodeTooManyRunners = "too_many_runners";
export const ErrCodeUnknownLease = "unknown_lease";
export const ErrCodeRunnerMismatch = "runner_mismatch";

export class QueueError extends Error {
  readonly httpStatus: number;
  readonly code: string;

  constructor(httpStatus: number, code: string, message: string) {
    super(`${code} (${httpStatus}): ${message}`);
    this.name = "QueueError";
    this.httpStatus = httpStatus;
    this.code = code;
  }
}

export function errInvalid(message: string): QueueError {
  return new QueueError(400, ErrCodeInvalidRequest, message);
}

export interface Limits {
  minLeaseTTLMS: number;
  maxTests: number;
  maxRunners: number;
}

export function defaultLimits(): Limits {
  return {
    minLeaseTTLMS: MinLeaseTTLMS,
    maxTests: MaxTests,
    maxRunners: MaxRunners,
  };
}

export function effectiveLimits(lim?: Partial<Limits>): Limits {
  return {
    minLeaseTTLMS: lim?.minLeaseTTLMS && lim.minLeaseTTLMS > 0 ? lim.minLeaseTTLMS : MinLeaseTTLMS,
    maxTests: lim?.maxTests && lim.maxTests > 0 ? lim.maxTests : MaxTests,
    maxRunners: lim?.maxRunners && lim.maxRunners > 0 ? lim.maxRunners : MaxRunners,
  };
}

export type Verdict = "running" | "passed" | "failed" | "unresolved";
export type TestStatus = "pending" | "leased" | "passed" | "failed" | "skipped" | "unresolved";
export type Outcome = "passed" | "failed" | "skipped" | "error";
export type AttemptStatus = "leased" | "expired" | "passed" | "failed" | "skipped" | "error";
export type Disposition = "accepted" | "duplicate" | "stale" | "conflict";
export type LeaseStatus = "active" | "reported" | "expired";
export type LeaseResponseStatus = "leased" | "wait" | "done";

export function isValidOutcome(o: string): o is Outcome {
  return o === "passed" || o === "failed" || o === "skipped" || o === "error";
}

export function hasReportedOutcome(a: AttemptStatus): boolean {
  return a === "passed" || a === "failed" || a === "skipped" || a === "error";
}

export interface Config {
  machines: number;
  lease_ttl_ms: number;
  build_timeout_ms: number;
  max_attempts: number;
  max_batch: number;
}

export function validateConfig(c: Config, lim: Limits): QueueError | null {
  const eff = effectiveLimits(lim);
  if (!Number.isInteger(c.machines) || c.machines < 1 || c.machines > MaxMachines) {
    return errInvalid(`machines ${c.machines} is outside 1..${MaxMachines}`);
  }
  if (!Number.isInteger(c.lease_ttl_ms) || c.lease_ttl_ms < eff.minLeaseTTLMS || c.lease_ttl_ms > MaxLeaseTTLMS) {
    return errInvalid(`lease_ttl_ms ${c.lease_ttl_ms} is outside ${eff.minLeaseTTLMS}..${MaxLeaseTTLMS}`);
  }
  if (
    !Number.isInteger(c.build_timeout_ms) ||
    c.build_timeout_ms < c.lease_ttl_ms ||
    c.build_timeout_ms > MaxBuildTimeoutMS
  ) {
    return errInvalid(`build_timeout_ms ${c.build_timeout_ms} is outside ${c.lease_ttl_ms}..${MaxBuildTimeoutMS}`);
  }
  if (!Number.isInteger(c.max_attempts) || c.max_attempts < 1 || c.max_attempts > MaxAttemptsLimit) {
    return errInvalid(`max_attempts ${c.max_attempts} is outside 1..${MaxAttemptsLimit}`);
  }
  if (!Number.isInteger(c.max_batch) || c.max_batch < 1 || c.max_batch > MaxBatchLimit) {
    return errInvalid(`max_batch ${c.max_batch} is outside 1..${MaxBatchLimit}`);
  }
  return null;
}

export interface Counts {
  total: number;
  pending: number;
  leased: number;
  passed: number;
  failed: number;
  skipped: number;
  unresolved: number;
}

export interface AttemptRecord {
  attempt_id: string;
  generation: number;
  lease_id: number;
  runner_id: string;
  status: AttemptStatus;
  timed: boolean;
  duration_ms: number;
  leased_at_ms: number;
  finished_at_ms: number;
}

export interface HistoryRecord {
  attempt_id: string;
  generation: number;
  lease_id: number;
  runner_id: string;
  outcome: Outcome;
  timed: boolean;
  duration_ms: number;
  disposition: Disposition;
  at_ms: number;
}

export interface TestState {
  index: number;
  path: string;
  predictedMS: number | null;
  effectiveMS: number;
  status: TestStatus;
  generation: number;
  activeLeaseID: number;
  timed: boolean;
  durationMS: number;
  attempts: AttemptRecord[];
  history: HistoryRecord[];
  historyTruncated: boolean;
}

export interface LeaseAttempt {
  test_index: number;
  generation: number;
}

export interface Lease {
  lease_id: number;
  runner_id: string;
  machine_index: number | null;
  request_id: string;
  status: LeaseStatus;
  leased_at_ms: number;
  deadline_ms: number;
  finished_at_ms: number;
  attempts: LeaseAttempt[];
}

export interface RunnerState {
  runnerID: string;
  machineIndex: number | null;
  firstSeenMS: number;
  leases: number;
  accepted: number;
  activeLeaseID: number;
  lastRequestID: string;
  lastLeaseID: number;
}

export interface InitConflict {
  runner_id: string;
  machine_index: number | null;
  got_digest: string;
  reason: string;
  at_ms: number;
}

export interface State {
  limits: Limits;
  initialized: boolean;
  buildID: string;
  digest: string;
  config: Config;
  createdAtMS: number;
  deadlineMS: number;
  finishedAtMS: number;
  lastSeenMS: number;
  tests: TestState[];
  dealOrder: number[];
  counts: Counts;
  leases: Lease[];
  activeLeases: number[];
  runners: Map<string, RunnerState>;
  runnerOrder: string[];
  conflicts: InitConflict[];
  conflictsTruncated: boolean;
}

export function newState(lim?: Partial<Limits>): State {
  return {
    limits: effectiveLimits(lim),
    initialized: false,
    buildID: "",
    digest: "",
    config: {
      machines: 0,
      lease_ttl_ms: 0,
      build_timeout_ms: 0,
      max_attempts: 0,
      max_batch: 0,
    },
    createdAtMS: 0,
    deadlineMS: 0,
    finishedAtMS: 0,
    lastSeenMS: 0,
    tests: [],
    dealOrder: [],
    counts: {
      total: 0,
      pending: 0,
      leased: 0,
      passed: 0,
      failed: 0,
      skipped: 0,
      unresolved: 0,
    },
    leases: [],
    activeLeases: [],
    runners: new Map(),
    runnerOrder: [],
    conflicts: [],
    conflictsTruncated: false,
  };
}

export function stateVerdict(s: State): Verdict {
  if (!s.initialized) {
    return "running";
  }
  return computeVerdict(s.counts, s.conflicts.length > 0);
}

export function computeVerdict(c: Counts, hasConflict: boolean): Verdict {
  if (c.pending > 0 || c.leased > 0) {
    return "running";
  }
  if (c.failed > 0) {
    return "failed";
  }
  if (c.unresolved > 0 || hasConflict) {
    return "unresolved";
  }
  return "passed";
}

export function formatAttemptID(testIndex: number, generation: number): string {
  return `${testIndex}.${generation}`;
}

function isAlnum(c: number): boolean {
  return (c >= 0x61 && c <= 0x7a) || (c >= 0x41 && c <= 0x5a) || (c >= 0x30 && c <= 0x39);
}

export function validBuildID(id: string): boolean {
  if (id.length === 0 || id.length > 128) {
    return false;
  }
  if (!isAlnum(id.charCodeAt(0))) {
    return false;
  }
  for (let i = 0; i < id.length; i++) {
    const c = id.charCodeAt(i);
    if (!isAlnum(c) && c !== 0x2e && c !== 0x5f && c !== 0x2d) {
      return false;
    }
  }
  return !id.includes("..");
}

function validToken(s: string, maxLen: number): boolean {
  if (s.length === 0 || s.length > maxLen) {
    return false;
  }
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (!isAlnum(c) && c !== 0x2e && c !== 0x5f && c !== 0x2d) {
      return false;
    }
  }
  return true;
}

export function validRunnerID(id: string): boolean {
  return validToken(id, 64);
}

export function validRequestID(id: string): boolean {
  return validToken(id, 128);
}

export function validTestID(id: string): boolean {
  return isCanonicalPOSIXTestID(id, MaxTestIDBytes);
}

export function validateMachineIndex(idx: number | null): QueueError | null {
  if (idx === null) {
    return null;
  }
  if (!Number.isInteger(idx) || idx < 0 || idx >= MaxMachines) {
    return errInvalid(`machine_index ${idx} is outside 0..${MaxMachines - 1}`);
  }
  return null;
}

export function validateTestList(
  tests: readonly string[],
  predictions: readonly (number | null)[],
  maxTests: number,
): QueueError | null {
  if (tests.length === 0) {
    return errInvalid("tests must not be empty");
  }
  if (tests.length > maxTests) {
    return errInvalid(`tests has ${tests.length} entries, over limit ${maxTests}`);
  }
  if (predictions.length !== tests.length) {
    return errInvalid(`predictions_ms length ${predictions.length} does not match tests length ${tests.length}`);
  }
  for (let i = 0; i < tests.length; i++) {
    const id = tests[i]!;
    if (!validTestID(id)) {
      return errInvalid(`test ${i} (${JSON.stringify(id)}) is not a canonical test identifier`);
    }
    if (i > 0 && compareUTF8(tests[i - 1]!, id) >= 0) {
      return errInvalid(
        `tests must be unique and sorted in byte order (${JSON.stringify(id)} after ${JSON.stringify(tests[i - 1]!)})`,
      );
    }
    const p = predictions[i]!;
    if (p !== null) {
      if (!Number.isInteger(p) || p < 0 || p > MaxDurationMS) {
        return errInvalid(`predictions_ms[${i}] = ${p} is outside 0..${MaxDurationMS}`);
      }
    }
  }
  return null;
}

export function configDigest(cfg: Config, tests: readonly string[]): string {
  const h = createHash("sha256");
  h.update(
    `bunpo queue ${ProtocolVersion}\nmachines ${cfg.machines}\nlease_ttl_ms ${cfg.lease_ttl_ms}\nbuild_timeout_ms ${cfg.build_timeout_ms}\nmax_attempts ${cfg.max_attempts}\nmax_batch ${cfg.max_batch}\n`,
  );
  if (tests.length > 0) {
    h.update(tests.join("\n"));
    h.update("\n");
  }
  return h.digest("hex");
}

export function computeEffectivePredictions(predictions: readonly (number | null)[]): number[] {
  const known: number[] = [];
  for (let i = 0; i < predictions.length; i++) {
    const p = predictions[i]!;
    if (p !== null) {
      known.push(p);
    }
  }
  const fill = median(known);
  const eff = new Array<number>(predictions.length);
  for (let i = 0; i < predictions.length; i++) {
    const p = predictions[i]!;
    eff[i] = p !== null ? p : fill;
  }
  return eff;
}
