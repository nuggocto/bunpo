// Ambient declarations for the plain-JS dashboard core module imported by
// worker/test/dashboard-core.test.ts.

declare module "*dashboard/core.js" {
  export const BASE_POLL_MS: number;
  export const MAX_POLL_MS: number;
  export const MAX_POLL_FAILURES: number;
  export const PAGE_LIMIT: number;

  export interface StatusMeta {
    symbol: string;
    label: string;
  }

  export function statusMeta(status: string): StatusMeta;
  export function isValidBuildID(id: string): boolean;
  export function parseRoute(pathname: string): { view: "builds" } | { view: "build"; buildID: string } | { view: "not_found" };
  export function buildPath(buildID: string): string;
  export function formatDuration(ms: number): string;
  export function formatAttemptDuration(attempt: {
    timed?: boolean;
    duration_ms?: number;
    leased_at_ms?: number;
    finished_at_ms?: number;
  }): string;
  export function formatTimeLeft(deadlineMS: number, nowMS: number, verdict: string): string;
  export function formatTimestamp(ms: number): string;
  export function sortBuilds<T extends { build_id: string; created_at_ms: number }>(builds: readonly T[]): T[];

  export type GuardedResult<T> =
    | { stale: true }
    | { stale: false; ok: true; value: T }
    | { stale: false; ok: false; error: unknown };

  export interface GuardedLoader<T = unknown> {
    invalidate(): number;
    current(): number;
    isCurrent(g: number): boolean;
    fetch(path: string, expectedGen: number): Promise<GuardedResult<T>>;
  }

  export function createGuardedLoader<T = unknown>(fetchJSON: (path: string) => Promise<T>): GuardedLoader<T>;

  export function fetchBuildInitial<T = unknown>(
    loader: GuardedLoader<T>,
    gen: number,
    buildID: string,
    testsQS: string,
    onStatus?: (status: T) => void,
  ): Promise<GuardedResult<{ status: T; timeline: T; tests: T }>>;

  export function fetchRefreshedPages<T = unknown>(
    loader: GuardedLoader<{ tests?: T[]; next_cursor?: number | null }>,
    gen: number,
    pathForCursor: (cursor: number) => string,
    pageCount: number,
  ): Promise<GuardedResult<{ tests: T[]; nextCursor: number | null; pagesLoaded: number }>>;

  export interface RefreshBuildResult<S = unknown, T = unknown> {
    status: S;
    detailsLoaded: boolean;
    detailsLastSeenMS: number | null;
    timeline?: { tests: T[]; nextCursor: number | null; pagesLoaded: number };
    tests?: { tests: T[]; nextCursor: number | null; pagesLoaded: number };
  }

  export function refreshBuild<
    S extends { last_seen_ms: number; verdict: string } = { last_seen_ms: number; verdict: string },
    T = unknown,
  >(
    loader: GuardedLoader<unknown>,
    gen: number,
    opts: {
      buildID: string;
      detailsLastSeenMS: number | null;
      manual: boolean;
      tlPages: number;
      testsPages: number;
      testsQuery: (cursor: number) => string;
      onStatus?: (status: S) => void;
    },
  ): Promise<GuardedResult<RefreshBuildResult<S, T>>>;

  export interface TimelineBar {
    leaseID: number;
    runnerID: string;
    machineIndex: number | null;
    startMS: number;
    endMS: number;
    kind: string;
    meta: StatusMeta;
    xPct: number;
    wPct: number;
    tests: { test: string; generation: number; status: string; durationText: string }[];
  }

  export interface TimelineLane {
    runnerID: string;
    machineIndex: number | null;
    firstSeenMS: number;
    bars: TimelineBar[];
  }

  export function buildTimeline(
    status: {
      created_at_ms: number;
      last_seen_ms: number;
      finished_at_ms: number | null;
      runners: { runner_id: string; machine_index: number | null; first_seen_ms: number }[];
    },
    tests: {
      test: string;
      attempts: {
        lease_id: number;
        runner_id: string;
        generation: number;
        status: string;
        timed?: boolean;
        leased_at_ms: number;
        finished_at_ms: number;
        duration_ms: number;
      }[];
    }[],
    nowMS: number,
  ): { minMS: number; maxMS: number; spanMS: number; lanes: TimelineLane[] };

  export interface PollState {
    active: boolean;
    visible: boolean;
    verdict: string | null;
    inFlight: boolean;
    nextAtMS: number | null;
    lastPollMS: number | null;
    failures: number;
    stoppedReason: string | null;
  }

  export type PollEvent =
    | { type: "watch"; verdict: string; nowMS: number }
    | { type: "stop"; reason?: string; nowMS: number }
    | { type: "visibility"; visible: boolean; nowMS: number }
    | { type: "tick"; nowMS: number }
    | { type: "response_ok"; verdict: string; manual?: boolean; nowMS: number }
    | { type: "response_error"; httpStatus: number; retryAfterMS?: number; nowMS: number };

  export type PollAction =
    | { type: "schedule"; atMS: number; delayMS: number }
    | { type: "cancel" }
    | { type: "fetch" };

  export function createPollState(): PollState;
  export function pollStep(state: PollState, ev: PollEvent): { state: PollState; actions: PollAction[] };
}
