declare module "*dashboard/core.js" {
  export const BASE_POLL_MS: number;
  export const MAX_POLL_MS: number;
  export const MAX_POLL_FAILURES: number;

  export interface StatusMeta {
    symbol: string;
    label: string;
  }

  export function statusMeta(status: string): StatusMeta;
  export function isValidBuildID(id: string): boolean;
  export function parseRoute(pathname: string): { view: "builds" } | { view: "build"; buildID: string } | { view: "not_found" };
  export function buildPath(buildID: string): string;
  export function formatDuration(ms: number): string;
  export function formatTimeLeft(deadlineMS: number, nowMS: number, verdict: string): string;
  export function formatTimestamp(ms: number): string;
  export function sortBuilds<T extends { build_id: string; created_at_ms: number }>(builds: readonly T[]): T[];

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
    tests: { test: string; generation: number; status: string; durationMS: number }[];
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
    | { type: "response_ok"; verdict: string; nowMS: number }
    | { type: "response_error"; httpStatus: number; retryAfterMS?: number; nowMS: number };

  export type PollAction =
    | { type: "schedule"; atMS: number; delayMS: number }
    | { type: "cancel" }
    | { type: "fetch" };

  export function createPollState(): PollState;
  export function pollStep(state: PollState, ev: PollEvent): { state: PollState; actions: PollAction[] };
}
