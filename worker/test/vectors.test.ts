import { readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import {
  type Config,
  type Counts,
  type HeartbeatCommand,
  type InitCommand,
  type LeaseCommand,
  type ListTestsCommand,
  type QueueEvent,
  type ReportCommand,
  type State,
  type TestStatus,
  type Verdict,
  applyEvent,
  decideHeartbeat,
  decideInit,
  decideLease,
  decideListTests,
  decideReport,
  decideStatus,
  defaultLimits,
  extendLease,
  newState,
  stateVerdict,
} from "../src/core/index.ts";
import { resetActiveLeaseDeadlinesForGoRestart } from "./restart-helper.ts";

interface VectorStep {
  now_ms: number;
  command: Record<string, unknown> & { type: string };
  expect_status: number;
  expect_error_code?: string;
  expect_events?: string[];
  expect_response?: Record<string, unknown>;
}

interface VectorFinalTest {
  test_index: number;
  state: TestStatus;
  generation: number;
  timed: boolean;
  duration_ms: number;
  history_len: number;
  history_truncated: boolean;
}

interface VectorFile {
  name: string;
  description: string;
  min_lease_ttl_ms?: number;
  steps: VectorStep[];
  expect_final: {
    verdict: Verdict;
    counts: Counts;
    conflicts_len: number;
    conflicts_truncated: boolean;
    tests: VectorFinalTest[];
  };
}

// When persistentOnly is true, lease deadline_ms is zeroed so state replayed
// from the event log can be compared against live in-memory state.
function serializeState(s: State, persistentOnly: boolean): string {
  const runners: Record<string, unknown> = {};
  for (const id of s.runnerOrder) {
    runners[id] = s.runners.get(id);
  }
  const leases = s.leases.map((l) =>
    persistentOnly ? { ...l, deadline_ms: 0 } : l,
  );
  return JSON.stringify({
    initialized: s.initialized,
    buildID: s.buildID,
    digest: s.digest,
    config: s.config,
    createdAtMS: s.createdAtMS,
    deadlineMS: s.deadlineMS,
    finishedAtMS: s.finishedAtMS,
    lastSeenMS: s.lastSeenMS,
    tests: s.tests,
    dealOrder: s.dealOrder,
    counts: s.counts,
    leases,
    activeLeases: s.activeLeases,
    runners,
    runnerOrder: s.runnerOrder,
    conflicts: s.conflicts,
    conflictsTruncated: s.conflictsTruncated,
  });
}

function runCommand(s: State, nowMS: number, cmd: Record<string, unknown> & { type: string }) {
  switch (cmd.type) {
    case "init": {
      const initCmd: InitCommand = {
        build_id: (cmd.build_id as string | undefined) ?? "",
        runner_id: (cmd.runner_id as string | undefined) ?? "",
        machine_index: (cmd.machine_index as number | null | undefined) ?? null,
        config: (cmd.config as Config | undefined) ?? {
          machines: 0,
          lease_ttl_ms: 0,
          build_timeout_ms: 0,
          max_attempts: 0,
          max_batch: 0,
        },
        tests: (cmd.tests as string[] | undefined) ?? [],
        predictions_ms: (cmd.predictions_ms as (number | null)[] | undefined) ?? [],
      };
      return decideInit(s, nowMS, initCmd);
    }
    case "lease": {
      const leaseCmd: LeaseCommand = {
        runner_id: (cmd.runner_id as string | undefined) ?? "",
        machine_index: (cmd.machine_index as number | null | undefined) ?? null,
        request_id: (cmd.request_id as string | undefined) ?? "",
      };
      return decideLease(s, nowMS, leaseCmd);
    }
    case "heartbeat": {
      const hbCmd: HeartbeatCommand = {
        runner_id: (cmd.runner_id as string | undefined) ?? "",
        lease_id: (cmd.lease_id as number | undefined) ?? 0,
      };
      return decideHeartbeat(s, nowMS, hbCmd);
    }
    case "report": {
      const repCmd: ReportCommand = {
        runner_id: (cmd.runner_id as string | undefined) ?? "",
        lease_id: (cmd.lease_id as number | undefined) ?? 0,
        results: (cmd.results as ReportCommand["results"] | undefined) ?? [],
      };
      return decideReport(s, nowMS, repCmd);
    }
    case "status":
      return decideStatus(s, nowMS);
    case "list_tests": {
      const ltCmd: ListTestsCommand = {
        cursor: (cmd.cursor as number | undefined) ?? 0,
        limit: (cmd.limit as number | undefined) ?? 0,
        ...(cmd.states !== undefined ? { states: cmd.states as TestStatus[] } : {}),
        ...(cmd.retried !== undefined ? { retried: Boolean(cmd.retried) } : {}),
      };
      return decideListTests(s, nowMS, ltCmd);
    }
    default:
      throw new Error(`unknown vector command type ${cmd.type}`);
  }
}

const vectorsDir = resolve(import.meta.dirname, "../../internal/queue/testdata/vectors");
const vectorFiles = readdirSync(vectorsDir)
  .filter((f) => f.endsWith(".json"))
  .sort();

describe("shared queue vectors", () => {
  it("loads at least 15 vector files", () => {
    expect(vectorFiles.length).toBeGreaterThanOrEqual(15);
  });

  for (const file of vectorFiles) {
    const raw = readFileSync(join(vectorsDir, file), "utf8");
    const vec = JSON.parse(raw) as VectorFile;

    it(`${file} (${vec.name})`, () => {
      const lim = defaultLimits();
      if (vec.min_lease_ttl_ms && vec.min_lease_ttl_ms > 0) {
        lim.minLeaseTTLMS = vec.min_lease_ttl_ms;
      }
      const st = newState(lim);
      const log: QueueEvent[] = [];

      for (let i = 0; i < vec.steps.length; i++) {
        const step = vec.steps[i]!;
        if (step.command.type === "restart") {
          resetActiveLeaseDeadlinesForGoRestart(st, step.now_ms);
          continue;
        }

        const before = serializeState(st, false);
        const out = runCommand(st, step.now_ms, step.command);
        expect(serializeState(st, false), `step ${i} (${step.command.type}): Decide mutated State`).toBe(before);

        const gotStatus = out.error ? out.error.httpStatus : 200;
        const gotCode = out.error ? out.error.code : "";
        expect(gotStatus, `step ${i} (${step.command.type}) status (err=${out.error?.message ?? ""})`).toBe(
          step.expect_status,
        );
        expect(gotCode, `step ${i} (${step.command.type}) error code`).toBe(step.expect_error_code ?? "");

        const gotTypes = out.events.map((e) => e.type);
        expect(gotTypes, `step ${i} (${step.command.type}) events`).toEqual(step.expect_events ?? []);

        for (const ev of out.events) {
          const decoded = JSON.parse(JSON.stringify(ev)) as QueueEvent;
          applyEvent(st, decoded);
          log.push(decoded);
        }

        if (gotStatus === 200 && out.response) {
          if (step.command.type === "heartbeat") {
            const r = out.response as { held: boolean; deadline_ms: number };
            if (r.held) {
              extendLease(st, step.command.lease_id as number, r.deadline_ms);
            }
          } else if (step.command.type === "lease") {
            const r = out.response as { status: string; lease_id?: number; deadline_ms?: number };
            if (r.status === "leased" && r.lease_id !== undefined && r.deadline_ms !== undefined) {
              extendLease(st, r.lease_id, r.deadline_ms);
            }
          }
        }

        if (step.expect_response) {
          const gotObj = JSON.parse(JSON.stringify(out.response)) as Record<string, unknown>;
          for (const [k, wv] of Object.entries(step.expect_response)) {
            expect(gotObj, `step ${i} missing key ${k}`).toHaveProperty(k);
            expect(gotObj[k], `step ${i} response[${k}]`).toEqual(wv);
          }
        }
      }

      expect(stateVerdict(st)).toBe(vec.expect_final.verdict);
      expect(st.counts).toEqual(vec.expect_final.counts);
      expect(st.conflicts.length).toBe(vec.expect_final.conflicts_len);
      expect(st.conflictsTruncated).toBe(vec.expect_final.conflicts_truncated);

      for (const wt of vec.expect_final.tests) {
        const gt = st.tests[wt.test_index]!;
        expect({
          test_index: gt.index,
          state: gt.status,
          generation: gt.generation,
          timed: gt.timed,
          duration_ms: gt.durationMS,
          history_len: gt.history.length,
          history_truncated: gt.historyTruncated,
        }).toEqual(wt);
      }

      const replayed = newState(lim);
      for (const ev of log) {
        applyEvent(replayed, ev);
      }
      expect(serializeState(replayed, true)).toBe(serializeState(st, true));
    });
  }
});
