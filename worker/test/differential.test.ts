import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  type Config,
  type HeartbeatCommand,
  type InitCommand,
  type LeaseCommand,
  type ListTestsCommand,
  type QueueEvent,
  type ReportCommand,
  type State,
  type TestStatus,
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

interface ExportedStep {
  kind: "command" | "restart";
  now_ms: number;
  command?: Record<string, unknown> & { type: string };
  expect_status?: number;
  expect_error_code?: string;
  expect_response?: unknown;
  expect_events?: QueueEvent[];
  reloaded_events?: QueueEvent[];
  log_synced?: boolean;
}

interface ExportedSeed {
  seed: number;
  name: string;
  min_lease_ttl_ms: number;
  max_tests?: number;
  max_runners?: number;
  steps: ExportedStep[];
}

// canonicalJSON recursively sorts object keys and omits undefined properties so
// Go and TypeScript JSON representations can be compared byte-for-byte.
function canonicalJSON(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value as Record<string, unknown>).sort()) {
      const v = (value as Record<string, unknown>)[k];
      if (v !== undefined) {
        out[k] = sortKeys(v);
      }
    }
    return out;
  }
  return value;
}

function runCommand(s: State, nowMS: number, cmd: Record<string, unknown> & { type: string }) {
  switch (cmd.type) {
    case "init": {
      const initCmd: InitCommand = {
        build_id: (cmd.build_id as string | undefined) ?? "",
        runner_id: (cmd.runner_id as string | undefined) ?? "",
        machine_index: (cmd.machine_index as number | null | undefined) ?? null,
        config: cmd.config as Config,
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
      throw new Error(`unknown exported command type ${cmd.type}`);
  }
}

const exportDir = process.env["BUNPO_SIM_EXPORT_DIR"] ?? "";

describe("differential replay against Go simulator", () => {
  it.skipIf(!exportDir)("matches Go core byte-for-byte across all exported seeds", () => {
    expect(existsSync(exportDir), `BUNPO_SIM_EXPORT_DIR=${exportDir} does not exist`).toBe(true);
    const files = readdirSync(exportDir)
      .filter((f) => f.endsWith(".json"))
      .sort();
    expect(files.length).toBeGreaterThan(0);

    let totalSteps = 0;
    let totalCommands = 0;
    let totalRestarts = 0;
    let totalEvents = 0;

    for (const file of files) {
      const seed = JSON.parse(readFileSync(join(exportDir, file), "utf8")) as ExportedSeed;
      const lim = defaultLimits();
      if (seed.min_lease_ttl_ms > 0) {
        lim.minLeaseTTLMS = seed.min_lease_ttl_ms;
      }
      if (seed.max_tests && seed.max_tests > 0) {
        lim.maxTests = seed.max_tests;
      }
      if (seed.max_runners && seed.max_runners > 0) {
        lim.maxRunners = seed.max_runners;
      }

      let st = newState(lim);
      let appliedLog: QueueEvent[] = [];

      for (let i = 0; i < seed.steps.length; i++) {
        const step = seed.steps[i]!;
        totalSteps++;

        if (step.kind === "restart") {
          totalRestarts++;
          const reloaded = step.reloaded_events ?? [];
          if (step.log_synced) {
            expect(
              canonicalJSON(appliedLog),
              `${seed.name} step ${i}: pre-restart applied event log diverged`,
            ).toBe(canonicalJSON(reloaded));
          }

          // Reconstruct State from disk log exactly like Go's DiskLog.Load + NewHandler.
          st = newState(lim);
          appliedLog = [];
          for (const ev of reloaded) {
            applyEvent(st, ev);
            appliedLog.push(ev);
          }
          if (st.initialized && stateVerdict(st) === "running") {
            resetActiveLeaseDeadlinesForGoRestart(st, step.now_ms);
            if (step.now_ms >= st.deadlineMS) {
              const fin = decideStatus(st, step.now_ms);
              expect(fin.error, `${seed.name} step ${i}: restart deadline finalize error`).toBeNull();
              for (const ev of fin.events) {
                applyEvent(st, ev);
                appliedLog.push(ev);
              }
            }
          }
          continue;
        }

        totalCommands++;
        const cmd = step.command!;
        const out = runCommand(st, step.now_ms, cmd);
        const gotStatus = out.error ? out.error.httpStatus : 200;
        const gotCode = out.error ? out.error.code : "";

        expect(
          gotStatus,
          `${seed.name} step ${i} (${cmd.type} @ ${step.now_ms}): status mismatch (err=${out.error?.message ?? ""})`,
        ).toBe(step.expect_status);

        expect(
          gotCode,
          `${seed.name} step ${i} (${cmd.type} @ ${step.now_ms}): error code mismatch`,
        ).toBe(step.expect_error_code ?? "");

        const wantEvents = step.expect_events ?? [];
        expect(
          canonicalJSON(out.events),
          `${seed.name} step ${i} (${cmd.type} @ ${step.now_ms}): events canonical JSON mismatch`,
        ).toBe(canonicalJSON(wantEvents));

        for (const ev of out.events) {
          applyEvent(st, ev);
          appliedLog.push(ev);
          totalEvents++;
        }

        if (gotStatus === 200) {
          expect(
            canonicalJSON(out.response),
            `${seed.name} step ${i} (${cmd.type} @ ${step.now_ms}): response canonical JSON mismatch`,
          ).toBe(canonicalJSON(step.expect_response));

          if (cmd.type === "heartbeat") {
            const r = out.response as { held: boolean; deadline_ms: number };
            if (r.held) {
              extendLease(st, cmd.lease_id as number, r.deadline_ms);
            }
          } else if (cmd.type === "lease") {
            const r = out.response as { status: string; lease_id?: number; deadline_ms?: number };
            if (r.status === "leased" && r.lease_id !== undefined && r.deadline_ms !== undefined) {
              extendLease(st, r.lease_id, r.deadline_ms);
            }
          }
        }
      }
    }

    console.log(
      `Differential replay passed: ${files.length} seeds, ${totalSteps} steps (${totalCommands} commands, ${totalRestarts} restarts), ${totalEvents} events`,
    );
  });
});
