import { DurableObject } from "cloudflare:workers";
import { parseMaxTests } from "./config.ts";
import {
  type InitCommand,
  type ListTestsCommand,
  type QueueEvent,
  type State,
  ProtocolVersion,
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
  resetActiveLeaseDeadlines,
  stateVerdict,
} from "./core/index.ts";
import type { IndexEntry } from "./index-core.ts";
import {
  type WireError,
  ErrCodeStorageError,
  parseHeartbeatBody,
  parseInitBody,
  parseLeaseBody,
  parseReportBody,
  wireErr,
} from "./wire.ts";

export const CATALOG_CHUNK_SIZE = 1_000;
export const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const INDEX_NAME = "global";

export interface DoHttpReply {
  status: number;
  body: string;
}

interface CatalogChunkPayload {
  tests: string[];
  predictions_ms: (number | null)[];
}

function okReply(payload: object): DoHttpReply {
  return {
    status: 200,
    body: JSON.stringify({ version: ProtocolVersion, ...payload }),
  };
}

function errReply(err: WireError): DoHttpReply {
  return {
    status: err.httpStatus,
    body: JSON.stringify({
      version: ProtocolVersion,
      error: { code: err.code, message: err.message },
    }),
  };
}

export class BuildDO extends DurableObject<Env> {
  private state: State;
  private nextSeq = 1;
  private schemaReady = false;
  private pendingWakeReset = false;
  private indexDirty = false;
  private readonly maxTests: number;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const parsedMax = parseMaxTests(env.MAX_TESTS);
    this.maxTests = parsedMax.ok ? parsedMax.maxTests : 5_000;
    this.state = this.freshState();

    this.ctx.blockConcurrencyWhile(async () => {
      this.ensureSchema();
      this.replayFromStorage();
    });
  }

  private freshState(): State {
    const lim = defaultLimits();
    lim.maxTests = this.maxTests;
    return newState(lim);
  }

  private resetMemory(): void {
    this.state = this.freshState();
    this.nextSeq = 1;
    this.schemaReady = false;
    this.pendingWakeReset = false;
    this.indexDirty = false;
  }

  private ensureSchema(): void {
    if (this.schemaReady) {
      return;
    }
    // INTEGER PRIMARY KEY uses SQLite's rowid B-tree directly (no extra index,
    // no sqlite_sequence table from AUTOINCREMENT), so each append writes 1 row.
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS catalog_chunks (idx INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS event_log (seq INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)",
    );
    this.schemaReady = true;
  }

  private replayFromStorage(): void {
    const evRows = this.ctx.storage.sql
      .exec<{ seq: number; payload: string }>("SELECT seq, payload FROM event_log ORDER BY seq ASC")
      .toArray();
    if (evRows.length === 0) {
      return;
    }

    const catRows = this.ctx.storage.sql
      .exec<{ idx: number; payload: string }>("SELECT idx, payload FROM catalog_chunks ORDER BY idx ASC")
      .toArray();

    const tests: string[] = [];
    const predictions: (number | null)[] = [];
    for (let i = 0; i < catRows.length; i++) {
      const chunk = JSON.parse(catRows[i]!.payload) as CatalogChunkPayload;
      for (let j = 0; j < chunk.tests.length; j++) {
        tests.push(chunk.tests[j]!);
        predictions.push(chunk.predictions_ms[j]!);
      }
    }

    for (let i = 0; i < evRows.length; i++) {
      const row = evRows[i]!;
      const events = JSON.parse(row.payload) as QueueEvent[];
      for (let j = 0; j < events.length; j++) {
        const ev = events[j]!;
        if (ev.type === "initialized" && ev.initialized) {
          ev.initialized.tests = tests;
          ev.initialized.predictions_ms = predictions;
          // Skip redundant per-path re-validation and SHA-256 re-hashing for our
          // own SQLite catalog; all structural checks and every other event still run.
          applyEvent(this.state, ev, { trustedInit: true });
        } else {
          applyEvent(this.state, ev);
        }
      }
      this.nextSeq = row.seq + 1;
    }

    const dirtyRows = this.ctx.storage.sql
      .exec<{ v: string }>("SELECT v FROM meta WHERE k = 'index_dirty'")
      .toArray();
    this.indexDirty = dirtyRows.length > 0 && dirtyRows[0]!.v === "1";
    this.pendingWakeReset = true;
  }

  private async beforeRequest(nowMS: number): Promise<void> {
    if (this.pendingWakeReset) {
      this.pendingWakeReset = false;
      if (this.state.initialized && stateVerdict(this.state) === "running") {
        resetActiveLeaseDeadlines(this.state, nowMS);
      }
    }
    if (this.indexDirty && this.state.initialized) {
      await this.trySyncIndex(nowMS);
    }
  }

  private buildIndexEntry(): IndexEntry {
    const verdict = stateVerdict(this.state);
    const finishedAt = this.state.finishedAtMS > 0 ? this.state.finishedAtMS : null;
    // Running builds expire 7 days past build deadline; finished builds expire 7 days past finish.
    const expiresAt =
      verdict === "running"
        ? this.state.deadlineMS + RETENTION_MS
        : (finishedAt ?? this.state.lastSeenMS) + RETENTION_MS;
    return {
      build_id: this.state.buildID,
      verdict,
      created_at_ms: this.state.createdAtMS,
      finished_at_ms: finishedAt,
      test_count: this.state.counts.total,
      failed_count: this.state.counts.failed,
      unresolved_count: this.state.counts.unresolved,
      has_conflicts: this.state.conflicts.length > 0,
      expires_at_ms: expiresAt,
    };
  }

  private async trySyncIndex(nowMS: number): Promise<void> {
    if (!this.state.initialized) {
      return;
    }
    try {
      const idx = this.env.BUILD_INDEX.get(this.env.BUILD_INDEX.idFromName(INDEX_NAME));
      await idx.upsert(this.buildIndexEntry(), nowMS);
      if (this.indexDirty) {
        this.ensureSchema();
        this.ctx.storage.sql.exec("DELETE FROM meta WHERE k = 'index_dirty'");
        this.indexDirty = false;
      }
    } catch {
      // Leave index_dirty = 1 in SQLite and memory; next wake, request, or alarm repairs it.
      this.indexDirty = true;
    }
  }

  private persistAndApply(events: QueueEvent[], markIndexDirty: boolean, trustedInit = false): boolean {
    if (events.length === 0) {
      return true;
    }
    this.ensureSchema();

    try {
      this.ctx.storage.transactionSync(() => {
        const first = events[0]!;
        if (first.type === "initialized" && first.initialized) {
          const init = first.initialized;
          const n = init.tests.length;
          let chunkIdx = 0;
          for (let offset = 0; offset < n; offset += CATALOG_CHUNK_SIZE) {
            const end = Math.min(offset + CATALOG_CHUNK_SIZE, n);
            const payload = JSON.stringify({
              tests: init.tests.slice(offset, end),
              predictions_ms: init.predictions_ms.slice(offset, end),
            });
            this.ctx.storage.sql.exec(
              "INSERT INTO catalog_chunks (idx, payload) VALUES (?, ?)",
              chunkIdx,
              payload,
            );
            chunkIdx++;
          }
          const headerRecord: QueueEvent[] = [
            {
              type: "initialized",
              at_ms: first.at_ms,
              initialized: {
                build_id: init.build_id,
                digest: init.digest,
                config: init.config,
                deadline_ms: init.deadline_ms,
                tests: [],
                predictions_ms: [],
              },
            },
            ...events.slice(1),
          ];
          this.ctx.storage.sql.exec(
            "INSERT INTO event_log (seq, payload) VALUES (?, ?)",
            this.nextSeq,
            JSON.stringify(headerRecord),
          );
        } else {
          this.ctx.storage.sql.exec(
            "INSERT INTO event_log (seq, payload) VALUES (?, ?)",
            this.nextSeq,
            JSON.stringify(events),
          );
        }

        if (markIndexDirty) {
          this.ctx.storage.sql.exec("INSERT OR REPLACE INTO meta (k, v) VALUES ('index_dirty', '1')");
        }
      });
    } catch (err) {
      // Abort the DO so in-memory state can never diverge from SQLite after a storage failure,
      // and throw so the RPC rejects cleanly instead of resolving against a broken output gate.
      this.ctx.abort(err instanceof Error ? err.message : "storage transaction failed");
      throw err;
    }

    this.nextSeq++;
    if (markIndexDirty) {
      this.indexDirty = true;
    }
    for (let i = 0; i < events.length; i++) {
      const ev = events[i]!;
      applyEvent(this.state, ev, trustedInit && ev.type === "initialized" ? { trustedInit: true } : undefined);
    }
    return true;
  }

  private async afterTransition(prevVerdict: string, hadConflict: boolean, created: boolean, nowMS: number): Promise<void> {
    if (!this.state.initialized) {
      return;
    }
    const newVerdict = stateVerdict(this.state);
    const becameTerminal = prevVerdict === "running" && newVerdict !== "running";
    const conflictChanged = !hadConflict && this.state.conflicts.length > 0;

    if (created) {
      await this.ctx.storage.setAlarm(this.state.deadlineMS);
      await this.trySyncIndex(nowMS);
      return;
    }
    if (becameTerminal) {
      const deleteAt = (this.state.finishedAtMS || nowMS) + RETENTION_MS;
      await this.ctx.storage.setAlarm(deleteAt);
      await this.trySyncIndex(nowMS);
      return;
    }
    if (conflictChanged) {
      await this.trySyncIndex(nowMS);
    }
  }

  private willNeedIndexSync(events: QueueEvent[], prevVerdict: string, hadConflict: boolean): boolean {
    if (events.length === 0) return false;
    for (let i = 0; i < events.length; i++) {
      const t = events[i]!.type;
      if (t === "initialized" || t === "finalized") return true;
      if (t === "conflicted" && !hadConflict) return true;
    }
    if (prevVerdict === "running") {
      // Check conservatively for reported/expired events that might complete the build.
      const last = events[events.length - 1]!;
      if (last.type === "reported" && last.reported?.close_lease) {
        const remaining = this.state.counts.pending + this.state.counts.leased;
        let resolvedNow = 0;
        for (const r of last.reported.results) {
          if (r.disposition === "accepted") {
            if (r.outcome !== "error" || r.generation >= this.state.config.max_attempts) {
              resolvedNow++;
            }
          }
        }
        // Account for any expired events in the same batch that pushed tests to unresolved.
        let expiredUnresolved = 0;
        for (let i = 0; i < events.length - 1; i++) {
          const ev = events[i]!;
          if (ev.type === "expired" && ev.expired) {
            const l = this.state.leases[ev.expired.lease_id - 1];
            if (l) {
              for (const a of l.attempts) {
                const ts = this.state.tests[a.test_index];
                if (ts && ts.status === "leased" && ts.activeLeaseID === ev.expired.lease_id && ts.generation >= this.state.config.max_attempts) {
                  expiredUnresolved++;
                }
              }
            }
          }
        }
        return resolvedNow + expiredUnresolved >= remaining;
      }
      if (last.type === "expired") {
        // Rare path: check if expiry exhausts the final active attempts.
        return this.state.counts.pending === 0;
      }
    }
    return false;
  }

  async init(nowMS: number, buildID: string, rawBody: string): Promise<DoHttpReply> {
    await this.beforeRequest(nowMS);
    const parsed = parseInitBody(buildID, rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }
    return this.handleInitCmd(nowMS, parsed.value);
  }

  private async handleInitCmd(nowMS: number, cmd: InitCommand): Promise<DoHttpReply> {
    const prevVerdict = this.state.initialized ? stateVerdict(this.state) : "running";
    const hadConflict = this.state.conflicts.length > 0;
    const out = decideInit(this.state, nowMS, cmd);
    const markDirty = this.willNeedIndexSync(out.events, prevVerdict, hadConflict);
    if (!this.persistAndApply(out.events, markDirty, true)) {
      return errReply(wireErr(503, ErrCodeStorageError, "failed to persist events"));
    }
    const created = !out.error && out.response !== null && out.response.created;
    await this.afterTransition(prevVerdict, hadConflict, created, nowMS);

    if (out.error) {
      return errReply(wireErr(out.error.httpStatus, out.error.code, out.error.message));
    }
    return okReply(out.response);
  }

  async lease(nowMS: number, rawBody: string): Promise<DoHttpReply> {
    await this.beforeRequest(nowMS);
    const parsed = parseLeaseBody(rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }
    const prevVerdict = this.state.initialized ? stateVerdict(this.state) : "running";
    const hadConflict = this.state.conflicts.length > 0;
    const out = decideLease(this.state, nowMS, parsed.value);
    const markDirty = this.willNeedIndexSync(out.events, prevVerdict, hadConflict);
    if (!this.persistAndApply(out.events, markDirty)) {
      return errReply(wireErr(503, ErrCodeStorageError, "failed to persist events"));
    }
    if (!out.error && out.response && out.response.status === "leased") {
      extendLease(this.state, out.response.lease_id, out.response.deadline_ms);
    }
    await this.afterTransition(prevVerdict, hadConflict, false, nowMS);

    if (out.error) {
      return errReply(wireErr(out.error.httpStatus, out.error.code, out.error.message));
    }
    return okReply(out.response);
  }

  async heartbeat(nowMS: number, rawBody: string): Promise<DoHttpReply> {
    await this.beforeRequest(nowMS);
    const parsed = parseHeartbeatBody(rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }
    const prevVerdict = this.state.initialized ? stateVerdict(this.state) : "running";
    const hadConflict = this.state.conflicts.length > 0;
    const out = decideHeartbeat(this.state, nowMS, parsed.value);
    // Healthy heartbeats emit zero events and never touch SQLite.
    const markDirty = this.willNeedIndexSync(out.events, prevVerdict, hadConflict);
    if (!this.persistAndApply(out.events, markDirty)) {
      return errReply(wireErr(503, ErrCodeStorageError, "failed to persist events"));
    }
    if (!out.error && out.response && out.response.held) {
      extendLease(this.state, parsed.value.lease_id, out.response.deadline_ms);
    }
    await this.afterTransition(prevVerdict, hadConflict, false, nowMS);

    if (out.error) {
      return errReply(wireErr(out.error.httpStatus, out.error.code, out.error.message));
    }
    return okReply(out.response);
  }

  async report(nowMS: number, rawBody: string): Promise<DoHttpReply> {
    await this.beforeRequest(nowMS);
    const parsed = parseReportBody(rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }
    const prevVerdict = this.state.initialized ? stateVerdict(this.state) : "running";
    const hadConflict = this.state.conflicts.length > 0;
    const out = decideReport(this.state, nowMS, parsed.value);
    const markDirty = this.willNeedIndexSync(out.events, prevVerdict, hadConflict);
    if (!this.persistAndApply(out.events, markDirty)) {
      return errReply(wireErr(503, ErrCodeStorageError, "failed to persist events"));
    }
    await this.afterTransition(prevVerdict, hadConflict, false, nowMS);

    if (out.error) {
      return errReply(wireErr(out.error.httpStatus, out.error.code, out.error.message));
    }
    return okReply(out.response);
  }

  async status(nowMS: number): Promise<DoHttpReply> {
    await this.beforeRequest(nowMS);
    const prevVerdict = this.state.initialized ? stateVerdict(this.state) : "running";
    const hadConflict = this.state.conflicts.length > 0;
    const out = decideStatus(this.state, nowMS);
    const markDirty = this.willNeedIndexSync(out.events, prevVerdict, hadConflict);
    if (!this.persistAndApply(out.events, markDirty)) {
      return errReply(wireErr(503, ErrCodeStorageError, "failed to persist events"));
    }
    await this.afterTransition(prevVerdict, hadConflict, false, nowMS);

    if (out.error) {
      return errReply(wireErr(out.error.httpStatus, out.error.code, out.error.message));
    }
    return okReply(out.response);
  }

  async listTests(nowMS: number, cmd: ListTestsCommand): Promise<DoHttpReply> {
    await this.beforeRequest(nowMS);
    const prevVerdict = this.state.initialized ? stateVerdict(this.state) : "running";
    const hadConflict = this.state.conflicts.length > 0;
    const out = decideListTests(this.state, nowMS, cmd);
    const markDirty = this.willNeedIndexSync(out.events, prevVerdict, hadConflict);
    if (!this.persistAndApply(out.events, markDirty)) {
      return errReply(wireErr(503, ErrCodeStorageError, "failed to persist events"));
    }
    await this.afterTransition(prevVerdict, hadConflict, false, nowMS);

    if (out.error) {
      return errReply(wireErr(out.error.httpStatus, out.error.code, out.error.message));
    }
    return okReply(out.response);
  }

  // Lifecycle alarm:
  //   1. While running: at build deadline, finalize to unresolved, update BuildIndex,
  //      and schedule cleanup for +7 days.
  //   2. Once terminal: at +7 days, deleteAll() and reset in-memory state.
  // Idempotent when delivered more than once.
  override async alarm(): Promise<void> {
    await this.runAlarmAt(Date.now());
  }

  // Exposed for deterministic alarm tests at explicit timestamps without relying on wall clock.
  async runAlarmAt(nowMS: number): Promise<void> {
    if (!this.state.initialized) {
      return;
    }
    const verdict = stateVerdict(this.state);
    if (verdict === "running") {
      if (nowMS >= this.state.deadlineMS) {
        const out = decideStatus(this.state, nowMS);
        if (!this.persistAndApply(out.events, true)) {
          throw new Error("failed to persist deadline finalization");
        }
        const deleteAt = (this.state.finishedAtMS || nowMS) + RETENTION_MS;
        await this.ctx.storage.setAlarm(deleteAt);
        await this.trySyncIndex(nowMS);
      } else {
        await this.ctx.storage.setAlarm(this.state.deadlineMS);
      }
      return;
    }

    const deleteAt = (this.state.finishedAtMS || this.state.lastSeenMS) + RETENTION_MS;
    if (nowMS >= deleteAt) {
      await this.ctx.storage.deleteAll();
      this.resetMemory();
      return;
    }

    if (this.indexDirty) {
      await this.trySyncIndex(nowMS);
    }
    await this.ctx.storage.setAlarm(deleteAt);
  }
}
