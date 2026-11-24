import { DurableObject } from "cloudflare:workers";
import {
  type HeartbeatCommand,
  type HeartbeatResponse,
  type InitCommand,
  type InitResponse,
  type LeaseCommand,
  type LeaseResponse,
  type ListTestsCommand,
  type ListTestsResponse,
  type QueueEvent,
  type ReportCommand,
  type ReportResponse,
  type State,
  type StatusResponse,
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

// Keep each catalog chunk well under SQLite's 100 KB statement limit and 8 MB row limit.
// At ~65 bytes per realistic path + ~6 bytes per prediction, 1,000 tests ≈ 71 KB JSON.
export const DEFAULT_CATALOG_CHUNK_SIZE = 1000;
const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

export interface SqlMetrics {
  rowsRead: number;
  rowsWritten: number;
  databaseSize: number;
  schemaRowsRead: number;
  schemaRowsWritten: number;
  wakeRowsRead: number;
  initCatalogRowsWritten: number;
  initEventRowsWritten: number;
  eventRowCount: number;
  catalogChunkCount: number;
  maxCatalogChunkBytes: number;
  maxEventRowBytes: number;
  setAlarmCalls: number;
  wokeFromStorage: boolean;
}

export interface RpcResult<T> {
  status: number;
  code: string;
  response: T | null;
  eventsCount: number;
  rowsRead: number;
  rowsWritten: number;
}

interface CatalogChunkPayload {
  tests: string[];
  predictions_ms: (number | null)[];
}

export class BuildDOProto extends DurableObject<Env> {
  private state: State;
  private nextSeq = 1;
  private schemaRowsRead = 0;
  private schemaRowsWritten = 0;
  private wakeRowsRead = 0;
  private totalRowsRead = 0;
  private totalRowsWritten = 0;
  private initCatalogRowsWritten = 0;
  private initEventRowsWritten = 0;
  private eventRowCount = 0;
  private catalogChunkCount = 0;
  private maxCatalogChunkBytes = 0;
  private maxEventRowBytes = 0;
  private setAlarmCalls = 0;
  private wokeFromStorage = false;
  private pendingWakeReset = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.state = newState(defaultLimits());
    this.ctx.blockConcurrencyWhile(async () => {
      this.initSchemaAndReplay();
    });
  }

  private execCounted<T extends Record<string, SqlStorageValue>>(
    query: string,
    ...bindings: unknown[]
  ): { rows: T[]; rowsRead: number; rowsWritten: number } {
    const cursor = this.ctx.storage.sql.exec<T>(query, ...bindings);
    const rows = cursor.toArray();
    return {
      rows,
      rowsRead: cursor.rowsRead,
      rowsWritten: cursor.rowsWritten,
    };
  }

  private initSchemaAndReplay(): void {
    // INTEGER PRIMARY KEY uses SQLite's rowid B-tree directly (no secondary index,
    // no sqlite_sequence table from AUTOINCREMENT), so each INSERT is 1 row write.
    const c1 = this.execCounted(
      "CREATE TABLE IF NOT EXISTS catalog_chunks (idx INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
    );
    const c2 = this.execCounted(
      "CREATE TABLE IF NOT EXISTS event_log (seq INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
    );
    this.schemaRowsRead = c1.rowsRead + c2.rowsRead;
    this.schemaRowsWritten = c1.rowsWritten + c2.rowsWritten;
    this.totalRowsRead += this.schemaRowsRead;
    this.totalRowsWritten += this.schemaRowsWritten;

    const evRes = this.execCounted<{ seq: number; payload: string }>(
      "SELECT seq, payload FROM event_log ORDER BY seq ASC",
    );
    this.wakeRowsRead += evRes.rowsRead;
    this.totalRowsRead += evRes.rowsRead;

    if (evRes.rows.length === 0) {
      return;
    }

    this.wokeFromStorage = true;
    const catRes = this.execCounted<{ idx: number; payload: string }>(
      "SELECT idx, payload FROM catalog_chunks ORDER BY idx ASC",
    );
    this.wakeRowsRead += catRes.rowsRead;
    this.totalRowsRead += catRes.rowsRead;

    const tests: string[] = [];
    const predictions: (number | null)[] = [];
    this.catalogChunkCount = catRes.rows.length;
    for (let i = 0; i < catRes.rows.length; i++) {
      const raw = catRes.rows[i]!.payload;
      if (raw.length > this.maxCatalogChunkBytes) {
        this.maxCatalogChunkBytes = raw.length;
      }
      const chunk = JSON.parse(raw) as CatalogChunkPayload;
      for (let j = 0; j < chunk.tests.length; j++) {
        tests.push(chunk.tests[j]!);
        predictions.push(chunk.predictions_ms[j]!);
      }
    }

    this.eventRowCount = evRes.rows.length;
    for (let i = 0; i < evRes.rows.length; i++) {
      const row = evRes.rows[i]!;
      if (row.payload.length > this.maxEventRowBytes) {
        this.maxEventRowBytes = row.payload.length;
      }
      const events = JSON.parse(row.payload) as QueueEvent[];
      for (let j = 0; j < events.length; j++) {
        const ev = events[j]!;
        if (ev.type === "initialized" && ev.initialized) {
          ev.initialized.tests = tests;
          ev.initialized.predictions_ms = predictions;
        }
        applyEvent(this.state, ev);
      }
      this.nextSeq = row.seq + 1;
    }
    this.pendingWakeReset = true;
  }

  private ensureWakeReset(nowMS: number): void {
    if (this.pendingWakeReset) {
      this.pendingWakeReset = false;
      if (this.state.initialized && stateVerdict(this.state) === "running") {
        resetActiveLeaseDeadlines(this.state, nowMS);
      }
    }
  }

  private persistAndApply(
    events: QueueEvent[],
    chunkSize = DEFAULT_CATALOG_CHUNK_SIZE,
  ): { rowsRead: number; rowsWritten: number } {
    if (events.length === 0) {
      return { rowsRead: 0, rowsWritten: 0 };
    }

    let opRowsRead = 0;
    let opRowsWritten = 0;

    this.ctx.storage.transactionSync(() => {
      const first = events[0]!;
      if (first.type === "initialized" && first.initialized) {
        const init = first.initialized;
        const n = init.tests.length;
        let chunkIdx = 0;
        for (let offset = 0; offset < n; offset += chunkSize) {
          const end = Math.min(offset + chunkSize, n);
          const payload = JSON.stringify({
            tests: init.tests.slice(offset, end),
            predictions_ms: init.predictions_ms.slice(offset, end),
          });
          if (payload.length > this.maxCatalogChunkBytes) {
            this.maxCatalogChunkBytes = payload.length;
          }
          const r = this.execCounted(
            "INSERT INTO catalog_chunks (idx, payload) VALUES (?, ?)",
            chunkIdx,
            payload,
          );
          opRowsRead += r.rowsRead;
          opRowsWritten += r.rowsWritten;
          this.initCatalogRowsWritten += r.rowsWritten;
          chunkIdx++;
        }
        this.catalogChunkCount = chunkIdx;

        // Store initialized header without duplicating the catalog in event_log.
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
        const evPayload = JSON.stringify(headerRecord);
        if (evPayload.length > this.maxEventRowBytes) {
          this.maxEventRowBytes = evPayload.length;
        }
        const r = this.execCounted(
          "INSERT INTO event_log (seq, payload) VALUES (?, ?)",
          this.nextSeq,
          evPayload,
        );
        opRowsRead += r.rowsRead;
        opRowsWritten += r.rowsWritten;
        this.initEventRowsWritten += r.rowsWritten;
        this.nextSeq++;
        this.eventRowCount++;
      } else {
        const evPayload = JSON.stringify(events);
        if (evPayload.length > this.maxEventRowBytes) {
          this.maxEventRowBytes = evPayload.length;
        }
        const r = this.execCounted(
          "INSERT INTO event_log (seq, payload) VALUES (?, ?)",
          this.nextSeq,
          evPayload,
        );
        opRowsRead += r.rowsRead;
        opRowsWritten += r.rowsWritten;
        this.nextSeq++;
        this.eventRowCount++;
      }
    });

    for (let i = 0; i < events.length; i++) {
      applyEvent(this.state, events[i]!);
    }

    this.totalRowsRead += opRowsRead;
    this.totalRowsWritten += opRowsWritten;
    return { rowsRead: opRowsRead, rowsWritten: opRowsWritten };
  }

  private async updateAlarmIfNeeded(wasRunning: boolean, nowMS: number, created: boolean): Promise<void> {
    if (created) {
      await this.ctx.storage.setAlarm(this.state.deadlineMS);
      this.setAlarmCalls++;
      return;
    }
    if (wasRunning && stateVerdict(this.state) !== "running") {
      await this.ctx.storage.setAlarm(nowMS + RETENTION_MS);
      this.setAlarmCalls++;
    }
  }

  async init(
    nowMS: number,
    cmd: InitCommand,
    chunkSize = DEFAULT_CATALOG_CHUNK_SIZE,
  ): Promise<RpcResult<InitResponse>> {
    this.ensureWakeReset(nowMS);
    const wasRunning = this.state.initialized && stateVerdict(this.state) === "running";
    const out = decideInit(this.state, nowMS, cmd);
    const io = this.persistAndApply(out.events, chunkSize);
    if (!out.error && out.response) {
      await this.updateAlarmIfNeeded(wasRunning, nowMS, out.response.created);
    }
    return {
      status: out.error ? out.error.httpStatus : 200,
      code: out.error ? out.error.code : "",
      response: out.response,
      eventsCount: out.events.length,
      rowsRead: io.rowsRead,
      rowsWritten: io.rowsWritten,
    };
  }

  async lease(nowMS: number, cmd: LeaseCommand): Promise<RpcResult<LeaseResponse>> {
    this.ensureWakeReset(nowMS);
    const wasRunning = this.state.initialized && stateVerdict(this.state) === "running";
    const out = decideLease(this.state, nowMS, cmd);
    const io = this.persistAndApply(out.events);
    if (!out.error && out.response && out.response.status === "leased") {
      extendLease(this.state, out.response.lease_id, out.response.deadline_ms);
    }
    if (out.events.length > 0) {
      await this.updateAlarmIfNeeded(wasRunning, nowMS, false);
    }
    return {
      status: out.error ? out.error.httpStatus : 200,
      code: out.error ? out.error.code : "",
      response: out.response,
      eventsCount: out.events.length,
      rowsRead: io.rowsRead,
      rowsWritten: io.rowsWritten,
    };
  }

  async heartbeat(nowMS: number, cmd: HeartbeatCommand): Promise<RpcResult<HeartbeatResponse>> {
    this.ensureWakeReset(nowMS);
    const wasRunning = this.state.initialized && stateVerdict(this.state) === "running";
    const out = decideHeartbeat(this.state, nowMS, cmd);
    // Healthy heartbeats produce zero events and never touch SQLite.
    const io = this.persistAndApply(out.events);
    if (!out.error && out.response && out.response.held) {
      extendLease(this.state, cmd.lease_id, out.response.deadline_ms);
    }
    if (out.events.length > 0) {
      await this.updateAlarmIfNeeded(wasRunning, nowMS, false);
    }
    return {
      status: out.error ? out.error.httpStatus : 200,
      code: out.error ? out.error.code : "",
      response: out.response,
      eventsCount: out.events.length,
      rowsRead: io.rowsRead,
      rowsWritten: io.rowsWritten,
    };
  }

  async report(nowMS: number, cmd: ReportCommand): Promise<RpcResult<ReportResponse>> {
    this.ensureWakeReset(nowMS);
    const wasRunning = this.state.initialized && stateVerdict(this.state) === "running";
    const out = decideReport(this.state, nowMS, cmd);
    const io = this.persistAndApply(out.events);
    if (out.events.length > 0) {
      await this.updateAlarmIfNeeded(wasRunning, nowMS, false);
    }
    return {
      status: out.error ? out.error.httpStatus : 200,
      code: out.error ? out.error.code : "",
      response: out.response,
      eventsCount: out.events.length,
      rowsRead: io.rowsRead,
      rowsWritten: io.rowsWritten,
    };
  }

  async status(nowMS: number): Promise<RpcResult<StatusResponse>> {
    this.ensureWakeReset(nowMS);
    const wasRunning = this.state.initialized && stateVerdict(this.state) === "running";
    const out = decideStatus(this.state, nowMS);
    const io = this.persistAndApply(out.events);
    if (out.events.length > 0) {
      await this.updateAlarmIfNeeded(wasRunning, nowMS, false);
    }
    return {
      status: out.error ? out.error.httpStatus : 200,
      code: out.error ? out.error.code : "",
      response: out.response,
      eventsCount: out.events.length,
      rowsRead: io.rowsRead,
      rowsWritten: io.rowsWritten,
    };
  }

  async listTests(nowMS: number, cmd: ListTestsCommand): Promise<RpcResult<ListTestsResponse>> {
    this.ensureWakeReset(nowMS);
    const wasRunning = this.state.initialized && stateVerdict(this.state) === "running";
    const out = decideListTests(this.state, nowMS, cmd);
    const io = this.persistAndApply(out.events);
    if (out.events.length > 0) {
      await this.updateAlarmIfNeeded(wasRunning, nowMS, false);
    }
    return {
      status: out.error ? out.error.httpStatus : 200,
      code: out.error ? out.error.code : "",
      response: out.response,
      eventsCount: out.events.length,
      rowsRead: io.rowsRead,
      rowsWritten: io.rowsWritten,
    };
  }

  async getMetrics(): Promise<SqlMetrics> {
    return {
      rowsRead: this.totalRowsRead,
      rowsWritten: this.totalRowsWritten,
      databaseSize: this.ctx.storage.sql.databaseSize,
      schemaRowsRead: this.schemaRowsRead,
      schemaRowsWritten: this.schemaRowsWritten,
      wakeRowsRead: this.wakeRowsRead,
      initCatalogRowsWritten: this.initCatalogRowsWritten,
      initEventRowsWritten: this.initEventRowsWritten,
      eventRowCount: this.eventRowCount,
      catalogChunkCount: this.catalogChunkCount,
      maxCatalogChunkBytes: this.maxCatalogChunkBytes,
      maxEventRowBytes: this.maxEventRowBytes,
      setAlarmCalls: this.setAlarmCalls,
      wokeFromStorage: this.wokeFromStorage,
    };
  }

  async deleteAllData(): Promise<{ sizeAfter: number }> {
    await this.ctx.storage.deleteAll();
    return { sizeAfter: this.ctx.storage.sql.databaseSize };
  }

  override async alarm(): Promise<void> {
    // Prototype no-op alarm handler so workerd never logs missing alarm() during tests.
  }
}

export default {
  fetch(): Response {
    return new Response("bunpo-queue-proto", { status: 200 });
  },
};
