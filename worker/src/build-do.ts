import { DurableObject } from "cloudflare:workers";
import { DEFAULT_WORKER_MAX_TESTS, parseMaxTests } from "./config.ts";
import {
  type ActiveLeaseSeed,
  type ListTestsCommand,
  type QueueError,
  type QueueEvent,
  type State,
  type Verdict,
  ErrCodeInvalidRequest,
  LeaseDeadlineTracker,
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
  parseSerializedDeadlines,
  stateVerdict,
} from "./core/index.ts";
import type { IndexEntry } from "./index-core.ts";
import {
  type WireError,
  parseHeartbeatBody,
  parseInitBody,
  parseLeaseBody,
  parseReportBody,
  wireErr,
} from "./wire.ts";

const CATALOG_CHUNK_SIZE = 1_000;
export const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
export const INDEX_RETRY_INTERVAL_MS = 60_000;
export const MAX_INDEX_RETRY_DELAY_MS = 60 * 60 * 1000;
export const MAX_INDEX_ALARM_RETRIES = 8;
const INDEX_NAME = "global";

export interface DoHttpReply {
  status: number;
  body: string;
}

interface CatalogChunkPayload {
  tests: string[];
  predictions_ms: (number | null)[];
}

interface IndexSyncState {
  verdict: Verdict;
  has_conflicts: boolean;
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

// Per-build SQLite Durable Object holding the append-only event log, chunked
// test catalog, throttled lease deadlines, and deadline/retention alarms.
export class BuildDO extends DurableObject<Env> {
  private state: State;
  private nextSeq = 1;
  private schemaReady = false;
  private readonly deadlines = new LeaseDeadlineTracker();
  private indexSynced: IndexSyncState | null = null;
  private indexRetryAttempts = 0;
  private lastIndexAttemptMS = 0;
  private indexSyncInFlight = false;
  private readonly maxTests: number;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const parsedMax = parseMaxTests(env.MAX_TESTS);
    this.maxTests = parsedMax.ok ? parsedMax.maxTests : DEFAULT_WORKER_MAX_TESTS;
    this.state = this.freshState();

    this.ctx.blockConcurrencyWhile(async () => {
      this.replayFromStorage();
      if (this.state.initialized) {
        const existingAlarm = await this.ctx.storage.getAlarm();
        if (existingAlarm === null) {
          const expected = this.expectedAlarmMS();
          if (expected !== null) {
            await this.ctx.storage.setAlarm(expected);
          }
        }
      }
    });
  }

  // Replay always uses the protocol limit (MaxTests = 20,000) so lowering
  // MAX_TESTS never breaks replay of already-created builds.
  private freshState(): State {
    return newState(defaultLimits());
  }

  private resetMemory(): void {
    this.state = this.freshState();
    this.nextSeq = 1;
    this.schemaReady = false;
    this.deadlines.clear();
    this.indexSynced = null;
    this.indexRetryAttempts = 0;
    this.lastIndexAttemptMS = 0;
    this.indexSyncInFlight = false;
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

  private expectedAlarmMS(): number | null {
    if (!this.state.initialized) {
      return null;
    }
    if (stateVerdict(this.state) === "running") {
      return this.state.deadlineMS;
    }
    return (this.state.finishedAtMS || this.state.lastSeenMS) + RETENTION_MS;
  }

  private replayFromStorage(): void {
    // Check sqlite_master first so reads on unknown build IDs leave storage
    // with zero application tables and zero writes.
    const tables = this.ctx.storage.sql
      .exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event_log'")
      .toArray();
    if (tables.length === 0) {
      return;
    }
    this.schemaReady = true;

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

    const metaRows = this.ctx.storage.sql
      .exec<{ k: string; v: string }>(
        "SELECT k, v FROM meta WHERE k IN ('lease_deadlines', 'index_synced', 'index_retry_attempts')",
      )
      .toArray();

    let storedDeadlinesRaw: string | null = null;
    for (let i = 0; i < metaRows.length; i++) {
      const row = metaRows[i]!;
      if (row.k === "lease_deadlines") {
        storedDeadlinesRaw = row.v;
      } else if (row.k === "index_synced") {
        const parsed = JSON.parse(row.v) as Partial<IndexSyncState>;
        if (typeof parsed.verdict === "string" && typeof parsed.has_conflicts === "boolean") {
          this.indexSynced = { verdict: parsed.verdict as Verdict, has_conflicts: parsed.has_conflicts };
        }
      } else if (row.k === "index_retry_attempts") {
        const n = Number(row.v);
        if (Number.isInteger(n) && n >= 0) {
          this.indexRetryAttempts = n;
        }
      }
    }

    if (this.state.initialized) {
      const activeSeeds: ActiveLeaseSeed[] = [];
      for (let i = 0; i < this.state.leases.length; i++) {
        const l = this.state.leases[i]!;
        if (l.status === "active") {
          activeSeeds.push({ leaseID: l.lease_id, grantedDeadlineMS: l.deadline_ms });
        }
      }

      const storedDeadlines = parseSerializedDeadlines(storedDeadlinesRaw);
      const effective = this.deadlines.restoreOnWake(
        activeSeeds,
        storedDeadlines,
        this.state.config.lease_ttl_ms,
      );
      for (const [id, effDeadline] of effective) {
        extendLease(this.state, id, effDeadline);
      }
    }
  }

  private needsIndexSync(): boolean {
    if (!this.state.initialized) {
      return false;
    }
    if (this.indexSynced === null) {
      return true;
    }
    const verdict = stateVerdict(this.state);
    const hasConflicts = this.state.conflicts.length > 0;
    return this.indexSynced.verdict !== verdict || this.indexSynced.has_conflicts !== hasConflicts;
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

  private async trySyncIndex(nowMS: number): Promise<boolean> {
    if (!this.state.initialized) {
      return true;
    }
    this.lastIndexAttemptMS = nowMS;
    this.indexSyncInFlight = true;
    const entry = this.buildIndexEntry();
    try {
      const idx = this.env.BUILD_INDEX.get(this.env.BUILD_INDEX.idFromName(INDEX_NAME));
      await idx.upsert(entry);

      const synced: IndexSyncState = { verdict: entry.verdict, has_conflicts: entry.has_conflicts };
      if (this.state.initialized && this.state.buildID === entry.build_id) {
        const hadRetries = this.indexRetryAttempts > 0;
        this.ensureSchema();
        this.ctx.storage.sql.exec(
          "INSERT OR REPLACE INTO meta (k, v) VALUES ('index_synced', ?)",
          JSON.stringify(synced),
        );
        if (hadRetries) {
          this.ctx.storage.sql.exec("DELETE FROM meta WHERE k = 'index_retry_attempts'");
          this.indexRetryAttempts = 0;
        }
        this.indexSynced = synced;
      }
      return true;
    } catch {
      // Leave indexSynced at its last confirmed state; alarm backoff or next wake repairs it.
      return false;
    } finally {
      this.indexSyncInFlight = false;
    }
  }

  // Schedules a bounded backoff retry alarm while BuildIndex is out of sync,
  // never past capMS (deadline while running, retention deletion once terminal).
  private async scheduleIndexRetryAlarm(nowMS: number, capMS: number): Promise<void> {
    if (!this.state.initialized || !this.needsIndexSync()) {
      return;
    }
    if (this.indexRetryAttempts >= MAX_INDEX_ALARM_RETRIES) {
      await this.ctx.storage.setAlarm(capMS);
      return;
    }
    const delay = Math.min(
      INDEX_RETRY_INTERVAL_MS * 2 ** this.indexRetryAttempts,
      MAX_INDEX_RETRY_DELAY_MS,
    );
    this.indexRetryAttempts++;
    this.ensureSchema();
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO meta (k, v) VALUES ('index_retry_attempts', ?)",
      String(this.indexRetryAttempts),
    );
    await this.ctx.storage.setAlarm(Math.min(capMS, nowMS + delay));
  }

  private scheduleBackgroundIndexSync(nowMS: number): void {
    if (!this.needsIndexSync() || this.indexSyncInFlight) {
      return;
    }
    if (this.lastIndexAttemptMS !== 0 && nowMS - this.lastIndexAttemptMS < INDEX_RETRY_INTERVAL_MS) {
      return;
    }
    this.lastIndexAttemptMS = nowMS;
    this.ctx.waitUntil(
      this.trySyncIndex(nowMS).then(async (ok) => {
        if (ok) {
          const expected = this.expectedAlarmMS();
          if (expected !== null) {
            await this.ctx.storage.setAlarm(expected);
          }
        }
      }),
    );
  }

  private persistAndApply(events: QueueEvent[], trustedInit = false): void {
    if (events.length === 0) {
      return;
    }

    try {
      this.ctx.storage.transactionSync(() => {
        this.ensureSchema();
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
      });
    } catch (err) {
      // Abort the DO so in-memory state can never diverge from SQLite after a storage failure,
      // and throw so the RPC rejects cleanly instead of resolving against a broken output gate.
      this.schemaReady = false;
      this.ctx.abort(err instanceof Error ? err.message : "storage transaction failed");
      throw err;
    }

    this.nextSeq++;
    for (let i = 0; i < events.length; i++) {
      const ev = events[i]!;
      applyEvent(this.state, ev, trustedInit && ev.type === "initialized" ? { trustedInit: true } : undefined);
      if (ev.type === "leased" && ev.leased) {
        this.deadlines.onGrant(ev.leased.lease_id, ev.leased.deadline_ms);
      } else if (ev.type === "reported" && ev.reported?.close_lease) {
        this.deadlines.onClose(ev.reported.lease_id);
      } else if (ev.type === "expired" && ev.expired) {
        this.deadlines.onClose(ev.expired.lease_id);
      } else if (ev.type === "finalized") {
        this.deadlines.clear();
      }
    }
  }

  private persistAndExtendLease(leaseID: number, promisedDeadlineMS: number, nowMS: number): void {
    const snap = this.deadlines.planExtend(leaseID, nowMS, this.state.config.lease_ttl_ms);
    if (snap !== null) {
      try {
        this.ctx.storage.transactionSync(() => {
          this.ensureSchema();
          this.ctx.storage.sql.exec(
            "INSERT OR REPLACE INTO meta (k, v) VALUES ('lease_deadlines', ?)",
            JSON.stringify(snap),
          );
        });
      } catch (err) {
        this.schemaReady = false;
        this.ctx.abort(err instanceof Error ? err.message : "storage transaction failed");
        throw err;
      }
      this.deadlines.commitPersist(snap);
    }
    extendLease(this.state, leaseID, promisedDeadlineMS);
  }

  private async executeDecision<T extends object>(
    nowMS: number,
    decide: () => { events: QueueEvent[]; response: T | null; error: QueueError | null },
    opts?: { trustedInit?: boolean; onSuccess?: (response: T) => void },
  ): Promise<DoHttpReply> {
    const wasInitialized = this.state.initialized;
    const prevVerdict = wasInitialized ? stateVerdict(this.state) : "running";
    const hadConflict = this.state.conflicts.length > 0;

    const out = decide();
    this.persistAndApply(out.events, opts?.trustedInit ?? false);
    if (!out.error && out.response && opts?.onSuccess) {
      opts.onSuccess(out.response);
    }

    // Issue setAlarm in the same synchronous turn as transactionSync so the alarm
    // commits atomically with the SQLite state transition.
    const created = !wasInitialized && this.state.initialized;
    const newVerdict = this.state.initialized ? stateVerdict(this.state) : "running";
    const becameTerminal = wasInitialized && prevVerdict === "running" && newVerdict !== "running";
    const conflictChanged = this.state.initialized && !hadConflict && this.state.conflicts.length > 0;

    if (created) {
      await this.ctx.storage.setAlarm(this.state.deadlineMS);
    } else if (becameTerminal) {
      await this.ctx.storage.setAlarm((this.state.finishedAtMS || nowMS) + RETENTION_MS);
    }

    if (created || becameTerminal || conflictChanged) {
      const synced = await this.trySyncIndex(nowMS);
      if (!synced && this.state.initialized) {
        const capMS =
          stateVerdict(this.state) === "running"
            ? this.state.deadlineMS
            : (this.state.finishedAtMS || nowMS) + RETENTION_MS;
        await this.scheduleIndexRetryAlarm(nowMS, capMS);
      }
    } else {
      this.scheduleBackgroundIndexSync(nowMS);
    }

    if (out.error) {
      return errReply(wireErr(out.error.httpStatus, out.error.code, out.error.message));
    }
    return {
      status: 200,
      body: JSON.stringify({ version: ProtocolVersion, ...out.response! }),
    };
  }

  async init(buildID: string, rawBody: string, nowOverride?: number): Promise<DoHttpReply> {
    const nowMS = nowOverride ?? Date.now();
    const parsed = parseInitBody(buildID, rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }

    if (!this.state.initialized && parsed.value.tests.length > this.maxTests) {
      return errReply(
        wireErr(
          400,
          ErrCodeInvalidRequest,
          `tests has ${parsed.value.tests.length} entries, over limit ${this.maxTests}`,
        ),
      );
    }

    return this.executeDecision(nowMS, () => decideInit(this.state, nowMS, parsed.value), {
      trustedInit: true,
    });
  }

  async lease(rawBody: string, nowOverride?: number): Promise<DoHttpReply> {
    const nowMS = nowOverride ?? Date.now();
    const parsed = parseLeaseBody(rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }
    return this.executeDecision(nowMS, () => decideLease(this.state, nowMS, parsed.value), {
      onSuccess: (res) => {
        if (res.status === "leased") {
          this.persistAndExtendLease(res.lease_id, res.deadline_ms, nowMS);
        }
      },
    });
  }

  async heartbeat(rawBody: string, nowOverride?: number): Promise<DoHttpReply> {
    const nowMS = nowOverride ?? Date.now();
    const parsed = parseHeartbeatBody(rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }
    return this.executeDecision(nowMS, () => decideHeartbeat(this.state, nowMS, parsed.value), {
      onSuccess: (res) => {
        if (res.held) {
          this.persistAndExtendLease(parsed.value.lease_id, res.deadline_ms, nowMS);
        }
      },
    });
  }

  async report(rawBody: string, nowOverride?: number): Promise<DoHttpReply> {
    const nowMS = nowOverride ?? Date.now();
    const parsed = parseReportBody(rawBody);
    if (!parsed.ok) {
      return errReply(parsed.error);
    }
    return this.executeDecision(nowMS, () => decideReport(this.state, nowMS, parsed.value));
  }

  async status(nowOverride?: number): Promise<DoHttpReply> {
    const nowMS = nowOverride ?? Date.now();
    return this.executeDecision(nowMS, () => decideStatus(this.state, nowMS));
  }

  async listTests(cmd: ListTestsCommand, nowOverride?: number): Promise<DoHttpReply> {
    const nowMS = nowOverride ?? Date.now();
    return this.executeDecision(nowMS, () => decideListTests(this.state, nowMS, cmd));
  }

  // Lifecycle alarm:
  //   1. While running: retries out-of-sync BuildIndex updates before deadline,
  //      then finalizes to unresolved at deadline and schedules +7d cleanup.
  //   2. Once terminal: retries out-of-sync BuildIndex updates before cleanup,
  //      then calls deleteAll() and resets in-memory state at +7d.
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
        this.persistAndApply(out.events);

        const deleteAt = (this.state.finishedAtMS || nowMS) + RETENTION_MS;
        // Same synchronous turn as persistAndApply so alarm + finalization commit atomically.
        await this.ctx.storage.setAlarm(deleteAt);

        const synced = await this.trySyncIndex(nowMS);
        if (!synced) {
          await this.scheduleIndexRetryAlarm(nowMS, deleteAt);
        }
      } else {
        if (this.needsIndexSync()) {
          const synced = await this.trySyncIndex(nowMS);
          if (!synced) {
            await this.scheduleIndexRetryAlarm(nowMS, this.state.deadlineMS);
            return;
          }
        }
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

    if (this.needsIndexSync()) {
      const synced = await this.trySyncIndex(nowMS);
      if (!synced) {
        await this.scheduleIndexRetryAlarm(nowMS, deleteAt);
        return;
      }
    }
    await this.ctx.storage.setAlarm(deleteAt);
  }
}
