import { DurableObject } from "cloudflare:workers";
import type { Verdict } from "./core/index.ts";
import {
  type BuildSummary,
  type IndexEntry,
  MAX_INDEX_BUILDS,
  pruneAndCapIndex,
  toBuildSummary,
} from "./index-core.ts";

interface BuildRow extends Record<string, SqlStorageValue> {
  build_id: string;
  verdict: string;
  created_at_ms: number;
  finished_at_ms: number | null;
  test_count: number;
  failed_count: number;
  unresolved_count: number;
  has_conflicts: number;
  expires_at_ms: number;
}

// Global SQLite Durable Object tracking summaries for up to MAX_INDEX_BUILDS
// recent builds so GET /api/v1/builds never fans out to individual BuildDOs.
export class BuildIndex extends DurableObject<Env> {
  private entries = new Map<string, IndexEntry>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS builds (
          build_id TEXT PRIMARY KEY,
          verdict TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL,
          finished_at_ms INTEGER,
          test_count INTEGER NOT NULL,
          failed_count INTEGER NOT NULL,
          unresolved_count INTEGER NOT NULL,
          has_conflicts INTEGER NOT NULL,
          expires_at_ms INTEGER NOT NULL
        )
      `);

      const rows = this.ctx.storage.sql.exec<BuildRow>("SELECT * FROM builds").toArray();
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i]!;
        this.entries.set(r.build_id, {
          build_id: r.build_id,
          verdict: r.verdict as Verdict,
          created_at_ms: r.created_at_ms,
          finished_at_ms: r.finished_at_ms,
          test_count: r.test_count,
          failed_count: r.failed_count,
          unresolved_count: r.unresolved_count,
          has_conflicts: r.has_conflicts !== 0,
          expires_at_ms: r.expires_at_ms,
        });
      }
    });
  }

  async upsert(entry: IndexEntry, nowOverride?: number): Promise<void> {
    const nowMS = nowOverride ?? Date.now();
    const expired: string[] = [];
    const nextEntries = new Map<string, IndexEntry>();
    for (const [id, e] of this.entries) {
      if (e.expires_at_ms <= nowMS) {
        expired.push(id);
      } else {
        nextEntries.set(id, e);
      }
    }
    nextEntries.set(entry.build_id, entry);

    const evicted: string[] = [];
    if (nextEntries.size > MAX_INDEX_BUILDS) {
      const kept = pruneAndCapIndex(nextEntries.values(), nowMS);
      const keptSet = new Set(kept.map((e) => e.build_id));
      for (const id of Array.from(nextEntries.keys())) {
        if (!keptSet.has(id)) {
          evicted.push(id);
          nextEntries.delete(id);
        }
      }
    }

    this.ctx.storage.transactionSync(() => {
      if (expired.length > 0) {
        this.ctx.storage.sql.exec("DELETE FROM builds WHERE expires_at_ms <= ?", nowMS);
      }
      this.ctx.storage.sql.exec(
        `INSERT OR REPLACE INTO builds (
          build_id, verdict, created_at_ms, finished_at_ms,
          test_count, failed_count, unresolved_count, has_conflicts, expires_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        entry.build_id,
        entry.verdict,
        entry.created_at_ms,
        entry.finished_at_ms,
        entry.test_count,
        entry.failed_count,
        entry.unresolved_count,
        entry.has_conflicts ? 1 : 0,
        entry.expires_at_ms,
      );
      for (let i = 0; i < evicted.length; i++) {
        this.ctx.storage.sql.exec("DELETE FROM builds WHERE build_id = ?", evicted[i]!);
      }
    });

    // Mutate in-memory map only after SQLite commits.
    this.entries = nextEntries;
  }

  async list(nowOverride?: number): Promise<BuildSummary[]> {
    const nowMS = nowOverride ?? Date.now();
    const expired: string[] = [];
    for (const [id, e] of this.entries) {
      if (e.expires_at_ms <= nowMS) {
        expired.push(id);
      }
    }

    if (expired.length > 0) {
      this.ctx.storage.sql.exec("DELETE FROM builds WHERE expires_at_ms <= ?", nowMS);
      for (let i = 0; i < expired.length; i++) {
        this.entries.delete(expired[i]!);
      }
    }

    return pruneAndCapIndex(this.entries.values(), nowMS).map(toBuildSummary);
  }
}
