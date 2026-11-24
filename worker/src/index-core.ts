import { type Verdict, compareUTF8 } from "./core/index.ts";

export const MAX_INDEX_BUILDS = 200;

export interface BuildSummary {
  build_id: string;
  verdict: Verdict;
  created_at_ms: number;
  finished_at_ms: number | null;
  test_count: number;
  failed_count: number;
  unresolved_count: number;
  has_conflicts: boolean;
}

export interface IndexEntry extends BuildSummary {
  expires_at_ms: number;
}

export function sortIndexEntries(entries: IndexEntry[]): IndexEntry[] {
  return entries.slice().sort((a, b) => {
    if (a.created_at_ms !== b.created_at_ms) {
      return b.created_at_ms - a.created_at_ms;
    }
    return compareUTF8(a.build_id, b.build_id);
  });
}

export function pruneAndCapIndex(entries: Iterable<IndexEntry>, nowMS: number): IndexEntry[] {
  const live: IndexEntry[] = [];
  for (const e of entries) {
    if (e.expires_at_ms > nowMS) {
      live.push(e);
    }
  }
  const sorted = sortIndexEntries(live);
  if (sorted.length > MAX_INDEX_BUILDS) {
    sorted.length = MAX_INDEX_BUILDS;
  }
  return sorted;
}

export function toBuildSummary(e: IndexEntry): BuildSummary {
  return {
    build_id: e.build_id,
    verdict: e.verdict,
    created_at_ms: e.created_at_ms,
    finished_at_ms: e.finished_at_ms,
    test_count: e.test_count,
    failed_count: e.failed_count,
    unresolved_count: e.unresolved_count,
    has_conflicts: e.has_conflicts,
  };
}
