// Pure dashboard logic (no DOM, no network, no wall clock).

const BUILD_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export const BASE_POLL_MS = 10_000;
export const MAX_POLL_MS = 60_000;
export const MAX_POLL_FAILURES = 5;
export const PAGE_LIMIT = 200;

const STATUS_META = {
  passed: { symbol: "✓", label: "passed" },
  failed: { symbol: "✗", label: "failed" },
  unresolved: { symbol: "!", label: "unresolved" },
  error: { symbol: "!", label: "error" },
  expired: { symbol: "⏱", label: "expired" },
  running: { symbol: "…", label: "running" },
  leased: { symbol: "…", label: "leased" },
  skipped: { symbol: "–", label: "skipped" },
  pending: { symbol: "·", label: "pending" },
};

export function statusMeta(status) {
  return STATUS_META[status] || { symbol: "?", label: String(status || "unknown") };
}

export function isValidBuildID(id) {
  return typeof id === "string" && BUILD_ID_RE.test(id) && !id.includes("..");
}

export function parseRoute(pathname) {
  if (pathname === "/" || pathname === "/index.html") return { view: "builds" };
  if (pathname.startsWith("/builds/")) {
    const raw = pathname.slice("/builds/".length);
    if (!raw.includes("/")) {
      try {
        const id = decodeURIComponent(raw);
        if (isValidBuildID(id)) return { view: "build", buildID: id };
      } catch {
        return { view: "not_found" };
      }
    }
  }
  return { view: "not_found" };
}

export function buildPath(buildID) {
  return "/builds/" + encodeURIComponent(buildID);
}

export function formatDuration(ms) {
  if (typeof ms !== "number" || !Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1_000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)} s`;
  const sec = Math.floor(ms / 1_000);
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return h > 0 ? `${h}h ${String(m).padStart(2, "0")}m` : `${m}m ${String(sec % 60).padStart(2, "0")}s`;
}

// Untimed attempts carry duration_ms=0 on the wire; show wall time when positive, else omit.
export function formatAttemptDuration(a) {
  if (a?.timed && typeof a.duration_ms === "number" && a.duration_ms >= 0) return formatDuration(a.duration_ms);
  if (a?.leased_at_ms > 0 && a?.finished_at_ms > a.leased_at_ms) return formatDuration(a.finished_at_ms - a.leased_at_ms);
  return "";
}

export function formatTimeLeft(deadlineMS, nowMS, verdict) {
  if (verdict !== "running") return "Finished";
  if (typeof deadlineMS !== "number") return "—";
  return deadlineMS <= nowMS ? "Deadline reached" : `${formatDuration(deadlineMS - nowMS)} left`;
}

export function formatTimestamp(ms) {
  if (typeof ms !== "number" || !Number.isFinite(ms) || ms <= 0) return "—";
  return new Date(ms).toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

export function sortBuilds(builds) {
  return [...builds].sort(
    (a, b) => b.created_at_ms - a.created_at_ms || (a.build_id < b.build_id ? -1 : a.build_id > b.build_id ? 1 : 0),
  );
}

// Guards async fetches with one monotonic generation bumped on navigation and sign-out.
export function createGuardedLoader(fetchJSON) {
  let gen = 0;
  return {
    invalidate: () => ++gen,
    current: () => gen,
    isCurrent: (g) => g === gen,
    async fetch(path, expectedGen) {
      if (expectedGen !== gen) return { stale: true };
      try {
        const value = await fetchJSON(path);
        return expectedGen === gen ? { stale: false, ok: true, value } : { stale: true };
      } catch (error) {
        return expectedGen === gen ? { stale: false, ok: false, error } : { stale: true };
      }
    },
  };
}

export async function fetchBuildInitial(loader, gen, buildID, testsQS, onStatus) {
  const enc = encodeURIComponent(buildID);
  const st = await loader.fetch(`/api/v1/builds/${enc}`, gen);
  if (st.stale || !st.ok) return st;
  if (onStatus) onStatus(st.value);
  const [tl, tests] = await Promise.all([
    loader.fetch(`/api/v1/builds/${enc}/tests?cursor=0&limit=${PAGE_LIMIT}`, gen),
    loader.fetch(`/api/v1/builds/${enc}/tests?${testsQS}`, gen),
  ]);
  if (tl.stale || tests.stale || !loader.isCurrent(gen)) return { stale: true };
  if (!tl.ok) return tl;
  if (!tests.ok) return tests;
  return { stale: false, ok: true, value: { status: st.value, timeline: tl.value, tests: tests.value } };
}

// Resets the test list to fresh page 1 on refresh so later pages never go stale or miss newly matching tests.
export function replaceWithRefreshedPage(freshPage) {
  return {
    tests: freshPage?.tests || [],
    nextCursor: freshPage?.next_cursor ?? null,
  };
}

export function buildTimeline(status, tests, nowMS) {
  const runnerByID = new Map((status?.runners || []).map((r) => [r.runner_id, r]));
  const leases = new Map();

  for (const t of tests || []) {
    for (const a of t.attempts || []) {
      const fin = a.finished_at_ms > 0 ? a.finished_at_ms : Math.max(nowMS, a.leased_at_ms);
      let entry = leases.get(a.lease_id);
      if (!entry) {
        const r = runnerByID.get(a.runner_id);
        entry = {
          leaseID: a.lease_id,
          runnerID: a.runner_id,
          machineIndex: r ? r.machine_index : null,
          startMS: a.leased_at_ms,
          endMS: fin,
          counts: { passed: 0, failed: 0, skipped: 0, error: 0, expired: 0, leased: 0 },
          tests: [],
        };
        leases.set(a.lease_id, entry);
      }
      if (a.leased_at_ms < entry.startMS) entry.startMS = a.leased_at_ms;
      if (fin > entry.endMS) entry.endMS = fin;
      entry.counts[a.status] = (entry.counts[a.status] || 0) + 1;
      entry.tests.push({ test: t.test, generation: a.generation, status: a.status, durationText: formatAttemptDuration(a) });
    }
  }

  const laneMap = new Map();
  for (const r of status?.runners || []) {
    laneMap.set(r.runner_id, { runnerID: r.runner_id, machineIndex: r.machine_index, firstSeenMS: r.first_seen_ms, bars: [] });
  }

  let minMS = status?.created_at_ms > 0 ? status.created_at_ms : Infinity;
  let maxMS = status?.finished_at_ms > 0 ? status.finished_at_ms : status?.last_seen_ms || 0;

  for (const entry of leases.values()) {
    if (entry.startMS < minMS) minMS = entry.startMS;
    if (entry.endMS > maxMS) maxMS = entry.endMS;
    let lane = laneMap.get(entry.runnerID);
    if (!lane) {
      lane = { runnerID: entry.runnerID, machineIndex: entry.machineIndex, firstSeenMS: entry.startMS, bars: [] };
      laneMap.set(entry.runnerID, lane);
    }
    const kind = leaseStatus(entry.counts);
    lane.bars.push({ ...entry, kind, meta: statusMeta(kind) });
  }

  if (!Number.isFinite(minMS)) return { minMS: 0, maxMS: 0, spanMS: 0, lanes: [] };
  if (maxMS <= minMS) maxMS = minMS + 1;
  const spanMS = maxMS - minMS;

  const lanes = [...laneMap.values()]
    .filter((l) => l.bars.length > 0)
    .sort((a, b) => {
      const am = a.machineIndex ?? Number.MAX_SAFE_INTEGER;
      const bm = b.machineIndex ?? Number.MAX_SAFE_INTEGER;
      if (am !== bm) return am - bm;
      if (a.firstSeenMS !== b.firstSeenMS) return a.firstSeenMS - b.firstSeenMS;
      return a.runnerID < b.runnerID ? -1 : a.runnerID > b.runnerID ? 1 : 0;
    });

  for (const lane of lanes) {
    lane.bars.sort((a, b) => a.startMS - b.startMS || a.leaseID - b.leaseID);
    for (const bar of lane.bars) {
      bar.xPct = ((bar.startMS - minMS) / spanMS) * 100;
      bar.wPct = Math.max(((bar.endMS - bar.startMS) / spanMS) * 100, 1.5);
      if (bar.xPct + bar.wPct > 100) bar.wPct = Math.max(0.8, 100 - bar.xPct);
    }
  }
  return { minMS, maxMS, spanMS, lanes };
}

function leaseStatus(c) {
  if (c.failed > 0) return "failed";
  if (c.error > 0) return "error";
  if (c.expired > 0) return "expired";
  if (c.leased > 0) return "leased";
  return c.passed > 0 ? "passed" : "skipped";
}

export function createPollState() {
  return { active: false, visible: true, verdict: null, inFlight: false, nextAtMS: null, lastPollMS: null, failures: 0, stoppedReason: null };
}

export function pollStep(state, ev) {
  const s = { ...state };
  const now = ev.nowMS;
  const actions = [];
  const schedule = (delayMS) => {
    const earliest = s.lastPollMS !== null ? s.lastPollMS + BASE_POLL_MS : now;
    const target = Math.max(now + delayMS, earliest);
    s.nextAtMS = target;
    actions.push({ type: "schedule", atMS: target, delayMS: Math.max(0, target - now) });
  };
  const cancel = () => {
    if (s.nextAtMS !== null) {
      s.nextAtMS = null;
      actions.push({ type: "cancel" });
    }
  };

  switch (ev.type) {
    case "watch":
      s.active = ev.verdict === "running";
      s.verdict = ev.verdict;
      s.failures = 0;
      s.stoppedReason = s.active ? null : "finished";
      s.lastPollMS = now;
      cancel();
      if (s.active && s.visible) schedule(BASE_POLL_MS);
      break;
    case "stop":
      s.active = false;
      s.inFlight = false;
      s.stoppedReason = ev.reason || "stopped";
      cancel();
      break;
    case "visibility":
      s.visible = Boolean(ev.visible);
      if (!s.visible) cancel();
      else if (s.active && s.verdict === "running" && !s.inFlight) {
        const elapsed = s.lastPollMS !== null ? now - s.lastPollMS : BASE_POLL_MS;
        schedule(elapsed >= BASE_POLL_MS ? 0 : BASE_POLL_MS - elapsed);
      }
      break;
    case "tick":
      if (s.active && s.visible && s.verdict === "running" && !s.inFlight && s.nextAtMS !== null && now >= s.nextAtMS) {
        s.nextAtMS = null;
        s.inFlight = true;
        s.lastPollMS = now;
        actions.push({ type: "fetch" });
      }
      break;
    case "response_ok":
      s.inFlight = false;
      s.verdict = ev.verdict;
      s.failures = 0;
      if (ev.verdict !== "running") {
        s.active = false;
        s.stoppedReason = "finished";
        cancel();
      } else if (ev.manual) {
        s.active = true;
        s.stoppedReason = null;
        s.lastPollMS = now;
        cancel();
        if (s.visible) schedule(BASE_POLL_MS);
      } else if (s.active && s.visible) {
        schedule(BASE_POLL_MS);
      }
      break;
    case "response_error": {
      s.inFlight = false;
      const code = ev.httpStatus;
      const retryable = code === 0 || code === 429 || (code >= 500 && code < 600);
      if (!retryable || s.failures + 1 >= MAX_POLL_FAILURES) {
        s.active = false;
        s.stoppedReason = retryable ? "max_retries" : "error";
        cancel();
        break;
      }
      s.failures++;
      const exp = Math.min(BASE_POLL_MS * 2 ** s.failures, MAX_POLL_MS);
      const retryAfter = typeof ev.retryAfterMS === "number" && ev.retryAfterMS > 0 ? ev.retryAfterMS : 0;
      if (s.active && s.visible) schedule(Math.min(Math.max(exp, retryAfter), MAX_POLL_MS));
      break;
    }
  }
  return { state: s, actions };
}
