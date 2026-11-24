import {
  BASE_POLL_MS,
  buildPath,
  buildTimeline,
  createPollState,
  formatDuration,
  formatTimeLeft,
  formatTimestamp,
  parseRoute,
  pollStep,
  sortBuilds,
  statusMeta,
} from "./core.js";

const TOKEN_KEY = "bunpo_queue_read_token";
const SVG_NS = "http://www.w3.org/2000/svg";
const PAGE_LIMIT = 200;

const $ = (id) => document.getElementById(id);

let pollState = createPollState();
let pollTimer = null;
let currentBuildID = null;
let currentStatus = null;
let currentFilter = "problems";
let loadedTests = [];
let nextCursor = null;
let timelineTests = [];
let timelineCursor = null;
let lastAnnouncement = "";

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = String(text);
  return node;
}

function svgEl(tag, attrs) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, String(v));
  return node;
}

function badge(status) {
  const m = statusMeta(status);
  return el("span", `badge badge-${status}`, `${m.symbol} ${m.label}`);
}

function announce(msg) {
  if (!msg || msg === lastAnnouncement) return;
  lastAnnouncement = msg;
  $("live-status").textContent = msg;
}

function showError(msg) {
  const banner = $("error-banner");
  banner.textContent = msg || "";
  banner.hidden = !msg;
}

function getToken() {
  try {
    return sessionStorage.getItem(TOKEN_KEY) || "";
  } catch {
    return "";
  }
}

function dispatchPoll(ev) {
  const res = pollStep(pollState, { ...ev, nowMS: Date.now() });
  pollState = res.state;
  for (const a of res.actions) {
    if (a.type === "cancel" && pollTimer !== null) {
      clearTimeout(pollTimer);
      pollTimer = null;
    } else if (a.type === "schedule") {
      if (pollTimer !== null) clearTimeout(pollTimer);
      pollTimer = setTimeout(() => {
        pollTimer = null;
        dispatchPoll({ type: "tick" });
      }, a.delayMS);
    } else if (a.type === "fetch") {
      void refreshActiveBuild();
    }
  }
  const label = $("refresh-state");
  if (!getToken() || !currentBuildID) label.textContent = "";
  else if (pollState.inFlight) label.textContent = "Refreshing…";
  else if (pollState.active && pollState.visible) {
    label.textContent =
      pollState.failures > 0 ? `Backing off (${pollState.failures})` : `Auto-refresh ${BASE_POLL_MS / 1000}s`;
  } else if (pollState.active) label.textContent = "Paused (tab hidden)";
  else if (pollState.stoppedReason === "finished") label.textContent = "Finished";
  else if (pollState.stoppedReason) label.textContent = "Auto-refresh stopped";
  else label.textContent = "";
}

async function apiFetch(path) {
  const resp = await fetch(path, {
    method: "GET",
    headers: { Authorization: `Bearer ${getToken()}` },
    cache: "no-store",
  });
  let body = null;
  try {
    body = await resp.json();
  } catch {
    body = null;
  }
  if (!resp.ok) {
    const retryAfterSec = Number(resp.headers.get("Retry-After") || "0");
    const err = new Error(body?.error?.message || `HTTP ${resp.status}`);
    err.httpStatus = resp.status;
    err.retryAfterMS = Number.isFinite(retryAfterSec) && retryAfterSec > 0 ? retryAfterSec * 1_000 : 0;
    throw err;
  }
  return body;
}

function showView(name) {
  const authed = Boolean(getToken());
  $("signout-btn").hidden = !authed;
  $("refresh-btn").hidden = !authed;
  $("auth-view").hidden = name !== "auth";
  $("builds-view").hidden = name !== "builds";
  $("build-view").hidden = name !== "build";
}

function signOut(message) {
  dispatchPoll({ type: "stop", reason: "signout" });
  try {
    sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    // Ignore storage restriction on sign-out.
  }
  currentBuildID = null;
  currentStatus = null;
  loadedTests = [];
  timelineTests = [];
  nextCursor = null;
  timelineCursor = null;
  for (const id of ["builds-tbody", "build-summary", "conflicts-list", "timeline-wrap", "lease-detail", "slowest-tbody", "tests-list"]) {
    $(id).replaceChildren();
  }
  $("lease-detail").hidden = true;
  $("token-input").value = "";
  showView("auth");
  showError(message || "");
  if (message) announce(message);
  $("token-input").focus();
}

function navigate(path, push = true) {
  if (push && window.location.pathname !== path) window.history.pushState(null, "", path);
  void renderRoute();
}

async function renderRoute() {
  showError("");
  dispatchPoll({ type: "stop", reason: "navigate" });
  if (!getToken()) {
    currentBuildID = null;
    showView("auth");
    return;
  }
  const route = parseRoute(window.location.pathname);
  if (route.view === "build") {
    currentBuildID = route.buildID;
    showView("build");
    await loadBuild(route.buildID);
    return;
  }
  currentBuildID = null;
  showView("builds");
  if (route.view === "not_found") showError("Page not found. Showing recent builds.");
  await loadBuilds();
}

async function loadBuilds() {
  try {
    const data = await apiFetch("/api/v1/builds");
    const rows = sortBuilds(data.builds || []);
    $("builds-count").textContent = `${rows.length} build${rows.length === 1 ? "" : "s"}`;
    const tbody = $("builds-tbody");
    tbody.replaceChildren();
    $("builds-empty").hidden = rows.length > 0;
    for (const b of rows) {
      const tr = el("tr");
      const link = el("a", "mono break-word", b.build_id);
      link.setAttribute("href", buildPath(b.build_id));
      link.addEventListener("click", (e) => {
        e.preventDefault();
        navigate(buildPath(b.build_id));
      });
      const tdID = el("td");
      tdID.append(link);
      const tdV = el("td");
      tdV.append(badge(b.verdict));
      if (b.has_conflicts) tdV.append(el("span", "muted", " (conflict)"));
      const dur = b.finished_at_ms ? formatDuration(b.finished_at_ms - b.created_at_ms) : "Running";
      tr.append(
        tdID,
        tdV,
        el("td", "mono", b.test_count),
        el("td", "mono", b.failed_count),
        el("td", "mono", b.unresolved_count),
        el("td", "mono", formatTimestamp(b.created_at_ms)),
        el("td", "mono", dur),
      );
      tbody.append(tr);
    }
    announce(`Loaded ${rows.length} recent builds.`);
  } catch (err) {
    handleFetchError(err, false);
  }
}

async function loadBuild(buildID) {
  currentStatus = null;
  loadedTests = [];
  nextCursor = null;
  timelineTests = [];
  timelineCursor = null;
  $("lease-detail").hidden = true;
  $("build-heading").textContent = buildID;
  $("timeline-legend").replaceChildren(
    ...["passed", "failed", "error", "expired", "leased", "skipped"].map((k) => badge(k)),
  );
  try {
    const st = await apiFetch(`/api/v1/builds/${encodeURIComponent(buildID)}`);
    currentStatus = st;
    renderBuildSummary(st);
    await Promise.all([loadTimelinePage(buildID, true), loadTestsPage(buildID, true)]);
    dispatchPoll({ type: "watch", verdict: st.verdict });
    announce(`Build ${buildID} is ${st.verdict}.`);
  } catch (err) {
    handleFetchError(err, false);
  }
}

async function refreshActiveBuild() {
  if (!currentBuildID) return;
  const buildID = currentBuildID;
  const prevLastSeen = currentStatus?.last_seen_ms ?? 0;
  const prevVerdict = currentStatus?.verdict ?? null;
  try {
    const st = await apiFetch(`/api/v1/builds/${encodeURIComponent(buildID)}`);
    if (buildID !== currentBuildID) return;
    currentStatus = st;
    renderBuildSummary(st);
    if (st.last_seen_ms !== prevLastSeen) {
      await loadTimelinePage(buildID, true);
      if (currentFilter !== "all" || loadedTests.length <= PAGE_LIMIT) await loadTestsPage(buildID, true);
    }
    dispatchPoll({ type: "response_ok", verdict: st.verdict });
    if (st.verdict !== prevVerdict) announce(`Build ${buildID} verdict changed to ${st.verdict}.`);
  } catch (err) {
    handleFetchError(err, true);
  }
}

function handleFetchError(err, fromPoll) {
  if (err.httpStatus === 401) {
    signOut("Read token rejected (401). Please sign in again.");
    return;
  }
  showError(`Request failed: ${err.message}`);
  if (fromPoll) {
    dispatchPoll({ type: "response_error", httpStatus: err.httpStatus || 0, retryAfterMS: err.retryAfterMS || 0 });
  }
  announce(`Error: ${err.message}`);
}

function renderBuildSummary(st) {
  $("build-verdict-slot").replaceChildren(badge(st.verdict));
  const c = st.counts;
  const summary = $("build-summary");
  summary.replaceChildren();
  for (const [label, val] of [
    ["Total", c.total],
    ["Passed", c.passed],
    ["Failed", c.failed],
    ["Unresolved", c.unresolved],
    ["Skipped", c.skipped],
    ["Leased / Pending", `${c.leased} / ${c.pending}`],
    ["Runners", st.runner_count],
    ["Time left", formatTimeLeft(st.deadline_ms, Date.now(), st.verdict)],
  ]) {
    const box = el("div", "summary-item");
    box.append(el("dt", "", label), el("dd", "mono", val));
    summary.append(box);
  }

  const cfList = $("conflicts-list");
  cfList.replaceChildren();
  const conflicts = st.conflicts || [];
  $("conflicts-panel").hidden = conflicts.length === 0;
  for (const cf of conflicts) cfList.append(el("li", "break-word", `${cf.runner_id}: ${cf.reason}`));
  if (st.conflicts_truncated) cfList.append(el("li", "muted", "Additional conflicts truncated at 8."));

  const slowBody = $("slowest-tbody");
  slowBody.replaceChildren();
  const slowest = st.slowest || [];
  $("slowest-empty").hidden = slowest.length > 0;
  for (const item of slowest) {
    const tr = el("tr");
    tr.append(el("td", "mono break-word", item.test), el("td", "mono", formatDuration(item.duration_ms)));
    slowBody.append(tr);
  }
}

async function loadTimelinePage(buildID, reset) {
  const cursor = reset ? 0 : timelineCursor;
  if (cursor === null) return;
  const data = await apiFetch(`/api/v1/builds/${encodeURIComponent(buildID)}/tests?cursor=${cursor}&limit=${PAGE_LIMIT}`);
  if (buildID !== currentBuildID) return;
  timelineTests = reset ? data.tests || [] : timelineTests.concat(data.tests || []);
  timelineCursor = data.next_cursor;
  renderTimeline();
}

function renderTimeline() {
  const wrap = $("timeline-wrap");
  wrap.replaceChildren();
  const tl = buildTimeline(currentStatus, timelineTests, Date.now());
  const total = currentStatus?.counts?.total ?? timelineTests.length;
  $("timeline-meta").textContent =
    timelineCursor !== null
      ? `Leases from first ${timelineTests.length} of ${total} tests`
      : `${tl.lanes.length} runner${tl.lanes.length === 1 ? "" : "s"}, span ${formatDuration(tl.spanMS)}`;
  if (tl.lanes.length === 0) {
    wrap.append(el("p", "muted", "No lease attempts recorded yet."));
    return;
  }

  const labelW = 130;
  const chartW = 670;
  const laneH = 28;
  const padTop = 20;
  const svg = svgEl("svg", {
    class: "timeline-svg",
    viewBox: `0 0 ${labelW + chartW + 10} ${padTop + tl.lanes.length * laneH + 10}`,
    role: "img",
    "aria-label": `Per-machine lease timeline across ${tl.lanes.length} runners`,
  });
  for (const [frac, anchor] of [[0, "start"], [0.5, "middle"], [1, "end"]]) {
    const t = svgEl("text", { x: (labelW + frac * chartW).toFixed(1), y: 13, class: "axis-label", "text-anchor": anchor });
    t.textContent = formatDuration(tl.spanMS * frac);
    svg.append(t);
  }

  tl.lanes.forEach((lane, idx) => {
    const y = padTop + idx * laneH;
    svg.append(svgEl("rect", { x: labelW, y, width: chartW, height: laneH - 4, rx: 3, class: "lane-bg" }));
    const lbl = svgEl("text", { x: 4, y: y + 16, class: "lane-label" });
    const shortID = lane.runnerID.length > 12 ? lane.runnerID.slice(0, 12) + "…" : lane.runnerID;
    lbl.textContent = lane.machineIndex !== null ? `m${lane.machineIndex} ${shortID}` : shortID;
    svg.append(lbl);

    for (const bar of lane.bars) {
      const bx = labelW + (bar.xPct / 100) * chartW;
      const bw = Math.max(8, (bar.wPct / 100) * chartW);
      const rect = svgEl("rect", {
        x: bx.toFixed(1),
        y: y + 2,
        width: bw.toFixed(1),
        height: laneH - 8,
        rx: 3,
        class: `bar bar-${bar.kind}`,
        tabindex: "0",
        role: "button",
        "aria-label": `Lease ${bar.leaseID} (${bar.meta.label}), ${bar.tests.length} files, ${formatDuration(bar.endMS - bar.startMS)}`,
      });
      const open = () => renderLeaseDetail(bar);
      rect.addEventListener("click", open);
      rect.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          open();
        }
      });
      svg.append(rect);
      if (bw >= 16) {
        const sym = svgEl("text", { x: (bx + 4).toFixed(1), y: y + 15, class: "bar-text" });
        sym.textContent = bw >= 44 ? `${bar.meta.symbol} ${bar.tests.length}` : bar.meta.symbol;
        svg.append(sym);
      }
    }
  });
  wrap.append(svg);

  if (timelineCursor !== null) {
    const pager = el("div", "pager");
    const more = el("button", "", "Load next 200 tests into timeline");
    more.setAttribute("type", "button");
    more.addEventListener("click", () => void loadTimelinePage(currentBuildID, false));
    pager.append(more);
    wrap.append(pager);
  }
}

function renderLeaseDetail(bar) {
  const detail = $("lease-detail");
  detail.replaceChildren();
  detail.hidden = false;
  const head = el("div", "section-head");
  head.append(el("strong", "mono", `Lease #${bar.leaseID} · runner ${bar.runnerID}`), badge(bar.kind));
  detail.append(head, el("p", "muted mono", `${formatDuration(bar.endMS - bar.startMS)} · ${bar.tests.length} attempt(s)`));
  const ul = el("ul", "attempt-list");
  for (const t of bar.tests.slice(0, 20)) {
    const m = statusMeta(t.status);
    ul.append(el("li", "mono", `${m.symbol} ${t.test} (gen ${t.generation}, ${t.status}, ${formatDuration(t.durationMS)})`));
  }
  if (bar.tests.length > 20) ul.append(el("li", "muted", `…and ${bar.tests.length - 20} more in this lease.`));
  detail.append(ul);
}

async function loadTestsPage(buildID, reset) {
  const cursor = reset ? 0 : nextCursor;
  if (cursor === null) return;
  let qs = `cursor=${cursor}&limit=${PAGE_LIMIT}`;
  if (currentFilter === "problems") qs += "&state=failed,unresolved";
  else if (currentFilter === "retried") qs += "&retried=true";
  const data = await apiFetch(`/api/v1/builds/${encodeURIComponent(buildID)}/tests?${qs}`);
  if (buildID !== currentBuildID) return;
  loadedTests = reset ? data.tests || [] : loadedTests.concat(data.tests || []);
  nextCursor = data.next_cursor;
  renderTests();
}

function renderTests() {
  const list = $("tests-list");
  list.replaceChildren();
  $("load-more-btn").hidden = nextCursor === null;
  $("tests-meta").textContent =
    loadedTests.length === 0
      ? "No matching tests."
      : `Showing ${loadedTests.length} test${loadedTests.length === 1 ? "" : "s"}${nextCursor !== null ? " (more available)" : ""}.`;

  for (const t of loadedTests) {
    const card = el("article", "test-item");
    const head = el("div", "test-item-head");
    head.append(el("span", "mono break-word", t.test), badge(t.state));
    card.append(head);

    const ul = el("ul", "attempt-list");
    for (const a of t.attempts || []) {
      const m = statusMeta(a.status);
      ul.append(
        el("li", "mono", `${m.symbol} attempt ${a.attempt_id} (${a.status}) · lease #${a.lease_id} · runner ${a.runner_id} · ${formatDuration(a.duration_ms)}`),
      );
    }
    for (const h of t.history || []) {
      const m = statusMeta(h.outcome);
      ul.append(
        el("li", "mono muted", `[${h.disposition}] gen ${h.generation} · ${m.symbol} ${h.outcome} · lease #${h.lease_id} · runner ${h.runner_id}`),
      );
    }
    if (t.history_truncated) ul.append(el("li", "muted", "Additional stale/conflict history truncated at 8."));
    card.append(ul);
    list.append(card);
  }
}

$("auth-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const tok = $("token-input").value.trim();
  if (!tok) return;
  sessionStorage.setItem(TOKEN_KEY, tok);
  $("token-input").value = "";
  void renderRoute();
});
$("signout-btn").addEventListener("click", () => signOut("Signed out."));
$("refresh-btn").addEventListener("click", () => (currentBuildID ? void refreshActiveBuild() : void loadBuilds()));
for (const id of ["brand-link", "back-link"]) {
  $(id).addEventListener("click", (e) => {
    e.preventDefault();
    navigate("/");
  });
}
$("load-more-btn").addEventListener("click", () => currentBuildID && void loadTestsPage(currentBuildID, false));

for (const btn of document.querySelectorAll(".filter-bar button")) {
  btn.addEventListener("click", () => {
    currentFilter = btn.getAttribute("data-filter") || "problems";
    for (const b of document.querySelectorAll(".filter-bar button")) b.setAttribute("aria-pressed", String(b === btn));
    if (currentBuildID) void loadTestsPage(currentBuildID, true);
  });
}
document.addEventListener("visibilitychange", () => dispatchPoll({ type: "visibility", visible: document.visibilityState === "visible" }));
window.addEventListener("popstate", () => void renderRoute());

void renderRoute();
