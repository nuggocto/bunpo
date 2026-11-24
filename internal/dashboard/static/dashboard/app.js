import {
  BASE_POLL_MS, PAGE_LIMIT, buildPath, buildTimeline, createGuardedLoader, createPollState,
  fetchBuildInitial, formatAttemptDuration, formatDuration, formatTimeLeft, formatTimestamp,
  mergeRefreshedPage, parseRoute, pollStep, sortBuilds, statusMeta,
} from "./core.js";

const TOKEN_KEY = "bunpo_queue_read_token";
const SVG_NS = "http://www.w3.org/2000/svg";
const $ = (id) => document.getElementById(id);

const ui = {
  poll: createPollState(),
  timer: null,
  buildID: null,
  status: null,
  filter: "problems",
  tests: [],
  nextCursor: null,
  testsPages: 0,
  filterSeq: 0,
  tlTests: [],
  tlCursor: null,
  tlPages: 0,
  leaseID: null,
  announced: "",
};

function resetBuildState() {
  ui.status = null;
  ui.tests = [];
  ui.nextCursor = null;
  ui.testsPages = 0;
  ui.tlTests = [];
  ui.tlCursor = null;
  ui.tlPages = 0;
  ui.leaseID = null;
}

async function apiFetch(path) {
  const resp = await fetch(path, {
    method: "GET",
    headers: { Authorization: `Bearer ${getToken()}` },
    cache: "no-store",
  });
  let body = null;
  try { body = await resp.json(); } catch { body = null; }
  if (!resp.ok) {
    const retryAfterSec = Number(resp.headers.get("Retry-After") || "0");
    const err = new Error(body?.error?.message || `HTTP ${resp.status}`);
    err.httpStatus = resp.status;
    err.retryAfterMS = Number.isFinite(retryAfterSec) && retryAfterSec > 0 ? retryAfterSec * 1_000 : 0;
    throw err;
  }
  return body;
}

const loader = createGuardedLoader(apiFetch);

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
  if (!msg || msg === ui.announced) return;
  ui.announced = msg;
  $("live-status").textContent = msg;
}

function showError(msg) {
  const banner = $("error-banner");
  banner.textContent = msg || "";
  banner.hidden = !msg;
}

function getToken() {
  try { return sessionStorage.getItem(TOKEN_KEY) || ""; } catch { return ""; }
}

function testsQuery(cursor) {
  let qs = `cursor=${cursor}&limit=${PAGE_LIMIT}`;
  if (ui.filter === "problems") qs += "&state=failed,unresolved";
  else if (ui.filter === "retried") qs += "&retried=true";
  return qs;
}

function dispatchPoll(ev) {
  const res = pollStep(ui.poll, { ...ev, nowMS: Date.now() });
  ui.poll = res.state;
  for (const a of res.actions) {
    if (a.type === "cancel" && ui.timer !== null) {
      clearTimeout(ui.timer);
      ui.timer = null;
    } else if (a.type === "schedule") {
      if (ui.timer !== null) clearTimeout(ui.timer);
      ui.timer = setTimeout(() => { ui.timer = null; dispatchPoll({ type: "tick" }); }, a.delayMS);
    } else if (a.type === "fetch") {
      void refreshActiveBuild(false);
    }
  }
  const label = $("refresh-state");
  if (!getToken() || !ui.buildID) label.textContent = "";
  else if (ui.poll.inFlight) label.textContent = "Refreshing…";
  else if (ui.poll.active && ui.poll.visible) {
    label.textContent = ui.poll.failures > 0 ? `Backing off (${ui.poll.failures})` : `Auto-refresh ${BASE_POLL_MS / 1000}s`;
  } else if (ui.poll.active) label.textContent = "Paused (tab hidden)";
  else if (ui.poll.stoppedReason === "finished") label.textContent = "Finished";
  else if (ui.poll.stoppedReason) label.textContent = "Auto-refresh stopped";
  else label.textContent = "";
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
  loader.invalidate();
  dispatchPoll({ type: "stop", reason: "signout" });
  try { sessionStorage.removeItem(TOKEN_KEY); } catch { /* ignore storage errors */ }
  ui.buildID = null;
  resetBuildState();
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
  const gen = loader.invalidate();
  showError("");
  dispatchPoll({ type: "stop", reason: "navigate" });
  if (!getToken()) {
    ui.buildID = null;
    showView("auth");
    return;
  }
  const route = parseRoute(window.location.pathname);
  if (route.view === "build") {
    ui.buildID = route.buildID;
    showView("build");
    await loadBuild(route.buildID, gen);
    return;
  }
  ui.buildID = null;
  showView("builds");
  if (route.view === "not_found") showError("Page not found. Showing recent builds.");
  await loadBuilds(gen);
}

async function loadBuilds(gen) {
  const res = await loader.fetch("/api/v1/builds", gen);
  if (res.stale) return;
  if (!res.ok) return handleFetchError(res.error, false);
  const rows = sortBuilds(res.value.builds || []);
  $("builds-count").textContent = `${rows.length} build${rows.length === 1 ? "" : "s"}`;
  const tbody = $("builds-tbody");
  tbody.replaceChildren();
  $("builds-empty").hidden = rows.length > 0;
  for (const b of rows) {
    const tr = el("tr");
    const link = el("a", "mono break-word", b.build_id);
    link.setAttribute("href", buildPath(b.build_id));
    link.addEventListener("click", (e) => { e.preventDefault(); navigate(buildPath(b.build_id)); });
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
}

async function loadBuild(buildID, gen) {
  resetBuildState();
  $("lease-detail").hidden = true;
  $("build-heading").textContent = buildID;
  $("timeline-legend").replaceChildren(
    ...["passed", "failed", "error", "expired", "leased", "skipped"].map((k) => badge(k)),
  );
  const res = await fetchBuildInitial(loader, gen, buildID, testsQuery(0), (st) => {
    ui.status = st;
    renderBuildSummary(st);
  });
  if (res.stale) return;
  if (!res.ok) return handleFetchError(res.error, false);
  const { status: st, timeline, tests } = res.value;
  ui.tlTests = timeline.tests || [];
  ui.tlCursor = timeline.next_cursor ?? null;
  ui.tlPages = 1;
  ui.tests = tests.tests || [];
  ui.nextCursor = tests.next_cursor ?? null;
  ui.testsPages = 1;
  renderTimeline();
  renderTests();
  dispatchPoll({ type: "watch", verdict: st.verdict });
  announce(`Build ${buildID} is ${st.verdict}.`);
}

async function refreshActiveBuild(manual) {
  if (!ui.buildID) return;
  const gen = loader.current();
  const buildID = ui.buildID;
  const prevLastSeen = ui.status?.last_seen_ms ?? 0;
  const prevVerdict = ui.status?.verdict ?? null;
  const enc = encodeURIComponent(buildID);

  const stRes = await loader.fetch(`/api/v1/builds/${enc}`, gen);
  if (stRes.stale) return;
  if (!stRes.ok) return handleFetchError(stRes.error, !manual);
  const st = stRes.value;
  showError("");
  ui.status = st;
  renderBuildSummary(st);

  if (st.last_seen_ms !== prevLastSeen || manual) {
    const seq = ui.filterSeq;
    const [tlRes, testsRes] = await Promise.all([
      loader.fetch(`/api/v1/builds/${enc}/tests?cursor=0&limit=${PAGE_LIMIT}`, gen),
      loader.fetch(`/api/v1/builds/${enc}/tests?${testsQuery(0)}`, gen),
    ]);
    if (tlRes.stale || testsRes.stale || !loader.isCurrent(gen)) return;
    if (!tlRes.ok) return handleFetchError(tlRes.error, !manual);
    if (!testsRes.ok) return handleFetchError(testsRes.error, !manual);

    const tl = mergeRefreshedPage(ui.tlTests, ui.tlCursor, ui.tlPages, tlRes.value);
    ui.tlTests = tl.tests;
    ui.tlCursor = tl.nextCursor;
    ui.tlPages = tl.pagesLoaded;
    renderTimeline();

    if (seq === ui.filterSeq) {
      const ts = mergeRefreshedPage(ui.tests, ui.nextCursor, ui.testsPages, testsRes.value);
      ui.tests = ts.tests;
      ui.nextCursor = ts.nextCursor;
      ui.testsPages = ts.pagesLoaded;
      renderTests();
    }
  }
  dispatchPoll({ type: "response_ok", verdict: st.verdict, manual });
  if (st.verdict !== prevVerdict) announce(`Build ${buildID} verdict changed to ${st.verdict}.`);
}

function handleFetchError(err, fromPoll) {
  if (err.httpStatus === 401) return signOut("Read token rejected (401). Please sign in again.");
  showError(`Request failed: ${err.message}`);
  if (fromPoll) dispatchPoll({ type: "response_error", httpStatus: err.httpStatus || 0, retryAfterMS: err.retryAfterMS || 0 });
  announce(`Error: ${err.message}`);
}

function renderBuildSummary(st) {
  $("build-verdict-slot").replaceChildren(badge(st.verdict));
  const c = st.counts;
  const summary = $("build-summary");
  summary.replaceChildren();
  for (const [label, val] of [
    ["Total", c.total], ["Passed", c.passed], ["Failed", c.failed], ["Unresolved", c.unresolved],
    ["Skipped", c.skipped], ["Leased / Pending", `${c.leased} / ${c.pending}`],
    ["Runners", st.runner_count], ["Time left", formatTimeLeft(st.deadline_ms, Date.now(), st.verdict)],
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

async function loadNextTimelinePage() {
  if (!ui.buildID || ui.tlCursor === null) return;
  const gen = loader.current();
  const res = await loader.fetch(`/api/v1/builds/${encodeURIComponent(ui.buildID)}/tests?cursor=${ui.tlCursor}&limit=${PAGE_LIMIT}`, gen);
  if (res.stale) return;
  if (!res.ok) return handleFetchError(res.error, false);
  ui.tlTests = ui.tlTests.concat(res.value.tests || []);
  ui.tlCursor = res.value.next_cursor ?? null;
  ui.tlPages++;
  renderTimeline();
}

function renderTimeline() {
  const wrap = $("timeline-wrap");
  const active = document.activeElement;
  const focusedLeaseID = active && wrap.contains(active) ? active.getAttribute("data-lease-id") : null;
  wrap.replaceChildren();

  const tl = buildTimeline(ui.status, ui.tlTests, Date.now());
  const total = ui.status?.counts?.total ?? ui.tlTests.length;
  $("timeline-meta").textContent =
    ui.tlCursor !== null
      ? `Leases from first ${ui.tlTests.length} of ${total} tests`
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
    role: "group",
    "aria-label": `Per-machine lease timeline across ${tl.lanes.length} runners`,
  });
  for (const [frac, anchor] of [[0, "start"], [0.5, "middle"], [1, "end"]]) {
    const t = svgEl("text", { x: (labelW + frac * chartW).toFixed(1), y: 13, class: "axis-label", "text-anchor": anchor });
    t.textContent = formatDuration(tl.spanMS * frac);
    svg.append(t);
  }

  let focusTarget = null;
  let updatedDetailBar = null;

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
      const leaseAttr = String(bar.leaseID);
      const rect = svgEl("rect", {
        x: bx.toFixed(1),
        y: y + 2,
        width: bw.toFixed(1),
        height: laneH - 8,
        rx: 3,
        class: `bar bar-${bar.kind}`,
        tabindex: "0",
        role: "button",
        "data-lease-id": leaseAttr,
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
      if (focusedLeaseID === leaseAttr) focusTarget = rect;
      if (ui.leaseID === bar.leaseID) updatedDetailBar = bar;
      svg.append(rect);
      if (bw >= 16) {
        const sym = svgEl("text", { x: (bx + 4).toFixed(1), y: y + 15, class: "bar-text" });
        sym.textContent = bw >= 44 ? `${bar.meta.symbol} ${bar.tests.length}` : bar.meta.symbol;
        svg.append(sym);
      }
    }
  });
  wrap.append(svg);

  if (focusTarget) focusTarget.focus({ preventScroll: true });
  if (updatedDetailBar && !$("lease-detail").hidden) renderLeaseDetail(updatedDetailBar);

  if (ui.tlCursor !== null) {
    const pager = el("div", "pager");
    const more = el("button", "", "Load next 200 tests into timeline");
    more.setAttribute("type", "button");
    more.addEventListener("click", () => void loadNextTimelinePage());
    pager.append(more);
    wrap.append(pager);
  }
}

function renderLeaseDetail(bar) {
  ui.leaseID = bar.leaseID;
  const detail = $("lease-detail");
  detail.replaceChildren();
  detail.hidden = false;
  const head = el("div", "section-head");
  head.append(el("strong", "mono", `Lease #${bar.leaseID} · runner ${bar.runnerID}`), badge(bar.kind));
  detail.append(head, el("p", "muted mono", `${formatDuration(bar.endMS - bar.startMS)} · ${bar.tests.length} attempt(s)`));
  const ul = el("ul", "attempt-list");
  for (const t of bar.tests.slice(0, 20)) {
    const m = statusMeta(t.status);
    const durPart = t.durationText ? `, ${t.durationText}` : "";
    ul.append(el("li", "mono", `${m.symbol} ${t.test} (gen ${t.generation}, ${t.status}${durPart})`));
  }
  if (bar.tests.length > 20) ul.append(el("li", "muted", `…and ${bar.tests.length - 20} more in this lease.`));
  detail.append(ul);
}

async function loadTestsPage(reset) {
  if (!ui.buildID) return;
  const cursor = reset ? 0 : ui.nextCursor;
  if (cursor === null) return;
  const gen = loader.current();
  const seq = reset ? ++ui.filterSeq : ui.filterSeq;
  const res = await loader.fetch(`/api/v1/builds/${encodeURIComponent(ui.buildID)}/tests?${testsQuery(cursor)}`, gen);
  if (res.stale || seq !== ui.filterSeq) return;
  if (!res.ok) return handleFetchError(res.error, false);
  ui.tests = reset ? res.value.tests || [] : ui.tests.concat(res.value.tests || []);
  ui.nextCursor = res.value.next_cursor ?? null;
  ui.testsPages = reset ? 1 : ui.testsPages + 1;
  renderTests();
}

function renderTests() {
  const list = $("tests-list");
  list.replaceChildren();
  $("load-more-btn").hidden = ui.nextCursor === null;
  $("tests-meta").textContent =
    ui.tests.length === 0
      ? "No matching tests."
      : `Showing ${ui.tests.length} test${ui.tests.length === 1 ? "" : "s"}${ui.nextCursor !== null ? " (more available)" : ""}.`;

  for (const t of ui.tests) {
    const card = el("article", "test-item");
    const head = el("div", "test-item-head");
    head.append(el("span", "mono break-word", t.test), badge(t.state));
    card.append(head);

    const ul = el("ul", "attempt-list");
    for (const a of t.attempts || []) {
      const m = statusMeta(a.status);
      const dur = formatAttemptDuration(a);
      ul.append(el("li", "mono", `${m.symbol} attempt ${a.attempt_id} (${a.status}) · lease #${a.lease_id} · runner ${a.runner_id}${dur ? ` · ${dur}` : ""}`));
    }
    for (const h of t.history || []) {
      const m = statusMeta(h.outcome);
      ul.append(el("li", "mono muted", `[${h.disposition}] gen ${h.generation} · ${m.symbol} ${h.outcome} · lease #${h.lease_id} · runner ${h.runner_id}`));
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
$("refresh-btn").addEventListener("click", () => (ui.buildID ? void refreshActiveBuild(true) : void loadBuilds(loader.current())));
for (const id of ["brand-link", "back-link"]) {
  $(id).addEventListener("click", (e) => { e.preventDefault(); navigate("/"); });
}
$("load-more-btn").addEventListener("click", () => void loadTestsPage(false));
for (const btn of document.querySelectorAll(".filter-bar button")) {
  btn.addEventListener("click", () => {
    ui.filter = btn.getAttribute("data-filter") || "problems";
    for (const b of document.querySelectorAll(".filter-bar button")) b.setAttribute("aria-pressed", String(b === btn));
    if (ui.buildID) void loadTestsPage(true);
  });
}
document.addEventListener("visibilitychange", () => dispatchPoll({ type: "visibility", visible: document.visibilityState === "visible" }));
window.addEventListener("popstate", () => void renderRoute());

void renderRoute();
