#!/usr/bin/env node
// Verifies a completed live-queue trial build via GET /api/v1/builds/{id} and
// GET /api/v1/builds/{id}/tests:
//   - verdict is "passed" with zero conflicts and zero unresolved/failed tests
//   - every expected test file has state "passed" and exactly one accepted "passed" attempt
//   - the killed machine's expired attempt is visible and reassigned (generation >= 2)
//
// Env: BUNPO_QUEUE_URL, BUNPO_QUEUE_READ_TOKEN (or BUNPO_QUEUE_TOKEN),
//      BUNPO_BUILD_ID, EXPECTED_TEST_COUNT (optional), KILLED_MACHINE_INDEX (default "0").

const baseURL = (process.env.BUNPO_QUEUE_URL || "").trim().replace(/\/+$/, "");
const token = process.env.BUNPO_QUEUE_READ_TOKEN || process.env.BUNPO_QUEUE_TOKEN || "";
const buildID = (process.env.BUNPO_BUILD_ID || "").trim();
const expectedCount = Number(process.env.EXPECTED_TEST_COUNT || "0");
const killedMachine = Number(process.env.KILLED_MACHINE_INDEX ?? "0");

function fail(msg) {
  console.error(`assert-queue-trial: FAIL: ${msg}`);
  process.exit(1);
}

if (!baseURL || !token || !buildID) {
  fail("BUNPO_QUEUE_URL, BUNPO_QUEUE_READ_TOKEN (or BUNPO_QUEUE_TOKEN), and BUNPO_BUILD_ID must be set");
}

async function getJSON(path) {
  const resp = await fetch(`${baseURL}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
    redirect: "manual",
  });
  const text = await resp.text();
  if (!resp.ok) {
    fail(`GET ${path} returned HTTP ${resp.status}: ${text.slice(0, 200)}`);
  }
  return JSON.parse(text);
}

const status = await getJSON(`/api/v1/builds/${encodeURIComponent(buildID)}`);
if (status.verdict !== "passed") {
  fail(`expected verdict "passed", got ${JSON.stringify(status.verdict)}`);
}
if ((status.conflicts || []).length > 0) {
  fail(`expected 0 init conflicts, got ${status.conflicts.length}`);
}
if (expectedCount > 0 && status.counts?.total !== expectedCount) {
  fail(`expected ${expectedCount} total tests, got ${status.counts?.total}`);
}

const runnerMachine = new Map((status.runners || []).map((r) => [r.runner_id, r.machine_index]));

const allTests = [];
let cursor = 0;
while (cursor !== null) {
  const page = await getJSON(`/api/v1/builds/${encodeURIComponent(buildID)}/tests?cursor=${cursor}&limit=200`);
  allTests.push(...(page.tests || []));
  cursor = page.next_cursor ?? null;
}

if (allTests.length !== status.counts.total) {
  fail(`paginated ${allTests.length} tests, but status.counts.total=${status.counts.total}`);
}

let killedExpiredVisible = 0;
for (const t of allTests) {
  if (t.state !== "passed") {
    fail(`test ${t.test} has state ${t.state}, expected passed`);
  }
  const passedAttempts = (t.attempts || []).filter((a) => a.status === "passed");
  if (passedAttempts.length !== 1) {
    fail(`test ${t.test} has ${passedAttempts.length} passed attempts, expected exactly 1`);
  }
  for (const a of t.attempts || []) {
    if (a.status === "expired" && runnerMachine.get(a.runner_id) === killedMachine && t.generation >= 2) {
      killedExpiredVisible++;
    }
  }
}

if (killedExpiredVisible === 0) {
  fail(`expected at least one expired attempt from killed machine ${killedMachine} reassigned to generation >= 2`);
}

console.log(
  `assert-queue-trial: PASS: build=${buildID} verdict=passed tests=${allTests.length} killed_machine=${killedMachine} expired_attempts_visible=${killedExpiredVisible}`,
);
