import {
  type Config,
  type HeartbeatCommand,
  type InitCommand,
  type LeaseCommand,
  type ListTestsCommand,
  type Outcome,
  type ReportCommand,
  type ReportResult,
  type TestStatus,
  ErrCodeInvalidRequest,
  MaxTests,
  MaxTestsPageLimit,
  ProtocolVersion,
} from "./core/index.ts";

export const MAX_INIT_BODY_BYTES = 4 * 1024 * 1024;
export const MAX_OTHER_BODY_BYTES = 1 * 1024 * 1024;
const CONTENT_TYPE_JSON = "application/json; charset=utf-8";

export const ErrCodeUnauthorized = "unauthorized";
export const ErrCodeForbidden = "forbidden";
export const ErrCodeNotFound = "not_found";
export const ErrCodeMethodNotAllowed = "method_not_allowed";
const ErrCodeUnsupportedVersion = "unsupported_version";
const ErrCodePayloadTooLarge = "payload_too_large";
export const ErrCodeStorageError = "storage_error";
export const ErrCodeServerMisconfigured = "server_misconfigured";

export interface WireError {
  httpStatus: number;
  code: string;
  message: string;
}

export type ParseResult<T> = { ok: true; value: T } | { ok: false; error: WireError };

const utf8Fatal = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export function wireErr(httpStatus: number, code: string, message: string): WireError {
  return { httpStatus, code, message };
}

// Allows an omitted Content-Type header; rejects non-JSON media types.
export function checkJSONContentType(contentType: string | null): WireError | null {
  if (!contentType) {
    return null;
  }
  const mediaType = contentType.split(";", 1)[0]!.trim().toLowerCase();
  if (mediaType !== "application/json") {
    return wireErr(400, ErrCodeInvalidRequest, "Content-Type must be application/json");
  }
  return null;
}

// readBoundedUTF8Body enforces maxBytes while streaming the body rather than
// trusting Content-Length, and rejects invalid UTF-8 sequences.
export async function readBoundedUTF8Body(req: Request, maxBytes: number): Promise<ParseResult<string>> {
  const body = req.body;
  if (!body) {
    return { ok: true, value: "" };
  }

  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (!value || value.byteLength === 0) {
        continue;
      }
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        return {
          ok: false,
          error: wireErr(413, ErrCodePayloadTooLarge, `request body exceeds ${maxBytes} byte limit`),
        };
      }
      chunks.push(value);
    }
  } catch {
    return {
      ok: false,
      error: wireErr(400, ErrCodeInvalidRequest, "failed to read request body"),
    };
  }

  let merged: Uint8Array;
  if (chunks.length === 0) {
    return { ok: true, value: "" };
  } else if (chunks.length === 1) {
    merged = chunks[0]!;
  } else {
    merged = new Uint8Array(total);
    let offset = 0;
    for (let i = 0; i < chunks.length; i++) {
      const c = chunks[i]!;
      merged.set(c, offset);
      offset += c.byteLength;
    }
  }

  try {
    return { ok: true, value: utf8Fatal.decode(merged) };
  } catch {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, "request body is not valid UTF-8") };
  }
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function parseEnvelopeObject(raw: string, allowedKeys: readonly string[]): ParseResult<Record<string, unknown>> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    return {
      ok: false,
      error: wireErr(400, ErrCodeInvalidRequest, `invalid JSON: ${e instanceof Error ? e.message : String(e)}`),
    };
  }
  if (!isPlainObject(parsed)) {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, "request body must be a JSON object") };
  }

  // Check protocol version before rejecting unknown/missing members so clients
  // from a newer protocol version always receive 400 unsupported_version.
  if (Object.prototype.hasOwnProperty.call(parsed, "version")) {
    const v = parsed["version"];
    if (typeof v !== "number" || !Number.isSafeInteger(v)) {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, 'member "version" must be an integer') };
    }
    if (v !== ProtocolVersion) {
      return {
        ok: false,
        error: wireErr(
          400,
          ErrCodeUnsupportedVersion,
          `unsupported protocol version ${v}; server expects version ${ProtocolVersion}`,
        ),
      };
    }
  }

  const allowed = new Set(allowedKeys);
  for (const k of Object.keys(parsed)) {
    if (!allowed.has(k)) {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `unknown member ${JSON.stringify(k)}`) };
    }
  }

  if (!Object.prototype.hasOwnProperty.call(parsed, "version")) {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, 'missing required member "version"') };
  }
  return { ok: true, value: parsed };
}

function requireKey(obj: Record<string, unknown>, key: string): WireError | null {
  if (!Object.prototype.hasOwnProperty.call(obj, key)) {
    return wireErr(400, ErrCodeInvalidRequest, `missing required member ${JSON.stringify(key)}`);
  }
  return null;
}

function requireString(obj: Record<string, unknown>, key: string): ParseResult<string> {
  const miss = requireKey(obj, key);
  if (miss) return { ok: false, error: miss };
  const v = obj[key];
  if (typeof v !== "string") {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `member ${JSON.stringify(key)} must be a string`) };
  }
  return { ok: true, value: v };
}

function requireInt(obj: Record<string, unknown>, key: string): ParseResult<number> {
  const miss = requireKey(obj, key);
  if (miss) return { ok: false, error: miss };
  const v = obj[key];
  if (typeof v !== "number" || !Number.isSafeInteger(v)) {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `member ${JSON.stringify(key)} must be an integer`) };
  }
  return { ok: true, value: v };
}

function requireBool(obj: Record<string, unknown>, key: string): ParseResult<boolean> {
  const miss = requireKey(obj, key);
  if (miss) return { ok: false, error: miss };
  const v = obj[key];
  if (typeof v !== "boolean") {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `member ${JSON.stringify(key)} must be a boolean`) };
  }
  return { ok: true, value: v };
}

function requireNullableInt(obj: Record<string, unknown>, key: string): ParseResult<number | null> {
  const miss = requireKey(obj, key);
  if (miss) return { ok: false, error: miss };
  const v = obj[key];
  if (v === null) {
    return { ok: true, value: null };
  }
  if (typeof v !== "number" || !Number.isSafeInteger(v)) {
    return {
      ok: false,
      error: wireErr(400, ErrCodeInvalidRequest, `member ${JSON.stringify(key)} must be an integer or null`),
    };
  }
  return { ok: true, value: v };
}

const INIT_KEYS = ["version", "runner_id", "machine_index", "config", "tests", "predictions_ms"] as const;
const CONFIG_KEYS = ["machines", "lease_ttl_ms", "build_timeout_ms", "max_attempts", "max_batch"] as const;
const LEASE_KEYS = ["version", "runner_id", "machine_index", "request_id"] as const;
const HEARTBEAT_KEYS = ["version", "runner_id", "lease_id"] as const;
const REPORT_KEYS = ["version", "runner_id", "lease_id", "results"] as const;
const RESULT_KEYS = ["attempt_id", "test_index", "generation", "outcome", "timed", "duration_ms"] as const;

export function parseInitBody(buildID: string, raw: string): ParseResult<InitCommand> {
  const env = parseEnvelopeObject(raw, INIT_KEYS);
  if (!env.ok) return env;
  const obj = env.value;

  const runnerID = requireString(obj, "runner_id");
  if (!runnerID.ok) return runnerID;
  const machineIndex = requireNullableInt(obj, "machine_index");
  if (!machineIndex.ok) return machineIndex;

  const cfgMiss = requireKey(obj, "config");
  if (cfgMiss) return { ok: false, error: cfgMiss };
  const rawCfg = obj["config"];
  if (!isPlainObject(rawCfg)) {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, 'member "config" must be an object') };
  }
  const cfgAllowed = new Set<string>(CONFIG_KEYS);
  for (const k of Object.keys(rawCfg)) {
    if (!cfgAllowed.has(k)) {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `unknown config member ${JSON.stringify(k)}`) };
    }
  }

  const machines = requireInt(rawCfg, "machines");
  if (!machines.ok) return machines;
  const leaseTTL = requireInt(rawCfg, "lease_ttl_ms");
  if (!leaseTTL.ok) return leaseTTL;
  const buildTimeout = requireInt(rawCfg, "build_timeout_ms");
  if (!buildTimeout.ok) return buildTimeout;
  const maxAttempts = requireInt(rawCfg, "max_attempts");
  if (!maxAttempts.ok) return maxAttempts;
  const maxBatch = requireInt(rawCfg, "max_batch");
  if (!maxBatch.ok) return maxBatch;

  const testsMiss = requireKey(obj, "tests");
  if (testsMiss) return { ok: false, error: testsMiss };
  const rawTests = obj["tests"];
  if (!Array.isArray(rawTests)) {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, 'member "tests" must be an array') };
  }
  for (let i = 0; i < rawTests.length; i++) {
    if (typeof rawTests[i] !== "string") {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `tests[${i}] must be a string`) };
    }
  }

  const predsMiss = requireKey(obj, "predictions_ms");
  if (predsMiss) return { ok: false, error: predsMiss };
  const rawPreds = obj["predictions_ms"];
  if (!Array.isArray(rawPreds)) {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, 'member "predictions_ms" must be an array') };
  }
  for (let i = 0; i < rawPreds.length; i++) {
    const p = rawPreds[i];
    if (p !== null && (typeof p !== "number" || !Number.isSafeInteger(p))) {
      return {
        ok: false,
        error: wireErr(400, ErrCodeInvalidRequest, `predictions_ms[${i}] must be an integer or null`),
      };
    }
  }

  const config: Config = {
    machines: machines.value,
    lease_ttl_ms: leaseTTL.value,
    build_timeout_ms: buildTimeout.value,
    max_attempts: maxAttempts.value,
    max_batch: maxBatch.value,
  };

  return {
    ok: true,
    value: {
      build_id: buildID,
      runner_id: runnerID.value,
      machine_index: machineIndex.value,
      config,
      tests: rawTests as string[],
      predictions_ms: rawPreds as (number | null)[],
    },
  };
}

export function parseLeaseBody(raw: string): ParseResult<LeaseCommand> {
  const env = parseEnvelopeObject(raw, LEASE_KEYS);
  if (!env.ok) return env;
  const obj = env.value;

  const runnerID = requireString(obj, "runner_id");
  if (!runnerID.ok) return runnerID;
  const machineIndex = requireNullableInt(obj, "machine_index");
  if (!machineIndex.ok) return machineIndex;
  const requestID = requireString(obj, "request_id");
  if (!requestID.ok) return requestID;

  return {
    ok: true,
    value: {
      runner_id: runnerID.value,
      machine_index: machineIndex.value,
      request_id: requestID.value,
    },
  };
}

export function parseHeartbeatBody(raw: string): ParseResult<HeartbeatCommand> {
  const env = parseEnvelopeObject(raw, HEARTBEAT_KEYS);
  if (!env.ok) return env;
  const obj = env.value;

  const runnerID = requireString(obj, "runner_id");
  if (!runnerID.ok) return runnerID;
  const leaseID = requireInt(obj, "lease_id");
  if (!leaseID.ok) return leaseID;

  return {
    ok: true,
    value: {
      runner_id: runnerID.value,
      lease_id: leaseID.value,
    },
  };
}

export function parseReportBody(raw: string): ParseResult<ReportCommand> {
  const env = parseEnvelopeObject(raw, REPORT_KEYS);
  if (!env.ok) return env;
  const obj = env.value;

  const runnerID = requireString(obj, "runner_id");
  if (!runnerID.ok) return runnerID;
  const leaseID = requireInt(obj, "lease_id");
  if (!leaseID.ok) return leaseID;

  const resMiss = requireKey(obj, "results");
  if (resMiss) return { ok: false, error: resMiss };
  const rawResults = obj["results"];
  if (!Array.isArray(rawResults)) {
    return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, 'member "results" must be an array') };
  }

  const resAllowed = new Set<string>(RESULT_KEYS);
  const results = new Array<ReportResult>(rawResults.length);
  for (let i = 0; i < rawResults.length; i++) {
    const item = rawResults[i];
    if (!isPlainObject(item)) {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `results[${i}] must be an object`) };
    }
    for (const k of Object.keys(item)) {
      if (!resAllowed.has(k)) {
        return {
          ok: false,
          error: wireErr(400, ErrCodeInvalidRequest, `unknown member ${JSON.stringify(k)} in results[${i}]`),
        };
      }
    }

    const attemptID = requireString(item, "attempt_id");
    if (!attemptID.ok) return attemptID;
    const testIndex = requireInt(item, "test_index");
    if (!testIndex.ok) return testIndex;
    const generation = requireInt(item, "generation");
    if (!generation.ok) return generation;
    const outcome = requireString(item, "outcome");
    if (!outcome.ok) return outcome;
    const timed = requireBool(item, "timed");
    if (!timed.ok) return timed;
    const durationMS = requireInt(item, "duration_ms");
    if (!durationMS.ok) return durationMS;

    results[i] = {
      attempt_id: attemptID.value,
      test_index: testIndex.value,
      generation: generation.value,
      outcome: outcome.value as Outcome,
      timed: timed.value,
      duration_ms: durationMS.value,
    };
  }

  return {
    ok: true,
    value: {
      runner_id: runnerID.value,
      lease_id: leaseID.value,
      results,
    },
  };
}

// Parses and validates query parameters for GET /api/v1/builds/:id/tests.
export function parseListTestsQuery(url: URL): ParseResult<ListTestsCommand> {
  let cursor = 0;
  let limit = 0;
  let states: TestStatus[] | undefined;
  let retried = false;

  // Same rule as pageParam in the Go server: plain decimal digits, no sign or
  // leading zero, within range. Huge values become large floats, which the
  // range check still rejects.
  const rawCursor = url.searchParams.get("cursor");
  if (rawCursor !== null && rawCursor !== "") {
    cursor = Number(rawCursor);
    if (!/^(0|[1-9][0-9]*)$/.test(rawCursor) || cursor > MaxTests) {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `cursor must be 0..${MaxTests}`) };
    }
  }

  const rawLimit = url.searchParams.get("limit");
  if (rawLimit !== null && rawLimit !== "") {
    limit = Number(rawLimit);
    if (!/^[1-9][0-9]*$/.test(rawLimit) || limit > MaxTestsPageLimit) {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, `limit must be 1..${MaxTestsPageLimit}`) };
    }
  }

  const rawState = url.searchParams.get("state");
  if (rawState !== null && rawState !== "") {
    states = rawState.split(",") as TestStatus[];
  }

  const rawRetried = url.searchParams.get("retried");
  if (rawRetried !== null && rawRetried !== "") {
    if (rawRetried === "true") {
      retried = true;
    } else if (rawRetried === "false") {
      retried = false;
    } else {
      return { ok: false, error: wireErr(400, ErrCodeInvalidRequest, "retried must be true or false") };
    }
  }

  return {
    ok: true,
    value: {
      cursor,
      limit,
      ...(states ? { states } : {}),
      retried,
    },
  };
}

export function apiHeaders(extra?: Record<string, string>): Headers {
  const h = new Headers({
    "Content-Type": CONTENT_TYPE_JSON,
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
  });
  if (extra) {
    for (const [k, v] of Object.entries(extra)) {
      h.set(k, v);
    }
  }
  return h;
}

export function jsonResponse(status: number, body: unknown, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: apiHeaders(extraHeaders),
  });
}

export function errorResponse(err: WireError, extraHeaders?: Record<string, string>): Response {
  return jsonResponse(
    err.httpStatus,
    {
      version: ProtocolVersion,
      error: {
        code: err.code,
        message: err.message,
      },
    },
    extraHeaders,
  );
}
