import { authenticateBearer, validateTokens } from "./auth.ts";
import { BuildDO } from "./build-do.ts";
import { BuildIndex } from "./build-index.ts";
import { parseMaxTests } from "./config.ts";
import { ErrCodeInvalidRequest, ProtocolVersion, validBuildID } from "./core/index.ts";
import {
  ErrCodeForbidden,
  ErrCodeMethodNotAllowed,
  ErrCodeNotFound,
  ErrCodeServerMisconfigured,
  ErrCodeStorageError,
  ErrCodeUnauthorized,
  MAX_INIT_BODY_BYTES,
  MAX_OTHER_BODY_BYTES,
  apiHeaders,
  checkJSONContentType,
  errorResponse,
  jsonResponse,
  parseListTestsQuery,
  readBoundedUTF8Body,
  wireErr,
} from "./wire.ts";

export { BuildDO, BuildIndex };

const INDEX_NAME = "global";

function doReplyToResponse(reply: { status: number; body: string }): Response {
  return new Response(reply.body, {
    status: reply.status,
    headers: apiHeaders(),
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Validate MAX_TESTS first; any invalid value refuses every request with 500.
    const maxTestsCfg = parseMaxTests(env.MAX_TESTS);
    if (!maxTestsCfg.ok) {
      return errorResponse(wireErr(500, ErrCodeServerMisconfigured, maxTestsCfg.message));
    }

    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/healthz") {
      if (request.method !== "GET") {
        return errorResponse(wireErr(405, ErrCodeMethodNotAllowed, "method not allowed"), { Allow: "GET" });
      }
      return jsonResponse(200, { ok: true });
    }

    if (!path.startsWith("/api/v1/")) {
      return errorResponse(wireErr(404, ErrCodeNotFound, "not found"));
    }

    // Fail closed with 503 if bearer tokens are missing, short, or equal.
    const tokenCfg = validateTokens(env.BUNPO_QUEUE_TOKEN, env.BUNPO_QUEUE_READ_TOKEN);
    if (!tokenCfg.ok) {
      return errorResponse(wireErr(503, ErrCodeServerMisconfigured, tokenCfg.message));
    }

    // Authenticate before route lookup, body reading, or waking any Durable Object.
    const role = authenticateBearer(request.headers.get("Authorization"), tokenCfg.digests);
    if (role === "none") {
      return errorResponse(wireErr(401, ErrCodeUnauthorized, "missing or invalid bearer token"));
    }

    const rest = path.slice("/api/v1/".length);
    const nowMS = Date.now();

    if (rest === "builds") {
      if (request.method !== "GET") {
        return errorResponse(wireErr(405, ErrCodeMethodNotAllowed, "method not allowed"), { Allow: "GET" });
      }
      const indexStub = env.BUILD_INDEX.get(env.BUILD_INDEX.idFromName(INDEX_NAME));
      const builds = await indexStub.list(nowMS);
      return jsonResponse(200, { version: ProtocolVersion, builds });
    }

    if (!rest.startsWith("builds/")) {
      return errorResponse(wireErr(404, ErrCodeNotFound, "not found"));
    }

    const tail = rest.slice("builds/".length);
    const slashIdx = tail.indexOf("/");
    const hasSlash = slashIdx !== -1;
    const buildID = hasSlash ? tail.slice(0, slashIdx) : tail;
    const action = hasSlash ? tail.slice(slashIdx + 1) : "";

    if (action.includes("/")) {
      return errorResponse(wireErr(404, ErrCodeNotFound, "not found"));
    }

    if (!hasSlash) {
      if (request.method !== "GET") {
        return errorResponse(wireErr(405, ErrCodeMethodNotAllowed, "method not allowed"), { Allow: "GET" });
      }
      if (!validBuildID(buildID)) {
        return errorResponse(wireErr(400, ErrCodeInvalidRequest, "invalid build ID"));
      }
      return callDO(env, buildID, (stub) => stub.status(nowMS));
    }

    if (action === "tests") {
      if (request.method !== "GET") {
        return errorResponse(wireErr(405, ErrCodeMethodNotAllowed, "method not allowed"), { Allow: "GET" });
      }
      if (!validBuildID(buildID)) {
        return errorResponse(wireErr(400, ErrCodeInvalidRequest, "invalid build ID"));
      }
      const parsedQuery = parseListTestsQuery(url);
      if (!parsedQuery.ok) {
        return errorResponse(parsedQuery.error);
      }
      return callDO(env, buildID, (stub) => stub.listTests(nowMS, parsedQuery.value));
    }

    if (action === "init" || action === "lease" || action === "heartbeat" || action === "report") {
      if (request.method !== "POST") {
        return errorResponse(wireErr(405, ErrCodeMethodNotAllowed, "method not allowed"), { Allow: "POST" });
      }
      if (role !== "write") {
        return errorResponse(wireErr(403, ErrCodeForbidden, "write token required"));
      }
      const ctErr = checkJSONContentType(request.headers.get("Content-Type"));
      if (ctErr) {
        return errorResponse(ctErr);
      }
      if (!validBuildID(buildID)) {
        return errorResponse(wireErr(400, ErrCodeInvalidRequest, "invalid build ID"));
      }

      const maxBytes = action === "init" ? MAX_INIT_BODY_BYTES : MAX_OTHER_BODY_BYTES;
      const bodyRes = await readBoundedUTF8Body(request, maxBytes);
      if (!bodyRes.ok) {
        return errorResponse(bodyRes.error);
      }
      const rawBody = bodyRes.value;

      switch (action) {
        case "init":
          return callDO(env, buildID, (stub) => stub.init(nowMS, buildID, rawBody));
        case "lease":
          return callDO(env, buildID, (stub) => stub.lease(nowMS, rawBody));
        case "heartbeat":
          return callDO(env, buildID, (stub) => stub.heartbeat(nowMS, rawBody));
        case "report":
          return callDO(env, buildID, (stub) => stub.report(nowMS, rawBody));
      }
    }

    return errorResponse(wireErr(404, ErrCodeNotFound, "not found"));
  },
};

async function callDO(
  env: Env,
  buildID: string,
  fn: (stub: DurableObjectStub<BuildDO>) => Promise<{ status: number; body: string }>,
): Promise<Response> {
  try {
    const stub = env.BUILD_DO.get(env.BUILD_DO.idFromName(buildID));
    const reply = await fn(stub);
    return doReplyToResponse(reply);
  } catch {
    return errorResponse(wireErr(503, ErrCodeStorageError, "durable object request failed"));
  }
}
