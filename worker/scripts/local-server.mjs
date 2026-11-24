// Starts the production Worker (src/index.ts) on local workerd via Miniflare
// for the Go black-box protocol suite and bunpo --queue E2E tests.
//
// Why Miniflare here instead of `wrangler dev --local`:
// workerd evicts idle Durable Objects after 10 seconds locally (vs 70-140 s in
// production), which collides with TestProtocolBlackBox's 10.08 s single-runner
// idle sleep at the 10 s minimum lease TTL. Setting unsafePreventEviction in
// this test harness prevents the local 10 s dev timer from firing mid-sleep
// without altering production Worker code or relaxing protocol TTL bounds.
// Real DO eviction is tested separately in test/worker.test.ts and test/do-dst.test.ts.

import { buildSync } from "esbuild";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const workerDir = resolve(scriptDir, "..");

const port = Number(process.env.BUNPO_WORKER_TEST_PORT || "18787");
const writeToken = process.env.BUNPO_QUEUE_TOKEN;
const readToken = process.env.BUNPO_QUEUE_READ_TOKEN;
const maxTests = process.env.MAX_TESTS || "5000";
const persistDir = process.env.BUNPO_WORKER_PERSIST_DIR;

if (!writeToken || !readToken) {
  console.error("BUNPO_QUEUE_TOKEN and BUNPO_QUEUE_READ_TOKEN must be set");
  process.exit(1);
}

const bundled = buildSync({
  entryPoints: [resolve(workerDir, "src/index.ts")],
  bundle: true,
  format: "esm",
  target: "es2024",
  write: false,
  external: ["cloudflare:workers", "node:crypto"],
});

const scriptText = bundled.outputFiles[0].text;

const mf = new Miniflare(
  convertV4MiniflareOptions({
    host: "127.0.0.1",
    port,
    durableObjectsPersist: persistDir || false,
    compatibilityDate: "2026-11-24",
    compatibilityFlags: ["nodejs_compat"],
    modules: true,
    script: scriptText,
    bindings: {
      MAX_TESTS: maxTests,
      BUNPO_QUEUE_TOKEN: writeToken,
      BUNPO_QUEUE_READ_TOKEN: readToken,
    },
    durableObjects: {
      BUILD_DO: { className: "BuildDO", useSQLite: true, unsafePreventEviction: true },
      BUILD_INDEX: { className: "BuildIndex", useSQLite: true, unsafePreventEviction: true },
    },
  }),
);

const url = await mf.ready;
console.log(`READY ${url.toString()}`);

const shutdown = async () => {
  await mf.dispose();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
