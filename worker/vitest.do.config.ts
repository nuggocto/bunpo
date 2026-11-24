import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          BUNPO_QUEUE_TOKEN: "test-write-token-0123456789abcdef",
          BUNPO_QUEUE_READ_TOKEN: "test-read-token-fedcba9876543210",
        },
      },
    }),
  ],
  test: {
    include: ["test/worker.test.ts", "test/do-dst.test.ts"],
    testTimeout: 120_000,
  },
});
