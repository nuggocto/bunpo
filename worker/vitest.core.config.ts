import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: [
      "test/vectors.test.ts",
      "test/differential.test.ts",
      "test/deadlines.test.ts",
      "test/dashboard-core.test.ts",
    ],
    environment: "node",
  },
});
