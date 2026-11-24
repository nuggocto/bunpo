import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/vectors.test.ts", "test/differential.test.ts"],
    environment: "node",
  },
});
