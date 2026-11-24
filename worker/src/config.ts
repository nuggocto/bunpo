import { MaxTests } from "./core/index.ts";

export const DEFAULT_WORKER_MAX_TESTS = 5_000;

export type MaxTestsResult =
  | { ok: true; maxTests: number }
  | { ok: false; message: string };

// parseMaxTests validates the MAX_TESTS Worker var (default 5,000, max 20,000).
// Any invalid value fails closed with HTTP 500 on every request.
export function parseMaxTests(raw: string | undefined): MaxTestsResult {
  if (raw === undefined) {
    return { ok: true, maxTests: DEFAULT_WORKER_MAX_TESTS };
  }
  if (!/^[1-9][0-9]*$/.test(raw)) {
    return { ok: false, message: `invalid MAX_TESTS ${JSON.stringify(raw)}: must be an integer between 1 and ${MaxTests}` };
  }
  const n = Number(raw);
  if (!Number.isSafeInteger(n) || n < 1 || n > MaxTests) {
    return { ok: false, message: `invalid MAX_TESTS ${JSON.stringify(raw)}: must be between 1 and ${MaxTests}` };
  }
  return { ok: true, maxTests: n };
}
