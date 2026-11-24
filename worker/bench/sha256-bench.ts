// Compare pure JS sha256.ts vs native node:crypto createHash("sha256")
// on 1k..20k realistic 60-byte test paths (warm & cold CPU via process.cpuUsage).
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { type Config, ProtocolVersion, configDigest } from "../src/core/index.ts";

function generatePaths(count: number): string[] {
  const tests = new Array<string>(count);
  for (let i = 0; i < count; i++) {
    const pkg = String(i % 40).padStart(2, "0");
    const mod = String(Math.floor(i / 40) % 25).padStart(2, "0");
    const num = String(i).padStart(5, "0");
    tests[i] = `services/pkg_${pkg}/internal/mod_${mod}/handlers/order_${num}.spec.ts`;
  }
  tests.sort();
  return tests;
}

const cfg: Config = {
  machines: 64,
  lease_ttl_ms: 60_000,
  build_timeout_ms: 21_600_000,
  max_attempts: 3,
  max_batch: 200,
};

function configDigestNodeCryptoStream(c: Config, tests: readonly string[]): string {
  const h = createHash("sha256");
  h.update(
    `bunpo queue ${ProtocolVersion}\nmachines ${c.machines}\nlease_ttl_ms ${c.lease_ttl_ms}\nbuild_timeout_ms ${c.build_timeout_ms}\nmax_attempts ${c.max_attempts}\nmax_batch ${c.max_batch}\n`,
  );
  for (let i = 0; i < tests.length; i++) {
    h.update(tests[i]!);
    h.update("\n");
  }
  return h.digest("hex");
}

function configDigestNodeCryptoJoined(c: Config, tests: readonly string[]): string {
  const h = createHash("sha256");
  h.update(
    `bunpo queue ${ProtocolVersion}\nmachines ${c.machines}\nlease_ttl_ms ${c.lease_ttl_ms}\nbuild_timeout_ms ${c.build_timeout_ms}\nmax_attempts ${c.max_attempts}\nmax_batch ${c.max_batch}\n`,
  );
  h.update(tests.join("\n"));
  h.update("\n");
  return h.digest("hex");
}

function measureCpuMS(fn: () => string): { ms: number; hex: string } {
  const start = process.cpuUsage();
  const hex = fn();
  const diff = process.cpuUsage(start);
  return { ms: (diff.user + diff.system) / 1000, hex };
}

function stats(samples: number[]): { p50: number; max: number } {
  const s = samples.slice().sort((a, b) => a - b);
  return {
    p50: Number(s[Math.floor(s.length / 2)]!.toFixed(3)),
    max: Number(s[s.length - 1]!.toFixed(3)),
  };
}

const args = process.argv.slice(2);
if (args[0] === "--cold") {
  const n = Number(args[1]);
  const mode = args[2]!;
  const paths = generatePaths(n);
  const res =
    mode === "js"
      ? measureCpuMS(() => configDigest(cfg, paths))
      : mode === "crypto_stream"
        ? measureCpuMS(() => configDigestNodeCryptoStream(cfg, paths))
        : measureCpuMS(() => configDigestNodeCryptoJoined(cfg, paths));
  process.stdout.write(String(res.ms));
  process.exit(0);
}

const sizes = [1_000, 5_000, 10_000, 20_000];
const out: unknown[] = [];
for (const n of sizes) {
  const paths = generatePaths(n);
  const hJs = configDigest(cfg, paths);
  const hStream = configDigestNodeCryptoStream(cfg, paths);
  const hJoined = configDigestNodeCryptoJoined(cfg, paths);
  if (hJs !== hStream || hJs !== hJoined) {
    throw new Error(`digest mismatch at n=${n}: ${hJs} vs ${hStream} vs ${hJoined}`);
  }

  const runWarm = (fn: () => string) => {
    for (let i = 0; i < 5; i++) fn();
    const samples: number[] = [];
    for (let i = 0; i < 30; i++) samples.push(measureCpuMS(fn).ms);
    return stats(samples);
  };

  const runCold = (mode: string) => {
    const samples: number[] = [];
    for (let i = 0; i < 7; i++) {
      const s = execFileSync(process.execPath, [import.meta.filename, "--cold", String(n), mode], {
        encoding: "utf8",
      });
      samples.push(Number(s.trim()));
    }
    return stats(samples);
  };

  out.push({
    tests: n,
    warm: {
      handwritten_js: runWarm(() => configDigest(cfg, paths)),
      node_crypto_stream: runWarm(() => configDigestNodeCryptoStream(cfg, paths)),
      node_crypto_joined: runWarm(() => configDigestNodeCryptoJoined(cfg, paths)),
    },
    cold: {
      handwritten_js: runCold("js"),
      node_crypto_stream: runCold("crypto_stream"),
      node_crypto_joined: runCold("crypto_joined"),
    },
  });
}

console.log(JSON.stringify(out, null, 2));
