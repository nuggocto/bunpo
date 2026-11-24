// Minimal Node ambient declarations for local test/benchmark harnesses without
// adding @types/node as a dependency.

interface ImportMeta {
  readonly dirname: string;
}

declare namespace NodeJS {
  interface CpuUsage {
    user: number;
    system: number;
  }
  interface Process {
    readonly env: Record<string, string | undefined>;
    cpuUsage(previousValue?: CpuUsage): CpuUsage;
  }
}

declare const process: NodeJS.Process;
declare const console: { log(...args: unknown[]): void; error(...args: unknown[]): void };

declare module "node:fs" {
  export function readdirSync(path: string): string[];
  export function readFileSync(path: string, encoding: "utf8"): string;
  export function existsSync(path: string): boolean;
}

declare module "node:path" {
  export function join(...paths: string[]): string;
  export function resolve(...paths: string[]): string;
}

declare module "node:crypto" {
  export interface Hash {
    update(data: string | Uint8Array): this;
    digest(encoding: "hex"): string;
    digest(): Uint8Array;
  }
  export function createHash(algorithm: string): Hash;
  export function timingSafeEqual(a: Uint8Array, b: Uint8Array): boolean;
}
