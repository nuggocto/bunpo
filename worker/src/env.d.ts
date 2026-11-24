// Secret bindings not stored in wrangler.jsonc.
// Merged with the bindings generated into worker-configuration.d.ts by `wrangler types`.
interface Env {
  BUNPO_QUEUE_TOKEN?: string;
  BUNPO_QUEUE_READ_TOKEN?: string;
}
