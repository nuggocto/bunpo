import { createHash, timingSafeEqual } from "node:crypto";

const MIN_TOKEN_BYTES = 16;

export type AuthRole = "none" | "read" | "write";

export interface TokenDigests {
  writeDigest: Uint8Array;
  readDigest: Uint8Array;
}

export type TokenConfigResult =
  | { ok: true; digests: TokenDigests }
  | { ok: false; message: string };

const utf8Encoder = new TextEncoder();

function sha256Bytes(s: string): Uint8Array {
  return createHash("sha256").update(s).digest();
}

// validateTokens checks both bearer tokens and returns their SHA-256 digests so
// timingSafeEqual always compares fixed 32-byte buffers without leaking lengths.
export function validateTokens(
  writeToken: string | undefined,
  readToken: string | undefined,
): TokenConfigResult {
  if (!writeToken || utf8Encoder.encode(writeToken).byteLength < MIN_TOKEN_BYTES) {
    return { ok: false, message: `BUNPO_QUEUE_TOKEN must be set and at least ${MIN_TOKEN_BYTES} bytes` };
  }
  if (!readToken || utf8Encoder.encode(readToken).byteLength < MIN_TOKEN_BYTES) {
    return { ok: false, message: `BUNPO_QUEUE_READ_TOKEN must be set and at least ${MIN_TOKEN_BYTES} bytes` };
  }

  const writeDigest = sha256Bytes(writeToken);
  const readDigest = sha256Bytes(readToken);
  if (timingSafeEqual(writeDigest, readDigest)) {
    return { ok: false, message: "BUNPO_QUEUE_TOKEN and BUNPO_QUEUE_READ_TOKEN must not be equal" };
  }

  return { ok: true, digests: { writeDigest, readDigest } };
}

// authenticateBearer always compares the presented token against both digests so
// response timing does not distinguish write-token matches from read-token matches.
export function authenticateBearer(authHeader: string | null, digests: TokenDigests): AuthRole {
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return "none";
  }
  const token = authHeader.slice("Bearer ".length);
  if (token.length === 0) {
    return "none";
  }

  const presented = sha256Bytes(token);
  const isWrite = timingSafeEqual(presented, digests.writeDigest);
  const isRead = timingSafeEqual(presented, digests.readDigest);

  if (isWrite) {
    return "write";
  }
  if (isRead) {
    return "read";
  }
  return "none";
}
