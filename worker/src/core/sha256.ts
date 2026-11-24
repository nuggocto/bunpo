// Pure synchronous FIPS 180-4 SHA-256 with streaming UTF-8 string updates.
// Zero dependencies, zero I/O, safe inside Durable Object transactionSync.

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const HEX = "0123456789abcdef";

export class Sha256 {
  private h0 = 0x6a09e667;
  private h1 = 0xbb67ae85;
  private h2 = 0x3c6ef372;
  private h3 = 0xa54ff53a;
  private h4 = 0x510e527f;
  private h5 = 0x9b05688c;
  private h6 = 0x1f83d9ab;
  private h7 = 0x5be0cd19;

  private readonly block = new Uint8Array(64);
  private readonly w = new Uint32Array(64);
  private blockLen = 0;
  private totalBytes = 0;

  private compress(): void {
    const b = this.block;
    const w = this.w;
    for (let i = 0; i < 16; i++) {
      const o = i * 4;
      w[i] = ((b[o]! << 24) | (b[o + 1]! << 16) | (b[o + 2]! << 8) | b[o + 3]!) >>> 0;
    }
    for (let i = 16; i < 64; i++) {
      const w15 = w[i - 15]!;
      const s0 = ((w15 >>> 7) | (w15 << 25)) ^ ((w15 >>> 18) | (w15 << 14)) ^ (w15 >>> 3);
      const w2 = w[i - 2]!;
      const s1 = ((w2 >>> 17) | (w2 << 15)) ^ ((w2 >>> 19) | (w2 << 13)) ^ (w2 >>> 10);
      w[i] = (w[i - 16]! + s0 + w[i - 7]! + s1) >>> 0;
    }

    let a = this.h0;
    let b0 = this.h1;
    let c = this.h2;
    let d = this.h3;
    let e = this.h4;
    let f = this.h5;
    let g = this.h6;
    let h = this.h7;

    for (let i = 0; i < 64; i++) {
      const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
      const ch = (e & f) ^ (~e & g);
      const temp1 = (h + S1 + ch + K[i]! + w[i]!) >>> 0;
      const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
      const maj = (a & b0) ^ (a & c) ^ (b0 & c);
      const temp2 = (S0 + maj) >>> 0;

      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b0;
      b0 = a;
      a = (temp1 + temp2) >>> 0;
    }

    this.h0 = (this.h0 + a) >>> 0;
    this.h1 = (this.h1 + b0) >>> 0;
    this.h2 = (this.h2 + c) >>> 0;
    this.h3 = (this.h3 + d) >>> 0;
    this.h4 = (this.h4 + e) >>> 0;
    this.h5 = (this.h5 + f) >>> 0;
    this.h6 = (this.h6 + g) >>> 0;
    this.h7 = (this.h7 + h) >>> 0;
    this.blockLen = 0;
  }

  updateByte(byte: number): void {
    this.block[this.blockLen++] = byte;
    this.totalBytes++;
    if (this.blockLen === 64) {
      this.compress();
    }
  }

  updateString(s: string): void {
    const len = s.length;
    const block = this.block;
    let blen = this.blockLen;
    let total = this.totalBytes;

    for (let i = 0; i < len; i++) {
      let c = s.charCodeAt(i);
      if (c < 0x80) {
        block[blen++] = c;
        total++;
        if (blen === 64) {
          this.blockLen = 64;
          this.totalBytes = total;
          this.compress();
          blen = 0;
        }
        continue;
      }
      if (c < 0x800) {
        this.blockLen = blen;
        this.totalBytes = total;
        this.updateByte(0xc0 | (c >> 6));
        this.updateByte(0x80 | (c & 0x3f));
        blen = this.blockLen;
        total = this.totalBytes;
        continue;
      }
      if (c >= 0xd800 && c <= 0xdbff && i + 1 < len) {
        const lo = s.charCodeAt(i + 1);
        if (lo >= 0xdc00 && lo <= 0xdfff) {
          c = 0x10000 + ((c - 0xd800) << 10) + (lo - 0xdc00);
          i++;
          this.blockLen = blen;
          this.totalBytes = total;
          this.updateByte(0xf0 | (c >> 18));
          this.updateByte(0x80 | ((c >> 12) & 0x3f));
          this.updateByte(0x80 | ((c >> 6) & 0x3f));
          this.updateByte(0x80 | (c & 0x3f));
          blen = this.blockLen;
          total = this.totalBytes;
          continue;
        }
      }
      this.blockLen = blen;
      this.totalBytes = total;
      this.updateByte(0xe0 | (c >> 12));
      this.updateByte(0x80 | ((c >> 6) & 0x3f));
      this.updateByte(0x80 | (c & 0x3f));
      blen = this.blockLen;
      total = this.totalBytes;
    }

    this.blockLen = blen;
    this.totalBytes = total;
  }

  digestHex(): string {
    const bitLen = this.totalBytes * 8;
    const hiBits = Math.floor(bitLen / 0x100000000) >>> 0;
    const loBits = bitLen >>> 0;

    this.updateByte(0x80);
    while (this.blockLen !== 56) {
      this.updateByte(0);
    }

    const b = this.block;
    b[56] = (hiBits >>> 24) & 0xff;
    b[57] = (hiBits >>> 16) & 0xff;
    b[58] = (hiBits >>> 8) & 0xff;
    b[59] = hiBits & 0xff;
    b[60] = (loBits >>> 24) & 0xff;
    b[61] = (loBits >>> 16) & 0xff;
    b[62] = (loBits >>> 8) & 0xff;
    b[63] = loBits & 0xff;
    this.compress();

    const words = [this.h0, this.h1, this.h2, this.h3, this.h4, this.h5, this.h6, this.h7];
    let out = "";
    for (let i = 0; i < 8; i++) {
      const v = words[i]!;
      out +=
        HEX[(v >>> 28) & 0xf]! +
        HEX[(v >>> 24) & 0xf]! +
        HEX[(v >>> 20) & 0xf]! +
        HEX[(v >>> 16) & 0xf]! +
        HEX[(v >>> 12) & 0xf]! +
        HEX[(v >>> 8) & 0xf]! +
        HEX[(v >>> 4) & 0xf]! +
        HEX[v & 0xf]!;
    }
    return out;
  }
}

export function sha256Hex(s: string): string {
  const h = new Sha256();
  h.updateString(s);
  return h.digestHex();
}
