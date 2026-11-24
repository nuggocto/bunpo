// Pure string, UTF-8, path canonicalization, and integer median helpers
// matching Go's internal/split and internal/timing byte-for-byte.

// compareUTF8 compares two valid Unicode strings in UTF-8 byte order
// (identical to Unicode code-point order, matching Go's `<` / `cmp.Compare`).
export function compareUTF8(a: string, b: string): number {
  const minLen = a.length < b.length ? a.length : b.length;
  let i = 0;
  while (i < minLen) {
    const ca = a.charCodeAt(i);
    const cb = b.charCodeAt(i);
    if (ca !== cb) {
      // If either unit is in the surrogate or post-surrogate range, compare by
      // full Unicode code point so supplementary chars (> U+FFFF) sort after
      // BMP chars U+E000..U+FFFF, matching UTF-8 byte order.
      if (ca >= 0xd800 || cb >= 0xd800) {
        const start = i > 0 && a.charCodeAt(i - 1) >= 0xd800 && a.charCodeAt(i - 1) <= 0xdbff ? i - 1 : i;
        const pa = a.codePointAt(start)!;
        const pb = b.codePointAt(start)!;
        return pa < pb ? -1 : 1;
      }
      return ca < cb ? -1 : 1;
    }
    i++;
  }
  return a.length < b.length ? -1 : a.length > b.length ? 1 : 0;
}

// isCanonicalPOSIXTestID reports whether id is already in canonical POSIX form
// and fits within maxBytes UTF-8 bytes, matching queue.ValidTestID.
export function isCanonicalPOSIXTestID(id: string, maxBytes: number): boolean {
  const len = id.length;
  if (len === 0) {
    return false;
  }

  let bytes = 0;
  let compStart = 0;
  let firstComp = true;

  for (let i = 0; i < len; i++) {
    const c = id.charCodeAt(i);
    if (c < 0x80) {
      bytes++;
      if (c === 0x00 || c === 0x0d || c === 0x0a || c === 0x5c) {
        return false;
      }
      if (c === 0x2f) {
        if (!validComponent(id, compStart, i, firstComp)) {
          return false;
        }
        firstComp = false;
        compStart = i + 1;
      }
    } else if (c < 0x800) {
      bytes += 2;
    } else if (c >= 0xd800 && c <= 0xdbff) {
      if (i + 1 >= len) {
        return false; // lone high surrogate -> invalid UTF-8
      }
      const lo = id.charCodeAt(i + 1);
      if (lo < 0xdc00 || lo > 0xdfff) {
        return false;
      }
      bytes += 4;
      i++;
    } else if (c >= 0xdc00 && c <= 0xdfff) {
      return false; // lone low surrogate -> invalid UTF-8
    } else {
      bytes += 3;
    }
    if (bytes > maxBytes) {
      return false;
    }
  }

  return validComponent(id, compStart, len, firstComp);
}

function validComponent(s: string, start: number, end: number, first: boolean): boolean {
  const compLen = end - start;
  if (compLen === 0) {
    return false;
  }
  if (compLen === 1 && s.charCodeAt(start) === 0x2e) {
    return false; // "."
  }
  if (compLen === 2 && s.charCodeAt(start) === 0x2e && s.charCodeAt(start + 1) === 0x2e) {
    return false; // ".."
  }
  if (first && compLen >= 2 && s.charCodeAt(start + 1) === 0x3a) {
    const c = s.charCodeAt(start);
    if ((c >= 0x61 && c <= 0x7a) || (c >= 0x41 && c <= 0x5a)) {
      return false; // Windows drive letter e.g. "C:"
    }
  }
  return true;
}

// median computes the integer half-up median of non-negative integers,
// matching timing.Median. Returns 0 when values is empty.
export function median(values: readonly number[]): number {
  const n = values.length;
  if (n === 0) {
    return 0;
  }
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = n >> 1;
  if ((n & 1) === 1) {
    return sorted[mid]!;
  }
  const lo = sorted[mid - 1]!;
  const hi = sorted[mid]!;
  const d = hi - lo;
  return lo + Math.floor(d / 2) + (d % 2);
}
