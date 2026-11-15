/** Date helpers — partitions are plain YYYY-MM-DD strings, no DST surprises (UTC). */

export function addDays(date: string, delta: number): string {
  const d = new Date(date + 'T00:00:00.000Z');
  d.setUTCDate(d.getUTCDate() + delta);
  return toDateStr(d);
}

export function toDateStr(d: Date): string {
  return d.toISOString().slice(0, 10);
}

export function listDates(start: string, end: string): string[] {
  const out: string[] = [];
  let cur = start;
  if (cur > end) throw new Error(`start ${start} is after end ${end}`);
  while (cur <= end) {
    out.push(cur);
    cur = addDays(cur, 1);
  }
  return out;
}

export function parseTs(s: string | null): number {
  if (!s) return 0;
  return new Date(s.endsWith('Z') || s.includes('+') ? s : s.replace(' ', 'T') + 'Z').getTime();
}

export function msSince(s: string | null): number {
  if (!s) return 0;
  return Date.now() - parseTs(s);
}

/** deterministic pseudo-random in [0,1) from a string seed (keeps generated DAG stable) */
export function seededRandom(seed: string): number {
  let h = 2166136261;
  for (let i = 0; i < seed.length; i++) {
    h ^= seed.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  // xorshift-ish mixing
  h ^= h << 13; h ^= h >>> 17; h ^= h << 5;
  return ((h >>> 0) % 1_000_000) / 1_000_000;
}
