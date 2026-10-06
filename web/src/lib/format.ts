export function fmtNum(n: number): string {
  if (!Number.isFinite(n)) return '–';
  return Math.round(n).toLocaleString('en-US');
}

export function fmtCompact(n: number): string {
  if (!Number.isFinite(n)) return '–';
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (a >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (a >= 1e4) return (n / 1e3).toFixed(1) + 'k';
  return fmtNum(n);
}

export function fmtRate(n: number): string {
  if (!Number.isFinite(n)) return '–';
  if (n === 0) return '0';
  if (n < 10) return n.toFixed(1);
  return fmtCompact(n);
}

export function fmtBytes(b: number): string {
  if (!Number.isFinite(b) || b < 0) return '–';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  let v = b;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v < 10 && i > 0 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

export function fmtDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return '–';
  const s = Math.floor(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

export function fmtTime(ms: number): string {
  const d = new Date(ms);
  return d.toLocaleTimeString('en-GB', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

export function fmtAgo(ms: number, now = Date.now()): string {
  const s = Math.max(0, Math.round((now - ms) / 1000));
  if (s < 5) return 'just now';
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  return `${Math.round(m / 60)}h ago`;
}

/** "orders-3" -> ["orders", 3] (topic names may contain dashes). */
export function splitTP(tp: string): [string, number] {
  const i = tp.lastIndexOf('-');
  return [tp.slice(0, i), Number(tp.slice(i + 1))];
}

/** Shorten a member id "client-1a2b3c4d5e6f7a8b" for display. */
export function shortMember(id: string): string {
  const i = id.lastIndexOf('-');
  return i > 0 && id.length - i > 8 ? `${id.slice(0, i)}·${id.slice(i + 1, i + 5)}` : id;
}

export function prettyJSON(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

export function cx(...parts: (string | false | null | undefined)[]): string {
  return parts.filter(Boolean).join(' ');
}
