/** Exact local time; the optional zone makes exports and tests deterministic. */
export function formatExactTime(iso: string, timeZone?: string): string {
  const date = new Date(iso);
  if (!Number.isFinite(date.getTime())) return "Unknown";
  const parts = new Intl.DateTimeFormat("en-GB", {
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
    hourCycle: "h23", ...(timeZone ? { timeZone } : {}),
  }).formatToParts(date);
  const value = (type: Intl.DateTimeFormatPartTypes) => parts.find((part) => part.type === type)?.value ?? "";
  return `${value("year")}-${value("month")}-${value("day")} ${value("hour")}:${value("minute")}`;
}

export function formatRelativeTime(iso: string, nowMs: number): string {
  const at = Date.parse(iso);
  if (!Number.isFinite(at) || !Number.isFinite(nowMs)) return "Unknown";
  const seconds = Math.max(0, Math.floor((nowMs - at) / 1000));
  if (seconds < 60) return `${seconds} s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} h ago`;
  const days = Math.floor(seconds / 86400);
  return `${days} ${days === 1 ? "day" : "days"} ago`;
}

export function timestampDescription(iso: string): string {
  const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  return `${formatExactTime(iso, zone)} · ${zone}`;
}
