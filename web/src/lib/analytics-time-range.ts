export type AnalyticsTimeRange =
  | { kind: "relative"; minutes: number }
  | { kind: "absolute"; startAt: string; endAt: string };

export const DEFAULT_ANALYTICS_TIME_RANGE = {
  kind: "relative",
  minutes: 1440,
} as const satisfies AnalyticsTimeRange;

const MAX_WINDOW_MINUTES = 365 * 24 * 60;
const RFC3339 = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

export interface AnalyticsSearch {
  window_minutes?: number;
  start_at?: string;
  end_at?: string;
  run_id?: string;
  view?: "runs" | "usage" | "reliability";
}

function parseTimestamp(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const parts = RFC3339.exec(value);
  if (!parts) return null;
  const [, year, month, day, hour, minute, second] = parts.map(Number);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (
    month < 1 || month > 12 || day < 1 || day > daysInMonth[month - 1]
    || hour > 23 || minute > 59 || second > 59
  ) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : null;
}

/** URL ranges are normalized to UTC so equivalent instants share one cache entry. */
export function validateAnalyticsSearch(search: Record<string, unknown>): AnalyticsSearch {
  const next: AnalyticsSearch = {};
  if (search.view === "runs" || search.view === "usage" || search.view === "reliability") {
    next.view = search.view;
  }
  if (typeof search.run_id === "string" && search.run_id.trim()) next.run_id = search.run_id;
  const start = parseTimestamp(search.start_at);
  const end = parseTimestamp(search.end_at);
  if (start !== null && end !== null && end > start) {
    next.start_at = new Date(start).toISOString();
    next.end_at = new Date(end).toISOString();
  } else {
    const minutes = typeof search.window_minutes === "number" || typeof search.window_minutes === "string"
      ? Number(search.window_minutes)
      : NaN;
    if (Number.isInteger(minutes) && minutes >= 1 && minutes <= MAX_WINDOW_MINUTES) next.window_minutes = minutes;
  }
  return next;
}

export function analyticsTimeRangeFromSearch(search: AnalyticsSearch): AnalyticsTimeRange {
  return search.start_at && search.end_at
    ? { kind: "absolute", startAt: search.start_at, endAt: search.end_at }
    : { kind: "relative", minutes: search.window_minutes ?? DEFAULT_ANALYTICS_TIME_RANGE.minutes };
}

export function analyticsRangeParams(range: AnalyticsTimeRange): Pick<AnalyticsSearch, "start_at" | "end_at" | "window_minutes"> {
  return range.kind === "absolute"
    ? { start_at: range.startAt, end_at: range.endAt }
    : { window_minutes: range.minutes };
}

export function analyticsTimeRangeIdentity(range: AnalyticsTimeRange): string {
  return range.kind === "absolute" ? `${range.startAt}/${range.endAt}` : `last:${range.minutes}`;
}

export function analyticsTimeWindowDescription(range: AnalyticsTimeRange): string {
  if (range.kind === "absolute") return "the selected date range";
  const minutes = range.minutes;
  const duration = minutes % 1440 === 0 && minutes !== 1440 ? `${minutes / 1440}d`
    : minutes % 60 === 0 ? `${minutes / 60}h` : `${minutes}m`;
  return `the last ${duration}`;
}
