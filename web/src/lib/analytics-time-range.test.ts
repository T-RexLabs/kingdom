import { describe, expect, it } from "vitest";
import { analyticsTimeRangeFromSearch, analyticsTimeWindowDescription, validateAnalyticsSearch } from "./analytics-time-range";

describe("analytics time range URLs", () => {
  it("normalizes offset dates to exact UTC boundaries and excludes rolling minutes", () => {
    const search = validateAnalyticsSearch({ start_at: "2026-09-01T00:00:00+02:00", end_at: "2026-09-03T00:00:00+02:00", window_minutes: 1440, view: "usage", run_id: "run-1" });
    expect(search).toEqual({ start_at: "2026-08-31T22:00:00.000Z", end_at: "2026-09-02T22:00:00.000Z", view: "usage", run_id: "run-1" });
    expect(analyticsTimeRangeFromSearch(search)).toEqual({ kind: "absolute", startAt: search.start_at, endAt: search.end_at });
  });
  it.each([
    { start_at: "2026-09-01T00:00:00Z" },
    { start_at: "2026-09-01T00:00:00", end_at: "2026-09-03T00:00:00" },
    { start_at: "2026-02-30T00:00:00Z", end_at: "2026-03-03T00:00:00Z" },
    { start_at: "2026-09-03T00:00:00Z", end_at: "2026-09-01T00:00:00Z" },
    { start_at: "2026-09-01T00:00:00Z", end_at: "2026-09-01T00:00:00Z" },
    { window_minutes: "30junk" },
    { window_minutes: 1.5 },
  ])("discards malformed or unsupported ranges: %o", (search) => {
    expect(validateAnalyticsSearch(search)).toEqual({});
    expect(analyticsTimeRangeFromSearch(validateAnalyticsSearch(search))).toEqual({ kind: "relative", minutes: 1440 });
  });
  it("allows future and multi-year calendar dates and retains valid minute precision", () => {
    expect(validateAnalyticsSearch({ start_at: "2020-01-01T00:00:00Z", end_at: "2026-09-02T00:00:00Z" }).start_at).toBe("2020-01-01T00:00:00.000Z");
    expect(validateAnalyticsSearch({ start_at: "2040-01-01T00:00:00Z", end_at: "2040-01-02T00:00:00Z" }).end_at).toBe("2040-01-02T00:00:00.000Z");
    expect(analyticsTimeRangeFromSearch(validateAnalyticsSearch({ window_minutes: "97" }))).toEqual({ kind: "relative", minutes: 97 });
  });
  it("describes absolute empty states without implying a window ending now", () => {
    expect(analyticsTimeWindowDescription({ kind: "absolute", startAt: "2026-09-01T00:00:00Z", endAt: "2026-09-02T00:00:00Z" })).toBe("the selected date range");
    expect(analyticsTimeWindowDescription({ kind: "relative", minutes: 1440 })).toBe("the last 24h");
  });
});
