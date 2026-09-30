import { describe, expect, it } from "vitest";
import { formatExactTime, formatRelativeTime } from "./date-time";

describe("timestamp presentation", () => {
  it("uses year-first local time and follows daylight saving transitions", () => {
    expect(formatExactTime("2026-09-30T10:07:59Z", "Europe/Stockholm")).toBe("2026-09-30 12:07");
    expect(formatExactTime("2026-12-30T10:07:59Z", "Europe/Stockholm")).toBe("2026-12-30 11:07");
    expect(formatExactTime("2026-09-30T00:00:00Z", "UTC")).toBe("2026-09-30 00:00");
  });
  it("keeps updates relative with clear units and handles unknown or future values", () => {
    const at = "2026-09-30T10:00:00Z";
    const now = Date.parse(at);
    for (const [seconds, label] of [[10, "10 s ago"], [60, "1 min ago"], [600, "10 min ago"], [3600, "1 h ago"], [86400, "1 day ago"]] as const) {
      expect(formatRelativeTime(at, now + seconds * 1000)).toBe(label);
    }
    expect(formatRelativeTime(at, now - 5000)).toBe("0 s ago");
    expect(formatRelativeTime("missing", now)).toBe("Unknown");
    expect(formatExactTime("missing")).toBe("Unknown");
  });
});
