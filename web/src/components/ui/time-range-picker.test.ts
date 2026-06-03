import { describe, expect, it } from "vitest";
import { minutesToRelativeExpr, parseRelativeTime } from "@/components/ui/time-range-picker";

describe("time range picker helpers", () => {
  it("does not emit fractional hour expressions", () => {
    expect(minutesToRelativeExpr(90)).toBe("now-90m");
    expect(minutesToRelativeExpr(150)).toBe("now-150m");
  });

  it("emits day expressions for exact-day windows", () => {
    expect(minutesToRelativeExpr(1440)).toBe("now-1d");
    expect(minutesToRelativeExpr(2880)).toBe("now-2d");
  });

  it("round-trips generated expressions through parser", () => {
    const expressions = [30, 90, 180, 1440, 43200].map(minutesToRelativeExpr);
    for (const expr of expressions) {
      expect(parseRelativeTime(expr)).not.toBeNull();
    }
  });
});
