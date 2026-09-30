import { describe, expect, it } from "vitest";
import {
  initialActivityPosition,
  reconcileActivityPosition,
} from "./activity-virtual-state";
const rows = (...keys: string[]) => keys.map((key) => ({ key }));

describe("activity virtual scroll position", () => {
  it("preserves a visible event while history prepends and live events append together", () => {
    const previous = {
      ...initialActivityPosition(rows("old-gap", "a", "b")),
      anchorKey: "b",
    };
    const next = reconcileActivityPosition(
      previous,
      rows("new-gap", "older-1", "older-2", "a", "b", "new-live"),
    );
    expect(next.firstItemIndex + 4).toBe(previous.firstItemIndex + 2);
  });
  it("accounts for a consumed gap control and inserted intervening events", () => {
    const previous = {
      ...initialActivityPosition(rows("a", "gap", "b")),
      anchorKey: "b",
    };
    const next = reconcileActivityPosition(
      previous,
      rows("a", "intervening-1", "intervening-2", "b"),
    );
    expect(next.firstItemIndex + 3).toBe(previous.firstItemIndex + 2);
  });
  it("does not move existing coordinates when only later events arrive", () => {
    const previous = {
      ...initialActivityPosition(rows("a", "b")),
      anchorKey: "a",
    };
    expect(
      reconcileActivityPosition(previous, rows("a", "b", "c")).firstItemIndex,
    ).toBe(previous.firstItemIndex);
  });
});
