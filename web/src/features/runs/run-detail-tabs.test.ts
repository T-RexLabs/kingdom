import { describe, expect, it } from "vitest";
import {
  isRunDetailTab,
  parseRunDetailSearch,
  RUN_DETAIL_TABS,
} from "./run-detail-tabs";

describe("run detail tabs", () => {
  it("scopes contextual sections to Run Details and rejects removed primary destinations", () => {
    expect(
      parseRunDetailSearch({
        tab: "output",
        panel: "details",
        section: "audit",
      }),
    ).toEqual({ panel: "details", section: "audit" });
    expect(
      parseRunDetailSearch({
        tab: "execution",
        panel: "files",
        section: "audit",
      }),
    ).toEqual({ tab: "execution", panel: "files" });
    expect(
      parseRunDetailSearch({ panel: "details", section: "execution" }),
    ).toEqual({ panel: "details" });
    expect(parseRunDetailSearch({ panel: "more", section: "context" })).toEqual(
      {},
    );
  });
  it("accepts the canonical run-detail tab ids", () => {
    expect(RUN_DETAIL_TABS).toEqual(["workflow", "execution"]);
    expect(RUN_DETAIL_TABS.every((tab) => isRunDetailTab(tab))).toBe(true);
  });

  it("rejects unknown search tab values", () => {
    expect(isRunDetailTab("overview")).toBe(false);
    expect(isRunDetailTab("output")).toBe(false);
    expect(isRunDetailTab("context")).toBe(false);
    expect(isRunDetailTab("execution")).toBe(true);
    expect(isRunDetailTab(null)).toBe(false);
    expect(isRunDetailTab(42)).toBe(false);
  });
});
