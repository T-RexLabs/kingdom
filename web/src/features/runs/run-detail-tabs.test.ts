import { describe, expect, it } from "vitest";
import { isRunDetailTab, RUN_DETAIL_TABS } from "./run-detail-tabs";

describe("run detail tabs", () => {
  it("accepts the canonical run-detail tab ids", () => {
    expect(RUN_DETAIL_TABS.every((tab) => isRunDetailTab(tab))).toBe(true);
  });

	it("rejects unknown search tab values", () => {
	  expect(isRunDetailTab("overview")).toBe(false);
	  expect(isRunDetailTab("execution")).toBe(true);
	  expect(isRunDetailTab(null)).toBe(false);
	  expect(isRunDetailTab(42)).toBe(false);
	});
});
