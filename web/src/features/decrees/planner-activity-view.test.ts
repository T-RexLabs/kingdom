import { describe, expect, it } from "vitest";
import { buildPlannerMessageActivityState } from "./planner-activity-view";

describe("buildPlannerMessageActivityState", () => {
  it("keeps cumulative thought text across multiple reasoning entries", () => {
    const state = buildPlannerMessageActivityState({
      liveItems: [
        { id: "thought-1", kind: "thinking", label: "Thinking", detail: "First reasoning step" },
        { id: "thought-2", kind: "thinking", label: "Thinking", detail: "Second reasoning step" },
      ],
    });

    expect(state.liveThoughtText).toBe("First reasoning step\n\nSecond reasoning step");
    expect(state.currentEvent?.label).toBe("Thinking");
  });

  it("prefers specific tool activity over generic processing", () => {
    const state = buildPlannerMessageActivityState({
      liveItems: [
        { id: "progress-1", kind: "progress", label: "Processing" },
        { id: "tool-1", kind: "tool", label: "Fetching https://example.com", input: '{"url":"https://example.com"}' },
      ],
    });

    expect(state.currentEvent?.label).toBe("Fetching https://example.com");
  });

  it("compacts webfetch bursts into one current event line", () => {
    const state = buildPlannerMessageActivityState({
      liveItems: [
        { id: "tool-1", kind: "tool", label: "Fetched https://popstroke.com/", input: '{"url":"https://popstroke.com/"}' },
        { id: "tool-2", kind: "tool", label: "Fetched https://www.driveshack.com/", input: '{"url":"https://www.driveshack.com/"}' },
        { id: "tool-3", kind: "tool", label: "Fetched https://bigshotsgolf.com/", input: '{"url":"https://bigshotsgolf.com/"}' },
        { id: "progress-1", kind: "progress", label: "Processing" },
      ],
    });

    expect(state.currentEvent?.label).toBe("Fetched 3 web sources");
  });

  it("switches current event to thinking when reasoning becomes the latest meaningful state", () => {
    const state = buildPlannerMessageActivityState({
      liveItems: [
        { id: "phase-1", kind: "progress", label: "Opening provider session" },
        { id: "thought-1", kind: "thinking", label: "Thinking", detail: "Encrypted reasoning placeholder" },
      ],
    });

    expect(state.currentEvent?.label).toBe("Thinking");
  });

  it("keeps source entries attached to the assistant run that fetched them", () => {
    const state = buildPlannerMessageActivityState({
      durableItems: [
        { id: "tool-1", kind: "tool", label: "Fetched https://redbull.com", input: '{"url":"https://redbull.com"}' },
        { id: "tool-2", kind: "tool", label: "Fetched https://cellucor.com", input: '{"url":"https://cellucor.com"}' },
      ],
    });

    expect(state.sources.map((source) => source.url)).toEqual([
      "https://redbull.com",
      "https://cellucor.com",
    ]);
  });
});
