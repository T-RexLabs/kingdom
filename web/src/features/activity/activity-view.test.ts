import { describe, expect, it } from "vitest";
import { buildActivityState, parseActivityEvent } from "./activity-view";

describe("parseActivityEvent", () => {
  it("maps a reasoning payload into a thinking item", () => {
    const item = parseActivityEvent({
      payload: { kind: "kingdom.reasoning", activity_id: "p1", label: "Thinking", detail: "step" },
      eventId: "evt-1",
      createdAt: "2026-06-14T10:00:00Z",
    });
    expect(item).toMatchObject({ id: "p1", kind: "thinking", label: "Thinking", detail: "step" });
  });

  it("maps a tool payload, preferring activity_id then falling back to the event id", () => {
    const tool = parseActivityEvent({
      payload: { kind: "kingdom.tool", label: "Reading foo.go", tool_input: '{"path":"foo.go"}' },
      eventId: "evt-2",
    });
    expect(tool).toMatchObject({ id: "evt-2", kind: "tool", label: "Reading foo.go", input: '{"path":"foo.go"}' });
  });

  it("returns null for payloads without a recognized activity kind", () => {
    expect(parseActivityEvent({ payload: { foo: "bar" }, eventId: "evt-3" })).toBeNull();
    expect(parseActivityEvent({ payload: { kind: "kingdom.tool" }, eventId: "" })).toBeNull();
  });
});

describe("buildActivityState", () => {
  it("keeps cumulative thought text across multiple reasoning entries", () => {
    const state = buildActivityState({
      liveItems: [
        { id: "thought-1", kind: "thinking", label: "Thinking", detail: "First reasoning step" },
        { id: "thought-2", kind: "thinking", label: "Thinking", detail: "Second reasoning step" },
      ],
    });

    expect(state.liveThoughtText).toBe("First reasoning step\n\nSecond reasoning step");
    expect(state.currentEvent?.label).toBe("Thinking");
  });

  it("prefers specific tool activity over generic processing", () => {
    const state = buildActivityState({
      liveItems: [
        { id: "progress-1", kind: "progress", label: "Processing" },
        { id: "tool-1", kind: "tool", label: "Fetching https://example.com", input: '{"url":"https://example.com"}' },
      ],
    });

    expect(state.currentEvent?.label).toBe("Fetching https://example.com");
  });

  it("compacts webfetch bursts into one current event line", () => {
    const state = buildActivityState({
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
    const state = buildActivityState({
      liveItems: [
        { id: "phase-1", kind: "progress", label: "Opening provider session" },
        { id: "thought-1", kind: "thinking", label: "Thinking", detail: "Encrypted reasoning placeholder" },
      ],
    });

    expect(state.currentEvent?.label).toBe("Thinking");
  });

  it("keeps source entries attached to the run that fetched them", () => {
    const state = buildActivityState({
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
