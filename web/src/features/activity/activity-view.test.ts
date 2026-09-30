import {activityPayload, availableActivityContent} from "./activity-test-helpers";
import { describe, expect, it } from "vitest";
import { buildActivityState, parseActivityEvent } from "./activity-view";

describe("parseActivityEvent", () => {
  it("uses the canonical event identity for runtime ACP phases without inventing provider identity", () => {
    const item = parseActivityEvent({ payload: activityPayload({ kind: "kingdom.phase", activity_id: undefined, phase: "provider_session_open", label: "Opening provider session" }), eventId: "run:3" });
    expect(item).toMatchObject({ id: "run:3", eventId: "run:3", kind: "progress", label: "Opening provider session" });
    expect(item?.providerPartId).toBeUndefined();
    expect(parseActivityEvent({ payload: activityPayload({ kind: "kingdom.tool", activity_id: undefined, label: "Tool Call" }), eventId: "run:4" })).toBeNull();
  });
  it("maps a reasoning payload into a thinking item", () => {
    const item = parseActivityEvent({
      payload: activityPayload({kind:"kingdom.reasoning",activity_id:"p1",label:"Thinking",reasoning:availableActivityContent("step")}),
      eventId: "evt-1",
      createdAt: "2026-06-14T10:00:00Z",
    });
    expect(item).toMatchObject({ id: "p1", kind: "thinking", label: "Thinking", detail: "step" });
  });

  it("maps a tool payload, preserving its stable identity and exact input preview", () => {
    const tool = parseActivityEvent({
      payload: activityPayload({kind:"kingdom.tool",activity_id:"evt-2",label:"Reading foo.go",input:availableActivityContent('{"path":"foo.go"}')}),
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
  it("keeps source entries attached to the run that fetched them", () => {
    const state = buildActivityState({
      durableItems: [
        { id: "tool-1", eventId: "event-14", sequence: 14, kind: "tool", toolName: "webfetch", toolStatus: "completed", label: "Fetched https://redbull.com", input: '{"url":"https://redbull.com"}' },
        { id: "tool-2", eventId: "event-15", sequence: 15, kind: "tool", toolName: "webfetch", toolStatus: "completed", label: "Fetched https://cellucor.com", input: '{"url":"https://cellucor.com"}' },
      ],
    });

    expect(state.sources.map((source) => source.url)).toEqual([
      "https://redbull.com",
      "https://cellucor.com",
    ]);
  });
});

it("retains observed messages independently from reasoning and reconciles repeated parts", () => {
  const content = availableActivityContent("Exact response");
  const first = parseActivityEvent({payload:activityPayload({kind:"kingdom.message",activity_id:"same-part",invocation_id:"inv",provider_part_id:"p",label:"Provider Message",message:content}),eventId:"message-1"});
  expect(first).toMatchObject({kind:"message",messageContent:content,invocationId:"inv",providerPartId:"p"});
  const next = parseActivityEvent({payload:activityPayload({kind:"kingdom.message",activity_id:"same-part",invocation_id:"inv",sequence:2,label:"Provider Message",message:availableActivityContent("Complete response")}),eventId:"message-2"});
  const state = buildActivityState({durableItems:[first!],liveItems:[next!,first!]});
  expect(state.items).toHaveLength(1);
  expect(state.items[0].eventId).toBe("message-2");
  expect(state.items[0].kind).toBe("message");
});
