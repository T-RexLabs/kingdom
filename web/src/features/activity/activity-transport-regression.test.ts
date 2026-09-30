import {activityPayload,availableActivityContent} from "./activity-test-helpers";
import { describe, expect, it } from "vitest";
import { mergeActivityTimeline, parseActivityEvent } from "./activity-view";

describe("activity transport fidelity", () => {
  it("uses the provider occurrence time rather than replay ingestion time", () => {
    const item = parseActivityEvent({
      eventId: "event-1",
      createdAt: "2026-09-30T10:00:05Z",
      payload: activityPayload({
        kind: "kingdom.tool", activity_id: "call-1", label: "Read file",
        tool_name: "read", tool_status: "completed", occurred_at: "2026-09-30T10:00:00Z",
      }),
    });
    expect(item?.occurredAt).toBe("2026-09-30T10:00:00Z");
  });

  it("preserves observed result content including meaningful whitespace", () => {
    const output = "\n  exact output\n";
    const item = parseActivityEvent({
      eventId: "event-2",
      payload: activityPayload({
        kind: "kingdom.tool", activity_id: "call-1", label: "Read file",
        tool_name: "read", tool_status: "completed", output: availableActivityContent(output),
      }),
    });
    expect(item?.output).toBe(output);
  });
});

 it("does not regress a completed tool when an older live observation arrives later",()=>{
  const base={id:"call",eventId:"new",sequence:20,kind:"tool" as const,label:"Read file",toolStatus:"completed",output:"",outputContent:availableActivityContent("")};
  const old={...base,eventId:"old",sequence:10,label:"Reading file",toolStatus:"running",output:undefined,outputContent:{availability:"not_provided" as const}};
  expect(mergeActivityTimeline([base],[old])).toEqual([base]);
  expect(mergeActivityTimeline([old],[base])).toEqual([base]);
 });
 it("keeps identical call IDs in different invocations separate",()=>{
  const base={id:"call",eventId:"one",sequence:1,kind:"tool" as const,label:"Read file",invocationId:"one"};
  expect(mergeActivityTimeline([base],[{...base,eventId:"two",sequence:2,invocationId:"two"}])).toHaveLength(2);
 });
