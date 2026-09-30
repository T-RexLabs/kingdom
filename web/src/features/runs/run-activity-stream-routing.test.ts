import {availableActivityContent} from "@/features/activity/activity-test-helpers";
import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import { applyRunStreamEvent } from "./use-run-event-stream";
import { liveRunActivityQueryKey, type LiveRunActivityBuffer } from "./run-live-activity";
import { liveTimelineQueryKey } from "./run-live-timeline";
import type { ActivityItem } from "@/features/activity/activity-view";
import type { TimelineEvent } from "@/api/hooks-types";

function sseEvent(id: string, data: Record<string, unknown>) {
  return { id, type: "message", data: JSON.stringify(data) };
}

describe("applyRunStreamEvent activity routing", () => {
  it("routes attempt.activity into the per-attempt buffer and never onto the timeline", () => {
    const qc = new QueryClient();
    const applied = applyRunStreamEvent(
      qc,
      "run-1",
      sseEvent("evt-1", {
        event_type: "attempt.activity",
        event_id: "evt-1",
        run_event_seq: 5,
        attempt_seq: 3,
        created_at: "2026-06-14T10:00:00Z",
        attempt_id: "run-1:node-a:1",
        kind: "kingdom.reasoning", sequence:5, input:{availability:"not_provided"},output:{availability:"not_provided"},message:{availability:"not_provided"},
        activity_id: "part-1",
        label: "Thinking",
        reasoning:availableActivityContent("Considering the layout"),
      }),
    );

    expect(applied).toBe(true);
    const buffer = qc.getQueryData<LiveRunActivityBuffer>(liveRunActivityQueryKey("run-1")) ?? {};
    const items = buffer["run-1:node-a:1"] as ActivityItem[] | undefined;
    expect(items).toHaveLength(1);
    expect(items?.[0]).toMatchObject({ id: "part-1", kind: "thinking", detail: "Considering the layout" });
    // Must not pollute the coarse timeline.
    expect(qc.getQueryData(liveTimelineQueryKey("run-1"))).toBeUndefined();
  });

  it("keeps non-activity events on the timeline and out of the activity buffer", () => {
    const qc = new QueryClient();
    applyRunStreamEvent(
      qc,
      "run-1",
      sseEvent("evt-2", {
        event_type: "attempt.started",
        event_id: "evt-2",
        run_event_seq: 6,
        attempt_seq: 1,
        created_at: "2026-06-14T10:01:00Z",
        node_run_id: "nr-1",
      }),
    );

    expect(qc.getQueryData(liveRunActivityQueryKey("run-1"))).toBeUndefined();
    const timeline = qc.getQueryData<TimelineEvent[]>(liveTimelineQueryKey("run-1")) ?? [];
    expect(timeline).toHaveLength(1);
    expect(timeline[0].event_id).toBe("evt-2");
  });
});
