import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import {
  appendLiveRunActivity,
  liveRunActivityQueryKey,
  selectLiveRunActivity,
  type LiveRunActivityBuffer,
} from "./run-live-activity";

function readBuffer(qc: QueryClient): LiveRunActivityBuffer {
  return qc.getQueryData<LiveRunActivityBuffer>(liveRunActivityQueryKey("run-1")) ?? {};
}

describe("run live activity buffer", () => {
  it("appends and isolates activity per attempt", () => {
    const qc = new QueryClient();
    appendLiveRunActivity(qc, "run-1", "att-1", { id: "p1", kind: "thinking", label: "Thinking", detail: "a" });
    appendLiveRunActivity(qc, "run-1", "att-2", { id: "p2", kind: "tool", label: "Reading x" });

    const buffer = readBuffer(qc);
    expect(selectLiveRunActivity(buffer, "att-1").map((item) => item.id)).toEqual(["p1"]);
    expect(selectLiveRunActivity(buffer, "att-2").map((item) => item.id)).toEqual(["p2"]);
    expect(selectLiveRunActivity(buffer, "missing")).toEqual([]);
    expect(selectLiveRunActivity(buffer, null)).toEqual([]);
  });

  it("merges repeated parts last-wins so growing reasoning replaces its snapshot", () => {
    const qc = new QueryClient();
    appendLiveRunActivity(qc, "run-1", "att-1", { id: "p1", kind: "thinking", label: "Thinking", detail: "First" });
    appendLiveRunActivity(qc, "run-1", "att-1", { id: "p1", kind: "thinking", label: "Thinking", detail: "First and more" });

    const items = selectLiveRunActivity(readBuffer(qc), "att-1");
    expect(items).toHaveLength(1);
    expect(items[0].detail).toBe("First and more");
  });
});
