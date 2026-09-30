import { describe, expect, it } from "vitest";
import { validateWorkOrderNewSearch } from "./work-order-route-search";

describe("validateWorkOrderNewSearch", () => {
  it("accepts only a coherent planning lineage", () => {
    expect(validateWorkOrderNewSearch({
      planning_session_id: " session-1 ",
      planning_draft_id: " draft-1 ",
      planning_context_snapshot_id: "must-not-cross-the-route",
      external_source_snapshot_id: "must-not-cross-the-route",
    })).toEqual({
      planning_session_id: "session-1",
      planning_draft_id: "draft-1",
    });
  });

  it("rejects partial lineage and orphaned context", () => {
    expect(validateWorkOrderNewSearch({ planning_session_id: "session-1" })).toEqual({});
    expect(validateWorkOrderNewSearch({ planning_draft_id: "draft-1" })).toEqual({});
    expect(validateWorkOrderNewSearch({ planning_context_snapshot_id: "context-1" })).toEqual({});
  });
});
