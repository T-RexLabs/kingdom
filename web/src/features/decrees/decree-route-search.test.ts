import { describe, expect, it } from "vitest";
import { validateDecreeNewSearch } from "./decree-route-search";

describe("validateDecreeNewSearch", () => {
  it("accepts only a coherent planning lineage", () => {
    expect(validateDecreeNewSearch({
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
    expect(validateDecreeNewSearch({ planning_session_id: "session-1" })).toEqual({});
    expect(validateDecreeNewSearch({ planning_draft_id: "draft-1" })).toEqual({});
    expect(validateDecreeNewSearch({ planning_context_snapshot_id: "context-1" })).toEqual({});
  });
});
