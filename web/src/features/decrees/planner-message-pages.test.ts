import { describe, expect, it } from "vitest";
import type { InfiniteData } from "@tanstack/react-query";
import type { PlanningMessage, PlanningMessagesPage } from "@/api/hooks";
import { replaceAssistantMessageAtTurn } from "./planner-message-pages";

function buildMessage(overrides: Partial<PlanningMessage> = {}): PlanningMessage {
  return {
    planning_message_id: "message-1",
    planning_session_id: "session-1",
    role: "assistant",
    content_markdown: "Original reply",
    created_at: "2026-04-21T10:00:00Z",
    ...overrides,
  };
}

function buildPage(items: PlanningMessage[]): PlanningMessagesPage {
  return {
    planning_session_id: "session-1",
    items,
    total_count: items.length,
    first_item_index: 0,
    previous_cursor: undefined,
  };
}

describe("replaceAssistantMessageAtTurn", () => {
  it("replaces the existing assistant reply sharing the new message's parent across pages", () => {
    const current = {
      pages: [
        buildPage([buildMessage({ planning_message_id: "assistant-4", parent_message_id: "user-4", content_markdown: "Latest reply" })]),
        buildPage([
          buildMessage({ planning_message_id: "assistant-2-old", parent_message_id: "user-2", content_markdown: "Old reply" }),
          buildMessage({ planning_message_id: "assistant-1", parent_message_id: "user-1", content_markdown: "Earlier reply" }),
        ]),
      ],
      pageParams: [undefined, "cursor-1"],
    } satisfies InfiniteData<PlanningMessagesPage, unknown>;

    const updated = replaceAssistantMessageAtTurn(
      current,
      buildMessage({
        planning_message_id: "assistant-2-new",
        parent_message_id: "user-2",
        content_markdown: "Regenerated reply",
      }),
    );

    const allIds = updated?.pages.flatMap((page) => page.items.map((item) => item.planning_message_id)) ?? [];
    expect(allIds).toContain("assistant-2-new");
    expect(allIds).not.toContain("assistant-2-old");
    expect(allIds).toContain("assistant-4");
    expect(allIds).toContain("assistant-1");
  });
});
