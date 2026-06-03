import { describe, expect, it } from "vitest";
import type { InfiniteData } from "@tanstack/react-query";
import type { PlanningMessage, PlanningMessagesPage } from "@/api/hooks";
import {
  flattenRenderedPlanningMessages,
  getPlanningMessagesTotalCount,
  getRenderedFirstItemIndex,
  getRenderedPlanningMessagePages,
  patchLatestPlanningMessagesPage,
  replaceAssistantMessageInLatestPlanningPage,
} from "./planner-message-pages";

function message(id: string, turnIndex: number, role: PlanningMessage["role"]): PlanningMessage {
  return {
    planning_message_id: id,
    planning_session_id: "session-1",
    turn_index: turnIndex,
    role,
    content_markdown: id,
    created_at: `2026-04-22T10:00:${String(turnIndex).padStart(2, "0")}Z`,
    variant_index: 0,
    active: true,
  };
}

function dataFromPages(pages: PlanningMessagesPage[]): InfiniteData<PlanningMessagesPage, string | undefined> {
  return {
    pages,
    pageParams: pages.map((page) => page.previous_cursor ?? undefined),
  };
}

describe("planner message page semantics", () => {
  it("renders pages oldest-to-newest while query order remains newest-first", () => {
    const data = dataFromPages([
      {
        planning_session_id: "session-1",
        items: [message("m-3", 3, "assistant"), message("m-4", 4, "user")],
        total_count: 4,
        first_item_index: 2,
        previous_cursor: "2",
      },
      {
        planning_session_id: "session-1",
        items: [message("m-1", 1, "assistant"), message("m-2", 2, "user")],
        total_count: 4,
        first_item_index: 0,
      },
    ]);

    const renderedPages = getRenderedPlanningMessagePages(data);
    expect(renderedPages[0]?.first_item_index).toBe(0);
    expect(renderedPages[1]?.first_item_index).toBe(2);

    const flattened = flattenRenderedPlanningMessages(data);
    expect(flattened.map((item) => item.planning_message_id)).toEqual(["m-1", "m-2", "m-3", "m-4"]);
  });

  it("derives first item index from the oldest rendered page", () => {
    const data = dataFromPages([
      {
        planning_session_id: "session-1",
        items: [message("m-51", 51, "assistant")],
        total_count: 100,
        first_item_index: 50,
        previous_cursor: "50",
      },
      {
        planning_session_id: "session-1",
        items: [message("m-1", 1, "assistant")],
        total_count: 100,
        first_item_index: 0,
      },
    ]);

    expect(getRenderedFirstItemIndex(data)).toBe(0);
  });

  it("keeps total count stable while patching the latest query page", () => {
    const current = dataFromPages([
      {
        planning_session_id: "session-1",
        items: [message("m-3", 3, "assistant"), message("optimistic-user-1", 4, "user")],
        total_count: 4,
        first_item_index: 2,
        previous_cursor: "2",
      },
      {
        planning_session_id: "session-1",
        items: [message("m-1", 1, "assistant"), message("m-2", 2, "user")],
        total_count: 4,
        first_item_index: 0,
      },
    ]);

    const canonicalUser = message("m-4", 4, "user");
    const patched = patchLatestPlanningMessagesPage(current, canonicalUser, "optimistic-user-1");
    expect(getPlanningMessagesTotalCount(patched)).toBe(4);
    expect(patched?.pages[0]?.items.map((item) => item.planning_message_id)).toEqual(["m-3", "m-4"]);
  });

  it("replaces assistant message on newest query page even with older pages loaded", () => {
    const current = dataFromPages([
      {
        planning_session_id: "session-1",
        items: [message("m-2", 2, "user"), message("m-3", 3, "assistant")],
        total_count: 3,
        first_item_index: 1,
        previous_cursor: "1",
      },
      {
        planning_session_id: "session-1",
        items: [message("m-1", 1, "assistant")],
        total_count: 3,
        first_item_index: 0,
      },
    ]);

    const replacement = {
      ...message("m-3", 3, "assistant"),
      content_markdown: "updated",
    };
    const patched = replaceAssistantMessageInLatestPlanningPage(current, replacement);
    expect(patched?.pages[1]?.items[0]?.content_markdown).toBe("m-1");
    expect(patched?.pages[0]?.items[1]?.content_markdown).toBe("updated");
    expect(patched?.pages[1]?.items[0]?.content_markdown).not.toBe("updated");
    expect(patched?.pages[0]?.items.find((item) => item.planning_message_id === "m-3")?.content_markdown).toBe("updated");
  });
});
