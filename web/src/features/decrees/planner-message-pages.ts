import type { InfiniteData } from "@tanstack/react-query";
import type { PlanningMessage, PlanningMessagesPage } from "@/api/hooks";
import { plannerKeys, PLANNER_MESSAGES_PAGE_SIZE } from "./planner-query-keys";

export { PLANNER_MESSAGES_PAGE_SIZE } from "./planner-query-keys";

type PlanningMessagesInfiniteData<TPageParam = unknown> = InfiniteData<PlanningMessagesPage, TPageParam>;

export function plannerMessagesQueryKey(sessionId: string, limit = PLANNER_MESSAGES_PAGE_SIZE) {
  return plannerKeys.messages(sessionId, limit);
}

export function getRenderedPlanningMessagePages(
  data: PlanningMessagesInfiniteData | undefined
): PlanningMessagesPage[] {
  if (!data?.pages?.length) {
    return [];
  }
  return [...data.pages].reverse();
}

export function flattenRenderedPlanningMessages(
  data: PlanningMessagesInfiniteData | undefined
): PlanningMessage[] {
  return getRenderedPlanningMessagePages(data).flatMap((page) => page.items ?? []);
}

export function getRenderedFirstItemIndex(
  data: PlanningMessagesInfiniteData | undefined
): number {
  const oldestRenderedPage = getRenderedPlanningMessagePages(data)[0];
  return oldestRenderedPage?.first_item_index ?? 0;
}

export function getPlanningMessagesTotalCount(
  data: PlanningMessagesInfiniteData | undefined
): number {
  return data?.pages[0]?.total_count ?? 0;
}

export function patchLatestPlanningMessagesPage(
  data: PlanningMessagesInfiniteData | undefined,
  message: PlanningMessage,
  optimisticKey?: string
): PlanningMessagesInfiniteData | undefined {
  if (!data?.pages?.length) {
    return data;
  }

  const pages = data.pages.map((page, index) => {
    if (index !== 0) {
      return page;
    }

    const filteredItems = page.items.filter((item) => item.planning_message_id !== message.planning_message_id);
    if (optimisticKey) {
      const optimisticIndex = filteredItems.findIndex((item) => item.planning_message_id === optimisticKey);
      if (optimisticIndex >= 0) {
        const nextItems = [...filteredItems];
        nextItems.splice(optimisticIndex, 1, message);
        return { ...page, items: nextItems };
      }
    }

    return { ...page, items: [...filteredItems, message] };
  });

  return { ...data, pages };
}

export function filterPlanningMessages(
  data: PlanningMessagesInfiniteData | undefined,
  predicate: (message: PlanningMessage) => boolean
): PlanningMessagesInfiniteData | undefined {
  if (!data?.pages?.length) {
    return data;
  }

  const pages = data.pages.map((page) => {
    const filtered = page.items.filter(predicate);
    if (filtered.length === page.items.length) {
      return page;
    }
    return { ...page, items: filtered };
  });

  const newTotal = pages.reduce((sum, page) => sum + page.items.length, 0);
  if (pages[0].total_count !== newTotal) {
    pages[0] = { ...pages[0], total_count: newTotal };
  }

  return { ...data, pages };
}

export function replaceAssistantMessageInLatestPlanningPage(
  data: PlanningMessagesInfiniteData | undefined,
  message: PlanningMessage
): PlanningMessagesInfiniteData | undefined {
  if (!data?.pages?.length) {
    return data;
  }

  const pages = data.pages.map((page, index) => {
    if (index !== 0) {
      return page;
    }

    const nextItems = page.items.filter((item) => item.planning_message_id !== message.planning_message_id);
    nextItems.push(message);
    return { ...page, items: nextItems };
  });

  return { ...data, pages };
}

export function replaceAssistantMessageAtTurn(
  data: PlanningMessagesInfiniteData | undefined,
  message: PlanningMessage
): PlanningMessagesInfiniteData | undefined {
  if (!data?.pages?.length) {
    return data;
  }

  let inserted = false;
  const pages = data.pages.map((page) => {
    let replacementIndex = -1;
    const nextItems = page.items.filter((item, index) => {
      if (item.planning_message_id === message.planning_message_id) {
        replacementIndex = index;
        return false;
      }
      const isSupersededAssistant = item.role === "assistant" && item.turn_index === message.turn_index;
      if (isSupersededAssistant) {
        replacementIndex = index;
        return false;
      }
      return true;
    });
    if (!inserted && replacementIndex >= 0) {
      const boundedIndex = Math.min(replacementIndex, nextItems.length);
      nextItems.splice(boundedIndex, 0, message);
      inserted = true;
    }
    return { ...page, items: nextItems };
  });

  if (!inserted) {
    pages[0] = {
      ...pages[0],
      items: [...pages[0].items, message],
    };
  }

  return { ...data, pages };
}
