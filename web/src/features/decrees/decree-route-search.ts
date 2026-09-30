export type DecreePlanningLineageSearch = {
  planning_session_id: string;
  planning_draft_id: string;
};

export function validateDecreeNewSearch(search: Record<string, unknown>): DecreePlanningLineageSearch | Record<string, never> {
  const planningSessionId = cleanSearchValue(search.planning_session_id);
  const planningDraftId = cleanSearchValue(search.planning_draft_id);

  if (!planningSessionId || !planningDraftId) {
    return {};
  }

  return {
    planning_session_id: planningSessionId,
    planning_draft_id: planningDraftId,
  };
}

function cleanSearchValue(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  return value.trim() || undefined;
}
