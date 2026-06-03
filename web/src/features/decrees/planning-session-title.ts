type PlanningSessionTitleState = {
  title?: string | null;
  title_state?: string | null;
};

export function hasPlaceholderTitle(session: PlanningSessionTitleState | null | undefined): boolean {
  return session?.title_state === "placeholder";
}

export function getCanonicalPlanningSessionTitle(
  session: PlanningSessionTitleState | null | undefined
): string {
  if (hasPlaceholderTitle(session)) {
    return "";
  }
  return session?.title?.trim() ?? "";
}

function coalescePlanningSessionDisplayTitle(title: string, emptyTitle: string): string {
  const trimmedTitle = title.trim();
  return trimmedTitle === "" ? emptyTitle : trimmedTitle;
}

export function getPlanningSessionDisplayTitle(
  session: PlanningSessionTitleState | null | undefined,
  emptyTitle = ""
): string {
  const title = getCanonicalPlanningSessionTitle(session);
  return coalescePlanningSessionDisplayTitle(title, emptyTitle);
}

export function getPlanningSessionDisplayTitleValue(title: string, emptyTitle = ""): string {
  return coalescePlanningSessionDisplayTitle(title, emptyTitle);
}
