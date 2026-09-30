import type { InfiniteData, QueryClient } from "@tanstack/react-query";
import { mapHistoryPages } from "@/api/history-pages";
import type { PlanningSessionDetail, PlanningSessionListResponse } from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";
import { plannerWorkspaceState } from "./planner-workspace-state";

/** Apply the same canonical cleanup after a delete response or outcome readback. */
export function removeDeletedPlanningSession(qc: QueryClient, sessionId: string): void {
  plannerWorkspaceState(qc).removeSession(sessionId);
  void qc.invalidateQueries({ queryKey: plannerKeys.all() });
  qc.removeQueries({ queryKey: plannerKeys.detail(sessionId) });
}

export function invalidatePlanningBranch(qc: QueryClient, sessionId: string): void {
  void qc.invalidateQueries({ queryKey: plannerKeys.messages(sessionId) });
  void qc.invalidateQueries({ queryKey: plannerKeys.detail(sessionId) });
  void qc.invalidateQueries({ queryKey: ["planning-sessions", sessionId, "variants"] });
}

/**
 * Fields that exist on both PlanningSessionDetail and the list-item
 * summary shape. applyPlanningSessionFields patches the detail cache
 * with arbitrary detail fields, but only fields in this whitelist are
 * applied to list-item summaries — detail-only fields like
 * `model_selection` would otherwise pollute the summary type.
 *
 * Adding a new mirrored field is a one-line edit here. Detail-only
 * fields (model_selection, project_root) are deliberately excluded.
 */
const LIST_MIRRORED_FIELDS = [
  "title",
  "title_state",
  "lifecycle_state",
  "conversation_state",
  "active_leaf_message_id",
  "current_run_id",
  "current_title_run_id",
  "current_draft_id",
  "context_snapshot_id",
  "updated_at",
] as const satisfies ReadonlyArray<keyof PlanningSessionDetail>;

type ListItem = PlanningSessionListResponse["items"][number];

/**
 * Apply a set of field patches to every cache entry that holds the
 * given session. The detail cache receives every supplied field; the
 * list-prefix slices receive only fields in LIST_MIRRORED_FIELDS so
 * detail-only fields can't pollute summary items.
 *
 * Spread is presence-based, so passing `{ current_run_id:
 * undefined }` explicitly clears the field — required for the SSE
 * terminal-event reconciliation path that needs to clear
 * current_run_id on done/error/cancelled.
 *
 * Reference stability: each cache entry returns its current reference
 * unchanged if no field actually changes, so React-query consumers
 * don't fire spurious re-renders.
 */
export function applyPlanningSessionFields(
  qc: QueryClient,
  sessionId: string,
  fields: Partial<PlanningSessionDetail>,
): void {
  if (Object.keys(fields).length === 0) return;

  qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId), (current) => {
    if (!current) return current;
    if (!detailWouldChange(current, fields)) return current;
    return { ...current, ...fields };
  });

  // Pre-compute the list-mirrored slice once; every list cache entry
  // receives the same patch, so there's no reason to recompute it
  // inside the closure for every list slice.
  const listFields: Partial<ListItem> = {};
  for (const key of LIST_MIRRORED_FIELDS) {
    if (key in fields) {
      // Cast safe because LIST_MIRRORED_FIELDS is constrained to
      // keyof PlanningSessionDetail and the matched field exists
      // identically on both shapes.
      (listFields as Record<string, unknown>)[key] = (fields as Record<string, unknown>)[key];
    }
  }
  if (Object.keys(listFields).length === 0) return;

  qc.setQueriesData<InfiniteData<PlanningSessionListResponse>>(
    { queryKey: plannerKeys.listPrefix(), exact: false },
    (current) => mapHistoryPages(current, (page) => patchListItem(page, sessionId, listFields)),
  );
}

export function applyGeneratedPlanningSessionTitleIfPlaceholder(
  qc: QueryClient,
  sessionId: string,
  generatedTitle: string,
): void {
  const trimmedTitle = generatedTitle.trim();
  if (trimmedTitle === "") return;
  const generatedTitleState: PlanningSessionDetail["title_state"] = "generated";
  const generatedListFields: Partial<ListItem> = {
    title: trimmedTitle,
    title_state: generatedTitleState,
  };

  qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId), (current) => {
    if (!current || current.title_state !== "placeholder") return current;
    return { ...current, ...generatedListFields };
  });

  qc.setQueriesData<InfiniteData<PlanningSessionListResponse>>(
    { queryKey: plannerKeys.listPrefix(), exact: false },
    (data) => mapHistoryPages(data, (current) => {
      let changed = false;
      const items = current.items.map((item) => {
        if (item.planning_session_id !== sessionId || item.title_state !== "placeholder") return item;
        changed = true;
        return { ...item, ...generatedListFields };
      });
      return changed ? { ...current, items } : current;
    }),
  );
  void qc.invalidateQueries({ queryKey: plannerKeys.listPrefix() });
}

function detailWouldChange(
  current: PlanningSessionDetail,
  fields: Partial<PlanningSessionDetail>,
): boolean {
  for (const key of Object.keys(fields) as Array<keyof PlanningSessionDetail>) {
    if ((current as Record<string, unknown>)[key as string] !== (fields as Record<string, unknown>)[key as string]) {
      return true;
    }
  }
  return false;
}

function patchListItem(
  current: PlanningSessionListResponse,
  sessionId: string,
  fields: Partial<ListItem>,
): PlanningSessionListResponse {
  let changed = false;
  const items = current.items.map((item) => {
    if (item.planning_session_id !== sessionId) return item;
    let itemChanged = false;
    for (const key of Object.keys(fields) as Array<keyof ListItem>) {
      if ((item as Record<string, unknown>)[key as string] !== (fields as Record<string, unknown>)[key as string]) {
        itemChanged = true;
        break;
      }
    }
    if (!itemChanged) return item;
    changed = true;
    return { ...item, ...fields };
  });
  return changed ? { ...current, items } : current;
}

/**
 * Compare-and-swap the title fields of a single session across every
 * planner cache entry that holds it. Used by the title-save rollback
 * path: when an optimistic write of (trimmed, "custom") fails to land
 * on the server, we revert iff the cache still contains exactly that
 * optimistic write — anything else (a successful concurrent later
 * save, a server-driven flip to "generated", an SSE-applied retry of
 * a different value) is the new source of truth and must not be
 * clobbered.
 *
 * The list cache key is parameterised on `limit`
 * (`plannerKeys.list(limit)`), so multiple list slices can coexist if
 * the app ever opens a list view with a non-default page size.
 * `setQueriesData` over the list prefix covers them all without the
 * call site needing to know which limits are live.
 */
export function compareAndSwapPlanningSessionTitle(
  qc: QueryClient,
  sessionId: string,
  expectedTitle: string,
  expectedTitleState: PlanningSessionDetail["title_state"],
  rollbackTitle: string,
  rollbackTitleState: PlanningSessionDetail["title_state"],
): void {
  qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId), (current) => {
    if (!current) return current;
    if (current.title !== expectedTitle || current.title_state !== expectedTitleState) {
      return current;
    }
    return { ...current, title: rollbackTitle, title_state: rollbackTitleState };
  });
  qc.setQueriesData<InfiniteData<PlanningSessionListResponse>>(
    { queryKey: plannerKeys.listPrefix(), exact: false },
    (data) => mapHistoryPages(data, (current) => {
      let changed = false;
      const items = current.items.map((item) => {
        if (item.planning_session_id !== sessionId) return item;
        if (item.title !== expectedTitle || item.title_state !== expectedTitleState) return item;
        changed = true;
        return { ...item, title: rollbackTitle, title_state: rollbackTitleState };
      });
      return changed ? { ...current, items } : current;
    }),
  );
}
