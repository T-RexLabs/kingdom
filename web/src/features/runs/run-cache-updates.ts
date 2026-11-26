import type { QueryClient } from "@tanstack/react-query";
import type { RunDetailSummary, RunListResponse, RunSummary } from "@/api/hooks-types";
import { isRunsCollectionQueryKey, parseRunsCollectionQueryKey, runQueryKeys, type RunsStateFilter } from "@/api/hooks-runs";

type RunModelSelection = NonNullable<RunSummary["model_selection"]>;
type RunModelSelectionEntry = RunModelSelection["global"];
type RunModelSelectionOverrides = NonNullable<RunModelSelection["role_overrides"]>;

const runStreamStateKey = (runId: string) => ["runs", runId, "__stream_state"] as const;
const runCollectionStreamStateKey = ["runs", "collection", "__stream_state"] as const;

interface RunStreamState {
	lastAppliedRunEventSeq: number;
	lastEventId: string | null;
}

interface RunCollectionStreamState {
	lastEventId: string | null;
}

interface InfiniteRunCollectionData {
  pageParams: unknown[];
  pages: RunListResponse[];
}

export function patchRunDetailFields(
  qc: QueryClient,
  runId: string,
  fields: Partial<RunDetailSummary>,
): void {
  if (Object.keys(fields).length === 0) return;
  qc.setQueryData<RunDetailSummary>(runQueryKeys.detail(runId), (current) => {
    if (!current) return current;
		const nextFields = normalizeRunDetailFields(current, fields);
		if (Object.keys(nextFields).length === 0 || !wouldChange(current, nextFields)) return current;
		return { ...current, ...nextFields };
  });
}

export function applyRunStreamHighWaterMark(
	qc: QueryClient,
	runId: string,
	runEventSeq: number | undefined,
): boolean {
	if (!Number.isInteger(runEventSeq) || runEventSeq === undefined || runEventSeq < 1) {
		return true;
	}

	const baseline = readRunStreamBootstrapState(qc, runId).lastAppliedRunEventSeq;
	if (runEventSeq <= baseline) {
		patchRunStreamState(qc, runId, { lastAppliedRunEventSeq: baseline });
		return false;
	}

	patchRunStreamState(qc, runId, { lastAppliedRunEventSeq: runEventSeq });
	qc.setQueryData<RunDetailSummary>(runQueryKeys.detail(runId), (current) => {
		if (!current) return current;
		const nextActivityEvents = Math.max(current.tab_counts?.activity_events ?? 0, runEventSeq);
		if ((current.tab_counts?.activity_events ?? 0) === nextActivityEvents) {
			return current;
		}
		return {
			...current,
			tab_counts: {
				...current.tab_counts,
				activity_events: nextActivityEvents,
			},
		};
	});
	return true;
}

export function readRunStreamBootstrapState(qc: QueryClient, runId: string): RunStreamState {
	const stored = readStoredRunStreamState(qc, runId);
	const cursor = readRunDetailStreamCursor(qc, runId);
	return {
		lastAppliedRunEventSeq: Math.max(stored?.lastAppliedRunEventSeq ?? 0, cursor?.run_event_seq ?? 0),
		lastEventId: stored ? stored.lastEventId : normalizeEventId(cursor?.event_id),
	};
}

export function writeRunStreamResumeCursor(qc: QueryClient, runId: string, eventId: string): void {
	const trimmedEventID = eventId.trim();
	if (trimmedEventID === "") {
		return;
	}
	patchRunStreamState(qc, runId, { lastEventId: trimmedEventID });
}

export function clearRunStreamResumeToken(qc: QueryClient, runId: string): void {
	patchRunStreamState(qc, runId, { lastEventId: null });
}

export function readRunCollectionStreamResumeCursor(qc: QueryClient): string | null {
	const lastEventId = qc.getQueryData<RunCollectionStreamState>(runCollectionStreamStateKey)?.lastEventId;
	return typeof lastEventId === "string" && lastEventId.trim() !== "" ? lastEventId : null;
}

export function writeRunCollectionStreamResumeCursor(qc: QueryClient, eventId: string): void {
	const trimmedEventID = eventId.trim();
	if (trimmedEventID === "") {
		return;
	}
	qc.setQueryData<RunCollectionStreamState>(runCollectionStreamStateKey, { lastEventId: trimmedEventID });
}

export function clearRunCollectionStreamResumeCursor(qc: QueryClient): void {
	qc.setQueryData<RunCollectionStreamState>(runCollectionStreamStateKey, { lastEventId: null });
}

export function upsertRunCollections(
	qc: QueryClient,
	run: RunSummary,
	): readonly (readonly unknown[])[] {
	const invalidatedKeys: Array<readonly unknown[]> = [];
	for (const query of qc.getQueryCache().findAll({ predicate: (candidate) => isRunsCollectionQueryKey(candidate.queryKey) && candidate.isActive() })) {
		const descriptor = parseRunsCollectionQueryKey(query.queryKey);
		if (!descriptor || descriptor.search !== null) {
			continue;
		}
		if (descriptor.infinite) {
			const current = query.state.data as InfiniteRunCollectionData | undefined;
			const result = patchInfiniteRunCollection(current, run, descriptor.stateFilter);
			if (result.needsInvalidation) {
				appendQueryKey(invalidatedKeys, query.queryKey);
			}
			if (result.data !== current) {
				qc.setQueryData<InfiniteRunCollectionData | undefined>(query.queryKey, result.data);
			}
			continue;
		}
		const current = query.state.data as RunListResponse | undefined;
		const result = patchFiniteRunCollection(current, run, descriptor.stateFilter, descriptor.limit);
		if (result.needsInvalidation) {
			appendQueryKey(invalidatedKeys, query.queryKey);
		}
		if (result.data !== current) {
			qc.setQueryData<RunListResponse | undefined>(query.queryKey, result.data);
		}
	}
	return invalidatedKeys;
}

export function activeStreamOwnedRunCollectionQueryKeys(qc: QueryClient): readonly (readonly unknown[])[] {
	return qc.getQueryCache()
		.findAll({ predicate: (candidate) => isRunsCollectionQueryKey(candidate.queryKey) && candidate.isActive() })
		.filter((query) => {
			const descriptor = parseRunsCollectionQueryKey(query.queryKey);
			return descriptor?.search === null;
		})
		.map((query) => query.queryKey);
}

export function inactiveStreamOwnedRunCollectionQueryKeys(qc: QueryClient): readonly (readonly unknown[])[] {
	return qc.getQueryCache()
		.findAll({ predicate: (candidate) => isRunsCollectionQueryKey(candidate.queryKey) && !candidate.isActive() })
		.filter((query) => {
			const descriptor = parseRunsCollectionQueryKey(query.queryKey);
			return descriptor?.search === null;
		})
		.map((query) => query.queryKey);
}

export function buildRunSummaryFromCollectionEvent(data: Record<string, unknown>): RunSummary | null {
	const runId = readString(data.run_id);
	const workflowId = readString(data.workflow_id);
	const workOrderId = readString(data.work_order_id);
	const workOrderTitle = readString(data.work_order_title);
	const status = readString(data.status);
	const planState = readString(data.plan_state);
	const displayState = readString(data.display_state);
	const createdAt = readString(data.created_at);
	const updatedAt = readString(data.updated_at);
	const lastActivityAt = readString(data.last_activity_at);
	const autonomousApprovals = readBoolean(data.autonomous_approvals);
	const pendingApprovalKey = readString(data.pending_approval_key);
	const publishMode = readString(data.publish_mode);
	const start = readRunStartProjection(data);
	if (!runId || !workflowId || !workOrderId || !workOrderTitle || !status || !planState || !displayState || !createdAt || !updatedAt || !lastActivityAt) {
		return null;
	}
	if (!start) return null;

	const summary: RunSummary = {
		run_id: runId,
		workflow_id: workflowId,
		work_order_id: workOrderId,
		work_order_title: workOrderTitle,
		status: status as RunSummary["status"],
		plan_state: planState as RunSummary["plan_state"],
		display_state: displayState as RunSummary["display_state"],
		created_at: createdAt,
		updated_at: updatedAt,
		last_activity_at: lastActivityAt,
		...start,
		...(autonomousApprovals === undefined ? {} : { autonomous_approvals: autonomousApprovals }),
		...(pendingApprovalKey ? { pending_approval_key: pendingApprovalKey } : {}),
		...(publishMode ? { publish_mode: publishMode as RunSummary["publish_mode"] } : {}),
	};

	if (isRecord(data.model_selection)) {
		summary.model_selection = data.model_selection as RunSummary["model_selection"];
	}

	return summary;
}

export function readRunStartProjection(data: Record<string, unknown>): Pick<RunSummary, "start_state" | "started_at"> | null {
	if (data.start_state === "observed") {
		const startedAt = readString(data.started_at);
		return startedAt && Number.isFinite(Date.parse(startedAt))
			? { start_state: "observed", started_at: startedAt }
			: null;
	}
	if (data.start_state === "not_started" || data.start_state === "unrecorded") {
		return data.started_at === undefined
			? { start_state: data.start_state, started_at: undefined }
			: null;
	}
	return null;
}
function wouldChange<T extends object>(current: T, fields: Partial<T>): boolean {
  for (const key of Object.keys(fields) as Array<keyof T>) {
    if ((current as Record<string, unknown>)[key as string] !== (fields as Record<string, unknown>)[key as string]) {
      return true;
    }
  }
  return false;
}

function patchRunStreamState(qc: QueryClient, runId: string, fields: Partial<RunStreamState>): void {
	qc.setQueryData<RunStreamState>(runStreamStateKey(runId), (current) => ({
		lastAppliedRunEventSeq: current?.lastAppliedRunEventSeq ?? 0,
		lastEventId: current?.lastEventId ?? null,
		...fields,
	}));
}

function readStoredRunStreamState(qc: QueryClient, runId: string): RunStreamState | null {
	const streamState = qc.getQueryData<RunStreamState>(runStreamStateKey(runId));
	if (!streamState) {
		return null;
	}
	const lastAppliedRunEventSeq = Number.isInteger(streamState.lastAppliedRunEventSeq) && streamState.lastAppliedRunEventSeq > 0
		? streamState.lastAppliedRunEventSeq
		: 0;
	const lastEventId = normalizeEventId(streamState.lastEventId);
	if (lastAppliedRunEventSeq === 0 && lastEventId === null) {
		return null;
	}
	return {
		lastAppliedRunEventSeq,
		lastEventId,
	};
}

function readRunDetailStreamCursor(qc: QueryClient, runId: string): RunDetailSummary["stream_cursor"] | null {
	const detail = qc.getQueryData<RunDetailSummary>(runQueryKeys.detail(runId));
	if (!detail?.stream_cursor) {
		return null;
	}
	const runEventSeq = detail.stream_cursor.run_event_seq;
	if (!Number.isInteger(runEventSeq) || runEventSeq < 1) {
		return null;
	}
	const eventId = normalizeEventId(detail.stream_cursor.event_id);
	if (!eventId) {
		return null;
	}
	return { event_id: eventId, run_event_seq: runEventSeq };
}

function normalizeRunDetailFields<T extends RunDetailSummary>(current: T, fields: Partial<T>): Partial<T> {
	const nextFields = { ...fields };
	if (
		typeof current.updated_at === "string"
		&& typeof nextFields.updated_at === "string"
		&& current.updated_at >= nextFields.updated_at
	) {
		delete nextFields.updated_at;
	}
	if (
		typeof current.last_activity_at === "string"
		&& typeof nextFields.last_activity_at === "string"
		&& current.last_activity_at >= nextFields.last_activity_at
	) {
		delete nextFields.last_activity_at;
	}
	return nextFields;
}

function normalizeEventId(value: unknown): string | null {
	return typeof value === "string" && value.trim() !== "" ? value.trim() : null;
}

interface FiniteCollectionPatchResult {
	data: RunListResponse | undefined;
	needsInvalidation: boolean;
}

interface InfiniteCollectionPatchResult {
  data: InfiniteRunCollectionData | undefined;
  needsInvalidation: boolean;
}

function patchFiniteRunCollection(
	current: RunListResponse | undefined,
	run: RunSummary,
	stateFilter: RunsStateFilter | null,
	limit: number | null,
	): FiniteCollectionPatchResult {
	if (!current) return { data: current, needsInvalidation: false };

	const existingIndex = current.items.findIndex((item) => item.run_id === run.run_id);
	const matches = matchesCollectionScope(run, stateFilter);
	if (!matches) {
		if (existingIndex === -1) {
			return { data: current, needsInvalidation: false };
		}
		const nextItems = current.items.filter((item) => item.run_id !== run.run_id);
		if (sameRunItems(current.items, nextItems)) {
			return { data: current, needsInvalidation: false };
		}
		return {
			data: { ...current, items: nextItems },
			needsInvalidation: Boolean(limit && limit > 0 && current.items.length >= limit),
		};
	}

	const items = patchCollectionItems(current.items, run, stateFilter);
	const boundedItems = limit && limit > 0 ? items.slice(0, limit) : items;
	if (sameRunItems(current.items, boundedItems)) {
		return { data: current, needsInvalidation: false };
	}
	return { data: { ...current, items: boundedItems }, needsInvalidation: false };
}

function patchCollectionItems(
	items: RunSummary[],
	run: RunSummary,
	stateFilter: RunsStateFilter | null,
): RunSummary[] {
	const existingIndex = items.findIndex((item) => item.run_id === run.run_id);
	const matches = matchesCollectionScope(run, stateFilter);

	if (!matches) {
		if (existingIndex === -1) {
			return items;
		}
		return items.filter((item) => item.run_id !== run.run_id);
	}

	const nextItems = existingIndex === -1
		? [run, ...items]
		: replaceCollectionItem(items, existingIndex, run);
	if (sameRunItems(items, nextItems)) {
		return items;
	}
	nextItems.sort((left, right) => compareRunsForCollection(left, right, stateFilter));
	return nextItems;
}

function replaceCollectionItem(items: RunSummary[], index: number, run: RunSummary): RunSummary[] {
	const currentItem = items[index];
	if (!currentItem) {
		return items;
	}
	const nextItem = mergeRunSummary(currentItem, run);
	if (nextItem === currentItem) {
		return items;
	}
	return items.map((item, currentIndex) => (currentIndex === index ? nextItem : item));
}

function patchInfiniteRunCollection(
  current: InfiniteRunCollectionData | undefined,
  run: RunSummary,
  stateFilter: RunsStateFilter | null,
): InfiniteCollectionPatchResult {
  if (!current) {
    return { data: current, needsInvalidation: false };
  }

  const existingLocation = findInfiniteRunLocation(current.pages, run.run_id);
	const matches = matchesCollectionScope(run, stateFilter);
  const hasStableOrder = stateFilter !== "awaiting_human_decision";

  if (!existingLocation) {
    return {
      data: current,
      needsInvalidation: matches,
    };
  }

  if (!matches) {
    return {
      data: removeInfiniteRunAt(current, existingLocation.pageIndex, existingLocation.itemIndex),
      needsInvalidation: true,
    };
  }

  return {
    data: replaceInfiniteRunAt(current, existingLocation.pageIndex, existingLocation.itemIndex, run),
    needsInvalidation: !hasStableOrder,
  };
}

function matchesCollectionScope(
	run: RunSummary,
	stateFilter: RunsStateFilter | null,
): boolean {
	return matchesStateFilter(run, stateFilter);
}

function matchesStateFilter(run: RunSummary, stateFilter: RunsStateFilter | null): boolean {
	if (!stateFilter) {
		return true;
	}
	switch (stateFilter) {
		case "pending":
			return run.status === "pending";
		case "running":
			return run.status === "running" && run.plan_state !== "awaiting_plan_decision";
		case "awaiting_human_decision":
			return run.autonomous_approvals === false
				&& ((run.status === "running" && run.plan_state === "awaiting_plan_decision") || run.status === "awaiting_human_confirmation");
		case "succeeded":
			return run.status === "succeeded";
		case "failed":
			return run.status === "failed";
		case "cancelled":
			return run.status === "cancelled";
	}
	return true;
}

function findInfiniteRunLocation(
  pages: RunListResponse[],
  runId: string,
): { pageIndex: number; itemIndex: number } | null {
  for (let pageIndex = 0; pageIndex < pages.length; pageIndex += 1) {
    const itemIndex = pages[pageIndex]?.items.findIndex((item) => item.run_id === runId) ?? -1;
    if (itemIndex !== -1) {
      return { pageIndex, itemIndex };
    }
  }
  return null;
}

function replaceInfiniteRunAt(
  current: InfiniteRunCollectionData,
  pageIndex: number,
  itemIndex: number,
  run: RunSummary,
): InfiniteRunCollectionData {
  const targetPage = current.pages[pageIndex];
  if (!targetPage) {
    return current;
  }
  const currentItem = targetPage.items[itemIndex];
  if (!currentItem) {
    return current;
  }

  const nextItem = mergeRunSummary(currentItem, run);
  if (nextItem === currentItem) {
    return current;
  }

  const nextPages = current.pages.map((page, currentPageIndex) => {
    if (currentPageIndex !== pageIndex) {
      return page;
    }
    return {
      ...page,
      items: page.items.map((item, currentItemIndex) => (
        currentItemIndex === itemIndex ? nextItem : item
      )),
    };
  });
  return { ...current, pages: nextPages };
}

function removeInfiniteRunAt(
  current: InfiniteRunCollectionData,
  pageIndex: number,
  itemIndex: number,
): InfiniteRunCollectionData {
  const targetPage = current.pages[pageIndex];
  if (!targetPage || !targetPage.items[itemIndex]) {
    return current;
  }

  const nextPages = current.pages.map((page, currentPageIndex) => {
    if (currentPageIndex !== pageIndex) {
      return page;
    }
    return {
      ...page,
      items: page.items.filter((_item, currentItemIndex) => currentItemIndex !== itemIndex),
    };
  });
  return { ...current, pages: nextPages };
}

function compareRunsForCollection(left: RunSummary, right: RunSummary, stateFilter: RunsStateFilter | null): number {
	const leftOrder = stateFilter === "awaiting_human_decision" ? left.updated_at : left.created_at;
	const rightOrder = stateFilter === "awaiting_human_decision" ? right.updated_at : right.created_at;
	if (leftOrder > rightOrder) return -1;
	if (leftOrder < rightOrder) return 1;
	if (left.run_id > right.run_id) return -1;
	if (left.run_id < right.run_id) return 1;
	return 0;
}

function sameRunItems(current: RunSummary[], next: RunSummary[]): boolean {
	if (current.length !== next.length) {
		return false;
	}
	for (let index = 0; index < current.length; index += 1) {
		if (current[index] !== next[index]) {
			return false;
		}
	}
	return true;
}

function shallowEqualRunSummary(left: RunSummary, right: RunSummary): boolean {
  for (const [key, value] of Object.entries(left)) {
    if (!equalRunSummaryField(key as keyof RunSummary, value, right[key as keyof RunSummary])) {
      return false;
    }
  }
  for (const [key, value] of Object.entries(right)) {
    if (!equalRunSummaryField(key as keyof RunSummary, value, left[key as keyof RunSummary])) {
      return false;
    }
  }
  return true;
}

function equalRunSummaryField(key: keyof RunSummary, left: unknown, right: unknown): boolean {
	if (key === "model_selection") {
		return equalRunModelSelection(
			left as RunSummary["model_selection"] | undefined,
			right as RunSummary["model_selection"] | undefined,
		);
	}
	return left === right;
}

function equalRunModelSelection(
	left: RunSummary["model_selection"] | undefined,
	right: RunSummary["model_selection"] | undefined,
): boolean {
	if (left === right) {
		return true;
	}
	if (!left || !right) {
		return left === right;
	}
	if (!equalModelSelectionEntry(left.global, right.global)) {
		return false;
	}
	return equalModelSelectionOverrides(left.role_overrides, right.role_overrides);
}

function equalModelSelectionOverrides(
	left: RunModelSelectionOverrides | undefined,
	right: RunModelSelectionOverrides | undefined,
): boolean {
	if (left === right) {
		return true;
	}
	const leftEntries = Object.entries(left ?? {}) as Array<[string, RunModelSelectionEntry]>;
	const rightEntries = Object.entries(right ?? {}) as Array<[string, RunModelSelectionEntry]>;
	if (leftEntries.length !== rightEntries.length) {
		return false;
	}
	for (const [roleId, leftEntry] of leftEntries) {
		const rightEntry = right?.[roleId];
		if (!rightEntry || !equalModelSelectionEntry(leftEntry, rightEntry)) {
			return false;
		}
	}
	return true;
}

function equalModelSelectionEntry(left: RunModelSelectionEntry, right: RunModelSelectionEntry): boolean {
	return left.model === right.model && left.thinking_effort === right.thinking_effort
    && left.connection?.id === right.connection?.id && left.connection?.revision === right.connection?.revision;
}

function mergeRunSummary(current: RunSummary, run: RunSummary): RunSummary {
	const next = { ...current, ...run };
	return shallowEqualRunSummary(current, next) ? current : next;
}

function appendQueryKey(
	queryKeys: Array<readonly unknown[]>,
	queryKey: readonly unknown[],
): void {
	if (queryKeys.some((candidate) => candidate.length === queryKey.length && candidate.every((value, index) => value === queryKey[index]))) {
		return;
	}
	queryKeys.push(queryKey);
}

function readBoolean(value: unknown): boolean | undefined {
	return typeof value === "boolean" ? value : undefined;
}

function readString(value: unknown): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}
