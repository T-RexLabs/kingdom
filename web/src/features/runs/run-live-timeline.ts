import { skipToken, useQuery, type QueryClient } from "@tanstack/react-query";
import { runQueryKeys } from "@/api/hooks-runs";
import type { TimelineEvent } from "@/api/hooks-types";

export const liveTimelineQueryKey = (runId: string) => ["runs", runId, "timeline", "live"] as const;
export const liveTimelineSyncQueryKey = (runId: string) => ["runs", runId, "timeline", "live", "sync"] as const;
export const MAX_LIVE_TIMELINE_EVENTS = 200;
const LIVE_TIMELINE_RETRY_BACKOFF_MS = [1_000, 3_000, 10_000, 30_000] as const;

interface LiveTimelineDirtyRange {
	startSeq: number;
	endSeq: number;
}

export type LiveTimelineReconciliationKind = "refetch-tail" | "fetch-older";

export interface LiveTimelineReconciliationAttempt {
  kind: LiveTimelineReconciliationKind;
  dirtyStartSeq: number;
  dirtyEndSeq: number;
  baseOldestSeq: number;
  baseLatestSeq: number;
}

export interface LiveTimelineSyncState {
	dirtyRanges: LiveTimelineDirtyRange[];
  dirtyStartSeq: number | null;
  dirtyEndSeq: number | null;
  reconciliationAttempt: LiveTimelineReconciliationAttempt | null;
  reconciliationRetryCount: number;
  nextRetryAtMs: number | null;
}

const initialLiveTimelineSyncState = (): LiveTimelineSyncState => ({
	dirtyRanges: [],
  dirtyStartSeq: null,
  dirtyEndSeq: null,
  reconciliationAttempt: null,
  reconciliationRetryCount: 0,
  nextRetryAtMs: null,
});

export function useLiveTimelineEvents(runId: string) {
  return useQuery({
    queryKey: liveTimelineQueryKey(runId),
    queryFn: skipToken,
    enabled: false,
    initialData: [] as TimelineEvent[],
    staleTime: Number.POSITIVE_INFINITY,
  });
}

export function useLiveTimelineSyncState(runId: string) {
  return useQuery({
    queryKey: liveTimelineSyncQueryKey(runId),
    queryFn: skipToken,
    enabled: false,
    initialData: initialLiveTimelineSyncState,
    staleTime: Number.POSITIVE_INFINITY,
  });
}

export function appendLiveTimelineEvent(
  qc: QueryClient,
  runId: string,
  event: TimelineEvent,
  markDirty = false,
): void {
  let trimmedRange: { startSeq: number; endSeq: number } | null = null;
  qc.setQueryData<TimelineEvent[]>(liveTimelineQueryKey(runId), (current) => {
    const items = current ?? [];
	    if (items.some((item) => item.event_id === event.event_id)) {
	      return items;
	    }
	    if (items.length > 0 && event.run_event_seq <= (items[items.length - 1]?.run_event_seq ?? 0)) {
	      return items;
	    }
	    if (items[items.length - 1]?.run_event_seq === event.run_event_seq) {
      return items;
    }

    const nextItems = [...items, event];
    if (nextItems.length <= MAX_LIVE_TIMELINE_EVENTS) {
      return nextItems;
    }

    const overflowCount = nextItems.length - MAX_LIVE_TIMELINE_EVENTS;
    const droppedItems = nextItems.slice(0, overflowCount);
    if (droppedItems.length > 0) {
      trimmedRange = {
        startSeq: droppedItems[0].run_event_seq,
        endSeq: droppedItems[droppedItems.length - 1].run_event_seq,
      };
    }
    return nextItems.slice(overflowCount);
  });

  if (!markDirty && trimmedRange === null) {
    return;
  }

  qc.setQueryData<LiveTimelineSyncState>(liveTimelineSyncQueryKey(runId), (current) => {
    const state = normalizeLiveTimelineSyncState(current);
    let nextState = state;
    if (markDirty) {
      nextState = extendDirtyRange(nextState, event.run_event_seq, event.run_event_seq);
    }
    if (trimmedRange !== null) {
      nextState = extendDirtyRange(nextState, trimmedRange.startSeq, trimmedRange.endSeq);
    }
    return nextState;
  });
}

export function reconcileLiveTimelineEvents(
  qc: QueryClient,
  runId: string,
  oldestCanonicalSeq: number,
  latestCanonicalSeq: number,
): void {
  if (oldestCanonicalSeq < 1 || latestCanonicalSeq < oldestCanonicalSeq) {
    return;
  }

  qc.setQueryData<TimelineEvent[]>(liveTimelineQueryKey(runId), (current) => {
    const items = current ?? [];
    const nextItems = items.filter(
      (item) => item.run_event_seq < oldestCanonicalSeq || item.run_event_seq > latestCanonicalSeq,
    );
    return nextItems.length === items.length ? items : nextItems;
  });

  qc.setQueryData<LiveTimelineSyncState>(liveTimelineSyncQueryKey(runId), (current) => {
    const state = normalizeLiveTimelineSyncState(current);
    if (state.dirtyRanges.length === 0) {
      return state;
    }

		const nextDirtyRanges = subtractDirtyRanges(state.dirtyRanges, {
			startSeq: oldestCanonicalSeq,
			endSeq: latestCanonicalSeq,
		});
		if (areDirtyRangesEqual(nextDirtyRanges, state.dirtyRanges)) {
      return state;
    }
		return applyDirtyRanges(state, nextDirtyRanges);
  });
}

export function startLiveTimelineReconciliationAttempt(
  qc: QueryClient,
  runId: string,
  attempt: LiveTimelineReconciliationAttempt,
): boolean {
  let started = false;
  qc.setQueryData<LiveTimelineSyncState>(liveTimelineSyncQueryKey(runId), (current) => {
    const state = normalizeLiveTimelineSyncState(current);
    if (state.reconciliationAttempt !== null) {
      return state;
    }
    started = true;
    return {
      ...state,
      reconciliationAttempt: attempt,
      nextRetryAtMs: null,
    };
  });
  return started;
}

export function settleLiveTimelineReconciliationAttempt(
  qc: QueryClient,
  runId: string,
  success: boolean,
  coverage: { oldestSeq: number; latestSeq: number },
  nowMs = Date.now(),
): void {
  qc.setQueryData<LiveTimelineSyncState>(liveTimelineSyncQueryKey(runId), (current) => {
    const state = normalizeLiveTimelineSyncState(current);
    const attempt = state.reconciliationAttempt;
    if (attempt === null) {
      return state;
    }
    if (state.dirtyStartSeq === null || state.dirtyEndSeq === null) {
      return initialLiveTimelineSyncState();
    }
    if (success && didTimelineReconciliationProgress(attempt, coverage)) {
      return resetLiveTimelineReconciliation(state);
    }

    const retryCount = state.reconciliationRetryCount + 1;
    return {
      ...state,
      reconciliationAttempt: null,
      reconciliationRetryCount: retryCount,
      nextRetryAtMs: nowMs + reconciliationRetryDelayMs(retryCount),
    };
  });
}

export function readCanonicalTimelineCoverage(
  qc: QueryClient,
  runId: string,
  limit: number,
): { oldestSeq: number; latestSeq: number } {
  const data = qc.getQueryData<{ pages?: Array<{ items?: TimelineEvent[] }> }>(runQueryKeys.timeline(runId, limit));
  const baseItems = [...(data?.pages ?? [])].reverse().flatMap((page) => page.items ?? []);
  return {
    oldestSeq: baseItems[0]?.run_event_seq ?? 0,
    latestSeq: baseItems[baseItems.length - 1]?.run_event_seq ?? 0,
  };
}

export function mergeTimelineEvents(base: TimelineEvent[], live: TimelineEvent[]): TimelineEvent[] {
  const merged: TimelineEvent[] = [];
  const seen = new Set<string>();
  let baseIndex = 0;
  let liveIndex = 0;

  while (baseIndex < base.length || liveIndex < live.length) {
    const nextBase = base[baseIndex];
    const nextLive = live[liveIndex];
    let nextItem: TimelineEvent | undefined;

    if (!nextLive || (nextBase && compareTimelineEventOrder(nextBase, nextLive) <= 0)) {
      nextItem = nextBase;
      baseIndex += 1;
    } else {
      nextItem = nextLive;
      liveIndex += 1;
    }

    if (!nextItem || seen.has(nextItem.event_id)) {
      continue;
    }
    seen.add(nextItem.event_id);
    merged.push(nextItem);
  }

  return merged;
}

function extendDirtyRange(
  state: LiveTimelineSyncState,
  startSeq: number,
  endSeq: number,
): LiveTimelineSyncState {
	const nextDirtyRanges = mergeDirtyRanges(state.dirtyRanges, { startSeq, endSeq });
	if (areDirtyRangesEqual(nextDirtyRanges, state.dirtyRanges)) {
		return state;
	}
	return applyDirtyRanges(state, nextDirtyRanges);
}

function normalizeLiveTimelineSyncState(
  current: Partial<LiveTimelineSyncState> | undefined,
): LiveTimelineSyncState {
	const dirtyRanges = normalizeDirtyRanges(current);
	const dirtyBounds = dirtyRangesToBounds(dirtyRanges);
  return {
		dirtyRanges,
		dirtyStartSeq: dirtyBounds.startSeq,
		dirtyEndSeq: dirtyBounds.endSeq,
		reconciliationAttempt: current?.reconciliationAttempt ?? null,
		reconciliationRetryCount: current?.reconciliationRetryCount ?? 0,
		nextRetryAtMs: current?.nextRetryAtMs ?? null,
  };
}

function resetLiveTimelineReconciliation(state: LiveTimelineSyncState): LiveTimelineSyncState {
  return {
    ...state,
    reconciliationAttempt: null,
    reconciliationRetryCount: 0,
    nextRetryAtMs: null,
  };
}

function didTimelineReconciliationProgress(
  attempt: LiveTimelineReconciliationAttempt,
  coverage: { oldestSeq: number; latestSeq: number },
): boolean {
  if (attempt.kind === "refetch-tail") {
    return coverage.latestSeq > attempt.baseLatestSeq;
  }
  return coverage.oldestSeq > 0 && coverage.oldestSeq < attempt.baseOldestSeq;
}

function reconciliationRetryDelayMs(retryCount: number): number {
  const index = Math.min(Math.max(retryCount - 1, 0), LIVE_TIMELINE_RETRY_BACKOFF_MS.length - 1);
  return LIVE_TIMELINE_RETRY_BACKOFF_MS[index] ?? LIVE_TIMELINE_RETRY_BACKOFF_MS[LIVE_TIMELINE_RETRY_BACKOFF_MS.length - 1];
}

function compareTimelineEventOrder(left: TimelineEvent, right: TimelineEvent): number {
  if (left.run_event_seq !== right.run_event_seq) {
    return left.run_event_seq - right.run_event_seq;
  }
  if (left.attempt_seq !== right.attempt_seq) {
    return left.attempt_seq - right.attempt_seq;
  }
	return left.event_id.localeCompare(right.event_id);
}

function applyDirtyRanges(
	state: LiveTimelineSyncState,
	dirtyRanges: LiveTimelineDirtyRange[],
): LiveTimelineSyncState {
	if (dirtyRanges.length === 0) {
		return initialLiveTimelineSyncState();
	}
	const dirtyBounds = dirtyRangesToBounds(dirtyRanges);
	return {
		...resetLiveTimelineReconciliation(state),
		dirtyRanges,
		dirtyStartSeq: dirtyBounds.startSeq,
		dirtyEndSeq: dirtyBounds.endSeq,
	};
}

function normalizeDirtyRanges(current: Partial<LiveTimelineSyncState> | undefined): LiveTimelineDirtyRange[] {
	const rangeCandidates = Array.isArray(current?.dirtyRanges) ? current.dirtyRanges : [];
	const normalizedRanges = mergeDirtyRanges([], ...rangeCandidates);
	if (normalizedRanges.length > 0 || rangeCandidates.length > 0) {
		return normalizedRanges;
	}
	return buildDirtyRangesFromBounds(current?.dirtyStartSeq ?? null, current?.dirtyEndSeq ?? null);
}

function buildDirtyRangesFromBounds(
	dirtyStartSeq: number | null,
	dirtyEndSeq: number | null,
): LiveTimelineDirtyRange[] {
	if (dirtyStartSeq === null || dirtyEndSeq === null || dirtyStartSeq < 1 || dirtyEndSeq < dirtyStartSeq) {
		return [];
	}
	return [{ startSeq: dirtyStartSeq, endSeq: dirtyEndSeq }];
}

function dirtyRangesToBounds(dirtyRanges: LiveTimelineDirtyRange[]): {
	startSeq: number | null;
	endSeq: number | null;
} {
	if (dirtyRanges.length === 0) {
		return { startSeq: null, endSeq: null };
	}
	return {
		startSeq: dirtyRanges[0]?.startSeq ?? null,
		endSeq: dirtyRanges[dirtyRanges.length - 1]?.endSeq ?? null,
	};
}

function mergeDirtyRanges(
	existingRanges: LiveTimelineDirtyRange[],
	...nextRanges: LiveTimelineDirtyRange[]
): LiveTimelineDirtyRange[] {
	const candidates = [...existingRanges, ...nextRanges]
		.filter((range) => Number.isInteger(range.startSeq) && Number.isInteger(range.endSeq) && range.startSeq >= 1 && range.endSeq >= range.startSeq)
		.sort((left, right) => left.startSeq - right.startSeq || left.endSeq - right.endSeq);
	if (candidates.length === 0) {
		return [];
	}

	const merged: LiveTimelineDirtyRange[] = [];
	for (const range of candidates) {
		const previous = merged[merged.length - 1];
		if (!previous || range.startSeq > previous.endSeq + 1) {
			merged.push({ ...range });
			continue;
		}
		previous.endSeq = Math.max(previous.endSeq, range.endSeq);
	}
	return merged;
}

function subtractDirtyRanges(
	dirtyRanges: LiveTimelineDirtyRange[],
	coveredRange: LiveTimelineDirtyRange,
): LiveTimelineDirtyRange[] {
	const remaining: LiveTimelineDirtyRange[] = [];
	for (const range of dirtyRanges) {
		if (coveredRange.endSeq < range.startSeq || coveredRange.startSeq > range.endSeq) {
			remaining.push(range);
			continue;
		}
		if (coveredRange.startSeq > range.startSeq) {
			remaining.push({ startSeq: range.startSeq, endSeq: coveredRange.startSeq - 1 });
		}
		if (coveredRange.endSeq < range.endSeq) {
			remaining.push({ startSeq: coveredRange.endSeq + 1, endSeq: range.endSeq });
		}
	}
	return remaining;
}

function areDirtyRangesEqual(left: LiveTimelineDirtyRange[], right: LiveTimelineDirtyRange[]): boolean {
	if (left.length !== right.length) {
		return false;
	}
	return left.every((range, index) => range.startSeq === right[index]?.startSeq && range.endSeq === right[index]?.endSeq);
}
