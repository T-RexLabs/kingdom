import { useEffect, useRef, useState } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { API_BASE, buildAuthHeaders } from "@/api/client";
import { subscribeSSE, type ParsedSSEEvent } from "@/lib/sse-stream";
import {
	activeStreamOwnedRunCollectionQueryKeys,
	buildRunSummaryFromCollectionEvent,
	clearRunCollectionStreamResumeCursor,
	inactiveStreamOwnedRunCollectionQueryKeys,
	readRunCollectionStreamResumeCursor,
	upsertRunCollections,
	writeRunCollectionStreamResumeCursor,
} from "./run-cache-updates";

export const RUN_COLLECTION_STREAM_RECONNECT_DELAY_MS = 3_000;
export const RUN_COLLECTION_INVALIDATION_COALESCE_MS = 250;
const FATAL_RUN_COLLECTION_STREAM_ERRORS = new Set(["HTTP 404", "HTTP 410"]);

interface PendingCollectionInvalidations {
	queryKeys: Map<string, readonly unknown[]>;
	timer: number | null;
}

const pendingCollectionInvalidations = new WeakMap<QueryClient, PendingCollectionInvalidations>();

export function useRunsCollectionEventStream(enabled = true) {
  const queryClient = useQueryClient();
  const lastEventIdRef = useRef<string | null>(null);
  const [restartNonce, setRestartNonce] = useState(0);

  useEffect(() => {
    if (!enabled) {
      return;
    }
    if (lastEventIdRef.current === null) {
      lastEventIdRef.current = readRunCollectionStreamResumeCursor(queryClient);
    }

    const controller = new AbortController();
    let restartTimer: number | null = null;
    let restartScheduled = false;

    void subscribeSSE({
      url: buildRunCollectionStreamUrl(),
      buildHeaders: () =>
        buildAuthHeaders(
          lastEventIdRef.current ? { "Last-Event-ID": lastEventIdRef.current } : undefined,
        ),
      isTerminalEventType: () => false,
      signal: controller.signal,
      onConnectionChange: (state) => {
			if (
				state.state === "failed"
				&& shouldResetRunCollectionResumeCursor(state.lastError, lastEventIdRef.current)
			) {
				clearRunCollectionStreamResumeCursor(queryClient);
				lastEventIdRef.current = null;
				void invalidateRunCollections(queryClient);
				setRestartNonce((current) => current + 1);
				return;
			}
        if (
          state.state !== "failed" ||
          controller.signal.aborted ||
          restartScheduled ||
          isFatalRunCollectionStreamFailure(state.lastError)
        ) {
          return;
        }
        restartScheduled = true;
        restartTimer = window.setTimeout(() => {
          if (!controller.signal.aborted) {
            setRestartNonce((current) => current + 1);
          }
        }, RUN_COLLECTION_STREAM_RECONNECT_DELAY_MS);
      },
      onEvent: async (event) => {
        const applied = await applyRunsCollectionStreamEvent(queryClient, event);
        if (applied && event.id) {
          lastEventIdRef.current = event.id;
			writeRunCollectionStreamResumeCursor(queryClient, event.id);
        }
        return applied;
      },
    }).catch((err: unknown) => {
      if (controller.signal.aborted) {
        return;
      }
      console.error("run collection SSE subscription failed", err);
    });

    return () => {
      controller.abort();
      cancelScheduledCollectionInvalidations(queryClient);
      if (restartTimer !== null) {
        window.clearTimeout(restartTimer);
      }
    };
  }, [enabled, queryClient, restartNonce]);
}

export function RunsCollectionEventStreamMount() {
  useRunsCollectionEventStream();
  return null;
}

export async function applyRunsCollectionStreamEvent(
  queryClient: ReturnType<typeof useQueryClient>,
  event: ParsedSSEEvent,
): Promise<boolean> {
  let data: Record<string, unknown>;
  try {
    data = JSON.parse(event.data) as Record<string, unknown>;
  } catch {
    return false;
  }

	const eventType = readCollectionEventType(event, data);
	if (!isSupportedRunCollectionEventType(eventType)) {
		await invalidateRunCollections(queryClient);
		return true;
	}

	const runId = readString(data.run_id);
	if (!runId) {
		return false;
	}

	const summary = buildRunSummaryFromCollectionEvent(data);
	if (!summary) {
		await invalidateRunCollections(queryClient);
		return true;
	}

	for (const queryKey of upsertRunCollections(queryClient, summary)) {
		scheduleCollectionInvalidation(queryClient, queryKey);
	}
	await invalidateInactiveRunCollections(queryClient);
	return true;
}

async function invalidateRunCollections(queryClient: QueryClient): Promise<void> {
	await Promise.all([
		invalidateTrackedCollectionQueries(queryClient, activeStreamOwnedRunCollectionQueryKeys(queryClient), "active"),
		invalidateTrackedCollectionQueries(queryClient, inactiveStreamOwnedRunCollectionQueryKeys(queryClient), "none"),
	]);
}

async function invalidateInactiveRunCollections(queryClient: QueryClient): Promise<void> {
	await invalidateTrackedCollectionQueries(queryClient, inactiveStreamOwnedRunCollectionQueryKeys(queryClient), "none");
}

function scheduleCollectionInvalidation(queryClient: QueryClient, queryKey: readonly unknown[]): void {
	let pending = pendingCollectionInvalidations.get(queryClient);
	if (!pending) {
		pending = { queryKeys: new Map(), timer: null };
		pendingCollectionInvalidations.set(queryClient, pending);
	}

	pending.queryKeys.set(JSON.stringify(queryKey), queryKey);
	if (pending.timer !== null) {
		return;
	}

	pending.timer = window.setTimeout(() => {
		const queryKeys = Array.from(pending.queryKeys.values());
		pending.queryKeys.clear();
		pending.timer = null;
		for (const scheduledQueryKey of queryKeys) {
			const trackedQuery = queryClient.getQueryCache().find({ queryKey: scheduledQueryKey, exact: true });
			if (!trackedQuery?.isActive()) {
				continue;
			}
			void queryClient.invalidateQueries({ queryKey: scheduledQueryKey, exact: true, refetchType: "active" });
		}
	}, RUN_COLLECTION_INVALIDATION_COALESCE_MS);
}

async function invalidateTrackedCollectionQueries(
	queryClient: QueryClient,
	queryKeys: readonly (readonly unknown[])[],
	refetchType: "active" | "none",
): Promise<void> {
	for (const queryKey of queryKeys) {
		await queryClient.invalidateQueries({ queryKey, exact: true, refetchType });
	}
}

function cancelScheduledCollectionInvalidations(queryClient: QueryClient): void {
	const pending = pendingCollectionInvalidations.get(queryClient);
	if (!pending) {
		return;
	}
	if (pending.timer !== null) {
		window.clearTimeout(pending.timer);
	}
	pending.queryKeys.clear();
	pending.timer = null;
}

function buildRunCollectionStreamUrl(): string {
  const params = new URLSearchParams({
    follow: "true",
    poll_ms: "2000",
  });
  return `${API_BASE}/runs/events/stream?${params.toString()}`;
}

function isFatalRunCollectionStreamFailure(lastError: string | undefined): boolean {
  if (!lastError) {
    return false;
  }
  return FATAL_RUN_COLLECTION_STREAM_ERRORS.has(lastError.trim().toUpperCase());
}

function shouldResetRunCollectionResumeCursor(lastError: string | undefined, currentCursor: string | null): boolean {
	if (!currentCursor) {
		return false;
	}
	return lastError?.trim().toUpperCase() === "HTTP 400";
}

function readCollectionEventType(event: ParsedSSEEvent, data: Record<string, unknown>): string {
	const payloadType = readString(data.event_type);
	if (payloadType) {
		return payloadType;
	}
	return typeof event.type === "string" ? event.type.trim() : "";
}

function isSupportedRunCollectionEventType(eventType: string): boolean {
	return eventType === "approval.recorded" || eventType.startsWith("run.");
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}
