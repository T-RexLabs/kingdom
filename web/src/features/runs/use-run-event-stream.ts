import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { API_BASE, buildAuthHeaders } from "@/api/client";
import {
	invalidateRunDetailRecoveryQueries,
	invalidateRunQuerySurfaces,
	type RunQuerySurface,
} from "@/api/hooks-runs";
import type { RunDetailSummary, RunExecution, TimelineEvent } from "@/api/hooks-types";
import { subscribeSSE, type ParsedSSEEvent, type SSEConnectionState } from "@/lib/sse-stream";
import { appendLiveTimelineEvent } from "./run-live-timeline";
import { appendLiveRunActivity } from "./run-live-activity";
import { parseActivityEvent } from "@/features/activity/activity-view";
import { liveExecutionLabel, patchLiveExecutionProjection } from "./run-live-execution-projection";
import {
	applyRunStreamHighWaterMark,
	clearRunStreamResumeToken,
	patchRunDetailFields,
	readRunStreamBootstrapState,
	readRunStartProjection,
	writeRunStreamResumeCursor,
} from "./run-cache-updates";
import { markRunQuerySurfacesDirty } from "./run-surface-freshness";

export const RUN_STREAM_RECONNECT_DELAY_MS = 3_000;
const FATAL_RUN_STREAM_ERRORS = new Set(["HTTP 404", "HTTP 410"]);
const RUN_STREAM_EXECUTION_IMPACT_SURFACES: readonly RunQuerySurface[] = ["detail", "graph", "executions"];
const RUN_STREAM_REOPEN_IMPACT_SURFACES: readonly RunQuerySurface[] = [
	"detail",
	"plan",
	"governanceSummary",
	"governanceHistory",
	"graph",
	"executions",
];
const RUN_STREAM_TIMELINE_ONLY_EVENT_TYPES = new Set([
	"attempt.progress",
	"attempt.sequence_gap",
	"attempt.timeout_warning",
	"attempt.recovery_blocked",
]);
// Fine-grained provider activity is observability-only: it is routed into the
// per-attempt live activity buffer and must never enter the coarse timeline or
// trigger surface refetches.
const RUN_ACTIVITY_EVENT_TYPE = "attempt.activity";

export type RunStreamConnectionState = SSEConnectionState["state"];

export function useRunEventStream(runId: string, enabled = true, markTimelineDirty = false, hiddenSurfaces: readonly RunQuerySurface[] = []): RunStreamConnectionState {
  const queryClient = useQueryClient();
  const lastEventIdRef = useRef<string | null>(null);
  const markTimelineDirtyRef = useRef(markTimelineDirty);
  const hiddenSurfacesRef = useRef(hiddenSurfaces);
  const previousRunIdRef = useRef<string>("");
  const [restartNonce, setRestartNonce] = useState(0);
  const [streamConnectionState, setStreamConnectionState] = useState<RunStreamConnectionState>(enabled && runId ? "connecting" : "idle");
	const connectionState: RunStreamConnectionState = !runId || !enabled
		? "idle"
		: (streamConnectionState === "idle" ? "connecting" : streamConnectionState);

	useEffect(() => {
		markTimelineDirtyRef.current = markTimelineDirty;
		hiddenSurfacesRef.current = hiddenSurfaces;
	}, [hiddenSurfaces, markTimelineDirty]);

	useEffect(() => {
		if (!runId) {
			lastEventIdRef.current = null;
			previousRunIdRef.current = "";
			return;
		}
		if (previousRunIdRef.current !== runId) {
			previousRunIdRef.current = runId;
			lastEventIdRef.current = readRunStreamBootstrapState(queryClient, runId).lastEventId;
		}
		if (!enabled) {
			return;
    }

    const controller = new AbortController();
    let restartTimer: number | null = null;
    let restartScheduled = false;
    const streamUrl = buildRunStreamUrl(runId);

    void subscribeSSE({
      url: streamUrl,
			buildHeaders: () =>
				buildAuthHeaders(
					lastEventIdRef.current ? { "Last-Event-ID": lastEventIdRef.current } : undefined,
				),
			isTerminalEventType: () => false,
			signal: controller.signal,
			onConnectionChange: (state) => {
				setStreamConnectionState(state.state);
				if (
					state.state === "failed"
					&& shouldResetRunStreamResumeCursor(state.lastError, lastEventIdRef.current)
				) {
					clearRunStreamResumeToken(queryClient, runId);
					lastEventIdRef.current = null;
					invalidateRunDetailRecoveryQueries(queryClient, runId);
					setRestartNonce((current) => current + 1);
					return;
				}
				if (
					state.state !== "failed" ||
					controller.signal.aborted ||
					restartScheduled ||
          isFatalRunStreamFailure(state.lastError)
        ) {
          return;
        }
        restartScheduled = true;
        restartTimer = window.setTimeout(() => {
          if (!controller.signal.aborted) {
            setRestartNonce((current) => current + 1);
          }
        }, RUN_STREAM_RECONNECT_DELAY_MS);
			},
			onEvent: async (event) => {
				const applied = applyRunStreamEvent(queryClient, runId, event, markTimelineDirtyRef.current, hiddenSurfacesRef.current);
				if (applied && event.id) {
					lastEventIdRef.current = event.id;
					writeRunStreamResumeCursor(queryClient, runId, event.id);
				}
				return applied;
			},
		}).catch((err: unknown) => {
      if (controller.signal.aborted) {
        return;
      }
      console.error("run SSE subscription failed", err);
    });

    return () => {
      controller.abort();
      if (restartTimer !== null) {
        window.clearTimeout(restartTimer);
      }
    };
  }, [enabled, queryClient, restartNonce, runId]);

  return connectionState;
}

function isFatalRunStreamFailure(lastError: string | undefined): boolean {
  if (!lastError) {
    return false;
  }
	return FATAL_RUN_STREAM_ERRORS.has(lastError.trim().toUpperCase());
}

function shouldResetRunStreamResumeCursor(lastError: string | undefined, currentCursor: string | null): boolean {
	if (!currentCursor) {
		return false;
	}
	return lastError?.trim().toUpperCase() === "HTTP 400";
}

export function applyRunStreamEvent(
  queryClient: ReturnType<typeof useQueryClient>,
  runId: string,
  event: ParsedSSEEvent,
  markTimelineDirty = false,
  hiddenSurfaces: readonly RunQuerySurface[] = [],
): boolean {
  let data: Record<string, unknown>;
  try {
    data = JSON.parse(event.data) as Record<string, unknown>;
  } catch {
    return false;
  }

	const eventType = normalizeEventType(event.type, data);
	const normalized = normalizeRunStreamData(data, eventType);

	// Route agent activity into its own buffer before any timeline / high-water-mark
	// / surface-invalidation machinery so it stays isolated from the coarse stream.
	if (eventType === RUN_ACTIVITY_EVENT_TYPE) {
		routeLiveRunActivity(queryClient, runId, normalized);
		return true;
	}

	const accepted = applyRunStreamHighWaterMark(queryClient, runId, normalized.runEventSeq);
	if (!accepted) {
		return true;
	}

	const timelineEvent = buildTimelineEvent(normalized);
	if (timelineEvent) {
		appendLiveTimelineEvent(queryClient, runId, timelineEvent, markTimelineDirty);
	}

	applyRunStreamSideEffects(queryClient, runId, normalized, hiddenSurfaces);
  return true;
}

function buildRunStreamUrl(runId: string): string {
  const params = new URLSearchParams({
    run_id: runId,
    follow: "true",
    poll_ms: "2000",
  });
  return `${API_BASE}/events/stream?${params.toString()}`;
}

function normalizeEventType(
  eventType: string,
  data: Record<string, unknown>,
): string {
  const payloadType = data.event_type;
  if (typeof payloadType === "string" && payloadType.trim() !== "") {
    return payloadType;
  }
  return eventType || "message";
}

function applyRunStreamSideEffects(
  queryClient: ReturnType<typeof useQueryClient>,
  runId: string,
  event: NormalizedRunStreamData,
  hiddenSurfaces: readonly RunQuerySurface[],
) {
	const hiddenSurfaceSet = new Set(hiddenSurfaces);
	const markHiddenSurfacesDirty = (surfaces: readonly RunQuerySurface[]) => {
		const dirtySurfaces = surfaces.filter((surface) => hiddenSurfaceSet.has(surface));
		if (dirtySurfaces.length > 0) {
			markRunQuerySurfacesDirty(queryClient, runId, dirtySurfaces);
		}
	};
	const invalidateSurfaces = (surfaces: readonly RunQuerySurface[]) => {
		markHiddenSurfacesDirty(surfaces);
		invalidateRunQuerySurfaces(queryClient, runId, surfaces);
	};


			switch (event.eventType) {
				case "run.state_changed":
					patchRunStateChange(queryClient, runId, event);
			invalidateSurfaces(["plan"]);
			return;
			case "approval.recorded":
				patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
				invalidateSurfaces(["detail", "plan", "governanceSummary", "governanceHistory"]);
				return;
				case "node.state_changed":
					patchExecutionLiveProjection(queryClient, runId, event);
					patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
					invalidateSurfaces(RUN_STREAM_EXECUTION_IMPACT_SURFACES);
					return;
			case "mailbox.message_recorded":
        patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
        return;

		case "attempt.started":
		case "attempt.retry_scheduled":
		case "attempt.retry_queued":
		case "attempt.rerun_queued":
		case "attempt.resume_queued":
		case "attempt.final_rework_queued":
			case "attempt.failed":
			case "attempt.succeeded":
			case "attempt.cancelled":
				patchExecutionLiveProjection(queryClient, runId, event);
				patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
				invalidateSurfaces(RUN_STREAM_EXECUTION_IMPACT_SURFACES);
				return;
			case "run.retry_reopened":
			case "run.rerun_reopened":
			case "run.resume_reopened":
				patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
				invalidateSurfaces(RUN_STREAM_REOPEN_IMPACT_SURFACES);
				return;
			case "escalation.raised":
				patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
				invalidateSurfaces(RUN_STREAM_EXECUTION_IMPACT_SURFACES);
				return;
		case "attempt.progress":
			patchExecutionLiveProjection(queryClient, runId, event);
			if (shouldPatchLastActivityFromAttemptProgress(event.reasonCode)) {
				patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
			}
			return;
		case "attempt.sequence_gap":
			case "attempt.timeout_warning":
			case "attempt.recovery_blocked":
				return;
			default:
				patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
				if (isRunMutationEventType(event.eventType) && !RUN_STREAM_TIMELINE_ONLY_EVENT_TYPES.has(event.eventType)) {
					invalidateRunDetailRecoveryQueries(queryClient, runId);
				}
			return;
	}
}

function routeLiveRunActivity(
	queryClient: ReturnType<typeof useQueryClient>,
	runId: string,
	event: NormalizedRunStreamData,
) {
	const attemptId = readString(event.payload.attempt_id);
	if (!attemptId) {
		return;
	}
	const item = parseActivityEvent({
		payload: event.payload,
		eventId: event.eventId ?? "",
		createdAt: event.createdAt,
	});
	if (!item) {
		return;
	}
	appendLiveRunActivity(queryClient, runId, attemptId, item);
	// Keep the run's "last activity" fresh so the live status headline reads as
	// cooking, but do not invalidate any surfaces.
	patchRunLastActivityTimestamp(queryClient, runId, event.createdAt);
}

function patchExecutionLiveProjection(
	queryClient: ReturnType<typeof useQueryClient>,
	runId: string,
	event: NormalizedRunStreamData,
) {
	const nodeRunId = readString(event.payload.node_run_id);
	if (!nodeRunId || !event.createdAt) {
		return;
	}
	if (shouldSkipLiveExecutionProjection(event.eventType, event.reasonCode)) {
		return;
	}
	const executionMode = readExecutionMode(event.payload.execution_mode);
	const nodeState = readString(event.payload.state);
	const liveStatus = executionStatusForEventType(event.eventType, event.reasonCode, nodeState);
	const liveLabel = liveExecutionLabel(event.reasonCode, event.eventType, nodeState);
	patchLiveExecutionProjection(queryClient, runId, {
		node_run_id: nodeRunId,
		activity_at: event.createdAt,
		...(typeof event.runEventSeq === "number" ? { run_event_seq: event.runEventSeq } : {}),
		...(typeof event.attemptSeq === "number" ? { attempt_seq: event.attemptSeq } : {}),
		...(readString(event.payload.attempt_id) ? { attempt_id: readString(event.payload.attempt_id) } : {}),
		...(readString(event.payload.node_id) ? { node_id: readString(event.payload.node_id) } : {}),
		...(typeof readInt(event.payload.attempt) === "number" ? { attempt: readInt(event.payload.attempt) } : {}),
		...(readString(event.payload.agent_id) ? { agent_id: readString(event.payload.agent_id) } : {}),
		...(readString(event.payload.required_role) ? { required_role: readString(event.payload.required_role) } : {}),
		...(executionMode ? { execution_mode: executionMode } : {}),
		...(liveStatus ? { status: liveStatus } : {}),
		...(liveStatus === "starting" ? { launch_started_at: event.createdAt } : {}),
		...(((event.eventType === "attempt.started") || (event.eventType === "node.state_changed" && nodeState === "running"))
			? { started_at: event.createdAt }
			: {}),
		...(event.eventType === "attempt.succeeded" || event.eventType === "attempt.failed" || event.eventType === "attempt.cancelled"
			? { terminal_observed_at: event.createdAt }
			: {}),
		...(liveLabel ? { live_label: liveLabel } : {}),
		...(event.reasonCode ? { live_reason_code: event.reasonCode } : {}),
	});
}

	function executionStatusForEventType(
		eventType: string,
		reasonCode?: string,
		nodeState?: string,
	): RunExecutionStatus | undefined {
		switch (eventType) {
			case "attempt.started":
				return "running";
		case "node.state_changed":
			switch ((nodeState ?? "").trim()) {
				case "starting":
					return "starting";
				case "running":
					return "running";
				case "blocked":
					return "blocked";
				case "succeeded":
					return "succeeded";
				case "failed":
					return "failed";
					case "cancelled":
						return "cancelled";
					default:
						return undefined;
				}
		case "attempt.progress":
			if (isCompletionObservationReasonCode(reasonCode)) {
				return "verifying";
			}
			switch ((reasonCode ?? "").trim()) {
				case "provider_invoked":
					return "running";
				default:
					return undefined;
			}
		case "attempt.retry_queued":
		case "attempt.rerun_queued":
		case "attempt.resume_queued":
		case "attempt.final_rework_queued":
			return "queued";
		case "attempt.succeeded":
			return "succeeded";
		case "attempt.failed":
			return "failed";
		case "attempt.cancelled":
			return "cancelled";
		default:
			return undefined;
	}
}

function shouldSkipLiveExecutionProjection(eventType: string, reasonCode?: string): boolean {
	return eventType === "attempt.progress" && isRuntimeObservationReasonCode(reasonCode);
}

function isRuntimeObservationReasonCode(reasonCode?: string): boolean {
	const normalized = reasonCode?.trim() ?? "";
	return normalized === "runtime_observation";
}

function isCompletionObservationReasonCode(reasonCode?: string): boolean {
	return (reasonCode?.trim() ?? "") === "runtime_completion_observation";
}

type RunExecutionStatus = RunExecution["status"];

interface NormalizedRunStreamData {
	attemptSeq?: number;
  createdAt?: string;
  eventId?: string;
  eventType: string;
  reasonCode?: string;
  runEventSeq?: number;
  payload: Record<string, unknown>;
}

function normalizeRunStreamData(
  data: Record<string, unknown>,
  eventType: string,
): NormalizedRunStreamData {
  return {
	attemptSeq: readInt(data.attempt_seq),
    createdAt: readString(data.created_at),
    eventId: readString(data.event_id),
    eventType,
    reasonCode: readString(data.reason_code),
    runEventSeq: readInt(data.run_event_seq),
    payload: stripTimelineMetadata(data),
  };
}

function buildTimelineEvent(event: NormalizedRunStreamData): TimelineEvent | null {
	if (!event.eventId || !event.runEventSeq || !event.attemptSeq || !event.eventType || !event.createdAt) {
    return null;
  }
  return {
    event_id: event.eventId,
	    run_event_seq: event.runEventSeq,
	    attempt_seq: event.attemptSeq,
    event_type: event.eventType,
    emitted_at: event.createdAt,
    payload_json: event.payload,
    ...(event.reasonCode ? { reason_code: event.reasonCode } : {}),
  };
}

function patchRunStateChange(
  queryClient: ReturnType<typeof useQueryClient>,
  runId: string,
  event: NormalizedRunStreamData,
) {
  const detailFields: Partial<RunDetailSummary> = {};
	Object.assign(detailFields, readRunStartProjection(event.payload));

	for (const [payloadKey, detailKey] of [["status", "status"], ["display_state", "display_state"], ["plan_state", "plan_state"]] as const) {
		const value = readString(event.payload[payloadKey]);
		if (!value) continue;
		(detailFields as Record<string, unknown>)[detailKey] = value;
	}
	if (event.createdAt) {
		detailFields.updated_at = event.createdAt;
	}
	const lastActivityAt = readString(event.payload.last_activity_at) ?? event.createdAt;
	if (lastActivityAt) {
		detailFields.last_activity_at = lastActivityAt;
	}

	patchRunDetailFields(queryClient, runId, detailFields);
}

function patchRunLastActivityTimestamp(
  queryClient: ReturnType<typeof useQueryClient>,
  runId: string,
  createdAt?: string,
) {
  if (!createdAt) {
    return;
  }
  patchRunDetailFields(queryClient, runId, { last_activity_at: createdAt } as Partial<RunDetailSummary>);
}

function shouldPatchLastActivityFromAttemptProgress(reasonCode?: string): boolean {
	const normalized = reasonCode?.trim() ?? "";
	return normalized !== "runtime_observation";
}

function stripTimelineMetadata(data: Record<string, unknown>): Record<string, unknown> {
  const payload = { ...data };
  delete payload.created_at;
  delete payload.event_id;
  delete payload.event_type;
  delete payload.reason_code;
	delete payload.attempt_seq;
  delete payload.run_event_seq;
  delete payload.seq;
  return payload;
}

function readInt(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) ? value : undefined;
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function readExecutionMode(value: unknown): "provider_backed" | "runtime_owned" | undefined {
	return value === "provider_backed" || value === "runtime_owned" ? value : undefined;
}

function isRunMutationEventType(eventType: string): boolean {
	return eventType.startsWith("run.")
		|| eventType.startsWith("node.")
		|| eventType.startsWith("attempt.")
		|| eventType.startsWith("approval.")
		|| eventType.startsWith("mailbox.")
		|| eventType.startsWith("escalation.");
}
