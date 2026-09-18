import { useEffect, useReducer, useRef, type Dispatch } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { API_BASE, buildAuthHeaders } from "@/api/client";
import type { AcpRun, AcpRunEvent, PlanningMessage, PlanningSessionDetail } from "@/api/hooks";
import { subscribeSSE } from "@/lib/sse-stream";
import {
  buildPlannerOperationState,
  reducePlannerOperationEvent,
  type PlannerOperationState,
} from "./planner-operation-controller";
import {
  replaceAssistantMessageAtTurn,
  replaceAssistantMessageInLatestPlanningPage,
} from "./planner-message-pages";
import { applyGeneratedPlanningSessionTitleIfPlaceholder, applyPlanningSessionFields } from "./planner-session-cache";
import { plannerKeys } from "./planner-query-keys";

export type PlannerConnection =
  | { state: "idle" }
  | { state: "connecting"; attempt: number }
  | { state: "open"; attempt: number }
  | { state: "retrying"; attempt: number; nextDelayMs: number; lastError: string }
  | { state: "failed"; attempt: number; lastError: string };

export interface UsePlannerOperationResult {
  operation: PlannerOperationState | null;
  connection: PlannerConnection;
}

type OperationStreamState = {
  operation: PlannerOperationState | null;
  connection: PlannerConnection;
};

type OperationStreamAction =
  | { type: "reset" }
  | { type: "hydrate"; operation: AcpRun }
  | { type: "terminal"; operation: AcpRun }
  | { type: "event"; event: AcpRunEvent }
  | { type: "connection"; connection: PlannerConnection };

const initialState: OperationStreamState = {
  operation: null,
  connection: { state: "idle" },
};

function reducer(state: OperationStreamState, action: OperationStreamAction): OperationStreamState {
  switch (action.type) {
    case "reset":
      return initialState;
    case "hydrate":
      return {
        operation: buildPlannerOperationState(action.operation),
        connection: state.connection,
      };
    case "terminal": {
      if (!state.operation || state.operation.operationId !== action.operation.run_id) {
        return {
          operation: buildPlannerOperationState(action.operation),
          connection: { state: "idle" },
        };
      }
      const terminal = buildPlannerOperationState(action.operation);
      return {
        operation: {
          ...state.operation,
          status: terminal.status,
          error: terminal.error,
          replyText: terminal.replyText || state.operation.replyText,
          formalizeText: terminal.formalizeText || state.operation.formalizeText,
        },
        connection: state.connection,
      };
    }
    case "event":
      if (!state.operation || state.operation.operationId !== action.event.run_id) {
        return state;
      }
      return {
        operation: reducePlannerOperationEvent(state.operation, action.event),
        connection: state.connection,
      };
    case "connection":
      return { ...state, connection: action.connection };
    default:
      return state;
  }
}

export function usePlannerOperation(operation?: AcpRun | null): UsePlannerOperationResult {
  const [state, dispatch] = useReducer(reducer, initialState);
  const ownerRef = useRef<{
    runId: string;
    controller: AbortController;
    active: boolean;
  } | null>(null);
  const operationRef = useRef<AcpRun | null>(operation ?? null);
  const queryClient = useQueryClient();
  const runId = operation?.run_id;
  const operationStatus = operation?.status;
  const operationOutputText = operation?.output_text;
  const operationError = operation?.error;

  useEffect(() => {
    operationRef.current = operation ?? null;
  }, [operation]);

  useEffect(() => {
    const initialOperation = operationRef.current;
    if (!initialOperation) {
      ownerRef.current?.controller.abort();
      ownerRef.current = null;
      dispatch({ type: "reset" });
      return;
    }

    dispatch({ type: "hydrate", operation: initialOperation });

    if (initialOperation.status === "failed" || initialOperation.status === "cancelled") {
      reconcileTerminalOperationSnapshot(queryClient, initialOperation);
      dispatch({ type: "connection", connection: { state: "idle" } });
      return;
    }

    const controller = new AbortController();
    const owner = {
      runId: initialOperation.run_id,
      controller,
      active: true,
    };
    ownerRef.current = owner;
    const isCurrentOwner = () => owner.active && ownerRef.current === owner && !controller.signal.aborted;

    void subscribeSSE({
      url: `${API_BASE}/acp/runs/${initialOperation.run_id}/events`,
      buildHeaders: (lastId) => buildAuthHeaders(lastId ? { "Last-Event-ID": lastId } : undefined),
      isTerminalEventType: (type) => type === "run.activity.reconciled" || type === "run.failed" || type === "run.cancelled",
      signal: controller.signal,
      onConnectionChange: (next) => {
        if (!isCurrentOwner()) return;
        dispatch({ type: "connection", connection: next });
      },
      onEvent: (parsed) => {
        if (!isCurrentOwner()) return false;
        const currentOperation = operationRef.current;
        if (!currentOperation || currentOperation.run_id !== owner.runId) return false;
        const applied = handleEventPayload({
          operation: currentOperation,
          eventType: parsed.type,
          data: parsed.data,
          eventId: parsed.id || null,
          dispatch,
          queryClient,
        });
        return applied;
      },
    }).catch((err: unknown) => {
      if (controller.signal.aborted) return;
      console.error("planner SSE subscription failed", err);
    });

    return () => {
      owner.active = false;
      controller.abort();
      if (ownerRef.current === owner) {
        ownerRef.current = null;
      }
    };
  }, [queryClient, runId]);

  useEffect(() => {
    const terminalOperation = operationRef.current;
    if (!terminalOperation || (operationStatus !== "completed" && operationStatus !== "failed" && operationStatus !== "cancelled")) {
      return;
    }
    const owner = ownerRef.current;
    if ((operationStatus === "failed" || operationStatus === "cancelled") && owner?.runId === terminalOperation.run_id) {
      owner.active = false;
      owner.controller.abort();
      ownerRef.current = null;
    }
    dispatch({ type: "terminal", operation: terminalOperation });
    reconcileTerminalOperationSnapshot(queryClient, terminalOperation);
  }, [operationError, operationOutputText, operationStatus, queryClient, runId]);

  return state;
}

export function reconcileTerminalOperationSnapshot(
  queryClient: ReturnType<typeof useQueryClient>,
  operation: AcpRun,
) {
  if (operation.status !== "completed" && operation.status !== "failed" && operation.status !== "cancelled") {
    return;
  }

  const fields: Partial<PlanningSessionDetail> =
    operation.agent_name === "planner-generate-title"
      ? { current_title_run_id: undefined }
      : {
          conversation_state: "ready",
          current_run_id: undefined,
        };
  applyPlanningSessionFields(queryClient, operation.planning_session_id, fields);
  void queryClient.invalidateQueries({ queryKey: plannerKeys.detail(operation.planning_session_id), exact: true });
  void queryClient.invalidateQueries({ queryKey: plannerKeys.listPrefix() });
  if (operation.agent_name === "planner-reply" || operation.agent_name === "planner-regenerate") {
    void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(operation.planning_session_id) });
  }
  if (operation.agent_name === "planner-formalize") {
    void queryClient.invalidateQueries({ queryKey: plannerKeys.plan(operation.planning_session_id), exact: true });
  }
}

function handleEventPayload(
  {
    operation,
    eventType,
    data,
    eventId,
    dispatch,
    queryClient,
  }: {
    operation: AcpRun;
    eventType: string;
    data: string;
    eventId: string | null;
    dispatch: Dispatch<OperationStreamAction>;
    queryClient: ReturnType<typeof useQueryClient>;
  },
): boolean {
  if (eventType === "keepalive") return true;
  let payload: Record<string, unknown>;
  try {
    payload = JSON.parse(data) as Record<string, unknown>;
  } catch {
    return false;
  }
  if (!isAcpEventType(eventType)) {
    return true;
  }
  const event = {
    event_id: eventId ?? `${operation.run_id}-${Date.now()}`,
    run_id: operation.run_id,
    seq: parseEventSeq(eventId) ?? 0,
    type: eventType,
    payload,
    created_at: new Date().toISOString(),
  } satisfies AcpRunEvent;
  patchPlanningCaches(queryClient, operation, event);
  patchPlannerTerminalCaches(queryClient, operation, event);
  dispatch({ type: "event", event });
  return true;
}

function isAcpEventType(eventType: string): eventType is AcpRunEvent["type"] {
  return [
    "run.created",
    "run.in-progress",
    "message.part",
    "message.completed",
    "run.completed",
    "run.activity.reconciled",
    "run.failed",
    "run.cancelled",
    "error",
    "generic",
  ].includes(eventType);
}

function parseEventSeq(eventId: string | null): number | null {
  if (!eventId) return null;
  const marker = eventId.lastIndexOf(":");
  if (marker < 0) return null;
  const parsed = Number.parseInt(eventId.slice(marker + 1), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function patchPlannerTerminalCaches(
  queryClient: ReturnType<typeof useQueryClient>,
  operation: AcpRun,
  event: AcpRunEvent,
) {
  if (event.type === "run.activity.reconciled") {
    void queryClient.invalidateQueries({ queryKey: plannerKeys.activity(operation.run_id) });
    return;
  }
  if (event.type !== "run.completed" && event.type !== "run.failed" && event.type !== "run.cancelled") return;
  const fields: Partial<PlanningSessionDetail> =
    operation.agent_name === "planner-generate-title"
      ? { current_title_run_id: undefined }
      : {
          conversation_state: "ready",
          current_run_id: undefined,
        };
	if (event.type === "run.completed" && (operation.agent_name === "planner-reply" || operation.agent_name === "planner-regenerate")) {
		const messageId = typeof event.payload.planning_message_id === "string" ? event.payload.planning_message_id : undefined;
    if (messageId) {
      fields.active_leaf_message_id = messageId;
    }
    const sessionPatch = sessionPatchFields(event.payload.session_patch);
    if (sessionPatch) {
      Object.assign(fields, sessionPatch);
		}
		void queryClient.invalidateQueries({ queryKey: plannerKeys.activity(operation.run_id) });
		void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(operation.planning_session_id) });
	}
	if (event.type === "run.completed" && operation.agent_name === "planner-formalize") {
    const draftId = typeof event.payload.planning_draft_id === "string" ? event.payload.planning_draft_id : undefined;
		if (draftId) {
			fields.current_draft_id = draftId;
		}
		void queryClient.invalidateQueries({ queryKey: plannerKeys.activity(operation.run_id) });
		void queryClient.invalidateQueries({ queryKey: plannerKeys.plan(operation.planning_session_id), exact: true });
	}
	if (event.type === "run.failed" || event.type === "run.cancelled") {
		void queryClient.invalidateQueries({ queryKey: plannerKeys.activity(operation.run_id) });
	}
	applyPlanningSessionFields(queryClient, operation.planning_session_id, fields);
}

function sessionPatchFields(value: unknown): Partial<PlanningSessionDetail> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const candidate = value as Record<string, unknown>;
  const fields: Partial<PlanningSessionDetail> = {};
  if (typeof candidate.auto_title_eligible === "boolean") {
    fields.auto_title_eligible = candidate.auto_title_eligible;
  }
  if (typeof candidate.current_title_run_id === "string" && candidate.current_title_run_id.trim() !== "") {
    fields.current_title_run_id = candidate.current_title_run_id.trim();
  }
  if (typeof candidate.updated_at === "string" && candidate.updated_at.trim() !== "") {
    fields.updated_at = candidate.updated_at.trim();
  }
  return Object.keys(fields).length > 0 ? fields : null;
}

function patchPlanningCaches(
  queryClient: ReturnType<typeof useQueryClient>,
  operation: AcpRun,
  event: AcpRunEvent,
) {
  if (event.type === "message.completed" && (operation.agent_name === "planner-reply" || operation.agent_name === "planner-regenerate")) {
    const message = planningMessageFromEvent(operation, event);
    if (message) {
      queryClient.setQueryData(
        plannerKeys.messages(operation.planning_session_id),
        (current: unknown) => (
          operation.agent_name === "planner-regenerate"
            ? replaceAssistantMessageAtTurn(current as never, message)
            : replaceAssistantMessageInLatestPlanningPage(current as never, message)
        ) as never,
      );
    }
    return;
  }
  if (event.type === "message.completed" && operation.agent_name === "planner-generate-title") {
    const title = typeof event.payload.title === "string" ? event.payload.title : undefined;
    if (title) {
      applyGeneratedPlanningSessionTitleIfPlaceholder(queryClient, operation.planning_session_id, title);
    }
  }
}

function planningMessageFromEvent(operation: AcpRun, event: AcpRunEvent): PlanningMessage | null {
  const planningMessageId = typeof event.payload.planning_message_id === "string" ? event.payload.planning_message_id.trim() : "";
  const contentMarkdown = typeof event.payload.content_markdown === "string"
    ? event.payload.content_markdown
    : typeof event.payload.text === "string"
      ? event.payload.text
      : "";
  const createdAt = typeof event.payload.created_at === "string" ? event.payload.created_at : event.created_at;
  if (!planningMessageId || !contentMarkdown) {
    return null;
  }
  return {
    planning_message_id: planningMessageId,
    planning_session_id: operation.planning_session_id,
    parent_message_id: typeof event.payload.parent_message_id === "string" ? event.payload.parent_message_id : undefined,
    role: (typeof event.payload.role === "string" ? event.payload.role : "assistant") as PlanningMessage["role"],
    content_markdown: contentMarkdown,
    source_run_id: operation.run_id,
    created_at: createdAt,
    // A freshly produced reply/regenerate is the newest branch at its point;
    // the authoritative branch_count arrives on the messages refetch.
    branch_index: 0,
    branch_count: 1,
    question_payload: isObject(event.payload.question_payload) ? (event.payload.question_payload as PlanningMessage["question_payload"]) : undefined,
  };
}

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
