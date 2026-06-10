import { useEffect, useReducer, useRef } from "react";
import { useQueryClient, type InfiniteData } from "@tanstack/react-query";
import { API_BASE, apiFetch, buildAuthHeaders } from "@/api/client";
import type {
  PlanningMessageVariantsResponse,
  PlanningMessagesPage,
  PlanningOperationDetail,
  PlanningOperationEvent,
  PlanningSessionDetail,
} from "@/api/hooks";
import { subscribeSSE } from "@/lib/sse-stream";
import {
  buildPlannerOperationState,
  reducePlannerOperationEvent,
  type PlannerOperationState,
} from "./planner-operation-controller";
import {
  patchLatestPlanningMessagesPage,
  replaceAssistantMessageInLatestPlanningPage,
} from "./planner-message-pages";
import {
  applyGeneratedPlanningSessionTitleIfPlaceholder,
  applyPlanningSessionFields,
} from "./planner-session-cache";
import { plannerKeys } from "./planner-query-keys";

/**
 * UI-friendly summary of the SSE connection state. Mirrors `SSEConnectionState`
 * but is shaped for direct consumption by banners/badges in the planner UI.
 */
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
  | { type: "hydrate"; operation: PlanningOperationDetail }
  | { type: "event"; event: PlanningOperationEvent }
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
    case "event":
      if (!state.operation || state.operation.operationId !== action.event.operation_id) {
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

/**
 * Subscribes to the planning operation's SSE event stream and returns the
 * accumulated operation state plus a granular connection summary.
 *
 * Replaces the previous infinite `while(!aborted) { catch{} sleep 500ms }`
 * loop, which silently retried forever and gave the UI no signal that the
 * stream had failed. Now: full-jitter exponential backoff via subscribeSSE
 * and a hard cap on attempts. SSE failure surfaces purely through the
 * `connection` field — the UI banner reads that — and a poll-fallback
 * effect (below) hydrates the canonical operation row when retries are
 * exhausted. Operation status is server-attested only: it transitions to
 * a terminal value via real SSE events or a poll-fallback hydrate, never
 * via a synthetic event. (R7 added the poll fallback; R8 removed an
 * earlier synthetic-error dispatch that conflated transport state with
 * operation state — that change resolved cascade bugs in
 * GlobalFormalizeProvider and the page's terminal-clear effect.)
 */
export function usePlannerOperation(operation?: PlanningOperationDetail | null): UsePlannerOperationResult {
  const [state, dispatch] = useReducer(reducer, initialState);
  const abortRef = useRef<AbortController | null>(null);
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!operation) {
      dispatch({ type: "reset" });
      return;
    }

    dispatch({ type: "hydrate", operation });

    if (operation.status === "succeeded" || operation.status === "failed" || operation.status === "cancelled") {
      reconcileTerminalOperationSnapshot(queryClient, operation);
      dispatch({ type: "connection", connection: { state: "idle" } });
      return;
    }

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    let lastEventId: string | null = null;

    void subscribeSSE({
      url: `${API_BASE}/planning-operations/${operation.operation_id}/events`,
      buildHeaders: (lastId) => buildAuthHeaders(lastId ? { "Last-Event-ID": lastId } : undefined),
      isTerminalEventType: (type) => type === "done" || type === "error" || type === "cancelled",
      signal: controller.signal,
      onConnectionChange: (next) => {
        // Connection state and operation status are independent concerns.
        // Dispatching a synthetic terminal "error" event here would lie to
        // the operation reducer: the server may still be processing the
        // operation, and a fake terminal status causes the page to clear
        // its activeOperationId, which can tear down the poll-fallback
        // before it ever reconciles. UI surfaces the transport hiccup via
        // result.connection (PlannerThread renders the connection-lost
        // banner from that field); the poll-fallback effect below is the
        // sole authority for hydrating a real terminal status.
        dispatch({ type: "connection", connection: next });
      },
      onEvent: (parsed) => {
        // handleEventPayload returns true when the event was applied (cache
        // patched + reducer updated), false when JSON parse / cache write
        // failed. The library uses this signal to advance lastEventId only
        // on apply success, so a malformed payload triggers replay rather
        // than a silent skip.
        const applied = handleEventPayload({
          operation,
          eventType: parsed.type,
          data: parsed.data,
          eventId: parsed.id || null,
          dispatch,
          queryClient,
        });
        if (applied && parsed.id) lastEventId = parsed.id;
        return applied;
      },
    }).catch((err: unknown) => {
      if (controller.signal.aborted) return;
      // subscribeSSE only rejects on programmer error; defend defensively.
      // eslint-disable-next-line no-console
      console.error("planner SSE subscription failed", err);
    });

    void lastEventId; // referenced via closure; prevents lint dead-code flag.

    return () => {
      controller.abort();
      if (abortRef.current === controller) {
        abortRef.current = null;
      }
    };
  }, [operation, queryClient]);

  // Bounded poll-fallback when SSE retries are exhausted. Reconciliation is
  // strictly canonical: on terminal status we hydrate the reducer from the
  // polled operation row and invalidate the dependent queries so server
  // truth replaces whatever stale optimistic state the client held. We do
  // NOT synthesise terminal payloads — the normal "done" handlers expect a
  // full PlanningMessage shape (reply/regenerate) or a draft id (formalize),
  // and a fabricated `{ result_ref }` would poison the message cache.
  useEffect(() => {
    if (state.connection.state !== "failed" || !operation) return;

    const controller = new AbortController();
    const pollIntervalMs = 5_000;
    const maxPolls = 60;

    void (async () => {
      for (let attempt = 0; attempt < maxPolls; attempt++) {
        if (controller.signal.aborted) return;
        try {
          const fresh = await apiFetch<PlanningOperationDetail>(
            `/planning-operations/${operation.operation_id}`,
            { signal: controller.signal },
          );
          if (fresh.status === "succeeded" || fresh.status === "failed" || fresh.status === "cancelled") {
            // Hydrate the local reducer from the canonical operation row.
            // This sets status to "done"/"error"/"cancelled" so the
            // operationError banner clears and the composer recovers.
            dispatch({ type: "hydrate", operation: fresh });
            // Reset connection state so the SSE-failed banner disappears.
            dispatch({ type: "connection", connection: { state: "idle" } });
            // Refetch every dependent surface so the UI reflects whatever
            // the server persisted while we were disconnected. Use the
            // canonical query keys so any other consumer of these caches
            // (sidebar, list, plan modal, variant pickers) re-renders.
            void queryClient.invalidateQueries({ queryKey: plannerKeys.operation(operation.operation_id) });
            void queryClient.invalidateQueries({ queryKey: plannerKeys.detail(operation.planning_session_id) });
            void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(operation.planning_session_id) });
            void queryClient.invalidateQueries({ queryKey: plannerKeys.plan(operation.planning_session_id) });
            // Sidebar/list views also need refresh after SSE
            // exhaustion — without this, a session whose status flipped
            // while we were disconnected stays stale in the list cache.
            // listPrefix() matches every cached limit slice.
            void queryClient.invalidateQueries({ queryKey: plannerKeys.listPrefix() });
            return;
          }
        } catch (err) {
          if (controller.signal.aborted) return;
          // Network still down — continue polling. A persistent server
          // outage just means the loop walks to its cap.
        }
        await new Promise<void>((resolve) => {
          const timer = setTimeout(() => {
            controller.signal.removeEventListener("abort", onAbort);
            resolve();
          }, pollIntervalMs);
          const onAbort = () => {
            clearTimeout(timer);
            resolve();
          };
          controller.signal.addEventListener("abort", onAbort, { once: true });
        });
      }
    })();

    return () => {
      controller.abort();
    };
  }, [state.connection.state, operation, queryClient]);

  return state;
}

export function reconcileTerminalOperationSnapshot(
  queryClient: ReturnType<typeof useQueryClient>,
  operation: PlanningOperationDetail,
) {
  const isTerminal = operation.status === "succeeded" || operation.status === "failed" || operation.status === "cancelled";
  if (!isTerminal) return;

  if (operation.type === "generate_title" && operation.status === "succeeded") {
    const generatedTitle = operation.observed_output_text?.trim();
    if (generatedTitle) {
      applyGeneratedPlanningSessionTitleIfPlaceholder(queryClient, operation.planning_session_id, generatedTitle);
    }
  }

  const fields: Partial<PlanningSessionDetail> =
    operation.type === "generate_title"
      ? {
          current_title_operation_id: undefined,
        }
      : {
          conversation_state: "ready",
          current_operation_id: undefined,
        };
  applyPlanningSessionFields(queryClient, operation.planning_session_id, fields);
  void queryClient.invalidateQueries({ queryKey: plannerKeys.detail(operation.planning_session_id), exact: true });
  void queryClient.invalidateQueries({ queryKey: plannerKeys.listPrefix() });
  if (operation.type === "reply" || operation.type === "regenerate") {
    void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(operation.planning_session_id) });
  }
  if (operation.type === "formalize") {
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
    operation: PlanningOperationDetail;
    eventType: string;
    data: string;
    eventId: string | null;
    dispatch: React.Dispatch<OperationStreamAction>;
    queryClient: ReturnType<typeof useQueryClient>;
  },
): boolean {
  // Keepalives are framing only — they have no payload and no caches to
  // touch. Treat them as applied so the SSE library is free to advance
  // lastEventId past them (they have no id anyway, but explicit is safer).
  if (eventType === "keepalive") return true;
  let payload: Record<string, unknown>;
  try {
    payload = JSON.parse(data) as Record<string, unknown>;
  } catch {
    // Refuse to advance lastEventId for an unparseable event so the next
    // reconnect replays this event rather than silently skipping it.
    // The reducer is left untouched — we don't want a phantom error event
    // poisoning the operation state for what may be a transient parse
    // glitch (truncated EOF, proxy mid-record close).
    return false;
  }
  const seq = parseEventSeq(eventId);
  const event = {
    event_id: eventId ?? `${operation.operation_id}-${Date.now()}`,
    operation_id: operation.operation_id,
    seq: seq ?? 0,
    event_type: eventType,
    payload,
    created_at: new Date().toISOString(),
  } satisfies PlanningOperationEvent;
  patchPlanningCaches(queryClient, operation, event);
  patchPlannerTerminalCaches(queryClient, operation, event);
  dispatch({ type: "event", event });
  return true;
}

function parseEventSeq(eventId: string | null): number | null {
  if (!eventId) return null;
  const marker = eventId.lastIndexOf(":");
  if (marker < 0) return null;
  const parsed = Number.parseInt(eventId.slice(marker + 1), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

// Exported for direct unit testing of the field-scoped reconciliation
// path. The hook itself is hard to set up in isolation (SSE fixtures,
// query-client wiring, async sequencing) and the reconciliation
// contract is what matters: a terminal event must apply only the
// fields it owns, leaving concurrent unrelated cache state intact.
export function patchPlannerTerminalCaches(
  queryClient: ReturnType<typeof useQueryClient>,
  operation: PlanningOperationDetail,
  event: PlanningOperationEvent,
) {
  // `cancelled` is treated like `error` for cache-reset purposes:
  // conversation_state returns to ready and current_operation_id is
  // cleared so the composer re-enables. The status difference lives in
  // the local PlannerOperationState reducer (and the banner styling),
  // not in the session-detail cache shape.
  if (event.event_type !== "done" && event.event_type !== "error" && event.event_type !== "cancelled") return;
  // Field-scoped reconciliation: only the fields the terminal event
  // actually owns are written. A previous full-snapshot replace
  // would clobber any concurrent SSE/poll updates that landed
  // between the cached read and the patch — the same race class the
  // optimistic title write fix addresses on the user-input side.
  const fields: Partial<PlanningSessionDetail> =
    operation.type === "generate_title"
      ? {
          current_title_operation_id: undefined,
        }
      : {
          conversation_state: "ready",
          current_operation_id: undefined,
        };
  if (event.event_type === "done" && (operation.type === "reply" || operation.type === "regenerate")) {
    const messageId = typeof event.payload.planning_message_id === "string" ? event.payload.planning_message_id : undefined;
    if (messageId) {
      fields.current_question_message_id = messageId;
    }
    const sessionPatch = sessionPatchFields(event.payload.session_patch);
    if (sessionPatch) {
      Object.assign(fields, sessionPatch);
    }
  }
  if (event.event_type === "done" && operation.type === "formalize") {
    const draftId = typeof event.payload.planning_draft_id === "string" ? event.payload.planning_draft_id : undefined;
    if (draftId) {
      fields.current_draft_id = draftId;
      void queryClient.invalidateQueries({ queryKey: plannerKeys.plan(operation.planning_session_id), exact: true });
    }
  }
  if (event.event_type === "done" && operation.type === "generate_title") {
    const title = typeof event.payload.title === "string" ? event.payload.title : undefined;
    if (title) {
      // Backend only adopts a generated title if the session still has a
      // placeholder title when the operation settles. Mirror that canonical
      // rule in cache reconciliation so a late terminal event cannot clobber a
      // newer user-edited custom title.
      applyGeneratedPlanningSessionTitleIfPlaceholder(queryClient, operation.planning_session_id, title);
    }
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
  if (typeof candidate.current_title_operation_id === "string" && candidate.current_title_operation_id.trim() !== "") {
    fields.current_title_operation_id = candidate.current_title_operation_id.trim();
  }
  if (typeof candidate.updated_at === "string" && candidate.updated_at.trim() !== "") {
    fields.updated_at = candidate.updated_at.trim();
  }
  return Object.keys(fields).length > 0 ? fields : null;
}

function patchPlanningCaches(
  queryClient: ReturnType<typeof useQueryClient>,
  operation: PlanningOperationDetail,
  event: PlanningOperationEvent,
) {
  const messagesKey = plannerKeys.messages(operation.planning_session_id);

  if (event.event_type === "user_message") {
    const message = event.payload as PlanningMessagesPage["items"][number];
    queryClient.setQueryData<InfiniteData<PlanningMessagesPage, unknown>>(messagesKey, (current) =>
      patchLatestPlanningMessagesPage(current, message),
    );
    return;
  }

  if (event.event_type === "done" && (operation.type === "reply" || operation.type === "regenerate")) {
    const message = event.payload as PlanningMessagesPage["items"][number];
    queryClient.setQueryData<InfiniteData<PlanningMessagesPage, unknown>>(messagesKey, (current) =>
      replaceAssistantMessageInLatestPlanningPage(current, message),
    );
    void queryClient.invalidateQueries({ queryKey: messagesKey });

    if (operation.type === "regenerate") {
      const messageId = typeof event.payload.planning_message_id === "string" ? event.payload.planning_message_id : undefined;
      if (messageId) {
        void queryClient.prefetchQuery({
          queryKey: plannerKeys.variants(operation.planning_session_id, messageId),
          queryFn: () => apiFetch<PlanningMessageVariantsResponse>(
            `/planning-sessions/${operation.planning_session_id}/messages/${messageId}/variants`,
          ),
        });
      }
    }
  }
}
