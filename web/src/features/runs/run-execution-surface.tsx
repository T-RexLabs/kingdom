import type {
  RunDetailSummary,
  RunExecutionDetail,
  RunNodeAttemptHistoryItem,
  RunNodeExecution,
} from "@/api/hooks";
import { RunExecutionPicker } from "./run-execution-picker";
import { RunExecutionActivity } from "./run-execution-activity";
import { RunAttemptTasks } from "./run-attempt-tasks";
import { RoleIdentity } from "@/theme";
import {
  RunInstructions,
  RunFinalResponse,
  RunRuntimeResponse,
} from "./run-conversation";
import { hasSettledResponse } from "./run-conversation-state";
import { presentRunNodeExecution } from "./run-execution-presentation";
import { followedAttemptId } from "./run-execution-selection";
import { isTerminalRunStatus } from "./run-freshness";
import { useId, useState, type ReactNode } from "react";

export function RunExecutionSurface({
  run,
  nodes,
  attempts,
  selectedNodeId,
  selectedAttemptId,
  onSelectStep,
  onSelectAttempt,
  onSelectCurrentAttempt,
  hasMoreAttempts,
  onLoadMoreAttempts,
  isLoadingMoreAttempts,
  isHistoryLoading,
  historyErrorMessage,
  onRetryHistory,
  detail,
  isDetailLoading,
  detailErrorMessage,
  onRetryDetail,
  onRetryStop,
  isStopRetryPending,
  stopRetryErrorMessage,
  children,
}: {
  run: RunDetailSummary;
  nodes: RunNodeExecution[];
  attempts: RunNodeAttemptHistoryItem[];
  selectedNodeId: string | null;
  selectedAttemptId: string | null;
  onSelectStep: (nodeId: string) => void;
  onSelectAttempt: (attemptId: string) => void;
  onSelectCurrentAttempt: () => void;
  hasMoreAttempts: boolean;
  onLoadMoreAttempts: () => void;
  isLoadingMoreAttempts: boolean;
  isHistoryLoading: boolean;
  historyErrorMessage?: string | null;
  onRetryHistory?: () => void;
  detail:
    | (RunExecutionDetail & {
        live_signal?: { live_label?: string; activity_at: string };
      })
    | null;
  isDetailLoading: boolean;
  detailErrorMessage: string | null;
  onRetryDetail: () => void;
  onRetryStop: (attemptID: string) => void;
  isStopRetryPending: boolean;
  stopRetryErrorMessage: string | null;
  children?: ReactNode;
}) {
  const [selectedView, setSelectedView] = useState<{
    scopeKey: string;
    value: "conversation" | "instructions" | "response";
  } | null>(null);
  const viewsId = useId();
  const node = nodes.find((item) => item.node_id === selectedNodeId);
  const current = node?.current_execution;
  const followedID = followedAttemptId(node, attempts);
  const selectedID = selectedAttemptId ?? followedID ?? undefined;
  // Following the latest attempt while the step's next attempt has not started.
  const awaitingNextAttempt =
    !current && !!selectedID && selectedID === followedID;
  const history = attempts.find((item) => item.attempt_id === selectedID);
  const selectedDetail = detail?.attempt_id === selectedID ? detail : null;
  const hasResponse = hasSettledResponse(selectedDetail);
  const scopeKey = JSON.stringify([run.run_id, selectedID ?? selectedNodeId]);
  const requestedView =
    selectedView?.scopeKey === scopeKey ? selectedView.value : "conversation";
  const view =
    requestedView === "response" && !hasResponse
      ? "conversation"
      : requestedView;
  const setView = (value: "conversation" | "instructions" | "response") => {
    if (value !== "conversation" && selectedID) onSelectAttempt(selectedID);
    setSelectedView({ scopeKey, value });
  };
  const views = [
    "conversation",
    "instructions",
    ...(hasResponse ? ["response" as const] : []),
  ] as const;
  const execution =
    selectedDetail ?? (selectedID === current?.attempt_id ? current : history);
  const historical = !!selectedID && selectedID !== current?.attempt_id;
  const presentation = node
    ? presentRunNodeExecution(
        historical
          ? {
              status: execution?.status ?? "pending",
              failure: history?.failure,
            }
          : node,
        execution,
      )
    : null;
  const showNotice = Boolean(
    presentation &&
    (presentation.failureDetail ||
      presentation.retryDetail ||
      presentation.requiredAction ||
      presentation.tone === "paused"),
  );

  return (
    <section
      className="mx-auto flex h-full min-h-0 w-full max-w-3xl flex-col gap-3"
      aria-label="Agent activity"
    >
      <RunExecutionPicker
        nodes={nodes}
        attempts={attempts}
        selectedNodeId={selectedNodeId}
        selectedAttemptId={selectedID ?? null}
        detail={selectedDetail}
        onSelectStep={onSelectStep}
        onSelectAttempt={onSelectAttempt}
        onSelectCurrentAttempt={onSelectCurrentAttempt}
        hasMoreAttempts={hasMoreAttempts}
        onLoadMoreAttempts={onLoadMoreAttempts}
        isLoadingMoreAttempts={isLoadingMoreAttempts}
        isHistoryLoading={isHistoryLoading}
        historyErrorMessage={historyErrorMessage}
        onRetryHistory={onRetryHistory}
      >
        {presentation && (
          <span
            className="inline-flex min-h-10 items-center text-xs text-foreground-muted"
            role={!showNotice ? presentation.announcement?.role : undefined}
            aria-live={
              !showNotice ? presentation.announcement?.ariaLive : undefined
            }
          >
            {presentation.heading}
          </span>
        )}
      </RunExecutionPicker>
      {awaitingNextAttempt ? (
        <p className="rounded-lg border border-border px-4 py-3 text-xs text-foreground-muted">
          Showing attempt {execution?.attempt ?? ""}.{" "}
          {isTerminalRunStatus(run.status)
            ? "The next attempt did not start."
            : "The next attempt has not started yet."}
        </p>
      ) : (
        historical && (
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-accent/25 bg-accent/5 px-4 py-3 text-xs">
            <span>
              Historical Attempt {execution?.attempt ?? ""}
              {history?.superseded ? " · Superseded" : ""}
            </span>
          </div>
        )
      )}
      {presentation && showNotice && (
        <div
          className="space-y-1 rounded-lg border border-border px-3 py-2 text-sm text-foreground-muted"
          role={presentation.announcement?.role}
          aria-live={presentation.announcement?.ariaLive}
        >
          <p>{presentation.description}</p>
          {presentation.retryDetail && (
            <p className="mt-1 text-xs text-foreground-muted">
              {presentation.recoveryKind} · {presentation.retryDetail}
            </p>
          )}
          {presentation.failureDetail && (
            <p className="mt-1 text-xs text-foreground-muted">
              {presentation.failureDetail}
            </p>
          )}
          {presentation.requiredAction && (
            <p className="mt-2 text-sm text-status-blocked">
              {presentation.requiredAction}
            </p>
          )}
        </div>
      )}
      {!historical &&
        execution?.status === "paused" &&
        execution.stop_retry_available && (
          <button
            type="button"
            disabled={isStopRetryPending}
            onClick={() => onRetryStop(execution.attempt_id)}
            className="ui-button ui-button--outline min-h-11 rounded-lg border border-status-blocked/40 px-4 text-sm text-status-blocked"
          >
            {isStopRetryPending ? "Retrying stop…" : "Retry Stop"}
          </button>
        )}
      {stopRetryErrorMessage && (
        <p role="alert" className="text-sm text-status-failed">
          {stopRetryErrorMessage}
        </p>
      )}
      {isDetailLoading && (
        <p className="text-xs text-foreground-muted">
          Loading the selected attempt…
        </p>
      )}
      {detailErrorMessage && (
        <p role="alert" className="text-sm text-status-failed">
          {detailErrorMessage}{" "}
          <button type="button" className="underline" onClick={onRetryDetail}>
            Retry Attempt
          </button>
        </p>
      )}
      <div
        data-ui-tabs=""
        role="tablist"
        aria-label="Agent conversation views"
        className="flex gap-5 text-xs text-foreground-muted"
        onKeyDown={(event) => {
          if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key))
            return;
          event.preventDefault();
          const index = views.indexOf(view);
          const nextIndex =
            event.key === "Home"
              ? 0
              : event.key === "End"
                ? views.length - 1
                : (index +
                    (event.key === "ArrowRight" ? 1 : -1) +
                    views.length) %
                  views.length;
          const next = views[nextIndex];
          setView(next);
          document.getElementById(`${viewsId}-${next}`)?.focus();
        }}
      >
        {views.map((item) => (
          <button
            key={item}
            id={`${viewsId}-${item}`}
            type="button"
            data-ui-tab=""
            role="tab"
            aria-selected={view === item}
            aria-controls={`${viewsId}-${item}-panel`}
            tabIndex={view === item ? 0 : -1}
            className={`min-h-10 border-b-2 ${view === item ? "border-accent text-foreground" : "border-transparent hover:text-foreground"}`}
            onClick={() => setView(item)}
          >
            {item === "conversation"
              ? "Conversation"
              : item === "instructions"
                ? "Instructions"
                : "Final Response"}
          </button>
        ))}
      </div>
      <div
        id={`${viewsId}-instructions-panel`}
        role="tabpanel"
        aria-labelledby={`${viewsId}-instructions`}
        hidden={view !== "instructions"}
        className="min-h-0 flex-1 overflow-y-auto"
      >
        {view === "instructions" && (
          <RunInstructions
            runId={run.run_id}
            attemptId={selectedID ?? null}
            executionMode={node?.execution_ownership}
            status={execution?.status}
          />
        )}
      </div>
      {hasResponse && (
        <div
          id={`${viewsId}-response-panel`}
          role="tabpanel"
          aria-labelledby={`${viewsId}-response`}
          hidden={view !== "response"}
          className="min-h-0 flex-1 overflow-y-auto"
        >
          <RunFinalResponse key={selectedID} detail={selectedDetail} />
        </div>
      )}
      <div
        id={`${viewsId}-conversation-panel`}
        role="tabpanel"
        aria-labelledby={`${viewsId}-conversation`}
        hidden={view !== "conversation"}
        className="flex min-h-0 flex-1 flex-col"
      >
        {node?.execution_ownership === "provider_backed" && (
          <div className="space-y-1 pt-1">
            <div className="text-sm font-medium">
              <RoleIdentity
                role={node.required_role}
                showResponsibility={false}
              />
            </div>
            <RunAttemptTasks
              key={selectedID}
              runId={run.run_id}
              attemptId={selectedID ?? null}
              status={execution?.status}
            />
          </div>
        )}
        {node?.execution_ownership === "provider_backed" &&
          (selectedID || !isHistoryLoading) && (
            <div className="min-h-0 flex-1">
              <RunExecutionActivity
                key={selectedID}
                runId={run.run_id}
                attemptId={selectedID ?? null}
                status={execution?.status}
                detail={selectedDetail}
                isVisible={view === "conversation"}
                isDetailLoading={isDetailLoading}
              />
            </div>
          )}
        {node?.execution_ownership === "runtime_owned" &&
          (selectedDetail?.output_status === "observed" &&
          selectedDetail.output_text ? (
            <div className="min-h-0 flex-1 overflow-y-auto">
              <RunRuntimeResponse key={selectedID} detail={selectedDetail} />
            </div>
          ) : (
            <p className="py-5 text-sm text-foreground-muted">
              Kingdom handles this step. Its transitions appear in Audit.
            </p>
          ))}
        {children}
      </div>
    </section>
  );
}
