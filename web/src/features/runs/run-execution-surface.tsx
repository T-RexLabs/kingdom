import type {
  RunDetailSummary,
  RunExecutionDetail,
  RunNodeAttemptHistoryItem,
  RunNodeExecution,
} from "@/api/hooks";
import { RunExecutionPicker } from "./run-execution-picker";
import { RunExecutionActivity } from "./run-execution-activity";
import { RoleIdentity } from "@/theme";
import { RunBrief } from "./run-conversation";
import { presentRunNodeExecution } from "./run-execution-presentation";
import type { ReactNode } from "react";

export function RunExecutionSurface({
  run,
  nodes,
  attempts,
  selectedNodeId,
  selectedAttemptId,
  onSelectStep,
  onSelectAttempt,
  onSelectCurrentAttempt,
  onViewDetails,
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
  onViewDetails?: () => void;
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
  const node = nodes.find((item) => item.node_id === selectedNodeId);
  const current = node?.current_execution;
  const selectedID = selectedAttemptId ?? current?.attempt_id;
  const history = attempts.find((item) => item.attempt_id === selectedID);
  const selectedDetail = detail?.attempt_id === selectedID ? detail : null;
  const execution =
    selectedID === current?.attempt_id ? current : (history ?? selectedDetail);
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
      className="mx-auto max-w-[800px] space-y-3"
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
        {selectedID && onViewDetails && (
          <button
            type="button"
            onClick={onViewDetails}
            className="min-h-10 text-xs text-foreground-muted hover:text-foreground"
          >
            Attempt Details
          </button>
        )}
      </RunExecutionPicker>
      {historical && (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-accent/25 bg-accent/5 px-4 py-3 text-xs">
          <span>
            Historical Attempt {execution?.attempt ?? ""}
            {history?.superseded ? " · Superseded" : ""}
          </span>
        </div>
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
            className="min-h-11 rounded-lg border border-status-blocked/40 px-4 text-sm text-status-blocked"
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
      <RunBrief workOrderId={run.work_order_id} />
      {node?.execution_ownership === "provider_backed" && (
        <div className="pt-4 text-sm font-medium">
          <RoleIdentity role={node.required_role} showResponsibility={false} />
        </div>
      )}
      {node?.execution_ownership === "provider_backed" && (
        <RunExecutionActivity
          key={selectedID}
          runId={run.run_id}
          attemptId={selectedID ?? null}
          status={execution?.status}
          detail={selectedDetail}
        />
      )}
      {node?.execution_ownership === "runtime_owned" && (
        <p className="py-5 text-sm text-foreground-muted">
          Kingdom owns this step. Its transitions appear in Run Details under
          Audit; it does not produce provider thoughts or tool calls.
        </p>
      )}
      {children}
    </section>
  );
}
