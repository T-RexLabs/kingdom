import type {
  RunDetailSummary,
  RunExecutionDetail,
  RunNodeAttemptHistoryItem,
  RunNodeExecution,
} from "@/api/hooks";
import { nodeLabel } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import { formatExactTime, timestampDescription } from "@/lib/date-time";
import { ThemeIcon, useTheme, roleIdentity } from "@/theme";
import { RunExecutionActivity } from "./run-execution-activity";
import { RunExecutionsPanel } from "./run-executions-panel";
import {
  presentAttemptHistoryItem,
  presentRunNodeExecution,
} from "./run-execution-presentation";
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
  onViewOutput,
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
  mode = "activity",
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
  onViewOutput?: () => void;
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
  mode?: "activity" | "output";
  children?: ReactNode;
}) {
  const { theme } = useTheme();
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
  const options = [
    ...new Map(
      [
        ...(current ? [current] : []),
        ...attempts,
        ...(selectedDetail ? [selectedDetail] : []),
      ].map((item) => [item.attempt_id, item]),
    ).values(),
  ].sort((a, b) => b.attempt - a.attempt);
  const identity = node ? roleIdentity(theme, node.required_role) : null;
  const showNotice = Boolean(
    presentation &&
    (presentation.failureDetail ||
      presentation.retryDetail ||
      presentation.requiredAction ||
      presentation.tone === "paused"),
  );

  return (
    <section
      className="space-y-3"
      aria-label={mode === "activity" ? "Agent activity" : "Agent output"}
    >
      <div className="flex flex-wrap items-end gap-3">
        <label className="block w-80 max-w-full min-w-0 text-xs font-medium text-foreground-muted">
          <span className="flex items-center gap-1.5">
            {node && (
              <ThemeIcon
                slot={`role.${node.required_role}`}
                size={14}
                className="text-accent"
              />
            )}
            {node?.execution_ownership === "runtime_owned" ? "Step" : "Agent"}
          </span>
          <select
            aria-label="Select workflow agent or step"
            value={node?.node_id ?? ""}
            onChange={(event) => onSelectStep(event.target.value)}
            className="mt-1 block min-h-10 w-full rounded-lg border border-border bg-background px-3 text-sm text-foreground"
          >
            {nodes.map((item) => (
              <option key={item.node_id} value={item.node_id}>
                {item.execution_ownership === "runtime_owned"
                  ? `${nodeLabel(item.node_id)} · Runtime`
                  : `${roleIdentity(theme, item.required_role).name} · ${nodeLabel(item.node_id)}`}
              </option>
            ))}
          </select>
        </label>
        {(node?.attempt_count ?? 0) > 1 && (
          <label className="text-xs font-medium text-foreground-muted">
            Attempt
            <select
              aria-label="Select attempt"
              value={selectedID ?? ""}
              onChange={(event) =>
                event.target.value === current?.attempt_id
                  ? onSelectCurrentAttempt()
                  : onSelectAttempt(event.target.value)
              }
              className="mt-1 block min-h-10 min-w-44 rounded-lg border border-border bg-background px-3 text-sm text-foreground"
            >
              {selectedID &&
                !options.some((item) => item.attempt_id === selectedID) && (
                  <option value={selectedID}>Selected Attempt</option>
                )}
              {options.map((item) => (
                <option key={item.attempt_id} value={item.attempt_id}>
                  Attempt {item.attempt} · {statusLabel(item.status)}
                  {item.attempt_id === current?.attempt_id ? " · Current" : ""}
                </option>
              ))}
            </select>
          </label>
        )}
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
        {mode === "activity" &&
          selectedDetail?.output_status === "settled" &&
          selectedDetail.settlement_status !== "pending" &&
          onViewOutput && (
            <button
              type="button"
              onClick={onViewOutput}
              className="min-h-10 text-sm font-medium text-accent"
            >
              View Settled Response
            </button>
          )}
        {hasMoreAttempts && (
          <button
            type="button"
            disabled={isLoadingMoreAttempts}
            onClick={onLoadMoreAttempts}
            className="min-h-11 text-xs font-medium text-accent"
          >
            {isLoadingMoreAttempts
              ? "Loading attempts…"
              : "Load Older Attempts"}
          </button>
        )}
        {isHistoryLoading && (
          <span className="text-xs text-foreground-muted">
            Loading attempts…
          </span>
        )}
        {historyErrorMessage && (
          <p role="alert" className="w-full text-sm text-status-failed">
            {historyErrorMessage}{" "}
            <button
              type="button"
              className="underline"
              onClick={onRetryHistory}
            >
              Retry Attempts
            </button>
          </p>
        )}
      </div>
      {historical && (
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-accent/25 bg-accent/5 px-4 py-3 text-xs">
          <span>
            Historical Attempt {execution?.attempt ?? ""}
            {history?.superseded ? " · Superseded" : ""}
          </span>
          <button
            type="button"
            className="min-h-9 text-accent"
            onClick={onSelectCurrentAttempt}
          >
            Return to Current Attempt
          </button>
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
      {mode === "activity" &&
        node?.execution_ownership === "provider_backed" && (
          <RunExecutionActivity
            key={selectedID}
            runId={run.run_id}
            attemptId={selectedID ?? null}
            status={execution?.status}
          />
        )}
      {mode === "activity" && node?.execution_ownership === "runtime_owned" && (
        <p className="rounded-lg border border-border p-5 text-sm text-foreground-muted">
          Kingdom owns this step. Its transitions appear in Audit; it does not
          produce provider thoughts or tool calls.
        </p>
      )}
      {mode === "output" && (
        <RunExecutionsPanel
          detail={selectedDetail}
          isDetailLoading={isDetailLoading}
          detailErrorMessage={detailErrorMessage}
          onRetryDetail={onRetryDetail}
          failure={historical ? history?.failure : node?.failure}
        />
      )}
      {node && presentation && (
        <details className="rounded-lg border border-border px-4 text-xs text-foreground-muted">
          <summary className="min-h-11 cursor-pointer py-3 font-medium text-foreground">
            Execution Details
          </summary>
          <div className="space-y-2 pb-4">
            <p>
              {node.execution_ownership === "runtime_owned" ? "Step" : "Agent"}:{" "}
              {identity?.name}
              {identity?.canonicalName !== identity?.name
                ? ` · ${identity?.canonicalName}`
                : ""}{" "}
              · {nodeLabel(node.node_id)}
            </p>
            {!showNotice && <p>{presentation.description}</p>}
            {history && (
              <>
                <p>
                  Attempt ID:{" "}
                  <code className="break-all">{history.attempt_id}</code>
                </p>
                <p>
                  Started:{" "}
                  {history.started_at ? (
                    <time
                      dateTime={history.started_at}
                      title={timestampDescription(history.started_at)}
                    >
                      {formatExactTime(history.started_at)}
                    </time>
                  ) : (
                    "Observation Not Recorded"
                  )}
                </p>
                {history.provider_invocation_id && (
                  <p>
                    Provider Invocation ID:{" "}
                    <code className="break-all">
                      {history.provider_invocation_id}
                    </code>
                  </p>
                )}
                {history.token_usage && (
                  <p>
                    Token Usage: {history.token_usage.total_tokens} total (
                    {history.token_usage.input_tokens} input,{" "}
                    {history.token_usage.output_tokens} output)
                  </p>
                )}
                {presentAttemptHistoryItem(
                  history,
                  current?.attempt_id,
                ).retryRelationships.map((relationship, index) => (
                  <p key={index}>{relationship}</p>
                ))}
              </>
            )}
          </div>
        </details>
      )}
      {children}
    </section>
  );
}
