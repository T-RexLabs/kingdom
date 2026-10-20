import { useId, type ReactNode } from "react";
import type {
  RunExecutionDetail,
  RunNodeAttemptHistoryItem,
  RunNodeExecution,
} from "@/api/hooks";
import { SelectField } from "@/components/ui/select-field";
import { nodeLabel } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import { useTheme, roleIdentity } from "@/theme";
import { executionDisplayStatus } from "./run-status-semantics";
import { followedAttemptId } from "./run-execution-selection";

export function RunExecutionPicker({
  nodes,
  attempts,
  selectedNodeId,
  selectedAttemptId,
  detail,
  onSelectStep,
  onSelectAttempt,
  onSelectCurrentAttempt,
  hasMoreAttempts,
  onLoadMoreAttempts,
  isLoadingMoreAttempts,
  isHistoryLoading,
  historyErrorMessage,
  onRetryHistory,
  children,
}: {
  nodes: RunNodeExecution[];
  attempts: RunNodeAttemptHistoryItem[];
  selectedNodeId: string | null;
  selectedAttemptId: string | null;
  detail: RunExecutionDetail | null;
  onSelectStep: (nodeId: string) => void;
  onSelectAttempt: (attemptId: string) => void;
  onSelectCurrentAttempt: () => void;
  hasMoreAttempts: boolean;
  onLoadMoreAttempts: () => void;
  isLoadingMoreAttempts: boolean;
  isHistoryLoading: boolean;
  historyErrorMessage?: string | null;
  onRetryHistory?: () => void;
  children?: ReactNode;
}) {
  const { theme } = useTheme();
  const pickerId = useId();
  const node = nodes.find((item) => item.node_id === selectedNodeId);
  const current = node?.current_execution;
  const followedID = followedAttemptId(node, attempts);
  const selectedID = selectedAttemptId ?? followedID;
  const selectedDetail = detail?.attempt_id === selectedID ? detail : null;
  const options = [
    ...new Map(
      [
        ...(current ? [current] : []),
        ...attempts,
        ...(selectedDetail ? [selectedDetail] : []),
      ].map((item) => [item.attempt_id, item]),
    ).values(),
  ].sort((a, b) => b.attempt - a.attempt);
  return (
    <div className="flex flex-wrap items-end gap-3">
      <SelectField
        id={`${pickerId}-agent`}
        label={node?.execution_ownership === "runtime_owned" ? "Step" : "Agent"}
        ariaLabel="Select workflow agent or step"
        className="w-72 max-w-full min-w-0"
        value={node?.node_id ?? ""}
        onChange={onSelectStep}
        options={nodes.map((item) => ({
          value: item.node_id,
          label:
            item.execution_ownership === "runtime_owned"
              ? `${nodeLabel(item.node_id)} · Runtime`
              : `${roleIdentity(theme, item.required_role).name} · ${nodeLabel(item.node_id)}`,
        }))}
      />
      {(node?.attempt_count ?? 0) > 1 && (
        <SelectField
          id={`${pickerId}-attempt`}
          label="Attempt"
          ariaLabel="Select attempt"
          className="w-56 max-w-full"
          value={selectedID ?? ""}
          onChange={(value) =>
            value === followedID
              ? onSelectCurrentAttempt()
              : onSelectAttempt(value)
          }
          options={[
            ...(selectedID &&
            !options.some((item) => item.attempt_id === selectedID)
              ? [{ value: selectedID, label: "Selected Attempt" }]
              : []),
            ...options.map((item) => ({
              value: item.attempt_id,
              label: `Attempt ${item.attempt} · ${executionDisplayStatus(item) === "rework" ? "Rework Requested" : statusLabel(item.status)}${item.attempt_id === current?.attempt_id ? " · Current" : ""}`,
            })),
          ]}
        />
      )}
      {selectedID && followedID && selectedID !== followedID && (
        <button
          type="button"
          className="min-h-10 text-xs text-accent"
          onClick={onSelectCurrentAttempt}
        >
          {current ? "Return to Current Attempt" : "Return to Latest Attempt"}
        </button>
      )}
      {children}
      {hasMoreAttempts && (
        <button
          type="button"
          disabled={isLoadingMoreAttempts}
          onClick={onLoadMoreAttempts}
          className="min-h-11 text-xs font-medium text-accent"
        >
          {isLoadingMoreAttempts ? "Loading attempts…" : "Load Older Attempts"}
        </button>
      )}
      {isHistoryLoading && (
        <span className="text-xs text-foreground-muted">Loading attempts…</span>
      )}
      {historyErrorMessage && (
        <p role="alert" className="w-full text-sm text-status-failed">
          {historyErrorMessage}{" "}
          <button type="button" className="underline" onClick={onRetryHistory}>
            Retry Attempts
          </button>
        </p>
      )}
    </div>
  );
}
