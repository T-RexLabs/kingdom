import type {
  RunExecutionDetail,
  RunExecutionFailure,
  RunNodeAttemptHistoryItem,
} from "@/api/hooks";
import { RunAttemptProvenance } from "./run-attempt-provenance";
import { statusLabel } from "@/lib/status-system";
import { formatExactTime, timestampDescription } from "@/lib/date-time";

export function RunDiagnostics({
  detail,
  history,
  currentAttemptId,
  failure,
  isLoading,
  errorMessage,
  onRetry,
}: {
  detail: RunExecutionDetail | null;
  history?: RunNodeAttemptHistoryItem;
  currentAttemptId?: string;
  failure?: RunExecutionFailure;
  isLoading: boolean;
  errorMessage: string | null;
  onRetry: () => void;
}) {
  const attempt = detail ?? history;
  const recordedFailure = failure ?? history?.failure;
  const failureCode =
    recordedFailure?.code ?? detail?.failure_code ?? history?.failure_code;
  const failureText =
    detail?.failure_text ??
    history?.failure_text ??
    recordedFailure?.operator_text;
  return (
    <div className="space-y-4 py-4 text-xs text-foreground-muted">
      {isLoading && <p>Loading diagnostics…</p>}
      {errorMessage && (
        <p role="alert" className="text-status-failed">
          {errorMessage}{" "}
          <button type="button" className="underline" onClick={onRetry}>
            Retry Diagnostics
          </button>
        </p>
      )}
      {!attempt && !isLoading && !errorMessage && (
        <p>This step has not recorded an attempt yet.</p>
      )}
      {attempt && (
        <>
          <RunAttemptProvenance
            detail={detail}
            history={history}
            currentAttemptId={currentAttemptId}
          />
          <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2">
            <dt>Status</dt>
            <dd>{statusLabel(attempt.status)}</dd>
            <dt>Node Run ID</dt>
            <dd className="break-all font-mono">{attempt.node_run_id}</dd>
            {attempt.provider_session_id && (
              <>
                <dt>Provider Session ID</dt>
                <dd className="break-all font-mono">
                  {attempt.provider_session_id}
                </dd>
              </>
            )}
            {attempt.queued_at && (
              <>
                <dt>Queued</dt>
                <dd>
                  <ExactTime iso={attempt.queued_at} />
                </dd>
              </>
            )}
            {attempt.launch_started_at && (
              <>
                <dt>Launching</dt>
                <dd>
                  <ExactTime iso={attempt.launch_started_at} />
                </dd>
              </>
            )}
            {attempt.handoff_state && (
              <>
                <dt>Handoff State</dt>
                <dd className="font-mono">{attempt.handoff_state}</dd>
              </>
            )}
            {detail?.settlement_status && (
              <>
                <dt>Settlement</dt>
                <dd>{statusLabel(detail.settlement_status)}</dd>
              </>
            )}
            {detail?.settled_at && (
              <>
                <dt>Settled</dt>
                <dd>
                  <ExactTime iso={detail.settled_at} />
                </dd>
              </>
            )}
            {detail?.settlement_detail?.reason && (
              <>
                <dt>Settlement Detail</dt>
                <dd className="whitespace-pre-wrap break-words">
                  {detail.settlement_detail.reason}
                </dd>
              </>
            )}
            {failureCode && (
              <>
                <dt>Failure Code</dt>
                <dd className="break-all font-mono">{failureCode}</dd>
              </>
            )}
            {failureText && (
              <>
                <dt>Failure Detail</dt>
                <dd className="whitespace-pre-wrap break-words">
                  {failureText}
                </dd>
              </>
            )}
            {recordedFailure && (
              <>
                <dt>Classification</dt>
                <dd>{recordedFailure.classification}</dd>
                <dt>Category</dt>
                <dd>{recordedFailure.category}</dd>
                <dt>Retryable</dt>
                <dd>{recordedFailure.retryable ? "Yes" : "No"}</dd>
              </>
            )}
          </dl>
        </>
      )}
    </div>
  );
}

function ExactTime({ iso }: { iso: string }) {
  return (
    <time dateTime={iso} title={timestampDescription(iso)}>
      {formatExactTime(iso)}
    </time>
  );
}
