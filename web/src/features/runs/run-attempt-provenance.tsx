import type {
  RunExecutionDetail,
  RunNodeAttemptHistoryItem,
} from "@/api/hooks";
import { formatExactTime, timestampDescription } from "@/lib/date-time";
import { presentAttemptHistoryItem } from "./run-execution-presentation";

export function RunAttemptProvenance({
  detail,
  history,
  currentAttemptId,
}: {
  detail: RunExecutionDetail | null;
  history?: RunNodeAttemptHistoryItem;
  currentAttemptId?: string;
}) {
  const attempt = history ?? detail;
  if (!attempt) return null;
  return (
    <dl className="my-5 grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-xs text-foreground-muted">
      <dt>Attempt ID</dt>
      <dd className="break-all font-mono">{attempt.attempt_id}</dd>
      <dt>Started</dt>
      <dd>
        {attempt.started_at ? (
          <time
            dateTime={attempt.started_at}
            title={timestampDescription(attempt.started_at)}
          >
            {formatExactTime(attempt.started_at)}
          </time>
        ) : attempt.execution_mode === "runtime_owned" ? (
          "Not Applicable"
        ) : ["queued", "starting"].includes(attempt.status) ? (
          "Not Started"
        ) : (
          "Observation Not Recorded"
        )}
      </dd>
      {attempt.provider_invocation_id && (
        <>
          <dt>Invocation ID</dt>
          <dd className="break-all font-mono">
            {attempt.provider_invocation_id}
          </dd>
        </>
      )}
      {history?.token_usage && (
        <>
          <dt>Token Usage</dt>
          <dd>
            {history.token_usage.total_tokens} total (
            {history.token_usage.input_tokens} input,{" "}
            {history.token_usage.output_tokens} output)
          </dd>
        </>
      )}
      {history && (
        <>
          <dt>Lineage</dt>
          <dd className="space-y-1">
            <p>
              {history.superseded
                ? "Superseded"
                : history.attempt_id === currentAttemptId
                  ? "Current Attempt"
                  : "Historical Attempt"}
            </p>
            {presentAttemptHistoryItem(
              history,
              currentAttemptId,
            ).retryRelationships.map((relationship, index) => (
              <p key={index}>{relationship}</p>
            ))}
          </dd>
        </>
      )}
    </dl>
  );
}
