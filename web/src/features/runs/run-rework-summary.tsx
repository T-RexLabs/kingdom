import type { RunDetailSummary } from "@/api/hooks";
import { isSkipReviewValidationWorkflowId } from "@/lib/workflows";

type ReworkRun = Pick<
  RunDetailSummary,
  | "workflow_id"
  | "run_until_all_approve"
  | "review_rework_cycles"
  | "validation_rework_waves"
  | "max_execution_review_cycles"
  | "max_validation_rework_cycles"
  | "review_auto_succeeded_count"
  | "validation_auto_succeeded_count"
>;

export function RunReworkSummary({ run }: { run: ReworkRun }) {
  return (
    <section aria-label="Review and validation rework" className="text-xs">
      {isSkipReviewValidationWorkflowId(run.workflow_id) ? (
        <p className="text-foreground-muted">
          Review and Validation{" "}
          <span className="ml-2 text-foreground">Skipped</span>
        </p>
      ) : (
        <dl className="flex flex-wrap gap-x-6 gap-y-2">
          <ReworkCount
            label="Review Rework"
            count={run.review_rework_cycles}
            cap={run.max_execution_review_cycles}
            uncapped={run.run_until_all_approve}
            autoSucceeded={run.review_auto_succeeded_count}
          />
          <ReworkCount
            label="Validation Rework"
            count={run.validation_rework_waves}
            cap={run.max_validation_rework_cycles}
            uncapped={run.run_until_all_approve}
            autoSucceeded={run.validation_auto_succeeded_count}
          />
        </dl>
      )}
    </section>
  );
}

function ReworkCount({
  label,
  count,
  cap,
  uncapped,
  autoSucceeded,
}: {
  label: string;
  count: number;
  cap?: number;
  uncapped?: boolean;
  autoSucceeded: number;
}) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
      <dt className="text-foreground-muted">{label}</dt>
      <dd className="flex flex-wrap items-baseline gap-x-2 gap-y-1 tabular-nums">
        <span
          title={
            uncapped
              ? "No rework cap; continues until approval or manual cancellation."
              : cap === undefined
                ? "The rework cap was not recorded."
                : `${count} rework requests; cap ${cap}.`
          }
        >
          {count}{" "}
          <span className="text-foreground-muted">
            {uncapped
              ? "· No Cap"
              : cap === undefined
                ? "· Cap Not Recorded"
                : `/ ${cap}`}
          </span>
        </span>
        {autoSucceeded > 0 && (
          <span className="text-status-blocked">
            {autoSucceeded} Auto-Succeeded at Cap
          </span>
        )}
      </dd>
    </div>
  );
}
