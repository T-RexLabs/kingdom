import { useState } from "react";
import {
  useWorkOrder,
  type Role,
  type RunSummary,
  type WorkOrderDetail,
} from "@/api/hooks";
import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import {
  ExternalSourceCoordinate,
  ExternalSourceStrip,
} from "@/features/connections/external-source-strip";
import {
  externalSourceFailureGuidance,
  externalSourceStatusNotice,
} from "@/features/connections/external-source-status";
import { ExternalSourceComparison } from "@/features/connections/external-source-comparison";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import { Loader2, RefreshCw } from "lucide-react";
import { WorkOrderSettings } from "./work-order-settings";

export function WorkOrderPanel({
  run,
  roles = [],
}: {
  run: RunSummary;
  roles?: Role[];
}) {
  const {
    data: workOrder,
    isLoading,
    error,
    refetch,
  } = useWorkOrder(run.work_order_id);
  const workOrderTitle = workOrder?.work_order_title?.trim();

  return (
    <div className="space-y-8">
      <section aria-label="Work order" className="space-y-5">
        <h2 className="font-display text-xl text-foreground">
          {workOrderTitle || "Work Order"}
        </h2>
        <WorkOrderContent
          text={workOrder?.work_order_text}
          sources={workOrder?.external_sources}
          isLoading={isLoading}
          hasError={Boolean(error) || !workOrder}
          onRefreshed={() => {
            void refetch();
          }}
        />
      </section>

      <WorkOrderSettings run={run} roles={roles} />
    </div>
  );
}

function WorkOrderContent({
  text,
  sources,
  isLoading,
  hasError,
  onRefreshed,
}: {
  text?: string;
  sources?: WorkOrderDetail["external_sources"];
  isLoading: boolean;
  hasError: boolean;
  onRefreshed: () => void;
}) {
  if (isLoading)
    return <p className="text-sm text-foreground-muted">Loading work order…</p>;
  if (hasError)
    return (
      <p role="alert" className="text-sm text-status-failed">
        Work order could not be loaded.{" "}
        <button type="button" className="underline" onClick={onRefreshed}>
          Retry Work Order
        </button>
      </p>
    );
  return (
    <div className="space-y-5">
      <MarkdownBlock
        content={text ?? ""}
        className="text-[15px] leading-7 text-foreground"
      />
      {sources && sources.length > 0 && (
        <section aria-label="Issued work order sources" className="space-y-3">
          <h3 className="text-sm font-medium text-foreground-muted">
            GitHub Sources
          </h3>
          {sources.map((source) => (
            <IssuedWorkOrderSourceCard
              key={source.link_id}
              source={source}
              onRefreshed={onRefreshed}
            />
          ))}
        </section>
      )}
    </div>
  );
}

type IssuedWorkOrderSource = WorkOrderDetail["external_sources"][number];

function IssuedWorkOrderSourceCard({ source, onRefreshed }: { source: IssuedWorkOrderSource; onRefreshed: () => void }) {
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState("");
  const [refreshResult, setRefreshResult] = useState<components["schemas"]["ExternalRefreshResult"]>();
  const recoverableRefresh = useRecoverableMutation(
    "work-order-source-refresh",
    (request: { linkId: string }, key) => apiFetch<components["schemas"]["ExternalRefreshResult"]>(`/external-source-links/${request.linkId}/refreshes`, {
      method: "POST",
      headers: { "Idempotency-Key": key },
    }),
    source.link_id,
  );
  const sourceStatus = refreshResult?.outcome ?? source.status;
  const latestSnapshotID = refreshResult?.link?.latest_snapshot_id ?? source.latest_snapshot_id;
  const updateAvailable = latestSnapshotID !== source.used_snapshot_id;
  const sourceFailureGuidance = externalSourceFailureGuidance(sourceStatus);
  // A source with no update and no problem says nothing; the card still shows
  // where the work order came from.
  const statusNotice = externalSourceStatusNotice(sourceStatus, updateAvailable);

  const refresh = async () => {
    setRefreshing(true);
    setRefreshError("");
    try {
      const result = await recoverableRefresh.mutateAsync({ linkId: source.link_id });
      setRefreshResult(result);
      onRefreshed();
    } catch (error) {
      setRefreshError(error instanceof Error ? error.message : "Could not check GitHub source");
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <ExternalSourceStrip
      label={`Issued source ${source.repository}#${source.issue_number}`}
      identity={<>Imported from GitHub · <ExternalSourceCoordinate canonicalURL={source.canonical_url} repository={source.repository} issueNumber={source.issue_number} /></>}
      actions={
        <Button type="button" size="sm" variant="ghost" aria-label={`${recoverableRefresh.hasPendingRecovery ? "Retry" : "Check"} GitHub source ${source.repository}#${source.issue_number}`} onClick={refresh} disabled={refreshing}>{refreshing ? <Loader2 className="mr-1 h-3 w-3 animate-spin" /> : <RefreshCw className="mr-1 h-3 w-3" />}{recoverableRefresh.hasPendingRecovery ? "Retry GitHub source check" : "Check GitHub source"}</Button>
      }
    >
      {statusNotice ? <p className="text-foreground-muted"><span className="text-foreground">{statusNotice}</span>{updateAvailable ? " — the issued work order remains unchanged." : ""}</p> : null}
      {sourceFailureGuidance ? <p role="status" className="text-status-failed">{sourceFailureGuidance}</p> : null}
      {refreshResult ? <p aria-live="polite" className="text-foreground-muted">{refreshResult.message}</p> : null}
      {updateAvailable ? <ExternalSourceComparison baselineId={source.used_snapshot_id} candidateId={latestSnapshotID} /> : null}
      {refreshResult?.retry_at ? <p className="text-foreground-muted">Retry after {new Date(refreshResult.retry_at).toLocaleString()}.</p> : null}
      {refreshError ? <p role="alert" className="text-status-failed">{refreshError}</p> : null}
    </ExternalSourceStrip>
  );
}
