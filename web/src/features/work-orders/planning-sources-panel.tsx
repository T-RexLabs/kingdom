import { apiFetch } from "@/api/client";
import type { PlanningSessionDetail } from "@/api/hooks";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { Modal, ModalFooter } from "@/components/ui/modal-shell";
import { PopoverSurface } from "@/components/ui/popover-surface";
import { ExternalSourceComparison } from "@/features/connections/external-source-comparison";
import { externalSourceFailureGuidance, externalSourceStatusNotice } from "@/features/connections/external-source-status";
import { safeGitHubIssueURL } from "@/features/connections/github-url";
import { IssuePicker, type IssueSelection } from "@/features/connections/issue-picker";
import { usePopoverFloating, usePopoverOpenState } from "@/lib/use-popover-floating";
import { cn } from "@/lib/utils";
import { useInteractions } from "@floating-ui/react";
import { ChevronDown, ExternalLink, Github, Loader2, RefreshCw, Trash2 } from "lucide-react";
import { useCallback, useState } from "react";
import { usePlanningMutationJournal } from "./use-planning-mutation-journal";

type PlanningExternalSource = PlanningSessionDetail["external_sources"][number];

type ExternalRefreshResult = components["schemas"]["ExternalRefreshResult"];

type SourceAction = "refresh" | "adopt" | "detach";
type SourceRequest = { action: SourceAction; linkId: string; label: string };
type SourceResult = ExternalRefreshResult | components["schemas"]["ExternalAdoptionResult"] | components["schemas"]["ExternalDetachmentResult"];
const sourceActionPath: Record<SourceAction, string> = { refresh: "refreshes", adopt: "adoptions", detach: "detachments" };

async function sendSourceRequest(request: SourceRequest, key: string): Promise<SourceResult> {
  return apiFetch(`/external-source-links/${encodeURIComponent(request.linkId)}/${sourceActionPath[request.action]}`, {
    method: "POST", headers: { "Idempotency-Key": key },
  });
}

/** The session owns unresolved source requests even when a detached row disappears. */
export function PlanningSourcesPanel({
  attachmentPending = false,
  workspace,
  sessionId,
  sources,
  onAttach,
  onChanged,
}: {
  attachmentPending?: boolean;
  workspace: string;
  sessionId: string;
  sources: PlanningExternalSource[];
  onAttach: (value: IssueSelection) => Promise<void>;
  onChanged: () => Promise<unknown>;
}) {
  const [importing, setImporting] = useState(false);
  const [reviewing, setReviewing] = useState<PlanningExternalSource>();
  const [resolving, setResolving] = useState(false);
  const [recoveryError, setRecoveryError] = useState("");
  const sourceJournal = usePlanningMutationJournal("planning-source-write", sendSourceRequest, JSON.stringify([workspace, sessionId]));
  const mutateSource = async (action: SourceAction, source: PlanningExternalSource) => {
    setRecoveryError("");
    return sourceJournal.mutateAsync({ action, linkId: source.link_id, label: sourceLabel(source) });
  };
  const recoverSource = async () => {
    setResolving(true);
    setRecoveryError("");
    try { await sourceJournal.retry(); await onChanged(); }
    catch (error) { setRecoveryError(error instanceof Error ? error.message : "The source request could not be resolved."); }
    finally { setResolving(false); }
  };
  const readSourceState = async () => {
    setResolving(true);
    setRecoveryError("");
    try { await onChanged(); }
    catch (error) { setRecoveryError(error instanceof Error ? error.message : "Source state could not be loaded."); }
    finally { setResolving(false); }
  };
  const updateCount = sources.filter(hasUpdate).length;
  // A source Kingdom can no longer read is not a detail. The panel is closed by
  // default, so the trigger has to carry that news itself or a broken source
  // would go unseen until someone happened to open it.
  const failedCount = sources.filter((source) => externalSourceFailureGuidance(source.status)).length;

  const { open, setOpen, handleOpenChange } = usePopoverOpenState();
  const {
    context,
    setReference,
    setPanel,
    modalOwned,
    panelStyle,
    baseInteractions,
  } = usePopoverFloating({
    open,
    onOpenChange: handleOpenChange,
    role: "dialog",
    placement: "bottom-end",
    widthMode: "css",
    maxHeight: 420,
    // Both the import dialog and the update review open over this panel. Left
    // dismissable, the first outside press that reached them would close the
    // list the operator is about to come back to.
    dismissDisabled: importing || reviewing !== undefined,
  });
  const { getReferenceProps, getFloatingProps } = useInteractions(baseInteractions);

  const attach = useCallback(async (value: IssueSelection) => {
    if (sourceJournal.hasPendingRecovery) throw new Error("Resolve the retained source request before adding another source.");
    await onAttach(value);
  }, [onAttach, sourceJournal.hasPendingRecovery]);

  return (
    <div className="min-w-0 max-w-xs flex-1 basis-52">
      <span id="planning-sources-label" className={fieldLabelClass}>
        Planning Sources
      </span>
      <button
        {...getReferenceProps({
          id: "planning-sources-trigger",
          type: "button",
          // Names itself from the field label plus its own summary, so the
          // control announces "Planning Sources, 2 linked, 1 update" rather
          // than a bare count with no idea what is being counted.
          "aria-labelledby": "planning-sources-label planning-sources-trigger",
        })}
        ref={setReference}
        className={cn(fieldClass, "flex items-center gap-2 text-left hover:border-border/80")}
      >
        <span className={cn("truncate", sources.length === 0 && "text-foreground-subtle")}>
          {sources.length === 0 ? "None linked" : `${sources.length} linked`}
        </span>
        <span className="ml-auto flex shrink-0 items-center gap-2">
          {failedCount > 0 ? (
            <span className="rounded-full border border-status-failed/40 bg-status-failed/10 px-2 py-0.5 text-[10px] text-status-failed">
              {failedCount} unreadable
            </span>
          ) : updateCount > 0 ? (
            <span className="rounded-full border border-accent/30 bg-accent/10 px-2 py-0.5 text-[10px] text-accent">
              {updateCount} update{updateCount === 1 ? "" : "s"}
            </span>
          ) : null}
          <ChevronDown className={cn("h-3.5 w-3.5 text-foreground-subtle transition-transform", open && "rotate-180")} />
        </span>
      </button>
      {open ? (
        <PopoverSurface
          context={context}
          panelRef={setPanel}
          style={panelStyle}
          modalOwned={modalOwned}
          floatingProps={getFloatingProps({ id: "planning-sources-popover" })}
          initialFocus={-1}
          className="flex w-[264px] max-w-[calc(100vw_-_16px)] flex-col overflow-hidden rounded-lg border border-border bg-background shadow-xl sm:w-[380px]"
        >
          <div aria-label="Planning Sources" className="flex min-h-0 flex-col">
            <fieldset disabled={attachmentPending || sourceJournal.hasPendingRecovery} className="shrink-0 border-b border-border p-2">
              <IssuePicker
                onSelect={attach}
                actionLabel="Add Source"
                triggerLabel={sources.length > 0 ? "Add Source" : "Add GitHub Issue"}
                compact
                onOpenChange={setImporting}
              />
            </fieldset>
            {sources.length === 0 ? (
              <p className="p-3 text-xs text-foreground-muted">
                Add a GitHub issue as context for planner operations.
              </p>
            ) : (
              <ul className="min-h-0 flex-1 divide-y divide-border/60 overflow-y-auto">
                {sources.map((source) => (
                  <li key={source.link_id}>
                    <PlanningSourceRow
                      source={source}
                      onReviewUpdate={() => setReviewing(source)}
                      onChanged={onChanged}
                      onMutate={mutateSource}
                      recoveryPending={attachmentPending || sourceJournal.hasPendingRecovery}
                    />
                  </li>
                ))}
              </ul>
            )}
          </div>
        </PopoverSurface>
      ) : null}
      {sourceJournal.pendingRequest && (
        <div role="status" className="mt-3 space-y-2 rounded-lg border border-status-warning/30 bg-status-warning/5 p-3 text-xs">
          <p>The {sourceJournal.pendingRequest.action} request for {sourceJournal.pendingRequest.label} has an unresolved outcome. Resolve this exact request before changing linked sources.</p>
          {sourceJournal.recoveryBlocked && <p>The authentication binding changed. Reading current source state does not retry or resolve the original request.</p>}
          <details><summary className="cursor-pointer font-medium">Request Details</summary><p className="mt-2 break-all">{sourceJournal.pendingKey}</p><p className="mt-1 break-all">Source ID: {sourceJournal.pendingRequest.linkId}</p></details>
          <div className="flex flex-wrap gap-2"><Button type="button" variant="outline" size="sm" disabled={resolving} onClick={() => void recoverSource()}>Recover Source Request</Button><Button type="button" variant="ghost" size="sm" disabled={resolving} onClick={() => void readSourceState()}>Refresh Source State</Button></div>
          {recoveryError && <p role="alert" className="text-status-failed">{recoveryError}</p>}
        </div>
      )}
      {reviewing ? (
        <UpdateReviewDialog
          source={reviewing}
          onClose={() => setReviewing(undefined)}
          onMutate={mutateSource}
          recoveryPending={attachmentPending || sourceJournal.hasPendingRecovery}
          onAdopted={async () => {
            setReviewing(undefined);
            await onChanged();
            // Adopting from a panel that is no longer open would leave the
            // operator staring at a trigger whose badge silently changed.
            setOpen(true);
          }}
        />
      ) : null}
    </div>
  );
}

function hasUpdate(source: PlanningExternalSource) {
  const active = source.active_snapshot_id ?? source.used_snapshot_id;
  return source.latest_snapshot_id !== active;
}

function sourceLabel(source: PlanningExternalSource) {
  return `${source.repository}#${source.issue_number}`;
}

function PlanningSourceRow({
  source,
  onReviewUpdate,
  onChanged,
  onMutate,
  recoveryPending,
}: {
  source: PlanningExternalSource;
  onReviewUpdate: () => void;
  onChanged: () => Promise<unknown>;
  onMutate: (action: SourceAction, source: PlanningExternalSource) => Promise<SourceResult>;
  recoveryPending: boolean;
}) {
  const [busy, setBusy] = useState<"refresh" | "detach" | "">("");
  const [error, setError] = useState("");
  const [confirmingRemoval, setConfirmingRemoval] = useState(false);
  const [refreshResult, setRefreshResult] = useState<ExternalRefreshResult>();
  const activeSnapshotID = source.active_snapshot_id ?? source.used_snapshot_id;
  const latestSnapshotID = refreshResult?.link?.latest_snapshot_id ?? source.latest_snapshot_id;
  const sourceStatus = refreshResult?.outcome ?? source.status;
  const updateAvailable = latestSnapshotID !== activeSnapshotID;
  const failureGuidance = externalSourceFailureGuidance(sourceStatus);
  const sourceURL = safeGitHubIssueURL(source.canonical_url, source.repository, source.issue_number);
  const label = sourceLabel(source);

  // The status is a label from Kingdom's status vocabulary, so it is rendered as
  // one rather than folded into a sentence. A live refresh result speaks for
  // itself and replaces it, and a source with nothing to report says nothing.
  const statusText = refreshResult?.message ?? externalSourceStatusNotice(sourceStatus, updateAvailable);

  const run = async (action: "refresh" | "detach", failure: string) => {
    setBusy(action);
    setError("");
    try {
      const result = await onMutate(action, source);
      if (action === "refresh" && "outcome" in result) setRefreshResult(result);
      await onChanged();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : failure);
    } finally {
      setBusy("");
    }
  };

  return (
    <section aria-label={`Linked GitHub source ${label}`} className="px-2 py-2">
      <div className="flex min-w-0 items-center gap-2">
        <Github className="h-3.5 w-3.5 shrink-0 text-accent" />
        <span className="min-w-0 flex-1 truncate text-xs font-medium text-foreground">{label}</span>
        <div className="flex shrink-0 items-center gap-1">
          {sourceURL ? (
            <a
              href={sourceURL}
              target="_blank"
              rel="noopener noreferrer"
              aria-label={`Open ${label} on GitHub`}
              className="rounded p-1.5 text-foreground-subtle hover:text-accent"
            >
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          ) : null}
          <button
            type="button"
            aria-label={`Refresh ${label}`}
            disabled={busy !== "" || recoveryPending}
            className="rounded p-1.5 text-foreground-subtle hover:text-accent disabled:opacity-50"
            onClick={() => void run("refresh", "Could not refresh GitHub source")}
          >
            {busy === "refresh" ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
          </button>
          <button
            type="button"
            aria-label={`Remove ${label}`}
            disabled={busy !== "" || recoveryPending || confirmingRemoval}
            className="rounded p-1.5 text-foreground-subtle hover:text-status-failed disabled:opacity-50"
            onClick={() => setConfirmingRemoval(true)}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
      {statusText ? (
        <p className={cn("mt-1 pl-6 text-xs", failureGuidance ? "text-status-failed" : updateAvailable ? "text-accent" : "text-foreground-muted")}>
          {statusText}
        </p>
      ) : null}
      {/* A diff needs room to be read, which a popover does not have. The
          review opens where it can be read, and adopting happens there. */}
      {updateAvailable ? (
        <div className="mt-1.5 pl-6">
          <Button type="button" size="sm" variant="outline" disabled={busy !== "" || recoveryPending} onClick={onReviewUpdate}>
            Review Update
          </Button>
        </div>
      ) : null}
      {/* Confirmation is the two buttons. What Remove does to a planning source
          is plain from the control that opened them, so the row does not
          re-explain it; the labels name the target for anyone who cannot see
          which row is asking. */}
      {confirmingRemoval ? (
        <div className="mt-1.5 flex justify-end gap-2 rounded-md border border-border bg-surface-raised/40 px-2 py-1.5">
          <Button type="button" size="sm" variant="ghost" aria-label={`Keep ${label}`} disabled={busy !== "" || recoveryPending} onClick={() => setConfirmingRemoval(false)}>
            Cancel
          </Button>
          <Button
            type="button"
            size="sm"
            variant="destructive"
            aria-label={`Confirm removing ${label}`}
            disabled={busy !== "" || recoveryPending}
            onClick={() => void run("detach", "Could not remove the GitHub source")}
          >
            {busy === "detach" ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
            Remove
          </Button>
        </div>
      ) : null}
      {error ? (
        <p role="alert" className="mt-1.5 pl-6 text-xs text-status-failed">
          {error}
        </p>
      ) : null}
      {failureGuidance && !refreshResult ? (
        <p role="status" className="mt-1.5 pl-6 text-xs text-status-failed">
          {failureGuidance}
        </p>
      ) : null}
      {/* Rate-limit timing is not a detail either: the operator still has to
          know when retrying is worth it. */}
      {refreshResult?.retry_at ? (
        <p className="mt-1.5 pl-6 text-xs text-foreground-muted">
          Retry after {new Date(refreshResult.retry_at).toLocaleString()}.
        </p>
      ) : null}
    </section>
  );
}

/** Where a snapshot diff gets the room a popover cannot give it. */
function UpdateReviewDialog({
  source,
  onClose,
  onAdopted,
  onMutate,
  recoveryPending,
}: {
  source: PlanningExternalSource;
  onClose: () => void;
  onAdopted: () => Promise<void>;
  onMutate: (action: SourceAction, source: PlanningExternalSource) => Promise<SourceResult>;
  recoveryPending: boolean;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const label = sourceLabel(source);
  const activeSnapshotID = source.active_snapshot_id ?? source.used_snapshot_id;

  const adopt = async () => {
    setBusy(true);
    setError("");
    try {
      await onMutate("adopt", source);
      await onAdopted();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not adopt the latest GitHub source");
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      open
      onClose={onClose}
      title="Review Source Update"
      caption={label}
      description="Adopting uses the refreshed issue in future planner operations. Messages already sent keep the source they were sent with."
      size="md"
      footer={
        <ModalFooter>
          <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>Cancel</Button>
          <Button type="button" onClick={() => void adopt()} disabled={busy || recoveryPending}>
            {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
            Adopt Update
          </Button>
        </ModalFooter>
      }
    >
      <ExternalSourceComparison baselineId={activeSnapshotID} candidateId={source.latest_snapshot_id} />
      {error ? <p role="alert" className="mt-3 text-xs text-status-failed">{error}</p> : null}
    </Modal>
  );
}
