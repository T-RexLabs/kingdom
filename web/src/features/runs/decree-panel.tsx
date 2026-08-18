import { type ReactNode, useState } from "react";
import {
  useDecree,
  useProviderModels,
  type ProviderModel,
  type Role,
  type RunModelSelectionEntry,
  type RunSummary,
  type DecreeDetail,
} from "@/api/hooks";
import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { ModalDialog } from "@/components/ui/modal-dialog";
import { ModalShell, modalShellSizeClass } from "@/components/ui/modal-shell";
import { modelDisplayLabel } from "@/features/decrees/provider-model-selection";
import { safeGitHubIssueURL } from "@/features/connections/github-url";
import { externalSourceFailureGuidance, externalSourceStatusLabel } from "@/features/connections/external-source-status";
import { ExternalSourceComparison } from "@/features/connections/external-source-comparison";
import { cn, roleTitle } from "@/lib/utils";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import { isSkipReviewValidationWorkflowId } from "@/lib/workflows";
import { ChevronRight, ExternalLink, Loader2, RefreshCw, Scroll, Settings2 } from "lucide-react";

export function DecreePanel({ run, roles = [] }: { run: RunSummary; roles?: Role[] }) {
  const [readOpen, setReadOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const { data: decree, isLoading, error, refetch } = useDecree(run.decree_id);
  const providerModelsQuery = useProviderModels({ enabled: settingsOpen });
  const providerModels = providerModelsQuery.data?.items ?? [];
  const decreeTitle = decree?.decree_title?.trim();

  return (
    <>
      <section className="rounded-md border border-border bg-surface-raised/40 p-3">
        <div className="flex items-center gap-2 rounded-md border border-border bg-background p-3">
          <button
            type="button"
            onClick={() => setReadOpen(true)}
            className="flex min-w-0 flex-1 items-center gap-2 text-left transition-colors hover:text-gold"
            aria-label="Read royal decree"
          >
            <Scroll className="h-4 w-4 shrink-0 text-gold" />
            <div className="min-w-0">
              <h2 className="truncate text-sm font-semibold text-foreground">Royal Decree</h2>
              <p className="truncate text-xs text-foreground-subtle">
                {decreeTitle ?? (isLoading ? "Loading decree…" : "View the decree")}
              </p>
            </div>
            <ChevronRight className="ml-auto h-3.5 w-3.5 shrink-0 text-foreground-subtle" />
          </button>

          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="h-8 w-8 shrink-0"
            aria-label="Open decree settings"
            title="Decree settings"
            onClick={() => setSettingsOpen(true)}
          >
            <Settings2 className="h-4 w-4" />
          </Button>
        </div>
      </section>

      <DecreeReadDialog
        open={readOpen}
        title={decreeTitle}
        text={decree?.decree_text}
        sources={decree?.external_sources}
        isLoading={isLoading}
        hasError={Boolean(error) || !decree}
        onRefreshed={() => { void refetch(); }}
        onClose={() => setReadOpen(false)}
      />

      <DecreeSettingsDialog
        open={settingsOpen}
        run={run}
        roles={roles}
        providerModels={providerModels}
        providerModelsLoading={providerModelsQuery.isLoading}
        onClose={() => setSettingsOpen(false)}
      />
    </>
  );
}

function DecreeReadDialog({
  open,
  title,
  text,
  sources,
  isLoading,
  hasError,
  onClose,
  onRefreshed,
}: {
  open: boolean;
  title?: string;
  text?: string;
  sources?: DecreeDetail["external_sources"];
  isLoading: boolean;
  hasError: boolean;
  onClose: () => void;
  onRefreshed: () => void;
}) {
  return (
    <ModalDialog
      open={open}
      onClose={onClose}
      ariaLabel="Royal decree"
      panelClassName={cn("w-full", modalShellSizeClass("lg"))}
    >
      <ModalShell title={title && title !== "" ? title : "Royal Decree"} caption="Royal Decree" onClose={onClose}>
        <div className="min-h-0 flex-1 overflow-y-auto">
          {isLoading ? (
            <p className="text-sm text-foreground-subtle">Loading decree…</p>
          ) : hasError ? (
            <p className="text-sm text-foreground-subtle">Decree not available.</p>
          ) : (
            <div>
              {sources && sources.length > 0 ? (
                <section aria-label="Issued decree sources" className="mb-4 space-y-2">
                  <p className="text-[10px] font-medium uppercase tracking-[0.18em] text-foreground-subtle">{sources.length} GitHub {sources.length === 1 ? "source" : "sources"}</p>
                  <div className="grid gap-3 lg:grid-cols-2">
                    {sources.map((source) => <IssuedDecreeSourceCard key={source.link_id} source={source} onRefreshed={onRefreshed} />)}
                  </div>
                </section>
              ) : null}
              <MarkdownBlock content={text ?? ""} className="text-sm text-foreground-muted" />
            </div>
          )}
        </div>
      </ModalShell>
    </ModalDialog>
  );
}

type IssuedDecreeSource = DecreeDetail["external_sources"][number];

function IssuedDecreeSourceCard({ source, onRefreshed }: { source: IssuedDecreeSource; onRefreshed: () => void }) {
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState("");
  const [refreshResult, setRefreshResult] = useState<components["schemas"]["ExternalRefreshResult"]>();
  const recoverableRefresh = useRecoverableMutation(
    "decree-source-refresh",
    (request: { linkId: string }, key) => apiFetch<components["schemas"]["ExternalRefreshResult"]>(`/external-source-links/${request.linkId}/refreshes`, {
      method: "POST",
      headers: { "Idempotency-Key": key },
    }),
    source.link_id,
  );
  const sourceURL = safeGitHubIssueURL(source.canonical_url, source.repository, source.issue_number);
  const sourceStatus = refreshResult?.outcome ?? source.status;
  const latestSnapshotID = refreshResult?.link?.latest_snapshot_id ?? source.latest_snapshot_id;
  const updateAvailable = latestSnapshotID !== source.used_snapshot_id;
  const sourceFailureGuidance = externalSourceFailureGuidance(sourceStatus);

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
    <section aria-label={`Issued source ${source.repository}#${source.issue_number}`} className="min-w-0 space-y-2 rounded-md border border-gold/25 bg-gold/5 p-3 text-xs">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="font-medium text-foreground">Imported from GitHub · {sourceURL ? <a href={sourceURL} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-gold underline">{source.repository}#{source.issue_number}<ExternalLink className="h-3 w-3" /></a> : <span>{source.repository}#{source.issue_number}</span>}</p>
          <p className="mt-1 text-foreground-muted"><span className="text-foreground">{externalSourceStatusLabel(sourceStatus, updateAvailable)}</span>{updateAvailable ? " — the issued decree remains unchanged." : ""}</p>
        </div>
        <Button type="button" size="sm" variant="ghost" aria-label={`${recoverableRefresh.hasPendingRecovery ? "Retry" : "Check"} GitHub source ${source.repository}#${source.issue_number}`} onClick={refresh} disabled={refreshing}>{refreshing ? <Loader2 className="mr-1 h-3 w-3 animate-spin" /> : <RefreshCw className="mr-1 h-3 w-3" />}{recoverableRefresh.hasPendingRecovery ? "Retry GitHub source check" : "Check GitHub source"}</Button>
      </div>
      {sourceFailureGuidance ? <p role="status" className="text-status-failed">{sourceFailureGuidance}</p> : null}
      {refreshResult ? <p aria-live="polite" className="text-foreground-muted">{refreshResult.message}</p> : null}
      {updateAvailable ? <ExternalSourceComparison baselineId={source.used_snapshot_id} candidateId={latestSnapshotID} /> : null}
      {refreshResult?.retry_at ? <p className="text-foreground-muted">Retry after {new Date(refreshResult.retry_at).toLocaleString()}.</p> : null}
      {refreshError ? <p role="alert" className="text-status-failed">{refreshError}</p> : null}
    </section>
  );
}

function DecreeSettingsDialog({
  open,
  run,
  roles,
  providerModels,
  providerModelsLoading,
  onClose,
}: {
  open: boolean;
  run: RunSummary;
  roles: Role[];
  providerModels: ProviderModel[];
  providerModelsLoading: boolean;
  onClose: () => void;
}) {
  const skipReviewAndValidation = isSkipReviewValidationWorkflowId(run.workflow_id);
  const roleOverrides = getOrderedRoleOverrides(run.model_selection?.role_overrides, roles);
  const globalSelection = run.model_selection?.global;
  const maxReviewCycles = run.max_execution_review_cycles ?? 0;
  const maxValidationCycles = run.max_validation_rework_cycles ?? 0;
  const showLimits = !skipReviewAndValidation && (run.run_until_all_approve || maxReviewCycles > 0 || maxValidationCycles > 0);

  return (
    <ModalDialog
      open={open}
      onClose={onClose}
      ariaLabel="Decree settings"
      panelClassName={cn("w-full", modalShellSizeClass("md"))}
    >
			<ModalShell title="Decree Settings" caption="Run Configuration" description={run.decree_title} onClose={onClose}>
	        <div data-testid="decree-settings-scroll-region" className="min-h-0 flex-1 space-y-6 overflow-y-auto">
            <SettingsSection title="How It Runs" description="Workflow, approval mode, review flow, and publish target.">
              <SettingsSurface>
                <SettingRow label="Approval">
                  {run.autonomous_approvals === false ? "Requires your approval" : "Autonomous"}
                </SettingRow>
                <SettingRow label="Review Flow">
                  {describeReviewFlow(run.workflow_id, !!run.run_until_all_approve)}
                </SettingRow>
                <SettingRow label="Publish Target">
                  {formatPublishTarget(run.publish_mode)}
                </SettingRow>
              </SettingsSurface>
            </SettingsSection>

            {showLimits ? (
              <SettingsSection title="Limits" description="Cycle caps applied to review and validation for this run.">
                <SettingsSurface>
                  {run.run_until_all_approve ? (
                    <SettingRow label="Review and Validation">
                      No cap
                    </SettingRow>
                  ) : (
                    <>
                      {maxReviewCycles > 0 ? (
                        <SettingRow label="Max Review Cycles">
                          {String(maxReviewCycles)}
                        </SettingRow>
                      ) : null}
                      {maxValidationCycles > 0 ? (
                        <SettingRow label="Max Validation Cycles">
                          {String(maxValidationCycles)}
                        </SettingRow>
                      ) : null}
                    </>
                  )}
                </SettingsSurface>
              </SettingsSection>
            ) : null}

            <SettingsSection
              title="Models"
              description="Default model selection and any role-specific overrides recorded for this run."
            >
              <div className="space-y-4">
                {providerModelsLoading ? (
                  <p className="text-xs text-foreground-subtle">Resolving model labels...</p>
                ) : null}

                <SettingsSurface>
                  <ModelSelectionRow
                    label="Default for all agents"
                    selection={globalSelection}
                    providerModels={providerModels}
                  />
                </SettingsSurface>

                <div className="space-y-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-foreground-subtle">Role Overrides</p>
                  <SettingsSurface>
                    {roleOverrides.length > 0 ? (
                      roleOverrides.map((override) => (
                        <ModelSelectionRow
                          key={override.roleId}
                          label={override.roleName}
                          selection={override.selection}
                          providerModels={providerModels}
                        />
                      ))
                    ) : (
                      <EmptyStateRow>No role overrides recorded. All roles used the default selection.</EmptyStateRow>
                    )}
                  </SettingsSurface>
                </div>
              </div>
	            </SettingsSection>
	          </div>
			</ModalShell>
    </ModalDialog>
  );
}

function SettingsSection({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <section className="space-y-3">
      <div className="space-y-1">
        <h4 className="text-sm font-semibold text-foreground">{title}</h4>
        {description ? <p className="text-sm text-foreground-muted">{description}</p> : null}
      </div>
      {children}
    </section>
  );
}

function SettingsSurface({ children }: { children: ReactNode }) {
  return (
    <div className="overflow-hidden rounded-lg border border-border/70 bg-background/50 divide-y divide-border/60">
      {children}
    </div>
  );
}

function SettingRow({
  label,
  children,
  mono = false,
}: {
  label: string;
  children: ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="flex flex-col gap-1 px-4 py-3 sm:flex-row sm:items-center sm:justify-between sm:gap-6">
      <p className="text-xs font-medium uppercase tracking-wide text-foreground-subtle">{label}</p>
      <div className={mono ? "text-sm font-mono text-foreground sm:text-right" : "text-sm text-foreground sm:text-right"}>
        {children}
      </div>
    </div>
  );
}

function EmptyStateRow({ children }: { children: ReactNode }) {
  return <p className="px-4 py-3 text-sm text-foreground-subtle">{children}</p>;
}

function ModelSelectionRow({
  label,
  selection,
  providerModels,
}: {
  label: string;
  selection?: RunModelSelectionEntry;
  providerModels: ProviderModel[];
}) {
  const modelSummary = formatModelSelection(selection, providerModels);

  return (
    <div className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-start sm:justify-between sm:gap-6">
      <div className="min-w-0">
        <p className="text-sm font-medium text-foreground">{label}</p>
      </div>

      <div className="space-y-1 sm:max-w-[60%] sm:text-right">
        <p className="text-sm text-foreground">{modelSummary.primary}</p>
        {modelSummary.secondary ? (
          <p className="font-mono text-xs text-foreground-subtle">{modelSummary.secondary}</p>
        ) : null}
        <p className="text-xs text-foreground-subtle">Thinking: {formatThinkingEffort(selection?.thinking_effort)}</p>
      </div>
    </div>
  );
}

function getOrderedRoleOverrides(
  roleOverrides: Record<string, RunModelSelectionEntry> | undefined,
  roles: Role[],
): Array<{ roleId: string; roleName: string; selection: RunModelSelectionEntry }> {
  const entries = Object.entries(roleOverrides ?? {});
  if (entries.length === 0) {
    return [];
  }

  const roleMeta = new Map(
    roles.map((role) => [
      role.role_id,
        {
          roleName: role.display_name,
          sectionOrder: role.agent_model_selector_section_order,
          roleOrder: role.agent_model_selector_role_order,
        },
      ] as const),
  );

  return entries
    .map(([roleId, selection]) => {
      const meta = roleMeta.get(roleId);
      return {
        roleId,
        roleName: meta?.roleName ?? roleTitle(roleId),
        selection,
        sectionOrder: meta?.sectionOrder ?? Number.MAX_SAFE_INTEGER,
        roleOrder: meta?.roleOrder ?? Number.MAX_SAFE_INTEGER,
      };
    })
    .sort((left, right) => {
      if (left.sectionOrder !== right.sectionOrder) {
        return left.sectionOrder - right.sectionOrder;
      }
      if (left.roleOrder !== right.roleOrder) {
        return left.roleOrder - right.roleOrder;
      }
      return left.roleName.localeCompare(right.roleName);
    })
    .map((override) => ({
      roleId: override.roleId,
      roleName: override.roleName,
      selection: override.selection,
    }));
}

function formatModelSelection(selection: RunModelSelectionEntry | undefined, providerModels: ProviderModel[]): {
  primary: string;
  secondary?: string;
} {
  const modelId = selection?.model?.trim() ?? "";
  if (modelId === "") {
    return { primary: "Not recorded" };
  }

  const providerModel = providerModels.find((model) => model.id === modelId);
  if (!providerModel) {
    return { primary: modelId };
  }

  return {
    primary: modelDisplayLabel(providerModel),
    secondary: modelDisplayLabel(providerModel) !== modelId ? modelId : undefined,
  };
}

function formatThinkingEffort(thinkingEffort: string | undefined): string {
  const trimmed = thinkingEffort?.trim() ?? "";
  if (trimmed === "") {
    return "Model default";
  }
  return trimmed.charAt(0).toUpperCase() + trimmed.slice(1);
}

function describeReviewFlow(
  workflowId: string,
  runUntilAllApprove: boolean,
): string {
  if (isSkipReviewValidationWorkflowId(workflowId)) {
    return "Skip reviews and validations";
  }
  if (runUntilAllApprove) {
    return "Run until all approve";
  }
  return "Standard review flow";
}

function formatPublishTarget(publishMode: RunSummary["publish_mode"]): string {
  return publishMode === "git_worktree" ? "Git worktree" : "Direct to project";
}
