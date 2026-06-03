import { type ReactNode, useState } from "react";
import {
  useDecree,
  useProviderModels,
  type ProviderModel,
  type Role,
  type RunModelSelectionEntry,
  type RunSummary,
} from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { ModalDialog } from "@/components/ui/modal-dialog";
import { ModalShell, modalShellSizeClass } from "@/components/ui/modal-shell";
import { modelDisplayLabel } from "@/features/decrees/provider-model-selection";
import { cn, roleTitle } from "@/lib/utils";
import { isSkipReviewValidationWorkflowId } from "@/lib/workflows";
import { ChevronDown, ChevronRight, Scroll, Settings2 } from "lucide-react";

export function DecreePanel({ run, roles = [] }: { run: RunSummary; roles?: Role[] }) {
  const [isOpen, setIsOpen] = useState(true);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const { data: decree, isLoading, error } = useDecree(run.decree_id);
  const providerModelsQuery = useProviderModels({ enabled: settingsOpen });
  const providerModels = providerModelsQuery.data?.items ?? [];

  return (
    <>
      <section className="rounded-md border border-border bg-surface-raised/40 p-3">
        <div className="rounded-md border border-border bg-background p-3">
          <div className="flex flex-wrap items-start gap-2">
            <button
              type="button"
              onClick={() => setIsOpen(!isOpen)}
              className="flex min-w-0 flex-1 items-center justify-between gap-2 text-left"
            >
              <div className="flex min-w-0 items-center gap-2">
                {isOpen ? <ChevronDown className="h-3.5 w-3.5 shrink-0 text-foreground-subtle" /> : <ChevronRight className="h-3.5 w-3.5 shrink-0 text-foreground-subtle" />}
                <Scroll className="h-4 w-4 shrink-0 text-gold" />
                <h2 className="truncate text-sm font-semibold text-foreground">Royal Decree</h2>
              </div>
            </button>

            <div className="ml-auto flex items-center">
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
          </div>

          {isOpen && (
            <div className="mt-3">
              {isLoading ? (
                <p className="text-sm text-foreground-subtle">Loading decree...</p>
              ) : error || !decree ? (
                <p className="text-sm text-foreground-subtle">Decree not available.</p>
              ) : (
                <div className="space-y-2">
                  <h3 className="text-sm font-semibold text-gold">{decree.decree_title}</h3>
                  <div className="max-h-[30vh] overflow-auto rounded-md border border-border bg-background p-3">
                    <MarkdownBlock content={decree.decree_text} className="text-sm text-foreground-muted" />
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </section>

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
    .map(({ sectionOrder: _sectionOrder, roleOrder: _roleOrder, ...override }) => override);
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
