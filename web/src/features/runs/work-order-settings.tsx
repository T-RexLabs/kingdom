import {
  useProviderModels,
  type ProviderModel,
  type Role,
  type RunModelSelectionEntry,
  type RunSummary,
} from "@/api/hooks";
import { modelDisplayLabel } from "@/features/work-orders/provider-model-selection";
import { roleTitle } from "@/lib/utils";
import { isSkipReviewValidationWorkflowId } from "@/lib/workflows";

export function WorkOrderSettings({
  run,
  roles,
}: {
  run: RunSummary;
  roles: Role[];
}) {
  const models = useProviderModels();
  const providerModels = models.data?.items ?? [];
  const overrides = getOrderedRoleOverrides(
    run.model_selection?.role_overrides,
    roles,
  );
  const reviewFlow = isSkipReviewValidationWorkflowId(run.workflow_id)
    ? "Review and Validation Skipped"
    : run.run_until_all_approve
      ? "Until All Approve"
      : "Capped Review and Validation";

  return (
    <section
      aria-label="Work order settings"
      className="space-y-6 border-t border-border/60 pt-6"
    >
      <h2 className="font-display text-xl text-foreground">
        Work Order Settings
      </h2>
      <dl className="grid gap-x-8 gap-y-5 sm:grid-cols-3">
        <Setting
          label="Approval"
          value={
            run.autonomous_approvals === false
              ? "Requires Your Approval"
              : "Autonomous"
          }
        />
        <Setting label="Review Flow" value={reviewFlow} />
        <Setting
          label="Publish Target"
          value={
            run.publish_mode === "git_worktree"
              ? "Git Worktree"
              : "Direct to Project"
          }
        />
      </dl>
      <div className="space-y-4">
        <h3 className="text-sm font-medium text-foreground">Agent Models</h3>
        {models.isLoading && (
          <p className="text-sm text-foreground-muted">
            Resolving model names…
          </p>
        )}
        {models.error && (
          <p role="alert" className="text-sm text-foreground-muted">
            Model names could not be loaded. Recorded selections are shown.{" "}
            <button
              type="button"
              className="underline underline-offset-4 hover:text-foreground"
              onClick={() => void models.refetch()}
            >
              Retry Model Names
            </button>
          </p>
        )}
        <dl className="space-y-4">
          <ModelSelectionRow
            label="Default Model"
            selection={run.model_selection?.global}
            providerModels={providerModels}
          />
          {overrides.map((override) => (
            <ModelSelectionRow
              key={override.roleId}
              label={override.roleName}
              selection={override.selection}
              providerModels={providerModels}
            />
          ))}
        </dl>
        {overrides.length === 0 && (
          <p className="text-sm text-foreground-muted">
            All agents use the default model.
          </p>
        )}
      </div>
    </section>
  );
}

function Setting({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 space-y-1">
      <dt className="text-xs text-foreground-muted">{label}</dt>
      <dd className="text-sm text-foreground">{value}</dd>
    </div>
  );
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
  const modelID = selection?.model?.trim() ?? "";
  const model = providerModels.find((item) => item.id === modelID);
  const thinking = selection?.thinking_effort?.trim();
  const effort = thinking
    ? thinking.charAt(0).toUpperCase() + thinking.slice(1)
    : "Model Default";
  return (
    <div className="grid min-w-0 gap-x-6 gap-y-1 sm:grid-cols-[minmax(0,1fr)_minmax(0,2fr)]">
      <dt className="text-sm text-foreground-muted">{label}</dt>
      <dd className="min-w-0 space-y-1">
        <p
          className="break-words text-sm text-foreground"
          title={model ? modelID : undefined}
        >
          {model ? modelDisplayLabel(model) : modelID || "Not Recorded"}
        </p>
        <p className="text-xs text-foreground-muted">Thinking: {effort}</p>
      </dd>
    </div>
  );
}

function getOrderedRoleOverrides(
  roleOverrides: Record<string, RunModelSelectionEntry> | undefined,
  roles: Role[],
) {
  const roleMeta = new Map(roles.map((role) => [role.role_id, role]));
  return Object.entries(roleOverrides ?? {})
    .map(([roleId, selection]) => {
      const role = roleMeta.get(roleId);
      return {
        roleId,
        roleName: role?.display_name ?? roleTitle(roleId),
        selection,
        sectionOrder:
          role?.agent_model_selector_section_order ?? Number.MAX_SAFE_INTEGER,
        roleOrder:
          role?.agent_model_selector_role_order ?? Number.MAX_SAFE_INTEGER,
      };
    })
    .sort(
      (left, right) =>
        left.sectionOrder - right.sectionOrder ||
        left.roleOrder - right.roleOrder ||
        left.roleName.localeCompare(right.roleName),
    );
}
