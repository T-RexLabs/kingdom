import { useEffect, useMemo, useState } from "react";
import {
  useAgents,
  useAgentConfiguration,
  useRoles,
  useUpdateAgentRoleConfiguration,
  useResetAgentRoleGuidance,
  useUpdateWorkspaceAgentVoice,
  type AgentConfiguration,
  type AgentRoleConfiguration,
  type RunAgent,
  type UpdateAgentRoleConfigurationRequest,
  type UpdateWorkspaceAgentVoiceRequest,
} from "@/api/hooks";
import type { components } from "@/api/schema";
import { WorkspacePage } from "@/components/workspace-page";
import { Button } from "@/components/ui/button";
import { SelectField, type SelectOption } from "@/components/ui/select-field";
import { InfoTooltip } from "@/components/ui/info-tooltip";
import { Modal } from "@/components/ui/modal-shell";
import { newIdempotencyKey } from "@/lib/idempotency";
import {
  ThemeIcon,
  ThemeArtwork,
  RolePortrait,
  useRoleIdentity,
  useTheme,
  useThemeCopy,
} from "@/theme";
import { isAgentRole, ROLE_HIERARCHY_ORDER } from "@/lib/utils";
import { agentAvailability } from "./agent-availability";

type RoleGroup = { id: string; description: string; agents: RunAgent[] };
type AgentVoiceSelection = components["schemas"]["AgentVoiceSelection"];
type VoiceOption = SelectOption & { selection?: AgentVoiceSelection };

export function AgentsPage() {
  const agentQuery = useAgents();
  const roleQuery = useRoles();
  const configurationQuery = useAgentConfiguration();
  const { catalog: themeCatalog, refreshCatalog } = useTheme();
  const title = useThemeCopy("agents.heading");
  const [selectedID, setSelectedID] = useState<string | null>(null);
  useEffect(() => {
    void refreshCatalog();
  }, [refreshCatalog]);

  const groups = useMemo(() => {
    const result = new Map<string, RoleGroup>();
    for (const role of roleQuery.data?.items ?? []) {
      if (isAgentRole(role.role_id))
        result.set(role.role_id, {
          id: role.role_id,
          description: role.description,
          agents: [],
        });
    }
    for (const agent of agentQuery.data?.items ?? []) {
      if (!isAgentRole(agent.role_id)) continue;
      const group = result.get(agent.role_id) ?? {
        id: agent.role_id,
        description: "This role is registered by the runtime.",
        agents: [],
      };
      group.agents.push(agent);
      result.set(agent.role_id, group);
    }
    return [...result.values()].sort(
      (a, b) =>
        (ROLE_HIERARCHY_ORDER[a.id] ?? 99) -
          (ROLE_HIERARCHY_ORDER[b.id] ?? 99) || a.id.localeCompare(b.id),
    );
  }, [roleQuery.data, agentQuery.data]);
  const availabilityKnowledge = agentQuery.data
    ? "known"
    : agentQuery.isLoading
      ? "loading"
      : "unknown";
  const selected = groups.find((group) => group.id === selectedID);
  const loading = agentQuery.isLoading || roleQuery.isLoading;
  const error = agentQuery.error || roleQuery.error;
  const voiceOptions = useMemo(
    () => buildVoiceOptions(themeCatalog?.items ?? [], configurationQuery.data),
    [themeCatalog, configurationQuery.data],
  );
  const selectedRoleConfiguration = configurationQuery.data?.roles.find(
    (role) => role.role_id === selectedID,
  );

  return (
    <WorkspacePage
      section="Agents"
      title={title}
      description="Meet the agents and their responsibilities"
      actions={
        <WorkspaceVoiceSettings
          configuration={configurationQuery.data}
          loading={configurationQuery.isLoading}
          error={configurationQuery.error}
          options={voiceOptions}
          onRetry={() => void configurationQuery.refetch()}
        />
      }
    >
      {!!error && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-status-failed/40 p-4 text-sm"
        >
          <p>
            Agent availability could not be refreshed.{" "}
            {agentQuery.data
              ? "Showing the last known availability."
              : "Availability is unknown until the registry can be loaded."}
          </p>
          <Button
            variant="outline"
            size="sm"
            className="mt-3"
            onClick={() => {
              void agentQuery.refetch();
              void roleQuery.refetch();
            }}
          >
            Try Again
          </Button>
        </div>
      )}
      {loading && groups.length === 0 ? (
        <p role="status" className="text-sm text-foreground-muted">
          Loading agents…
        </p>
      ) : groups.length === 0 && !error ? (
        <div className="rounded-xl border border-dashed border-border px-6 py-12 text-center">
          <h2 className="text-lg">No Agents Yet</h2>
          <p className="mt-2 text-sm text-foreground-muted">
            Agents will appear when the runtime registers its roles.
          </p>
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {groups.map((group) => (
            <AgentCard
              key={group.id}
              group={group}
              knowledge={availabilityKnowledge}
              onSelect={() => setSelectedID(group.id)}
            />
          ))}
        </div>
      )}
      {selected && (
        <AgentDetails
          key={selected.id}
          group={selected}
          configuration={selectedRoleConfiguration}
          promptCatalogDigest={configurationQuery.data?.prompt_catalog_digest}
          workspaceVoice={configurationQuery.data?.workspace.voice}
          settingsLoading={configurationQuery.isLoading}
          settingsError={configurationQuery.error}
          voiceOptions={voiceOptions}
          onRetrySettings={() => void configurationQuery.refetch()}
          onClose={() => setSelectedID(null)}
        />
      )}
    </WorkspacePage>
  );
}

function WorkspaceVoiceSettings({
  configuration,
  loading,
  error,
  options,
  onRetry,
}: {
  configuration?: AgentConfiguration;
  loading: boolean;
  error: unknown;
  options: VoiceOption[];
  onRetry: () => void;
}) {
  const mutation = useUpdateWorkspaceAgentVoice();
  const [draft, setDraft] = useState<{ value: string; instruction: string; expectedRowVersion: number } | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const currentValue = configuration
    ? voiceOptionValue(configuration.workspace.voice, "workspace")
    : "neutral-v1";
  const selectedValue = draft?.value ?? currentValue;
  const currentInstruction = configuration?.workspace.voice.instruction ?? "";
  const hasUnsavedChanges = Boolean(
    draft && (
      draft.value !== currentValue ||
      (draft.value === "custom:workspace" &&
        draft.instruction !== (currentValue === "custom:workspace" ? currentInstruction : ""))
    ),
  );
  const workspaceOptions: VoiceOption[] = [
    ...options,
    { label: "Custom", value: "custom:workspace", selection: { kind: "custom" } },
  ];

  async function save() {
    if (!draft || !hasUnsavedChanges) return;
    const voice = parseVoiceOption(draft.value, workspaceOptions, draft.instruction);
    if (!voice) {
      setSaveError("This theme voice is no longer available. Reload the theme list and try again.");
      return;
    }
    const body: UpdateWorkspaceAgentVoiceRequest = {
      expected_row_version: draft.expectedRowVersion,
      voice,
    };
    setSaveError(null);
    try {
      await mutation.mutateAsync({ body, key: newIdempotencyKey("agent-workspace-voice") });
      setDraft(null);
    } catch (caught) {
      setSaveError(messageForSettingsError(caught));
    }
  }

  return (
    <div className="flex w-full min-w-0 flex-col gap-2 sm:w-[22rem]">
      <div className="flex items-center gap-1.5">
        <label htmlFor="workspace-agent-voice" className="text-xs font-medium">Communication Style</label>
        <InfoTooltip
          label="About the workspace communication style"
          message="This style applies to new runs unless an agent has its own override."
        />
      </div>
      <SelectField
        id="workspace-agent-voice"
        ariaLabel="Communication Style"
        value={selectedValue}
        options={workspaceOptions}
        className="w-full min-w-0"
        disabled={loading || !configuration || mutation.isPending}
        onChange={(value) => {
          if (!configuration) return;
          setSaveError(null);
          const sameSelection = value === currentValue;
          setDraft({
            value,
            instruction: sameSelection ? configuration.workspace.voice.instruction ?? "" : "",
            expectedRowVersion: configuration.workspace.row_version,
          });
        }}
      />
      {selectedValue === "custom:workspace" && (
        <textarea
          aria-label="Workspace Custom Communication Style"
          data-ui-field=""
          className="min-h-20 w-full min-w-0 resize-y rounded-lg border border-border bg-surface px-3 py-2 text-xs leading-relaxed text-foreground"
          maxLength={2000}
          value={draft?.instruction ?? configuration?.workspace.voice.instruction ?? ""}
          disabled={mutation.isPending || loading}
          onChange={(event) => {
            if (!configuration) return;
            setSaveError(null);
            const instruction = event.currentTarget.value;
            setDraft((current) => ({
              value: "custom:workspace",
              instruction,
              expectedRowVersion: current?.expectedRowVersion ?? configuration.workspace.row_version,
            }));
          }}
        />
      )}
      {hasUnsavedChanges && (
        <div className="flex justify-end gap-2">
          <Button variant="outline" size="sm" onClick={() => setDraft(null)} disabled={mutation.isPending}>
            Cancel
          </Button>
          <Button size="sm" onClick={() => void save()} disabled={mutation.isPending || loading || (selectedValue === "custom:workspace" && !draft?.instruction.trim())}>
            {mutation.isPending ? "Saving…" : "Save"}
          </Button>
        </div>
      )}
      {loading && <p role="status" className="w-full text-right text-xs text-foreground-muted">Loading agent settings…</p>}
      {!!error && (
        <div role="alert" className="w-full rounded-lg border border-status-failed/40 p-2 text-xs">
          <span>Agent settings could not be loaded.</span>
          <Button variant="outline" size="sm" onClick={onRetry}>Try Again</Button>
        </div>
      )}
      {saveError && (
        <div role="alert" className="w-full rounded-lg border border-status-failed/40 p-2 text-xs">
          <span>{saveError}</span>
          {saveError.includes("another window") && (
            <Button variant="outline" size="sm" onClick={() => { setDraft(null); onRetry(); }}>Reload Settings</Button>
          )}
        </div>
      )}
    </div>
  );
}

function AgentCard({
  group,
  knowledge,
  onSelect,
}: {
  group: RoleGroup;
  knowledge: AvailabilityKnowledge;
  onSelect: () => void;
}) {
  const identity = useRoleIdentity(group.id);
  const availability = knownAvailability(group.agents, knowledge);
  return (
    <button
      type="button"
      onClick={onSelect}
      className="ui-card group flex min-h-60 flex-col rounded-xl border border-border bg-surface p-6 text-left transition-colors hover:border-accent/60 hover:bg-surface-raised"
      aria-label={`About ${identity.name}`}
    >
      <ThemeArtwork slot="agentBackdrop" className="mb-5 h-24 w-full rounded-lg object-cover" />
      <span className="mb-5 flex h-11 w-11 items-center justify-center rounded-xl bg-accent/10 text-accent">
        <ThemeIcon slot={`role.${group.id}`} className="h-5 w-5" />
      </span>
      <h2 className="font-display text-3xl leading-tight">{identity.name}</h2>
      {identity.name !== identity.canonicalName && (
        <p className="mt-1 text-xs text-accent">{identity.canonicalName}</p>
      )}
      <p className="mb-6 mt-3 text-sm leading-relaxed text-foreground-muted">
        {identity.description}
      </p>
      <span
        className="mt-auto inline-flex items-center gap-2 text-xs text-foreground-muted"
        title={availability.details}
      >
        <span
          aria-hidden="true"
          className="h-1.5 w-1.5 rounded-full"
          style={{ backgroundColor: `var(--theme-status-${availability.tone}-foreground)` }}
        />
        {availability.label}
      </span>
    </button>
  );
}

function AgentDetails({
  group,
  configuration,
  promptCatalogDigest,
  workspaceVoice,
  settingsLoading,
  settingsError,
  voiceOptions,
  onRetrySettings,
  onClose,
}: {
  group: RoleGroup;
  configuration?: AgentRoleConfiguration;
  promptCatalogDigest?: string;
  workspaceVoice?: components["schemas"]["AgentVoice"];
  settingsLoading: boolean;
  settingsError: unknown;
  voiceOptions: VoiceOption[];
  onRetrySettings: () => void;
  onClose: () => void;
}) {
  const identity = useRoleIdentity(group.id);
  const mutation = useUpdateAgentRoleConfiguration();
  const resetMutation = useResetAgentRoleGuidance();
  const [draft, setDraft] = useState<RoleDraft | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const currentDraft = draft ?? roleDraftFromConfiguration(configuration, promptCatalogDigest);
  const workspaceStyle = workspaceVoice ?? configuration?.effective_voice;
  const styleOptions: VoiceOption[] = [
    {
      label: "Use Workspace Style",
      selectedLabel: workspaceStyle
        ? communicationStyleLabel(workspaceStyle, voiceOptions)
        : "Neutral · neutral-v1",
      value: "workspace",
    },
    ...voiceOptions,
    { label: "Custom", value: "custom:role", selection: { kind: "custom" } },
  ];

  function updateDraft(patch: Partial<RoleDraft>) {
    setSaveError(null);
    setDraft((current) => ({ ...(current ?? roleDraftFromConfiguration(configuration, promptCatalogDigest)), ...patch }));
  }

  async function save() {
    if (!configuration || !draft) return;
    const body: UpdateAgentRoleConfigurationRequest = {
      expected_row_version: draft.expectedRowVersion,
      expected_prompt_catalog_digest: draft.expectedPromptCatalogDigest,
      guidance_override: draft.guidanceOverride,
      voice_override: null,
    };
    if (draft.voiceValue !== "workspace") {
      const override = parseVoiceOption(draft.voiceValue, styleOptions, draft.voiceInstruction);
      if (!override) {
        setSaveError("This communication style is no longer available. Reload the theme list and try again.");
        return;
      }
      body.voice_override = override;
    }
    setSaveError(null);
    try {
      await mutation.mutateAsync({
        roleId: group.id,
        body,
        key: newIdempotencyKey(`agent-role-${group.id}`),
      });
      setDraft(null);
    } catch (caught) {
      setSaveError(messageForSettingsError(caught));
    }
  }

  async function reset() {
    if (!configuration) return;
    setSaveError(null);
    const current = draft ?? roleDraftFromConfiguration(configuration, promptCatalogDigest);
    if (configuration.guidance_override === null) {
      setDraft({
        ...current,
        guidanceText: configuration.default_guidance,
        guidanceOverride: null,
      });
      return;
    }
    try {
      const updated = await resetMutation.mutateAsync({
        roleId: group.id,
        body: {
          expected_row_version: current.expectedRowVersion,
          expected_prompt_catalog_digest: current.expectedPromptCatalogDigest,
        },
        key: newIdempotencyKey(`agent-role-reset-${group.id}`),
      });
      setDraft({
        ...current,
        expectedRowVersion: updated.row_version,
        guidanceText: updated.effective_guidance,
        guidanceOverride: null,
      });
    } catch (caught) {
      setSaveError(messageForSettingsError(caught));
    }
  }

  return (
    <Modal open onClose={onClose} title={identity.name} caption={identity.canonicalName} size="md">
      <div className="space-y-6">
        <RolePortrait role={group.id} className="h-24 w-24 rounded-xl object-cover" />
        {identity.lore && (
          <p className="font-display text-2xl leading-relaxed text-foreground-muted">{identity.lore}</p>
        )}
        <section className="border-t border-border pt-5">
          <h3 className="text-sm font-medium">Responsibility</h3>
          <p className="mt-2 text-sm leading-relaxed text-foreground-muted">{group.description}</p>
        </section>
        <section className="space-y-4 border-t border-border pt-5">
          {!!settingsError && (
            <div role="alert" className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-status-failed/40 p-3 text-sm">
              <span>Agent settings could not be loaded.</span>
              <Button variant="outline" size="sm" onClick={onRetrySettings}>Try Again</Button>
            </div>
          )}
          {!settingsError && !configuration && (
            <p role="status" className="text-sm text-foreground-muted">Loading agent settings…</p>
          )}
          {configuration && (
            <>
              <div>
                <div className="mb-2 flex items-center gap-1.5">
                  <label className="text-sm font-medium" htmlFor={`agent-guidance-${group.id}`}>
                    Agent Guidance
                  </label>
                  <InfoTooltip
                    label="About agent guidance"
                    message={agentGuidanceHelp(group.id)}
                  />
                </div>
                <textarea
                  id={`agent-guidance-${group.id}`}
                  data-ui-field=""
                  className="min-h-36 w-full resize-y rounded-lg border border-border bg-surface px-3 py-2 text-sm font-normal leading-relaxed text-foreground"
                  maxLength={8000}
                  value={currentDraft.guidanceText}
                  disabled={mutation.isPending || resetMutation.isPending || settingsLoading}
                  onChange={(event) => updateDraft({ guidanceText: event.currentTarget.value, guidanceOverride: event.currentTarget.value })}
                />
              </div>
              <div className="space-y-3">
                <SelectField
                  id={`agent-tone-${group.id}`}
                  label="Communication Style"
                  value={currentDraft.voiceValue}
                  options={styleOptions}
                  disabled={mutation.isPending || settingsLoading || resetMutation.isPending}
                  onChange={(value) => updateDraft({
                    voiceValue: value,
                    voiceInstruction: value === "custom:role"
                      ? currentDraft.voiceValue === "custom:role"
                        ? currentDraft.voiceInstruction
                        : configuration.voice_override?.source.kind === "custom"
                          ? configuration.voice_override.instruction ?? ""
                          : ""
                      : "",
                  })}
                />
                {currentDraft.voiceValue === "workspace" && (
                  <p className="text-xs text-foreground-muted">Using workspace style</p>
                )}
                {currentDraft.voiceValue === "custom:role" && (
                  <textarea
                    aria-label="Custom communication style for this agent"
                    data-ui-field=""
                    className="min-h-24 w-full resize-y rounded-lg border border-border bg-surface px-3 py-2 text-sm leading-relaxed text-foreground"
                    maxLength={2000}
                    value={currentDraft.voiceInstruction}
                    disabled={mutation.isPending || settingsLoading || resetMutation.isPending}
                    onChange={(event) => updateDraft({ voiceInstruction: event.currentTarget.value })}
                  />
                )}
              </div>
              <div className="flex flex-wrap justify-end gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => { void reset(); }}
                  disabled={mutation.isPending || resetMutation.isPending || (
                    configuration.guidance_override === null && currentDraft.guidanceText === configuration.default_guidance
                  )}
                >
                  Reset Agent Guidance
                </Button>
                <Button size="sm" onClick={() => void save()} disabled={!draft || mutation.isPending || resetMutation.isPending || (currentDraft.voiceValue === "custom:role" && !currentDraft.voiceInstruction.trim())}>
                  {mutation.isPending ? "Saving…" : "Save Settings"}
                </Button>
              </div>
              {(mutation.isPending || resetMutation.isPending) && <p role="status" className="text-xs text-foreground-muted">Saving agent settings…</p>}
              {saveError && (
                <div role="alert" className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-status-failed/40 p-3 text-sm">
                  <span>{saveError}</span>
                  {saveError.includes("another window") && (
                    <Button variant="outline" size="sm" onClick={() => { setDraft(null); onRetrySettings(); }}>Reload Settings</Button>
                  )}
                </div>
              )}
            </>
          )}
        </section>
      </div>
    </Modal>
  );
}

type RoleDraft = {
  expectedRowVersion: number;
  expectedPromptCatalogDigest: string;
  guidanceText: string;
  guidanceOverride: string | null;
  voiceValue: string;
  voiceInstruction: string;
};

function roleDraftFromConfiguration(
  configuration?: AgentRoleConfiguration,
  promptCatalogDigest?: string,
): RoleDraft {
  return {
    expectedRowVersion: configuration?.row_version ?? 0,
    expectedPromptCatalogDigest: promptCatalogDigest ?? "",
    guidanceText: configuration?.effective_guidance ?? "",
    guidanceOverride: configuration?.guidance_override ?? null,
    voiceValue: configuration?.voice_override
      ? voiceOptionValue(configuration.voice_override, "role")
      : "workspace",
    voiceInstruction: configuration?.voice_override?.source.kind === "custom"
      ? configuration.voice_override.instruction ?? ""
      : "",
  };
}

function buildVoiceOptions(
  themes: NonNullable<ReturnType<typeof useTheme>["catalog"]>["items"],
  configuration?: AgentConfiguration,
): VoiceOption[] {
  const options: VoiceOption[] = [{ label: "Neutral · neutral-v1", value: "neutral-v1", selection: { kind: "neutral-v1" } }];
  const seen = new Set<string>(["neutral-v1"]);
  for (const item of themes) {
    if (!item.agentVoice?.trim()) continue;
    const selection: AgentVoiceSelection = { kind: "theme", theme_id: item.id, theme_digest: item.resolvedDigest };
    const value = voiceOptionValue({ source: selection });
    if (seen.has(value)) continue;
    seen.add(value);
    options.push({ label: `${item.name} · ${item.version}`, value, selection });
  }
  const selectedVoices = [
    configuration?.workspace.voice,
    ...(configuration?.roles ?? []).map((role) => role.voice_override),
  ];
  for (const voice of selectedVoices) {
    if (!voice || voice.source.kind !== "theme") continue;
    const value = voiceOptionValue(voice);
    if (seen.has(value)) continue;
    seen.add(value);
    options.push({
      label: `${voice.source.theme_id ?? "Theme"} · Pinned Revision`,
      value,
      selection: {
        kind: "theme",
        theme_id: voice.source.theme_id,
        theme_digest: voice.source.theme_digest,
      },
    });
  }
  return options;
}

function voiceOptionValue(voice?: components["schemas"]["AgentVoice"], customScope: "workspace" | "role" = "workspace"): string {
  if (!voice || voice.source.kind === "neutral-v1") return "neutral-v1";
  if (voice.source.kind === "custom") return customScope === "role" ? "custom:role" : "custom:workspace";
  return `theme:${voice.source.theme_id ?? ""}:${voice.source.theme_digest ?? ""}`;
}

function parseVoiceOption(value: string, options: VoiceOption[], instruction = ""): AgentVoiceSelection | undefined {
  const selection = options.find((option) => option.value === value)?.selection;
  if (!selection) return undefined;
  if (selection.kind === "custom") return { ...selection, instruction };
  return selection;
}

function communicationStyleLabel(
  voice: components["schemas"]["AgentVoice"],
  options: VoiceOption[],
): string {
  if (voice.source.kind === "custom") return "Custom - Workspace";
  const selected = options.find((option) => option.value === voiceOptionValue(voice));
  return selected?.label ?? "Neutral · neutral-v1";
}

function agentGuidanceHelp(roleID: string): string {
  if (roleID === "governance") {
    return "Governance decisions are recorded by Kingdom without a model session. These settings do not change plan approval or final acceptance.";
  }
  return roleID === "plan-author"
    ? "Guidance and communication style changes apply to new runs and planning sessions. Guidance sets the agent’s working approach; communication style affects phrasing."
    : "Guidance and communication style changes apply to new runs. Guidance sets the agent’s working approach; communication style affects phrasing.";
}

function messageForSettingsError(error: unknown): string {
  const message = error instanceof Error ? error.message : "Agent settings could not be saved.";
  return message.includes("changed in another window")
    ? "Agent settings changed in another window. Reload the latest settings before saving."
    : message;
}

type AvailabilityKnowledge = "known" | "loading" | "unknown";
function knownAvailability(agents: RunAgent[], knowledge: AvailabilityKnowledge) {
  if (knowledge === "known") return agentAvailability(agents);
  return {
    label: knowledge === "loading" ? "Loading" : "Unknown",
    tone: "neutral",
    details:
      knowledge === "loading"
        ? "Loading agent availability…"
        : "Agent availability could not be loaded.",
  };
}
