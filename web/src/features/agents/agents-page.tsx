import { useMemo, useState } from "react";
import { useAgents, useRoles, type RunAgent } from "@/api/hooks";
import { WorkspacePage } from "@/components/workspace-page";
import { Button } from "@/components/ui/button";
import { Modal } from "@/components/ui/modal-shell";
import {
  ThemeIcon,
  RolePortrait,
  useRoleIdentity,
  useThemeCopy,
} from "@/theme";
import { isAgentRole, ROLE_HIERARCHY_ORDER } from "@/lib/utils";
import { agentAvailability } from "./agent-availability";

type RoleGroup = { id: string; description: string; agents: RunAgent[] };
export function AgentsPage() {
  const agentQuery = useAgents();
  const roleQuery = useRoles();
  const title = useThemeCopy("agents.heading");
  const [selectedID, setSelectedID] = useState<string | null>(null);
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
  return (
    <WorkspacePage
      section="Agents"
      title={title}
      description="Meet the agents and their responsibilities. Availability reflects all their work across this workspace."
    >
      {error && (
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
          group={selected}
          knowledge={availabilityKnowledge}
          onClose={() => setSelectedID(null)}
        />
      )}
    </WorkspacePage>
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
          style={{
            backgroundColor: `var(--theme-status-${availability.tone}-foreground)`,
          }}
        />
        {availability.label}
      </span>
    </button>
  );
}
function AgentDetails({
  group,
  knowledge,
  onClose,
}: {
  group: RoleGroup;
  knowledge: AvailabilityKnowledge;
  onClose: () => void;
}) {
  const identity = useRoleIdentity(group.id);
  const availability = knownAvailability(group.agents, knowledge);
  return (
    <Modal
      open
      onClose={onClose}
      title={identity.name}
      caption={identity.canonicalName}
      size="md"
    >
      <div className="space-y-6">
        <RolePortrait
          role={group.id}
          className="h-24 w-24 rounded-xl object-cover"
        />
        {identity.lore && (
          <p className="font-display text-2xl leading-relaxed text-foreground-muted">
            {identity.lore}
          </p>
        )}
        <section className="border-t border-border pt-5">
          <h3 className="text-sm font-medium">Responsibility</h3>
          <p className="mt-2 text-sm leading-relaxed text-foreground-muted">
            {group.description}
          </p>
        </section>
        <dl className="text-sm">
          <div className="flex items-center justify-between gap-4">
            <dt className="text-foreground-muted">Availability</dt>
            <dd>{availability.label}</dd>
          </div>
        </dl>
        <p className="text-xs text-foreground-muted">{availability.details}</p>
      </div>
    </Modal>
  );
}

type AvailabilityKnowledge = "known" | "loading" | "unknown";
function knownAvailability(
  agents: RunAgent[],
  knowledge: AvailabilityKnowledge,
) {
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
