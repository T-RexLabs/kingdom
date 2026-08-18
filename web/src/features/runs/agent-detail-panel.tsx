import { createElement, useMemo, useState } from "react";
import { Badge } from "@/components/ui/badge";
import {
  getRoleColor,
  getRoleGlowStyle,
  getRoleIcon,
  isAgentRole,
  roleTitle,
  ROLE_HIERARCHY_ORDER,
  AGENT_STATUS_ORDER,
  cn,
} from "@/lib/utils";
import type { AgentStatus } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import { Users } from "lucide-react";
import type { RunParticipant } from "@/api/hooks";

/* ── Types ── */

interface RoleGroup {
  roleId: string;
  agents: RunParticipant[];
}

/* ── Helpers ── */

function buildRoleGroups(agents: RunParticipant[]): RoleGroup[] {
  const byRole = new Map<string, RunParticipant[]>();

  for (const agent of agents) {
    const role = agent.role_id;
    const list = byRole.get(role);
    if (list) {
      list.push(agent);
    } else {
      byRole.set(role, [agent]);
    }
  }

  // Sort agents within each group by spawn_depth (ascending), then display_name
  for (const list of byRole.values()) {
    list.sort((a, b) => {
      const depthDiff = a.spawn_depth - b.spawn_depth;
      if (depthDiff !== 0) return depthDiff;
      return a.display_name.localeCompare(b.display_name);
    });
  }

  // Sort groups by role hierarchy
  const groups: RoleGroup[] = [];
  for (const [roleId, agentsList] of byRole) {
    groups.push({ roleId, agents: agentsList });
  }
  groups.sort((a, b) => {
    const aOrder = ROLE_HIERARCHY_ORDER[a.roleId] ?? 99;
    const bOrder = ROLE_HIERARCHY_ORDER[b.roleId] ?? 99;
    return aOrder - bOrder;
  });

  return groups;
}

/* ── Status Summary Bar ── */

function StatusSummaryBar({ agents }: { agents: RunParticipant[] }) {
  const counts = useMemo(() => {
    const result: Record<string, number> = {};
    for (const status of AGENT_STATUS_ORDER) {
      result[status] = 0;
    }
    for (const agent of agents) {
      result[agent.status] = (result[agent.status] ?? 0) + 1;
    }
    return result;
  }, [agents]);

  return (
    <div className="flex items-center gap-3 rounded-md border border-border bg-surface px-3 py-1.5">
      <div className="flex items-center gap-1.5 text-xs text-foreground-muted">
        <Users className="h-3.5 w-3.5" />
        <span className="font-medium">{agents.length}</span>
        <span>agents</span>
      </div>
      <div className="h-3.5 w-px bg-border" />
      {AGENT_STATUS_ORDER.map((status) => {
        const count = counts[status];
        if (count === 0) return null;
        return (
          <div key={status} className="flex items-center gap-1.5 text-xs text-foreground-muted">
            <span className="text-xs text-foreground-muted">
              {count} {statusLabel(status)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/* ── Role Group Section ── */

function RoleGroupSection({
  group,
  description,
  isExpanded,
  onToggle,
}: {
  group: RoleGroup;
  description?: string;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const roleColorClass = getRoleColor(group.roleId);
  const hasBusyAgent = group.agents.some((a) => a.status === "busy");
  const statusCounts = group.agents.reduce<Record<string, number>>((acc, agent) => {
    acc[agent.status] = (acc[agent.status] ?? 0) + 1;
    return acc;
  }, {});

  return (
    <div
      className={cn(
        "rounded-lg border border-border bg-surface",
        hasBusyAgent && "animate-glow-pulse-subtle",
      )}
      style={hasBusyAgent ? getRoleGlowStyle(group.roleId) : undefined}
    >
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full flex-wrap items-center gap-x-2.5 gap-y-1.5 px-3 py-2 text-left"
      >
        {createElement(getRoleIcon(group.roleId), {
          className: cn("h-4 w-4 shrink-0", roleColorClass, hasBusyAgent && "animate-glow-pulse"),
        })}
        <span className={cn("text-sm font-semibold text-wrap-anywhere", roleColorClass)}>
          {roleTitle(group.roleId)}
        </span>
        <span className="text-xs text-foreground-subtle">
          {group.agents.length} {group.agents.length === 1 ? "agent" : "agents"}
        </span>

        <div className="flex flex-wrap items-center gap-2 text-xs text-foreground-subtle sm:ml-auto">
          {AGENT_STATUS_ORDER.map((status) => {
            const count = statusCounts[status];
            if (!count) return null;
            return (
              <Badge key={status} variant="status" status={status as AgentStatus}>
                {statusLabel(status)}
                {count > 1 ? ` ${count}` : ""}
              </Badge>
            );
          })}
        </div>
      </button>

      <div
        data-testid={`role-description-${group.roleId}`}
        aria-hidden={!isExpanded}
        className={cn(
          "grid transition-[grid-template-rows,opacity] duration-200 ease-out",
          isExpanded ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0",
        )}
      >
        <div
          className={cn(
            "overflow-hidden px-3 text-xs text-foreground-subtle",
            isExpanded ? "border-t border-border py-2" : "py-0",
          )}
        >
          {description || "Role description unavailable."}
        </div>
      </div>
    </div>
  );
}

/* ── Main Panel ── */

export function AgentDetailPanel({
  agents,
  roleDescriptions,
}: {
  agents: RunParticipant[];
  roleDescriptions?: Record<string, string>;
}) {
  const visibleAgents = useMemo(
    () => agents.filter((agent) => isAgentRole(agent.role_id)),
    [agents],
  );
  const groups = useMemo(() => buildRoleGroups(visibleAgents), [visibleAgents]);
  const [expandedRoleId, setExpandedRoleId] = useState<string | null>(null);

  if (visibleAgents.length === 0) {
    return (
      <div className="flex h-64 items-center justify-center text-sm text-foreground-subtle">
        No agents spawned yet.
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <StatusSummaryBar agents={visibleAgents} />

      <div className="space-y-1.5">
        {groups.map((group) => (
          <RoleGroupSection
            key={group.roleId}
            group={group}
            description={roleDescriptions?.[group.roleId]}
            isExpanded={expandedRoleId === group.roleId}
            onToggle={() => {
              setExpandedRoleId((current) => (current === group.roleId ? null : group.roleId));
            }}
          />
        ))}
      </div>
    </div>
  );
}
