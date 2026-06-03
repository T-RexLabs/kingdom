import { useMemo } from "react";
import { useAgents, usePendingGovernanceRuns, useRoles, useRuns, type RunAgent, type Role } from "@/api/hooks";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { HeartbeatIndicator } from "@/components/heartbeat-indicator";
import {
  capitalize,
  cn,
  getRoleColor,
  getRoleGlowStyle,
  getRoleIcon,
  isAgentRole,
  ROLE_HIERARCHY_ORDER,
  AGENT_STATUS_ORDER,
  AGENT_STATUS_DOT_COLORS,
} from "@/lib/utils";
import type { AgentStatus } from "@/lib/utils";
import { Users, Shield } from "lucide-react";
import { PageHeader, PageContent } from "@/components/page-layout";

function StatusSummaryBar({
  agents,
  totalRoles,
  hasActiveRun,
}: {
  agents: RunAgent[];
  totalRoles: number;
  hasActiveRun: boolean;
}) {
  const counts = useMemo(() => {
    const result: Record<AgentStatus, number> = {
      busy: 0,
      idle: 0,
      blocked: 0,
      offline: 0,
      failed: 0,
    };
    for (const agent of agents) {
      result[agent.status as AgentStatus] =
        (result[agent.status as AgentStatus] ?? 0) + 1;
    }
    return result;
  }, [agents]);

  const assignedRoleCount = useMemo(() => {
    return new Set(agents.map((a) => a.role_id)).size;
  }, [agents]);

  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-border bg-surface px-4 py-3">
      <div className="flex items-center gap-1.5 text-xs text-foreground-muted">
        <Shield className="h-3.5 w-3.5" />
        <span className="font-medium">{totalRoles}</span>
        <span>roles</span>
      </div>
      <div className="hidden h-4 w-px bg-border md:block" />
      <div className="flex items-center gap-1.5 text-xs text-foreground-muted">
        <Users className="h-3.5 w-3.5" />
        <span className="font-medium">{agents.length}</span>
        <span>{hasActiveRun ? "agents active" : "agents registered"}</span>
      </div>
      {!hasActiveRun && (
        <>
          <div className="hidden h-4 w-px bg-border md:block" />
          <span className="text-xs text-foreground-subtle">No active run</span>
        </>
      )}
      {assignedRoleCount < totalRoles && (
        <>
          <div className="hidden h-4 w-px bg-border md:block" />
          <span className="text-xs text-foreground-subtle">
            {totalRoles - assignedRoleCount} roles awaiting assignment
          </span>
        </>
      )}
      {agents.length > 0 && (
        <>
          <div className="hidden h-4 w-px bg-border md:block" />
          {AGENT_STATUS_ORDER.map((status) => {
            const count = counts[status];
            if (count === 0) return null;
            return (
              <div key={status} className="flex items-center gap-1.5">
                <span className={cn("h-2 w-2 rounded-full", AGENT_STATUS_DOT_COLORS[status])} />
                <span className="text-xs text-foreground-muted">
                  {count} {capitalize(status)}
                </span>
              </div>
            );
          })}
        </>
      )}
    </div>
  );
}

interface RoleGroup {
  roleId: string;
  roleName: string;
  roleDescription: string;
  canSpawn: boolean;
  agents: RunAgent[];
}

export function AgentsPage() {
  const { data: agentsData, isLoading: agentsLoading } = useAgents();
  const { data: rolesData, isLoading: rolesLoading } = useRoles();
  const { data: runningRuns } = useRuns({ stateFilter: "running", limit: 1 });
  const { data: pendingDecisionRuns } = usePendingGovernanceRuns(1);

  const agents = useMemo(
    () => (agentsData?.items ?? []).filter((agent) => isAgentRole(agent.role_id)),
    [agentsData?.items],
  );
  const roles = useMemo(
    () => (rolesData?.items ?? []).filter((role) => isAgentRole(role.role_id)),
    [rolesData],
  );
  const isLoading = agentsLoading || rolesLoading;
  const hasActiveRun =
    (runningRuns?.items.length ?? 0) > 0
    || (pendingDecisionRuns?.items.length ?? 0) > 0;

  return (
    <div className="flex flex-col">
      <PageHeader>
        <div className="w-full">
          <h1 className="font-display text-2xl font-bold text-gold">Agent Registry</h1>
          <p className="text-sm text-foreground-muted">
            Court roster, delegation depth, and live runtime status.
          </p>
        </div>
      </PageHeader>

      <PageContent className="space-y-6">
        <AgentList
          agents={agents}
          roles={roles}
          hasActiveRun={hasActiveRun}
          isLoading={isLoading}
        />
      </PageContent>
    </div>
  );
}

function AgentList({
  agents,
  roles,
  hasActiveRun,
  isLoading,
}: {
  agents: RunAgent[];
  roles: Role[];
  hasActiveRun: boolean;
  isLoading: boolean;
}) {
  const groupedByRole = useMemo((): RoleGroup[] => {
    const agentsByRole = agents.reduce<Record<string, RunAgent[]>>((acc, agent) => {
      (acc[agent.role_id] ??= []).push(agent);
      return acc;
    }, {});

    // One card per canonical role
    const groups: RoleGroup[] = roles.map((role) => ({
      roleId: role.role_id,
      roleName: role.display_name,
      roleDescription: role.description,
      canSpawn: role.can_spawn,
      agents: (agentsByRole[role.role_id] ?? []).sort(
        (a, b) =>
          a.spawn_depth - b.spawn_depth ||
          a.display_name.localeCompare(b.display_name),
      ),
    }));

    // Safety: include agents whose role_id isn't in the canonical list
    const knownRoleIds = new Set(roles.map((r) => r.role_id));
    for (const [roleId, roleAgents] of Object.entries(agentsByRole)) {
      if (!knownRoleIds.has(roleId)) {
        groups.push({
          roleId,
          roleName: roleAgents[0]?.display_name ?? roleId,
          roleDescription: "Unknown role.",
          canSpawn: false,
          agents: [...roleAgents].sort(
            (a, b) =>
              a.spawn_depth - b.spawn_depth ||
              a.display_name.localeCompare(b.display_name),
          ),
        });
      }
    }

    // Sort by kingdom hierarchy order
    return groups.sort((a, b) => {
      const aOrder = ROLE_HIERARCHY_ORDER[a.roleId] ?? 99;
      const bOrder = ROLE_HIERARCHY_ORDER[b.roleId] ?? 99;
      return aOrder - bOrder;
    });
  }, [agents, roles]);

  if (isLoading) {
    return <div className="py-10 text-sm text-foreground-subtle">Loading agent roster...</div>;
  }

  if (roles.length === 0 && agents.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-border bg-surface px-6 py-16 text-center">
        <Users className="mb-3 h-12 w-12 text-foreground-subtle" />
        <p className="text-base font-medium text-foreground">No agents or roles found</p>
        <p className="mt-1 max-w-md text-sm text-foreground-muted">
          The system has not been initialized. Start the orchestrator to seed canonical roles.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <StatusSummaryBar
        agents={agents}
        totalRoles={roles.length}
        hasActiveRun={hasActiveRun}
      />
      <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
        {groupedByRole.map((group) => (
          <RoleGroupCard key={group.roleId} group={group} />
        ))}
      </div>
    </div>
  );
}

function RoleGroupCard({ group }: { group: RoleGroup }) {
  const { roleId, roleName, roleDescription, agents } = group;
  const hasAgents = agents.length > 0;
  const Icon = getRoleIcon(roleId);
  const hasBusyAgent = agents.some((a) => a.status === "busy");

  const roleStatusCounts = useMemo(() => {
    const counts: Record<AgentStatus, number> = {
      busy: 0,
      idle: 0,
      blocked: 0,
      offline: 0,
      failed: 0,
    };
    for (const agent of agents) {
      counts[agent.status as AgentStatus] =
        (counts[agent.status as AgentStatus] ?? 0) + 1;
    }
    return counts;
  }, [agents]);

  return (
    <Card
      className={cn(
        "border-border transition-opacity duration-200",
        hasAgents
          ? "bg-surface-raised/60"
          : "bg-surface-raised/30 opacity-60 hover:opacity-80",
        hasBusyAgent && "animate-glow-pulse-subtle",
      )}
      style={hasBusyAgent ? getRoleGlowStyle(roleId) : undefined}
    >
      <CardContent className="p-0">
        <div className="px-3 py-2.5">
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <Icon
                className={cn(
                  "h-4 w-4 shrink-0",
                  hasAgents ? getRoleColor(roleId) : "text-foreground-subtle",
                  hasBusyAgent && "animate-glow-pulse",
                )}
              />
              <h3
                className={cn(
                  "truncate text-sm font-semibold",
                  hasAgents ? getRoleColor(roleId) : "text-foreground-subtle",
                )}
              >
                {roleName}
              </h3>
              {hasAgents && (
                <Badge className="ml-1">{agents.length}</Badge>
              )}
            </div>
            <p className="text-[11px] text-foreground-muted">{roleDescription}</p>
            {hasAgents ? (
              <div className="flex flex-wrap items-center gap-2 pt-1">
                {AGENT_STATUS_ORDER.map((status) => {
                  const count = roleStatusCounts[status];
                  if (count === 0) return null;
                  return (
                    <span
                      key={status}
                      className="inline-flex items-center gap-1 text-xs text-foreground-subtle"
                    >
                      <span
                        className={cn(
                          "h-1.5 w-1.5 rounded-full",
                          AGENT_STATUS_DOT_COLORS[status],
                        )}
                      />
                      {count} {capitalize(status)}
                    </span>
                  );
                })}
              </div>
            ) : (
              <p className="pt-1 text-xs italic text-foreground-subtle">
                Awaiting assignment
              </p>
            )}
          </div>
        </div>

        {hasAgents && (
          <div className="space-y-1.5 border-t border-border px-3 py-2">
            {agents.map((agent) => (
              <div
                key={agent.agent_id}
                className="rounded-md border border-border/80 bg-surface px-2.5 py-1.5"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="flex items-center gap-2 text-[11px] text-foreground-subtle">
                    <span>Depth {agent.spawn_depth}</span>
                    <span className="text-border">·</span>
                    <HeartbeatIndicator lastHeartbeatAt={agent.last_heartbeat_at} />
                  </div>
                  <Badge variant="status" status={agent.status}>
                    {capitalize(agent.status)}
                  </Badge>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
