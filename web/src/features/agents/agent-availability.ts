const labels: Record<string, string> = {
  busy: "Running",
  idle: "Available",
  blocked: "Blocked",
  offline: "Offline",
  failed: "Failed",
};

/** Registry availability is aggregated across every instance of a role. It
 * never chooses an arbitrary run to represent that role. */
export function agentAvailability(agents: readonly { status: string }[]) {
  const counts = new Map<string, number>();
  for (const agent of agents)
    counts.set(agent.status, (counts.get(agent.status) ?? 0) + 1);
  const statuses = [...counts.keys()];
  const details = statuses
    .map((status) => `${counts.get(status)} ${labels[status] ?? "Unknown"}`)
    .join(" · ");
  if (statuses.length === 0)
    return {
      label: "Unassigned",
      tone: "neutral",
      details: "No agent instance has been assigned yet.",
    } as const;
  if (statuses.every((status) => status === "busy" || status === "idle")) {
    return {
      label: counts.has("busy") ? "Running" : "Available",
      tone: counts.has("busy") ? "running" : "succeeded",
      details,
    } as const;
  }
  if (statuses.length > 1)
    return { label: "Mixed", tone: "attention", details } as const;
  const status = statuses[0];
  return {
    label: labels[status] ?? "Unknown",
    tone:
      status === "failed"
        ? "failed"
        : status === "blocked"
          ? "attention"
          : "neutral",
    details,
  } as const;
}
