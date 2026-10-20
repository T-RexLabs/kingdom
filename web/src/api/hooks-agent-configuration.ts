import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiFetch } from "./client";
import type { components } from "./schema";

export type AgentConfiguration = components["schemas"]["AgentConfiguration"];
export type AgentRoleConfiguration = components["schemas"]["AgentRoleConfiguration"];
export type AgentWorkspaceConfiguration = components["schemas"]["AgentWorkspaceConfiguration"];
export type UpdateAgentRoleConfigurationRequest = components["schemas"]["UpdateAgentRoleConfigurationRequest"];
export type ResetAgentRoleGuidanceRequest = components["schemas"]["ResetAgentRoleGuidanceRequest"];
export type UpdateWorkspaceAgentVoiceRequest = components["schemas"]["UpdateWorkspaceAgentVoiceRequest"];

export const agentConfigurationKey = ["agent-configuration"] as const;

export function useAgentConfiguration() {
  return useQuery({
    queryKey: agentConfigurationKey,
    queryFn: () => apiFetch<AgentConfiguration>("/agent-configuration"),
    refetchInterval: 10_000,
    refetchOnWindowFocus: true,
  });
}

export function useUpdateWorkspaceAgentVoice() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ body, key }: { body: UpdateWorkspaceAgentVoiceRequest; key: string }) =>
      apiFetch<AgentWorkspaceConfiguration>("/agent-configuration/workspace-voice", {
        method: "PUT",
        body: JSON.stringify(body),
        headers: { "Idempotency-Key": key },
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: agentConfigurationKey }),
  });
}

export function useUpdateAgentRoleConfiguration() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roleId, body, key }: { roleId: string; body: UpdateAgentRoleConfigurationRequest; key: string }) =>
      apiFetch<AgentRoleConfiguration>(`/agent-configuration/roles/${encodeURIComponent(roleId)}`, {
        method: "PUT",
        body: JSON.stringify(body),
        headers: { "Idempotency-Key": key },
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: agentConfigurationKey }),
  });
}

export function useResetAgentRoleGuidance() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roleId, body, key }: { roleId: string; body: ResetAgentRoleGuidanceRequest; key: string }) =>
      apiFetch<AgentRoleConfiguration>(`/agent-configuration/roles/${encodeURIComponent(roleId)}/guidance/reset`, {
        method: "POST",
        body: JSON.stringify(body),
        headers: { "Idempotency-Key": key },
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: agentConfigurationKey }),
  });
}
