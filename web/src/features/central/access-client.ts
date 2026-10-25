import { centralFetch } from "@/api/client";

export type ProjectRole = "owner" | "member";
export type CentralProject = { id: string; display_name: string; role: ProjectRole };
export type CentralUser = { id: string; display_name: string; team_owner: boolean };
export type DirectoryUser = CentralUser & { accounts: { provider_id: string; account_id: string }[] };
export type AdmissionProvider = { id: "github" | "oidc"; label: string; account_id_label: string; help: string };
export type ProjectMembership = { user: CentralUser; role: ProjectRole };
export type AdmissionRequest = { provider_id: string; account_id: string; display_name: string };
export type AccessRequest = { path: string; method: "POST" | "PUT" | "DELETE"; body?: object };
export const accessKeys = {
  users: (actor: string, session: string) => ["central-users", actor, session] as const,
  providers: (actor: string, session: string) => ["central-user-providers", actor, session] as const,
  memberships: (actor: string, session: string, project: string) => ["central-memberships", actor, session, project] as const,
};
export const accessAPI = {
  users: (signal?: AbortSignal) => centralFetch<DirectoryUser[]>("/users", { signal }),
  providers: (signal?: AbortSignal) => centralFetch<AdmissionProvider[]>("/users/providers", { signal }),
  memberships: (project: string, signal?: AbortSignal) => centralFetch<ProjectMembership[]>(`/projects/${encodeURIComponent(project)}/memberships`, { signal }),
};
export const membershipPath = (project: string, user: string) => `/projects/${encodeURIComponent(project)}/memberships/${encodeURIComponent(user)}`;
export const roleLabel = (role: ProjectRole) => role === "owner" ? "Owner" : "Member";
