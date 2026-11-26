import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TeamMembers, ProjectAccess } from "./access";
import { ApiError } from "@/api/client";
import { SessionContext } from "./session-context";
import type { CentralProject, ProjectMembership } from "./access-client";
const { centralFetch } = vi.hoisted(() => ({ centralFetch: vi.fn() }));
vi.mock("@/api/client", async () => ({ ...await vi.importActual<typeof import("@/api/client")>("@/api/client"), centralFetch }));
const project: CentralProject = { id: "project-one", display_name: "Shared Project", role: "owner" };
const owner = { id: "owner", display_name: "Owner", team_owner: true };
const person = { id: "person", display_name: "Same Name", team_owner: false };
const providers = [{ id: "github", label: "GitHub", account_id_label: "GitHub User ID", help: "Use the permanent numeric GitHub user ID." }, { id: "oidc", label: "Organization Sign-In", account_id_label: "Organization Account ID", help: "Preserve the exact stable account ID." }];
const directory = [{ ...owner, accounts: [{ provider_id: "github", account_id: "1" }] }, { ...person, accounts: [{ provider_id: "oidc", account_id: "Case-Sensitive" }] }, { id: "other", display_name: "Same Name", team_owner: false, accounts: [{ provider_id: "github", account_id: "42" }] }];
function view(teamOwner = true, projects = [project]) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const session = { session_id: "session", csrf_token: "csrf", user: { ...owner, team_owner: teamOwner } };
  const tree = (id = "session") => <QueryClientProvider client={client}><SessionContext value={{ ...session, session_id: id }}><TeamMembers key={`team:${id}`} /><ProjectAccess key={`project:${id}`} projects={projects} /></SessionContext></QueryClientProvider>;
  const result = render(tree()); return { client, result, tree };
}
afterEach(() => { cleanup(); centralFetch.mockReset(); });
function reads(members: ProjectMembership[] = [{ user: owner, role: "owner" }, { user: person, role: "member" }]) {
  return (path: string, init?: RequestInit) => {
    if (init?.method) return Promise.resolve(undefined);
    if (path === "/users/providers") return Promise.resolve(providers);
    if (path === "/users") return Promise.resolve(directory);
    if (path.endsWith("/memberships")) return Promise.resolve(members);
    return Promise.reject(new Error("unexpected endpoint"));
  };
}
describe("team and project access", () => {
  it("keeps an uncertain admission's exact provider account bytes and retry key", async () => {
    const user = userEvent.setup(); const base = reads(); let posts = 0;
    centralFetch.mockImplementation((path: string, init?: RequestInit) => {
      if (init?.method === "POST") return ++posts === 1 ? Promise.reject(new Error("lost reply")) : Promise.resolve(person);
      return base(path, init);
    });
    view(); await user.click(screen.getByRole("button", { name: "Manage Team Members" }));
    await user.type(await screen.findByLabelText("Display Name"), "Person");
    await user.selectOptions(screen.getByLabelText("Sign-In Provider"), "oidc");
    await user.type(screen.getByLabelText("Organization Account ID"), "Case-Sensitive");
    await user.click(screen.getByRole("button", { name: "Authorize Account" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("result is not confirmed");
    expect(screen.getByLabelText("Organization Account ID")).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Retry Same Request" }));
    expect(await screen.findByRole("status")).toHaveTextContent("authorization is confirmed");
    const calls = centralFetch.mock.calls.filter(call => call[1]?.method === "POST");
    expect(calls).toHaveLength(2); expect(calls[0][1].body).toBe(calls[1][1].body); expect(calls[0][1].headers).toEqual(calls[1][1].headers);
    expect(JSON.parse(calls[0][1].body)).toEqual({ provider_id: "oidc", account_id: "Case-Sensitive", display_name: "Person" });
    expect(JSON.parse(calls[0][1].body)).not.toHaveProperty("issuer");
  });

  it("lets a non-team project owner manage existing members without reading the directory", async () => {
    const user = userEvent.setup(); centralFetch.mockImplementation(reads()); view(false);
    expect(screen.queryByRole("heading", { name: "Team Members" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Manage Project Access" }));
    await user.selectOptions(screen.getByLabelText("Project"), project.id);
    expect(await screen.findByRole("button", { name: "Change Role for Same Name" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Add Project Member" })).not.toBeInTheDocument();
    expect(centralFetch.mock.calls.some(call => call[0] === "/users" || call[0] === "/users/providers")).toBe(false);
    await user.click(screen.getByRole("button", { name: "Change Role for Same Name" }));
    const dialog = await screen.findByRole("dialog", { name: "Change Project Role" });
    await user.selectOptions(within(dialog).getByLabelText("Project Role"), "owner");
    await user.click(within(dialog).getByRole("button", { name: "Save Role" }));
    await waitFor(() => expect(centralFetch.mock.calls.some(call => call[0] === "/projects/project-one/memberships/person" && call[1]?.method === "PUT")).toBe(true));
    const call = centralFetch.mock.calls.find(call => call[1]?.method === "PUT"); expect(JSON.parse(call?.[1].body)).toEqual({ role: "owner" });
  });

  it("confirms removal before sending an actor/project-bound DELETE", async () => {
    const user = userEvent.setup(); centralFetch.mockImplementation(reads()); view();
    await user.click(screen.getByRole("button", { name: "Manage Project Access" })); await user.selectOptions(screen.getByLabelText("Project"), project.id);
    await user.click(await screen.findByRole("button", { name: "Remove Access for Same Name" }));
    const dialog = screen.getByRole("dialog", { name: "Remove Project Access" });
    expect(centralFetch.mock.calls.some(call => call[1]?.method === "DELETE")).toBe(false);
    await user.click(within(dialog).getByRole("button", { name: "Remove Access" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    const call = centralFetch.mock.calls.find(call => call[1]?.method === "DELETE"); expect(call?.[0]).toBe("/projects/project-one/memberships/person"); expect(call?.[1].body).toBeUndefined(); expect(call?.[1].headers["Idempotency-Key"]).toBeTruthy();
  });

  it("uses account references to distinguish duplicate directory names and adds an exact user ID", async () => {
    const user = userEvent.setup(); centralFetch.mockImplementation(reads([{ user: owner, role: "owner" }])); view();
    await user.click(screen.getByRole("button", { name: "Manage Project Access" })); await user.selectOptions(screen.getByLabelText("Project"), project.id);
    const selector = await screen.findByLabelText("Authorized Account");
    expect(screen.getByRole("option", { name: "Same Name · Organization Case-Sensitive" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Same Name · GitHub 42" })).toBeInTheDocument();
    await user.selectOptions(selector, "other"); await user.click(screen.getByRole("button", { name: "Add Member" }));
    await waitFor(() => expect(centralFetch.mock.calls.some(call => call[0] === "/projects/project-one/memberships/other" && call[1]?.method === "PUT")).toBe(true));
  });

  it("does not infer project ownership from team ownership", () => {
    centralFetch.mockImplementation(reads()); view(true, [{ ...project, role: "member" }]);
    expect(screen.queryByRole("heading", { name: "Project Access" })).not.toBeInTheDocument();
    expect(centralFetch).not.toHaveBeenCalled();
  });

  it("drops an uncertain request when the authenticated session changes", async () => {
    const user = userEvent.setup(); const base = reads(); centralFetch.mockImplementation((path: string, init?: RequestInit) => init?.method ? Promise.reject(new Error("lost reply")) : base(path, init));
    const rendered = view(); await user.click(screen.getByRole("button", { name: "Manage Project Access" })); await user.selectOptions(screen.getByLabelText("Project"), project.id);
    await user.click(await screen.findByRole("button", { name: "Remove Access for Same Name" })); await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Remove Access" }));
    expect(await screen.findByRole("button", { name: "Retry Same Request" })).toBeInTheDocument();
    rendered.result.rerender(rendered.tree("another-session"));
    expect(screen.queryByRole("button", { name: "Retry Same Request" })).not.toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(centralFetch.mock.calls.filter(call => call[1]?.method === "DELETE")).toHaveLength(1);
  });
  it("hides directory account references when an uncertain retry is denied", async () => {
    const user = userEvent.setup(); const base = reads(); let posts = 0;
    const denied = new ApiError(403, { message: "Denied", code: "unknown", retryable: false, category: "core", details: {}, correlationId: "test" });
    centralFetch.mockImplementation((path: string, init?: RequestInit) => init?.method === "POST" ? Promise.reject(++posts === 1 ? new Error("lost reply") : denied) : base(path, init));
    view(); await user.click(screen.getByRole("button", { name: "Manage Team Members" }));
    expect(await screen.findByText("GitHub · 42")).toBeInTheDocument();
    await user.type(screen.getByLabelText("Display Name"), "Another"); await user.type(screen.getByLabelText("GitHub User ID"), "99");
    await user.click(screen.getByRole("button", { name: "Authorize Account" }));
    await user.click(await screen.findByRole("button", { name: "Retry Same Request" }));
    expect(await screen.findByRole("button", { name: "Reload Access List" })).toBeInTheDocument();
    expect(screen.queryByText("GitHub · 42")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Display Name")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry Same Request" })).not.toBeInTheDocument();
    const requests = centralFetch.mock.calls.filter(call => call[1]?.method === "POST");
    expect(requests).toHaveLength(2); expect(requests[0][1].body).toBe(requests[1][1].body); expect(requests[0][1].headers).toEqual(requests[1][1].headers);
  });

  it("keeps last-owner rejection separate from a successful role change", async () => {
    const user = userEvent.setup(); const base = reads([{ user: owner, role: "owner" }]);
    centralFetch.mockImplementation((path: string, init?: RequestInit) => init?.method === "PUT" ? Promise.reject(new ApiError(403, { message: "Denied", code: "unknown", retryable: false, category: "core", details: {}, correlationId: "test" })) : base(path, init));
    view(false); await user.click(screen.getByRole("button", { name: "Manage Project Access" })); await user.selectOptions(screen.getByLabelText("Project"), project.id);
    await user.click(await screen.findByRole("button", { name: "Change Role for Owner" }));
    const dialog = screen.getByRole("dialog"); await user.selectOptions(within(dialog).getByLabelText("Project Role"), "member"); await user.click(within(dialog).getByRole("button", { name: "Save Role" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent("at least one project owner");
    expect(within(dialog).getByRole("button", { name: "Save Role" })).toBeDisabled();
    expect(screen.queryByText("Project access was updated.")).not.toBeInTheDocument();
    expect(centralFetch.mock.calls.filter(call => call[1]?.method === "PUT")).toHaveLength(1);
  });

  it("does not reuse a completed member-add message in a new role confirmation", async () => {
    const user = userEvent.setup(); centralFetch.mockImplementation(reads()); view();
    await user.click(screen.getByRole("button", { name: "Manage Project Access" })); await user.selectOptions(screen.getByLabelText("Project"), project.id);
    await user.selectOptions(await screen.findByLabelText("Authorized Account"), "other");
    await user.click(screen.getByRole("button", { name: "Add Member" }));
    expect(await screen.findByRole("status")).toHaveTextContent("Project access was added.");
    await user.click(screen.getByRole("button", { name: "Change Role for Same Name" }));
    const dialog = screen.getByRole("dialog", { name: "Change Project Role" });
    expect(within(dialog).queryByText("Project access was added.")).not.toBeInTheDocument();
    expect(screen.queryByText("Project access was added.")).not.toBeInTheDocument();
    expect(within(dialog).getByLabelText("Project Role")).toHaveValue("member");
  });

});
