import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { HostedSessionBoundary } from "./session";
import { CentralPortal } from "./portal";
const id = "9d38bdaa-34be-4d48-9b1c-a75ecc927847";
function setup(owner = true, ambiguous = false) {
  const writes: RequestInit[] = [];
  vi.stubGlobal("fetch", vi.fn((url: string, init?: RequestInit) => {
    if (url === "/api/central/v1/session") return Promise.resolve(new Response(JSON.stringify({ session_id: "session", csrf_token: "csrf", user: { id: "owner", display_name: "Owner", team_owner: owner } })));
    if (url === "/api/central/v1/projects" && init?.method === "POST") {
      writes.push(init);
      if (ambiguous && writes.length === 1) return Promise.reject(new TypeError("connection lost"));
      return Promise.resolve(new Response(JSON.stringify({ id, display_name: "Shared Project" }), { status: 201 }));
    }
    if (url === "/api/central/v1/projects") return Promise.resolve(new Response("[]"));
    if (url === "/api/central/v1/connections") return Promise.resolve(new Response("[]"));
    return Promise.resolve(new Response(null, { status: 404 }));
  }));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(<QueryClientProvider client={client}><HostedSessionBoundary><CentralPortal /></HostedSessionBoundary></QueryClientProvider>);
  return writes;
}
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
describe("central projects", () => {
  it("retries an uncertain creation with the same key and payload", async () => {
    const writes = setup(true, true); const user = userEvent.setup();
    const field = await screen.findByRole("textbox", { name: "Project Name" });
    await user.type(field, "Shared Project"); await user.click(screen.getByRole("button", { name: "Create Project" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("result is not confirmed"); expect(field).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Retry Same Request" }));
    expect(await screen.findByRole("link", { name: "Shared Project Open Project" })).toHaveAttribute("href", `/projects/${id}/`);
    expect(writes).toHaveLength(2); expect(writes[0].body).toEqual(writes[1].body);
    expect(new Headers(writes[0].headers).get("Idempotency-Key")).toBe(new Headers(writes[1].headers).get("Idempotency-Key"));
  });
  it("shows the membership empty state without owner controls", async () => {
    setup(false); expect(await screen.findByText("You do not have access to a project yet. Contact your team owner.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Create Project" })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Model Connections" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Add Connection" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Team" })).not.toBeInTheDocument();
  });
});
