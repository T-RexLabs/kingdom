import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { HostedSessionBoundary } from "./session";
import { CentralPortal } from "./portal";
import { SessionContext, type CentralSession } from "./session-context";
const id = "9d38bdaa-34be-4d48-9b1c-a75ecc927847";
function setup(owner = true, ambiguous = false, rejected = false) {
  const writes: RequestInit[] = [];
  vi.stubGlobal("fetch", vi.fn((url: string, init?: RequestInit) => {
    if (url === "/api/central/v1/session") return Promise.resolve(new Response(JSON.stringify({ session_id: "session", csrf_token: "csrf", user: { id: "owner", display_name: "Owner", team_owner: owner } })));
    if (url === "/api/central/v1/projects" && init?.method === "POST") {
      writes.push(init);
      if (ambiguous && writes.length === 1) return Promise.reject(new TypeError("connection lost"));
      if (rejected && writes.length === 1) return Promise.resolve(new Response("Invalid project name.", { status: 400 }));
      return Promise.resolve(new Response(JSON.stringify({ id, display_name: "Shared Project", role: "owner" }), { status: 201 }));
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
    expect(await screen.findByRole("button", { name: "Open Project" })).toBeEnabled();
    expect(screen.getByRole("heading", { name: "Shared Project" })).toBeInTheDocument();
    expect(writes).toHaveLength(2); expect(writes[0].body).toEqual(writes[1].body);
    expect(new Headers(writes[0].headers).get("Idempotency-Key")).toBe(new Headers(writes[1].headers).get("Idempotency-Key"));
  });
  it("allows correction after a confirmed project rejection", async () => {
    const writes = setup(true, false, true); const user = userEvent.setup();
    const field = await screen.findByRole("textbox", { name: "Project Name" });
    await user.type(field, "Rejected Name"); await user.click(screen.getByRole("button", { name: "Create Project" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("could not create the project");
    expect(field).toBeEnabled();
    await user.clear(field); await user.type(field, "Shared Project");
    await user.click(screen.getByRole("button", { name: "Create Project" }));
    expect(await screen.findByRole("heading", { name: "Shared Project" })).toBeInTheDocument();
    expect(writes).toHaveLength(2);
    expect(new Headers(writes[0].headers).get("Idempotency-Key")).not.toBe(new Headers(writes[1].headers).get("Idempotency-Key"));
    expect(JSON.parse(writes[1].body as string)).toEqual({ display_name: "Shared Project" });
  });
  it("does not publish a late project response into another session", async () => {
    let finish: ((response: Response) => void) | undefined;
    let signal: AbortSignal | null | undefined;
    vi.stubGlobal("fetch", vi.fn((url: string, init?: RequestInit) => {
      if (url === "/api/central/v1/projects" && init?.method === "POST") {
        signal = init.signal;
        return new Promise<Response>(resolve => { finish = resolve; });
      }
      return Promise.resolve(new Response("[]"));
    }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const session = (actor: string): CentralSession => ({ session_id: `${actor}-session`, csrf_token: "csrf", user: { id: actor, display_name: actor, team_owner: true } });
    const view = (actor: string) => <QueryClientProvider client={client}><SessionContext.Provider value={session(actor)}><CentralPortal /></SessionContext.Provider></QueryClientProvider>;
    const { rerender } = render(view("first"));
    const user = userEvent.setup();
    await user.type(await screen.findByRole("textbox", { name: "Project Name" }), "Private Project");
    await user.click(screen.getByRole("button", { name: "Create Project" }));
    await waitFor(() => expect(finish).toBeDefined());
    rerender(view("second"));
    expect(signal?.aborted).toBe(true);
    await act(async () => { finish?.(new Response(JSON.stringify({ id, display_name: "Private Project", role: "owner" }), { status: 201 })); });
    expect(screen.queryByRole("heading", { name: "Private Project" })).not.toBeInTheDocument();
    expect(client.getQueryData(["central-projects", "first", "first-session"])).toEqual([]);
    expect(screen.getByRole("textbox", { name: "Project Name" })).toHaveValue("");
  });
  it("shows the membership empty state without owner controls", async () => {
    setup(false); expect(await screen.findByText("You do not have access to a project yet. Contact your team owner.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Create Project" })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Model Connections" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Add Connection" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "Team" })).not.toBeInTheDocument();
  });
});
