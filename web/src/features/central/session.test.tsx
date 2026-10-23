import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { signalUnauthorized } from "@/api/client";
import { HostedSessionBoundary } from "./session";
import { useCentralSession } from "./session-context";
const session = { session_id: "opaque-session-id", csrf_token: "opaque-csrf", user: { id: "owner", display_name: "Owner", team_owner: true } };
function User() { return <p>Signed in as {useCentralSession().user.display_name}</p>; }
function setup() { const client = new QueryClient({ defaultOptions: { queries: { retry: false } } }); render(<QueryClientProvider client={client}><HostedSessionBoundary><User /></HostedSessionBoundary></QueryClientProvider>); return client; }
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
describe("hosted session", () => {
  it("loads the existing central session without local bootstrap", async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(session), { status: 200 })); vi.stubGlobal("fetch", fetcher); setup();
    expect(await screen.findByText("Signed in as Owner")).toBeInTheDocument();
    expect(fetcher).toHaveBeenCalledTimes(1); expect(fetcher.mock.calls[0][0]).toBe("/api/central/v1/session"); expect(fetcher.mock.calls[0][1].method).toBeUndefined();
  });
  it("offers only configured sign-in providers", async () => {
    const fetcher = vi.fn().mockResolvedValueOnce(new Response(null, { status: 401 })).mockResolvedValueOnce(new Response(JSON.stringify({ providers: ["github", "oidc", "foreign"] }), { status: 200 })); vi.stubGlobal("fetch", fetcher); setup();
    expect(await screen.findByRole("link", { name: "Continue with GitHub" })).toHaveAttribute("href", "/auth/github/start");
    expect(screen.getByRole("link", { name: "Continue with Your Organization" })).toHaveAttribute("href", "/auth/oidc/start");
    expect(screen.queryByText("Signed in as Owner")).not.toBeInTheDocument();
  });
  it("clears prior actor data and rechecks a lost session without repeating a mutation", async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(new Response(JSON.stringify(session), { status: 200 }))); vi.stubGlobal("fetch", fetcher); const client = setup();
    await screen.findByText("Signed in as Owner"); client.setQueryData(["private-old-actor"], "private");
    act(() => signalUnauthorized("mutation"));
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(2));
    expect(client.getQueryData(["private-old-actor"])).toBeUndefined();
    expect(fetcher.mock.calls.every(call => call[0] === "/api/central/v1/session")).toBe(true);
  });
});
