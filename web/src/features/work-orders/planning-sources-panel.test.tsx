import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { setCSRFToken } from "@/api/client";
import { PlanningSourcesPanel } from "./planning-sources-panel";

vi.mock("@/features/connections/external-source-comparison", () => ({
  ExternalSourceComparison: () => <div>Source Comparison</div>,
}));
vi.mock("@/features/connections/issue-picker", () => ({ IssuePicker: () => <button>Add Source</button> }));

const source = {
  link_id: "source-1", provider: "github" as const, repository: "owner/repo", issue_number: 1,
  canonical_url: "https://github.com/owner/repo/issues/1", used_snapshot_id: "old", active_snapshot_id: "old",
  latest_snapshot_id: "latest", status: "changed" as const,
};
function mount(sources = [source], onChanged = vi.fn(async () => undefined)) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><PlanningSourcesPanel workspace="/project" sessionId="session-1" sources={sources} onAttach={async () => undefined} onChanged={onChanged} /></QueryClientProvider>);
}
async function startAction(user: ReturnType<typeof userEvent.setup>, action: string) {
  await user.click(screen.getByRole("button", { name: /^Planning Sources/ }));
  if (action === "refresh") await user.click(screen.getByRole("button", { name: "Refresh owner/repo#1" }));
  if (action === "detach") {
    await user.click(screen.getByRole("button", { name: "Remove owner/repo#1" }));
    await user.click(screen.getByRole("button", { name: "Confirm removing owner/repo#1" }));
  }
  if (action === "adopt") {
    await user.click(screen.getByRole("button", { name: "Review Update" }));
    await user.click(screen.getByRole("button", { name: "Adopt Update" }));
  }
}

beforeEach(() => { sessionStorage.clear(); setCSRFToken("token", "binding-original"); });
afterEach(() => { vi.unstubAllGlobals(); setCSRFToken(""); });

describe("PlanningSourcesPanel recovery", () => {
  it.each(["refresh", "adopt", "detach"])("retains the exact %s request across a fresh mount", async (action) => {
    const sent: { url: string; key: string; body: unknown }[] = [];
    vi.stubGlobal("fetch", vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
      sent.push({ url: String(url), key: new Headers(init?.headers).get("Idempotency-Key") ?? "", body: init?.body });
      if (sent.length === 1) throw new TypeError("Connection lost");
      return new Response(JSON.stringify(action === "refresh" ? { outcome: "unchanged", snapshot_created: false } : { status: action === "adopt" ? "adopted" : "detached" }), { status: 201 });
    }));
    const user = userEvent.setup();
    const original = mount();
    await startAction(user, action);
    await waitFor(() => expect(sent).toHaveLength(1));
    original.unmount();
    // A detached link may already be absent from canonical source state.
    mount(action === "detach" ? [] : [source]);
    await user.click(screen.getByRole("button", { name: "Recover Source Request" }));
    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent[1]).toEqual(sent[0]);
    expect(sent[0].key).toMatch(/^kd1\./);
    await waitFor(() => expect(screen.queryByRole("button", { name: "Recover Source Request" })).not.toBeInTheDocument());
  });

  it("keeps a source request unresolved after authentication changes and allows state readback", async () => {
    const fetchMock = vi.fn(async () => { throw new TypeError("Connection lost"); });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    const original = mount();
    await startAction(user, "refresh");
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    original.unmount();
    setCSRFToken("new-token", "binding-replacement");
    const readState = vi.fn(async () => undefined);
    mount([source], readState);
    await user.click(screen.getByRole("button", { name: "Recover Source Request" }));
    expect(await screen.findByText(/belongs to an earlier browser session/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Refresh Source State" }));
    expect(readState).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Recover Source Request" })).toBeInTheDocument();
  });
});
