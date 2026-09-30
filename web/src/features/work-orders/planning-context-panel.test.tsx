import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { setCSRFToken } from "@/api/client";
import type { PlanningSessionDetail } from "@/api/hooks";
import { PlanningContextPanel } from "./planning-context-panel";

const contextEntries = vi.fn<
  (...args: unknown[]) => { data: { pages: { items: never[] }[] } }
>(() => ({ data: { pages: [{ items: [] }] } }));
vi.mock("@/api/hooks", () => ({
  usePlanningContextEntries: (...args: unknown[]) => contextEntries(...args),
}));

const session: PlanningSessionDetail = {
  planning_session_id: "session-a",
  acp_session_id: "acp-a",
  title: "Planning",
  title_state: "custom",
  auto_title_eligible: false,
  lifecycle_state: "active",
  conversation_state: "ready",
  project_root: "/project",
  model_selection: { model: "openai/gpt-5.6-luna" },
  external_sources: [],
  created_at: "2026-09-30T10:00:00Z",
  updated_at: "2026-09-30T10:00:00Z",
};

beforeEach(() => {
  sessionStorage.clear();
  contextEntries.mockClear();
  setCSRFToken("token", "original-binding");
});
afterEach(() => {
  vi.unstubAllGlobals();
});

it("loads snapshot detail only when opened and keeps included/excluded selection available", async () => {
  const user = userEvent.setup();
  render(
    <QueryClientProvider client={new QueryClient()}>
      <PlanningContextPanel
        session={{ ...session, context_snapshot_id: "snapshot-a" }}
        onChanged={async () => undefined}
      />
    </QueryClientProvider>,
  );
  expect(contextEntries).toHaveBeenLastCalledWith("session-a", "", "included");
  expect(
    screen.queryByText("No included files in this snapshot."),
  ).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Project Context" }));
  expect(contextEntries).toHaveBeenLastCalledWith(
    "session-a",
    "snapshot-a",
    "included",
  );
  await user.click(screen.getByRole("button", { name: "Excluded Files" }));
  expect(contextEntries).toHaveBeenLastCalledWith(
    "session-a",
    "snapshot-a",
    "excluded",
  );
  expect(
    screen.getByRole("button", { name: "Excluded Files" }),
  ).toHaveAttribute("aria-pressed", "true");
  await user.click(screen.getByRole("button", { name: "Done" }));
  expect(contextEntries).toHaveBeenLastCalledWith("session-a", "", "excluded");
});

it("recovers a restored context refresh after renewal without capturing another snapshot", async () => {
  const fetchMock = vi
    .fn()
    .mockRejectedValueOnce(new TypeError("Response lost"))
    .mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          state: "completed",
          response_status: 200,
          response_body: { context_snapshot_id: "snapshot-a" },
        }),
        { status: 200 },
      ),
    );
  vi.stubGlobal("fetch", fetchMock);
  const onChanged = vi.fn(async () => undefined);
  const mount = () =>
    render(
      <QueryClientProvider client={new QueryClient()}>
        <PlanningContextPanel session={session} onChanged={onChanged} />
      </QueryClientProvider>,
    );
  const user = userEvent.setup();
  const original = mount();
  await user.click(screen.getByRole("button", { name: "Project Context" }));
  await user.click(screen.getByRole("button", { name: "Refresh Context" }));
  await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce());
  const key = new Headers(fetchMock.mock.calls[0][1]?.headers).get(
    "Idempotency-Key",
  );
  original.unmount();
  setCSRFToken("renewed-token", "renewed-binding");
  mount();
  await user.click(
    screen.getByRole("button", { name: "Resolve Context Refresh" }),
  );
  await user.click(
    within(screen.getByRole("dialog", { name: "Project Context" })).getByRole(
      "button",
      { name: "Resolve Context Refresh" },
    ),
  );
  await waitFor(() =>
    expect(
      screen.getByRole("button", { name: "Refresh Context" }),
    ).toBeEnabled(),
  );
  expect(fetchMock).toHaveBeenCalledTimes(2);
  expect(fetchMock.mock.calls[1][0]).toBe(
    `/api/v1/mutation-outcomes/${encodeURIComponent(key!)}?operation=planning-context-refresh`,
  );
  expect(fetchMock.mock.calls[1][1]?.method ?? "GET").toBe("GET");
  expect(onChanged).toHaveBeenCalledOnce();
});
