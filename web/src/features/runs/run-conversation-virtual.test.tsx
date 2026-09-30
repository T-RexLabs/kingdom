import { useState, type PropsWithChildren } from "react";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { VirtuosoMockContext } from "react-virtuoso";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { mockActivityScrollGeometry } from "@/features/activity/activity-virtual-test-helpers";
import type { RunExecutionDetail } from "@/api/hooks";
import { RunExecutionActivity } from "./run-execution-activity";

const data = vi.hoisted(() => {
  const absent = { availability: "not_provided" as const };
  const text = "## Canonical Result\nThe result is ready.";
  const base = {
    input: absent,
    output: absent,
    reasoning: absent,
    message: absent,
    recorded_at: "2026-09-30T10:00:00Z",
  };
  return {
    text,
    items: [
      ...Array.from({ length: 100 }, (_, i) => ({
        ...base,
        id: `p-${i}`,
        event_id: `e-${i}`,
        sequence: i + 1,
        kind: "tool",
        label: `Tool ${i + 1}`,
      })),
      {
        ...base,
        id: "message",
        event_id: "message",
        sequence: 101,
        invocation_id: "inv",
        kind: "message",
        label: "Provider Message",
        message: {
          availability: "available",
          preview: text,
          byte_length: text.length,
        },
      },
    ],
  };
});
vi.mock("@/api/hooks-runs", () => ({
  useRunExecutionActivity: (_runId: string, attemptId: string) => ({
    data: {
      items: attemptId === "attempt-a" ? data.items : [],
      snapshotThrough: 101,
    },
    gaps: [],
    loadMore: vi.fn(),
    isLoadingMore: false,
    loadError: null,
  }),
}));
vi.mock("./run-live-activity", () => ({
  useLiveRunActivity: () => ({}),
  selectLiveRunActivity: () => [],
}));
function Wrapper({ children }: PropsWithChildren) {
  const [client] = useState(
    () => new QueryClient({ defaultOptions: { queries: { retry: false } } }),
  );
  return (
    <QueryClientProvider client={client}>
      <VirtuosoMockContext.Provider
        value={{ viewportHeight: 260, itemHeight: 52 }}
      >
        {children}
      </VirtuosoMockContext.Provider>
    </QueryClientProvider>
  );
}
let restoreGeometry: () => void;
beforeEach(() => {
  restoreGeometry = mockActivityScrollGeometry();
});
afterEach(() => restoreGeometry());

it("keeps the inline settled response outside virtual activity and resets its source on attempt change", async () => {
  const detail = {
    attempt_id: "attempt-a",
    provider_invocation_id: "inv",
    settlement_status: "succeeded",
    output_status: "settled",
    output_text: data.text,
  } as RunExecutionDetail;
  const user = userEvent.setup();
  const view = render(
    <RunExecutionActivity
      runId="run"
      attemptId="attempt-a"
      status="succeeded"
      detail={detail}
    />,
    { wrapper: Wrapper },
  );
  const response = screen.getByRole("region", { name: "Final response" });
  expect(
    within(response).getByRole("heading", { name: "Canonical Result" }),
  ).toBeVisible();
  await user.click(within(response).getByText("Response Details"));
  expect(response.querySelector("pre")).toBeVisible();
  expect(response.querySelector("pre")?.textContent).toBe(data.text);
  expect(screen.queryByTestId("virtuoso-scroller")).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Show Activity" }));
  const scroller = await screen.findByTestId("virtuoso-scroller");
  expect(screen.queryByText("Provider Message")).not.toBeInTheDocument();
  expect(screen.getAllByRole("heading", { name: "Canonical Result" })).toHaveLength(1);
  await waitFor(() =>
    expect(within(scroller).getByText("Tool 100")).toBeVisible(),
  );
  expect(scroller.querySelectorAll("[data-item-index]").length).toBeLessThan(
    data.items.length,
  );
  act(() => scroller.scrollTo({ top: 0 }));
  await waitFor(() =>
    expect(within(scroller).getByText("Tool 1")).toBeVisible(),
  );
  expect(within(scroller).queryByText("Tool 100")).not.toBeInTheDocument();
  expect(screen.getByRole("region", { name: "Final response" })).toBe(response);
  expect(response.querySelector("pre")).toBeVisible();
  expect(response.querySelector("pre")?.textContent).toBe(data.text);
  act(() => scroller.scrollTo({ top: scroller.scrollHeight }));
  await waitFor(() =>
    expect(within(scroller).getByText("Tool 100")).toBeVisible(),
  );
  await user.click(screen.getByRole("button", { name: "Hide Activity" }));
  expect(
    within(response).getByRole("heading", { name: "Canonical Result" }),
  ).toBeVisible();
  expect(
    within(response).getByText("Response Details").closest("details"),
  ).toHaveAttribute("open");
  view.rerender(
    <RunExecutionActivity
      runId="run"
      attemptId="attempt-b"
      status="succeeded"
      detail={{
        ...detail,
        attempt_id: "attempt-b",
        output_text: "## New Attempt\nNew result.",
      }}
    />,
  );
  expect(screen.getByRole("heading", { name: "New Attempt" })).toBeVisible();
  expect(
    screen.queryByRole("heading", { name: "Canonical Result" }),
  ).not.toBeInTheDocument();
  expect(
    screen.getByText("Response Details").closest("details"),
  ).not.toHaveAttribute("open");
  expect(document.querySelector("pre")).toBeNull();
});
