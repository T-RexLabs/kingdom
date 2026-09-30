import { useState, type PropsWithChildren } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
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
        message: { availability: "available", preview: text, byte_length: text.length },
      },
    ],
  };
});
vi.mock("@/api/hooks-runs", () => ({
  useRunExecutionActivity: () => ({
    data: { items: data.items, snapshotThrough: 101 },
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

it("retains final response and exact source disclosures across virtual unmounts and resets on attempt change", async () => {
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
  await waitFor(() => expect(screen.getByText("Final Response")).toBeVisible());
  expect(screen.getByText("Final Response").tagName).toBe("SUMMARY");
  await user.click(screen.getByText("Final Response"));
  await user.click(await screen.findByText("Response Details"));
  await waitFor(() =>
    expect(document.querySelector("pre")?.textContent).toBe(data.text),
  );
  const scroller = screen.getByTestId("virtuoso-scroller");
  act(() => scroller.scrollTo({ top: 0 }));
  await waitFor(() =>
    expect(screen.queryByText("Final Response")).not.toBeInTheDocument(),
  );
  act(() => scroller.scrollTo({ top: scroller.scrollHeight }));
  await waitFor(() => expect(screen.getByText("Final Response")).toBeVisible());
  expect(screen.getByText("Final Response").closest("details")).toHaveAttribute(
    "open",
  );
  expect(document.querySelector("pre")?.textContent).toBe(data.text);
  view.rerender(
    <RunExecutionActivity
      runId="run"
      attemptId="attempt-b"
      status="succeeded"
      detail={{ ...detail, attempt_id: "attempt-b" }}
    />,
  );
  await waitFor(() => expect(screen.getByText("Final Response")).toBeVisible());
  expect(
    screen.getByText("Final Response").closest("details"),
  ).not.toHaveAttribute("open");
  expect(document.querySelector("pre")).toBeNull();
});
