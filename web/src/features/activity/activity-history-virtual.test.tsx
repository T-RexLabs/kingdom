import { useState, type PropsWithChildren } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { VirtuosoMockContext } from "react-virtuoso";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActivityHistory } from "./activity-history";
import type { ActivityItem } from "./activity-view";
import { mockActivityScrollGeometry } from "./activity-virtual-test-helpers";
import { availableActivityContent } from "./activity-test-helpers";
import { apiFetch } from "@/api/client";

vi.mock("@/api/client", () => ({ apiFetch: vi.fn() }));
const fetchMock = vi.mocked(apiFetch);

function item(sequence: number): ActivityItem {
  return {
    id: `part-${sequence}`,
    eventId: `event-${sequence}`,
    sequence,
    kind: "progress",
    label: `Event ${sequence}`,
  };
}

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

describe("virtual provider activity", () => {
  let restoreGeometry: () => void;
  beforeEach(() => {
    fetchMock.mockReset();
    restoreGeometry = mockActivityScrollGeometry();
  });
  afterEach(() => restoreGeometry());
  it("fits a short activity trail instead of reserving an empty transcript viewport", async () => {
    render(
      <ActivityHistory
        scopeKey="short-trail"
        items={[item(1)]}
        gaps={[]}
        isLoadingMore={false}
        loadError={null}
        loadMore={vi.fn()}
      />,
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(screen.getByText("Event 1")).toBeVisible());
    await waitFor(() => {
      const height = Number.parseFloat(screen.getByTestId("virtuoso-scroller").style.height);
      expect(height).toBeGreaterThan(0);
      expect(height).toBeLessThan(150);
    });
  });
  it("keeps rendered history bounded after the operator loads 50,000 events", async () => {
    const items = Array.from({ length: 50_000 }, (_, index) => item(index + 1));
    render(
      <ActivityHistory
        scopeKey="attempt-a"
        items={items}
        gaps={[]}
        isLoadingMore={false}
        loadError={null}
        loadMore={vi.fn()}
      />,
      { wrapper: Wrapper },
    );
    await waitFor(() =>
      expect(document.querySelectorAll("details").length).toBeGreaterThan(0),
    );
    expect(document.querySelectorAll("details").length).toBeLessThan(30);
    expect(
      screen.getByRole("region", { name: "Provider activity history" }),
    ).toBeInTheDocument();
  });
  it("retains the visible event when earlier pages prepend while live events append", async () => {
    const props = {
      scopeKey: "attempt-a",
      gaps: [],
      isLoadingMore: false,
      loadError: null,
      loadMore: vi.fn(),
    };
    const { rerender } = render(
      <ActivityHistory
        {...props}
        items={Array.from({ length: 100 }, (_, i) => item(i + 201))}
      />,
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(screen.getByText("Event 300")).toBeVisible());
    const scroller = screen.getByTestId("virtuoso-scroller");
    act(() => scroller.scrollTo({ top: 20 * 52 }));
    await waitFor(() => expect(screen.getByText("Event 221")).toBeVisible());
    rerender(
      <ActivityHistory
        {...props}
        items={Array.from({ length: 201 }, (_, i) => item(i + 101))}
      />,
    );
    await waitFor(() => expect(scroller.scrollTop).toBe(120 * 52));
    expect(screen.getByText("Event 221")).toBeVisible();
    expect(
      document.querySelectorAll("[data-activity-event]").length,
    ).toBeLessThan(30);
  });
  it("pauses live following while reading earlier events and resumes explicitly", async () => {
    const user = userEvent.setup();
    const props = {
      scopeKey: "attempt-a",
      gaps: [],
      isLoadingMore: false,
      loadError: null,
      loadMore: vi.fn(),
    };
    const { rerender } = render(
      <ActivityHistory
        {...props}
        items={Array.from({ length: 100 }, (_, i) => item(i + 1))}
      />,
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(screen.getByText("Event 100")).toBeVisible());
    const scroller = screen.getByTestId("virtuoso-scroller");
    act(() => scroller.scrollTo({ top: 20 * 52 }));
    expect(
      await screen.findByRole("button", { name: "Jump to Latest" }),
    ).toBeInTheDocument();
    rerender(
      <ActivityHistory
        {...props}
        items={Array.from({ length: 101 }, (_, i) => item(i + 1))}
      />,
    );
    expect(scroller.scrollTop).toBe(20 * 52);
    await user.click(screen.getByRole("button", { name: "Jump to Latest" }));
    await waitFor(() => expect(screen.getByText("Event 101")).toBeVisible());
    await waitFor(() => expect(screen.queryByRole("button", { name: "Jump to Latest" })).not.toBeInTheDocument());
  });
  it("retains lazy tool disclosures across virtual unmounts and resets them for another attempt", async () => {
    const user = userEvent.setup();
    const content = availableActivityContent("Exact result");
    fetchMock.mockResolvedValue({
      text: "Exact result",
      offset: 0,
      byte_length: content.byte_length,
      sha256: content.sha256,
    });
    const items = Array.from({ length: 100 }, (_, i) => item(i + 1));
    items[99] = { ...items[99], kind: "tool", outputContent: content };
    const props = {
      items,
      gaps: [],
      isLoadingMore: false,
      loadError: null,
      loadMore: vi.fn(),
    };
    const { rerender } = render(
      <ActivityHistory {...props} scopeKey="attempt-a" />,
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(screen.getByText("Event 100")).toBeVisible());
    expect(fetchMock).not.toHaveBeenCalled();
    await user.click(screen.getByText("Event 100"));
    await user.click(await screen.findByText("Output"));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const scroller = screen.getByTestId("virtuoso-scroller");
    act(() => scroller.scrollTo({ top: 0 }));
    await waitFor(() =>
      expect(screen.queryByText("Event 100")).not.toBeInTheDocument(),
    );
    act(() => scroller.scrollTo({ top: scroller.scrollHeight }));
    await waitFor(() => expect(screen.getByText("Exact result")).toBeVisible());
    expect(fetchMock).toHaveBeenCalledTimes(1);
    rerender(<ActivityHistory {...props} scopeKey="attempt-b" />);
    await waitFor(() => expect(screen.getByText("Event 100")).toBeVisible());
    expect(screen.queryByText("Output")).not.toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
