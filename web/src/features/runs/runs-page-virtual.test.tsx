import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { VirtuosoMockContext } from "react-virtuoso";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { mockActivityScrollGeometry } from "@/features/activity/activity-virtual-test-helpers";
import { RunsPage } from "./runs-page";

const router = vi.hoisted(() => ({
  search: {} as { q?: string; state_filter?: string },
  listeners: new Set<() => void>(),
}));
const queryOptions = vi.fn();
const fetchNextPage = vi.fn();
const refetch = vi.fn();

function run(index: number) {
  return {
    run_id: `run-${index}`,
    work_order_id: `work-${index}`,
    work_order_title: `Run ${index}`,
    workflow_id: "work-order-v1",
    autonomous_approvals: true,
    status: "succeeded",
    display_state: "succeeded",
    plan_state: "approved",
    start_state: "started",
    started_at: "2026-09-29T10:00:00Z",
    last_activity_at: "2026-09-29T10:01:00Z",
  };
}
function result(items: ReturnType<typeof run>[]) {
  return {
    data: { pages: [{ items }] },
    error: null,
    isError: false,
    isPending: false,
    isFetching: false,
    isFetchingNextPage: false,
    hasNextPage: true,
    isFetchNextPageError: false,
    fetchNextPage,
    refetch,
  };
}
let collection: ReturnType<typeof result>;
let filtered: ReturnType<typeof result>;

vi.mock("@/theme", () => ({
  useThemeCopy: () => "Work Order",
  ThemeIcon: () => <span aria-hidden="true" />,
}));

vi.mock("@/api/hooks", () => ({
  useInfiniteRuns: (options: { search?: string; stateFilter?: string; limit: number }) => {
    queryOptions(options);
    return options.search ? filtered : collection;
  },
}));
vi.mock("@tanstack/react-router", async () => {
  const { useSyncExternalStore } = await vi.importActual<typeof import("react")>("react");
  return {
    Link: ({ children, to, params, ...props }: { children: ReactNode; to: string; params?: { runId: string } }) => (
      <a href={params ? to.replace("$runId", params.runId) : to} {...props}>{children}</a>
    ),
    useSearch: () => useSyncExternalStore(
      (listener) => { router.listeners.add(listener); return () => { router.listeners.delete(listener); }; },
      () => router.search,
    ),
    useNavigate: () => ({ search }: { search: typeof router.search }) => {
      router.search = search;
      router.listeners.forEach((listener) => listener());
    },
  };
});

let restoreGeometry: () => void;
beforeEach(() => {
  router.search = {};
  queryOptions.mockClear();
  fetchNextPage.mockClear();
  collection = result(Array.from({ length: 50_000 }, (_, index) => run(index)));
  filtered = { ...result([run(49_999)]), hasNextPage: false };
  restoreGeometry = mockActivityScrollGeometry();
});
afterEach(() => { restoreGeometry(); router.listeners.clear(); });

function page() {
  return <VirtuosoMockContext.Provider value={{ viewportHeight: 260, itemHeight: 52 }}><RunsPage /></VirtuosoMockContext.Provider>;
}
function mountedRunRows() {
  return screen.getByRole("region", { name: "Runs" }).querySelectorAll('a[href^="/runs/run-"]');
}

it("bounds the real virtualizer at 50,000 loaded runs while preserving paging and server search", async () => {
  const user = userEvent.setup();
  const view = render(page());
  expect(await screen.findByText("Run 0")).toBeInTheDocument();
  expect(screen.getByText("50000 Loaded")).toBeInTheDocument();
  expect(mountedRunRows().length).toBeGreaterThan(0);
  expect(mountedRunRows().length).toBeLessThan(40);
  expect(queryOptions).toHaveBeenLastCalledWith({ stateFilter: undefined, search: undefined, limit: 50 });

  await user.click(screen.getByRole("button", { name: "Load More Runs" }));
  expect(fetchNextPage).toHaveBeenCalledOnce();
  collection = { ...collection, isFetchingNextPage: true };
  view.rerender(page());
  expect(screen.getByRole("button", { name: "Loading runs…" })).toBeDisabled();
  collection = {
    ...collection,
    isFetchingNextPage: false,
    data: { pages: [...collection.data.pages, { items: Array.from({ length: 50 }, (_, index) => run(50_000 + index)) }] },
  };
  view.rerender(page());
  expect(screen.getByText("50050 Loaded")).toBeInTheDocument();
  expect(mountedRunRows().length).toBeLessThan(40);

  await user.type(screen.getByRole("textbox", { name: "Search Runs" }), "Run 49999");
  await user.click(screen.getByRole("button", { name: "Search" }));
  await waitFor(() => expect(queryOptions).toHaveBeenLastCalledWith({ stateFilter: undefined, search: "Run 49999", limit: 50 }));
  expect(await screen.findByText("Run 49999")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: /Run 49999/ })).toHaveAttribute("href", "/runs/run-49999");
  expect(screen.getByText("1 Loaded")).toBeInTheDocument();
  expect(screen.queryByText("Run 0")).not.toBeInTheDocument();
  expect(mountedRunRows()).toHaveLength(1);
  expect(screen.queryByRole("button", { name: "Load More Runs" })).not.toBeInTheDocument();
});
