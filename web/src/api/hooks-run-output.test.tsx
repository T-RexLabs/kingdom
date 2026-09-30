import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { apiFetch, ApiError } from "./client";
import { useRunArtifacts, outputSnapshotChanged } from "./hooks-run-output";

vi.mock("./client", async () => ({
  ...(await vi.importActual<typeof import("./client")>("./client")),
  apiFetch: vi.fn(),
}));
beforeEach(() => vi.mocked(apiFetch).mockReset());

describe("run output snapshots", () => {
  it("loads bounded pages and resets all retained pages after a lineage conflict", async () => {
    const changed = new ApiError(409, {
      message: "Lineage changed",
      code: "dependency_blocked",
      category: "core",
      retryable: false,
      details: { refresh_required: true },
      correlationId: "test",
    });
    vi.mocked(apiFetch)
      .mockResolvedValueOnce({
        run_id: "r",
        snapshot_id: "old",
        items: [],
        next_cursor: "pinned",
      })
      .mockRejectedValueOnce(changed)
      .mockResolvedValueOnce({ run_id: "r", snapshot_id: "new", items: [] });
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useRunArtifacts("r"), { wrapper });
    await waitFor(() =>
      expect(result.current.data?.pages[0].snapshot_id).toBe("old"),
    );
    await act(async () => {
      await result.current.fetchNextPage();
    });
    await waitFor(() =>
      expect(outputSnapshotChanged(result.current.error)).toBe(true),
    );
    expect(apiFetch).toHaveBeenNthCalledWith(
      2,
      "/runs/r/artifacts",
      expect.objectContaining({ params: { limit: 50, cursor: "pinned" } }),
    );
    await act(async () => {
      await result.current.refresh();
    });
    await waitFor(() =>
      expect(
        result.current.data?.pages.map((page) => page.snapshot_id),
      ).toEqual(["new"]),
    );
    expect(apiFetch).toHaveBeenNthCalledWith(
      3,
      "/runs/r/artifacts",
      expect.objectContaining({ params: { limit: 50, cursor: undefined } }),
    );
    client.clear();
  });
  it("starts an independent bounded snapshot when the agent filter changes", async () => {
    vi.mocked(apiFetch).mockImplementation(async (_path, options) => ({
      run_id: "r",
      snapshot_id: options?.params?.agent_id ?? "all",
      items: [],
      next_cursor: "scoped-next",
    }));
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result, rerender } = renderHook(
      ({ agent }) => useRunArtifacts("r", agent),
      { initialProps: { agent: "" }, wrapper },
    );
    await waitFor(() =>
      expect(result.current.data?.pages[0].snapshot_id).toBe("all"),
    );
    await act(async () => {
      await result.current.fetchNextPage();
    });
    rerender({ agent: "builder-2" });
    await waitFor(() =>
      expect(result.current.data?.pages[0].snapshot_id).toBe("builder-2"),
    );
    expect(result.current.data?.pages).toHaveLength(1);
    expect(apiFetch).toHaveBeenLastCalledWith(
      "/runs/r/artifacts",
      expect.objectContaining({
        params: { limit: 50, cursor: undefined, agent_id: "builder-2" },
      }),
    );
    await act(async () => {
      await result.current.fetchNextPage();
    });
    expect(apiFetch).toHaveBeenLastCalledWith(
      "/runs/r/artifacts",
      expect.objectContaining({
        params: { limit: 50, cursor: "scoped-next", agent_id: "builder-2" },
      }),
    );
    client.clear();
  });
});
