import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AnalyticsTimeRange } from "@/lib/analytics-time-range";
import { analyticsSnapshotQueryKey, analyticsExecutionDetailQueryKey, useAnalyticsSnapshot, useAnalyticsExecutionDetailSnapshot } from "./hooks-metrics";

const apiFetchMock = vi.fn();
vi.mock("./client", async () => ({
  ...await vi.importActual<typeof import("./client")>("./client"),
  apiFetch: (...args: unknown[]) => apiFetchMock(...args),
}));
const dates: AnalyticsTimeRange = { kind: "absolute", startAt: "2026-09-01T22:00:00.000Z", endAt: "2026-09-08T22:00:00.000Z" };
let client: QueryClient;
beforeEach(() => { client = new QueryClient({ defaultOptions: { queries: { retry: false } } }); apiFetchMock.mockReset(); });
afterEach(() => client.clear());
function wrapper({ children }: { children: ReactNode }) { return <QueryClientProvider client={client}>{children}</QueryClientProvider>; }

describe("analytics calendar queries", () => {
  it("sends exact UTC boundaries without window minutes to both aggregate endpoints", async () => {
    apiFetchMock.mockResolvedValue({ snapshot_at: dates.endAt });
    const hook = renderHook(() => ({ snapshot: useAnalyticsSnapshot(dates), detail: useAnalyticsExecutionDetailSnapshot(dates) }), { wrapper });
    await waitFor(() => expect(hook.result.current.snapshot.isSuccess && hook.result.current.detail.isSuccess).toBe(true));
    for (const path of ["/analytics/snapshot", "/analytics/execution-detail"]) {
      expect(apiFetchMock).toHaveBeenCalledWith(path, expect.objectContaining({ params: { start_at: dates.startAt, end_at: dates.endAt }, signal: expect.any(AbortSignal) }));
    }
  });
  it("separates equal-duration date windows and rolling windows in the query cache", () => {
    const other: AnalyticsTimeRange = { kind: "absolute", startAt: "2026-09-08T22:00:00.000Z", endAt: "2026-09-15T22:00:00.000Z" };
    const relative: AnalyticsTimeRange = { kind: "relative", minutes: 10080 };
    expect(analyticsSnapshotQueryKey(dates)).not.toEqual(analyticsSnapshotQueryKey(other));
    expect(analyticsSnapshotQueryKey(dates)).not.toEqual(analyticsSnapshotQueryKey(relative));
    expect(analyticsExecutionDetailQueryKey(dates, null)).not.toEqual(analyticsExecutionDetailQueryKey(other, null));
  });
  it("keeps selected-run details independent of calendar or relative windows", async () => {
    apiFetchMock.mockResolvedValue({ execution_detail: {} });
    const hook = renderHook(({ range }: { range: AnalyticsTimeRange }) => useAnalyticsExecutionDetailSnapshot(range, "run-1"), { wrapper, initialProps: { range: dates as AnalyticsTimeRange } });
    await waitFor(() => expect(hook.result.current.isSuccess).toBe(true));
    expect(apiFetchMock).toHaveBeenCalledWith("/analytics/execution-detail", expect.objectContaining({ params: { run_id: "run-1" } }));
    hook.rerender({ range: { kind: "relative", minutes: 30 } });
    expect(apiFetchMock).toHaveBeenCalledTimes(1);
    expect(analyticsExecutionDetailQueryKey(dates, "run-1")).toEqual(analyticsExecutionDetailQueryKey({ kind: "relative", minutes: 30 }, "run-1"));
  });
});
