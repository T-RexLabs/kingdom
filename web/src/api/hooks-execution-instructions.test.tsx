import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { useRunExecutionInstructions } from "./hooks-execution-instructions";
import { ApiError } from "./client";

const fetchPrompt = vi.hoisted(() => vi.fn());
vi.mock("./client", async () => ({
  ...(await vi.importActual<typeof import("./client")>("./client")),
  apiFetch: fetchPrompt,
}));
afterEach(() => fetchPrompt.mockReset());
function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
let client: QueryClient;
it("fetches lazily with exact run/attempt scope and never reuses another attempt's prompt", async () => {
  client = new QueryClient();
  fetchPrompt.mockImplementation(async (path: string) =>
    path.includes("old%3A1") ? "  historical\n" : "current prompt",
  );
  const view = renderHook(
    ({ enabled, attempt }) =>
      useRunExecutionInstructions("run/id", attempt, enabled),
    { wrapper, initialProps: { enabled: false, attempt: "old:1" } },
  );
  expect(fetchPrompt).not.toHaveBeenCalled();
  view.rerender({ enabled: true, attempt: "old:1" });
  await waitFor(() => expect(view.result.current.data).toBe("  historical\n"));
  expect(fetchPrompt).toHaveBeenCalledWith(
    "/runs/run%2Fid/executions/old%3A1/instructions",
    expect.objectContaining({
      responseType: "text",
      signal: expect.any(AbortSignal),
    }),
  );
  view.rerender({ enabled: true, attempt: "current" });
  expect(view.result.current.data).toBeUndefined();
  await waitFor(() => expect(view.result.current.data).toBe("current prompt"));
  view.unmount();
  client.clear();
});
it("does not automatically retry a missing recorded prompt or substitute current state", async () => {
  client = new QueryClient();
  fetchPrompt.mockRejectedValue(
    new ApiError(404, {
      code: "dependency_blocked",
      message: "No recorded prompt",
      retryable: false,
      category: "core",
      details: {},
      correlationId: "test",
    }),
  );
  const view = renderHook(
    () => useRunExecutionInstructions("run", "old", true),
    { wrapper },
  );
  await waitFor(() => expect(view.result.current.isError).toBe(true));
  expect(view.result.current.data).toBeUndefined();
  expect(fetchPrompt).toHaveBeenCalledOnce();
  view.unmount();
  client.clear();
});
