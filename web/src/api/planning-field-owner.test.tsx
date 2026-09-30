import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import { apiFetch, setCSRFToken } from "@/api/client";
import { useUpdatePlanningSessionTitle, useUpdatePlanningSessionModelSelection, useGeneratePlanningSessionTitle, useActivateVariant } from "./hooks-planning";
import { plannerKeys } from "@/features/work-orders/planner-query-keys";
vi.mock("@/api/client", async (original) => ({ ...await original(), apiFetch: vi.fn() }));
beforeEach(() => { sessionStorage.clear(); vi.clearAllMocks(); vi.mocked(apiFetch).mockReset(); setCSRFToken("token", "same-binding"); });
it.each(["title", "model"] as const)("reconciles delayed %s saves only in the originating session", async (field) => {
  const client = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const a = { planning_session_id: "session-a", title: "A", title_state: "custom", model_selection: { model: "provider/a" } };
  const b = { planning_session_id: "session-b", title: "B", title_state: "custom", model_selection: { model: "provider/b" } };
  client.setQueryData(plannerKeys.detail("session-a"), a);
  client.setQueryData(plannerKeys.detail("session-b"), b);
  let finish!: (response: unknown) => void;
  vi.mocked(apiFetch).mockReturnValueOnce(new Promise((resolve) => { finish = resolve; }));
  const view = renderHook(({ id }) => {
    const title = useUpdatePlanningSessionTitle(id);
    const model = useUpdatePlanningSessionModelSelection(id);
    return { title, model };
  }, { initialProps: { id: "session-a" }, wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
  let request!: Promise<unknown>;
  act(() => { request = field === "title" ? view.result.current.title.mutateAsync({ title: "Changed A", key: "original-key" }) : view.result.current.model.mutateAsync({ modelSelection: { model: "provider/changed-a" }, key: "original-key" }); });
  await waitFor(() => expect(apiFetch).toHaveBeenCalledTimes(1));
  view.rerender({ id: "session-b" });
  const updatedB = { ...b, ...(field === "title" ? { title: "Changed B" } : { model_selection: { model: "provider/changed-b" } }) };
  vi.mocked(apiFetch).mockResolvedValueOnce(updatedB);
  await act(async () => {
    await (field === "title" ? view.result.current.title.mutateAsync({ title: "Changed B", key: "b-key" }) : view.result.current.model.mutateAsync({ modelSelection: { model: "provider/changed-b" }, key: "b-key" }));
  });
  const updated = { ...a, ...(field === "title" ? { title: "Changed A" } : { model_selection: { model: "provider/changed-a" } }) };
  await act(async () => { finish(updated); await request; });
  expect(client.getQueryData(plannerKeys.detail("session-b"))).toEqual(updatedB);
  expect(client.getQueryData(plannerKeys.detail("session-a"))).toEqual(updated);
});

it("keeps a delayed generated-title operation owned by its originating session", async () => {
  const client = new QueryClient();
  const a = { planning_session_id: "session-a", title: "A" };
  const b = { planning_session_id: "session-b", title: "B" };
  client.setQueryData(plannerKeys.detail("session-a"), a);
  client.setQueryData(plannerKeys.detail("session-b"), b);
  let finish!: (response: unknown) => void;
  vi.mocked(apiFetch).mockReturnValueOnce(new Promise((resolve) => { finish = resolve; }));
  const view = renderHook(({ id }) => useGeneratePlanningSessionTitle(id), { initialProps: { id: "session-a" }, wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
  let request!: Promise<unknown>;
  act(() => { request = view.result.current.mutateAsync({ key: "title-operation-key" }); });
  await waitFor(() => expect(apiFetch).toHaveBeenCalledTimes(1));
  view.rerender({ id: "session-b" });
  await act(async () => { finish({ planning_session_id: "session-a", run_id: "title-run-a" }); await request; });
  expect(client.getQueryData(plannerKeys.detail("session-b"))).toEqual(b);
  expect(client.getQueryData(plannerKeys.detail("session-a"))).toEqual({ ...a, current_title_run_id: "title-run-a" });
});

it.each(["title", "model"] as const)("recovers a restored %s save by reading its original outcome after authentication renewal", async (field) => {
  const client = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const initial = { planning_session_id: "session-a", title: "Before", title_state: "custom", model_selection: { model: "provider/before" } };
  const updated = { ...initial, ...(field === "title" ? { title: "Saved title" } : { model_selection: { model: "provider/saved", thinking_effort: "none" } }) };
  const mount = (cache: QueryClient) => renderHook(() => ({
    title: useUpdatePlanningSessionTitle("session-a"),
    model: useUpdatePlanningSessionModelSelection("session-a"),
  }), { wrapper: ({ children }) => <QueryClientProvider client={cache}>{children}</QueryClientProvider> });
  vi.mocked(apiFetch).mockRejectedValueOnce(new TypeError("Response lost"));
  const original = mount(client);
  await act(async () => {
    const request = field === "title"
      ? original.result.current.title.mutateAsync({ title: "Saved title", key: "original-key" })
      : original.result.current.model.mutateAsync({ modelSelection: updated.model_selection, key: "original-key" });
    await expect(request).rejects.toThrow("Response lost");
  });
  original.unmount();
  setCSRFToken("renewed-token", "renewed-binding");
  const restoredClient = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  restoredClient.setQueryData(plannerKeys.detail("session-a"), initial);
  vi.mocked(apiFetch).mockResolvedValueOnce({ state: "completed", response_status: 200, response_body: updated });
  const restored = mount(restoredClient);
  await act(async () => { await restored.result.current[field].retryRecovery(); });
  expect(apiFetch).toHaveBeenLastCalledWith("/mutation-outcomes/original-key", { params: { operation: "planning-session-update" } });
  expect(apiFetch).toHaveBeenCalledTimes(2);
  expect(restoredClient.getQueryData(plannerKeys.detail("session-a"))).toEqual(updated);
  expect(restored.result.current[field].hasPendingRecovery).toBe(false);
});
it("refreshes the originating session after a delayed branch activation", async () => {
  const client = new QueryClient();
  const invalidate = vi.spyOn(client, "invalidateQueries");
  let finish!: (response: unknown) => void;
  vi.mocked(apiFetch).mockReturnValueOnce(new Promise((resolve) => { finish = resolve; }));
  const view = renderHook(({ id }) => useActivateVariant(id), { initialProps: { id: "session-a" }, wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
  let request!: Promise<unknown>;
  act(() => { request = view.result.current.mutateAsync({ value: "branch-a", key: "branch-key" }); });
  await waitFor(() => expect(apiFetch).toHaveBeenCalledTimes(1));
  view.rerender({ id: "session-b" });
  await act(async () => { finish({ planning_session_id: "session-a", planning_message_id: "branch-a" }); await request; });
  expect(invalidate.mock.calls.map(([options]) => options?.queryKey)).toEqual([
    plannerKeys.messages("session-a"), plannerKeys.detail("session-a"), ["planning-sessions", "session-a", "variants"],
  ]);
});
