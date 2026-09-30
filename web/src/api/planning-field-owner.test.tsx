import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import { apiFetch, setCSRFToken } from "@/api/client";
import { useUpdatePlanningSessionTitle, useUpdatePlanningSessionModelSelection, useGeneratePlanningSessionTitle, useActivateVariant } from "./hooks-planning";
import { plannerKeys } from "@/features/work-orders/planner-query-keys";
vi.mock("@/api/client", async (original) => ({ ...await original(), apiFetch: vi.fn() }));
beforeEach(() => { sessionStorage.clear(); vi.clearAllMocks(); setCSRFToken("token", "same-binding"); });
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
