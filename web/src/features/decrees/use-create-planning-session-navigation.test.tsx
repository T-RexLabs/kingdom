import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import { useCreatePlanningSessionNavigation } from "./use-create-planning-session-navigation";

const createMutateAsync = vi.fn();
const resetMock = vi.fn();
let idempotencyKeyCounter = 0;

vi.mock("@/api/hooks", async () => {
	const actual = await vi.importActual<typeof import("@/api/hooks")>("@/api/hooks");
	return {
		...actual,
		newIdempotencyKey: (scope: string) => `${scope}-test-${++idempotencyKeyCounter}`,
		useCreatePlanningSession: () => ({
			mutateAsync: createMutateAsync,
			reset: resetMock,
			error: null,
			isPending: false,
		}),
	};
});

function makeClient() {
	return new QueryClient({ defaultOptions: { mutations: { retry: false } } });
}

function makeWrapper(client: QueryClient) {
	return function Wrapper({ children }: { children: ReactNode }) {
		return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
	};
}

describe("useCreatePlanningSessionNavigation", () => {
	beforeEach(() => {
		createMutateAsync.mockReset();
		resetMock.mockReset();
		idempotencyKeyCounter = 0;
	});

	it("reuses the same idempotency key when retrying a pending create", async () => {
		const client = makeClient();
		const wrapper = makeWrapper(client);
		const body = { model_selection: { model: "gpt-5.4" } };
		createMutateAsync
			.mockRejectedValueOnce(new ApiError(409, {
				message: "reservation pending",
				code: "idempotency_pending",
				retryable: true,
				category: "core",
				details: {},
				correlationId: "test",
			}))
			.mockResolvedValueOnce({
				session: {
					planning_session_id: "planning-session-1",
					title: "Planner kickoff",
					title_state: "custom",
					lifecycle_state: "active",
					conversation_state: "ready",
					current_question_message_id: "msg_1",
					created_at: "2026-03-01T12:00:00Z",
					updated_at: "2026-03-01T12:00:00Z",
					project_root: "/tmp/project",
					model_selection: { model: "gpt-5.4" },
				},
				opening_message: null,
			});

		const { result } = renderHook(() => useCreatePlanningSessionNavigation(), { wrapper });

		let err: unknown;
		await act(async () => {
			try {
				await result.current.mutateAsync(body);
			} catch (caught) {
				err = caught;
			}
		});
		expect(err).toBeInstanceOf(Error);
		expect((err as Error).message).toBe("reservation pending");
		await act(async () => {
			await Promise.resolve();
		});

		expect(result.current.hasRecovery).toBe(true);
		const firstRequest = createMutateAsync.mock.calls[0]?.[0];

		await act(async () => {
			await result.current.retryRecovery();
		});

		expect(createMutateAsync.mock.calls[1]?.[0]).toEqual(firstRequest);
		expect(result.current.hasRecovery).toBe(false);
		expect(resetMock).toHaveBeenCalled();
	});

	it("does not replace an ambiguous request when the form changes", async () => {
		const client = makeClient();
		const wrapper = makeWrapper(client);
		const originalBody = { model_selection: { model: "gpt-5.4" } };
		const updatedBody = { model_selection: { model: "gpt-5.4-mini", thinking_effort: "low" } };
		createMutateAsync
			.mockRejectedValueOnce(new TypeError("connection lost"))
			.mockResolvedValueOnce({
				session: {
					planning_session_id: "planning-session-2",
					title: "Planner kickoff",
					title_state: "custom",
					lifecycle_state: "active",
					conversation_state: "ready",
					current_question_message_id: "msg_2",
					created_at: "2026-03-01T12:00:00Z",
					updated_at: "2026-03-01T12:00:00Z",
					project_root: "/tmp/project",
					model_selection: { model: "gpt-5.4-mini", thinking_effort: "low" },
				},
				opening_message: null,
			});

		const { result } = renderHook(() => useCreatePlanningSessionNavigation(), { wrapper });

		await act(async () => {
			await expect(result.current.mutateAsync(originalBody)).rejects.toThrow("connection lost");
		});
		expect(result.current.hasRecovery).toBe(true);

		await act(async () => {
			await result.current.mutateAsync(updatedBody);
		});

		expect(createMutateAsync).toHaveBeenCalledTimes(2);
  const firstRequest = createMutateAsync.mock.calls[0]?.[0];
  const secondRequest = createMutateAsync.mock.calls[1]?.[0];
  expect(firstRequest?.body).toEqual(originalBody);
  expect(secondRequest?.body).toEqual(originalBody);
  expect(firstRequest?.key).toMatch(/^kd1\./);
  expect(secondRequest?.key).toBe(firstRequest?.key);
		expect(result.current.hasRecovery).toBe(false);
	});
});
