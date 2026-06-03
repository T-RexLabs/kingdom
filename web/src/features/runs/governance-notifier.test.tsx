import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { NotificationProvider } from "@/components/notifications";
import { NotificationStack } from "@/components/notifications/notification-stack";
import { GovernanceNotifierMount } from "./governance-notifier";

const { navigateMock, pathnameMock } = vi.hoisted(() => ({
	navigateMock: vi.fn(),
	pathnameMock: vi.fn(() => "/"),
}));

function buildRun(overrides: Record<string, unknown> = {}) {
	return {
		run_id: "run-1",
		decree_id: "decree-1",
		decree_title: "Title",
		workflow_id: "royal-decree-v1",
		status: "awaiting_human_confirmation",
		display_state: "awaiting_final_decision",
		plan_state: "approved",
		created_at: "2026-03-06T10:00:00.000Z",
		updated_at: "2026-03-06T10:01:00.000Z",
		last_activity_at: "2026-03-06T10:01:00.000Z",
		autonomous_approvals: false,
		pending_approval_key: "final:gate-1",
		...overrides,
	};
}

let mockRuns = [buildRun()];

vi.mock("@/api/hooks", () => ({
	usePendingGovernanceRuns: () => ({ data: { items: mockRuns } }),
}));

vi.mock("@tanstack/react-router", () => ({
	useNavigate: () => navigateMock,
	useRouterState: ({ select }: { select?: (state: { location: { pathname: string } }) => unknown } = {}) => {
		const state = { location: { pathname: pathnameMock() } };
		return typeof select === "function" ? select(state) : state;
	},
}));

describe("governance notifier", () => {
	beforeEach(() => {
		mockRuns = [buildRun()];
		navigateMock.mockReset();
		pathnameMock.mockReset();
		pathnameMock.mockReturnValue("/");
		sessionStorage.clear();
		vi.useFakeTimers();
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	it("keeps a dismissed final-approval notification hidden while only timestamps change", () => {
		const view = render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.getByText("Final Approval Required")).toBeInTheDocument();
		act(() => {
			fireEvent.click(screen.getByRole("button", { name: /Dismiss notification/i }));
			vi.advanceTimersByTime(300);
		});
		expect(screen.queryByText("Final Approval Required")).not.toBeInTheDocument();

		mockRuns = [buildRun({ updated_at: "2026-03-06T10:02:00.000Z", last_activity_at: "2026-03-06T10:02:00.000Z" })];
		view.rerender(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.queryByText("Final Approval Required")).not.toBeInTheDocument();
	});

	it("keeps a dismissed plan-approval notification hidden while only timestamps change", () => {
		mockRuns = [buildRun({
			status: "running",
			display_state: "awaiting_plan_decision",
			plan_state: "awaiting_king_decision",
			pending_approval_key: "plan:planrev-1",
		})];
		const view = render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.getByText("Plan Approval Required")).toBeInTheDocument();
		act(() => {
			fireEvent.click(screen.getByRole("button", { name: /Dismiss notification/i }));
			vi.advanceTimersByTime(300);
		});
		expect(screen.queryByText("Plan Approval Required")).not.toBeInTheDocument();

		mockRuns = [buildRun({
			status: "running",
			display_state: "awaiting_plan_decision",
			plan_state: "awaiting_king_decision",
			updated_at: "2026-03-06T10:02:00.000Z",
			last_activity_at: "2026-03-06T10:02:00.000Z",
			pending_approval_key: "plan:planrev-1",
		})];
		view.rerender(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.queryByText("Plan Approval Required")).not.toBeInTheDocument();
	});

	it("recreates a dismissed notification when the approval gate changes", () => {
		const view = render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		act(() => {
			fireEvent.click(screen.getByRole("button", { name: /Dismiss notification/i }));
			vi.advanceTimersByTime(300);
		});
		expect(screen.queryByText("Final Approval Required")).not.toBeInTheDocument();

		mockRuns = [buildRun({
			updated_at: "2026-03-06T10:02:00.000Z",
			pending_approval_key: "final:gate-2",
		})];
		view.rerender(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.getByText("Final Approval Required")).toBeInTheDocument();
	});

	it("does not recreate a notification after Open Runs until the approval gate changes", () => {
		render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		act(() => {
			fireEvent.click(screen.getByRole("button", { name: "Open Runs" }));
			vi.advanceTimersByTime(300);
		});

		expect(navigateMock).toHaveBeenCalledWith({ to: "/runs" });
		expect(screen.queryByText("Final Approval Required")).not.toBeInTheDocument();
	});

	it("keeps a dismissed notification hidden when entering and leaving the active run route", () => {
		const view = render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.getByText("Final Approval Required")).toBeInTheDocument();
		act(() => {
			fireEvent.click(screen.getByRole("button", { name: /Dismiss notification/i }));
			vi.advanceTimersByTime(300);
		});
		expect(screen.queryByText("Final Approval Required")).not.toBeInTheDocument();

		pathnameMock.mockReturnValue("/runs/run-1");
		view.rerender(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});
		expect(screen.queryByText("Final Approval Required")).not.toBeInTheDocument();

		pathnameMock.mockReturnValue("/runs");
		view.rerender(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});
		expect(screen.queryByText("Final Approval Required")).not.toBeInTheDocument();
	});

	it("suppresses the currently viewed run detail approval from the global notifier", () => {
		pathnameMock.mockReturnValue("/runs/run-1");
		mockRuns = [
			buildRun({ run_id: "run-1", decree_title: "Current Run", pending_approval_key: "final:gate-1" }),
			buildRun({
				run_id: "run-2",
				decree_id: "decree-2",
				decree_title: "Other Run",
				status: "running",
				display_state: "awaiting_plan_decision",
				plan_state: "awaiting_king_decision",
				pending_approval_key: "plan:planrev-2",
				updated_at: "2026-03-06T10:02:00.000Z",
				last_activity_at: "2026-03-06T10:02:00.000Z",
			}),
		];

		render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.getByText("Plan Approval Required")).toBeInTheDocument();
		expect(screen.getByText("Other Run")).toBeInTheDocument();
		expect(screen.queryByText("Current Run")).not.toBeInTheDocument();
	});

	it("groups multiple pending governance runs into a single summary notification", () => {
		mockRuns = [
			buildRun({
				run_id: "run-plan",
				decree_id: "decree-plan",
				decree_title: "Plan Run",
				status: "running",
				display_state: "awaiting_plan_decision",
				plan_state: "awaiting_king_decision",
				pending_approval_key: "plan:planrev-1",
			}),
			buildRun({
				run_id: "run-final-old",
				decree_id: "decree-final-old",
				decree_title: "Final Old",
				pending_approval_key: "final:gate-1",
				updated_at: "2026-03-06T10:02:00.000Z",
				last_activity_at: "2026-03-06T10:02:00.000Z",
			}),
			buildRun({
				run_id: "run-final-latest",
				decree_id: "decree-final-latest",
				decree_title: "Final Latest",
				pending_approval_key: "final:gate-2",
				updated_at: "2026-03-06T10:03:00.000Z",
				last_activity_at: "2026-03-06T10:03:00.000Z",
			}),
		];

		render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.getByText("Pending Manual Approvals")).toBeInTheDocument();
		expect(screen.getByText("1 plan approval, 2 final approvals across 3 runs. Latest: Final Latest.")).toBeInTheDocument();
		expect(screen.getAllByRole("status")).toHaveLength(1);
	});

	it("shows a notification for an older run returned by the pending-governance feed", () => {
		mockRuns = [
			buildRun({
				run_id: "run-legacy",
				decree_id: "decree-legacy",
				decree_title: "Legacy Run",
				created_at: "2026-03-01T10:00:00.000Z",
			}),
		];

		render(
			<NotificationProvider>
				<GovernanceNotifierMount />
				<NotificationStack />
			</NotificationProvider>,
		);
		act(() => {
			vi.runAllTimers();
		});

		expect(screen.getByText("Final Approval Required")).toBeInTheDocument();
		expect(screen.getByText("Legacy Run")).toBeInTheDocument();
	});
});
