import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render as rtlRender, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { CurrentPlanPanel } from "./current-plan-panel";

const mutatePlanApproval = vi.fn().mockResolvedValue({ status: "approved" });
const fetchNextGovernancePage = vi.fn().mockResolvedValue(undefined);
const useRunGovernanceDecisionsMock = vi.fn();
const useRunLatestGovernanceDecisionMock = vi.fn();

let latestGovernanceDecisionItems = [
	{
		decision_id: "decision-1",
		stage: "plan",
		decision: "needs_rework",
		feedback_markdown: "Tighten the rollout scope before approval.",
		created_at: "2026-03-06T11:00:00.000Z",
	},
];

let governanceDecisionHistoryPages = [
	{
		run_id: "run-1",
		items: latestGovernanceDecisionItems,
		next_cursor: "cursor-1",
	},
];

let runPlanState: {
	run_id: string;
	plan_state: string;
	revision_status: "pending" | "ready";
	plan_revision_id?: string;
	components: Array<{
		node_id: string;
		planning_output_id: string;
		created_at: string;
		content: string;
	}>;
	format: string;
	content: string;
	created_at: string;
	updated_at: string;
	last_activity_at: string;
} = {
	run_id: "run-1",
	plan_state: "awaiting_plan_decision",
	revision_status: "ready",
	plan_revision_id: "planrev-1",
	components: [{
		node_id: "plan_drafting",
		planning_output_id: "planout-1",
		created_at: "2026-03-06T10:00:00.000Z",
		content: "Execution plan",
	}],
	format: "markdown",
	content: [
		"## Execution Plan",
		"",
		"| Step | Owner |",
		"| --- | --- |",
		"| Implement endpoint | Builder Builder |",
		"| Verify response | Scout |",
	].join("\n"),
	created_at: "2026-03-06T10:00:00.000Z",
	updated_at: "2026-03-06T10:00:00.000Z",
	last_activity_at: "2026-03-06T10:00:00.000Z",
};

let runPlanLoading = false;
let runPlanError: Error | null = null;
let runPlanAvailable = true;
let queryClient: QueryClient;

function render(ui: ReactNode) {
  return rtlRender(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>);
}

vi.mock("@/api/hooks", () => ({
  useRunPlan: () => ({
    data: runPlanAvailable ? runPlanState : undefined,
    isLoading: runPlanLoading,
    error: runPlanError,
  }),
  useRunLatestGovernanceDecision: (...args: unknown[]) => useRunLatestGovernanceDecisionMock(...args),
  useRunGovernanceDecisions: (...args: unknown[]) => useRunGovernanceDecisionsMock(...args),
  useCreatePlanApproval: () => ({
    mutateAsync: mutatePlanApproval,
    isPending: false,
    isSuccess: false,
    data: null,
    error: null,
  }),
  newIdempotencyKey: () => "idem-plan-approval-test",
}));

describe("current plan panel", () => {
  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
    mutatePlanApproval.mockClear();
    fetchNextGovernancePage.mockClear();
		useRunLatestGovernanceDecisionMock.mockClear();
		useRunGovernanceDecisionsMock.mockClear();
		latestGovernanceDecisionItems = [
			{
				decision_id: "decision-1",
				stage: "plan",
				decision: "needs_rework",
				feedback_markdown: "Tighten the rollout scope before approval.",
				created_at: "2026-03-06T11:00:00.000Z",
			},
		];
		governanceDecisionHistoryPages = [
			{
				run_id: "run-1",
				items: latestGovernanceDecisionItems,
				next_cursor: "cursor-1",
			},
		];
		useRunLatestGovernanceDecisionMock.mockImplementation(() => ({
			data: {
				run_id: "run-1",
				items: latestGovernanceDecisionItems,
			},
		}));
		useRunGovernanceDecisionsMock.mockImplementation((_runId: string, options?: { enabled?: boolean }) => ({
			data: options?.enabled
				? {
					pages: governanceDecisionHistoryPages,
				}
				: undefined,
			hasNextPage: options?.enabled ? true : false,
			isFetchingNextPage: false,
			fetchNextPage: fetchNextGovernancePage,
		}));
		runPlanState = {
			run_id: "run-1",
			plan_state: "awaiting_plan_decision",
			revision_status: "ready",
			plan_revision_id: "planrev-1",
			components: [{
				node_id: "plan_drafting",
				planning_output_id: "planout-1",
				created_at: "2026-03-06T10:00:00.000Z",
				content: "Execution plan",
			}],
			format: "markdown",
			content: [
				"## Execution Plan",
				"",
				"| Step | Owner |",
				"| --- | --- |",
				"| Implement endpoint | Builder Builder |",
				"| Verify response | Scout |",
			].join("\n"),
			created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
		};
		runPlanLoading = false;
		runPlanError = null;
		runPlanAvailable = true;
  });

  afterEach(() => {
    queryClient.clear();
    vi.useRealTimers();
  });

  it("uses fixed scroll panel without expand toggle", () => {
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: false,
          status: "running",
          display_state: "running",
          plan_state: "approved",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    expect(screen.getByText("Current Plan")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Expand full plan/i })).not.toBeInTheDocument();
  });

  it("does not render an updated timestamp in the current plan header", () => {
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: false,
          status: "running",
          display_state: "running",
          plan_state: "approved",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    expect(screen.queryByText(/Updated/i)).not.toBeInTheDocument();
  });

  it("renders markdown tables in current plan content", () => {
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: false,
          status: "running",
          display_state: "running",
          plan_state: "approved",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    expect(screen.getByRole("table")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Step" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "Builder Builder" })).toBeInTheDocument();
  });

  it("renders direct plan approval actions without a selector toggle", () => {
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: false,
          status: "running",
          display_state: "awaiting_plan_decision",
          plan_state: "awaiting_plan_decision",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    expect(screen.queryByRole("group", { name: /Plan approval decision/i })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Confirm Plan/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^Request Rework$/i })).toBeInTheDocument();
  });

  it("keeps autonomous runs in awaiting_plan_decision non-actionable", () => {
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: true,
          status: "running",
          display_state: "awaiting_plan_decision",
          plan_state: "awaiting_plan_decision",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    expect(screen.getByText("Awaiting Plan Decision")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Confirm Plan/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /^Request Rework$/i })).not.toBeInTheDocument();
  });

  it("allows confirming the current plan without notes", async () => {
    const user = userEvent.setup();
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: false,
          status: "running",
          display_state: "awaiting_plan_decision",
          plan_state: "awaiting_plan_decision",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    await user.click(screen.getByRole("button", { name: /Confirm Plan/i }));

    expect(mutatePlanApproval).toHaveBeenCalledWith({
      body: {
        action: "confirm",
			plan_revision_id: "planrev-1",
      },
      key: expect.stringMatching(/^kd1\./),
    });
  });

  it("requires notes only when requesting plan rework", async () => {
    const user = userEvent.setup();
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: false,
          status: "running",
          display_state: "awaiting_plan_decision",
          plan_state: "awaiting_plan_decision",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    await user.click(screen.getByRole("button", { name: /^Request Rework$/i }));

    expect(screen.getByText("Add rework guidance before requesting rework.")).toBeInTheDocument();
    expect(mutatePlanApproval).not.toHaveBeenCalled();
  });

	it("shows a non-error sync state while the approval-ready plan revision is still pending", () => {
		runPlanState = {
			...runPlanState,
			revision_status: "pending",
			plan_revision_id: undefined,
			content: "",
		};

		render(
			<CurrentPlanPanel
				run={{
					run_id: "run-1",
					work_order_id: "work order-1",
					work_order_title: "Title",
					workflow_id: "work-order-v1",
					autonomous_approvals: false,
					status: "running",
					display_state: "awaiting_plan_decision",
					plan_state: "awaiting_plan_decision",
					created_at: "2026-03-06T10:00:00.000Z",
					updated_at: "2026-03-06T10:00:00.000Z",
					last_activity_at: "2026-03-06T10:00:00.000Z",
					start_state: "not_started",
				}}
			/>,
		);

		expect(screen.getByText("Syncing the current plan revision for review.")).toBeInTheDocument();
		expect(screen.getByText(/approval actions will unlock automatically once the current revision is ready/i)).toBeInTheDocument();
		expect(screen.queryByRole("button", { name: /Confirm Plan/i })).not.toBeInTheDocument();
		expect(screen.queryByRole("button", { name: /^Request Rework$/i })).not.toBeInTheDocument();
	});

	it("shows an explicit error state when the current plan revision cannot be loaded", () => {
		runPlanError = new Error("plan snapshot unavailable");
		runPlanAvailable = false;

		render(
			<CurrentPlanPanel
				run={{
					run_id: "run-1",
					work_order_id: "work order-1",
					work_order_title: "Title",
					workflow_id: "work-order-v1",
					autonomous_approvals: false,
					status: "running",
					display_state: "awaiting_plan_decision",
					plan_state: "awaiting_plan_decision",
					created_at: "2026-03-06T10:00:00.000Z",
					updated_at: "2026-03-06T10:00:00.000Z",
					last_activity_at: "2026-03-06T10:00:00.000Z",
					start_state: "not_started",
				}}
			/>,
		);

		expect(screen.getByText(/current plan revision could not be loaded/i)).toBeInTheDocument();
		expect(screen.queryByRole("button", { name: /Confirm Plan/i })).not.toBeInTheDocument();
		expect(screen.queryByRole("button", { name: /^Request Rework$/i })).not.toBeInTheDocument();
	});

	it("keeps the last ready plan visible when a background refresh fails", () => {
		runPlanError = new Error("plan refresh failed");

		render(
			<CurrentPlanPanel
				run={{
					run_id: "run-1",
					work_order_id: "work order-1",
					work_order_title: "Title",
					workflow_id: "work-order-v1",
					autonomous_approvals: false,
					status: "running",
					display_state: "awaiting_plan_decision",
					plan_state: "awaiting_plan_decision",
					created_at: "2026-03-06T10:00:00.000Z",
					updated_at: "2026-03-06T10:00:00.000Z",
					last_activity_at: "2026-03-06T10:00:00.000Z",
					start_state: "not_started",
				}}
			/>,
		);

		expect(screen.getByText(/showing the last successful snapshot while the latest refresh recovers/i)).toBeInTheDocument();
		expect(screen.getByRole("table")).toBeInTheDocument();
		expect(screen.getByRole("button", { name: /Confirm Plan/i })).toBeInTheDocument();
		expect(screen.getByRole("button", { name: /^Request Rework$/i })).toBeInTheDocument();
	});

  it("submits the current plan message id and notes when requesting rework", async () => {
    const user = userEvent.setup();
    render(
      <CurrentPlanPanel
        run={{
          run_id: "run-1",
          work_order_id: "work order-1",
          work_order_title: "Title",
          workflow_id: "work-order-v1",
          autonomous_approvals: false,
          status: "running",
          display_state: "awaiting_plan_decision",
          plan_state: "awaiting_plan_decision",
          created_at: "2026-03-06T10:00:00.000Z",
			updated_at: "2026-03-06T10:00:00.000Z",
			last_activity_at: "2026-03-06T10:00:00.000Z",
			start_state: "not_started",
        }}
      />,
    );

    await user.type(screen.getByLabelText(/^Notes$/i), "Refine the rollout scope.");
    await user.click(screen.getByRole("button", { name: /^Request Rework$/i }));

    expect(mutatePlanApproval).toHaveBeenCalledWith({
      body: {
        action: "rework",
			plan_revision_id: "planrev-1",
        reason: "Refine the rollout scope.",
      },
      key: expect.stringMatching(/^kd1\./),
    });
  });

	it("opens governance decision history from the latest guidance card", async () => {
		const user = userEvent.setup();
		render(
			<CurrentPlanPanel
				run={{
					run_id: "run-1",
					work_order_id: "work order-1",
					work_order_title: "Title",
					workflow_id: "work-order-v1",
					autonomous_approvals: false,
					status: "running",
					display_state: "running",
					plan_state: "approved",
					created_at: "2026-03-06T10:00:00.000Z",
					updated_at: "2026-03-06T10:00:00.000Z",
					last_activity_at: "2026-03-06T10:00:00.000Z",
					start_state: "not_started",
				}}
			/>,
		);

		expect(screen.getByText("Operator Guidance")).toBeInTheDocument();
		expect(screen.getByText("Tighten the rollout scope before approval.")).toBeInTheDocument();

		await user.click(screen.getByRole("button", { name: /Requested Plan Rework/i }));

		expect(screen.getByRole("dialog", { name: "Operator Decision History" })).toBeInTheDocument();
		expect(screen.getAllByText("Requested Plan Rework").length).toBeGreaterThan(0);
	});

	it("keeps governance history disabled until the history modal opens", async () => {
		const user = userEvent.setup();
		render(
			<CurrentPlanPanel
				run={{
					run_id: "run-1",
					work_order_id: "work order-1",
					work_order_title: "Title",
					workflow_id: "work-order-v1",
					autonomous_approvals: false,
					status: "running",
					display_state: "running",
					plan_state: "approved",
					created_at: "2026-03-06T10:00:00.000Z",
					updated_at: "2026-03-06T10:00:00.000Z",
					last_activity_at: "2026-03-06T10:00:00.000Z",
					start_state: "not_started",
				}}
			/>,
		);

		expect(useRunGovernanceDecisionsMock).toHaveBeenCalledWith("run-1", { enabled: false });

		await user.click(screen.getByRole("button", { name: /Requested Plan Rework/i }));

		expect(
			useRunGovernanceDecisionsMock.mock.calls.some(
				([runId, options]) => runId === "run-1" && options?.enabled === true,
			),
		).toBe(true);
	});

	it("loads older governance decisions from the history modal", async () => {
		const user = userEvent.setup();
		render(
			<CurrentPlanPanel
				run={{
					run_id: "run-1",
					work_order_id: "work order-1",
					work_order_title: "Title",
					workflow_id: "work-order-v1",
					autonomous_approvals: false,
					status: "running",
					display_state: "running",
					plan_state: "approved",
					created_at: "2026-03-06T10:00:00.000Z",
					updated_at: "2026-03-06T10:00:00.000Z",
					last_activity_at: "2026-03-06T10:00:00.000Z",
					start_state: "not_started",
				}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Requested Plan Rework/i }));
		await user.click(screen.getByRole("button", { name: /Load Older Decisions/i }));

		expect(fetchNextGovernancePage).toHaveBeenCalledTimes(1);
	});

	it("does not show stale rework guidance after a newer blank confirmation", () => {
		latestGovernanceDecisionItems = [
			{
				decision_id: "decision-2",
				stage: "plan",
				decision: "approved",
				feedback_markdown: "",
				created_at: "2026-03-06T12:00:00.000Z",
			},
			{
				decision_id: "decision-1",
				stage: "plan",
				decision: "needs_rework",
				feedback_markdown: "Tighten the rollout scope before approval.",
				created_at: "2026-03-06T11:00:00.000Z",
			},
		];
		governanceDecisionHistoryPages = [
			{
				run_id: "run-1",
				items: latestGovernanceDecisionItems,
				next_cursor: "cursor-1",
			},
		];

		render(
			<CurrentPlanPanel
				run={{
					run_id: "run-1",
					work_order_id: "work order-1",
					work_order_title: "Title",
					workflow_id: "work-order-v1",
					autonomous_approvals: false,
					status: "running",
					display_state: "running",
					plan_state: "approved",
					created_at: "2026-03-06T10:00:00.000Z",
					updated_at: "2026-03-06T10:00:00.000Z",
					last_activity_at: "2026-03-06T10:00:00.000Z",
					start_state: "not_started",
				}}
			/>,
		);

		expect(screen.getByText("Operator Guidance")).toBeInTheDocument();
		expect(screen.queryByText("Tighten the rollout scope before approval.")).not.toBeInTheDocument();
		expect(screen.getByRole("button", { name: /View the complete Operator decision history for this run\./i })).toBeInTheDocument();
	});
});
