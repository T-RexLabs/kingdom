import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { RunDetailSummary, RunExecution, RunExecutionDetail } from "@/api/hooks";
import type { ReactNode } from "react";
import { RunExecutionSurface } from "./run-execution-surface";
import { buildActivityStepGroups } from "./run-activity-selection";

vi.mock("@/components/ui/popover-listbox", () => ({
	PopoverListbox: ({
		triggerId,
		triggerAriaLabel,
		triggerClassName,
		items,
		triggerContent,
	}: {
		triggerId: string;
		triggerAriaLabel?: string;
		triggerClassName?: string;
		items?: Array<{ id: string; label: string }>;
		triggerContent: ReactNode;
	}) => (
		<div>
			<button type="button" aria-label={triggerAriaLabel ?? triggerId} className={triggerClassName}>{triggerContent}</button>
			<ol data-testid={`${(triggerAriaLabel ?? triggerId).toLowerCase().replace(/\s+/g, "-")}-options`}>
				{items?.map((item) => <li key={item.id}>{item.label}</li>)}
			</ol>
		</div>
	),
}));

vi.mock("@/components/ui/relative-time", () => ({
	RelativeTime: ({ iso }: { iso: string }) => <span>{iso}</span>,
}));

vi.mock("./run-executions-panel", () => ({
	RunExecutionsPanel: () => <div data-testid="run-executions-panel" />,
}));

vi.mock("./timeline-panel", () => ({
	TimelinePanel: () => <div data-testid="timeline-panel" />,
}));

vi.mock("./run-execution-activity", () => ({
	RunExecutionActivity: () => <div data-testid="run-execution-activity" />,
}));

vi.mock("./run-activity-lanes", () => ({
	RunActivityLanes: () => <div data-testid="run-activity-lanes" />,
}));

vi.mock("./use-node-activity-recency", () => ({
	useNodeActivityRecency: () => undefined,
}));

function makeRun(overrides: Partial<RunDetailSummary> = {}): RunDetailSummary {
	return {
		run_id: "run-1",
		decree_id: "decree-1",
		decree_title: "Fix Activity Hero",
		workflow_id: "royal-decree-v1",
		status: "running",
		display_state: "running",
		plan_state: "approved",
		created_at: "2026-03-01T10:00:00Z",
		updated_at: "2026-03-01T10:10:00Z",
		last_activity_at: "2026-03-01T10:10:00Z",
		autonomous_approvals: true,
		run_until_all_approve: false,
		publish_mode: "direct",
		model_selection: {
			global_model: "gpt-5.4-mini",
			role_overrides: [],
		},
		tab_counts: { dag_nodes: 14, agents: 11, activity_events: 15, executions: 4, messages: 15 },
		...overrides,
	} as RunDetailSummary;
}

function makeExecution(overrides: Partial<RunExecution> = {}): RunExecution {
	return {
		attempt_id: "attempt-1",
		node_run_id: "nr-1",
		node_id: "advisor_consultation_peasant",
		attempt: 1,
		agent_id: "agent-1",
		required_role: "peasant-delegate",
		execution_mode: "provider_backed",
		status: "succeeded",
		queued_at: "2026-03-01T10:08:00Z",
		activity_at: "2026-03-01T10:09:00Z",
		updated_at: "2026-03-01T10:09:00Z",
		committed_at: "2026-03-01T10:09:00Z",
		...overrides,
	};
}

type PresentedExecutionDetail = RunExecutionDetail & { live_signal?: { live_label?: string; activity_at: string } };

function renderSurface({
	run = makeRun(),
	executions = [makeExecution()],
	executionsErrorMessage = null,
	showPrimaryExecutionError = false,
	selectedAttemptId = executions[0]?.attempt_id ?? null,
	selectedAttemptSource = "auto" as const,
	selectedNodeId = executions[0]?.node_id ?? null,
	detail = null,
}: {
	run?: RunDetailSummary;
	executions?: RunExecution[];
	executionsErrorMessage?: string | null;
	showPrimaryExecutionError?: boolean;
	selectedAttemptId?: string | null;
	selectedAttemptSource?: "auto" | "manual" | null;
	selectedNodeId?: string | null;
	detail?: PresentedExecutionDetail | null;
} = {}) {
	const stepGroups = buildActivityStepGroups(executions);
	render(
		<RunExecutionSurface
			run={run}
			executions={executions}
			stepGroups={stepGroups}
			executionsErrorMessage={executionsErrorMessage}
			showPrimaryExecutionError={showPrimaryExecutionError}
			isSummaryLoading={false}
			onRetryExecutions={() => {}}
			selectedNodeId={selectedNodeId}
			selectedAttemptId={selectedAttemptId}
			selectedAttemptSource={selectedAttemptSource}
			onSelectStep={() => {}}
			onSelectAttempt={() => {}}
			detail={detail}
			isDetailLoading={false}
			detailErrorMessage={null}
			onRetryDetail={() => {}}
			events={[]}
			hasMoreEvents={false}
			onLoadMoreEvents={() => {}}
			isLoadingMoreEvents={false}
		/>,
	);
}

describe("run execution surface", () => {
	it("shows Latest Completed Step with a single step-status pill for auto-selected terminal work", () => {
		renderSurface();

		expect(screen.getByText("Latest Completed Step")).toBeInTheDocument();
		expect(screen.getByText("Succeeded")).toBeInTheDocument();
		expect(screen.queryByText("Current Activity")).not.toBeInTheDocument();
		expect(screen.queryByText("Active")).not.toBeInTheDocument();
	});

	it("shows Current Activity for in-flight selected work", () => {
		renderSurface({
			executions: [makeExecution({ status: "running", started_at: "2026-03-01T10:08:00Z", committed_at: undefined })],
		});

		expect(screen.getByText("Current Activity")).toBeInTheDocument();
		expect(screen.getByText("Active")).toBeInTheDocument();
		expect(screen.queryByText("Latest Completed Step")).not.toBeInTheDocument();
	});

	it("uses launching copy while the selected step is still starting", () => {
		renderSurface({
			executions: [makeExecution({ status: "starting", launch_started_at: "2026-03-01T10:07:30Z", started_at: undefined, committed_at: undefined })],
		});

		expect(screen.getByText("Current Activity")).toBeInTheDocument();
		expect(screen.getByText("Launching")).toBeInTheDocument();
		expect(screen.getByText("This step is launching now.")).toBeInTheDocument();
	});

	it("keeps the hero in verifying while settlement is still pending", () => {
		renderSurface({
			detail: {
				...makeExecution(),
				provider_session_id: "ses-1",
				provider_invocation_id: "inv-1",
				settlement_status: "pending",
				output_status: "observed",
				output_text: "Observed output",
			},
		});

		expect(screen.getByText("Current Activity")).toBeInTheDocument();
		expect(screen.getByText("Verifying")).toBeInTheDocument();
		expect(screen.getByText("Kingdom is still confirming the final result for this step.")).toBeInTheDocument();
		expect(screen.queryByText("Latest Completed Step")).not.toBeInTheDocument();
	});

	it("shows finishing copy when runtime completion has been observed live", () => {
		renderSurface({
			executions: [makeExecution({ status: "running", committed_at: undefined, started_at: "2026-03-01T10:08:00Z" })],
			detail: {
				...makeExecution({ status: "running", committed_at: undefined, started_at: "2026-03-01T10:08:00Z" }),
				provider_session_id: "ses-1",
				provider_invocation_id: "inv-1",
				settlement_status: "pending",
				output_status: "none",
				live_signal: {
					activity_at: "2026-03-01T10:08:05Z",
					live_label: "Completion observed",
				},
			} as PresentedExecutionDetail,
		});

		expect(screen.getByText("This step is finishing up.")).toBeInTheDocument();
	});

	it("uses durable progress metadata when no live signal is present", () => {
		renderSurface({
			executions: [makeExecution({ status: "verifying", committed_at: undefined, terminal_observed_at: "2026-03-01T10:08:05Z" })],
			detail: {
				...makeExecution({ status: "verifying", committed_at: undefined, terminal_observed_at: "2026-03-01T10:08:05Z" }),
				provider_session_id: "ses-1",
				provider_invocation_id: "inv-1",
				settlement_status: "pending",
				output_status: "none",
				progress_event_type: "attempt.progress",
				progress_reason_code: "runtime_completion_observation",
				progress_at: "2026-03-01T10:08:05Z",
			} as PresentedExecutionDetail,
		});

		expect(screen.getByText("This step is finishing up.")).toBeInTheDocument();
	});

	it("keeps queued work in the current activity lane without claiming it completed", () => {
		renderSurface({
			executions: [makeExecution({ status: "queued", committed_at: undefined, started_at: undefined, terminal_observed_at: undefined })],
		});

		expect(screen.getByText("Current Activity")).toBeInTheDocument();
		expect(screen.getByText("Queued")).toBeInTheDocument();
		expect(screen.getByText("This step is queued to start.")).toBeInTheDocument();
		expect(screen.queryByText("This step completed.")).not.toBeInTheDocument();
	});

	it("keeps blocked work in the current activity lane without claiming it completed", () => {
		renderSurface({
			executions: [makeExecution({ status: "blocked", committed_at: undefined, started_at: "2026-03-01T10:08:00Z", terminal_observed_at: "2026-03-01T10:08:30Z" })],
		});

		expect(screen.getByText("Current Activity")).toBeInTheDocument();
		expect(screen.getByText("Blocked")).toBeInTheDocument();
		expect(screen.getByText("This step is blocked while Kingdom decides or performs recovery.")).toBeInTheDocument();
		expect(screen.queryByText("This step completed.")).not.toBeInTheDocument();
	});

	it("keeps a blocked lineage selected over a newer queued retry for the same step", () => {
		renderSurface({
			executions: [
				makeExecution({
					attempt_id: "attempt-blocked",
					node_run_id: "nr-blocked",
					node_id: "delegated_execution_berserker",
					attempt: 1,
					required_role: "builder-berserker",
					status: "blocked",
					activity_at: "2026-03-01T10:08:00Z",
					updated_at: "2026-03-01T10:08:00Z",
					started_at: "2026-03-01T10:07:00Z",
					committed_at: undefined,
				}),
				makeExecution({
					attempt_id: "attempt-queued",
					node_run_id: "nr-queued",
					node_id: "delegated_execution_berserker",
					attempt: 2,
					required_role: "builder-berserker",
					status: "queued",
					activity_at: "2026-03-01T10:09:00Z",
					updated_at: "2026-03-01T10:09:00Z",
					started_at: undefined,
					committed_at: undefined,
				}),
			],
			selectedAttemptId: null,
			selectedNodeId: null,
		});

		expect(screen.getByText("Current Activity")).toBeInTheDocument();
		expect(screen.getByText("Blocked")).toBeInTheDocument();
		expect(screen.getByText("This step is blocked while Kingdom decides or performs recovery.")).toBeInTheDocument();
		expect(screen.queryByText("Queued")).not.toBeInTheDocument();
	});

	it("keeps rework summary copy aligned with the rework pill when detail is unavailable", () => {
		renderSurface({
			executions: [makeExecution({ status: "failed", failure_code: "execution_review_rework" })],
			detail: null,
		});

		expect(screen.getByText("Rework")).toBeInTheDocument();
		expect(screen.getByText("Changes were requested for this step.")).toBeInTheDocument();
		expect(screen.queryByText("This step failed.")).not.toBeInTheDocument();
	});

	it("shows receipt conflict copy for failed provider incomplete turn details", () => {
		renderSurface({
			executions: [makeExecution({ status: "failed", failure_code: "provider_incomplete_turn" })],
			detail: {
				...makeExecution({ status: "failed", failure_code: "provider_incomplete_turn" }),
				provider_session_id: "ses-1",
				provider_invocation_id: "inv-1",
				settlement_status: "failed",
				output_status: "none",
				failure_text: "completion_receipt_conflict: conflicting receipt invalidated the invocation",
			} as PresentedExecutionDetail,
		});

		expect(screen.getByText("Kingdom detected conflicting completion receipts for this step and could not safely accept the result.")).toBeInTheDocument();
		expect(screen.queryByText("Kingdom could not confirm a clean finish for this step.")).not.toBeInTheDocument();
	});

	it("shows Selected Step when the operator manually picks a terminal step", () => {
		renderSurface({ selectedAttemptSource: "manual" });

		expect(screen.getByText("Selected Step")).toBeInTheDocument();
		expect(screen.getByText("Succeeded")).toBeInTheDocument();
		expect(screen.queryByText("Current Activity")).not.toBeInTheDocument();
	});

	it("orders selector options by most recent activity time", () => {
		renderSurface({
			executions: [
				makeExecution({ attempt_id: "attempt-1", node_id: "plan_drafting", activity_at: "2026-03-01T10:08:00Z" }),
				makeExecution({ attempt_id: "attempt-2", node_id: "advisor_consultation_noble", activity_at: "2026-03-01T10:10:00Z" }),
				makeExecution({ attempt_id: "attempt-3", node_id: "execution_kickoff", activity_at: "2026-03-01T10:09:00Z" }),
			],
			selectedAttemptId: "attempt-2",
		});

		expect(screen.getByTestId("current-step-options")).toHaveTextContent("Advisor Consultation NobleExecution KickoffPlan Drafting");
	});

	it("shows a dedicated attempt selector only when the selected step has multiple attempts", () => {
		renderSurface({
			executions: [
				makeExecution({ attempt_id: "attempt-1", node_id: "execution_review_shieldmaiden", attempt: 1, activity_at: "2026-03-01T10:08:00Z" }),
				makeExecution({ attempt_id: "attempt-2", node_id: "execution_review_shieldmaiden", attempt: 2, activity_at: "2026-03-01T10:10:00Z" }),
			],
			selectedNodeId: "execution_review_shieldmaiden",
			selectedAttemptId: "attempt-2",
		});

		expect(screen.queryByText(/^Attempt$/)).not.toBeInTheDocument();
		expect(screen.getByRole("button", { name: /selected attempt/i })).toHaveClass("px-2.5", "py-1.5", "text-xs");
		expect(screen.getByTestId("current-step-options")).toHaveTextContent("Execution Review Shieldmaiden");
		expect(screen.getByTestId("selected-attempt-options")).toHaveTextContent("Attempt 2Attempt 1");
	});

	it("keeps the timeline collapsed by default until the operator opens it", async () => {
		const user = userEvent.setup();
		renderSurface();

		const timelineToggle = screen.getByRole("button", { name: /^timeline/i });
		expect(timelineToggle).toHaveAttribute("aria-expanded", "false");
		expect(screen.queryByTestId("timeline-panel")).not.toBeInTheDocument();

		await user.click(timelineToggle);

		expect(timelineToggle).toHaveAttribute("aria-expanded", "true");
		expect(screen.getByTestId("timeline-panel")).toBeInTheDocument();
	});

	it("shows a primary execution error state instead of waiting copy when the execution list fails", () => {
		renderSurface({
			executions: [],
			executionsErrorMessage: "Execution inspector unavailable",
			showPrimaryExecutionError: true,
		});

		expect(screen.getByText("The execution step list could not be loaded.")).toBeInTheDocument();
		expect(screen.getByText("Execution steps unavailable")).toBeInTheDocument();
		expect(screen.getByText("Execution inspector unavailable")).toBeInTheDocument();
		expect(screen.queryByText("Waiting to start")).not.toBeInTheDocument();
	});

	it("shows loading copy when activity exists but the execution summary is still loading", () => {
		render(
			<RunExecutionSurface
				run={makeRun()}
				executions={[]}
				stepGroups={[]}
				executionsErrorMessage={null}
				showPrimaryExecutionError={false}
				isSummaryLoading
				onRetryExecutions={() => {}}
				selectedNodeId={null}
				selectedAttemptId={null}
				selectedAttemptSource={null}
				onSelectStep={() => {}}
				onSelectAttempt={() => {}}
				detail={null}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
				events={[]}
				hasMoreEvents={false}
				onLoadMoreEvents={() => {}}
				isLoadingMoreEvents={false}
			/>,
		);

		expect(screen.getByText("Loading current step")).toBeInTheDocument();
		expect(screen.queryByText("Waiting to start")).not.toBeInTheDocument();
	});
});
