import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { RunDetailSummary, RunExecution, RunNodeAttemptHistoryItem, RunNodeExecution } from "@/api/hooks";
import { RunExecutionSurface } from "./run-execution-surface";

vi.mock("@/components/ui/relative-time", () => ({ RelativeTime: ({ iso }: { iso: string }) => <span>{iso}</span> }));
vi.mock("./run-executions-panel", () => ({ RunExecutionsPanel: () => <div data-testid="run-executions-panel" /> }));
vi.mock("./timeline-panel", () => ({ TimelinePanel: () => <div data-testid="timeline-panel" /> }));
vi.mock("./run-execution-activity", () => ({ RunExecutionActivity: () => <div data-testid="run-execution-activity" /> }));

function execution(overrides: Partial<RunExecution> = {}): RunExecution {
	return { attempt_id: "attempt-current", node_run_id: "node-current", node_id: "delegated_execution_berserker", attempt: 2, agent_id: "agent-1", required_role: "builder-berserker", execution_mode: "provider_backed", status: "running", queued_at: "2026-03-01T10:00:00Z", activity_at: "2026-03-01T10:01:00Z", updated_at: "2026-03-01T10:01:00Z", stop_retry_available: false, ...overrides };
}

const run = { run_id: "run-1", decree_id: "decree-1", decree_title: "Phase 9", workflow_id: "royal-decree-v1", status: "running", display_state: "running", plan_state: "approved", created_at: "2026-03-01T10:00:00Z", updated_at: "2026-03-01T10:01:00Z", last_activity_at: "2026-03-01T10:01:00Z", autonomous_approvals: true, run_until_all_approve: false, publish_mode: "direct", model_selection: { global: { model: "model" }, role_overrides: {} }, tab_counts: { dag_nodes: 1, agents: 1, activity_events: 1, executions: 1, messages: 1 } } as RunDetailSummary;

function node(overrides: Partial<RunNodeExecution> = {}): RunNodeExecution {
	return { node_id: "delegated_execution_berserker", required_role: "builder-berserker", execution_ownership: "provider_backed", status: "running", attempt_count: 2, current_execution: execution(), ...overrides };
}

function historyAttempt(overrides: Partial<RunNodeAttemptHistoryItem> = {}): RunNodeAttemptHistoryItem {
	return { ...execution({ attempt_id: "attempt-old", node_run_id: "node-old", attempt: 1, status: "failed", failure_text: "provider error" }), superseded: true, failure: { code: "llm_rate_limited", classification: "retryable", category: "provider", retryable: true, operator_text: "The provider rate limit was reached." }, retry_relationships: [{ direction: "source", phase: "scheduled", trigger: "automatic_retry", reason_code: "llm_rate_limited", source_attempt_id: "attempt-old", source_node_run_id: "node-old", target_attempt_id: "attempt-current", target_node_run_id: "node-current", retry_index: 1, retry_max: 3, scheduled_at: "2026-03-01T10:02:00Z", last_updated_at: "2026-03-01T10:02:00Z" }], token_usage: { input_tokens: 10, output_tokens: 20, total_tokens: 30, reasoning_tokens: 4, cache_read_tokens: 2, cache_write_tokens: 1 }, provider_invocation_id: "inv-old", ...overrides };
}

function renderSurface(overrides: Partial<Parameters<typeof RunExecutionSurface>[0]> = {}) {
	const props: Parameters<typeof RunExecutionSurface>[0] = {
		run, nodes: [node()], attempts: [historyAttempt()], executionsErrorMessage: null, showPrimaryExecutionError: false, isSummaryLoading: false, onRetryExecutions: vi.fn(), selectedNodeId: "delegated_execution_berserker", selectedAttemptId: null, onSelectStep: vi.fn(), onSelectAttempt: vi.fn(), onSelectCurrentAttempt: vi.fn(), historyOpen: false, onToggleHistory: vi.fn(), hasMoreAttempts: false, onLoadMoreAttempts: vi.fn(), isLoadingMoreAttempts: false, isHistoryLoading: false, detail: null, isDetailLoading: false, detailErrorMessage: null, onRetryDetail: vi.fn(), onRetryStop: vi.fn(), isStopRetryPending: false, stopRetryErrorMessage: null, events: [], hasMoreEvents: false, onLoadMoreEvents: vi.fn(), isLoadingMoreEvents: false,
		...overrides,
	};
	render(<RunExecutionSurface {...props} />);
	return props;
}

describe("run execution surface", () => {
	it("renders one logical node card while showing current failure and automatic recovery before history", () => {
		renderSurface({ nodes: [node({ status: "blocked", recovery: { phase: "scheduled", trigger: "automatic_retry", reason_code: "llm_rate_limited", source_attempt_id: "attempt-current", source_node_run_id: "node-current", target_attempt_id: "attempt-next", target_node_run_id: "node-next", retry_index: 1, retry_max: 3, scheduled_at: "2026-03-01T10:02:00Z", last_updated_at: "2026-03-01T10:02:00Z" } })] });
		const workflowNodes = screen.getByRole("region", { name: "Workflow nodes" });
		expect(within(workflowNodes).getAllByRole("button")).toHaveLength(1);
		expect(within(workflowNodes).getByRole("button")).toHaveClass("min-h-11");
		expect(screen.getByText("Retry Scheduled")).toBeInTheDocument();
		expect(screen.getByText(/The provider rate limit was reached/)).toBeInTheDocument();
		expect(screen.getByText("Automatic Recovery · Automatic retry 1 of 3")).toBeInTheDocument();
		expect(screen.getByRole("status")).toHaveAttribute("aria-live", "polite");
	});

	it("announces provider settlement verification to assistive technology", () => {
		renderSurface({ nodes: [node({ current_execution: execution({ status: "verifying" }) })] });

		const announcement = screen.getByRole("status");
		expect(announcement).toHaveAttribute("aria-live", "polite");
		expect(announcement).toHaveTextContent("Kingdom is still confirming the final result for this step.");
	});

	it("keeps attempt history collapsed until the page-owned disclosure is opened", () => {
		renderSurface();
		const history = screen.getByRole("button", { name: "Attempt History (2)" });
		expect(history).toHaveAttribute("aria-expanded", "false");
		expect(screen.queryByText("Provider Invocation ID:")).not.toBeInTheDocument();
	});

	it("shows historical evidence, technical provider data, and preserves the return action", async () => {
		const user = userEvent.setup();
		renderSurface({ historyOpen: true, selectedAttemptId: "attempt-old" });
		expect(screen.getAllByText("Historical Attempt 1")).toHaveLength(2);
		expect(screen.getByText("Superseded")).toBeInTheDocument();
		expect(screen.getByText(/Token Usage: 30 total/)).toBeInTheDocument();
		await user.click(screen.getByText("Technical Details"));
		expect(screen.getByText(/Provider Invocation ID:/)).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: "Return to Current Attempt" }));
		expect(document.activeElement).toHaveTextContent("Current Attempt 2");
	});

	it("keeps the historical attempt identified after its list evidence refreshes away", () => {
		renderSurface({ attempts: [], selectedAttemptId: "attempt-old", detail: { ...historyAttempt(), provider_session_id: "session-old", settlement_status: "failed", output_status: "unavailable" } });
		expect(screen.getByText("Historical Attempt 1")).toBeInTheDocument();
		expect(screen.getByRole("button", { name: "Return to Current Attempt" })).toBeInTheDocument();
	});

	it("offers Retry Stop only for the current paused attempt", async () => {
		const user = userEvent.setup();
		const onRetryStop = vi.fn();
		renderSurface({ nodes: [node({ status: "blocked", current_execution: execution({ status: "paused", stop_retry_available: true }) })], onRetryStop });
		await user.click(screen.getByRole("button", { name: "Retry Stop" }));
		expect(onRetryStop).toHaveBeenCalledWith("attempt-current");
	});

	it("keeps the retryable execution load error without an empty-state claim", () => {
		renderSurface({
			nodes: [],
			executionsErrorMessage: "Execution inspector unavailable",
			showPrimaryExecutionError: true,
		});

		expect(screen.getByText("The execution step list could not be loaded.")).toBeInTheDocument();
		expect(screen.getByText("Execution inspector unavailable")).toBeInTheDocument();
		expect(screen.getByRole("button", { name: "Retry Step List" })).toBeInTheDocument();
		expect(screen.queryByText("No execution steps have been recorded yet.")).not.toBeInTheDocument();
	});
});
