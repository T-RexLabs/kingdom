import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { RunExecution, RunExecutionDetail } from "@/api/hooks";
import { RunExecutionsPanel } from "./run-executions-panel";

function makeExecution(overrides: Partial<RunExecution> = {}): RunExecution {
	return {
		attempt_id: overrides.attempt_id ?? "attempt-1",
		node_run_id: overrides.node_run_id ?? "nr-1",
		node_id: overrides.node_id ?? "delegated_execution_berserker",
		attempt: overrides.attempt ?? 1,
		agent_id: overrides.agent_id ?? "agent-builder",
		required_role: overrides.required_role ?? "builder-berserker",
		execution_mode: overrides.execution_mode ?? "provider_backed",
		status: overrides.status ?? "succeeded",
		queued_at: overrides.queued_at ?? "2026-03-06T10:00:00.000Z",
		activity_at: overrides.activity_at ?? "2026-03-06T10:01:00.000Z",
		updated_at: overrides.updated_at ?? "2026-03-06T10:01:00.000Z",
		provider_session_id: overrides.provider_session_id,
		logical_session_id: overrides.logical_session_id,
		progress_event_type: overrides.progress_event_type,
		progress_reason_code: overrides.progress_reason_code,
		progress_at: overrides.progress_at,
		launch_started_at: overrides.launch_started_at,
		started_at: overrides.started_at,
		terminal_observed_at: overrides.terminal_observed_at,
		committed_at: overrides.committed_at,
		failure_code: overrides.failure_code,
		failure_text: overrides.failure_text,
	};
}

function makeExecutionDetail(overrides: Partial<RunExecutionDetail> = {}): RunExecutionDetail {
	const base = makeExecution(overrides as Partial<RunExecution>);
	return {
		...base,
		provider_session_id: overrides.provider_session_id ?? "ses-1",
		provider_invocation_id: overrides.provider_invocation_id ?? "inv-1",
		provider_invocation_at: overrides.provider_invocation_at,
		settlement_status: overrides.settlement_status ?? "succeeded",
		settlement_detail: overrides.settlement_detail,
		settled_at: overrides.settled_at,
		output_status: overrides.output_status ?? "settled",
		output_text: overrides.output_text,
		output_verdict: overrides.output_verdict,
		output_verdict_status: overrides.output_verdict_status,
	};
}

describe("run executions panel", () => {
	it("shows explicit provisional output and settlement verification state", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "verifying",
			settlement_status: "pending",
			output_status: "observed",
			output_text: "Partial output",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.queryByText("Kingdom is still confirming the final result for this step.")).not.toBeInTheDocument();
		expect(screen.getByText("The output below may still change.")).toBeInTheDocument();
		expect(screen.getByText("Partial output")).toBeInTheDocument();
	});

	it("uses live finishing copy when completion has been observed", async () => {
		const user = userEvent.setup();
		const detail = {
			...makeExecutionDetail({
				status: "verifying",
				settlement_status: "pending",
				output_status: "none",
				output_text: undefined,
			}),
			live_signal: {
				activity_at: "2026-03-06T10:01:05.000Z",
				live_label: "Completion observed",
			},
		};

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("This step is finishing up.")).toBeInTheDocument();
	});

	it("shows queued execution state without implying completion", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "queued",
			settlement_status: "not_started",
			output_status: "none",
			output_text: undefined,
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("This step is queued to start.")).toBeInTheDocument();
		expect(screen.queryByText("This step completed.")).not.toBeInTheDocument();
	});

	it("shows launching copy for steps that are still in starting", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "starting",
			launch_started_at: "2026-03-06T10:00:30.000Z",
			started_at: undefined,
			settlement_status: "not_started",
			output_status: "none",
			output_text: undefined,
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("This step is launching now.")).toBeInTheDocument();
	});

	it("shows blocked copy instead of completed copy", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "blocked",
			settlement_status: "failed",
			output_status: "none",
			output_text: undefined,
			failure_code: "dependency_blocked",
			failure_text: "validator blocked",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("This step is blocked while Kingdom decides or performs recovery.")).toBeInTheDocument();
		expect(screen.queryByText("This step completed.")).not.toBeInTheDocument();
	});

	it("shows provider incomplete turn as a receipt-backed failure", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "failed",
			settlement_status: "failed",
			failure_code: "provider_incomplete_turn",
			failure_text: "provider exited without an accepted kingdom_complete_attempt receipt",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("Kingdom could not confirm a clean finish for this step.")).toBeInTheDocument();
		expect(screen.queryByText(/kingdom_complete_attempt/i)).not.toBeInTheDocument();
	});

	it("shows canonical settled verdicts without recovery-era labels", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "failed",
			settlement_status: "failed",
			settled_at: "2026-03-06T10:05:00.000Z",
			output_status: "settled",
			output_text: "VERDICT: REWORK\n\nCanonical reviewer response.",
			output_verdict: "REWORK",
			output_verdict_status: "settled",
			failure_code: "execution_review_rework",
			failure_text: "review requested rework",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("Changes were requested for this step.")).toBeInTheDocument();
		expect(screen.getByText("The output below contains the explanation for what needs to change.")).toHaveClass("text-status-rework");
		expect(screen.getByText(/VERDICT: REWORK/)).toBeInTheDocument();
		expect(screen.queryByText("Observed Output")).not.toBeInTheDocument();
		expect(screen.queryByText(/Completion Verification In Progress/)).not.toBeInTheDocument();
	});

	it("hides role and agent details from the default result path", () => {
		const detail = makeExecutionDetail();

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		expect(screen.queryByText("Builder Berserker")).not.toBeInTheDocument();
		expect(screen.queryByText("agent-builder")).not.toBeInTheDocument();
		expect(screen.queryByText("Role")).not.toBeInTheDocument();
		expect(screen.queryByText("Agent")).not.toBeInTheDocument();
	});

	it("keeps diagnostics collapsed by default until requested", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			attempt_id: "attempt-diag",
			provider_session_id: "ses-diag",
			provider_invocation_id: "inv-diag",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		expect(screen.getByRole("button", { name: /Outcome/i })).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.queryByText("Attempt ID")).not.toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /Diagnostics/i }));
		expect(screen.queryByText("Attempt ID")).not.toBeInTheDocument();
		expect(screen.queryByText("Session ID")).not.toBeInTheDocument();
		expect(screen.queryByText("Invocation ID")).not.toBeInTheDocument();
	});

	it("shows both launch and start timestamps once execution becomes observable", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			queued_at: "2026-03-06T10:00:00.000Z",
			attempt_id: "attempt-launch-start",
			launch_started_at: "2026-03-06T10:00:30.000Z",
			started_at: "2026-03-06T10:01:00.000Z",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Diagnostics/i }));
		expect(screen.getByText("Launching")).toBeInTheDocument();
		expect(screen.getByText((_, element) => element?.tagName === "TIME" && element.getAttribute("datetime") === "2026-03-06T10:00:30.000Z")).toBeInTheDocument();
		expect(screen.getByText("Started")).toBeInTheDocument();
		expect(screen.getByText((_, element) => element?.tagName === "TIME" && element.getAttribute("datetime") === "2026-03-06T10:01:00.000Z")).toBeInTheDocument();
	});

	it("shows queue duration and explains diagnostics labels with tooltips", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "running",
			queued_at: "2026-03-06T10:00:00.000Z",
			launch_started_at: "2026-03-06T10:00:30.000Z",
			started_at: "2026-03-06T10:01:00.000Z",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Diagnostics/i }));

		expect(screen.getByText("Queued for")).toBeInTheDocument();
		expect(screen.getByText("30s")).toBeInTheDocument();

		await user.hover(screen.getByLabelText("Queued for info"));
		expect(screen.getByRole("tooltip")).toHaveTextContent("How long this step waited before it began running.");
		await user.unhover(screen.getByLabelText("Queued for info"));

		await user.hover(screen.getByLabelText("Launching info"));
		expect(screen.getByRole("tooltip")).toHaveTextContent("When Kingdom began handing this step off to the provider.");
		await user.unhover(screen.getByLabelText("Launching info"));

		await user.hover(screen.getByLabelText("Started info"));
		expect(screen.getByRole("tooltip")).toHaveTextContent("When Kingdom observed this step actually begin running.");
	});

	it("shows durable progress messaging after a refetch without live signal state", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			status: "blocked",
			output_status: "none",
			output_text: undefined,
			progress_event_type: "attempt.retry_exhausted",
			progress_reason_code: undefined,
			progress_at: "2026-03-06T10:02:00.000Z",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("Kingdom exhausted provider retries for this step.")).toBeInTheDocument();
	});

	it("exposes disclosure state to assistive tech", async () => {
		const user = userEvent.setup();
		render(
			<RunExecutionsPanel
				detail={makeExecutionDetail()}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		const outcomeButton = screen.getByRole("button", { name: /Outcome/i });
		expect(outcomeButton).toHaveAttribute("aria-expanded", "false");
		const controlsId = outcomeButton.getAttribute("aria-controls");
		expect(controlsId).toBeTruthy();

		await user.click(outcomeButton);

		expect(outcomeButton).toHaveAttribute("aria-expanded", "true");
		expect(document.getElementById(controlsId ?? "")).not.toBeNull();
	});

	it("omits provider-only diagnostics for runtime-owned attempts", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			attempt_id: "attempt-runtime",
			node_id: "king_plan_decision",
			required_role: "king",
			execution_mode: "runtime_owned",
			output_status: "none",
			provider_session_id: undefined,
			provider_invocation_id: undefined,
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));
		expect(screen.getByText("This was a runtime-owned step, so no user-facing output was expected.")).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /Diagnostics/i }));
		expect(screen.queryByText("Session ID")).not.toBeInTheDocument();
		expect(screen.queryByText("Invocation ID")).not.toBeInTheDocument();
	});

	it("shows only the start timestamp in diagnostics for a selected step", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			started_at: "2026-03-06T10:00:00.000Z",
			terminal_observed_at: "2026-03-06T10:01:00.000Z",
			settled_at: "2026-03-06T10:02:00.000Z",
			committed_at: "2026-03-06T10:03:00.000Z",
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Diagnostics/i }));

		expect(screen.getByText("Started")).toBeInTheDocument();
		expect(screen.queryByText("Verification observed")).not.toBeInTheDocument();
		expect(screen.queryByText("Settled")).not.toBeInTheDocument();
		expect(screen.queryByText("Committed")).not.toBeInTheDocument();
	});

	it("wraps long output safely without dropping the execution text", async () => {
		const user = userEvent.setup();
		const detail = makeExecutionDetail({
			output_text: "x".repeat(300),
		});

		render(
			<RunExecutionsPanel
				detail={detail}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Outcome/i }));

		expect(screen.getByText("x".repeat(300))).toHaveClass("text-wrap-anywhere");
	});

	it("shows a neutral placeholder when no step is selected", () => {
		render(
			<RunExecutionsPanel
				detail={null}
				isDetailLoading={false}
				detailErrorMessage={null}
				onRetryDetail={() => {}}
			/>,
		);

		expect(screen.getByText("Select a step to inspect the outcome.")).toBeInTheDocument();
	});
});
