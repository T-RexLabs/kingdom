import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { TimelinePanel } from "./timeline-panel";
import { buildTimelineLookups, synthesizeMilestones } from "./timeline-milestones";
import { orderMilestonesForDisplay } from "./timeline-ordering";

describe("timeline panel", () => {
  it("shows a cancellation reason once while retaining its canonical code in the record", async () => {
    const user = userEvent.setup();
    render(<TimelinePanel events={[{
      event_id: "evt-cancel",
      run_event_seq: 1,
      attempt_seq: 1,
      event_type: "run.state_changed",
      reason_code: "run_cancelled_by_user",
      emitted_at: "2026-03-18T11:40:00Z",
      payload_json: { status: "cancelled" },
    }]} hasMore={false} />);
    await user.click(screen.getByRole("button", { name: "Raw Events" }));
    expect(screen.getAllByText("Run Cancelled by User", { exact: false })).toHaveLength(1);
    expect(screen.queryByText("run cancelled by user", { exact: true })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Run Cancelled.*Run Cancelled by User/i }));
    expect(screen.getByText("run_cancelled_by_user")).toBeInTheDocument();
  });

	beforeEach(() => {
		vi.restoreAllMocks();
	});

  it("shows the active audit view and keeps complete records available even without a payload", async () => {
    const user = userEvent.setup();
    render(<TimelinePanel events={[{
      event_id: "evt-empty-record",
      run_event_seq: 42,
      attempt_seq: 7,
      event_type: "attempt.started",
      reason_code: "provider_dispatch",
      emitted_at: "2026-03-18T11:40:00Z",
      payload_json: {},
    }]} hasMore />);

    expect(screen.getByRole("button", { name: "Activity", pressed: true })).toBeInTheDocument();
    expect(screen.getByText(/1 milestone.*1 loaded event/i)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Raw Events" }));
    expect(screen.getByRole("button", { name: "Raw Events", pressed: true })).toBeInTheDocument();
    const record = screen.getByRole("button", { name: /Node started/i });
    record.focus();
    await user.keyboard("{Enter}");
    expect(record).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("evt-empty-record")).toBeInTheDocument();
    expect(screen.getByText("attempt.started")).toBeInTheDocument();
    expect(screen.getByText("provider_dispatch")).toBeInTheDocument();
    expect(screen.getByText("2026-03-18T11:40:00Z")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();
    expect(screen.getByTestId("timeline-json-payload")).toHaveTextContent("{}");
  });

	it("preserves canonical milestone sequence even when timestamps move backward", () => {
		expect(orderMilestonesForDisplay([
			{ id: "milestone-1", timestamp: "2026-03-18T11:41:00Z" },
			{ id: "milestone-2", timestamp: "2026-03-18T11:40:00Z" },
		]).map((milestone) => milestone.id)).toEqual(["milestone-2", "milestone-1"]);
	});

  it("shows explicit labels for loop cap auto-succeed and cancel events", () => {
    render(
      <TimelinePanel
        events={[
          {
            event_id: "evt-1",
	            run_event_seq: 1,
	            attempt_seq: 1,
            event_type: "attempt.succeeded",
            reason_code: "execution_review_cap_auto_succeeded",
            emitted_at: "2026-03-18T11:40:00Z",
            payload_json: { status: "ok" },
          },
          {
            event_id: "evt-2",
	            run_event_seq: 2,
	            attempt_seq: 2,
            event_type: "run.state_changed",
            emitted_at: "2026-03-18T11:41:00Z",
            payload_json: { action: "cancel" },
          },
        ]}
        hasMore={false}
      />,
    );

    expect(screen.getByText("Review Cap Auto-Succeeded")).toBeInTheDocument();
    expect(screen.getByText("Run Cancelled by User")).toBeInTheDocument();
  });

  it("treats mixed validator waves with later cap auto-success as succeeded milestones", () => {
    const events = [
      {
        event_id: "evt-qa-failed",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.failed",
        reason_code: "validation_gate_rework",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "quality_validation", node_run_id: "nr-qa-1", error: "validator requested rework" },
      },
      {
        event_id: "evt-qa-cap-success",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.succeeded",
        reason_code: "validation_gate_cap_auto_succeeded",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "quality_validation", node_run_id: "nr-qa-1", status: "ok" },
      },
      {
        event_id: "evt-night-success",
        run_event_seq: 3,
        attempt_seq: 3,
        event_type: "attempt.succeeded",
        reason_code: "execution_completed",
        emitted_at: "2026-03-18T11:41:30Z",
        payload_json: { node_id: "security_validation", node_run_id: "nr-night-1", status: "ok" },
      },
    ];
    const graph = {
      nodes: [
        { node_id: "execution_review", required_role: "reviewer", execution_ownership: "provider_backed" as const, state: "succeeded" as const },
        { node_id: "quality_validation", required_role: "qa-validator", execution_ownership: "provider_backed" as const, state: "succeeded" as const },
        { node_id: "security_validation", required_role: "security-validator", execution_ownership: "provider_backed" as const, state: "succeeded" as const },
      ],
      edges: [
        { from: "execution_review", to: "quality_validation", on: "success" },
        { from: "execution_review", to: "security_validation", on: "success" },
      ],
    };

    const lookups = buildTimelineLookups(events, graph.nodes);
    const milestones = synthesizeMilestones(events, graph.nodes, graph.edges, lookups);
    const validationMilestone = milestones.find((milestone) => milestone.kind === "fan_out");

    expect(validationMilestone?.status).toBe("succeeded");
    expect(validationMilestone?.badge).toBe("Validation Cap Auto-Succeeded");
    expect(validationMilestone?.children?.map((child) => child.status)).toEqual(["succeeded", "succeeded"]);
  });

	it("classifies review and validation rework outcomes separately from failures", () => {
		const events = [
			{
				event_id: "evt-review-rework",
				run_event_seq: 1,
				attempt_seq: 1,
				event_type: "attempt.failed",
				reason_code: "execution_review_rework",
				emitted_at: "2026-03-18T11:40:00Z",
				payload_json: { node_id: "execution_review", node_run_id: "nr-review-1", error: "review requested rework" },
			},
		];
		const graph = {
			nodes: [
				{ node_id: "execution_review", required_role: "reviewer", execution_ownership: "provider_backed" as const, state: "failed" as const },
			],
			edges: [],
		};

		const lookups = buildTimelineLookups(events, graph.nodes);
		const milestones = synthesizeMilestones(events, graph.nodes, graph.edges, lookups);
		const reviewMilestone = milestones.find((milestone) => milestone.id === "node-execution_review");

		expect(reviewMilestone?.status).toBe("rework");
	});

  it("renders approval-request mailbox activity without completing runtime-owned nodes", () => {
		render(
			<TimelinePanel
				events={[
					{
						event_id: "evt-approval-request",
						run_event_seq: 1,
						attempt_seq: 1,
						event_type: "mailbox.message_recorded",
						emitted_at: "2026-03-18T11:40:00Z",
						payload_json: {
							node_id: "plan_decision",
							communication_type: "approval_request",
							visible_to_operator: true,
						},
					},
				]}
				graph={{
					nodes: [{
						node_id: "plan_decision",
						required_role: "governance",
						execution_ownership: "runtime_owned",
						state: "pending",
					}],
					edges: [],
				}}
				hasMore={false}
			/>,
		);

		expect(screen.getByText("Plan Decision Awaiting Approval")).toBeInTheDocument();
		expect(screen.queryByText(/Crown-handled/i)).not.toBeInTheDocument();
		expect(screen.queryByText(/authority/i)).not.toBeInTheDocument();
	});

	it("suppresses duplicate response milestones when a terminal attempt event already represents the same outcome", async () => {
		const user = userEvent.setup();
		render(
			<TimelinePanel
				events={[
					{
						event_id: "evt-attempt-failed",
						run_event_seq: 1,
						attempt_seq: 1,
						event_type: "attempt.failed",
						reason_code: "provider_incomplete_turn",
						emitted_at: "2026-03-18T11:40:00Z",
						payload_json: { node_id: "implementation", node_run_id: "nr-builder-1", error: "incomplete terminal state" },
					},
					{
						event_id: "evt-response",
						run_event_seq: 2,
						attempt_seq: 2,
						event_type: "mailbox.message_recorded",
						reason_code: "provider_incomplete_turn",
						emitted_at: "2026-03-18T11:40:01Z",
						payload_json: {
							node_id: "implementation",
							node_run_id: "nr-builder-1",
							communication_type: "response_received",
							response_full: "Builder response",
						},
					},
				]}
				graph={{
					nodes: [{
						node_id: "implementation",
						required_role: "builder",
						execution_ownership: "provider_backed",
						state: "failed",
					}],
					edges: [],
				}}
				hasMore={false}
			/>,
		);

		expect(screen.queryByText("Implementation Response Received")).not.toBeInTheDocument();
		expect(screen.getByText("Implementation")).toBeInTheDocument();

		await user.click(screen.getByRole("button", { name: /Raw Events/i }));

		expect(screen.getByText("Implementation Response Received")).toBeInTheDocument();
	});

  it("keeps the default timeline focused on the step outcome instead of response mailbox noise", () => {
		render(
			<TimelinePanel
				events={[
					{
						event_id: "evt-review-failed-1",
						run_event_seq: 1,
						attempt_seq: 1,
						event_type: "attempt.failed",
						reason_code: "execution_review_rework",
						emitted_at: "2026-03-18T11:40:00Z",
						payload_json: { node_id: "execution_review", node_run_id: "nr-review-1", error: "rework requested" },
					},
					{
						event_id: "evt-review-response-2",
						run_event_seq: 2,
						attempt_seq: 2,
						event_type: "mailbox.message_recorded",
						reason_code: "execution_review_rework",
						emitted_at: "2026-03-18T11:41:00Z",
						payload_json: {
							node_id: "execution_review",
							node_run_id: "nr-review-2",
							communication_type: "response_received",
							response_full: "second attempt review response",
						},
					},
				]}
				graph={{
					nodes: [{
						node_id: "execution_review",
						required_role: "reviewer",
						execution_ownership: "provider_backed",
						state: "running",
					}],
					edges: [],
				}}
				hasMore={false}
			/>,
		);

		expect(screen.getByText("Execution Review")).toBeInTheDocument();
		expect(screen.queryByText("Execution Review Response Received")).not.toBeInTheDocument();
	});

	it("shows the newest raw events first inside an expanded milestone", async () => {
		const user = userEvent.setup();
		render(
			<TimelinePanel
				events={[
					{
						event_id: "evt-started",
						run_event_seq: 1,
						attempt_seq: 1,
						event_type: "attempt.started",
						emitted_at: "2026-03-18T11:40:00Z",
						payload_json: { node_id: "plan_drafting", node_run_id: "nr-plan-1" },
					},
					{
						event_id: "evt-failed",
						run_event_seq: 2,
						attempt_seq: 2,
						event_type: "attempt.failed",
						emitted_at: "2026-03-18T11:41:00Z",
						payload_json: { node_id: "plan_drafting", node_run_id: "nr-plan-1" },
					},
				]}
				graph={{
					nodes: [{
						node_id: "plan_drafting",
						required_role: "plan-author",
						execution_ownership: "provider_backed",
						state: "failed",
					}],
					edges: [],
				}}
				hasMore={false}
			/>,
		);

		await user.click(screen.getByRole("button", { name: /Plan Drafting/i }));

		expect(screen.getAllByText(/Plan Drafting (Failed|Started)/).map((node) => node.textContent)).toEqual([
			"Plan Drafting Failed",
			"Plan Drafting Started",
		]);
	});

  it("renders payload strings with escaped quotes as plain text in raw events", async () => {
    const user = userEvent.setup();
    render(
      <TimelinePanel
        events={[
          {
            event_id: "evt-1",
	            run_event_seq: 1,
	            attempt_seq: 1,
            event_type: "attempt.failed",
            emitted_at: "2026-03-18T11:40:00Z",
            payload_json: { message: 'He said "go" <tag>' },
          },
        ]}
        hasMore={false}
      />,
    );

    await user.click(screen.getByRole("button", { name: /Raw Events/i }));
    await user.click(screen.getByRole("button", { name: /Node failed/i }));

    expect(screen.getByText((content) => content.includes('"He said \\"go\\" <tag>"'))).toBeInTheDocument();

    const payload = screen.getByTestId("timeline-json-payload");
    expect(payload).toHaveClass("max-w-full", "border-border");
    expect(payload).not.toHaveClass("border-border-strong");
    expect(payload.closest(".min-w-0")).not.toBeNull();
  });

  it("keeps expanded milestone payloads aligned without an extra left indent", async () => {
    const user = userEvent.setup();
    render(
      <TimelinePanel
        events={[
          {
            event_id: "evt-1",
            run_event_seq: 1,
            attempt_seq: 1,
            event_type: "attempt.failed",
            emitted_at: "2026-03-18T11:40:00Z",
            payload_json: { message: "layout overflow" },
          },
        ]}
        hasMore={false}
      />,
    );

    await user.click(screen.getByRole("button", { name: /background attempt failed/i }));
    await user.click(screen.getByRole("button", { name: /Node failed/i }));

    const payload = screen.getByTestId("timeline-json-payload");
    expect(payload).not.toHaveClass("ml-2");
  });

  it("keeps the activity timeline on an internally scrollable managed panel layout", () => {
    const { container } = render(
      <TimelinePanel
        events={[
          {
            event_id: "evt-1",
            run_event_seq: 1,
            attempt_seq: 1,
            event_type: "attempt.started",
            emitted_at: "2026-03-18T11:40:00Z",
            payload_json: {},
          },
        ]}
        hasMore={false}
      />,
    );

    const root = container.firstElementChild;
    expect(root).toHaveClass("min-h-0");

    const scrollRegion = container.querySelector(".overflow-y-auto");
    expect(scrollRegion).toHaveClass("min-h-0");
  });

  it("shows a load-more affordance when pagination is available", () => {
    render(
      <TimelinePanel
        events={[
          {
            event_id: "evt-1",
	            run_event_seq: 1,
	            attempt_seq: 1,
            event_type: "attempt.started",
            emitted_at: "2026-03-18T11:40:00Z",
            payload_json: {},
          },
        ]}
        hasMore
        onLoadMore={() => undefined}
      />,
    );

    expect(screen.getByRole("button", { name: /Load/i })).toBeInTheDocument();
  });

	it("does not auto-scroll to latest when older history is prepended", () => {
		const scrollIntoView = vi.fn();
		Object.defineProperty(Element.prototype, "scrollIntoView", {
			configurable: true,
			value: scrollIntoView,
		});

		const initialEvents = [{
			event_id: "evt-2",
			run_event_seq: 2,
			attempt_seq: 2,
			event_type: "attempt.succeeded",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: {},
		}];
		const { rerender } = render(<TimelinePanel events={initialEvents} hasMore />);
		scrollIntoView.mockClear();

		rerender(
			<TimelinePanel
				events={[
					{
						event_id: "evt-1",
						run_event_seq: 1,
						attempt_seq: 1,
						event_type: "attempt.started",
						emitted_at: "2026-03-18T11:40:00Z",
						payload_json: {},
					},
					...initialEvents,
				]}
				hasMore
			/>,
		);

		expect(scrollIntoView).not.toHaveBeenCalled();
	});

	it("uses instant auto-follow when newer live events append", () => {
		const scrollTo = vi.fn();
		Object.defineProperty(HTMLDivElement.prototype, "scrollTo", {
			configurable: true,
			value: scrollTo,
		});

		const initialEvents = [{
			event_id: "evt-1",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.started",
			emitted_at: "2026-03-18T11:40:00Z",
			payload_json: {},
		}];
		const { rerender } = render(<TimelinePanel events={initialEvents} hasMore={false} />);
		scrollTo.mockClear();

		rerender(
			<TimelinePanel
				events={[
					...initialEvents,
					{
						event_id: "evt-2",
						run_event_seq: 2,
						attempt_seq: 2,
						event_type: "attempt.succeeded",
						emitted_at: "2026-03-18T11:41:00Z",
						payload_json: {},
					},
				]}
				hasMore={false}
			/>,
		);

		expect(scrollTo).toHaveBeenCalledWith({ top: 0, behavior: "auto" });
	});

	it("does not auto-follow newer events while an activity item is expanded", async () => {
		const user = userEvent.setup();
		const scrollTo = vi.fn();
		Object.defineProperty(HTMLDivElement.prototype, "scrollTo", {
			configurable: true,
			value: scrollTo,
		});

		const initialEvents = [
			{
				event_id: "evt-1",
				run_event_seq: 1,
				attempt_seq: 1,
				event_type: "attempt.started",
				emitted_at: "2026-03-18T11:40:00Z",
				payload_json: { node_id: "usability_consultation", node_run_id: "nr-peasant-1" },
			},
		];
		const { rerender } = render(
			<TimelinePanel
				events={initialEvents}
				graph={{
					nodes: [{
						node_id: "usability_consultation",
						required_role: "usability-advisor",
						execution_ownership: "provider_backed",
						state: "running",
					}],
					edges: [],
				}}
				hasMore={false}
			/>,
		);
		scrollTo.mockClear();

		await user.click(screen.getByRole("button", { name: /Usability Consultation/i }));

		rerender(
			<TimelinePanel
				events={[
					...initialEvents,
					{
						event_id: "evt-2",
						run_event_seq: 2,
						attempt_seq: 2,
						event_type: "attempt.started",
						emitted_at: "2026-03-18T11:41:00Z",
						payload_json: { node_id: "plan_drafting", node_run_id: "nr-plan-1" },
					},
				]}
				graph={{
					nodes: [
						{
							node_id: "usability_consultation",
							required_role: "usability-advisor",
							execution_ownership: "provider_backed",
							state: "running",
						},
						{
							node_id: "plan_drafting",
							required_role: "plan-author",
							execution_ownership: "provider_backed",
							state: "running",
						},
					],
					edges: [],
				}}
				hasMore={false}
			/>,
		);

		expect(scrollTo).not.toHaveBeenCalled();
		expect(screen.getByRole("button", { name: /Jump to latest/i })).toBeInTheDocument();
	});

	it("shows a hidden latest update when a fixed-size tail advances while inspection is locked", async () => {
		const user = userEvent.setup();
		const initialEvents = [
			{
				event_id: "evt-1",
				run_event_seq: 1,
				attempt_seq: 1,
				event_type: "attempt.started",
				emitted_at: "2026-03-18T11:40:00Z",
				payload_json: { node_id: "usability_consultation", node_run_id: "nr-peasant-1" },
			},
			{
				event_id: "evt-2",
				run_event_seq: 2,
				attempt_seq: 2,
				event_type: "attempt.progress",
				emitted_at: "2026-03-18T11:41:00Z",
				payload_json: { node_id: "usability_consultation", node_run_id: "nr-peasant-1" },
			},
		];
		const graph = {
			nodes: [{
				node_id: "usability_consultation",
				required_role: "usability-advisor",
				execution_ownership: "provider_backed" as const,
				state: "running" as const,
			}],
			edges: [],
		};
		const view = render(<TimelinePanel events={initialEvents} graph={graph} hasMore={false} />);
		await user.click(screen.getByRole("button", { name: /Usability Consultation/i }));
		expect(screen.queryByRole("button", { name: /Jump to latest/i })).not.toBeInTheDocument();

		view.rerender(
			<TimelinePanel
				events={[
					initialEvents[1],
					{
						event_id: "evt-3",
						run_event_seq: 3,
						attempt_seq: 3,
						event_type: "attempt.succeeded",
						emitted_at: "2026-03-18T11:42:00Z",
						payload_json: { node_id: "usability_consultation", node_run_id: "nr-peasant-1" },
					},
				]}
				graph={graph}
				hasMore={false}
			/>,
		);

		expect(screen.getByRole("button", { name: /Jump to latest/i })).toBeInTheDocument();
	});
});
