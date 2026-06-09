import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render as rtlRender, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { RunDetailPage } from "./run-detail-page";
import type { ReactNode, Ref } from "react";
import { ApiError } from "@/api/client";
import { liveExecutionProjectionKey } from "./run-live-execution-projection";
import { liveTimelineQueryKey, liveTimelineSyncQueryKey } from "./run-live-timeline";
import { runSurfaceFreshnessQueryKey } from "./run-surface-freshness";

function render(ui: ReactNode = <RunDetailPage />) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
  const rendered = rtlRender(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>);
  return {
    ...rendered,
    queryClient,
    rerender: (nextUi: ReactNode) => rendered.rerender(<QueryClientProvider client={queryClient}>{nextUi}</QueryClientProvider>),
  };
}

const useRunEventStreamMock = vi.fn();
const timelinePanelPropsMock = vi.fn();
const executionSurfacePropsMock = vi.fn();
const governanceActionsPropsMock = vi.fn();
const currentPlanPanelPropsMock = vi.fn();
const fetchNextPageMock = vi.fn();
const timelineRefetchMock = vi.fn();
const navigateMock = vi.fn();
const useRunGraphMock = vi.fn();
const useRunExecutionsMock = vi.fn();
const useRunExecutionMock = vi.fn();
const useTimelineMock = vi.fn();
const useRunAgentsMock = vi.fn();
const useRolesMock = vi.fn();
const useMailboxMock = vi.fn();
const useMailboxMessageMock = vi.fn();
const useCancelRunMock = vi.fn();
const agentsRefetchMock = vi.fn();
const executionsRefetchMock = vi.fn();
const executionDetailRefetchMock = vi.fn();
const mailboxMessageRefetchMock = vi.fn();
const mockSearch: { tab?: string } = { tab: "graph" };
const mockRouteParams = { runId: "run-1" };

const mockRun = {
  run_id: "run-1",
  decree_id: "decree-1",
  decree_title: "Balance Layout",
  workflow_id: "royal-decree-v1",
  autonomous_approvals: false,
  status: "running",
  display_state: "running",
  plan_state: "approved",
  created_at: "2026-03-06T10:00:00.000Z",
  updated_at: "2026-03-06T10:01:00.000Z",
  last_activity_at: "2026-03-06T10:01:00.000Z",
  run_until_all_approve: false,
  review_rework_cycles: 0,
  review_current_wave: 0,
  validation_rework_waves: 0,
  validation_current_wave: 0,
  review_auto_succeeded_count: 0,
  validation_auto_succeeded_count: 0,
  max_execution_review_cycles: 5,
  max_validation_rework_cycles: 5,
  runtime_retries: [] as Array<{
    node_id: string;
    node_run_id: string;
    retry_index: number;
    retry_max: number;
    delay_seconds: number;
  }>,
	 tab_counts: {
	   dag_nodes: 6,
	   agents: 3,
	   activity_events: 50,
	   executions: 1,
	   messages: 2,
	 },
  failed_nodes: [] as Array<Record<string, unknown>>,
};

const mockTimelineItems: Array<Record<string, unknown>> = [];
const mockTimelineData: {
  items?: Array<Record<string, unknown>>;
  next_cursor?: string;
  pages?: Array<{ items: Array<Record<string, unknown>>; next_cursor?: string }>;
} = {
  items: mockTimelineItems,
  pages: [{ items: mockTimelineItems }],
};
let mockHasNextPage = false;
let mockRunError: Error | null = null;
let mockRunPending = false;
let mockRunDataUpdatedAt = 0;
let mockMailboxItems: Array<Record<string, unknown>> = [];
let mockExecutions: Array<Record<string, unknown>> = [];
let mockMailboxPending = false;
let mockMailboxError: Error | null = null;
let mockExecutionsPending = false;
let mockExecutionsError: Error | null = null;
let mockCancelPending = false;
let mockCancelError: Error | null = null;
const mailboxRefetchMock = vi.fn();
const cancelMutateAsyncMock = vi.fn().mockResolvedValue(undefined);
const graphRefetchMock = vi.fn();

vi.mock("@tanstack/react-router", () => ({
  useParams: () => mockRouteParams,
  useSearch: () => mockSearch,
  useNavigate: () => navigateMock,
  Link: ({ children }: { children: ReactNode }) => <a href="#">{children}</a>,
}));

vi.mock("./use-run-event-stream", () => ({
  useRunEventStream: (...args: unknown[]) => useRunEventStreamMock(...args),
}));

vi.mock("./dag-visualization", () => ({
  DAGVisualization: () => <div>graph</div>,
}));

vi.mock("./agent-detail-panel", () => ({
  AgentDetailPanel: () => <div>agents</div>,
}));

vi.mock("./run-execution-surface", () => ({
	RunExecutionSurface: (props: {
		events: Array<{ event_id: string }>;
		graph?: unknown;
		executions: Array<unknown>;
		stepGroups?: Array<{ nodeId: string; attempts: Array<unknown> }>;
		executionsErrorMessage?: string | null;
		showPrimaryExecutionError?: boolean;
		isSummaryLoading?: boolean;
		selectedNodeId?: string | null;
		selectedAttemptId?: string | null;
		onSelectStep?: (nodeId: string) => void;
		onSelectAttempt?: (attemptId: string) => void;
		onRetryExecutions?: () => void;
		detailPlaceholderMessage?: string | null;
		hasMoreEvents: boolean;
		onLoadMoreEvents?: () => void;
		isLoadingMoreEvents?: boolean;
	}) => {
		executionSurfacePropsMock(props);
		timelinePanelPropsMock({
			events: props.events,
			graph: props.graph,
			executions: props.executions,
			hasMore: props.hasMoreEvents,
			onLoadMore: props.onLoadMoreEvents,
			isLoadingMore: props.isLoadingMoreEvents,
		});
		return (
			<>
				<div>timeline</div>
				<div>executions</div>
			</>
		);
	},
}));

vi.mock("./mailbox-panel", () => ({
  MailboxPanel: () => <div>mailbox</div>,
}));

vi.mock("./governance-actions", () => ({
  GovernanceActions: (props: { finalApprovalCardRef?: Ref<HTMLDivElement> }) => {
    governanceActionsPropsMock(props);
    return <div ref={props.finalApprovalCardRef as never} data-testid="run-final-approval-target" tabIndex={-1}>governance</div>;
  },
}));

vi.mock("./current-plan-panel", () => ({
  CurrentPlanPanel: (props: { approvalSectionRef?: Ref<HTMLDivElement> }) => {
    currentPlanPanelPropsMock(props);
    return <div ref={props.approvalSectionRef as never} data-testid="run-plan-approval-target" tabIndex={-1}>plan</div>;
  },
}));

vi.mock("./decree-panel", () => ({
  DecreePanel: () => <div>decree</div>,
}));

vi.mock("@/api/hooks", () => ({
  useRun: () => ({
    data: mockRunError || mockRunPending ? undefined : mockRun,
    error: mockRunError,
    dataUpdatedAt: mockRunDataUpdatedAt,
    isError: mockRunError !== null,
    isPending: mockRunPending,
    isSuccess: !mockRunPending && mockRunError === null,
  }),
  useRunGraph: (...args: unknown[]) => useRunGraphMock(...args),
  useRunExecutions: (...args: unknown[]) => useRunExecutionsMock(...args),
  useRunExecution: (...args: unknown[]) => useRunExecutionMock(...args),
  useTimeline: (...args: unknown[]) => useTimelineMock(...args),
  useRunAgents: (...args: unknown[]) => useRunAgentsMock(...args),
  useRoles: (...args: unknown[]) => useRolesMock(...args),
  useMailbox: (...args: unknown[]) => useMailboxMock(...args),
  useMailboxMessage: (...args: unknown[]) => useMailboxMessageMock(...args),
  useCancelRun: (...args: unknown[]) => useCancelRunMock(...args),
  newIdempotencyKey: () => "idem-cancel-test",
}));

describe("run detail page", () => {
  beforeEach(() => {
    useRunEventStreamMock.mockReset();
    useRunEventStreamMock.mockReturnValue(undefined);
    timelinePanelPropsMock.mockReset();
		executionSurfacePropsMock.mockReset();
    governanceActionsPropsMock.mockReset();
    currentPlanPanelPropsMock.mockReset();
    fetchNextPageMock.mockReset();
    timelineRefetchMock.mockReset();
    navigateMock.mockReset();
    mockRouteParams.runId = "run-1";
    mockSearch.tab = "graph";
    mockRun.status = "running";
    mockRun.display_state = "running";
    mockRun.plan_state = "approved";
    mockRun.autonomous_approvals = false;
    mockRun.run_until_all_approve = false;
    mockRun.review_rework_cycles = 0;
    mockRun.review_auto_succeeded_count = 0;
    mockRun.validation_rework_waves = 0;
    mockRun.validation_auto_succeeded_count = 0;
    mockRun.runtime_retries = [];
    mockRun.tab_counts.dag_nodes = 6;
	    mockRun.tab_counts.agents = 3;
	    mockRun.tab_counts.activity_events = 50;
	    mockRun.tab_counts.executions = 1;
	    mockRun.tab_counts.messages = 2;
    mockRun.failed_nodes = [];
    mockTimelineItems.length = 0;
    mockExecutions = [{
			attempt_id: "attempt-1",
			node_run_id: "nr-1",
			node_id: "delegated_execution_berserker",
			attempt: 1,
			agent_id: "agent-1",
			required_role: "builder-berserker",
			execution_mode: "provider_backed",
			status: "succeeded",
			activity_at: "2026-03-06T10:01:00.000Z",
			updated_at: "2026-03-06T10:01:00.000Z",
		}];
    mockTimelineData.items = mockTimelineItems;
    mockTimelineData.next_cursor = undefined;
    mockTimelineData.pages = [{ items: mockTimelineItems }];
    mockHasNextPage = false;
    mockRunError = null;
    mockRunPending = false;
    mockRunDataUpdatedAt = 0;
    mockMailboxItems = [];
    mockExecutionsPending = false;
    mockExecutionsError = null;
    mockMailboxPending = false;
    mockMailboxError = null;
    mockCancelPending = false;
    mockCancelError = null;
    useRunGraphMock.mockReset();
    graphRefetchMock.mockReset();
    useRunGraphMock.mockImplementation(() => ({
      data: { run_id: "run-1", nodes: [], edges: [] },
      isPending: false,
	      isError: false,
	      error: null,
	      refetch: graphRefetchMock,
    }));
    useTimelineMock.mockReset();
    useTimelineMock.mockImplementation(() => ({
      data: mockTimelineData,
      isPending: false,
      isError: false,
      error: null,
      hasNextPage: mockHasNextPage,
      isFetching: false,
      isFetchingNextPage: false,
      fetchNextPage: fetchNextPageMock,
      refetch: timelineRefetchMock,
    }));
    useRunExecutionsMock.mockReset();
    executionsRefetchMock.mockReset();
    useRunExecutionsMock.mockImplementation(() => ({
			data: mockExecutionsPending || mockExecutionsError ? undefined : { run_id: "run-1", items: mockExecutions },
			isPending: mockExecutionsPending,
			isFetching: false,
			isError: mockExecutionsError !== null,
			error: mockExecutionsError,
			refetch: executionsRefetchMock,
		}));
    useRunExecutionMock.mockReset();
    executionDetailRefetchMock.mockReset();
    useRunExecutionMock.mockImplementation(() => ({
			data: {
				...mockExecutions[0],
				provider_invocation_id: "inv-1",
				settlement_status: "succeeded",
				output_status: "settled",
				output_text: "VERDICT: PASS\n\nCaptured output.",
				output_verdict: "PASS",
				output_verdict_status: "settled",
			},
			isPending: false,
			isFetching: false,
			isError: false,
			error: null,
			refetch: executionDetailRefetchMock,
		}));
    useRunAgentsMock.mockReset();
    agentsRefetchMock.mockReset();
    useRunAgentsMock.mockImplementation(() => ({
      data: { run_id: "run-1", items: [] },
      isFetching: false,
      refetch: agentsRefetchMock,
    }));
    useRolesMock.mockReset();
    useRolesMock.mockImplementation(() => ({ data: { items: [] } }));
    useMailboxMock.mockReset();
    useMailboxMock.mockImplementation(() => ({
      data: mockMailboxPending || mockMailboxError ? undefined : { run_id: "run-1", items: mockMailboxItems },
      isPending: mockMailboxPending,
      isError: mockMailboxError !== null,
      error: mockMailboxError,
      refetch: mailboxRefetchMock,
    }));
    useMailboxMessageMock.mockReset();
    mailboxMessageRefetchMock.mockReset();
    useMailboxMessageMock.mockImplementation(() => ({ data: null, isFetching: false, refetch: mailboxMessageRefetchMock }));
    useCancelRunMock.mockReset();
    useCancelRunMock.mockImplementation(() => ({
      mutateAsync: cancelMutateAsyncMock,
      isPending: mockCancelPending,
      error: mockCancelError,
    }));
    mailboxRefetchMock.mockReset();
    cancelMutateAsyncMock.mockClear();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses equal desktop columns for workspace and current plan", () => {
    render(<RunDetailPage />);
    expect(screen.getByTestId("run-detail-layout")).toHaveClass("lg:grid-cols-2");
  });

	it("subscribes the run-detail page to the canonical run SSE hook", () => {
	  render(<RunDetailPage />);
	  expect(useRunEventStreamMock).toHaveBeenCalledWith("run-1", true, true, ["executions", "timeline", "agents", "mailbox"]);
	});

	it("marks the full recovery scope stale while the run stream is retrying", async () => {
		vi.useFakeTimers();
		useRunEventStreamMock.mockReturnValue("retrying");

		const { queryClient } = render(<RunDetailPage />);
		const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");

		await act(async () => {
		  await vi.advanceTimersByTimeAsync(3_000);
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "plan"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "governance-summary"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "governance-decisions", "history"], exact: true, refetchType: "active" });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "executions"], exact: false, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "graph"], exact: true, refetchType: "active" });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false, refetchType: "active" });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "agents"], exact: true, refetchType: "active" });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "mailbox"], exact: false, refetchType: "active" });
		expect(queryClient.getQueryData(runSurfaceFreshnessQueryKey("run-1"))).toEqual({ dirtySurfaces: ["executions", "timeline", "agents", "mailbox"] });
	});

	it("bootstrap-refetches the run-detail recovery scope when reopening from stale cache without a resume cursor", async () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2026-03-06T12:00:00.000Z"));
		mockRunDataUpdatedAt = Date.now() - 5_000;
		const invalidateSpy = vi.spyOn(QueryClient.prototype, "invalidateQueries");

		render(<RunDetailPage />);

		await act(async () => {
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "plan"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "governance-summary"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "governance-decisions", "history"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "executions"], exact: false, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "graph"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "agents"], exact: true, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "mailbox"], exact: false, refetchType: "active" });
		invalidateSpy.mockRestore();
	});

	it("keeps executions lazy while hiding non-active timeline, agent, and mailbox queries on the default graph tab", () => {
	  render(<RunDetailPage />);

	  expect(useRunExecutionsMock).toHaveBeenCalledWith("run-1", { enabled: false });
	  expect(useRunExecutionMock).toHaveBeenCalledWith("run-1", null, { enabled: false });
	  expect(useRunGraphMock).toHaveBeenCalledWith("run-1", { enabled: true });
	  expect(useTimelineMock).toHaveBeenCalledWith("run-1", 50, { enabled: false });
	  expect(useRunAgentsMock).toHaveBeenCalledWith("run-1", { enabled: false });
	  expect(useMailboxMock).toHaveBeenCalledWith("run-1", {}, { enabled: false });
	  expect(useMailboxMessageMock).toHaveBeenCalledWith("run-1", null, { enabled: false });
	});

	it("enables only the active agents tab query for a running run", () => {
	  mockSearch.tab = "agents";

	  render(<RunDetailPage />);

	  expect(useRunExecutionsMock).toHaveBeenCalledWith("run-1", { enabled: false });
	  expect(useRunGraphMock).toHaveBeenCalledWith("run-1", { enabled: false });
	  expect(useRunAgentsMock).toHaveBeenCalledWith("run-1", { enabled: true });
	  expect(useMailboxMock).toHaveBeenCalledWith("run-1", {}, { enabled: false });
	});

	it("fetches graph context when the activity tab is opened directly", () => {
		mockSearch.tab = "execution";

		render(<RunDetailPage />);

		expect(useRunGraphMock).toHaveBeenCalledWith("run-1", { enabled: true });
		expect(useRunExecutionsMock).toHaveBeenCalledWith("run-1", { enabled: true });
		expect(useTimelineMock).toHaveBeenCalledWith("run-1", 50, { enabled: true });
	});

	it("uses failed-node summaries while still fetching graph context for activity", () => {
		mockRun.status = "failed";
		mockRun.display_state = "failed";
		mockSearch.tab = "execution";
    mockRun.failed_nodes = [
      {
        node_id: "delegated_execution_berserker",
        required_role: "builder-berserker",
        execution_ownership: "provider_backed",
        state: "failed",
        failure_reason: "Builder crashed while preparing workspace.",
      },
    ];

		render(<RunDetailPage />);

		expect(useRunGraphMock).toHaveBeenCalledWith("run-1", { enabled: true });
		expect(governanceActionsPropsMock).toHaveBeenCalledWith(
			expect.objectContaining({
				failedNodes: mockRun.failed_nodes,
        showFinalApproval: false,
      }),
    );
  });

  it("renders activity as soon as timeline data is ready even if the graph query is unresolved", () => {
    mockSearch.tab = "execution";
    useRunGraphMock.mockImplementation(() => ({
      data: undefined,
      isPending: true,
    }));
	    mockTimelineItems.push({ event_id: "evt-1", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 1, emitted_at: "2026-03-06T10:00:00.000Z", payload_json: {} });

    render(<RunDetailPage />);

    expect(screen.getByText("timeline")).toBeInTheDocument();
    expect(screen.queryByText("Loading...")).not.toBeInTheDocument();
  });

	it("passes graph data into the activity timeline when available", () => {
		mockSearch.tab = "execution";
		useRunGraphMock.mockImplementation(() => ({
			data: {
				run_id: "run-1",
				nodes: [{ node_id: "plan_drafting", required_role: "council-member", execution_ownership: "provider_backed", state: "succeeded" }],
				edges: [{ from: "advisory_synthesis", to: "plan_drafting", on: "success" }],
			},
			isPending: false,
			isError: false,
			error: null,
			refetch: graphRefetchMock,
		}));
		mockTimelineItems.push({ event_id: "evt-1", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 1, emitted_at: "2026-03-06T10:00:00.000Z", payload_json: { node_id: "plan_drafting" } });

		render(<RunDetailPage />);

		const lastCall = timelinePanelPropsMock.mock.calls.at(-1)?.[0] as {
			graph?: { nodes: Array<{ node_id: string }>; edges: Array<{ from: string; to: string }> };
		};
		expect(lastCall.graph?.nodes).toEqual([{ node_id: "plan_drafting", required_role: "council-member", execution_ownership: "provider_backed", state: "succeeded" }]);
		expect(lastCall.graph?.edges).toEqual([{ from: "advisory_synthesis", to: "plan_drafting", on: "success" }]);
	});

	it("shows a loading state while the timeline query is still resolving", () => {
		mockSearch.tab = "execution";
    useTimelineMock.mockImplementation(() => ({
      data: undefined,
      isPending: true,
      isFetching: true,
      isFetchingNextPage: false,
      isError: false,
      error: null,
      hasNextPage: false,
      fetchNextPage: fetchNextPageMock,
      refetch: timelineRefetchMock,
    }));

    render(<RunDetailPage />);

		expect(screen.getByText("Loading...")).toBeInTheDocument();
		expect(screen.queryByText("timeline")).not.toBeInTheDocument();
	});

	it("keeps the activity tab in a loading state while executions are still pending after an empty timeline settles", () => {
		mockSearch.tab = "execution";
		mockExecutionsPending = true;
		useTimelineMock.mockImplementation(() => ({
			data: mockTimelineData,
			isPending: false,
			isFetching: false,
			isFetchingNextPage: false,
			isError: false,
			error: null,
			hasNextPage: false,
			fetchNextPage: fetchNextPageMock,
			refetch: timelineRefetchMock,
		}));

		render(<RunDetailPage />);

		expect(screen.getByText("Loading...")).toBeInTheDocument();
		expect(screen.queryByText("timeline")).not.toBeInTheDocument();
	});

	it("shows an explicit timeline error state with retry instead of an empty state", async () => {
    const user = userEvent.setup();
    mockSearch.tab = "execution";
		mockExecutionsError = new Error("Execution list unavailable");
    useTimelineMock.mockImplementation(() => ({
      data: undefined,
      isPending: false,
      isFetching: false,
      isFetchingNextPage: false,
      isError: true,
      error: new Error("Timeline unavailable"),
      hasNextPage: false,
      fetchNextPage: fetchNextPageMock,
      refetch: timelineRefetchMock,
    }));

    render(<RunDetailPage />);

    expect(screen.getByText("Timeline unavailable")).toBeInTheDocument();
    expect(screen.queryByText("No events recorded yet.")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(timelineRefetchMock).toHaveBeenCalledTimes(1);
  });

	it("shows an explicit executions error state when activity has no usable data yet", async () => {
		const user = userEvent.setup();
		mockSearch.tab = "execution";
		mockExecutionsError = new Error("Execution list unavailable");
		useTimelineMock.mockImplementation(() => ({
			data: mockTimelineData,
			isPending: false,
			isFetching: false,
			isFetchingNextPage: false,
			isError: false,
			error: null,
			hasNextPage: false,
			fetchNextPage: fetchNextPageMock,
			refetch: timelineRefetchMock,
		}));

		render(<RunDetailPage />);

		expect(screen.getByText("Execution list unavailable")).toBeInTheDocument();
		expect(screen.queryByText("Waiting to start")).not.toBeInTheDocument();
		expect(screen.queryByText("timeline")).not.toBeInTheDocument();

		await user.click(screen.getByRole("button", { name: "Retry" }));
		expect(executionsRefetchMock).toHaveBeenCalledTimes(1);
	});

	it("surfaces timeline refresh failures even while cached activity remains visible", async () => {
		const user = userEvent.setup();
		mockSearch.tab = "execution";
		mockTimelineItems.push({ event_id: "evt-1", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 1, emitted_at: "2026-03-06T10:00:00.000Z", payload_json: {} });
		useTimelineMock.mockImplementation(() => ({
			data: mockTimelineData,
			isPending: false,
			isFetching: false,
			isFetchingNextPage: false,
			isError: true,
			error: new Error("Timeline refresh failed"),
			hasNextPage: false,
			fetchNextPage: fetchNextPageMock,
			refetch: timelineRefetchMock,
		}));

		render(<RunDetailPage />);

		expect(screen.getByText("timeline")).toBeInTheDocument();
		expect(screen.getByText("Timeline refresh failed")).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: "Retry" }));
		expect(timelineRefetchMock).toHaveBeenCalledTimes(1);
	});

	it("keeps the execution surface visible when the timeline fails but executions loaded", () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			{
				attempt_id: "attempt-2",
				node_run_id: "nr-2",
				node_id: "execution_review_shieldmaiden",
				attempt: 1,
				agent_id: "agent-2",
				required_role: "reviewer-shieldmaiden",
				execution_mode: "provider_backed",
				status: "running",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			},
		];
		useTimelineMock.mockImplementation(() => ({
			data: undefined,
			isPending: false,
			isFetching: false,
			isFetchingNextPage: false,
			isError: true,
			error: new Error("Timeline unavailable"),
			hasNextPage: false,
			fetchNextPage: fetchNextPageMock,
			refetch: timelineRefetchMock,
		}));

		render(<RunDetailPage />);

		expect(screen.getByText("executions")).toBeInTheDocument();
		expect(screen.getByText("timeline")).toBeInTheDocument();
		expect(screen.getByText("Timeline unavailable")).toBeInTheDocument();
		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			executions: Array<{ attempt_id: string }>;
		};
		expect(lastCall.executions).toEqual([
			expect.objectContaining({ attempt_id: "attempt-2" }),
		]);
	});

	it("keeps the execution surface visible and passes through executions fetch failures", () => {
		mockSearch.tab = "execution";
		mockExecutions = [];
		mockExecutionsError = new Error("Execution inspector unavailable");
		mockTimelineItems.push({ event_id: "evt-1", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 1, emitted_at: "2026-03-06T10:00:00.000Z", payload_json: {} });

		render(<RunDetailPage />);

		expect(screen.getByText("timeline")).toBeInTheDocument();
		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			executions: Array<unknown>;
			executionsErrorMessage?: string | null;
			showPrimaryExecutionError?: boolean;
		};
		expect(lastCall.executions).toEqual([]);
		expect(lastCall.executionsErrorMessage).toBe("Execution inspector unavailable");
		expect(lastCall.showPrimaryExecutionError).toBe(true);
	});

	it("marks the execution hero as loading when timeline activity is visible before executions resolve", () => {
		mockSearch.tab = "execution";
		mockExecutions = [];
		mockTimelineItems.push({ event_id: "evt-1", event_type: "attempt.started", run_event_seq: 1, attempt_seq: 1, emitted_at: "2026-03-06T10:00:00.000Z", payload_json: {} });
		useRunExecutionsMock.mockImplementation(() => ({
			data: undefined,
			isPending: true,
			isFetching: false,
			isError: false,
			error: null,
			refetch: executionsRefetchMock,
		}));

		render(<RunDetailPage />);

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			isSummaryLoading?: boolean;
			showPrimaryExecutionError?: boolean;
		};
		expect(lastCall.isSummaryLoading).toBe(true);
		expect(lastCall.showPrimaryExecutionError).toBe(false);
	});

	it("auto-focuses the most relevant active execution instead of the first returned item", () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			{
				attempt_id: "attempt-succeeded",
				node_run_id: "nr-succeeded",
				node_id: "delegated_execution_berserker",
				attempt: 1,
				agent_id: "agent-1",
				required_role: "builder-berserker",
				execution_mode: "provider_backed",
				status: "succeeded",
				activity_at: "2026-03-06T10:03:00.000Z",
				updated_at: "2026-03-06T10:03:00.000Z",
			},
			{
				attempt_id: "attempt-running",
				node_run_id: "nr-running",
				node_id: "execution_review_shieldmaiden",
				attempt: 2,
				agent_id: "agent-2",
				required_role: "reviewer-shieldmaiden",
				execution_mode: "provider_backed",
				status: "running",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			},
			{
				attempt_id: "attempt-failed",
				node_run_id: "nr-failed",
				node_id: "validation_gates_qa_scout",
				attempt: 3,
				agent_id: "agent-3",
				required_role: "intelligence-qa-scout",
				execution_mode: "provider_backed",
				status: "failed",
				activity_at: "2026-03-06T10:04:00.000Z",
				updated_at: "2026-03-06T10:04:00.000Z",
			},
		];

		render(<RunDetailPage />);

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedAttemptId?: string | null;
		};
		expect(lastCall.selectedAttemptId).toBe("attempt-running");
	});

	it("auto-focuses the latest terminal execution when no work is in flight", () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			{
				attempt_id: "attempt-failed",
				node_run_id: "nr-failed",
				node_id: "execution_review_shieldmaiden",
				attempt: 1,
				agent_id: "agent-1",
				required_role: "reviewer-shieldmaiden",
				execution_mode: "provider_backed",
				status: "failed",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			},
			{
				attempt_id: "attempt-succeeded",
				node_run_id: "nr-succeeded",
				node_id: "validation_gates_qa_scout",
				attempt: 2,
				agent_id: "agent-2",
				required_role: "intelligence-qa-scout",
				execution_mode: "provider_backed",
				status: "succeeded",
				activity_at: "2026-03-06T10:04:00.000Z",
				updated_at: "2026-03-06T10:04:00.000Z",
			},
		];

		render(<RunDetailPage />);

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedAttemptId?: string | null;
			selectedNodeId?: string | null;
		};
		expect(lastCall.selectedNodeId).toBe("validation_gates_qa_scout");
		expect(lastCall.selectedAttemptId).toBe("attempt-succeeded");
	});

	it("preserves a manual execution selection after the operator changes focus", async () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			{
				attempt_id: "attempt-running",
				node_run_id: "nr-running",
				node_id: "execution_review_shieldmaiden",
				attempt: 2,
				agent_id: "agent-2",
				required_role: "reviewer-shieldmaiden",
				execution_mode: "provider_backed",
				status: "running",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			},
			{
				attempt_id: "attempt-failed",
				node_run_id: "nr-failed",
				node_id: "validation_gates_qa_scout",
				attempt: 3,
				agent_id: "agent-3",
				required_role: "intelligence-qa-scout",
				execution_mode: "provider_backed",
				status: "failed",
				activity_at: "2026-03-06T10:04:00.000Z",
				updated_at: "2026-03-06T10:04:00.000Z",
			},
		];

		const { rerender } = render(<RunDetailPage />);
		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			onSelectStep?: (nodeId: string) => void;
		};

		await act(async () => {
			lastCall.onSelectStep?.("validation_gates_qa_scout");
		});
		await act(async () => {
			rerender(<RunDetailPage />);
		});

		const updatedCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId?: string | null;
			selectedAttemptId?: string | null;
		};
		expect(updatedCall.selectedNodeId).toBe("validation_gates_qa_scout");
		expect(updatedCall.selectedAttemptId).toBe("attempt-failed");
	});

	it("keeps a manual execution selection stable through transient refresh gaps and unrelated newer steps", async () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			{
				attempt_id: "attempt-review",
				node_run_id: "nr-review",
				node_id: "execution_review_shieldmaiden",
				attempt: 1,
				agent_id: "agent-review",
				required_role: "reviewer-shieldmaiden",
				execution_mode: "provider_backed",
				status: "running",
				queued_at: "2026-03-06T10:00:00.000Z",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			},
			{
				attempt_id: "attempt-peasant",
				node_run_id: "nr-peasant",
				node_id: "advisor_consultation_peasant",
				attempt: 1,
				agent_id: "agent-peasant",
				required_role: "peasant-delegate",
				execution_mode: "provider_backed",
				status: "succeeded",
				queued_at: "2026-03-06T10:00:00.000Z",
				activity_at: "2026-03-06T10:01:00.000Z",
				updated_at: "2026-03-06T10:01:00.000Z",
			},
		];

		const { rerender } = render(<RunDetailPage />);
		const firstCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			onSelectStep?: (nodeId: string) => void;
		};

		await act(async () => {
			firstCall.onSelectStep?.("advisor_consultation_peasant");
		});

		mockExecutions = [];
		await act(async () => {
			rerender(<RunDetailPage />);
		});

		let updatedCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId?: string | null;
			selectedAttemptId?: string | null;
		};
		expect(updatedCall.selectedNodeId).toBe("advisor_consultation_peasant");
		expect(updatedCall.selectedAttemptId).toBe("attempt-peasant");

		mockExecutions = [
			{
				attempt_id: "attempt-plan",
				node_run_id: "nr-plan",
				node_id: "plan_drafting",
				attempt: 2,
				agent_id: "agent-plan",
				required_role: "council-member",
				execution_mode: "provider_backed",
				status: "running",
				queued_at: "2026-03-06T10:03:00.000Z",
				activity_at: "2026-03-06T10:05:00.000Z",
				updated_at: "2026-03-06T10:05:00.000Z",
			},
			{
				attempt_id: "attempt-peasant",
				node_run_id: "nr-peasant",
				node_id: "advisor_consultation_peasant",
				attempt: 1,
				agent_id: "agent-peasant",
				required_role: "peasant-delegate",
				execution_mode: "provider_backed",
				status: "succeeded",
				queued_at: "2026-03-06T10:00:00.000Z",
				activity_at: "2026-03-06T10:01:00.000Z",
				updated_at: "2026-03-06T10:01:00.000Z",
			},
		];
		await act(async () => {
			rerender(<RunDetailPage />);
		});

		updatedCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId?: string | null;
			selectedAttemptId?: string | null;
		};
		expect(updatedCall.selectedNodeId).toBe("advisor_consultation_peasant");
		expect(updatedCall.selectedAttemptId).toBe("attempt-peasant");
	});

	it("shows live-only execution activity immediately and defers detail fetch until executions catch up", async () => {
		mockSearch.tab = "execution";
		mockExecutions = [];
		mockRun.tab_counts.executions = 0;
		useRunExecutionMock.mockImplementation((runId: string, attemptId: string | null, options: { enabled?: boolean }) => ({
			data: options.enabled && attemptId
				? {
					...mockExecutions[0],
					attempt_id: attemptId,
					node_run_id: `${runId}-${attemptId}`,
					provider_invocation_id: "inv-1",
					settlement_status: "succeeded",
					output_status: "settled",
					output_text: "VERDICT: PASS\n\nCaptured output.",
					output_verdict: "PASS",
					output_verdict_status: "settled",
				}
				: undefined,
			isPending: false,
			isFetching: false,
			isError: false,
			error: null,
			refetch: executionDetailRefetchMock,
			options,
		}));

		const { queryClient, rerender } = render(<RunDetailPage />);

		await act(async () => {
			queryClient.setQueryData(liveExecutionProjectionKey("run-1"), {
				"nr-live-only": {
					node_run_id: "nr-live-only",
					attempt_id: "attempt-live-only",
					node_id: "delegated_execution_berserker",
					attempt: 2,
					required_role: "builder-berserker",
					execution_mode: "provider_backed",
					activity_at: "2026-03-06T10:05:00.000Z",
					status: "running",
					started_at: "2026-03-06T10:05:00.000Z",
					live_label: "Execution started",
				},
			});
		});
		await act(async () => {
			rerender(<RunDetailPage />);
		});

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			executions?: Array<{ attempt_id: string; node_id: string; status: string }>;
			selectedAttemptId?: string | null;
			detailPlaceholderMessage?: string | null;
		};
		expect(lastCall.executions).toContainEqual(expect.objectContaining({
			attempt_id: "attempt-live-only",
			node_id: "delegated_execution_berserker",
			status: "running",
		}));
		expect(lastCall.selectedAttemptId).toBe("attempt-live-only");
		expect(lastCall.detailPlaceholderMessage).toContain("Live execution details are arriving");

		const latestExecutionDetailCall = useRunExecutionMock.mock.calls.at(-1);
		expect(latestExecutionDetailCall).toEqual(["run-1", "attempt-live-only", { enabled: false }]);
	});

	it("refetches the canonical timeline when hidden activity outruns the cached page", async () => {
	  const { queryClient, rerender } = render(<RunDetailPage />);

	  await act(async () => {
	    queryClient.setQueryData(liveTimelineQueryKey("run-1"), [
	      {
	        event_id: "evt-hidden-51",
	        event_type: "attempt.started",
	        run_event_seq: 51,
	        attempt_seq: 1,
	        emitted_at: "2026-03-06T10:02:00.000Z",
	        payload_json: {},
	      },
	    ]);
	    queryClient.setQueryData(liveTimelineSyncQueryKey("run-1"), {
	      dirtyStartSeq: 51,
	      dirtyEndSeq: 51,
	    });
	  });

	  mockSearch.tab = "execution";
	  rerender(<RunDetailPage />);
	  await act(async () => {
	    await Promise.resolve();
	  });

	  expect(timelineRefetchMock).toHaveBeenCalledTimes(1);
	});

	it("backs off hidden timeline refetch retries after a failed reconciliation attempt", async () => {
	  vi.useFakeTimers();
	  timelineRefetchMock.mockRejectedValue(new Error("timeline refetch failed"));
	  const { queryClient, rerender } = render(<RunDetailPage />);

	  await act(async () => {
	    queryClient.setQueryData(liveTimelineQueryKey("run-1"), [
	      {
	        event_id: "evt-hidden-51",
	        event_type: "attempt.started",
	        run_event_seq: 51,
	        attempt_seq: 1,
	        emitted_at: "2026-03-06T10:02:00.000Z",
	        payload_json: {},
	      },
	    ]);
	    queryClient.setQueryData(liveTimelineSyncQueryKey("run-1"), {
	      dirtyStartSeq: 51,
	      dirtyEndSeq: 51,
	    });
	  });

	  mockSearch.tab = "execution";
	  rerender(<RunDetailPage />);
	  await act(async () => {
	    await Promise.resolve();
	  });

	  expect(timelineRefetchMock).toHaveBeenCalledTimes(1);

	  rerender(<RunDetailPage />);
	  await act(async () => {
	    await Promise.resolve();
	  });

	  expect(timelineRefetchMock).toHaveBeenCalledTimes(1);

	  await act(async () => {
	    await vi.advanceTimersByTimeAsync(1_000);
	  });

	  expect(timelineRefetchMock).toHaveBeenCalledTimes(2);
	});

	it("fetches older canonical timeline pages until a hidden gap is covered", async () => {
	  mockSearch.tab = "execution";
	  mockHasNextPage = true;
	  mockTimelineData.pages = [{
	    items: [{
	      event_id: "evt-51",
	      event_type: "attempt.started",
	      run_event_seq: 51,
	      attempt_seq: 1,
	      emitted_at: "2026-03-06T10:02:00.000Z",
	      payload_json: {},
	    }],
	    next_cursor: "cursor-2",
	  }];

	  const { queryClient, rerender } = render(<RunDetailPage />);

	  await act(async () => {
	    queryClient.setQueryData(liveTimelineQueryKey("run-1"), [
	      {
	        event_id: "evt-41",
	        event_type: "attempt.started",
	        run_event_seq: 41,
	        attempt_seq: 1,
	        emitted_at: "2026-03-06T10:01:00.000Z",
	        payload_json: {},
	      },
	      {
	        event_id: "evt-51",
	        event_type: "attempt.started",
	        run_event_seq: 51,
	        attempt_seq: 1,
	        emitted_at: "2026-03-06T10:02:00.000Z",
	        payload_json: {},
	      },
	    ]);
	    queryClient.setQueryData(liveTimelineSyncQueryKey("run-1"), {
	      dirtyStartSeq: 41,
	      dirtyEndSeq: 51,
	    });
	  });
	  rerender(<RunDetailPage />);
	  await act(async () => {
	    await Promise.resolve();
	    await Promise.resolve();
	  });

	  expect(fetchNextPageMock).toHaveBeenCalledTimes(1);
	});

	it("shows an explicit DAG error state with retry when the graph query fails", async () => {
	  const user = userEvent.setup();
	  useRunGraphMock.mockImplementation(() => ({
	    data: undefined,
	    isPending: false,
	    isError: true,
	    error: new Error("Graph unavailable"),
	    refetch: graphRefetchMock,
	  }));

	  render(<RunDetailPage />);

	  expect(screen.getByText("Graph unavailable")).toBeInTheDocument();
	  await user.click(screen.getByRole("button", { name: "Retry" }));
	  expect(graphRefetchMock).toHaveBeenCalledTimes(1);
	});

	it("renders a not-found state and disables live SSE when the run query returns 404", () => {
	    mockRunError = new ApiError(404, {
	      message: "run not found",
	      code: "dependency_blocked",
	      retryable: false,
	      category: "core",
	      details: {},
	      correlationId: "api.get_run",
	    });

	    render(<RunDetailPage />);

    expect(screen.getByText("Run not found")).toBeInTheDocument();
	    expect(screen.getByText("The requested run is unavailable or may have been removed."))
	      .toBeInTheDocument();
	    expect(screen.getByRole("link", { name: "Back to runs" })).toBeInTheDocument();
	    expect(screen.queryByText("Loading run...")).not.toBeInTheDocument();
	    expect(useRunEventStreamMock).toHaveBeenCalledWith("run-1", false, true, ["executions", "timeline", "agents", "mailbox"]);
  });

	it("transitions from pending to loaded without violating hook order", () => {
		mockRunPending = true;

		const { rerender } = render(<RunDetailPage />);

		expect(screen.getByText("Loading run...")).toBeInTheDocument();

		mockRunPending = false;
		mockRun.plan_state = "awaiting_king_decision";
		mockRun.display_state = "awaiting_plan_decision";
		mockRun.autonomous_approvals = false;

		rerender(<RunDetailPage />);

		expect(screen.getByTestId("run-governance-notice")).toHaveTextContent("Awaiting your plan approval");
		expect(screen.getByRole("button", { name: "Review Plan" })).toBeInTheDocument();

		mockRun.plan_state = "approved";
		mockRun.display_state = "running";
	});

	it("refetches a dirty hidden agents surface when the tab becomes visible", async () => {
		const { queryClient, rerender } = render(<RunDetailPage />);

		await act(async () => {
			queryClient.setQueryData(runSurfaceFreshnessQueryKey("run-1"), { dirtySurfaces: ["agents"] });
		});

		mockSearch.tab = "agents";
		rerender(<RunDetailPage />);
		await act(async () => {
			await Promise.resolve();
		});

		expect(agentsRefetchMock).toHaveBeenCalledTimes(1);
	});

	it("refetches a dirty executions surface when the tab becomes visible", async () => {
		const { queryClient, rerender } = render(<RunDetailPage />);

		await act(async () => {
			queryClient.setQueryData(runSurfaceFreshnessQueryKey("run-1"), { dirtySurfaces: ["executions"] });
		});

		mockSearch.tab = "execution";
		rerender(<RunDetailPage />);

		await act(async () => {
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(executionsRefetchMock).toHaveBeenCalledTimes(1);
		expect(executionDetailRefetchMock).toHaveBeenCalledTimes(1);
	});

	it("does not reuse a previous run execution selection after navigation", async () => {
		mockSearch.tab = "execution";
		mockRun.run_id = "run-1";
		useRunExecutionsMock.mockImplementation((runId: string) => ({
			data: {
				run_id: runId,
				items: runId === "run-1"
					? [{ ...mockExecutions[0], attempt_id: "attempt-run-1" }]
					: [{ ...mockExecutions[0], attempt_id: "attempt-run-2", node_run_id: "nr-2" }],
			},
			isPending: false,
			isFetching: false,
			isError: false,
			error: null,
			refetch: executionsRefetchMock,
		}));
		useRunExecutionMock.mockImplementation((runId: string, attemptId: string | null, options: { enabled?: boolean }) => ({
			data: attemptId
				? {
					...mockExecutions[0],
					attempt_id: attemptId,
					node_run_id: `${runId}-${attemptId}`,
					provider_invocation_id: "inv-1",
					settlement_status: "succeeded",
					output_status: "settled",
					output_text: "VERDICT: PASS\n\nCaptured output.",
					output_verdict: "PASS",
					output_verdict_status: "settled",
				}
				: undefined,
			isPending: false,
			isFetching: false,
			isError: false,
			error: null,
			refetch: executionDetailRefetchMock,
			options,
		}));

		const { rerender } = render(<RunDetailPage />);

		await act(async () => {
			await Promise.resolve();
		});

		mockRouteParams.runId = "run-2";
		mockRun.run_id = "run-2";
		rerender(<RunDetailPage />);

		const run2Calls = useRunExecutionMock.mock.calls.filter((call) => call[0] === "run-2");
		expect(run2Calls[0]).toEqual(["run-2", null, { enabled: false }]);
	});

	it("shows run tabs in DAG, Activity, Agents, Messages order", () => {
    render(<RunDetailPage />);
    const tabNav = screen.getByTestId("run-tab-nav");
    const tabButtons = within(tabNav)
      .getAllByRole("tab")
      .map((button) => button.textContent ?? "");

    expect(tabButtons[0]).toContain("DAG");
	  expect(tabButtons[1]).toContain("Activity");
	  expect(tabButtons[2]).toContain("Agents");
	  expect(tabButtons[3]).toContain("Messages");
	});

  it("renders semantic tabs for run detail navigation", () => {
    render(<RunDetailPage />);

	  expect(screen.getByRole("tablist", { name: "Run detail sections" })).toBeInTheDocument();
	  expect(screen.getByRole("tab", { name: /DAG/i })).toHaveAttribute("aria-selected", "true");
	  expect(screen.getByRole("tab", { name: /Agents/i })).toHaveAttribute("aria-selected", "false");
	});

  it("shows an explicit plan approval notice when the run awaits manual plan approval", () => {
    mockRun.plan_state = "awaiting_king_decision";
    mockRun.display_state = "awaiting_plan_decision";
    mockRun.autonomous_approvals = false;

    render(<RunDetailPage />);

    const notice = screen.getByTestId("run-governance-notice");
    expect(notice).toHaveTextContent("Awaiting your plan approval");
    expect(notice).toHaveTextContent("Execution is paused until you confirm this plan or request rework.");

    mockRun.plan_state = "approved";
    mockRun.display_state = "running";
  });

  it("suppresses manual-governance notices for autonomous runs in awaiting_king_decision", () => {
    mockRun.plan_state = "awaiting_king_decision";
    mockRun.display_state = "awaiting_plan_decision";
    mockRun.autonomous_approvals = true;

    render(<RunDetailPage />);

    expect(screen.queryByText("Awaiting Your Plan Approval")).not.toBeInTheDocument();
    expect(screen.queryByTestId("run-governance-notice")).not.toBeInTheDocument();

    mockRun.plan_state = "approved";
    mockRun.display_state = "running";
    mockRun.autonomous_approvals = false;
  });

  it("shows an explicit final approval notice when the run awaits final confirmation", () => {
    mockRun.status = "awaiting_human_confirmation";
    mockRun.display_state = "awaiting_final_decision";
    mockRun.autonomous_approvals = false;

    render(<RunDetailPage />);

    const notice = screen.getByTestId("run-governance-notice");
    expect(notice).toHaveTextContent("Awaiting your final approval");
    expect(notice).toHaveTextContent("This run is paused until you confirm completion or request rework.");

    mockRun.status = "running";
    mockRun.display_state = "running";
  });

  it("keeps autonomous runs in awaiting_human_confirmation out of the manual governance stack", () => {
    mockRun.status = "awaiting_human_confirmation";
    mockRun.display_state = "awaiting_final_decision";
    mockRun.autonomous_approvals = true;

    render(<RunDetailPage />);

    expect(screen.queryByText("Awaiting Your Final Approval")).not.toBeInTheDocument();
    expect(screen.queryByTestId("run-governance-notice")).not.toBeInTheDocument();
    expect(screen.queryByText("governance")).not.toBeInTheDocument();

    mockRun.status = "running";
    mockRun.display_state = "running";
    mockRun.autonomous_approvals = false;
  });

  it("keeps final approval in the same right-column governance stack after the plan", () => {
    mockRun.status = "awaiting_human_confirmation";
    mockRun.display_state = "awaiting_final_decision";
    mockRun.autonomous_approvals = false;

    render(<RunDetailPage />);

    const plan = screen.getByText("plan");
    const governance = screen.getByText("governance");
    expect(plan.compareDocumentPosition(governance) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    mockRun.status = "running";
    mockRun.display_state = "running";
  });

  it("replaces the old title-row governance pill with a banner jump action for plan approval", () => {
    mockRun.plan_state = "awaiting_king_decision";
    mockRun.display_state = "awaiting_plan_decision";
    mockRun.autonomous_approvals = false;

    render(<RunDetailPage />);

    expect(screen.getByRole("button", { name: "Review Plan" })).toBeInTheDocument();
    expect(screen.queryByText("Governance Action Required")).not.toBeInTheDocument();

    mockRun.plan_state = "approved";
    mockRun.display_state = "running";
  });

  it("scrolls and focuses the plan approval target from the banner action", async () => {
    const user = userEvent.setup();
    const scrollIntoView = vi.fn();
    Object.defineProperty(Element.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    mockRun.plan_state = "awaiting_king_decision";
    mockRun.display_state = "awaiting_plan_decision";
    mockRun.autonomous_approvals = false;

    render(<RunDetailPage />);

    const target = screen.getByTestId("run-plan-approval-target");
    await user.click(screen.getByRole("button", { name: "Review Plan" }));

    expect(scrollIntoView).toHaveBeenCalledWith({ behavior: "smooth", block: "nearest", inline: "nearest" });
    expect(target).toHaveFocus();

    mockRun.plan_state = "approved";
    mockRun.display_state = "running";
  });

  it("scrolls and focuses the final approval target from the banner action", async () => {
    const user = userEvent.setup();
    const scrollIntoView = vi.fn();
    Object.defineProperty(Element.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    mockRun.status = "awaiting_human_confirmation";
    mockRun.display_state = "awaiting_final_decision";
    mockRun.autonomous_approvals = false;

    render(<RunDetailPage />);

    const target = screen.getByTestId("run-final-approval-target");
    await user.click(screen.getByRole("button", { name: "Review Final Decision" }));

    expect(scrollIntoView).toHaveBeenCalledWith({ behavior: "smooth", block: "nearest", inline: "nearest" });
    expect(target).toHaveFocus();

    mockRun.status = "running";
    mockRun.display_state = "running";
  });

	it("shows tab badge counts from the run detail summary before hidden tabs are opened", () => {
	  render(<RunDetailPage />);

	  expect(screen.getByRole("tab", { name: /DAG/i })).toHaveTextContent("DAG6");
	  expect(screen.getByRole("tab", { name: /Activity/i })).toHaveTextContent("Activity");
	  expect(screen.getByRole("tab", { name: /Agents/i })).toHaveTextContent("Agents3");
	  expect(screen.getByRole("tab", { name: /Messages/i })).toHaveTextContent("Messages2");
	});

  it("wires timeline pagination into the activity panel", () => {
    mockSearch.tab = "execution";
    mockTimelineData.items = [];
	    mockTimelineData.pages = [
	      {
	        items: [
	          { event_id: "evt-2", run_event_seq: 2, attempt_seq: 2, event_type: "attempt.succeeded", emitted_at: "2026-03-18T11:41:00Z", payload_json: {} },
	        ],
	        next_cursor: "cursor-1",
	      },
	      {
	        items: [
	          { event_id: "evt-1", run_event_seq: 1, attempt_seq: 1, event_type: "attempt.started", emitted_at: "2026-03-18T11:40:00Z", payload_json: {} },
	        ],
	      },
	    ];
    mockHasNextPage = true;

    render(<RunDetailPage />);

    const lastCall = timelinePanelPropsMock.mock.calls.at(-1)?.[0] as {
      events: Array<{ event_id: string }>;
      hasMore: boolean;
      onLoadMore?: () => void;
    };
    expect(lastCall.events.map((event) => event.event_id)).toEqual(["evt-1", "evt-2"]);
    expect(lastCall.hasMore).toBe(true);
    expect(typeof lastCall.onLoadMore).toBe("function");
  });

  it("uses non-wrapping metadata row in header command strip", () => {
    render(<RunDetailPage />);
    const metadataGroup = screen.getByTestId("run-header-meta-group");
    const metadataRow = screen.getByTestId("run-header-meta");
    expect(metadataGroup).toHaveClass("sm:flex");
    expect(metadataRow).toHaveTextContent("Created:");
    expect(metadataRow).toHaveTextContent("Last Activity:");
  });

  it("keeps a fixed header chrome row for separator alignment", () => {
    render(<RunDetailPage />);
    const title = screen.getByText("Balance Layout");
    const header = title.closest("header");
    expect(header).toHaveClass("h-[76px]");
    expect(header).not.toHaveClass("items-start");
  });

  it("renders run controls directly inside the fixed header", () => {
    render(<RunDetailPage />);
    const strip = screen.getByTestId("run-header-command-strip");
    expect(strip).toHaveTextContent("Running");
    expect(strip).not.toHaveTextContent("Mode:");
    expect(strip).not.toHaveTextContent("Created:");
  });

  it("keeps command metadata visible in header without workflow or run id", () => {
    render(<RunDetailPage />);
    const metadata = screen.getByTestId("run-header-meta");
    expect(metadata).toHaveTextContent("Created:");
    expect(metadata).toHaveTextContent("Last Activity:");
    expect(metadata).not.toHaveTextContent("Workflow:");
    expect(metadata).not.toHaveTextContent("run-1");
  });

  it("renders a space between timestamp labels and relative values", () => {
    render(<RunDetailPage />);
	    expect(screen.getByTestId("run-created-meta")).toHaveClass("gap-1");
	    expect(screen.getByTestId("run-last-activity-meta")).toHaveClass("gap-1");
  });

  it("lays out header timestamps in a compact row with stable value widths", () => {
    render(<RunDetailPage />);
    const metadataRow = screen.getByTestId("run-header-meta");
    expect(metadataRow).toHaveClass("flex", "min-w-0", "items-center", "gap-2");
    expect(metadataRow.className).toContain("xl:gap-3");
    expect(screen.getByTestId("run-created-meta-value")).toHaveClass("min-w-[7ch]");
    expect(screen.getByTestId("run-last-activity-meta-value")).toHaveClass("min-w-[7ch]");
  });

  it("renders status before cancel in the primary controls order", () => {
    render(<RunDetailPage />);
    const controls = screen.getByTestId("run-primary-controls");
    const text = controls.textContent ?? "";
    expect(text.indexOf("Running")).toBeLessThan(text.indexOf("Cancel Run"));
  });

  it("opens a themed cancel dialog without invoking browser confirm", async () => {
    const user = userEvent.setup();
    const confirmSpy = vi.spyOn(window, "confirm").mockImplementation(() => true);

    render(<RunDetailPage />);

    await user.click(screen.getByTestId("run-cancel-pill"));

    expect(screen.getByRole("dialog", { name: "Cancel run" })).toBeInTheDocument();
    expect(cancelMutateAsyncMock).not.toHaveBeenCalled();
    expect(confirmSpy).not.toHaveBeenCalled();

    confirmSpy.mockRestore();
  });

  it("submits cancellation only after confirming in the themed dialog", async () => {
    const user = userEvent.setup();

    render(<RunDetailPage />);

    await user.click(screen.getByTestId("run-cancel-pill"));
    const dialog = screen.getByRole("dialog", { name: "Cancel run" });

    await user.click(within(dialog).getByRole("button", { name: "Cancel Run" }));

    expect(cancelMutateAsyncMock).toHaveBeenCalledWith({
      body: { reason: "manual cancellation from run detail" },
      key: "idem-cancel-test",
    });
    expect(screen.queryByRole("dialog", { name: "Cancel run" })).not.toBeInTheDocument();
  });

  it("lets the operator dismiss the cancel dialog without mutating the run", async () => {
    const user = userEvent.setup();

    render(<RunDetailPage />);

    await user.click(screen.getByTestId("run-cancel-pill"));
    const dialog = screen.getByRole("dialog", { name: "Cancel run" });

    await user.click(within(dialog).getByRole("button", { name: "Keep Run Active" }));

    expect(cancelMutateAsyncMock).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog", { name: "Cancel run" })).not.toBeInTheDocument();
  });

  it("keeps header metadata in a separate right-side cluster", () => {
    render(<RunDetailPage />);
    const info = screen.getByTestId("run-header-meta");
    const divider = screen.getByTestId("run-header-divider");
    expect(divider).toHaveClass("h-6");
    expect(divider).toHaveClass("w-px");
    expect(info).toHaveTextContent("Created:");
    expect(info).toHaveTextContent("Last Activity:");
  });

  it("reserves scrollbar gutter space for the right sidebar cards", () => {
    render(<RunDetailPage />);
    const sidebar = screen.getByTestId("run-sidebar-scroll");
    expect(sidebar).toHaveClass("overflow-y-auto", "pr-1");
    expect(sidebar.className).toContain("[scrollbar-gutter:stable]");
  });

  it("keeps only action controls as pills and renders timestamps as plain metadata", () => {
    render(<RunDetailPage />);
    expect(screen.getByTestId("run-status-pill")).toHaveClass("h-7");
    expect(screen.getByTestId("run-cancel-pill")).toHaveClass("h-7");
    expect(screen.queryByTestId("run-mode-pill")).not.toBeInTheDocument();
    const created = screen.getByTestId("run-created-meta");
    const updated = screen.getByTestId("run-last-activity-meta");
    expect(created).not.toHaveClass("rounded-md");
    expect(updated).not.toHaveClass("rounded-md");
    expect(created).not.toHaveClass("border");
    expect(updated).not.toHaveClass("border");
    expect(created).not.toHaveClass("bg-surface-raised/30");
    expect(updated).not.toHaveClass("bg-surface-raised/30");
  });

  it("in unbounded mode, warning text appears after cancel", () => {
    mockRun.run_until_all_approve = true;
    render(<RunDetailPage />);
    const controls = screen.getByTestId("run-primary-controls");
    const text = controls.textContent ?? "";
    const notices = screen.getByTestId("run-header-notices");
    expect(notices).toHaveTextContent("Unbounded review/validation mode");
    expect(text.indexOf("Running")).toBeLessThan(text.indexOf("Cancel Run"));
    mockRun.run_until_all_approve = false;
  });

	it("does not surface execution recovery notices in the run header", () => {
		mockExecutions = [
			{
				attempt_id: "attempt-recovery-pending",
				node_run_id: "nr-recovery-pending",
				node_id: "delegated_execution_berserker",
				attempt: 2,
				agent_id: "agent-builder",
				required_role: "builder-berserker",
				execution_mode: "provider_backed",
				status: "verifying",
				activity_at: "2026-03-06T10:01:00.000Z",
				updated_at: "2026-03-06T10:01:00.000Z",
			},
			{
				attempt_id: "attempt-recovery-exhausted",
				node_run_id: "nr-recovery-exhausted",
				node_id: "delegated_execution_berserker",
				attempt: 3,
				agent_id: "agent-builder",
				required_role: "builder-berserker",
				execution_mode: "provider_backed",
				status: "failed",
				failure_code: "provider_incomplete_turn",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			},
		];

		render(<RunDetailPage />);

		expect(screen.queryByTestId("run-header-notices")).not.toBeInTheDocument();
		expect(screen.queryByText(/Recovery Pending:/)).not.toBeInTheDocument();
		expect(screen.queryByText(/Verification Expired:/)).not.toBeInTheDocument();
		expect(screen.queryByText(/Response Captured, Verifying:/)).not.toBeInTheDocument();
	});

  it("uses a managed non-scrolling tab panel for the DAG tab", () => {
    render(<RunDetailPage />);

    const panel = screen.getByRole("tabpanel");
    expect(panel).toHaveClass("overflow-hidden");
    expect(panel).toHaveClass("flex");
    expect(panel).not.toHaveClass("overflow-auto");
  });

  it("keeps the mailbox tab on the managed non-scrolling panel layout", () => {
    mockSearch.tab = "mailbox";

    render(<RunDetailPage />);

    const panel = screen.getByRole("tabpanel");
    expect(panel).toHaveClass("overflow-hidden");
    expect(panel).toHaveClass("flex");
    expect(panel).not.toHaveClass("overflow-auto");
  });

	it("lets the execution tab use the standard scrolling panel layout", () => {
	    mockSearch.tab = "execution";

	    render(<RunDetailPage />);

	    const panel = screen.getByRole("tabpanel");
	    expect(panel).toHaveClass("overflow-auto");
	    expect(panel).not.toHaveClass("overflow-hidden");
	  });

	it("shows auto-succeeded indicators only when triggered", () => {
	  const { rerender } = render(<RunDetailPage />);
	  expect(screen.queryByText(/Review Auto-Succeeded at Cap/)).not.toBeInTheDocument();
	  expect(screen.queryByText(/Validation Auto-Succeeded at Cap/)).not.toBeInTheDocument();

    mockRun.review_auto_succeeded_count = 1;
	  mockRun.validation_auto_succeeded_count = 2;
	  rerender(<RunDetailPage />);

	  expect(screen.getByText(/^Review Auto-Succeeded at Cap$/)).toBeInTheDocument();
	  expect(screen.getByText(/^Validation Auto-Succeeded at Cap$/)).toBeInTheDocument();
	  expect(screen.queryByText(/^1 Review Auto-Succeeded at Cap$/)).not.toBeInTheDocument();
	  expect(screen.queryByText(/^2 Validation Auto-Succeeded at Cap$/)).not.toBeInTheDocument();
	});

  it("shows cycle info helpers for review and validation caps", () => {
    render(<RunDetailPage />);
    expect(screen.getByLabelText("Review cycles info")).toBeInTheDocument();
    expect(screen.getByLabelText("Validation cycles info")).toBeInTheDocument();
  });

	it("uses aligned tooltip wording for rework and auto-succeeded counters", async () => {
		const user = userEvent.setup();
		mockRun.review_auto_succeeded_count = 1;
		mockRun.validation_auto_succeeded_count = 2;

		render(<RunDetailPage />);

		await user.hover(screen.getByLabelText("Review cycles info"));
		expect(screen.getByRole("tooltip")).toHaveTextContent("How many times review requested rework.");
		await user.unhover(screen.getByLabelText("Review cycles info"));

		await user.hover(screen.getByLabelText("Validation cycles info"));
		expect(screen.getByRole("tooltip")).toHaveTextContent("How many times validation requested rework.");
		await user.unhover(screen.getByLabelText("Validation cycles info"));

		await user.hover(screen.getByLabelText("Review auto-succeeded count"));
		expect(screen.getByRole("tooltip")).toHaveTextContent("Review auto-succeeded after the review cap was reached.");
		await user.unhover(screen.getByLabelText("Review auto-succeeded count"));

		await user.hover(screen.getByLabelText("Validation auto-succeeded count"));
		expect(screen.getByRole("tooltip")).toHaveTextContent("Validation auto-succeeded after the validation cap was reached.");
	});

  it("shows single review/validation cycle counters", () => {
    mockRun.review_rework_cycles = 2;
    mockRun.validation_rework_waves = 1;
    render(<RunDetailPage />);
    expect(screen.getByText(/Review Rework Requests:/)).toHaveTextContent("Review Rework Requests: 2");
    expect(screen.getByText(/Validation Rework Requests:/)).toHaveTextContent("Validation Rework Requests: 1");
  });

  it("shows skip-review mode and hides review metrics when quality gates are skipped", () => {
    mockRun.workflow_id = "royal-decree-skip-review-validation-v1";
    render(<RunDetailPage />);

    expect(screen.getByText("Review and validation stages were skipped for this decree.")).toBeInTheDocument();
    expect(screen.queryByText(/Review Rework Requests:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Validation Rework Requests:/)).not.toBeInTheDocument();

    mockRun.workflow_id = "royal-decree-v1";
  });

  it("shows runtime retry status with exact delay", () => {
    mockRun.runtime_retries = [{
      node_id: "delegated_execution_berserker",
      node_run_id: "nr_builder_1",
      retry_index: 2,
      retry_max: 3,
      delay_seconds: 10,
    }];
    render(<RunDetailPage />);
		expect(screen.getByTestId("runtime-retry-status")).toHaveTextContent("Runtime retrying empty provider output");
    expect(screen.getByTestId("runtime-retry-status")).toHaveTextContent("(2/3) after 10s");
  });

  it("hides runtime retry status once retry recovers", () => {
    render(<RunDetailPage />);
    expect(screen.queryByTestId("runtime-retry-status")).not.toBeInTheDocument();
  });

  it("shows runtime retry status on non-activity tabs without timeline data", () => {
    mockSearch.tab = "agents";
    mockRun.runtime_retries = [{
      node_id: "delegated_execution_berserker",
      node_run_id: "nr_builder_1",
      retry_index: 1,
      retry_max: 3,
      delay_seconds: 5,
    }];

    render(<RunDetailPage />);

    expect(useTimelineMock).toHaveBeenCalledWith("run-1", 50, { enabled: false });
    expect(screen.getByTestId("runtime-retry-status")).toHaveTextContent("(1/3) after 5s");
  });

	it("shows every active runtime retry when multiple nodes are retrying", () => {
		mockRun.runtime_retries = [
			{
				node_id: "delegated_execution_berserker",
				node_run_id: "nr_builder_1",
				retry_index: 1,
				retry_max: 3,
				delay_seconds: 5,
			},
			{
				node_id: "validation_gates_qa_scout",
				node_run_id: "nr_validator_1",
				retry_index: 2,
				retry_max: 4,
				delay_seconds: 8,
			},
		];

		render(<RunDetailPage />);

		expect(screen.getByTestId("runtime-retry-status")).toHaveTextContent("2 runtime retries in progress");
		expect(screen.getByTestId("runtime-retry-status-nr_builder_1")).toHaveTextContent("delegated_execution_berserker");
		expect(screen.getByTestId("runtime-retry-status-nr_validator_1")).toHaveTextContent("validation_gates_qa_scout");
	});
});
