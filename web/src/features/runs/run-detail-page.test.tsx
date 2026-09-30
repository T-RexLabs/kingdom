import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render as rtlRender, screen, within, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { RunDetailPage } from "./run-detail-page";
import type { ReactNode, Ref } from "react";
import { ApiError } from "@/api/client";
import type { RunExecution, RunExecutionDetail, RunNodeExecution } from "@/api/hooks";
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
const dagVisualizationPropsMock = vi.fn();
const useInfiniteRunNodeAttemptsMock = vi.fn();
const useRunExecutionMock = vi.fn();
const useTimelineMock = vi.fn();
const useRunAgentsMock = vi.fn();
const useRolesMock = vi.fn();
const useMailboxMock = vi.fn();
const useMailboxMessageMock = vi.fn();
const useCancelRunMock = vi.fn();
const useRetryExecutionStopMock = vi.fn();
const agentsRefetchMock = vi.fn();
const executionsRefetchMock = vi.fn();
const executionDetailRefetchMock = vi.fn();
const mailboxMessageRefetchMock = vi.fn();
const mockSearch: { tab?: string; panel?: string; section?: string } = { tab: "execution" };
const mockRouteParams = { runId: "run-1" };

const mockRun = {
  start_state: "not_started" as "observed" | "not_started" | "unrecorded",
  started_at: undefined as string | undefined,
  run_id: "run-1",
  work_order_id: "work order-1",
  work_order_title: "Balance Layout",
  workflow_id: "work-order-v1",
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
let mockExecutions: RunNodeExecution[] = [];
let mockMailboxPending = false;
let mockMailboxError: Error | null = null;
let mockExecutionsPending = false;
let mockExecutionsError: Error | null = null;
let mockCancelPending = false;
let mockCancelError: Error | null = null;
const mailboxRefetchMock = vi.fn();
const cancelMutateAsyncMock = vi.fn().mockResolvedValue(undefined);
const graphRefetchMock = vi.fn();

function runExecution(overrides: Partial<RunExecution> = {}): RunExecution {
  return {
    attempt_id: "attempt-1",
    node_run_id: "nr-1",
    node_id: "implementation",
    attempt: 1,
    agent_id: "agent-1",
    required_role: "builder",
    execution_mode: "provider_backed",
    status: "succeeded",
    queued_at: "2026-03-06T10:00:00.000Z",
    activity_at: "2026-03-06T10:01:00.000Z",
    updated_at: "2026-03-06T10:01:00.000Z",
    stop_retry_available: false,
    ...overrides,
  };
}

function runNode(current_execution: RunExecution, overrides: Partial<RunNodeExecution> = {}): RunNodeExecution {
  return {
    node_id: current_execution.node_id,
    required_role: current_execution.required_role,
    execution_ownership: current_execution.execution_mode,
    status: current_execution.status === "queued" ? "ready" : current_execution.status === "paused" ? "blocked" : current_execution.status === "verifying" ? "running" : current_execution.status as RunNodeExecution["status"],
    attempt_count: 1,
    current_execution,
    ...overrides,
  };
}

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
	DAGVisualization: (props: {
		nodes: RunNodeExecution[];
		onViewActivity?: (nodeId: string) => void;
	}) => {
		dagVisualizationPropsMock(props);
		return (
			<div>
				<div>graph</div>
				<button type="button" onClick={() => props.onViewActivity?.("implementation")}>
					View Delegated Execution Builder in Activity
				</button>
			</div>
		);
	},
}));

vi.mock("./run-execution-picker", () => ({ RunExecutionPicker: () => <div>Attempt Picker</div> }));
vi.mock("./run-output", () => ({
  RunOutput: () => <div>artifacts</div>,
  RunPublicationPanel: ({ picker }: { picker: ReactNode }) => <details><summary>Publication Results</summary>{picker}</details>,
}));

vi.mock("./run-live-status", () => ({ RunLiveStatus: ({ onSelect }: { onSelect: (nodeId: string) => void }) => <button onClick={() => onSelect("implementation")}>Open Live Builder Activity</button> }));

vi.mock("./timeline-panel", () => ({ TimelinePanel: (props: unknown) => { timelinePanelPropsMock(props); return <div>timeline</div>; } }));


vi.mock("./run-execution-surface", () => ({
	RunExecutionSurface: (props: {
		events: Array<{ event_id: string }>;
		graph?: unknown;
		nodes: RunNodeExecution[];
		executionsErrorMessage?: string | null;
		showPrimaryExecutionError?: boolean;
		isSummaryLoading?: boolean;
		selectedNodeId?: string | null;
		selectedAttemptId?: string | null;
		onSelectStep?: (nodeId: string) => void;
		onSelectAttempt?: (attemptId: string) => void;
		onToggleHistory?: () => void;
		detail?: RunExecutionDetail | null;
		onRetryExecutions?: () => void;
		hasMoreEvents: boolean;
		onLoadMoreEvents?: () => void;
		isLoadingMoreEvents?: boolean;
	}) => {
		executionSurfacePropsMock(props);
		return (
			<>
				<div>executions</div>
			</>
		);
	},
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

vi.mock("./work-order-panel", () => ({
  WorkOrderPanel: () => <div>work order</div>,
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
  useInfiniteRunNodeAttempts: (...args: unknown[]) => useInfiniteRunNodeAttemptsMock(...args),
  useRunExecution: (...args: unknown[]) => useRunExecutionMock(...args),
  useTimeline: (...args: unknown[]) => useTimelineMock(...args),
  useRunAgents: (...args: unknown[]) => useRunAgentsMock(...args),
  useRoles: (...args: unknown[]) => useRolesMock(...args),
  useMailbox: (...args: unknown[]) => useMailboxMock(...args),
  useMailboxMessage: (...args: unknown[]) => useMailboxMessageMock(...args),
  useCancelRun: (...args: unknown[]) => useCancelRunMock(...args),
	useRetryExecutionStop: (...args: unknown[]) => useRetryExecutionStopMock(...args),
  newIdempotencyKey: () => "idem-cancel-test",
}));

describe("run detail page", () => {
  beforeEach(() => {
    useRunEventStreamMock.mockReset();
    useRunEventStreamMock.mockReturnValue(undefined);
		dagVisualizationPropsMock.mockReset();
    timelinePanelPropsMock.mockReset();
		executionSurfacePropsMock.mockReset();
    governanceActionsPropsMock.mockReset();
    currentPlanPanelPropsMock.mockReset();
    fetchNextPageMock.mockReset();
    timelineRefetchMock.mockReset();
    navigateMock.mockReset();
    navigateMock.mockImplementation(({ search }: { search: typeof mockSearch }) => { Object.keys(mockSearch).forEach((key) => delete mockSearch[key as keyof typeof mockSearch]); Object.assign(mockSearch, search); });
    delete mockSearch.panel; delete mockSearch.section;
    mockRouteParams.runId = "run-1";
    mockSearch.tab = "execution";
    mockRun.start_state = "not_started";
    mockRun.started_at = undefined;
    mockRun.status = "running";
    mockRun.display_state = "running";
    mockRun.plan_state = "approved";
    mockRun.autonomous_approvals = false;
    mockRun.run_until_all_approve = false;
    mockRun.review_rework_cycles = 0;
    mockRun.review_auto_succeeded_count = 0;
    mockRun.validation_rework_waves = 0;
    mockRun.validation_auto_succeeded_count = 0;
    mockRun.tab_counts.dag_nodes = 6;
	    mockRun.tab_counts.agents = 3;
	    mockRun.tab_counts.activity_events = 50;
	    mockRun.tab_counts.executions = 1;
	    mockRun.tab_counts.messages = 2;
    mockRun.failed_nodes = [];
    mockTimelineItems.length = 0;
    mockExecutions = [runNode(runExecution())];
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
    useInfiniteRunNodeAttemptsMock.mockReset();
    useInfiniteRunNodeAttemptsMock.mockImplementation(() => ({
      data: { pages: [{ items: mockExecutions.flatMap((node) => node.current_execution ? [{ ...node.current_execution, superseded: false, retry_relationships: [] }] : []) }] },
      isSuccess: true,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    }));
    useRunExecutionMock.mockReset();
    executionDetailRefetchMock.mockReset();
		useRunExecutionMock.mockImplementation((_runId: string, attemptId: string | null, options: { enabled?: boolean }) => {
			const execution = mockExecutions[0]?.current_execution;
			const data = options.enabled && attemptId === execution?.attempt_id
				? {
					...execution,
					provider_invocation_id: "inv-1",
					settlement_status: "succeeded",
					output_status: "settled",
					output_text: "VERDICT: PASS\n\nCaptured output.",
					output_verdict: "PASS",
					output_verdict_status: "settled",
				} satisfies RunExecutionDetail
				: undefined;
			return {
				data,
				isPending: false,
				isFetching: false,
				isError: false,
				isSuccess: data !== undefined,
				error: null,
				refetch: executionDetailRefetchMock,
			};
		});
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
		useRetryExecutionStopMock.mockReset();
		useRetryExecutionStopMock.mockImplementation(() => ({
			mutateAsync: vi.fn().mockResolvedValue(undefined),
			isPending: false,
			error: null,
		}));
    mailboxRefetchMock.mockReset();
    cancelMutateAsyncMock.mockClear();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

	it("subscribes the run-detail page to the canonical run SSE hook", () => {
	  render(<RunDetailPage />);
	  expect(useRunEventStreamMock).toHaveBeenCalledWith("run-1", true, true, ["timeline"]);
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
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "nodes"], exact: false, refetchType: "active" });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "graph"], exact: true, refetchType: "active" });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "timeline"], exact: false, refetchType: "active" });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "agents"], exact: true, refetchType: "active" });
		expect(invalidateSpy).not.toHaveBeenCalledWith({ queryKey: ["runs", "run-1", "mailbox"], exact: false, refetchType: "active" });
		expect(queryClient.getQueryData(runSurfaceFreshnessQueryKey("run-1"))).toEqual({ dirtySurfaces: ["timeline"] });
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
		invalidateSpy.mockRestore();
	});

	it("keeps Audit lazy while observing the graph and selected attempt", () => {
	  render(<RunDetailPage />);

	  expect(useRunExecutionsMock).toHaveBeenCalledWith("run-1", { enabled: true });
	  expect(useInfiniteRunNodeAttemptsMock).toHaveBeenCalledWith("run-1", "implementation", { enabled: false });
	  expect(useRunExecutionMock).toHaveBeenCalledWith("run-1", "attempt-1", { enabled: true });
	  expect(useRunGraphMock).toHaveBeenCalledWith("run-1", { enabled: true });
	  expect(useTimelineMock).toHaveBeenCalledWith("run-1", 50, { enabled: false });
	  expect(useRunAgentsMock).not.toHaveBeenCalled();
	  expect(useMailboxMock).not.toHaveBeenCalled();
	  expect(useMailboxMessageMock).not.toHaveBeenCalled();
	});

	it("supplies logical nodes to the graph fallback and opens its node in Activity", async () => {
		mockSearch.tab = "workflow";
		const user = userEvent.setup();
		navigateMock.mockImplementation(({ search }: { search: { tab: string } }) => {
			mockSearch.tab = search.tab;
		});
		const { rerender } = render(<RunDetailPage />);

		expect(dagVisualizationPropsMock).toHaveBeenLastCalledWith(
			expect.objectContaining({ nodes: mockExecutions }),
		);

		await user.click(screen.getByRole("button", { name: "View Delegated Execution Builder in Activity" }));
		expect(navigateMock).toHaveBeenCalledWith({
			to: "/runs/$runId",
			params: { runId: "run-1" },
			search: { tab: "execution" },
			replace: true,
		});

		rerender(<RunDetailPage />);
		const activityProps = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId: string | null;
		};
		expect(activityProps.selectedNodeId).toBe("implementation");
		expect(screen.queryByText("graph")).not.toBeInTheDocument();
	});

  it("keeps Run Details limited to work order settings and Audit without roster or messages", () => {
    mockSearch.panel = "details"; mockSearch.section = "context";
    render(<RunDetailPage />);
    expect(screen.getByRole("button", { name: "Work Order and Settings" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Audit" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "Messages" })).not.toBeInTheDocument();
    expect(useRunAgentsMock).not.toHaveBeenCalled();
    expect(useMailboxMock).not.toHaveBeenCalled();
    expect(screen.queryByText("Attempt Record")).not.toBeInTheDocument();
  });

	it("fetches graph context when the activity tab is opened directly", () => {
		mockSearch.tab = "execution";

		render(<RunDetailPage />);

		expect(useRunGraphMock).toHaveBeenCalledWith("run-1", { enabled: true });
		expect(useRunExecutionsMock).toHaveBeenCalledWith("run-1", { enabled: true });
		expect(useInfiniteRunNodeAttemptsMock).toHaveBeenCalledWith("run-1", "implementation", { enabled: false });
		expect(useTimelineMock).toHaveBeenCalledWith("run-1", 50, { enabled: false });
	});

  it("loads scoped attempt history when the selected node has multiple attempts", () => {
    mockExecutions[0].attempt_count = 2;
    render(<RunDetailPage />);
    expect(useInfiniteRunNodeAttemptsMock).toHaveBeenLastCalledWith("run-1", "implementation", { enabled: true });
  });

  it("uses failed-node summaries while still fetching graph context for activity", () => {
		mockRun.status = "failed";
		mockRun.display_state = "failed";
		mockSearch.tab = "execution";
    mockRun.failed_nodes = [
      {
        node_id: "implementation",
        required_role: "builder",
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

  it("loads graph nodes for cancelled-run replay controls", () => {
    const cancelledNode = {
      node_id: "implementation",
      required_role: "builder",
      execution_ownership: "provider_backed",
      state: "cancelled",
    };
    mockRun.status = "cancelled";
    mockRun.display_state = "cancelled";
    mockSearch.panel = "details"; mockSearch.section = "context";
    useRunGraphMock.mockImplementation(() => ({
      data: { run_id: "run-1", nodes: [cancelledNode], edges: [] },
      isPending: false,
      isError: false,
      error: null,
      refetch: graphRefetchMock,
    }));

    render(<RunDetailPage />);

    expect(useRunGraphMock).toHaveBeenCalledWith("run-1", { enabled: true });
    expect(governanceActionsPropsMock).toHaveBeenCalledWith(
      expect.objectContaining({
        cancelledNodes: [cancelledNode],
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

    expect(screen.queryByText("timeline")).not.toBeInTheDocument();
    expect(screen.getByText("executions")).toBeInTheDocument();
  });

	it("passes graph data into the activity timeline when available", () => {
		mockSearch.panel = "details"; mockSearch.section = "audit";
		useRunGraphMock.mockImplementation(() => ({
			data: {
				run_id: "run-1",
				nodes: [{ node_id: "plan_drafting", required_role: "plan-author", execution_ownership: "provider_backed", state: "succeeded" }],
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
		expect(lastCall.graph?.nodes).toEqual([{ node_id: "plan_drafting", required_role: "plan-author", execution_ownership: "provider_backed", state: "succeeded" }]);
		expect(lastCall.graph?.edges).toEqual([{ from: "advisory_synthesis", to: "plan_drafting", on: "success" }]);
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
    mockSearch.panel = "details"; mockSearch.section = "audit";
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
    await user.click(within(screen.getByRole("dialog", { name: "Run Details" })).getByRole("button", { name: "Retry" }));
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
		mockSearch.panel = "details"; mockSearch.section = "audit";
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
			runNode(runExecution({
				attempt_id: "attempt-2",
				node_run_id: "nr-2",
				node_id: "execution_review",
				attempt: 1,
				agent_id: "agent-2",
				required_role: "reviewer",
				execution_mode: "provider_backed",
				status: "running",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			})),
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
		expect(screen.queryByText("timeline")).not.toBeInTheDocument();
		expect(screen.queryByText("Timeline unavailable")).not.toBeInTheDocument();
		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			nodes: Array<{ current_execution?: { attempt_id: string } }>;
		};
		expect(lastCall.nodes).toEqual([
			expect.objectContaining({ current_execution: expect.objectContaining({ attempt_id: "attempt-2" }) }),
		]);
	});

  it("loads bounded prior history when the current node generation has no physical attempt", () => {
    mockExecutions = [{ node_id: "quality_validation", required_role: "quality-validator", execution_ownership: "provider_backed", status: "pending", attempt_count: 1 }];
    render(<RunDetailPage />);
    expect(useInfiniteRunNodeAttemptsMock).toHaveBeenCalledWith("run-1", "quality_validation", { enabled: true });
    expect(executionSurfacePropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ selectedNodeId: "quality_validation", selectedAttemptId: null }));
  });

  it("keeps a recorded conversation visible when active agents settle at the plan gate", () => {
    const future: RunNodeExecution = { node_id: "implementation", required_role: "builder", execution_ownership: "provider_backed", status: "pending", attempt_count: 0 };
    const advisor = runExecution({ node_id: "risk_advisory", attempt_id: "advisor-attempt", required_role: "risk-advisor", status: "running", activity_at: "2026-09-30T10:05:00Z" });
    mockExecutions = [future, runNode(advisor)];
    const view = render(<RunDetailPage />);
    expect(executionSurfacePropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ selectedNodeId: "risk_advisory", selectedAttemptId: "advisor-attempt" }));
    mockExecutions = [future, runNode({ ...advisor, status: "succeeded" }, { status: "succeeded" })];
    mockRun.display_state = "awaiting_plan_decision";
    mockRun.plan_state = "awaiting_plan_decision";
    view.rerender(<RunDetailPage />);
    expect(executionSurfacePropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ selectedNodeId: "risk_advisory", selectedAttemptId: "advisor-attempt" }));
  });

	it("defaults to the first observed active provider node before settled steps", () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			runNode(runExecution({
				attempt_id: "attempt-succeeded",
				node_run_id: "nr-succeeded",
				node_id: "implementation",
				attempt: 1,
				agent_id: "agent-1",
				required_role: "builder",
				execution_mode: "provider_backed",
				status: "succeeded",
				activity_at: "2026-03-06T10:03:00.000Z",
				updated_at: "2026-03-06T10:03:00.000Z",
			})),
			runNode(runExecution({
				attempt_id: "attempt-running",
				node_run_id: "nr-running",
				node_id: "execution_review",
				attempt: 2,
				agent_id: "agent-2",
				required_role: "reviewer",
				execution_mode: "provider_backed",
				status: "running",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			})),
			runNode(runExecution({
				attempt_id: "attempt-failed",
				node_run_id: "nr-failed",
				node_id: "quality_validation",
				attempt: 3,
				agent_id: "agent-3",
				required_role: "qa-validator",
				execution_mode: "provider_backed",
				status: "failed",
				activity_at: "2026-03-06T10:04:00.000Z",
				updated_at: "2026-03-06T10:04:00.000Z",
			})),
		];

		render(<RunDetailPage />);

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedAttemptId?: string | null;
		};
		expect(lastCall.selectedAttemptId).toBe("attempt-running");
	});

	it("defaults Activity to the first server-provided active node", () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			runNode(runExecution({
				attempt_id: "attempt-running",
				node_run_id: "nr-running",
				node_id: "execution_review",
				required_role: "reviewer",
				status: "running",
			})),
			runNode(runExecution({
				attempt_id: "attempt-succeeded",
				node_run_id: "nr-succeeded",
				node_id: "implementation",
				status: "succeeded",
				activity_at: "2026-03-06T10:04:00.000Z",
				updated_at: "2026-03-06T10:04:00.000Z",
			})),
		];

		render(<RunDetailPage />);

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId?: string | null;
			selectedAttemptId?: string | null;
		};
		expect(lastCall.selectedNodeId).toBe("execution_review");
		expect(lastCall.selectedAttemptId).toBe("attempt-running");
	});

	it("shows the latest recorded conversation when no work is in flight", () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			runNode(runExecution({
				attempt_id: "attempt-failed",
				node_run_id: "nr-failed",
				node_id: "execution_review",
				attempt: 1,
				agent_id: "agent-1",
				required_role: "reviewer",
				execution_mode: "provider_backed",
				status: "failed",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			})),
			runNode(runExecution({
				attempt_id: "attempt-succeeded",
				node_run_id: "nr-succeeded",
				node_id: "quality_validation",
				attempt: 2,
				agent_id: "agent-2",
				required_role: "qa-validator",
				execution_mode: "provider_backed",
				status: "succeeded",
				activity_at: "2026-03-06T10:04:00.000Z",
				updated_at: "2026-03-06T10:04:00.000Z",
			})),
		];

		render(<RunDetailPage />);

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedAttemptId?: string | null;
			selectedNodeId?: string | null;
		};
		expect(lastCall.selectedNodeId).toBe("quality_validation");
		expect(lastCall.selectedAttemptId).toBe("attempt-succeeded");
	});

	it("preserves a manual execution selection after the operator changes focus", async () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			runNode(runExecution({
				attempt_id: "attempt-running",
				node_run_id: "nr-running",
				node_id: "execution_review",
				attempt: 2,
				agent_id: "agent-2",
				required_role: "reviewer",
				execution_mode: "provider_backed",
				status: "running",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			})),
			runNode(runExecution({
				attempt_id: "attempt-failed",
				node_run_id: "nr-failed",
				node_id: "quality_validation",
				attempt: 3,
				agent_id: "agent-3",
				required_role: "qa-validator",
				execution_mode: "provider_backed",
				status: "failed",
				activity_at: "2026-03-06T10:04:00.000Z",
				updated_at: "2026-03-06T10:04:00.000Z",
			})),
		];

		const { rerender } = render(<RunDetailPage />);
		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			onSelectStep?: (nodeId: string) => void;
		};

		await act(async () => {
			lastCall.onSelectStep?.("quality_validation");
		});
		await act(async () => {
			rerender(<RunDetailPage />);
		});

		const updatedCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId?: string | null;
			selectedAttemptId?: string | null;
		};
		expect(updatedCall.selectedNodeId).toBe("quality_validation");
		expect(updatedCall.selectedAttemptId).toBe("attempt-failed");
	});


  it("keeps exact attempt evidence within Audit and pins that attempt across retries", async () => {
    mockSearch.tab = "workflow";
    const original = runExecution({
      attempt_id: "attempt-answer",
      node_run_id: "nr-answer",
    });
    mockExecutions = [runNode(original)];
    const { rerender } = render(<RunDetailPage />);
    expect(useRunExecutionMock.mock.calls.at(-1)?.[2]).toEqual({
      enabled: false,
    });
    await userEvent.click(screen.getByRole("button", { name: "Run Details" }));
    rerender(<RunDetailPage />);
    const modal = screen.getByRole("dialog", { name: "Run Details" });
    expect(
      within(modal).queryByRole("button", { name: "Selected Attempt" }),
    ).not.toBeInTheDocument();
    expect(within(modal).queryByText("Attempt Picker")).not.toBeInTheDocument();
    expect(useRunExecutionMock.mock.calls.at(-1)?.[2]).toEqual({
      enabled: false,
    });
    await userEvent.click(within(modal).getByRole("button", { name: "Audit" }));
    rerender(<RunDetailPage />);
    await userEvent.click(within(modal).getByText("Attempt Record"));
    await waitFor(() =>
      expect(within(modal).getByText("attempt-answer")).toBeVisible(),
    );
    expect(within(modal).getByText("inv-1")).toBeVisible();
    expect(
      within(modal).queryByText(/Captured output/),
    ).not.toBeInTheDocument();
    expect(
      within(modal).queryByText("Settled Response"),
    ).not.toBeInTheDocument();
    mockExecutions = [
      runNode(
        {
          ...original,
          attempt_id: "attempt-next",
          node_run_id: "nr-next",
          attempt: 2,
          status: "running",
        },
        { attempt_count: 2 },
      ),
    ];
    rerender(<RunDetailPage />);
    expect(useRunExecutionMock.mock.calls.at(-1)?.[1]).toBe("attempt-answer");
    expect(useRunExecutionMock.mock.calls.at(-1)?.[2]).toEqual({
      enabled: true,
    });
    await userEvent.click(within(modal).getByText("Attempt Record"));
    await waitFor(() =>
      expect(useRunExecutionMock.mock.calls.at(-1)?.[2]).toEqual({
        enabled: false,
      }),
    );
  });

  it("pins the first current attempt without history and keeps it when workflow activity advances", async () => {
    const coordinator = runExecution({
      node_id: "advisory_synthesis",
      attempt_id: "coordinator:1",
      required_role: "coordinator",
      status: "running",
    });
    const builder = runExecution({
      node_id: "implementation",
      attempt_id: "builder:1",
      required_role: "builder",
      status: "queued",
    });
    mockExecutions = [runNode(coordinator), runNode(builder)];
    useInfiniteRunNodeAttemptsMock.mockReturnValue({
      data: undefined,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    });
    const view = render(<RunDetailPage />);
    expect(useInfiniteRunNodeAttemptsMock).toHaveBeenLastCalledWith(
      "run-1",
      "advisory_synthesis",
      { enabled: false },
    );
    const activity = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
      onSelectAttempt: (attemptId: string) => void;
    };
    await act(async () => activity.onSelectAttempt("coordinator:1"));
    mockExecutions = [
      runNode({ ...coordinator, status: "succeeded" }),
      runNode({ ...builder, status: "running" }),
    ];
    view.rerender(<RunDetailPage />);
    expect(executionSurfacePropsMock.mock.calls.at(-1)?.[0]).toMatchObject({
      selectedNodeId: "advisory_synthesis",
      selectedAttemptId: "coordinator:1",
    });
    await userEvent.click(screen.getByRole("button", { name: "Run Details" }));
    view.rerender(<RunDetailPage />);
    expect(executionSurfacePropsMock.mock.calls.at(-1)?.[0]).toMatchObject({
      selectedNodeId: "advisory_synthesis",
      selectedAttemptId: "coordinator:1",
    });
  });

  it("rejects an attempt that belongs to neither the selected current node nor loaded history", async () => {
    useInfiniteRunNodeAttemptsMock.mockReturnValue({
      data: undefined,
      hasNextPage: false,
      isFetchingNextPage: false,
      fetchNextPage: vi.fn(),
    });
    render(<RunDetailPage />);
    const activity = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
      onSelectAttempt: (attemptId: string) => void;
    };
    await act(async () => activity.onSelectAttempt("unrelated-attempt"));
    expect(
      executionSurfacePropsMock.mock.calls.at(-1)?.[0].selectedAttemptId,
    ).toBe("attempt-1");
  });

	it("keeps a selected historical attempt through canonical and SSE refreshes", async () => {
		mockSearch.tab = "execution";
		mockExecutions = [runNode(runExecution({
				attempt_id: "attempt-current", node_run_id: "nr-current", node_id: "implementation", attempt: 2,
				agent_id: "agent-1", required_role: "builder", execution_mode: "provider_backed", status: "running",
				queued_at: "2026-03-06T10:00:00.000Z", activity_at: "2026-03-06T10:02:00.000Z", updated_at: "2026-03-06T10:02:00.000Z", stop_retry_available: false,
			}), { status: "running", attempt_count: 2 })];
		const historicalAttempt = runExecution({
			attempt_id: "attempt-history", node_run_id: "nr-history", node_id: "implementation", attempt: 1,
			agent_id: "agent-1", required_role: "builder", execution_mode: "provider_backed", status: "failed",
			queued_at: "2026-03-06T09:00:00.000Z", activity_at: "2026-03-06T09:02:00.000Z", updated_at: "2026-03-06T09:02:00.000Z", stop_retry_available: false,
		});
		useInfiniteRunNodeAttemptsMock.mockImplementation(() => ({
			data: { pages: [{ items: [mockExecutions[0].current_execution, historicalAttempt] }] },
			isSuccess: true,
			hasNextPage: true,
			isFetchingNextPage: false,
			fetchNextPage: fetchNextPageMock,
		}));
		useRunExecutionMock.mockImplementation((_runId: string, attemptId: string | null, options: { enabled?: boolean }) => ({
			data: options.enabled && attemptId === historicalAttempt.attempt_id
				? { ...historicalAttempt, settlement_status: "failed", output_status: "unavailable" }
				: undefined,
			isPending: false, isFetching: false, isError: false, error: null, refetch: executionDetailRefetchMock,
		}));

		const { queryClient, rerender } = render(<RunDetailPage />);
		const initial = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as { onSelectAttempt: (attemptId: string) => void };
		await act(async () => initial.onSelectAttempt(historicalAttempt.attempt_id));
		await act(async () => rerender(<RunDetailPage />));
		useInfiniteRunNodeAttemptsMock.mockImplementation(() => ({
			data: { pages: [{ items: [mockExecutions[0].current_execution] }] },
			isSuccess: true, hasNextPage: true, isFetchingNextPage: false, fetchNextPage: fetchNextPageMock,
		}));
		useRunExecutionMock.mockImplementation(() => ({
			data: { ...historicalAttempt, settlement_status: "failed", output_status: "unavailable" },
			isPending: false, isFetching: false, isError: false, error: null, refetch: executionDetailRefetchMock,
		}));
		await act(async () => rerender(<RunDetailPage />));
		await act(async () => {
			queryClient.setQueryData(liveExecutionProjectionKey("run-1"), {
				"nr-current": { node_run_id: "nr-current", attempt_id: "attempt-current", activity_at: "2026-03-06T10:03:00.000Z", status: "running" },
			});
			rerender(<RunDetailPage />);
		});

		const refreshed = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as { selectedAttemptId: string | null };
		expect(refreshed.selectedAttemptId).toBe(historicalAttempt.attempt_id);
	});

	it("clears a manual selection when its logical node disappears and defaults to the next server node", async () => {
		mockSearch.tab = "execution";
		mockExecutions = [
			runNode(runExecution({
				attempt_id: "attempt-review",
				node_run_id: "nr-review",
				node_id: "execution_review",
				attempt: 1,
				agent_id: "agent-review",
				required_role: "reviewer",
				execution_mode: "provider_backed",
				status: "running",
				queued_at: "2026-03-06T10:00:00.000Z",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			})),
			runNode(runExecution({
				attempt_id: "attempt-peasant",
				node_run_id: "nr-peasant",
				node_id: "usability_consultation",
				attempt: 1,
				agent_id: "agent-peasant",
				required_role: "usability-advisor",
				execution_mode: "provider_backed",
				status: "succeeded",
				queued_at: "2026-03-06T10:00:00.000Z",
				activity_at: "2026-03-06T10:01:00.000Z",
				updated_at: "2026-03-06T10:01:00.000Z",
			})),
		];

		const { rerender } = render(<RunDetailPage />);
		const firstCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			onSelectStep?: (nodeId: string) => void;
		};

		await act(async () => {
			firstCall.onSelectStep?.("usability_consultation");
		});

		mockExecutions = [];
		await act(async () => {
			rerender(<RunDetailPage />);
		});

		let updatedCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId?: string | null;
			selectedAttemptId?: string | null;
		};
		expect(updatedCall.selectedNodeId).toBeNull();
		expect(updatedCall.selectedAttemptId).toBeNull();

		mockExecutions = [
			runNode(runExecution({
				attempt_id: "attempt-plan",
				node_run_id: "nr-plan",
				node_id: "plan_drafting",
				attempt: 2,
				agent_id: "agent-plan",
				required_role: "plan-author",
				execution_mode: "provider_backed",
				status: "running",
				queued_at: "2026-03-06T10:03:00.000Z",
				activity_at: "2026-03-06T10:05:00.000Z",
				updated_at: "2026-03-06T10:05:00.000Z",
			})),
		];
		await act(async () => {
			rerender(<RunDetailPage />);
		});

		updatedCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedNodeId?: string | null;
			selectedAttemptId?: string | null;
		};
		expect(updatedCall.selectedNodeId).toBe("plan_drafting");
		expect(updatedCall.selectedAttemptId).toBe("attempt-plan");
	});

	it("never passes a detail for a different effective attempt", () => {
		mockSearch.tab = "execution";
		const mismatchedDetail = {
			...runExecution({ attempt_id: "attempt-stale", node_run_id: "nr-stale" }),
			settlement_status: "succeeded" as const,
			output_status: "settled" as const,
		};
		useRunExecutionMock.mockImplementation(() => ({
			data: mismatchedDetail,
			isPending: false,
			isFetching: false,
			isError: false,
			isSuccess: true,
			error: null,
			refetch: executionDetailRefetchMock,
		}));

		render(<RunDetailPage />);

		const lastCall = executionSurfacePropsMock.mock.calls.at(-1)?.[0] as {
			selectedAttemptId?: string | null;
			detail?: RunExecutionDetail | null;
		};
		expect(lastCall.selectedAttemptId).toBe("attempt-1");
		expect(lastCall.detail).toBeNull();
	});

	it("does not synthesize logical nodes from live execution telemetry", async () => {
		mockSearch.tab = "execution";
		mockExecutions = [];
		mockRun.tab_counts.executions = 0;
		useRunExecutionMock.mockImplementation((runId: string, attemptId: string | null, options: { enabled?: boolean }) => ({
			data: options.enabled && attemptId
				? {
					...runExecution(),
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
					node_id: "implementation",
					attempt: 2,
					required_role: "builder",
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
			nodes?: Array<{ node_id: string }>;
			selectedAttemptId?: string | null;
			detail?: RunExecutionDetail | null;
		};
		expect(lastCall.nodes).toEqual([]);
		expect(lastCall.selectedAttemptId).toBeNull();
		expect(lastCall.detail).toBeNull();

		const latestExecutionDetailCall = useRunExecutionMock.mock.calls.at(-1);
		expect(latestExecutionDetailCall).toEqual(["run-1", null, { enabled: false }]);
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

	  mockSearch.panel = "details"; mockSearch.section = "audit";
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

	  mockSearch.panel = "details"; mockSearch.section = "audit";
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
	  mockSearch.panel = "details"; mockSearch.section = "audit";
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
	  mockSearch.tab = "workflow";
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
	    expect(useRunEventStreamMock).toHaveBeenCalledWith("run-1", false, true, ["timeline"]);
  });

	it("transitions from pending to loaded without violating hook order", () => {
		mockRunPending = true;

		const { rerender } = render(<RunDetailPage />);

		expect(screen.getByText("Loading run...")).toBeInTheDocument();

		mockRunPending = false;
		mockRun.plan_state = "awaiting_plan_decision";
		mockRun.display_state = "awaiting_plan_decision";
		mockRun.autonomous_approvals = false;

		rerender(<RunDetailPage />);

		expect(screen.getByTestId("run-governance-notice")).toHaveTextContent("Awaiting Your Plan Approval");
		expect(screen.getByRole("button", { name: "Review Plan" })).toBeInTheDocument();

		mockRun.plan_state = "approved";
		mockRun.display_state = "running";
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
					? [runNode(runExecution({ ...mockExecutions[0]?.current_execution, attempt_id: "attempt-run-1" }))]
					: [runNode(runExecution({ ...mockExecutions[0]?.current_execution, attempt_id: "attempt-run-2", node_run_id: "nr-2" }))],
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
					...runExecution({ ...mockExecutions[0]?.current_execution }),
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
		expect(run2Calls).toContainEqual(["run-2", "attempt-run-2", { enabled: true }]);
	});

  it("renders semantic tabs for run detail navigation", () => {
    render(<RunDetailPage />);

	  expect(screen.getByRole("tablist", { name: "Run detail sections" })).toBeInTheDocument();
	  expect(screen.getByRole("tab", { name: /^Agent Activity$/i })).toHaveAttribute("aria-selected", "true");
	  expect(screen.queryByRole("combobox", { name: "More run views" })).not.toBeInTheDocument();
	});

  it("shows an explicit plan approval notice when the run awaits manual plan approval", () => {
    mockRun.plan_state = "awaiting_plan_decision";
    mockRun.display_state = "awaiting_plan_decision";
    mockRun.autonomous_approvals = false;

    render(<RunDetailPage />);

    const notice = screen.getByTestId("run-governance-notice");
    expect(notice).toHaveTextContent("Awaiting Your Plan Approval");
    expect(notice).toHaveTextContent("Execution is paused until you confirm this plan or request rework.");

    mockRun.plan_state = "approved";
    mockRun.display_state = "running";
  });

  it("suppresses manual-governance notices for autonomous runs in awaiting_plan_decision", () => {
    mockRun.plan_state = "awaiting_plan_decision";
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
    expect(notice).toHaveTextContent("Awaiting Your Final Approval");
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

  it("keeps final approval after the current plan in the Plan tab", () => {
    mockRun.status = "awaiting_human_confirmation";
    mockRun.display_state = "awaiting_final_decision";
    mockRun.autonomous_approvals = false;

    mockSearch.panel = "plan";
    render(<RunDetailPage />);

    const plan = screen.getByText("plan");
    const governance = screen.getByText("governance");
    expect(plan.compareDocumentPosition(governance) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    mockRun.status = "running";
    mockRun.display_state = "running";
  });

  it("replaces the old title-row governance pill with a banner jump action for plan approval", () => {
    mockRun.plan_state = "awaiting_plan_decision";
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
    mockRun.plan_state = "awaiting_plan_decision";
    mockRun.display_state = "awaiting_plan_decision";
    mockRun.autonomous_approvals = false;

    const { rerender } = render(<RunDetailPage />);

    await user.click(screen.getByRole("button", { name: "Review Plan" }));

    rerender(<RunDetailPage />);
    const target = screen.getByTestId("run-plan-approval-target");
    expect(navigateMock).toHaveBeenCalledWith(expect.objectContaining({ search: { tab: "execution", panel: "plan" } }));
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
    mockSearch.tab = "workflow";

    const { rerender } = render(<RunDetailPage />);

    await user.click(screen.getByRole("button", { name: "Review Final Decision" }));

    rerender(<RunDetailPage />);
    const target = screen.getByTestId("run-final-approval-target");
    expect(navigateMock).toHaveBeenCalledWith(expect.objectContaining({ search: { tab: "workflow", panel: "plan" } }));
    expect(screen.getByRole("dialog", { name: "Plan Review" })).toContainElement(target);
    expect(scrollIntoView).toHaveBeenCalledWith({ behavior: "smooth", block: "nearest", inline: "nearest" });
    expect(target).toHaveFocus();

    mockRun.status = "running";
    mockRun.display_state = "running";
  });

  it("wires timeline pagination into the activity panel", () => {
    mockSearch.panel = "details"; mockSearch.section = "audit";
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

  it("renders status before cancel in the primary controls order", () => {
    render(<RunDetailPage />);
    const controls = screen.getByTestId("run-primary-controls");
    const text = controls.textContent ?? "";
    expect(text.indexOf("Active")).toBeLessThan(text.indexOf("Cancel Run"));
  });

  it("opens a themed cancel dialog without invoking browser confirm", async () => {
    const user = userEvent.setup();
    const confirmSpy = vi.spyOn(window, "confirm").mockImplementation(() => true);

    render(<RunDetailPage />);

    await user.click(screen.getByTestId("run-cancel-pill"));

    expect(screen.getByRole("dialog", { name: "Balance Layout" })).toBeInTheDocument();
    expect(cancelMutateAsyncMock).not.toHaveBeenCalled();
    expect(confirmSpy).not.toHaveBeenCalled();

    confirmSpy.mockRestore();
  });

  it("submits cancellation only after confirming in the themed dialog", async () => {
    const user = userEvent.setup();

    render(<RunDetailPage />);

    await user.click(screen.getByTestId("run-cancel-pill"));
    const dialog = screen.getByRole("dialog", { name: "Balance Layout" });

    await user.click(within(dialog).getByRole("button", { name: "Cancel Run" }));

    expect(cancelMutateAsyncMock).toHaveBeenCalledWith({
      body: { reason: "manual cancellation from run detail" },
      key: expect.stringMatching(/^kd1\./),
    });
    expect(screen.queryByRole("dialog", { name: "Balance Layout" })).not.toBeInTheDocument();
  });

  it("lets the operator dismiss the cancel dialog without mutating the run", async () => {
    const user = userEvent.setup();

    render(<RunDetailPage />);

    await user.click(screen.getByTestId("run-cancel-pill"));
    const dialog = screen.getByRole("dialog", { name: "Balance Layout" });

    await user.click(within(dialog).getByRole("button", { name: "Keep Run Active" }));

    expect(cancelMutateAsyncMock).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog", { name: "Balance Layout" })).not.toBeInTheDocument();
  });

	it("does not surface execution recovery notices in the run header", () => {
		mockExecutions = [
			runNode(runExecution({
				attempt_id: "attempt-recovery-pending",
				node_run_id: "nr-recovery-pending",
				node_id: "implementation",
				attempt: 2,
				agent_id: "agent-builder",
				required_role: "builder",
				execution_mode: "provider_backed",
				status: "verifying",
				activity_at: "2026-03-06T10:01:00.000Z",
				updated_at: "2026-03-06T10:01:00.000Z",
			})),
			runNode(runExecution({
				attempt_id: "attempt-recovery-exhausted",
				node_run_id: "nr-recovery-exhausted",
				node_id: "implementation",
				attempt: 3,
				agent_id: "agent-builder",
				required_role: "builder",
				execution_mode: "provider_backed",
				status: "failed",
				failure_code: "provider_incomplete_turn",
				activity_at: "2026-03-06T10:02:00.000Z",
				updated_at: "2026-03-06T10:02:00.000Z",
			})),
		];

		render(<RunDetailPage />);

		expect(screen.queryByTestId("run-header-notices")).not.toBeInTheDocument();
		expect(screen.queryByText(/Recovery Pending:/)).not.toBeInTheDocument();
		expect(screen.queryByText(/Verification Expired:/)).not.toBeInTheDocument();
		expect(screen.queryByText(/Response Captured, Verifying:/)).not.toBeInTheDocument();
	});

  it("shows auto-succeeded indicators only when triggered", () => {
    const { rerender } = render(<RunDetailPage />);
    expect(screen.queryByText(/Auto-Succeeded at Cap/)).not.toBeInTheDocument();
    mockRun.review_auto_succeeded_count = 1;
    mockRun.validation_auto_succeeded_count = 2;
    rerender(<RunDetailPage />);
    expect(screen.getByText("1 Auto-Succeeded at Cap")).toBeVisible();
    expect(screen.getByText("2 Auto-Succeeded at Cap")).toBeVisible();
  });

  it("shows single review/validation cycle counters in the main run frame", () => {
    mockRun.review_rework_cycles = 2;
    mockRun.validation_rework_waves = 1;
    render(<RunDetailPage />);
    const summary = screen.getByRole("region", { name: "Review and validation rework" });
    expect(within(summary).getByText("Review Rework").parentElement).toHaveTextContent("2 / 5");
    expect(within(summary).getByText("Validation Rework").parentElement).toHaveTextContent("1 / 5");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("does not duplicate the main rework counters inside Run Details", () => {
    mockSearch.panel = "details"; mockSearch.section = "context";
    render(<RunDetailPage />);
    const details = screen.getByRole("dialog", { name: "Run Details" });
    expect(within(details).queryByText("Review Rework")).not.toBeInTheDocument();
    expect(within(details).queryByText("Validation Rework")).not.toBeInTheDocument();
  });

  it("shows skip-review mode in the main frame without inventing zero-count quality gates", () => {
    mockRun.workflow_id = "work-order-skip-review-validation-v1";
    render(<RunDetailPage />);
    const summary = screen.getByRole("region", { name: "Review and validation rework" });
    expect(within(summary).getByText("Skipped")).toBeVisible();
    expect(within(summary).queryByText("Review Rework")).not.toBeInTheDocument();
    expect(within(summary).queryByText("Validation Rework")).not.toBeInTheDocument();
    mockRun.workflow_id = "work-order-v1";
  });

  it("opens run files contextually without adding an Output primary view", async () => {
    const { rerender } = render(<RunDetailPage />);
    await userEvent.click(screen.getByRole("button", { name: "Run Files" }));
    rerender(<RunDetailPage />);
    const dialog = screen.getByRole("dialog", { name: "Run Files" });
    expect(within(dialog).getByText("artifacts")).toBeVisible();
    expect(within(dialog).getByText("Attempt Picker")).not.toBeVisible();
    await userEvent.click(within(dialog).getByText("Publication Results"));
    expect(within(dialog).getByText("Attempt Picker")).toBeVisible();
    expect(screen.queryByRole("tab", { name: "Output" })).not.toBeInTheDocument();
    expect(navigateMock).toHaveBeenCalledWith(expect.objectContaining({ search: { tab: "execution", panel: "files" } }));
  });

  it("defaults to Workflow with exactly two primary views and contextual resources", () => {
    mockSearch.tab = undefined;
    render(<RunDetailPage />);
    expect(screen.getByText("graph")).toBeInTheDocument();
    expect(screen.getAllByRole("tab").map((tab) => tab.textContent)).toEqual(["Workflow", "Agent Activity"]);
    expect(screen.getByRole("tab", { name: "Workflow" })).toHaveAttribute("aria-selected", "true");
    expect(screen.queryByRole("combobox", { name: "More run views" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run Files" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Run Details" })).toBeVisible();
    expect(screen.queryByTestId("run-plan-approval-target")).not.toBeInTheDocument();
    expect(executionSurfacePropsMock).not.toHaveBeenCalled();
  });
  it("opens a live agent's focused activity view and removes the graph from that view", async () => {
    mockSearch.tab = "workflow";
    const user = userEvent.setup();
    const { rerender } = render(<RunDetailPage />);
    await user.click(screen.getByRole("button", { name: "Open Live Builder Activity" }));
    rerender(<RunDetailPage />);
    expect(screen.queryByText("graph")).not.toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Agent Activity" })).toHaveAttribute("aria-selected", "true");
    expect(executionSurfacePropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ selectedNodeId: "implementation", selectedAttemptId: "attempt-1" }));
  });
  it("labels the run identifier and omits a redundant active status when agents have a live bar", () => {
    mockExecutions = [runNode(runExecution({ status: "running" }))];
    render(<RunDetailPage />);
    expect(screen.getByText("Run ID")).toBeInTheDocument();
    expect(screen.queryByTestId("run-status-pill")).not.toBeInTheDocument();
  });
  it("keeps a truthful status when no provider agent is active or the run is terminal", () => {
    const { rerender } = render(<RunDetailPage />);
    expect(screen.getByTestId("run-status-pill")).toHaveTextContent("Active");
    mockRun.status = "succeeded";
    mockRun.display_state = "succeeded";
    rerender(<RunDetailPage />);
    expect(screen.getByTestId("run-status-pill")).toHaveTextContent("Succeeded");
  });
  it("opens Audit contextually through Run Details", async () => {
    const user = userEvent.setup();
    mockSearch.tab = "workflow";
    const { rerender } = render(<RunDetailPage />);
    await user.click(screen.getByRole("button", { name: "Run Details" }));
    rerender(<RunDetailPage />);
    await user.click(screen.getByRole("button", { name: "Audit" }));
    rerender(<RunDetailPage />);
    expect(screen.getByRole("dialog", { name: "Run Details" })).toBeInTheDocument();
    expect(useTimelineMock).toHaveBeenLastCalledWith("run-1", 50, { enabled: true });
  });
  it("distinguishes unrecorded start observation from creation time", () => {
    mockRun.start_state = "unrecorded";
    render(<RunDetailPage />);
    expect(screen.getByText("Observation Not Recorded")).toBeInTheDocument();
    expect(screen.getByText("Started")).toBeInTheDocument();
    expect(screen.getByText("Updated")).toBeInTheDocument();
    expect(screen.queryByText("Created")).not.toBeInTheDocument();
  });

});
