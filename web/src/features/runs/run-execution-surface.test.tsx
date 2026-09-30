import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type {
  RunDetailSummary,
  RunExecution,
  RunNodeAttemptHistoryItem,
  RunNodeExecution,
} from "@/api/hooks";
import { RunExecutionSurface } from "./run-execution-surface";

vi.mock("@/theme", async () => {
  const actual = await vi.importActual<typeof import("@/theme")>("@/theme");
  return {
    ...actual,
    useTheme: () => ({ theme: actual.builtInThemes[3] }),
    useRoleIdentity: (role: string) =>
      actual.roleIdentity(actual.builtInThemes[3], role),
    ThemeIcon: () => <span aria-hidden="true" />,
  };
});

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

function execution(overrides: Partial<RunExecution> = {}): RunExecution {
  return {
    attempt_id: "attempt-current",
    node_run_id: "node-current",
    node_id: "implementation",
    attempt: 2,
    agent_id: "agent-1",
    required_role: "builder",
    execution_mode: "provider_backed",
    status: "running",
    queued_at: "2026-03-01T10:00:00Z",
    activity_at: "2026-03-01T10:01:00Z",
    updated_at: "2026-03-01T10:01:00Z",
    stop_retry_available: false,
    ...overrides,
  };
}

const run = {
  run_id: "run-1",
  work_order_id: "work order-1",
  work_order_title: "Phase 9",
  workflow_id: "work-order-v1",
  status: "running",
  display_state: "running",
  plan_state: "approved",
  created_at: "2026-03-01T10:00:00Z",
  updated_at: "2026-03-01T10:01:00Z",
  last_activity_at: "2026-03-01T10:01:00Z",
  autonomous_approvals: true,
  run_until_all_approve: false,
  publish_mode: "direct",
  model_selection: { global: { model: "model" }, role_overrides: {} },
  tab_counts: {
    dag_nodes: 1,
    agents: 1,
    activity_events: 1,
    executions: 1,
    messages: 1,
  },
} as RunDetailSummary;

function node(overrides: Partial<RunNodeExecution> = {}): RunNodeExecution {
  return {
    node_id: "implementation",
    required_role: "builder",
    execution_ownership: "provider_backed",
    status: "running",
    attempt_count: 2,
    current_execution: execution(),
    ...overrides,
  };
}

function historyAttempt(
  overrides: Partial<RunNodeAttemptHistoryItem> = {},
): RunNodeAttemptHistoryItem {
  return {
    ...execution({
      attempt_id: "attempt-old",
      node_run_id: "node-old",
      attempt: 1,
      status: "failed",
      failure_text: "provider error",
    }),
    superseded: true,
    failure: {
      code: "llm_rate_limited",
      classification: "retryable",
      category: "provider",
      retryable: true,
      operator_text: "The provider rate limit was reached.",
    },
    retry_relationships: [
      {
        direction: "source",
        phase: "scheduled",
        trigger: "automatic_retry",
        reason_code: "llm_rate_limited",
        source_attempt_id: "attempt-old",
        source_node_run_id: "node-old",
        target_attempt_id: "attempt-current",
        target_node_run_id: "node-current",
        retry_index: 1,
        retry_max: 3,
        scheduled_at: "2026-03-01T10:02:00Z",
        last_updated_at: "2026-03-01T10:02:00Z",
      },
    ],
    token_usage: {
      input_tokens: 10,
      output_tokens: 20,
      total_tokens: 30,
      reasoning_tokens: 4,
      cache_read_tokens: 2,
      cache_write_tokens: 1,
    },
    provider_invocation_id: "inv-old",
    ...overrides,
  };
}

function renderSurface(
  overrides: Partial<Parameters<typeof RunExecutionSurface>[0]> = {},
) {
  const props: Parameters<typeof RunExecutionSurface>[0] = {
    run,
    nodes: [node()],
    attempts: [historyAttempt()],
    selectedNodeId: "implementation",
    selectedAttemptId: null,
    onSelectStep: vi.fn(),
    onSelectAttempt: vi.fn(),
    onSelectCurrentAttempt: vi.fn(),
    hasMoreAttempts: false,
    onLoadMoreAttempts: vi.fn(),
    isLoadingMoreAttempts: false,
    isHistoryLoading: false,
    detail: null,
    isDetailLoading: false,
    detailErrorMessage: null,
    onRetryDetail: vi.fn(),
    onRetryStop: vi.fn(),
    isStopRetryPending: false,
    stopRetryErrorMessage: null,
    ...overrides,
  };
  render(<RunExecutionSurface {...props} />);
  return props;
}

describe("focused execution inspector", () => {
  it("keeps the selected identity in compact controls and moves routine status prose into details", async () => {
    renderSurface();
    expect(
      screen.getByRole("combobox", { name: "Select workflow agent or step" }),
    ).toBeVisible();
    expect(
      screen.getByRole("combobox", { name: "Select attempt" }),
    ).toBeVisible();
    expect(
      screen.queryByRole("heading", { name: /Builder/ }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("This step is still running.")).not.toBeVisible();
    await userEvent.click(screen.getByText("Execution Details"));
    expect(screen.getByText("This step is still running.")).toBeVisible();
  });
  it("shows one agent selector and an attempt selector only when more than one attempt exists", () => {
    renderSurface({ nodes: [node({ attempt_count: 1 })] });
    expect(
      screen.getByRole("combobox", { name: "Select workflow agent or step" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("combobox", { name: "Select attempt" }),
    ).not.toBeInTheDocument();
  });
  it("switches to a historical attempt and preserves an explicit return to current", async () => {
    const props = renderSurface({ selectedAttemptId: "attempt-old" });
    expect(
      screen.getByRole("combobox", { name: "Select attempt" }),
    ).toHaveValue("attempt-old");
    expect(
      screen.getByText(/Historical Attempt 1.*Superseded/),
    ).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole("button", { name: "Return to Current Attempt" }),
    );
    expect(props.onSelectCurrentAttempt).toHaveBeenCalledOnce();
  });
  it("never presents current evidence as a missing historical attempt", () => {
    renderSurface({ attempts: [], selectedAttemptId: "attempt-old" });
    expect(
      screen.getByRole("combobox", { name: "Select attempt" }),
    ).toHaveValue("attempt-old");
    expect(screen.getByText("Selected Attempt")).toBeInTheDocument();
    expect(
      screen.queryByText("This step is still running."),
    ).not.toBeInTheDocument();
  });
  it("offers Retry Stop only for the current paused attempt", async () => {
    const props = renderSurface({
      nodes: [
        node({
          current_execution: execution({
            status: "paused",
            stop_retry_available: true,
          }),
        }),
      ],
    });
    await userEvent.click(screen.getByRole("button", { name: "Retry Stop" }));
    expect(props.onRetryStop).toHaveBeenCalledWith("attempt-current");
  });
  it("preserves failure/recovery evidence and exposes paged attempt history", async () => {
    const props = renderSurface({
      hasMoreAttempts: true,
      nodes: [
        node({
          recovery: {
            phase: "scheduled",
            trigger: "automatic_retry",
            reason_code: "llm_rate_limited",
            source_attempt_id: "attempt-current",
            source_node_run_id: "node-current",
            retry_index: 1,
            retry_max: 3,
            scheduled_at: "2026-03-01T10:02:00Z",
            last_updated_at: "2026-03-01T10:02:00Z",
          },
        }),
      ],
    });
    expect(
      screen.getByText("Automatic Recovery · Automatic retry 1 of 3"),
    ).toBeInTheDocument();
    await userEvent.click(
      screen.getByRole("button", { name: "Load Older Attempts" }),
    );
    expect(props.onLoadMoreAttempts).toHaveBeenCalledOnce();
  });
  it("opens Output for the exact selected historical settled attempt", async () => {
    const onViewOutput = vi.fn();
    renderSurface({
      selectedAttemptId: "attempt-old",
      detail: {
        ...execution({
          attempt_id: "attempt-old",
          attempt: 1,
          status: "succeeded",
        }),
        settlement_status: "succeeded",
        output_status: "settled",
        output_text: "Historical answer",
      },
      onViewOutput,
    });
    await userEvent.click(
      screen.getByRole("button", { name: "View Settled Response" }),
    );
    expect(onViewOutput).toHaveBeenCalledOnce();
    expect(
      screen.getByRole("combobox", { name: "Select attempt" }),
    ).toHaveValue("attempt-old");
  });
});
