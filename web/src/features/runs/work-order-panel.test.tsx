import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { WorkOrderPanel } from "./work-order-panel";

const useWorkOrderMock = vi.fn();
const useProviderModelsMock = vi.fn();
const refetchWorkOrder = vi.fn();

vi.mock("@/api/hooks", () => ({
  useWorkOrder: (...args: unknown[]) => useWorkOrderMock(...args),
  useProviderModels: (...args: unknown[]) => useProviderModelsMock(...args),
  newIdempotencyKey: () => "refresh-key",
}));

vi.mock("@/features/connections/external-source-comparison", () => ({
  ExternalSourceComparison: ({ baselineId, candidateId }: { baselineId: string; candidateId: string }) => <div aria-label="GitHub source comparison" data-baseline={baselineId} data-candidate={candidateId}>Source comparison</div>,
}));

const baseRun = {
  run_id: "run-1",
  work_order_id: "work order-1",
  work_order_title: "Royal Landing Page",
  workflow_id: "work-order-v1",
  status: "running",
  display_state: "running",
  plan_state: "approved",
  created_at: "2026-03-06T10:00:00.000Z",
  updated_at: "2026-03-06T10:01:00.000Z",
  last_activity_at: "2026-03-06T10:01:00.000Z",
  start_state: "not_started",
  run_until_all_approve: false,
  autonomous_approvals: true,
  publish_mode: "git_worktree",
  max_execution_review_cycles: 5,
  max_validation_rework_cycles: 5,
  model_selection: {
    global: {
      model: "github-copilot/gpt-5.3-codex",
      thinking_effort: "high",
    },
    role_overrides: {
      "builder": {
        model: "opencode/gpt-5-nano",
      },
    },
  },
} as const;

const baseRoles = [
  {
    role_id: "builder",
    display_name: "Builder",
    description: "",
    can_spawn: false,
    agent_model_selector_visible: true,
    agent_model_selector_section_id: "builder",
    agent_model_selector_section_label: "Builder",
    agent_model_selector_section_order: 50,
    agent_model_selector_role_order: 10,
  },
  {
    role_id: "coordinator",
    display_name: "Coordinator",
    description: "",
    can_spawn: true,
    agent_model_selector_visible: true,
    agent_model_selector_section_id: "coordinator",
    agent_model_selector_section_label: "Coordinator",
    agent_model_selector_section_order: 30,
    agent_model_selector_role_order: 10,
  },
];

describe("work order panel", () => {
  afterEach(() => vi.unstubAllGlobals());
  beforeEach(() => {
    useWorkOrderMock.mockReset();
    useProviderModelsMock.mockReset();
    refetchWorkOrder.mockReset();
    useWorkOrderMock.mockReturnValue({
      data: {
        work_order_id: "work order-1",
        work_order_title: "Royal Landing Page",
        work_order_text: "Build the new experience.",
        metadata: {
          planning_session_id: "planning-session-1",
        },
        created_at: "2026-03-06T10:00:00.000Z",
      },
      isLoading: false,
      error: null,
      refetch: refetchWorkOrder,
    });
    useProviderModelsMock.mockReturnValue({
      data: undefined,
      isLoading: false,
    });
  });

  it("shows the complete work order immediately without a nested read dialog", () => {
    render(<WorkOrderPanel run={baseRun} roles={baseRoles} />);
    expect(screen.getByText("Royal Landing Page")).toBeVisible();
    expect(screen.getByText("Build the new experience.")).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Read work order" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("region", { name: "Work order settings" }),
    ).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Open work order settings" }),
    ).not.toBeInTheDocument();
  });

  it("shows complete settings inline without opening a second dialog", () => {
    useProviderModelsMock.mockReturnValue({
      data: {
        items: [
          {
            id: "github-copilot/gpt-5.3-codex",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.3-Codex",
            thinking_effort_options: [],
          },
          {
            id: "opencode/gpt-5-nano",
            provider_id: "opencode",
            provider_name: "OpenCode",
            model_name: "GPT-5 Nano",
            thinking_effort_options: [],
          },
        ],
      },
      isLoading: false,
    });
    render(<WorkOrderPanel run={baseRun} roles={baseRoles} />);

    const settings = screen.getByRole("region", { name: "Work order settings" });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(within(settings).getByText("Autonomous")).toBeVisible();
    expect(within(settings).getByText("Git Worktree")).toBeVisible();
    expect(within(settings).getByText("Default Model")).toBeVisible();
    expect(
      within(settings).getByText("GitHub Copilot - GPT-5.3-Codex"),
    ).toBeVisible();
    expect(within(settings).getByText("OpenCode - GPT-5 Nano")).toBeVisible();
    expect(within(settings).getByText("Builder")).toBeVisible();
    expect(within(settings).getByText("Thinking: High")).toBeVisible();
    expect(within(settings).getByText("Thinking: Model Default")).toBeVisible();
    expect(
      within(settings).queryByText("github-copilot/gpt-5.3-codex"),
    ).not.toBeInTheDocument();
    expect(within(settings).queryByText("Limits")).not.toBeInTheDocument();
    expect(within(settings).queryByText("run-1")).not.toBeInTheDocument();
  });

  it("falls back to raw ids and skip-review wording when lookup data is unavailable", async () => {
    useProviderModelsMock.mockReturnValue({
      data: undefined,
      isLoading: true,
    });

    render(
      <WorkOrderPanel
        run={{
          ...baseRun,
          workflow_id: "work-order-skip-review-validation-v1",
          publish_mode: "direct",
          model_selection: {
            global: { model: "github-copilot/gpt-5.4-mini" },
            role_overrides: {
              "unknown-role": {
                model: "custom/model-id",
                thinking_effort: "low",
              },
            },
          },
        }}
        roles={[]}
      />,
    );

    expect(screen.getByText("Review and Validation Skipped")).toBeInTheDocument();
    expect(screen.queryByText("Limits")).not.toBeInTheDocument();
    expect(screen.getByText("Direct to Project")).toBeInTheDocument();
    expect(screen.getByText("github-copilot/gpt-5.4-mini")).toBeInTheDocument();
    expect(screen.getByText("Unknown Role")).toBeInTheDocument();
    expect(screen.queryByText("unknown-role")).not.toBeInTheDocument();
    expect(screen.getByText("custom/model-id")).toBeInTheDocument();
    expect(screen.getByText("Thinking: Low")).toBeInTheDocument();
    expect(screen.getByText("Resolving model names…")).toBeInTheDocument();
  });

  it("keeps the recorded selections visible when model names fail to load", async () => {
    const retry = vi.fn();
    useProviderModelsMock.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error("Unavailable"),
      refetch: retry,
    });
    render(<WorkOrderPanel run={baseRun} roles={baseRoles} />);
    const settings = screen.getByRole("region", { name: "Work order settings" });
    expect(
      within(settings).getByText("github-copilot/gpt-5.3-codex"),
    ).toBeVisible();
    expect(within(settings).getByText("opencode/gpt-5-nano")).toBeVisible();
    expect(within(settings).getByRole("alert")).toHaveTextContent(
      "Model names could not be loaded. Recorded selections are shown.",
    );
    await userEvent.click(
      within(settings).getByRole("button", { name: "Retry Model Names" }),
    );
    expect(retry).toHaveBeenCalledOnce();
  });

  it("shows settings while work order text is loading and allows a failed work order read to retry", async () => {
    useWorkOrderMock.mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
      refetch: refetchWorkOrder,
    });
    const { rerender } = render(
      <WorkOrderPanel run={baseRun} roles={baseRoles} />,
    );
    expect(screen.getByText("Loading work order…")).toBeVisible();
    expect(
      screen.getByRole("region", { name: "Work order settings" }),
    ).toBeVisible();
    useWorkOrderMock.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error("Unavailable"),
      refetch: refetchWorkOrder,
    });
    rerender(<WorkOrderPanel run={baseRun} roles={baseRoles} />);
    await userEvent.click(
      screen.getByRole("button", { name: "Retry Work Order" }),
    );
    expect(refetchWorkOrder).toHaveBeenCalledOnce();
  });

  it("retains every role override in catalog order with the recorded thinking effort", () => {
    render(
      <WorkOrderPanel
        run={{
          ...baseRun,
          autonomous_approvals: false,
          run_until_all_approve: true,
          model_selection: {
            global: { model: "global/model" },
            role_overrides: {
              builder: { model: "builder/model", thinking_effort: "none" },
              coordinator: {
                model: "coordinator/model",
                thinking_effort: "high",
              },
              "unknown-role": { model: "unknown/model", thinking_effort: "low" },
            },
          },
        }}
        roles={baseRoles}
      />,
    );
    const settings = screen.getByRole("region", { name: "Work order settings" });
    expect(within(settings).getByText("Requires Your Approval")).toBeVisible();
    expect(within(settings).getByText("Until All Approve")).toBeVisible();
    expect(
      within(settings)
        .getAllByRole("term")
        .map((term) => term.textContent),
    ).toEqual([
      "Approval",
      "Review Flow",
      "Publish Target",
      "Default Model",
      "Coordinator",
      "Builder",
      "Unknown Role",
    ]);
    expect(
      within(settings).getByText("coordinator/model").parentElement,
    ).toHaveTextContent("Thinking: High");
    expect(
      within(settings).getByText("builder/model").parentElement,
    ).toHaveTextContent("Thinking: None");
    expect(
      within(settings).getByText("unknown/model").parentElement,
    ).toHaveTextContent("Thinking: Low");
  });

  it("shows every issued source and refreshes their comparisons independently without changing work order text", async () => {
    useWorkOrderMock.mockReturnValue({
      data: {
        work_order_id: "work order-1", work_order_title: "Royal Landing Page", work_order_text: "Immutable operator text.", created_at: "2026-03-06T10:00:00.000Z",
        external_sources: [
          { link_id: "link-1", provider: "github", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "unchanged" },
          { link_id: "link-2", provider: "github", repository: "owner/other", issue_number: 8, canonical_url: "https://github.com/owner/other/issues/8", used_snapshot_id: "snapshot-c", latest_snapshot_id: "snapshot-c", status: "unchanged" },
        ],
      },
      isLoading: false, error: null, refetch: refetchWorkOrder,
    });
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const second = String(input).includes("link-2");
      return new Response(JSON.stringify({
        link: { link_id: second ? "link-2" : "link-1", target_kind: "work_order", target_id: "work order-1", source_id: second ? "source-2" : "source-1", used_snapshot_id: second ? "snapshot-c" : "snapshot-a", latest_snapshot_id: second ? "snapshot-d" : "snapshot-b", status: "changed", row_version: 2, provider: "github", repository: second ? "owner/other" : "owner/repo", canonical_url: second ? "https://github.com/owner/other/issues/8" : "https://github.com/owner/repo/issues/7", issue_number: second ? 8 : 7, created_at: "2026-03-06T10:00:00Z" },
        outcome: "changed", snapshot_created: true, changes: { title: true, body: true, state: false, repository: false, issue_number: false, canonical_url: false, created_at: false, updated_at: true, comment_count: false, comments: false }, message: second ? "Second source changed." : "First source changed.",
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<WorkOrderPanel run={baseRun} roles={baseRoles} />);
    expect(screen.getByRole("link", { name: /owner\/repo#7/i })).toHaveAttribute("rel", "noopener noreferrer");
    expect(screen.getByRole("link", { name: /owner\/other#8/i })).toBeInTheDocument();
    const firstSource = screen.getByRole("region", { name: "Issued source owner/repo#7" });
    const secondSource = screen.getByRole("region", { name: "Issued source owner/other#8" });
    await user.click(within(firstSource).getByRole("button", { name: /check github source/i }));
    expect(await within(firstSource).findByText("First source changed.")).toBeInTheDocument();
    expect(within(secondSource).queryByLabelText("GitHub source comparison")).not.toBeInTheDocument();
    expect(within(firstSource).getByLabelText("GitHub source comparison")).toHaveAttribute("data-candidate", "snapshot-b");
    await user.click(within(secondSource).getByRole("button", { name: /check github source/i }));
    expect(await within(secondSource).findByText("Second source changed.")).toBeInTheDocument();
    expect(within(secondSource).getByLabelText("GitHub source comparison")).toHaveAttribute("data-baseline", "snapshot-c");
    expect(within(secondSource).getByLabelText("GitHub source comparison")).toHaveAttribute("data-candidate", "snapshot-d");
    expect(within(firstSource).getByText("First source changed.")).toBeInTheDocument();
    expect(screen.getByText("Immutable operator text.")).toBeInTheDocument();
    await waitFor(() => expect(refetchWorkOrder).toHaveBeenCalledTimes(2));
  });

  it("retains revoked source recovery guidance after work order detail reload", async () => {
    useWorkOrderMock.mockReturnValue({
      data: {
        work_order_id: "work order-1", work_order_title: "Royal Landing Page", work_order_text: "Immutable operator text.", created_at: "2026-03-06T10:00:00.000Z",
        external_sources: [{ link_id: "link-1", provider: "github", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "revoked" }],
      },
      isLoading: false, error: null, refetch: refetchWorkOrder,
    });
    render(<WorkOrderPanel run={baseRun} roles={baseRoles} />);


    expect(screen.getByText(/GitHub App access was revoked/i)).toBeInTheDocument();
    expect(screen.getByText(/run the connection health check/i)).toBeInTheDocument();
    expect(screen.getByText("Immutable operator text.")).toBeInTheDocument();
  });

  it("retries an issued-source pending reservation with the same key", async () => {
    useWorkOrderMock.mockReturnValue({
      data: {
        work_order_id: "work order-1", work_order_title: "Royal Landing Page", work_order_text: "Immutable operator text.", created_at: "2026-03-06T10:00:00.000Z",
        external_sources: [{ link_id: "link-1", provider: "github", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "unchanged" }],
      },
      isLoading: false, error: null, refetch: refetchWorkOrder,
    });
    const keys: string[] = [];
    let attempts = 0;
    vi.stubGlobal("fetch", vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      attempts++;
      keys.push(new Headers(init?.headers).get("Idempotency-Key") || "");
      if (attempts === 1) {
        return new Response(JSON.stringify({ error: { code: "idempotency_pending", message: "Refresh is settling", retryable: true, category: "core", details: {}, correlation_id: "test" } }), { status: 409, headers: { "Content-Type": "application/json" } });
      }
      return new Response(JSON.stringify({
        link: { link_id: "link-1", target_kind: "work_order", target_id: "work order-1", source_id: "source-1", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "unchanged", row_version: 2, provider: "github", repository: "owner/repo", canonical_url: "https://github.com/owner/repo/issues/7", issue_number: 7, created_at: "2026-03-06T10:00:00Z" },
        outcome: "unchanged", snapshot_created: false, changes: {}, message: "Source is current.",
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<WorkOrderPanel run={baseRun} roles={baseRoles} />);
    await user.click(screen.getByRole("button", { name: "Check GitHub source owner/repo#7" }));

    await user.click(await screen.findByRole("button", { name: "Retry GitHub source owner/repo#7" }));

    expect(await screen.findByText("Source is current.")).toBeInTheDocument();
    expect(keys).toHaveLength(2);
    expect(keys[1]).toBe(keys[0]);
  });
});
