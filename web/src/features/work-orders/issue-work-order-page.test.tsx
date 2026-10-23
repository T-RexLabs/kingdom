import { render } from "./planning-test-render";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode, type ReactNode } from "react";
import { ApiError, setCSRFToken } from "@/api/client";
import { IssueWorkOrderPage } from "./issue-work-order-page";

const createWorkOrderMutate = vi.fn<(request: Record<string, unknown>) => Promise<Record<string, unknown>>>();
const createRunMutate = vi.fn<(request: Record<string, unknown>) => Promise<Record<string, unknown>>>();
const mockUseProviderModels = vi.fn();
const refreshProviderModelsMutate = vi.fn();
const navigate = vi.fn();
let mockSearch: Record<string, string> = {};

function makeWorkOrderResult(overrides: Record<string, unknown> = {}) {
  return {
    work_order_id: "work order-1",
    created_at: "2026-03-01T10:00:00.000Z",
    ...overrides,
  };
}

function makeRunResult(overrides: Record<string, unknown> = {}) {
  return {
    run_id: "run-1",
    workflow_id: "work-order-v1",
    work_order_title: "test",
    status: "pending",
    plan_state: "draft",
    created_at: "2026-03-01T10:00:00.000Z",
    updated_at: "2026-03-01T10:00:00.000Z",
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function makeApiError(status: number, message: string, overrides: Partial<ConstructorParameters<typeof ApiError>[1]> = {}) {
  return new ApiError(status, {
    message,
    code: "internal_error",
    retryable: true,
    category: "core",
    details: {},
    correlationId: "api.test",
    ...overrides,
  });
}

let mockWorkspaceCapabilities = {
  project_root: "/tmp/project",
  default_publish_mode: "direct",
  git_repo: true,
  git_worktree_available: true,
  git_worktree_reason: undefined as string | undefined,
};

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, ...props }: { children: ReactNode }) => <a href="#" {...props}>{children}</a>,
  useSearch: () => mockSearch,
  useNavigate: () => (...args: unknown[]) => navigate(...args),
}));

vi.mock("@/features/connections/issue-picker", () => ({
  IssuePicker: ({ onSelect }: { onSelect: (selection: unknown) => Promise<void> }) => <button type="button" onClick={() => void onSelect({ intake: { connection_id: "conn-1", repository_id: "repo-1", issue_number: 7, include_comments: false }, issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" } })}>Mock prefill issue</button>,
}));

vi.mock("@/features/connections/external-source-comparison", () => ({
  ExternalSourceComparison: ({ baselineId, candidateId }: { baselineId: string; candidateId: string }) => <div aria-label="GitHub source comparison" data-baseline={baselineId} data-candidate={candidateId}>Source comparison</div>,
}));

vi.mock("@/api/hooks", async () => ({
  newIdempotencyKey: (await vi.importActual<typeof import("@/lib/idempotency")>("@/lib/idempotency")).newIdempotencyKey,
  useCreateWorkOrder: () => ({ mutateAsync: createWorkOrderMutate, isPending: false, error: null, reset: vi.fn() }),
  useCreateRun: () => ({ mutateAsync: createRunMutate, isPending: false, error: null, reset: vi.fn() }),
  usePlanningSessions: () => ({
    data: {
      items: [
        {
          planning_session_id: "planning-session-1",
          title: "Test session",
          status: "plan_ready",
          current_draft_id: "planning-draft-1",
          created_at: "2026-03-01T10:00:00.000Z",
          updated_at: "2026-03-01T10:00:00.000Z",
        },
      ],
    },
    isLoading: false,
  }),
  usePlanningSessionDraft: () => ({
    data: {
      planning_draft_id: "planning-draft-1",
      planning_session_id: "planning-session-1",
      work_order_title: "Draft supplied title",
      work_order_text: "Draft supplied text",
      created_at: "2026-03-01T10:00:00.000Z",
    },
  }),
  useProviderModels: () => mockUseProviderModels(),
  useRefreshProviderModels: () => ({ mutate: refreshProviderModelsMutate, isPending: false }),
  useWorkspaceCapabilities: () => ({
    data: mockWorkspaceCapabilities,
    isLoading: false,
    error: null,
  }),
  useRoles: () => ({
    data: {
      items: [
        { role_id: "governance", display_name: "Governance", description: "", can_spawn: true, agent_model_selector_visible: true, agent_model_selector_section_id: "governance", agent_model_selector_section_label: "Governance", agent_model_selector_section_order: 10, agent_model_selector_role_order: 10 },
        { role_id: "architecture-advisor", display_name: "Architecture Advisor", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 10 },
        { role_id: "risk-advisor", display_name: "Risk Advisor", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 20 },
        { role_id: "systems-advisor", display_name: "Systems Advisor", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 30 },
        { role_id: "usability-advisor", display_name: "Usability Advisor", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 40 },
        { role_id: "coordinator", display_name: "Coordinator", description: "", can_spawn: true, agent_model_selector_visible: true, agent_model_selector_section_id: "coordinator", agent_model_selector_section_label: "Coordinator", agent_model_selector_section_order: 30, agent_model_selector_role_order: 10 },
        { role_id: "plan-author", display_name: "Plan Author", description: "", can_spawn: true, agent_model_selector_visible: true, agent_model_selector_section_id: "plan-author", agent_model_selector_section_label: "Plan Author", agent_model_selector_section_order: 40, agent_model_selector_role_order: 10 },
        { role_id: "builder", display_name: "Builder", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "builder", agent_model_selector_section_label: "Builder", agent_model_selector_section_order: 50, agent_model_selector_role_order: 10 },
        { role_id: "reviewer", display_name: "Reviewer", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "reviewers", agent_model_selector_section_label: "Reviewers", agent_model_selector_section_order: 60, agent_model_selector_role_order: 10 },
        { role_id: "qa-validator", display_name: "Quality Validator", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "validators", agent_model_selector_section_label: "Validators", agent_model_selector_section_order: 70, agent_model_selector_role_order: 10 },
        { role_id: "security-validator", display_name: "Security Validator", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "validators", agent_model_selector_section_label: "Validators", agent_model_selector_section_order: 70, agent_model_selector_role_order: 20 },
        { role_id: "resilience-validator", display_name: "Resilience Validator", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "validators", agent_model_selector_section_label: "Validators", agent_model_selector_section_order: 70, agent_model_selector_role_order: 30 },
        { role_id: "operator", display_name: "Operator", description: "", can_spawn: false, agent_model_selector_visible: false, agent_model_selector_section_id: "operators", agent_model_selector_section_label: "Operators", agent_model_selector_section_order: 0, agent_model_selector_role_order: 10 },
      ],
    },
  }),
}));

function renderWithReviewOptions(view: Parameters<typeof render>[0]) {
  const result = render(view);
  const summary = screen.getByText("Review Options").closest("summary");
  if (summary && !summary.closest("details")?.open) fireEvent.click(summary);
  return result;
}

describe("issue work order page", () => {
  afterEach(() => vi.unstubAllGlobals());
  beforeEach(() => {
    setCSRFToken("token", "same-binding");
    createWorkOrderMutate.mockReset();
    createRunMutate.mockReset();
    navigate.mockReset();
    createWorkOrderMutate.mockResolvedValue(makeWorkOrderResult());
    createRunMutate.mockResolvedValue(makeRunResult());
    mockSearch = {
      planning_session_id: "planning-session-1",
      planning_draft_id: "planning-draft-1",
    };
    refreshProviderModelsMutate.mockReset();
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "fresh",
        items: [
          {
            id: "github-copilot/gpt-5.3-codex",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.3-Codex",
            thinking_effort_default: "medium",
            thinking_effort_options: [
              { value: "low", label: "Low" },
              { value: "medium", label: "Medium" },
              { value: "high", label: "High" },
            ],
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
      error: null,
      refetch: vi.fn(),
    });
    mockWorkspaceCapabilities = {
      project_root: "/tmp/project",
      default_publish_mode: "direct",
      git_repo: true,
      git_worktree_available: true,
      git_worktree_reason: undefined,
    };
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: undefined,
    });
  });

  async function selectGlobalModel(user: ReturnType<typeof userEvent.setup>) {
    await user.click(screen.getByRole("combobox", { name: /Global Model/ }));
    await user.click(screen.getByRole("option", { name: "GitHub Copilot - GPT-5.3-Codex" }));
  }

  it("keeps creation focused without project location, recent runs, or success ID cards", () => {
    render(<IssueWorkOrderPage />);
    expect(screen.queryByText("Project Location")).not.toBeInTheDocument();
    expect(screen.queryByText("Recent Runs")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Recent Work Order Runs" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toBeVisible();
    expect(navigate).not.toHaveBeenCalled();
  });

  it("opens the confirmed run exactly once after clearing the draft, including StrictMode rerenders", async () => {
    const user = userEvent.setup();
    let persistedTitleAtNavigation: unknown;
    navigate.mockImplementation(() => {
      const key = Object.keys(sessionStorage).find((candidate) =>
        candidate.startsWith("kingdom:work-order-draft:"),
      );
      persistedTitleAtNavigation = key
        ? JSON.parse(sessionStorage.getItem(key)!).title
        : undefined;
    });
    const view = render(
      <StrictMode>
        <IssueWorkOrderPage />
      </StrictMode>,
    );
    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));
    await waitFor(() =>
      expect(navigate).toHaveBeenCalledWith({
        to: "/runs/$runId",
        params: { runId: "run-1" },
        replace: true,
      }),
    );
    expect(persistedTitleAtNavigation).toBe("");
    expect(screen.queryByText("Work Order Dispatched")).not.toBeInTheDocument();
    expect(screen.queryByText("work order-1")).not.toBeInTheDocument();
    expect(screen.queryByText("run-1")).not.toBeInTheDocument();
    view.rerender(
      <StrictMode>
        <IssueWorkOrderPage />
      </StrictMode>,
    );
    expect(navigate).toHaveBeenCalledOnce();
    expect(createWorkOrderMutate).toHaveBeenCalledOnce();
    expect(createRunMutate).toHaveBeenCalledOnce();
  });

  it("keeps model and approval decisions visible while advanced controls stay optional", async () => {
    const user = userEvent.setup();
    render(<IssueWorkOrderPage />);
    expect(screen.getByRole("heading", { name: /^Model$/ })).toBeVisible();
    expect(screen.getByRole("heading", { name: /^Run Options$/ })).toBeVisible();
    expect(screen.getByRole("combobox", { name: "Global Model" })).toBeVisible();
    expect(screen.getByRole("switch", { name: "Require Human Approval" })).toBeVisible();
    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toBeVisible();
    expect(screen.queryByRole("switch", { name: "Use for All Agents" })).not.toBeInTheDocument();
    expect(screen.getByText("Review Options").closest("details")).not.toHaveAttribute("open");
    await user.click(screen.getByRole("switch", { name: "Require Human Approval" }));
    expect(screen.getByRole("switch", { name: "Require Human Approval" })).toHaveAttribute("aria-checked", "true");
    await user.click(screen.getByText("Review Options"));
    expect(screen.getByRole("switch", { name: "Skip Reviews and Validations" })).toBeVisible();
    await user.click(screen.getByRole("button", { name: /Role Models/i }));
    expect(screen.getByRole("switch", { name: "Use for All Agents" })).toBeVisible();
  });

  it.each([false, true])("retains a confirmed run for local draft cleanup without reissuing (remount: %s)", async (remount) => {
    const user = userEvent.setup();
    let view = render(<IssueWorkOrderPage />);
    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.type(screen.getByLabelText("Title"), "Already dispatched work");
    const originalSetItem = Storage.prototype.setItem;
    const storage = vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (this: Storage, key, value) {
      if (
        key.startsWith("kingdom:work-order-draft:") &&
        JSON.parse(value).title === "" &&
        createRunMutate.mock.calls.length > 0
      ) {
        throw new DOMException("Storage unavailable", "SecurityError");
      }
      return originalSetItem.call(this, key, value);
    });
    try {
      await user.click(screen.getByRole("button", { name: "Issue Work Order" }));
      await waitFor(() => expect(createRunMutate).toHaveBeenCalledOnce());
      expect(navigate).not.toHaveBeenCalled();
      expect(screen.getByLabelText("Title")).toBeDisabled();
      expect(
        screen.getByRole("switch", { name: "Require Human Approval" }),
      ).toBeDisabled();
      expect(
        screen.queryByRole("button", { name: "Start New Work Order" }),
      ).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Open Run" })).toBeEnabled();
      const draftKey = Object.keys(sessionStorage).find((key) =>
        key.startsWith("kingdom:work-order-draft:"),
      )!;
      expect(JSON.parse(sessionStorage.getItem(draftKey)!).title).toBe(
        "Already dispatched work",
      );

      if (remount) {
        view.unmount();
        view = render(<IssueWorkOrderPage />);
      }
      await user.click(screen.getByRole("button", { name: "Open Run" }));
      expect(navigate).not.toHaveBeenCalled();
      expect(createWorkOrderMutate).toHaveBeenCalledOnce();
      expect(createRunMutate).toHaveBeenCalledOnce();

      storage.mockRestore();
      await user.click(screen.getByRole("button", { name: "Open Run" }));
      await waitFor(() =>
        expect(navigate).toHaveBeenCalledWith({
          to: "/runs/$runId",
          params: { runId: "run-1" },
          replace: true,
        }),
      );
      expect(JSON.parse(sessionStorage.getItem(draftKey)!).title).toBe("");
      expect(
        Object.keys(sessionStorage).some((key) =>
          key.startsWith("kingdom:work-order-dispatch:"),
        ),
      ).toBe(false);
      expect(createWorkOrderMutate).toHaveBeenCalledOnce();
      expect(createRunMutate).toHaveBeenCalledOnce();
    } finally {
      storage.mockRestore();
      view.unmount();
    }
  });

  it("renders issue work order content", () => {
    renderWithReviewOptions(<IssueWorkOrderPage />);
    expect(screen.getByRole("heading", { name: "New Work Order" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Issue Work Order" })).toBeInTheDocument();
  });

  it("prefills work order title and text from selected planning draft", () => {
    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.getByLabelText("Title")).toHaveValue("Draft supplied title");
    expect(screen.getByLabelText("Work Brief")).toHaveValue("Draft supplied text");
  });

  it("retains separate unsent creation briefs for each source and workspace", async () => {
    const user = userEvent.setup();
    const fromPlan = renderWithReviewOptions(<IssueWorkOrderPage />);
    await user.clear(screen.getByLabelText("Title"));
    await user.type(screen.getByLabelText("Title"), "My plan draft");
    fromPlan.unmount();
    mockSearch = {};
    const manual = renderWithReviewOptions(<IssueWorkOrderPage />);
    expect(screen.getByLabelText("Title")).toHaveValue("");
    await user.type(screen.getByLabelText("Title"), "My separate brief");
    manual.unmount();
    mockSearch = { planning_session_id: "planning-session-1", planning_draft_id: "planning-draft-1" };
    const backToPlan = renderWithReviewOptions(<IssueWorkOrderPage />);
    expect(screen.getByLabelText("Title")).toHaveValue("My plan draft");
    backToPlan.unmount();
    mockSearch = {};
    mockWorkspaceCapabilities = { ...mockWorkspaceCapabilities, project_root: "/tmp/another-project" };
    renderWithReviewOptions(<IssueWorkOrderPage />);
    expect(screen.getByLabelText("Title")).toHaveValue("");
  });

  it("forces autonomous approvals when operator toggle is off", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Title"), "Work Order A");
    await user.type(screen.getByLabelText("Work Brief"), "Do the thing");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { autonomous_approvals: true, publish_mode: "direct" } });
  });

  it("forces manual approvals when operator toggle is on", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("switch", { name: /Require Human Approval/i }));
    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Title"), "Work Order B");
    await user.type(screen.getByLabelText("Work Brief"), "Do the other thing");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { autonomous_approvals: false, publish_mode: "direct" } });
  });

  it("ignores repeated submit attempts while work order creation is already in flight", async () => {
    const pendingWorkOrder = deferred<Record<string, unknown>>();
    createWorkOrderMutate.mockImplementationOnce(() => pendingWorkOrder.promise);
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    const form = screen.getByRole("button", { name: "Issue Work Order" }).closest("form") as HTMLFormElement;

    fireEvent.submit(form);
    fireEvent.submit(form);

    expect(createWorkOrderMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate).not.toHaveBeenCalled();
    expect(navigate).not.toHaveBeenCalled();

    pendingWorkOrder.resolve(makeWorkOrderResult());
    await waitFor(() => expect(createRunMutate).toHaveBeenCalledTimes(1));
  });

  it("clears the issued draft when returning to creation after opening the run", async () => {
    const user = userEvent.setup();
    const view = renderWithReviewOptions(<IssueWorkOrderPage />);
    createRunMutate
      .mockResolvedValueOnce(makeRunResult())
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Title"), "First work order");
    await user.type(screen.getByLabelText("Work Brief"), "First work order body");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await waitFor(() => expect(navigate).toHaveBeenCalled());

    view.unmount();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.getByLabelText("Title")).toHaveValue("");
    expect(screen.getByLabelText("Work Brief")).toHaveValue("");
    expect(screen.getByRole("button", { name: "Issue Work Order" })).toBeDisabled();

    await user.type(screen.getByLabelText("Title"), "Second work order");
    await user.type(screen.getByLabelText("Work Brief"), "Second work order body");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await waitFor(() =>
      expect(navigate).toHaveBeenLastCalledWith({
        to: "/runs/$runId",
        params: { runId: "run-2" },
        replace: true,
      }),
    );
    expect(createWorkOrderMutate).toHaveBeenCalledTimes(2);
    expect(createRunMutate).toHaveBeenCalledTimes(2);
  });

  it("starts a fresh run request after a known run-start rejection", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(400, "invalid run settings", { code: "policy_violation", retryable: false, correlationId: "api.create_run" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Title"), "Known failure work order");
    await user.type(screen.getByLabelText("Work Brief"), "Known failure body");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await screen.findByText(/Kingdom rejected the run start request/i);

    expect(createWorkOrderMutate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Start Run" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Start New Work Order" })).toBeEnabled();
    expect(screen.getByLabelText("Title")).toBeDisabled();
    expect(screen.getByLabelText("Work Brief")).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeEnabled();

    await user.clear(screen.getByLabelText("Max Review Cycles"));
    await user.type(screen.getByLabelText("Max Review Cycles"), "7");
    await user.click(screen.getByRole("button", { name: "Start Run" }));

    await waitFor(() => expect(navigate).toHaveBeenCalled());

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { body: { work_order_id: string; max_execution_review_cycles: number }; key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { body: { work_order_id: string; max_execution_review_cycles: number }; key: string };
    expect(firstRunRequest.body.work_order_id).toBe("work order-1");
    expect(secondRunRequest.body.work_order_id).toBe("work order-1");
    expect(firstRunRequest.key).not.toBe(secondRunRequest.key);
    expect(secondRunRequest.body.max_execution_review_cycles).toBe(7);
  });

  it("starts a fresh run request after an HTTP 400 rejection even when retryable metadata is true", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(400, "proxy rejected the request", { retryable: true, correlationId: "api.proxy" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Title"), "Proxy rejection work order");
    await user.type(screen.getByLabelText("Work Brief"), "Proxy rejection body");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await screen.findByText(/Kingdom rejected the run start request/i);

    expect(screen.getByRole("button", { name: "Start Run" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Start New Work Order" })).toBeEnabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeEnabled();

    await user.click(screen.getByRole("button", { name: "Start Run" }));
    await waitFor(() => expect(navigate).toHaveBeenCalled());

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { key: string };
    expect(firstRunRequest.key).not.toBe(secondRunRequest.key);
  });

  it("replays the same work order request after an ambiguous work order creation failure", async () => {
    const user = userEvent.setup();
    createWorkOrderMutate
      .mockRejectedValueOnce(new Error("connection lost"))
      .mockResolvedValueOnce(makeWorkOrderResult({ work_order_id: "work-order-replayed" }));
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Title"), "Ambiguous work order failure");
    await user.type(screen.getByLabelText("Work Brief"), "Recover the work order create request");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await screen.findByText(/may have accepted this work order, but the connection dropped before the response arrived/i);
    expect(navigate).not.toHaveBeenCalled();

    expect(createWorkOrderMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Resolve Work Order Request" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Discard Recovery" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("Title")).toBeDisabled();
    expect(screen.getByLabelText("Work Brief")).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Resolve Work Order Request" }));
    await waitFor(() => expect(navigate).toHaveBeenCalled());

    expect(createWorkOrderMutate).toHaveBeenCalledTimes(2);
    expect(createRunMutate).toHaveBeenCalledTimes(1);
    const firstWorkOrderRequest = createWorkOrderMutate.mock.calls[0][0] as { body: { work_order_title: string; work_order_text: string }; key: string };
    const secondWorkOrderRequest = createWorkOrderMutate.mock.calls[1][0] as { body: { work_order_title: string; work_order_text: string }; key: string };
    expect(firstWorkOrderRequest.body).toEqual(secondWorkOrderRequest.body);
    expect(firstWorkOrderRequest.key).toBe(secondWorkOrderRequest.key);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { work_order_id: "work-order-replayed" } });
  });

  it("replays the same run request after an ambiguous run-start failure", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(new Error("connection lost"))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Title"), "Ambiguous failure work order");
    await user.type(screen.getByLabelText("Work Brief"), "Ambiguous failure body");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await screen.findByText(/connection dropped before Kingdom could confirm whether the run started/i);
    expect(navigate).not.toHaveBeenCalled();

    expect(createWorkOrderMutate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Retry Run Start" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Discard Recovery" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("Title")).toBeDisabled();
    expect(screen.getByLabelText("Work Brief")).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Retry Run Start" }));
    await waitFor(() =>
      expect(navigate).toHaveBeenCalledWith({
        to: "/runs/$runId",
        params: { runId: "run-2" },
        replace: true,
      }),
    );

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { body: { work_order_id: string }; key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { body: { work_order_id: string }; key: string };
    expect(firstRunRequest.body.work_order_id).toBe("work order-1");
    expect(secondRunRequest.body.work_order_id).toBe("work order-1");
    expect(firstRunRequest.key).toBe(secondRunRequest.key);
  });

  it("allows only same-key recovery while run start is pending", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(409, "request is still pending", { code: "idempotency_pending", retryable: true, correlationId: "idempotency-middleware" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Title"), "Pending run work order");
    await user.type(screen.getByLabelText("Work Brief"), "Pending run body");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await screen.findByText(/run start remains pending/i);
    expect(navigate).not.toHaveBeenCalled();
    expect(screen.queryByRole("button", { name: "Discard Recovery" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retry Run Start" }));
    await waitFor(() => expect(navigate).toHaveBeenCalled());

    const firstRunRequest = createRunMutate.mock.calls[0][0] as { key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { key: string };
    expect(secondRunRequest.key).toBe(firstRunRequest.key);
  });

  it("keeps an ambiguous dispatch guarded across remount without creating another work order", async () => {
    const user = userEvent.setup();
    createRunMutate.mockRejectedValueOnce(new Error("connection lost"));
    const view = renderWithReviewOptions(<IssueWorkOrderPage />);
    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));
    await screen.findByText(/connection dropped before Kingdom could confirm whether the run started/i);
    expect(navigate).not.toHaveBeenCalled();
    const originalRun = createRunMutate.mock.calls[0][0];
    expect(screen.queryByRole("button", { name: "Discard Recovery" })).not.toBeInTheDocument();
    view.unmount();
    renderWithReviewOptions(<IssueWorkOrderPage />);
    expect(screen.getByLabelText("Title")).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Retry Run Start" }));
    await waitFor(() => expect(navigate).toHaveBeenCalled());
    expect(createWorkOrderMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[1][0]).toEqual(originalRun);
  });

  it("uses a fresh run request after a definitive retryable HTTP failure", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(503, "provider model catalog unavailable", { code: "internal_error", retryable: true, correlationId: "api.create_run" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Title"), "Ambiguous HTTP run failure");
    await user.type(screen.getByLabelText("Work Brief"), "Recover the original run start");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    await screen.findByText(/Kingdom rejected the run start request before it was accepted/i);

    expect(createWorkOrderMutate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Start Run" })).toBeEnabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeEnabled();

    await user.click(screen.getByRole("button", { name: "Start Run" }));
    await waitFor(() => expect(navigate).toHaveBeenCalled());

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { body: { work_order_id: string }; key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { body: { work_order_id: string }; key: string };
    expect(firstRunRequest.body.work_order_id).toBe("work order-1");
    expect(secondRunRequest.body.work_order_id).toBe("work order-1");
    expect(firstRunRequest.key).not.toBe(secondRunRequest.key);
  });

  it("renders a git worktree toggle with repository guidance", () => {
    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toBeEnabled();
    expect(screen.queryByText("Git worktree is available for this project root.")).not.toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toHaveAccessibleDescription(/separate Git working copy/);
  });

  it("disables the git worktree toggle outside a git repository", () => {
    mockWorkspaceCapabilities = {
      project_root: "/tmp/project",
      default_publish_mode: "direct",
      git_repo: false,
      git_worktree_available: false,
      git_worktree_reason: "project root is not a valid git working tree",
    };

    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toBeDisabled();
    expect(screen.getByText("project root is not a valid git working tree")).toBeInTheDocument();
  });

  it("submits git_worktree publish mode when the toggle is enabled", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("switch", { name: "Use Git Worktree" }));
    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Title"), "Work Order Worktree");
    await user.type(screen.getByLabelText("Work Brief"), "Publish in worktree");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { publish_mode: "git_worktree" } });
  });

  it("disables cycle inputs and shows warning when run-until-all-approve is enabled", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("switch", { name: "Run Until All Approve" }));

    expect(screen.getByText("Uses more tokens: review and validation keep running until they pass.")).toBeInTheDocument();
    expect(screen.getByLabelText("Max Review Cycles")).toBeDisabled();
    expect(screen.getByLabelText("Max Validation Cycles")).toBeDisabled();
  });

  it("switches to skip-review workflow and greys out review controls", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("switch", { name: "Run Until All Approve" }));
    await user.click(screen.getByRole("switch", { name: "Skip Reviews and Validations" }));

    expect(screen.getByText("Review and validation are skipped. Builder output proceeds directly to completion for this run.")).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Run Until All Approve" })).toHaveAttribute("aria-checked", "false");
    expect(screen.getByRole("switch", { name: "Run Until All Approve" })).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeDisabled();
    expect(screen.getByLabelText("Max Validation Cycles")).toBeDisabled();
  });

	it("preserves zero-valued cycle inputs on blur and explains immediate auto-success", async () => {
		const user = userEvent.setup();
		renderWithReviewOptions(<IssueWorkOrderPage />);

		const reviewInput = screen.getByLabelText("Max Review Cycles");
		await user.clear(reviewInput);
		await user.type(reviewInput, "0");
		await user.tab();
		expect(reviewInput).toHaveValue(0);

		const validationInput = screen.getByLabelText("Max Validation Cycles");
		await user.clear(validationInput);
		await user.type(validationInput, "0");
		await user.tab();
		expect(validationInput).toHaveValue(0);
		expect(screen.getByText("Set either cap to 0 to auto-succeed the first ready review or validation wave without provider dispatch.")).toBeInTheDocument();
	});

	it("submits zero-valued cycle inputs to the runtime unchanged", async () => {
		const user = userEvent.setup();
		renderWithReviewOptions(<IssueWorkOrderPage />);

		await selectGlobalModel(user);
		await user.clear(screen.getByLabelText("Title"));
		await user.type(screen.getByLabelText("Title"), "Zero cap work order");
		await user.clear(screen.getByLabelText("Work Brief"));
		await user.type(screen.getByLabelText("Work Brief"), "Use immediate review auto-success");

		const reviewInput = screen.getByLabelText("Max Review Cycles");
		await user.clear(reviewInput);
		await user.type(reviewInput, "0");
		await user.tab();

		const validationInput = screen.getByLabelText("Max Validation Cycles");
		await user.clear(validationInput);
		await user.type(validationInput, "0");
		await user.tab();

		await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

		expect(createRunMutate).toHaveBeenCalledTimes(1);
		expect(createRunMutate.mock.calls[0][0]).toMatchObject({
			body: {
				max_execution_review_cycles: 0,
				max_validation_rework_cycles: 0,
			},
		});
	});

  it("defaults bounded cycle inputs to five", async () => {
    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.getByLabelText("Max Review Cycles")).toHaveValue(5);
    expect(screen.getByLabelText("Max Validation Cycles")).toHaveValue(5);
  });

  it("keeps cycle inputs editable without forcing leading zeroes", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    const reviewInput = screen.getByLabelText("Max Review Cycles");
    await user.clear(reviewInput);
    expect(reviewInput).toHaveValue(null);
    await user.type(reviewInput, "10");
    expect(reviewInput).toHaveValue(10);

    const validationInput = screen.getByLabelText("Max Validation Cycles");
    await user.clear(validationInput);
    expect(validationInput).toHaveValue(null);
    await user.type(validationInput, "12");
    expect(validationInput).toHaveValue(12);
  });

  it("keeps approval and delivery separate from optional review policy", () => {
    renderWithReviewOptions(<IssueWorkOrderPage />);
    for (const label of ["Require Human Approval", "Use Git Worktree"]) {
      expect(screen.getByRole("switch", { name: label }).closest("details")).toBeNull();
    }
    for (const label of ["Skip Reviews and Validations", "Run Until All Approve"]) {
      expect(screen.getByRole("switch", { name: label }).closest("details")).toBe(screen.getByText("Review Options").closest("details"));
    }
    expect(screen.getByLabelText("Max Review Cycles")).toBeVisible();
    expect(screen.getByLabelText("Max Validation Cycles")).toBeVisible();
    expect(screen.queryByLabelText("Workflow ID")).not.toBeInTheDocument();
  });

  it("explains consequential choices inline without extra tooltip controls", () => {
    renderWithReviewOptions(<IssueWorkOrderPage />);
    expect(screen.getByRole("switch", { name: "Require Human Approval" })).toHaveAccessibleDescription("Pause for your decision before implementation and final completion.");
    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toHaveAccessibleDescription("Keep changes in a separate Git working copy. When off, publish into this project.");
    expect(screen.queryByLabelText("Require Human Approval definition")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Use Git Worktree definition")).not.toBeInTheDocument();
  });

  it("renders model controls outside advanced options with provider-driven thinking effort", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.queryByLabelText("Model Search")).not.toBeInTheDocument();
    expect(screen.getByText("Not available for this model")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Role Models/i }));
    expect(screen.getByRole("switch", { name: "Use for All Agents" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("combobox", { name: /Global Model/ })).toHaveTextContent("Select model");
    expect(screen.getByRole("button", { name: /Role Models/i })).toBeInTheDocument();
  });

  it("shows provider model catalog unavailable state instead of an empty dropdown", () => {
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "unavailable",
        items: [],
        failure_reason: "timeout",
        failure_message: "provider model catalog unavailable",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.getByText("provider model catalog unavailable")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Global Model/ })).toBeDisabled();
  });

  it("says nothing about a stale catalog while its models are still selectable", () => {
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "stale",
        items: [
          {
            id: "github-copilot/gpt-5.3-codex",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.3-Codex",
            thinking_effort_default: "medium",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
        failure_reason: "service_unavailable",
        failure_message: "Showing the models Kingdom already had.",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderWithReviewOptions(<IssueWorkOrderPage />);

    // A refresh Kingdom failed on its own costs the operator nothing here, and
    // recovers on the next poll. Interrupting them over it would report a
    // background chore as a problem.
    expect(screen.queryByText("Showing the models Kingdom already had.")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry" })).not.toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Global Model/ })).not.toBeDisabled();
  });

  it("stays silent when a background refresh fails but cached models remain", () => {
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "fresh",
        items: [
          {
            id: "github-copilot/gpt-5.3-codex",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.3-Codex",
            thinking_effort_default: "medium",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
      },
      isLoading: false,
      error: makeApiError(503, "provider model catalog refresh failed"),
      refetch: vi.fn(),
    });

    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.queryByText("provider model catalog refresh failed")).not.toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Global Model/ })).not.toBeDisabled();
  });

  it("uses a live refresh when retrying an unreachable catalog", async () => {
    const user = userEvent.setup();
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "unavailable",
        items: [],
        failure_reason: "timeout",
        failure_message: "provider model catalog unavailable",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderWithReviewOptions(<IssueWorkOrderPage />);
    await user.click(screen.getByRole("button", { name: "Retry" }));

    expect(refreshProviderModelsMutate).toHaveBeenCalledTimes(1);
  });

  it("shows a retry failure in place of the copy it replaced", async () => {
    const user = userEvent.setup();
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "unavailable",
        items: [],
        failure_reason: "timeout",
        failure_message: "provider model catalog unavailable",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      dataUpdatedAt: 1,
    });
    refreshProviderModelsMutate.mockImplementation((_vars, callbacks) => {
      callbacks?.onError?.(new Error("live refresh failed"), undefined, undefined);
    });

    renderWithReviewOptions(<IssueWorkOrderPage />);
    await user.click(screen.getByRole("button", { name: "Retry" }));

    expect(await screen.findByText("live refresh failed")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Global Model/ })).toBeDisabled();
  });

  it("disables work order submission until a global model is selected", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.type(screen.getByLabelText("Title"), "Work Order A");
    await user.type(screen.getByLabelText("Work Brief"), "Do the thing");

    expect(screen.getByRole("button", { name: "Issue Work Order" })).toBeDisabled();

    fireEvent.submit(screen.getByRole("button", { name: "Issue Work Order" }).closest("form") as HTMLFormElement);

    expect(createWorkOrderMutate).not.toHaveBeenCalled();
    expect(createRunMutate).not.toHaveBeenCalled();
  });

  it("supports searchable model combobox filtering", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("combobox", { name: /Global Model/ }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");

    expect(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "GitHub Copilot - GPT-5.3-Codex" })).not.toBeInTheDocument();
  });

  it("selects the active searchable model option with Enter after arrow navigation", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    const globalModel = screen.getByRole("combobox", { name: /Global Model/ });
    await user.click(globalModel);
    await user.keyboard("{ArrowDown}{ArrowDown}{Enter}");

    expect(globalModel).toHaveTextContent("OpenCode - GPT-5 Nano");
  });

  it("hides thinking effort when the selected model does not support it", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("combobox", { name: /Global Model/ }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");
    await user.click(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" }));

    expect(screen.getByText("Not available for this model")).toBeInTheDocument();
  });

  it("defaults all agent selectors to global model and preserves non-diverged selections", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: /Role Models/i }));

    expect(screen.getByRole("combobox", { name: "Governance Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
    expect(screen.getByRole("combobox", { name: "Reviewer Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
    expect(screen.getAllByLabelText("Thinking Effort")[1]).toHaveTextContent("Medium");

    await user.click(screen.getByRole("combobox", { name: "Reviewer Model" }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");
    await user.click(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" }));

    expect(screen.getByRole("switch", { name: "Use for All Agents" })).toHaveAttribute("aria-checked", "false");
    expect(screen.getByRole("combobox", { name: "Governance Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
    expect(screen.getByRole("combobox", { name: "Reviewer Model" })).toHaveTextContent("OpenCode - GPT-5 Nano");
    expect(screen.getByText("Not available for this model")).toBeInTheDocument();

    await user.click(screen.getByRole("switch", { name: "Use for All Agents" }));

    expect(screen.getByRole("switch", { name: "Use for All Agents" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("combobox", { name: "Reviewer Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
  });

  it("submits global and role-specific model selections", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.click(screen.getByLabelText("Thinking Effort"));
    await user.click(screen.getByRole("option", { name: "High" }));
    await user.click(screen.getByRole("button", { name: /Role Models/i }));
    await user.click(screen.getByRole("combobox", { name: "Reviewer Model" }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");
    await user.click(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" }));

    await user.type(screen.getByLabelText("Title"), "Work Order C");
    await user.type(screen.getByLabelText("Work Brief"), "Use model overrides");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({
      body: {
        model_selection: {
          global: {
            model: "github-copilot/gpt-5.3-codex",
            thinking_effort: "high",
          },
          role_overrides: {
            "reviewer": {
              model: "opencode/gpt-5-nano",
            },
          },
        },
      },
    });
  });

  it("submits exact hosted connection revisions for global and role-specific selections", async () => {
    const user = userEvent.setup();
    const meta = document.createElement("meta"); meta.name = "kingdom-runtime-mode"; meta.content = "hosted-project"; document.head.append(meta);
    const catalog = mockUseProviderModels();
    mockUseProviderModels.mockReturnValue({ ...catalog, data: { ...catalog.data, items: catalog.data.items.map((item: {id:string}, index:number) => ({ ...item, id: JSON.stringify([`connection-${index}`, index+2, item.id]), canonical_model: item.id, connection: {id:`connection-${index}`,revision:index+2} })) } });
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.click(screen.getByLabelText("Thinking Effort"));
    await user.click(screen.getByRole("option", { name: "High" }));
    await user.click(screen.getByRole("button", { name: /Role Models/i }));
    await user.click(screen.getByRole("combobox", { name: "Reviewer Model" }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");
    await user.click(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" }));

    await user.type(screen.getByLabelText("Title"), "Work Order C");
    await user.type(screen.getByLabelText("Work Brief"), "Use model overrides");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({
      body: {
        model_selection: {
          global: {
            model: "github-copilot/gpt-5.3-codex",
            connection: {id:"connection-0",revision:2},
            thinking_effort: "high",
          },
          role_overrides: {
            "reviewer": {
              model: "opencode/gpt-5-nano",
              connection: {id:"connection-1",revision:3},
            },
          },
        },
      },
    });
    meta.remove();
  });

  it("sends lineage metadata on work order create while preserving run configuration", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Title"));
    await user.type(screen.getByLabelText("Title"), "Chosen work order title");
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Work Brief"), "Chosen work order text");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    expect(createWorkOrderMutate).toHaveBeenCalledWith(expect.objectContaining({
      body: expect.objectContaining({
        work_order_title: "Chosen work order title",
        work_order_text: "Chosen work order text",
        planning_lineage: {
          planning_session_id: "planning-session-1",
          planning_draft_id: "planning-draft-1",
        },
      }),
    }));
    const workOrderRequest = createWorkOrderMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(workOrderRequest.body).not.toHaveProperty("external_source_snapshot_id");
		expect(workOrderRequest.body.planning_lineage).not.toHaveProperty("planning_context_snapshot_id");
	    expect(createRunMutate).toHaveBeenCalledWith(expect.objectContaining({
	      body: expect.objectContaining({
	        autonomous_approvals: true,
	        publish_mode: "direct",
	      }),
	    }));
	    const request = createRunMutate.mock.calls[0][0] as { body: Record<string, unknown> };
	    expect(request.body).not.toHaveProperty("workflow_id");
	    expect(request.body).not.toHaveProperty("skip_reviews_and_validations");
	  });

	  it("submits the skip-review intent when the toggle is enabled", async () => {
	    const user = userEvent.setup();
	    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("switch", { name: "Skip Reviews and Validations" }));
    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Title"), "Skip quality gates");
    await user.type(screen.getByLabelText("Work Brief"), "Go straight to closure");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

	    expect(createRunMutate).toHaveBeenCalledTimes(1);
	    const request = createRunMutate.mock.calls[0][0] as { body: Record<string, unknown> };
	    expect(request).toMatchObject({
	      body: {
	        skip_reviews_and_validations: true,
	      },
	    });
	    expect(request.body).not.toHaveProperty("workflow_id");
	    expect(request.body).not.toHaveProperty("run_until_all_approve");
	    expect(request.body).not.toHaveProperty("max_execution_review_cycles");
	    expect(request.body).not.toHaveProperty("max_validation_rework_cycles");
	  });

  it("reset clears planning lineage before issuing a fresh work order", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Reset" }));

    expect(screen.getByRole("button", { name: "Work Source" })).toHaveTextContent("Write a Brief");
    expect(screen.queryByRole("button", { name: "From Plan" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("Title")).toHaveValue("");
    expect(screen.getByLabelText("Work Brief")).toHaveValue("");

    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Title"), "Fresh work order title");
    await user.type(screen.getByLabelText("Work Brief"), "Fresh work order text");
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    const workOrderRequest = createWorkOrderMutate.mock.calls[0]?.[0] as {
      body: {
        work_order_title: string;
        work_order_text: string;
        metadata?: Record<string, string>;
      };
    };

    expect(createWorkOrderMutate).toHaveBeenCalledTimes(1);
    expect(workOrderRequest).toMatchObject({
      body: {
        work_order_title: "Fresh work order title",
        work_order_text: "Fresh work order text",
      },
    });
    expect(workOrderRequest.body.metadata).toBeUndefined();
    expect(workOrderRequest.body).not.toHaveProperty("planning_lineage");
    expect(workOrderRequest.body).not.toHaveProperty("external_source_snapshot_id");
  });

  it("renders agent override roles in required execution order", async () => {
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

		await user.click(screen.getByRole("button", { name: /Role Models/i }));

		const roleComboboxIDs = screen
			.getAllByRole("combobox")
			.map((item) => item.getAttribute("id"))
			.filter((id) => id && id !== "global-model") as string[];
		expect(roleComboboxIDs).toEqual([
			"role-model-governance",
			"role-model-architecture-advisor",
			"role-model-risk-advisor",
			"role-model-systems-advisor",
			"role-model-usability-advisor",
			"role-model-coordinator",
			"role-model-plan-author",
			"role-model-builder",
			"role-model-reviewer",
			"role-model-qa-validator",
			"role-model-security-validator",
			"role-model-resilience-validator",
		]);

    const groupHeadings = screen.getAllByRole("heading", { level: 4 }).map((item) => item.textContent?.trim());
    expect(groupHeadings).toEqual(["Governance", "Advisors", "Coordinator", "Plan Author", "Builder", "Reviewers", "Validators"]);
  });

  it("promotes refreshed provenance atomically with any applied fields", async () => {
    let captureCount = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) {
        captureCount += 1;
        return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: captureCount === 1 ? "snapshot-a" : "snapshot-b" }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      const refreshed = url.includes("snapshot-b");
      if (url.endsWith("/work-order-text")) {
        return new Response(JSON.stringify({ markdown: refreshed ? "Body B" : "Body A" }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      return new Response(JSON.stringify({ snapshot_id: refreshed ? "snapshot-b" : "snapshot-a", source_id: "source-1", issue: { title: refreshed ? "Issue B" : "Issue A", body: refreshed ? "Body B" : "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: refreshed ? "2026-08-03T00:00:00Z" : "2026-08-02T00:00:00Z" }, profile: { include_comments: false }, provider_updated_at: "2026-08-03T00:00:00Z", fetched_at: "2026-08-03T00:00:00Z", comment_count: 0, fetched_comment_count: 0, retained_comment_count: 0, retained_comment_bytes: 0, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);
    await user.click(screen.getByRole("button", { name: "Work Source" }));
    await user.click(screen.getByRole("option", { name: "GitHub Issue" }));
    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));
    expect(await screen.findByDisplayValue("Body A")).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Work Brief"));
    await user.type(screen.getByLabelText("Work Brief"), "Operator edit");
    await user.click(screen.getByRole("button", { name: /check for updates/i }));
    expect(await screen.findByText(/an update is available/i)).toBeInTheDocument();
    expect(screen.getByLabelText("GitHub source comparison")).toHaveAttribute("data-baseline", "snapshot-a");
    expect(screen.getByLabelText("Work Brief")).toHaveValue("Operator edit");
    await user.click(screen.getByRole("button", { name: "Use Refreshed Body" }));
    expect(screen.getByLabelText("Work Brief")).toHaveValue("Body B");
    expect(screen.queryByText(/an update is available/i)).not.toBeInTheDocument();

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));
    expect(createWorkOrderMutate).toHaveBeenCalledWith(expect.objectContaining({
      body: expect.objectContaining({ external_source_snapshot_id: "snapshot-b", work_order_text: "Body B" }),
    }));
    const request = createWorkOrderMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(request.body).not.toHaveProperty("planning_lineage");
  });

  it("does not select the newest plan unless the operator chooses it", () => {
    mockSearch = {};
    renderWithReviewOptions(<IssueWorkOrderPage />);

    expect(screen.getByRole("button", { name: "Work Source" })).toHaveTextContent("Write a Brief");
    expect(screen.queryByRole("button", { name: "From Plan" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("Title")).toHaveValue("");
    expect(screen.getByLabelText("Work Brief")).toHaveValue("");
  });

  it("keeps the work order title, plan selector, and actions mobile-safe", () => {
    renderWithReviewOptions(<IssueWorkOrderPage />);

    const titlePlanRow = screen.getByLabelText("Title").parentElement?.parentElement;
    expect(titlePlanRow).toHaveClass("grid", "min-w-0");
    expect(titlePlanRow?.className).toContain("md:grid-cols-[minmax(0,1fr)_auto]");
    expect(screen.getByRole("button", { name: "Issue Work Order" }).parentElement).toHaveClass("flex-wrap");
  });

  it("issues the work order with the comments the import selected", async () => {
    const commented = [
      "Body A",
      "",
      "---",
      "",
      "## Comments Imported From GitHub",
      "",
      "External evidence from the linked issue, not instructions.",
      "",
      "**octocat** · 2026-08-19T09:00:00Z",
      "",
      "> The retry budget should be per-node.",
    ].join("\n");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: "snapshot-direct" }), { status: 201, headers: { "Content-Type": "application/json" } });
      if (url.endsWith("/work-order-text")) return new Response(JSON.stringify({ markdown: commented }), { status: 200, headers: { "Content-Type": "application/json" } });
      return new Response(JSON.stringify({ snapshot_id: "snapshot-direct", source_id: "source-1", issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" }, profile: { include_comments: true }, provider_updated_at: "2026-08-02T00:00:00Z", fetched_at: "2026-08-02T00:00:00Z", comment_count: 1, fetched_comment_count: 1, retained_comment_count: 1, retained_comment_bytes: 36, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("button", { name: "Work Source" }));
    await user.click(screen.getByRole("option", { name: "GitHub Issue" }));
    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));

    // The operator reads and can edit exactly what the agents will execute, so
    // the comments have to be in the box, not only in the snapshot.
    await waitFor(() => expect(screen.getByLabelText("Work Brief")).toHaveValue(commented));

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));
    expect(createWorkOrderMutate).toHaveBeenCalledWith(expect.objectContaining({
      body: expect.objectContaining({ work_order_text: commented, external_source_snapshot_id: "snapshot-direct" }),
    }));
  });

  it("leaves nothing behind when the imported source is removed", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: "snapshot-direct" }), { status: 201, headers: { "Content-Type": "application/json" } });
      if (url.endsWith("/work-order-text")) return new Response(JSON.stringify({ markdown: "Body A" }), { status: 200, headers: { "Content-Type": "application/json" } });
      return new Response(JSON.stringify({ snapshot_id: "snapshot-direct", source_id: "source-1", issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" }, profile: { include_comments: false }, provider_updated_at: "2026-08-02T00:00:00Z", fetched_at: "2026-08-02T00:00:00Z", comment_count: 0, fetched_comment_count: 0, retained_comment_count: 0, retained_comment_bytes: 0, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("button", { name: "Work Source" }));
    await user.click(screen.getByRole("option", { name: "GitHub Issue" }));
    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));
    expect(await screen.findByLabelText("Selected GitHub source")).toBeInTheDocument();
    // A freshly imported source has nothing to report about its own freshness.
    expect(screen.queryByText(/work order fields remain editable/i)).not.toBeInTheDocument();
    expect(screen.queryByText("Up to Date")).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByLabelText("Title")).toHaveValue("Issue A"));

    // Removing the source has to take the text it imported with it, or the
    // next work order silently ships content the operator believes they discarded.
    await user.click(screen.getByRole("button", { name: "Remove Source" }));
    expect(screen.queryByLabelText("Selected GitHub source")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Title")).toHaveValue("");
    expect(screen.getByLabelText("Work Brief")).toHaveValue("");
  });

  it("clears planning lineage when a direct GitHub issue is imported", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: "snapshot-direct" }), { status: 201, headers: { "Content-Type": "application/json" } });
      if (url.endsWith("/work-order-text")) return new Response(JSON.stringify({ markdown: "Body A" }), { status: 200, headers: { "Content-Type": "application/json" } });
      return new Response(JSON.stringify({ snapshot_id: "snapshot-direct", source_id: "source-1", issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" }, profile: { include_comments: false }, provider_updated_at: "2026-08-02T00:00:00Z", fetched_at: "2026-08-02T00:00:00Z", comment_count: 0, fetched_comment_count: 0, retained_comment_count: 0, retained_comment_bytes: 0, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("button", { name: "Work Source" }));
    await user.click(screen.getByRole("option", { name: "GitHub Issue" }));
    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));
    expect(screen.queryByRole("button", { name: "From Plan" })).not.toBeInTheDocument();
    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));

    const request = createWorkOrderMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(request.body).toMatchObject({ external_source_snapshot_id: "snapshot-direct" });
    expect(request.body).not.toHaveProperty("planning_lineage");
  });

  it("clears direct GitHub provenance when a plan is selected", async () => {
    mockSearch = {};
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: "snapshot-direct" }), { status: 201, headers: { "Content-Type": "application/json" } });
      if (url.endsWith("/work-order-text")) return new Response(JSON.stringify({ markdown: "Body A" }), { status: 200, headers: { "Content-Type": "application/json" } });
      return new Response(JSON.stringify({ snapshot_id: "snapshot-direct", source_id: "source-1", issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" }, profile: { include_comments: false }, provider_updated_at: "2026-08-02T00:00:00Z", fetched_at: "2026-08-02T00:00:00Z", comment_count: 0, fetched_comment_count: 0, retained_comment_count: 0, retained_comment_bytes: 0, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    renderWithReviewOptions(<IssueWorkOrderPage />);

    await user.click(screen.getByRole("button", { name: "Work Source" }));
    await user.click(screen.getByRole("option", { name: "GitHub Issue" }));
    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));
    expect(await screen.findByLabelText("Selected GitHub source")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Work Source" }));
    await user.click(screen.getByRole("option", { name: "Saved Plan" }));
    await user.click(screen.getByRole("button", { name: "From Plan" }));
    await user.click(screen.getByRole("option", { name: "Test session" }));
    await waitFor(() => expect(screen.getByLabelText("Title")).toHaveValue("Draft supplied title"));
    expect(screen.queryByLabelText("Selected GitHub source")).not.toBeInTheDocument();

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Work Order" }));
    const request = createWorkOrderMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(request.body).toMatchObject({ planning_lineage: { planning_session_id: "planning-session-1", planning_draft_id: "planning-draft-1" } });
    expect(request.body).not.toHaveProperty("external_source_snapshot_id");
  });
});

vi.mock("@/theme", async () => {
  const actual = await vi.importActual<typeof import("@/theme")>("@/theme");
  return { useThemeCopy: () => "Work Order", useTheme: () => ({ theme: actual.defaultTheme }), roleIdentity: (_theme: unknown, role: string) => ({ name: actual.canonicalRoleName(role) }) };
});
