import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { ApiError } from "@/api/client";
import { IssueDecreePage } from "./issue-decree-page";

const createDecreeMutate = vi.fn<(request: Record<string, unknown>) => Promise<Record<string, unknown>>>();
const createRunMutate = vi.fn<(request: Record<string, unknown>) => Promise<Record<string, unknown>>>();
let idempotencyKeyCounter = 0;
const mockUseProviderModels = vi.fn();
const refreshProviderModelsMutate = vi.fn();
let mockSearch: Record<string, string> = {};

function makeDecreeResult(overrides: Record<string, unknown> = {}) {
  return {
    decree_id: "decree-1",
    created_at: "2026-03-01T10:00:00.000Z",
    ...overrides,
  };
}

function makeRunResult(overrides: Record<string, unknown> = {}) {
  return {
    run_id: "run-1",
    workflow_id: "royal-decree-v1",
    decree_title: "test",
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
}));

vi.mock("@/features/connections/issue-picker", () => ({
  IssuePicker: ({ onSelect }: { onSelect: (selection: unknown) => Promise<void> }) => <button type="button" onClick={() => void onSelect({ intake: { connection_id: "conn-1", repository_id: "repo-1", issue_number: 7, include_comment_count: true, include_comments: false }, issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" } })}>Mock prefill issue</button>,
}));

vi.mock("@/features/connections/external-source-comparison", () => ({
  ExternalSourceComparison: ({ baselineId, candidateId }: { baselineId: string; candidateId: string }) => <div aria-label="GitHub source comparison" data-baseline={baselineId} data-candidate={candidateId}>Source comparison</div>,
}));

vi.mock("@/api/hooks", () => ({
  newIdempotencyKey: (scope: string) => `${scope}-test-${++idempotencyKeyCounter}`,
  useCreateDecree: () => ({ mutateAsync: createDecreeMutate, isPending: false, error: null, reset: vi.fn() }),
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
      decree_title: "Draft supplied title",
      decree_text: "Draft supplied text",
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
  useRuns: () => ({ data: { items: [] }, isLoading: false }),
  useRoles: () => ({
    data: {
      items: [
        { role_id: "king", display_name: "King", description: "", can_spawn: true, agent_model_selector_visible: true, agent_model_selector_section_id: "king", agent_model_selector_section_label: "King", agent_model_selector_section_order: 10, agent_model_selector_role_order: 10 },
        { role_id: "noble-delegate", display_name: "Noble Delegate", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 10 },
        { role_id: "clergy-delegate", display_name: "Clergy Delegate", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 20 },
        { role_id: "burgher-delegate", display_name: "Burgher Delegate", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 30 },
        { role_id: "peasant-delegate", display_name: "Peasant Delegate", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "advisors", agent_model_selector_section_label: "Advisors", agent_model_selector_section_order: 20, agent_model_selector_role_order: 40 },
        { role_id: "chancellor", display_name: "Chancellor", description: "", can_spawn: true, agent_model_selector_visible: true, agent_model_selector_section_id: "chancellor", agent_model_selector_section_label: "Chancellor", agent_model_selector_section_order: 30, agent_model_selector_role_order: 10 },
        { role_id: "council-member", display_name: "Council Member", description: "", can_spawn: true, agent_model_selector_visible: true, agent_model_selector_section_id: "council-member", agent_model_selector_section_label: "Council Member", agent_model_selector_section_order: 40, agent_model_selector_role_order: 10 },
        { role_id: "builder-berserker", display_name: "Berserker", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "builder", agent_model_selector_section_label: "Builder", agent_model_selector_section_order: 50, agent_model_selector_role_order: 10 },
        { role_id: "reviewer-shieldmaiden", display_name: "Shieldmaiden Reviewer", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "reviewers", agent_model_selector_section_label: "Reviewers", agent_model_selector_section_order: 60, agent_model_selector_role_order: 10 },
        { role_id: "intelligence-qa-scout", display_name: "Scout", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "validators", agent_model_selector_section_label: "Validators", agent_model_selector_section_order: 70, agent_model_selector_role_order: 10 },
        { role_id: "intelligence-nightraven", display_name: "Nightraven", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "validators", agent_model_selector_section_label: "Validators", agent_model_selector_section_order: 70, agent_model_selector_role_order: 20 },
        { role_id: "intelligence-loki-chaos", display_name: "Loki Servant", description: "", can_spawn: false, agent_model_selector_visible: true, agent_model_selector_section_id: "validators", agent_model_selector_section_label: "Validators", agent_model_selector_section_order: 70, agent_model_selector_role_order: 30 },
        { role_id: "allfather", display_name: "Allfather", description: "", can_spawn: false, agent_model_selector_visible: false, agent_model_selector_section_id: "operators", agent_model_selector_section_label: "Operators", agent_model_selector_section_order: 0, agent_model_selector_role_order: 10 },
      ],
    },
  }),
}));

describe("issue decree page", () => {
  afterEach(() => vi.unstubAllGlobals());
  beforeEach(() => {
    createDecreeMutate.mockReset();
    createRunMutate.mockReset();
    createDecreeMutate.mockResolvedValue(makeDecreeResult());
    createRunMutate.mockResolvedValue(makeRunResult());
    idempotencyKeyCounter = 0;
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

  it("renders issue decree content", () => {
    render(<IssueDecreePage />);
    expect(screen.getByRole("heading", { name: "Issue Decree" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Issue Decree" })).toBeInTheDocument();
  });

  it("prefills decree title and text from selected planning draft", () => {
    render(<IssueDecreePage />);

    expect(screen.getByLabelText("Decree Title")).toHaveValue("Draft supplied title");
    expect(screen.getByLabelText("Decree Text")).toHaveValue("Draft supplied text");
  });

  it("forces autonomous approvals when allfather toggle is off", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Decree Title"), "Decree A");
    await user.type(screen.getByLabelText("Decree Text"), "Do the thing");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { autonomous_approvals: true, publish_mode: "direct" } });
  });

  it("forces manual approvals when allfather toggle is on", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("switch", { name: /Allfather \(Human\) in the Loop/i }));
    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Decree Title"), "Decree B");
    await user.type(screen.getByLabelText("Decree Text"), "Do the other thing");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { autonomous_approvals: false, publish_mode: "direct" } });
  });

  it("ignores repeated submit attempts while decree creation is already in flight", async () => {
    const pendingDecree = deferred<Record<string, unknown>>();
    createDecreeMutate.mockImplementationOnce(() => pendingDecree.promise);
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    const form = screen.getByRole("button", { name: "Issue Decree" }).closest("form") as HTMLFormElement;

    fireEvent.submit(form);
    fireEvent.submit(form);

    expect(createDecreeMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate).not.toHaveBeenCalled();

    pendingDecree.resolve(makeDecreeResult());
    await waitFor(() => expect(createRunMutate).toHaveBeenCalledTimes(1));
  });

  it("clears decree inputs after a successful dispatch so another decree can be issued immediately", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Title"), "First decree");
    await user.type(screen.getByLabelText("Decree Text"), "First decree body");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText("Decree Dispatched");

    expect(screen.getByLabelText("Decree Title")).toHaveValue("");
    expect(screen.getByLabelText("Decree Text")).toHaveValue("");
    expect(screen.getByRole("button", { name: "Issue Decree" })).toBeDisabled();

    await user.type(screen.getByLabelText("Decree Title"), "Second decree");
    await user.type(screen.getByLabelText("Decree Text"), "Second decree body");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    expect(createDecreeMutate).toHaveBeenCalledTimes(2);
    expect(createRunMutate).toHaveBeenCalledTimes(2);
  });

  it("starts a fresh run request after a known run-start rejection", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(400, "invalid run settings", { code: "policy_violation", retryable: false, correlationId: "api.create_run" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Title"), "Known failure decree");
    await user.type(screen.getByLabelText("Decree Text"), "Known failure body");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText(/Kingdom rejected the run start request/i);

    expect(createDecreeMutate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Start Run" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Start New Decree" })).toBeEnabled();
    expect(screen.getByLabelText("Decree Title")).toBeDisabled();
    expect(screen.getByLabelText("Decree Text")).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeEnabled();

    await user.clear(screen.getByLabelText("Max Review Cycles"));
    await user.type(screen.getByLabelText("Max Review Cycles"), "7");
    await user.click(screen.getByRole("button", { name: "Start Run" }));

    await screen.findByText("Decree Dispatched");

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { body: { decree_id: string; max_execution_review_cycles: number }; key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { body: { decree_id: string; max_execution_review_cycles: number }; key: string };
    expect(firstRunRequest.body.decree_id).toBe("decree-1");
    expect(secondRunRequest.body.decree_id).toBe("decree-1");
    expect(firstRunRequest.key).not.toBe(secondRunRequest.key);
    expect(secondRunRequest.body.max_execution_review_cycles).toBe(7);
  });

  it("starts a fresh run request after an HTTP 400 rejection even when retryable metadata is true", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(400, "proxy rejected the request", { retryable: true, correlationId: "api.proxy" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Title"), "Proxy rejection decree");
    await user.type(screen.getByLabelText("Decree Text"), "Proxy rejection body");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText(/Kingdom rejected the run start request/i);

    expect(screen.getByRole("button", { name: "Start Run" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Start New Decree" })).toBeEnabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeEnabled();

    await user.click(screen.getByRole("button", { name: "Start Run" }));
    await screen.findByText("Decree Dispatched");

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { key: string };
    expect(firstRunRequest.key).not.toBe(secondRunRequest.key);
  });

  it("replays the same decree request after an ambiguous decree creation failure", async () => {
    const user = userEvent.setup();
    createDecreeMutate
      .mockRejectedValueOnce(new Error("connection lost"))
      .mockResolvedValueOnce(makeDecreeResult({ decree_id: "decree-replayed" }));
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Title"), "Ambiguous decree failure");
    await user.type(screen.getByLabelText("Decree Text"), "Recover the decree create request");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText(/may have accepted this decree, but the connection dropped before the response arrived/i);

    expect(createDecreeMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Retry Decree" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Discard Recovery" })).toBeEnabled();
    expect(screen.getByLabelText("Decree Title")).toBeDisabled();
    expect(screen.getByLabelText("Decree Text")).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeEnabled();

    await user.click(screen.getByRole("button", { name: "Retry Decree" }));
    await screen.findByText("Decree Dispatched");

    expect(createDecreeMutate).toHaveBeenCalledTimes(2);
    expect(createRunMutate).toHaveBeenCalledTimes(1);
    const firstDecreeRequest = createDecreeMutate.mock.calls[0][0] as { body: { decree_title: string; decree_text: string }; key: string };
    const secondDecreeRequest = createDecreeMutate.mock.calls[1][0] as { body: { decree_title: string; decree_text: string }; key: string };
    expect(firstDecreeRequest.body).toEqual(secondDecreeRequest.body);
    expect(firstDecreeRequest.key).toBe(secondDecreeRequest.key);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { decree_id: "decree-replayed" } });
  });

  it("replays the same run request after an ambiguous run-start failure", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(new Error("connection lost"))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Title"), "Ambiguous failure decree");
    await user.type(screen.getByLabelText("Decree Text"), "Ambiguous failure body");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText(/connection dropped before Kingdom could confirm whether the run started/i);

    expect(createDecreeMutate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Retry Run Start" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Discard Recovery" })).toBeEnabled();
    expect(screen.getByLabelText("Decree Title")).toBeDisabled();
    expect(screen.getByLabelText("Decree Text")).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Retry Run Start" }));
    await screen.findByText("Decree Dispatched");

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { body: { decree_id: string }; key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { body: { decree_id: string }; key: string };
    expect(firstRunRequest.body.decree_id).toBe("decree-1");
    expect(secondRunRequest.body.decree_id).toBe("decree-1");
    expect(firstRunRequest.key).toBe(secondRunRequest.key);
  });

  it("allows only same-key recovery while run start is pending", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(409, "request is still pending", { code: "idempotency_pending", retryable: true, correlationId: "idempotency-middleware" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Decree Title"), "Pending run decree");
    await user.type(screen.getByLabelText("Decree Text"), "Pending run body");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText(/run start remains pending/i);
    expect(screen.queryByRole("button", { name: "Discard Recovery" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retry Run Start" }));
    await screen.findByText("Decree Dispatched");

    const firstRunRequest = createRunMutate.mock.calls[0][0] as { key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { key: string };
    expect(secondRunRequest.key).toBe(firstRunRequest.key);
  });

  it("lets operators explicitly discard ambiguous run recovery before starting over", async () => {
    const user = userEvent.setup();
    createRunMutate.mockRejectedValueOnce(new Error("connection lost"));
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Title"), "Discard recovery decree");
    await user.type(screen.getByLabelText("Decree Text"), "Discard recovery body");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText(/connection dropped before Kingdom could confirm whether the run started/i);

    await user.click(screen.getByRole("button", { name: "Discard Recovery" }));

    expect(screen.queryByText(/connection dropped before Kingdom could confirm whether the run started/i)).not.toBeInTheDocument();
    expect(screen.getByLabelText("Decree Title")).not.toBeDisabled();
    expect(screen.getByLabelText("Decree Text")).not.toBeDisabled();
    expect(screen.getByRole("button", { name: "Issue Decree" })).toBeEnabled();
  });

  it("uses a fresh run request after a definitive retryable HTTP failure", async () => {
    const user = userEvent.setup();
    createRunMutate
      .mockRejectedValueOnce(makeApiError(503, "provider model catalog unavailable", { code: "internal_error", retryable: true, correlationId: "api.create_run" }))
      .mockResolvedValueOnce(makeRunResult({ run_id: "run-2" }));
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Title"), "Ambiguous HTTP run failure");
    await user.type(screen.getByLabelText("Decree Text"), "Recover the original run start");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText(/Kingdom rejected the run start request before it was accepted/i);

    expect(createDecreeMutate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Start Run" })).toBeEnabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeEnabled();

    await user.click(screen.getByRole("button", { name: "Start Run" }));
    await screen.findByText("Decree Dispatched");

    expect(createRunMutate).toHaveBeenCalledTimes(2);
    const firstRunRequest = createRunMutate.mock.calls[0][0] as { body: { decree_id: string }; key: string };
    const secondRunRequest = createRunMutate.mock.calls[1][0] as { body: { decree_id: string }; key: string };
    expect(firstRunRequest.body.decree_id).toBe("decree-1");
    expect(secondRunRequest.body.decree_id).toBe("decree-1");
    expect(firstRunRequest.key).not.toBe(secondRunRequest.key);
  });

  it("scrolls smoothly to the dispatch confirmation after a successful dispatch", async () => {
    const user = userEvent.setup();
    const scrollIntoView = vi.fn();
    Object.defineProperty(Element.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    scrollIntoView.mockClear();
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText("Decree Dispatched");

    const confirmationRegion = screen.getByRole("region", { name: "Decree Dispatched" });
    const scrollTarget = scrollIntoView.mock.instances[scrollIntoView.mock.instances.length - 1];

    expect(screen.getByText("Decree Dispatched")).not.toHaveFocus();
    expect(scrollTarget).toBe(confirmationRegion);
    expect(scrollIntoView).toHaveBeenCalledWith({ behavior: "smooth", block: "start", inline: "nearest" });
  });

  it("renders the dispatch confirmation before recent runs", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText("Decree Dispatched");

    const confirmationHeading = screen.getByText("Decree Dispatched");
    const recentRunsHeading = screen.getByRole("heading", { name: "Recent Decree Runs" });
    expect(confirmationHeading.compareDocumentPosition(recentRunsHeading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("uses instant confirmation scrolling when reduced motion is enabled", async () => {
    const user = userEvent.setup();
    const scrollIntoView = vi.fn();
    Object.defineProperty(Element.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: query === "(prefers-reduced-motion: reduce)",
        media: query,
        onchange: null,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        addListener: vi.fn(),
        removeListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    });
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    scrollIntoView.mockClear();
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    await screen.findByText("Decree Dispatched");

    const confirmationRegion = screen.getByRole("region", { name: "Decree Dispatched" });
    const scrollTarget = scrollIntoView.mock.instances[scrollIntoView.mock.instances.length - 1];

    expect(screen.getByText("Decree Dispatched")).not.toHaveFocus();
    expect(scrollTarget).toBe(confirmationRegion);
    expect(scrollIntoView).toHaveBeenCalledWith({ behavior: "auto", block: "start", inline: "nearest" });
  });

  it("renders a git worktree toggle with repository guidance", () => {
    render(<IssueDecreePage />);

    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toBeEnabled();
    expect(screen.queryByText("Git worktree is available for this project root.")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Use Git Worktree definition")).toBeInTheDocument();
  });

  it("disables the git worktree toggle outside a git repository", () => {
    mockWorkspaceCapabilities = {
      project_root: "/tmp/project",
      default_publish_mode: "direct",
      git_repo: false,
      git_worktree_available: false,
      git_worktree_reason: "project root is not a valid git working tree",
    };

    render(<IssueDecreePage />);

    expect(screen.getByRole("switch", { name: "Use Git Worktree" })).toBeDisabled();
    expect(screen.getByText("project root is not a valid git working tree")).toBeInTheDocument();
  });

  it("submits git_worktree publish mode when the toggle is enabled", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("switch", { name: "Use Git Worktree" }));
    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Decree Title"), "Decree Worktree");
    await user.type(screen.getByLabelText("Decree Text"), "Publish in worktree");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({ body: { publish_mode: "git_worktree" } });
  });

  it("disables cycle inputs and shows warning when run-until-all-approve is enabled", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("switch", { name: "Run Until All Approve" }));

    expect(screen.getByText("Uses more tokens: review and validation keep running until they pass.")).toBeInTheDocument();
    expect(screen.getByLabelText("Max Review Cycles")).toBeDisabled();
    expect(screen.getByLabelText("Max Validation Cycles")).toBeDisabled();
  });

  it("switches to skip-review workflow and greys out review controls", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("switch", { name: "Run Until All Approve" }));
    await user.click(screen.getByRole("switch", { name: "Skip Reviews and Validations" }));

    expect(screen.getByText("Review and validation are skipped. Builder output proceeds directly to royal closure for this run.")).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Run Until All Approve" })).toHaveAttribute("aria-checked", "false");
    expect(screen.getByRole("switch", { name: "Run Until All Approve" })).toBeDisabled();
    expect(screen.getByLabelText("Max Review Cycles")).toBeDisabled();
    expect(screen.getByLabelText("Max Validation Cycles")).toBeDisabled();
  });

	it("preserves zero-valued cycle inputs on blur and explains immediate auto-success", async () => {
		const user = userEvent.setup();
		render(<IssueDecreePage />);

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
		render(<IssueDecreePage />);

		await selectGlobalModel(user);
		await user.clear(screen.getByLabelText("Decree Title"));
		await user.type(screen.getByLabelText("Decree Title"), "Zero cap decree");
		await user.clear(screen.getByLabelText("Decree Text"));
		await user.type(screen.getByLabelText("Decree Text"), "Use immediate review auto-success");

		const reviewInput = screen.getByLabelText("Max Review Cycles");
		await user.clear(reviewInput);
		await user.type(reviewInput, "0");
		await user.tab();

		const validationInput = screen.getByLabelText("Max Validation Cycles");
		await user.clear(validationInput);
		await user.type(validationInput, "0");
		await user.tab();

		await user.click(screen.getByRole("button", { name: "Issue Decree" }));

		expect(createRunMutate).toHaveBeenCalledTimes(1);
		expect(createRunMutate.mock.calls[0][0]).toMatchObject({
			body: {
				max_execution_review_cycles: 0,
				max_validation_rework_cycles: 0,
			},
		});
	});

  it("defaults bounded cycle inputs to five", async () => {
    render(<IssueDecreePage />);

    expect(screen.getByLabelText("Max Review Cycles")).toHaveValue(5);
    expect(screen.getByLabelText("Max Validation Cycles")).toHaveValue(5);
  });

  it("keeps cycle inputs editable without forcing leading zeroes", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

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

  it("keeps review toggles above cycle inputs and keeps git worktree above allfather approval", () => {
    render(<IssueDecreePage />);

    const skipToggle = screen.getByRole("switch", { name: "Skip Reviews and Validations" });
    const reviewInput = screen.getByLabelText("Max Review Cycles");
    const validationInput = screen.getByLabelText("Max Validation Cycles");
    const runUntilToggle = screen.getByRole("switch", { name: "Run Until All Approve" });
    const gitWorktreeToggle = screen.getByRole("switch", { name: "Use Git Worktree" });
    const allfatherToggle = screen.getByRole("switch", {
      name: /Allfather \(Human\) in the Loop/i,
    });

    expect(reviewInput).toBeInTheDocument();
    expect(validationInput).toBeInTheDocument();
    expect(skipToggle).toBeInTheDocument();
    expect(runUntilToggle).toBeInTheDocument();
    expect(gitWorktreeToggle).toBeInTheDocument();
    expect(screen.queryByLabelText("Workflow ID")).not.toBeInTheDocument();
    expect(skipToggle.compareDocumentPosition(runUntilToggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
	    expect(runUntilToggle.compareDocumentPosition(reviewInput) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
	    expect(reviewInput.compareDocumentPosition(validationInput) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
	    expect(runUntilToggle.compareDocumentPosition(gitWorktreeToggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
	    expect(gitWorktreeToggle.compareDocumentPosition(allfatherToggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
	    expect(reviewInput.compareDocumentPosition(allfatherToggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
	    expect(validationInput.compareDocumentPosition(allfatherToggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
	    expect(screen.queryByText("Advanced Run Options")).not.toBeInTheDocument();
	  });

  it("uses shared info tooltip triggers for decree toggles", async () => {
    render(<IssueDecreePage />);

    expect(screen.getByLabelText("Skip Reviews and Validations definition")).toBeInTheDocument();
    expect(screen.getByLabelText("Allfather (Human) in the Loop definition")).toBeInTheDocument();
    expect(screen.getByLabelText("Use Git Worktree definition")).toBeInTheDocument();
    expect(screen.getByLabelText("Run Until All Approve definition")).toBeInTheDocument();
  });

  it("renders model controls outside advanced options with provider-driven thinking effort", () => {
    render(<IssueDecreePage />);

    expect(screen.queryByLabelText("Model Search")).not.toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Use for all Agents" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("combobox", { name: /Global Model/ })).toHaveTextContent("Select model");
    expect(screen.getByText("Not available for this model")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Show role controls/i })).toBeInTheDocument();
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

    render(<IssueDecreePage />);

    expect(screen.getByText("provider model catalog unavailable")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Global Model/ })).toBeDisabled();
  });

  it("shows stale provider model catalog warning while keeping selection enabled", () => {
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
        failure_reason: "command_failed",
        failure_message: "showing cached models",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<IssueDecreePage />);

    expect(screen.getByText("showing cached models")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Global Model/ })).not.toBeDisabled();
  });

  it("keeps cached provider models selectable when a background refresh fails", () => {
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

    render(<IssueDecreePage />);

    expect(screen.getByText("provider model catalog refresh failed")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: /Global Model/ })).not.toBeDisabled();
  });

	it("uses a live refresh when retrying a stale provider model catalog warning", async () => {
		const user = userEvent.setup();
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
				failure_reason: "command_failed",
				failure_message: "showing cached models",
			},
			isLoading: false,
			error: null,
			refetch: vi.fn(),
		});

		render(<IssueDecreePage />);
		await user.click(screen.getByRole("button", { name: "Retry refresh" }));

		expect(refreshProviderModelsMutate).toHaveBeenCalledTimes(1);
	});

	it("shows retry refresh failures inline while keeping cached models selectable", async () => {
		const user = userEvent.setup();
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
				failure_reason: "command_failed",
				failure_message: "showing cached models",
			},
			isLoading: false,
			error: null,
			refetch: vi.fn(),
			dataUpdatedAt: 1,
		});
		refreshProviderModelsMutate.mockImplementation((_vars, callbacks) => {
			callbacks?.onError?.(new Error("live refresh failed"), undefined, undefined);
		});

		render(<IssueDecreePage />);
		await user.click(screen.getByRole("button", { name: "Retry refresh" }));

		expect(await screen.findByText("live refresh failed")).toBeInTheDocument();
		expect(screen.getByRole("combobox", { name: /Global Model/ })).not.toBeDisabled();
	});

  it("disables decree submission until a global model is selected", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.type(screen.getByLabelText("Decree Title"), "Decree A");
    await user.type(screen.getByLabelText("Decree Text"), "Do the thing");

    expect(screen.getByRole("button", { name: "Issue Decree" })).toBeDisabled();

    fireEvent.submit(screen.getByRole("button", { name: "Issue Decree" }).closest("form") as HTMLFormElement);

    expect(createDecreeMutate).not.toHaveBeenCalled();
    expect(createRunMutate).not.toHaveBeenCalled();
  });

  it("supports searchable model combobox filtering", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("combobox", { name: /Global Model/ }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");

    expect(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "GitHub Copilot - GPT-5.3-Codex" })).not.toBeInTheDocument();
  });

  it("selects the active searchable model option with Enter after arrow navigation", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    const globalModel = screen.getByRole("combobox", { name: /Global Model/ });
    await user.click(globalModel);
    await user.keyboard("{ArrowDown}{ArrowDown}{Enter}");

    expect(globalModel).toHaveTextContent("OpenCode - GPT-5 Nano");
  });

  it("hides thinking effort when the selected model does not support it", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("combobox", { name: /Global Model/ }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");
    await user.click(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" }));

    expect(screen.getByText("Not available for this model")).toBeInTheDocument();
  });

  it("defaults all agent selectors to global model and preserves non-diverged selections", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: /Show role controls/i }));

    expect(screen.getByRole("combobox", { name: "King Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
    expect(screen.getByRole("combobox", { name: "Shieldmaiden Reviewer Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
    expect(screen.getAllByLabelText("Thinking Effort")[1]).toHaveTextContent("Medium");

    await user.click(screen.getByRole("combobox", { name: "Shieldmaiden Reviewer Model" }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");
    await user.click(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" }));

    expect(screen.getByRole("switch", { name: "Use for all Agents" })).toHaveAttribute("aria-checked", "false");
    expect(screen.getByRole("combobox", { name: "King Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
    expect(screen.getByRole("combobox", { name: "Shieldmaiden Reviewer Model" })).toHaveTextContent("OpenCode - GPT-5 Nano");
    expect(screen.getByText("Not available for this model")).toBeInTheDocument();

    await user.click(screen.getByRole("switch", { name: "Use for all Agents" }));

    expect(screen.getByRole("switch", { name: "Use for all Agents" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("combobox", { name: "Shieldmaiden Reviewer Model" })).toHaveTextContent("GitHub Copilot - GPT-5.3-Codex");
  });

  it("submits global and role-specific model selections", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.click(screen.getByLabelText("Thinking Effort"));
    await user.click(screen.getByRole("option", { name: "High" }));
    await user.click(screen.getByRole("button", { name: /Show role controls/i }));
    await user.click(screen.getByRole("combobox", { name: "Shieldmaiden Reviewer Model" }));
    await user.type(screen.getByPlaceholderText("Search models"), "nano");
    await user.click(screen.getByRole("option", { name: "OpenCode - GPT-5 Nano" }));

    await user.type(screen.getByLabelText("Decree Title"), "Decree C");
    await user.type(screen.getByLabelText("Decree Text"), "Use model overrides");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    expect(createRunMutate).toHaveBeenCalledTimes(1);
    expect(createRunMutate.mock.calls[0][0]).toMatchObject({
      body: {
        model_selection: {
          global: {
            model: "github-copilot/gpt-5.3-codex",
            thinking_effort: "high",
          },
          role_overrides: {
            "reviewer-shieldmaiden": {
              model: "opencode/gpt-5-nano",
            },
          },
        },
      },
    });
  });

  it("sends lineage metadata on decree create while preserving run configuration", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.clear(screen.getByLabelText("Decree Title"));
    await user.type(screen.getByLabelText("Decree Title"), "Chosen decree title");
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Text"), "Chosen decree text");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    expect(createDecreeMutate).toHaveBeenCalledWith(expect.objectContaining({
      body: expect.objectContaining({
        decree_title: "Chosen decree title",
        decree_text: "Chosen decree text",
        planning_lineage: {
          planning_session_id: "planning-session-1",
          planning_draft_id: "planning-draft-1",
        },
      }),
    }));
    const decreeRequest = createDecreeMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(decreeRequest.body).not.toHaveProperty("external_source_snapshot_id");
		expect(decreeRequest.body.planning_lineage).not.toHaveProperty("planning_context_snapshot_id");
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
	    render(<IssueDecreePage />);

    await user.click(screen.getByRole("switch", { name: "Skip Reviews and Validations" }));
    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Decree Title"), "Skip quality gates");
    await user.type(screen.getByLabelText("Decree Text"), "Go straight to closure");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

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

  it("reset clears planning lineage before issuing a fresh decree", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Reset" }));

    expect(screen.getByRole("button", { name: "From Plan" })).toHaveTextContent("No plan");
    expect(screen.getByLabelText("Decree Title")).toHaveValue("");
    expect(screen.getByLabelText("Decree Text")).toHaveValue("");

    await selectGlobalModel(user);
    await user.type(screen.getByLabelText("Decree Title"), "Fresh decree title");
    await user.type(screen.getByLabelText("Decree Text"), "Fresh decree text");
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    const decreeRequest = createDecreeMutate.mock.calls[0]?.[0] as {
      body: {
        decree_title: string;
        decree_text: string;
        metadata?: Record<string, string>;
      };
    };

    expect(createDecreeMutate).toHaveBeenCalledTimes(1);
    expect(decreeRequest).toMatchObject({
      body: {
        decree_title: "Fresh decree title",
        decree_text: "Fresh decree text",
      },
    });
    expect(decreeRequest.body.metadata).toBeUndefined();
    expect(decreeRequest.body).not.toHaveProperty("planning_lineage");
    expect(decreeRequest.body).not.toHaveProperty("external_source_snapshot_id");
  });

  it("renders agent override roles in required execution order", async () => {
    const user = userEvent.setup();
    render(<IssueDecreePage />);

		await user.click(screen.getByRole("button", { name: /Show role controls/i }));

		const roleComboboxIDs = screen
			.getAllByRole("combobox")
			.map((item) => item.getAttribute("id"))
			.filter((id) => id && id !== "global-model") as string[];
		expect(roleComboboxIDs).toEqual([
			"role-model-king",
			"role-model-noble-delegate",
			"role-model-clergy-delegate",
			"role-model-burgher-delegate",
			"role-model-peasant-delegate",
			"role-model-chancellor",
			"role-model-council-member",
			"role-model-builder-berserker",
			"role-model-reviewer-shieldmaiden",
			"role-model-intelligence-qa-scout",
			"role-model-intelligence-nightraven",
			"role-model-intelligence-loki-chaos",
		]);

    const groupHeadings = screen.getAllByRole("heading", { level: 4 }).map((item) => item.textContent?.trim());
    expect(groupHeadings).toEqual(["King", "Advisors", "Chancellor", "Council Member", "Builder", "Reviewers", "Validators"]);
  });

  it("promotes refreshed provenance atomically with any applied fields", async () => {
    let captureCount = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) {
        captureCount += 1;
        return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: captureCount === 1 ? "snapshot-a" : "snapshot-b" }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      const refreshed = url.endsWith("snapshot-b");
      return new Response(JSON.stringify({ snapshot_id: refreshed ? "snapshot-b" : "snapshot-a", source_id: "source-1", issue: { title: refreshed ? "Issue B" : "Issue A", body: refreshed ? "Body B" : "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: refreshed ? "2026-08-03T00:00:00Z" : "2026-08-02T00:00:00Z" }, profile: { include_comment_count: true, include_comments: false }, provider_updated_at: "2026-08-03T00:00:00Z", fetched_at: "2026-08-03T00:00:00Z", comment_count: 0, fetched_comment_count: 0, retained_comment_count: 0, retained_comment_bytes: 0, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<IssueDecreePage />);
    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));
    expect(await screen.findByDisplayValue("Body A")).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Decree Text"));
    await user.type(screen.getByLabelText("Decree Text"), "Operator edit");
    await user.click(screen.getByRole("button", { name: /check for updates/i }));
    expect(await screen.findByText(/an update is available/i)).toBeInTheDocument();
    expect(screen.getByLabelText("GitHub source comparison")).toHaveAttribute("data-baseline", "snapshot-a");
    expect(screen.getByLabelText("Decree Text")).toHaveValue("Operator edit");
    await user.click(screen.getByRole("button", { name: "Use refreshed body" }));
    expect(screen.getByLabelText("Decree Text")).toHaveValue("Body B");
    expect(screen.queryByText(/an update is available/i)).not.toBeInTheDocument();

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));
    expect(createDecreeMutate).toHaveBeenCalledWith(expect.objectContaining({
      body: expect.objectContaining({ external_source_snapshot_id: "snapshot-b", decree_text: "Body B" }),
    }));
    const request = createDecreeMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(request.body).not.toHaveProperty("planning_lineage");
  });

  it("does not select the newest plan unless the operator chooses it", () => {
    mockSearch = {};
    render(<IssueDecreePage />);

    expect(screen.getByRole("button", { name: "From Plan" })).toHaveTextContent("No plan");
    expect(screen.getByLabelText("Decree Title")).toHaveValue("");
    expect(screen.getByLabelText("Decree Text")).toHaveValue("");
  });

  it("keeps the decree title, plan selector, and actions mobile-safe", () => {
    render(<IssueDecreePage />);

    const titlePlanRow = screen.getByLabelText("Decree Title").parentElement?.parentElement;
    expect(titlePlanRow).toHaveClass("grid", "min-w-0");
    expect(titlePlanRow?.className).toContain("md:grid-cols-[minmax(0,1fr)_minmax(12rem,15rem)_auto]");
    expect(screen.getByRole("button", { name: "Issue Decree" }).parentElement).toHaveClass("flex-wrap");
  });

  it("clears planning lineage when a direct GitHub issue is imported", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: "snapshot-direct" }), { status: 201, headers: { "Content-Type": "application/json" } });
      return new Response(JSON.stringify({ snapshot_id: "snapshot-direct", source_id: "source-1", issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" }, profile: { include_comment_count: true, include_comments: false }, provider_updated_at: "2026-08-02T00:00:00Z", fetched_at: "2026-08-02T00:00:00Z", comment_count: 0, fetched_comment_count: 0, retained_comment_count: 0, retained_comment_bytes: 0, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));
    expect(await screen.findByRole("button", { name: "From Plan" })).toHaveTextContent("No plan");
    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));

    const request = createDecreeMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(request.body).toMatchObject({ external_source_snapshot_id: "snapshot-direct" });
    expect(request.body).not.toHaveProperty("planning_lineage");
  });

  it("clears direct GitHub provenance when a plan is selected", async () => {
    mockSearch = {};
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/external-source-captures")) return new Response(JSON.stringify({ source_id: "source-1", snapshot_id: "snapshot-direct" }), { status: 201, headers: { "Content-Type": "application/json" } });
      return new Response(JSON.stringify({ snapshot_id: "snapshot-direct", source_id: "source-1", issue: { title: "Issue A", body: "Body A", state: "open", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-02T00:00:00Z" }, profile: { include_comment_count: true, include_comments: false }, provider_updated_at: "2026-08-02T00:00:00Z", fetched_at: "2026-08-02T00:00:00Z", comment_count: 0, fetched_comment_count: 0, retained_comment_count: 0, retained_comment_bytes: 0, comments_truncated: false }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<IssueDecreePage />);

    await user.click(screen.getByRole("button", { name: "Mock prefill issue" }));
    expect(await screen.findByLabelText("Selected GitHub source")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "From Plan" }));
    await user.click(screen.getByRole("option", { name: "Test session" }));
    await waitFor(() => expect(screen.getByLabelText("Decree Title")).toHaveValue("Draft supplied title"));
    expect(screen.queryByLabelText("Selected GitHub source")).not.toBeInTheDocument();

    await selectGlobalModel(user);
    await user.click(screen.getByRole("button", { name: "Issue Decree" }));
    const request = createDecreeMutate.mock.calls[0][0] as { body: Record<string, unknown> };
    expect(request.body).toMatchObject({ planning_lineage: { planning_session_id: "planning-session-1", planning_draft_id: "planning-draft-1" } });
    expect(request.body).not.toHaveProperty("external_source_snapshot_id");
  });
});
