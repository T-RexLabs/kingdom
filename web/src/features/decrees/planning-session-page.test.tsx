import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import { PlanningSessionPage } from "./planning-session-page";

const titleMutateAsync = vi.fn(async () => ({}));
const generateTitleMutateAsync = vi.fn(async () => ({}));
const modelMutateAsync = vi.fn(async () => ({}));
// .mutate is the fire-and-forget shape the model autosave + chip
// retry now uses (errors land on hook.error instead of leaking
// unhandled rejections). The test calls these to assert the right
// fire path was invoked.
const modelMutate = vi.fn();
// reset() is the mutation-state clearer the page now calls when the
// user reverts to canonical with a stale error. The chip render gate
// drops on dirty=false, but reset() also wipes the underlying
// mutation state so subsequent dirty edits don't surface a stale
// error before the new autosave fires.
const modelReset = vi.fn();
const invalidateQueries = vi.fn();
const setQueryData = vi.fn();
// setQueriesData backs the CAS rollback's list-prefix patch. Tests
// assert against this when verifying the rollback covers every list
// slice; for tests that don't care, the default no-op is fine.
const setQueriesData = vi.fn();
const mockUseProviderModels = vi.fn();
const refreshProviderModelsMutate = vi.fn();
const mockUsePlanningSession = vi.fn();
const mockUsePlanningSessionDraft = vi.fn();
const mockUseUpdatePlanningSessionTitle = vi.fn();
const mockUseUpdatePlanningSessionModelSelection = vi.fn();
const mockUseGeneratePlanningSessionTitle = vi.fn();
const mockUseCreatePlanningOperation = vi.fn();
const mockUsePlanningOperation = vi.fn();
const mockUseParams = vi.fn(() => ({ sessionId: "planning-session-1" }));
const mockUseSearch = vi.fn(() => ({}));
const mockNavigate = vi.fn();
const mockUsePlannerMessagePages = vi.fn();
const mockUsePlannerOperation = vi.fn();
const refetchPlanningSession = vi.fn(async () => ({}));
const mockNewIdempotencyKey = vi.fn((scope: string) => `${scope}-test`);

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, ...props }: { children: ReactNode }) => (
    <a href="#" {...props}>
      {children}
    </a>
  ),
  useParams: () => mockUseParams(),
  useSearch: () => mockUseSearch(),
  useNavigate: () => mockNavigate,
}));

vi.mock("@tanstack/react-query", async () => {
  const actual = await vi.importActual<typeof import("@tanstack/react-query")>("@tanstack/react-query");
  return {
    ...actual,
    useQueryClient: () => ({ invalidateQueries, setQueryData, setQueriesData }),
  };
});

vi.mock("@/api/hooks", () => ({
  newIdempotencyKey: (scope: string) => mockNewIdempotencyKey(scope),
  useProviderModels: () => mockUseProviderModels(),
  useRefreshProviderModels: () => ({ mutate: refreshProviderModelsMutate, isPending: false }),
  usePlanningSession: (...args: unknown[]) => mockUsePlanningSession(...args),
  usePlanningSessionDraft: (...args: unknown[]) => mockUsePlanningSessionDraft(...args),
  useUpdatePlanningSessionTitle: (...args: unknown[]) => mockUseUpdatePlanningSessionTitle(...args),
  useUpdatePlanningSessionModelSelection: (...args: unknown[]) => mockUseUpdatePlanningSessionModelSelection(...args),
  useGeneratePlanningSessionTitle: (...args: unknown[]) => mockUseGeneratePlanningSessionTitle(...args),
  useCreatePlanningOperation: (...args: unknown[]) => mockUseCreatePlanningOperation(...args),
  usePlanningOperation: (...args: unknown[]) => mockUsePlanningOperation(...args),
  useActivateVariant: () => ({ mutateAsync: vi.fn().mockResolvedValue(undefined) }),
}));

vi.mock("./use-planner-message-pages", () => ({
  usePlannerMessagePages: () => mockUsePlannerMessagePages(),
}));

vi.mock("./planner-thread", () => ({
  PlannerThread: ({ session }: { session: { planning_session_id: string } }) => <div>Planner Thread {session.planning_session_id}</div>,
}));

vi.mock("./planner-draft-panel", () => ({
  PlanResultModal: ({ open, draft }: { open: boolean; draft: unknown }) => (open && draft ? <div>Plan Modal</div> : null),
}));

vi.mock("@/features/connections/external-source-comparison", () => ({
  ExternalSourceComparison: ({ baselineId, candidateId }: { baselineId: string; candidateId: string }) => <div aria-label="GitHub source comparison" data-baseline={baselineId} data-candidate={candidateId}>Source comparison</div>,
}));

vi.mock("@/features/connections/issue-picker", () => ({
  IssuePicker: ({ onSelect, triggerLabel }: { onSelect: (selection: unknown) => Promise<void>; triggerLabel: string }) => (
    <button
      type="button"
      onClick={() => {
        void onSelect({
          intake: {
            connection_id: "connection-1",
            repository_id: "repository-1",
            issue_number: 23,
            include_comments: false,
          },
          issue: {},
        }).catch(() => undefined);
      }}
    >
      {triggerLabel}
    </button>
  ),
}));

vi.mock("./use-planner-operation", () => ({
  usePlannerOperation: (...args: unknown[]) => mockUsePlannerOperation(...args),
}));

vi.mock("./global-formalize-context", () => ({
  useGlobalFormalize: () => ({
    current: null,
    trackFormalize: vi.fn(),
    clearFormalize: vi.fn(),
  }),
}));

/**
 * Planning sources are a collapsed control in the settings row — the header
 * stays one row tall on load — so any test that works with the list has to
 * open it the way an operator would.
 */
async function openPlanningSources(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: /^Planning Sources/ }));
}

describe("PlanningSessionPage", () => {
  beforeEach(() => {
    titleMutateAsync.mockReset();
    titleMutateAsync.mockResolvedValue({});
    generateTitleMutateAsync.mockReset();
    generateTitleMutateAsync.mockResolvedValue({});
    modelMutateAsync.mockReset();
    modelMutateAsync.mockResolvedValue({});
    modelMutate.mockReset();
    modelReset.mockReset();
    refreshProviderModelsMutate.mockReset();
    // Default: a single Medium option matching the canonical session.
    // Tests that need divergent state (e.g. the dirty-state regression)
    // override this with multi-option mocks.
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "fresh",
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    invalidateQueries.mockClear();
    setQueryData.mockClear();
    setQueriesData.mockClear();
    mockNavigate.mockReset();
    refetchPlanningSession.mockClear();
    mockNewIdempotencyKey.mockReset();
    mockNewIdempotencyKey.mockImplementation((scope: string) => `${scope}-test`);
    mockUseParams.mockReturnValue({ sessionId: "planning-session-1" });
    mockUseSearch.mockReturnValue({});
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "Planner kickoff",
        title_state: "custom",
        auto_title_eligible: false,
        lifecycle_state: "active",
        conversation_state: "ready",
        current_question_message_id: "assistant-1",
        current_operation_id: undefined,
        current_draft_id: undefined,
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: {
          model: "github-copilot/gpt-5.4",
          thinking_effort: "medium",
        },
      },
      error: null,
      refetch: refetchPlanningSession,
    });
    mockUsePlanningSessionDraft.mockReturnValue({ data: null });
    mockUseUpdatePlanningSessionTitle.mockReturnValue({
      mutateAsync: titleMutateAsync,
      error: null,
    });
    mockUseUpdatePlanningSessionModelSelection.mockReturnValue({
      mutate: modelMutate,
      mutateAsync: modelMutateAsync,
      reset: modelReset,
      error: null,
    });
    mockUseGeneratePlanningSessionTitle.mockReturnValue({
      mutateAsync: generateTitleMutateAsync,
      isPending: false,
    });
    mockUseCreatePlanningOperation.mockReturnValue({
      mutateAsync: vi.fn(async () => ({ operation: { operation_id: "operation-1" } })),
      error: null,
    });
    mockUsePlanningOperation.mockReturnValue({ data: null });
    mockUsePlannerOperation.mockReturnValue({ operation: null, connection: { state: "idle" } });
    mockUsePlannerMessagePages.mockReturnValue({
      messages: [],
      firstItemIndex: 0,
      totalCount: 0,
      hasPreviousPage: false,
      isFetchingPreviousPage: false,
      fetchPreviousPage: vi.fn(),
      error: null,
      isLoading: false,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("shows session title in the top bar", () => {
    render(<PlanningSessionPage />);
    expect(screen.getByText("Planner kickoff")).toBeInTheDocument();
  });

  it("renders the planner thread content", () => {
    render(<PlanningSessionPage />);
    expect(screen.getByText("Planner Thread planning-session-1")).toBeInTheDocument();
  });

  it("shows Generate Plan button", () => {
    render(<PlanningSessionPage />);
    expect(screen.getByRole("button", { name: "Generate Plan" })).toBeInTheDocument();
  });

  it("attaches a GitHub issue to the existing session and refreshes every source card", async () => {
    let request: { url: string; init?: RequestInit } | undefined;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      request = { url: String(input), init };
      return new Response(JSON.stringify({
        link_id: "link-23",
        provider: "github",
        repository: "owner/repo",
        issue_number: 23,
        canonical_url: "https://github.com/owner/repo/issues/23",
        used_snapshot_id: "snapshot-23",
        active_snapshot_id: "snapshot-23",
        latest_snapshot_id: "snapshot-23",
        status: "never_checked",
      }), { status: 201, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await openPlanningSources(user);
    await user.click(screen.getByRole("button", { name: "Add GitHub Issue" }));

    await waitFor(() => expect(refetchPlanningSession).toHaveBeenCalledTimes(1));
    expect(request?.url).toMatch(/\/planning-sessions\/planning-session-1\/external-sources$/);
    expect(request?.init?.method).toBe("POST");
    expect(new Headers(request?.init?.headers).get("Idempotency-Key")).toMatch(/^kd1\./);
    expect(JSON.parse(String(request?.init?.body))).toEqual({
      connection_id: "connection-1",
      repository_id: "repository-1",
      issue_number: 23,
      include_comments: false,
    });
  });

  it("retains the attachment idempotency key while the reservation is pending", async () => {
    const requestKeys: string[] = [];
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      requestKeys.push(new Headers(init?.headers).get("Idempotency-Key") ?? "");
      if (requestKeys.length === 1) {
        return new Response(JSON.stringify({
          error: {
            code: "idempotency_pending",
            message: "The original request is still pending recovery.",
            retryable: true,
            category: "conflict",
            details: {},
            correlation_id: "test-idempotency-pending",
            occurred_at: "2026-04-21T10:06:00Z",
          },
        }), { status: 409, headers: { "Content-Type": "application/json" } });
      }
      return new Response(JSON.stringify({ link_id: "link-23" }), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      });
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await openPlanningSources(user);
    await user.click(screen.getByRole("button", { name: "Add GitHub Issue" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole("button", { name: "Add GitHub Issue" }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(requestKeys[0]).toMatch(/^kd1\./);
    expect(requestKeys[1]).toBe(requestKeys[0]);
    expect(refetchPlanningSession).toHaveBeenCalledTimes(1);
  });

  it("lists every source as one row and expands one at a time", async () => {
    const base = mockUsePlanningSession().data;
    mockUsePlanningSession.mockReturnValue({
      data: {
        ...base,
        external_sources: [
          { link_id: "link-1", provider: "github", repository: "owner/one", issue_number: 1, canonical_url: "https://github.com/owner/one/issues/1", used_snapshot_id: "snapshot-1", active_snapshot_id: "snapshot-1", latest_snapshot_id: "snapshot-1", status: "never_checked" },
          { link_id: "link-2", provider: "github", repository: "owner/two", issue_number: 2, canonical_url: "https://github.com/owner/two/issues/2", used_snapshot_id: "snapshot-2", active_snapshot_id: "snapshot-2", latest_snapshot_id: "snapshot-2", status: "never_checked" },
        ],
      },
      error: null,
      refetch: refetchPlanningSession,
    });

    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await openPlanningSources(user);
    expect(screen.getByRole("button", { name: "Add Source" })).toBeInTheDocument();
    expect(screen.getAllByRole("region", { name: /Linked GitHub source/ })).toHaveLength(2);
    // A current source has nothing behind a disclosure, so it has no
    // disclosure — and nothing to say about its own freshness.
    expect(screen.queryByRole("button", { name: /^Details for / })).not.toBeInTheDocument();
    expect(screen.queryByText("Not Checked")).not.toBeInTheDocument();
    expect(screen.queryByText("Up to Date")).not.toBeInTheDocument();
  });

  it("opens detail only for the source that has an update, one row at a time", async () => {
    const base = mockUsePlanningSession().data;
    mockUsePlanningSession.mockReturnValue({
      data: {
        ...base,
        external_sources: [
          { link_id: "link-1", provider: "github", repository: "owner/one", issue_number: 1, canonical_url: "https://github.com/owner/one/issues/1", used_snapshot_id: "snapshot-1", active_snapshot_id: "snapshot-1", latest_snapshot_id: "snapshot-1-next", status: "changed" },
          { link_id: "link-2", provider: "github", repository: "owner/two", issue_number: 2, canonical_url: "https://github.com/owner/two/issues/2", used_snapshot_id: "snapshot-2", active_snapshot_id: "snapshot-2", latest_snapshot_id: "snapshot-2", status: "never_checked" },
        ],
      },
      error: null,
      refetch: refetchPlanningSession,
    });

    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await openPlanningSources(user);
    expect(screen.getByRole("button", { name: "Details for owner/one#1" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Details for owner/two#2" })).not.toBeInTheDocument();
    expect(screen.getByText("Update Available")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Details for owner/one#1" }));
    expect(screen.getByLabelText("GitHub source comparison")).toBeInTheDocument();
  });

  it("shows changed fields after refresh and keeps adoption explicit", async () => {
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "Planner kickoff",
        title_state: "custom",
        auto_title_eligible: false,
        lifecycle_state: "active",
        conversation_state: "ready",
        current_question_message_id: "assistant-1",
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4", thinking_effort: "medium" },
        external_sources: [{
          link_id: "link-1",
          provider: "github",
          repository: "owner/repo",
          issue_number: 17,
          canonical_url: "https://github.com/owner/repo/issues/17",
          used_snapshot_id: "snapshot-used",
          latest_snapshot_id: "snapshot-latest",
          status: "changed",
        }, {
          link_id: "link-2",
          provider: "github",
          repository: "owner/other",
          issue_number: 18,
          canonical_url: "https://github.com/owner/other/issues/18",
          used_snapshot_id: "snapshot-other-used",
          active_snapshot_id: "snapshot-other-active",
          latest_snapshot_id: "snapshot-other-latest",
          status: "changed",
        }],
      },
      error: null,
      refetch: refetchPlanningSession,
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(String(input)).toMatch(/\/external-source-links\/link-1\/refreshes$/);
      expect(init?.method).toBe("POST");
      expect(new Headers(init?.headers).get("Idempotency-Key")).toBe("external-refresh-test");
      return new Response(JSON.stringify({
        outcome: "changed",
        snapshot_created: true,
        changes: {
          title: true,
          body: true,
          state: false,
          repository: false,
          issue_number: false,
          canonical_url: false,
          created_at: false,
          updated_at: true,
          comment_count: false,
          comments: false,
        },
        message: "A newer GitHub issue version is available.",
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await openPlanningSources(user);
    expect(screen.getAllByText(/update available/i)).toHaveLength(2);
    expect(screen.getByRole("link", { name: /open owner\/repo#17 on github/i })).toHaveAttribute("rel", "noopener noreferrer");
    expect(screen.getByRole("link", { name: /open owner\/other#18 on github/i })).toBeInTheDocument();
    const firstSource = screen.getByRole("region", { name: "Linked GitHub source owner/repo#17" });
    const secondSource = screen.getByRole("region", { name: "Linked GitHub source owner/other#18" });
    expect(within(firstSource).getByRole("button", { name: /adopt latest/i })).toBeInTheDocument();
    expect(within(secondSource).getByRole("button", { name: /adopt latest/i })).toBeInTheDocument();

    // The diff belongs to the row's detail, and each source keeps its own baseline.
    await user.click(within(firstSource).getByRole("button", { name: "Details for owner/repo#17" }));
    expect(within(firstSource).getByLabelText("GitHub source comparison")).toHaveAttribute("data-baseline", "snapshot-used");
    await user.click(within(secondSource).getByRole("button", { name: "Details for owner/other#18" }));
    expect(within(secondSource).getByLabelText("GitHub source comparison")).toHaveAttribute("data-baseline", "snapshot-other-active");

    await user.click(within(firstSource).getByRole("button", { name: /^refresh /i }));

    expect(await within(firstSource).findByText("A newer GitHub issue version is available.")).toBeInTheDocument();
    expect(refetchPlanningSession).toHaveBeenCalledTimes(1);
  });

  it("removes a source only after confirmation and detaches it on the server", async () => {
    const base = mockUsePlanningSession().data;
    mockUsePlanningSession.mockReturnValue({
      data: {
        ...base,
        external_sources: [
          { link_id: "link-1", provider: "github", repository: "owner/one", issue_number: 1, canonical_url: "https://github.com/owner/one/issues/1", used_snapshot_id: "snapshot-1", active_snapshot_id: "snapshot-1", latest_snapshot_id: "snapshot-1", status: "never_checked" },
        ],
      },
      error: null,
      refetch: refetchPlanningSession,
    });
    let request: { url: string; init?: RequestInit } | undefined;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      request = { url: String(input), init };
      return new Response(JSON.stringify({ status: "detached" }), { status: 201, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await openPlanningSources(user);
    // Removal is confirmed inline; nothing leaves until the operator says so.
    await user.click(screen.getByRole("button", { name: "Remove owner/one#1" }));
    expect(request).toBeUndefined();
    // The confirmation is the two buttons. It carries no prose, so the labels
    // are what name the target and separate the ask from the answer.
    expect(screen.getByRole("button", { name: "Keep owner/one#1" })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Confirm removing owner/one#1" }));
    await waitFor(() => expect(refetchPlanningSession).toHaveBeenCalledTimes(1));
    expect(request?.url).toMatch(/\/external-source-links\/link-1\/detachments$/);
    expect(request?.init?.method).toBe("POST");
    expect(new Headers(request?.init?.headers).get("Idempotency-Key")).toBe("external-detach-test");
  });

  it("shows durable recovery guidance after a revoked refresh", async () => {
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "Planner kickoff",
        title_state: "custom",
        auto_title_eligible: false,
        lifecycle_state: "active",
        conversation_state: "ready",
        current_question_message_id: "assistant-1",
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4", thinking_effort: "medium" },
        external_sources: [{
          link_id: "link-1",
          provider: "github",
          repository: "owner/repo",
          issue_number: 17,
          canonical_url: "https://github.com/owner/repo/issues/17",
          used_snapshot_id: "snapshot-used",
          latest_snapshot_id: "snapshot-used",
          status: "revoked",
        }],
      },
      error: null,
      refetch: refetchPlanningSession,
    });

    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    // A source Kingdom can no longer read has to be visible before anything is
    // expanded, or the operator plans against context that is silently gone.
    expect(screen.getByRole("button", { name: /^Planning Sources/ })).toHaveTextContent("1 unreadable");

    await openPlanningSources(user);
    expect(screen.getByText(/GitHub App access was revoked/i)).toBeInTheDocument();
    expect(screen.getByText(/run the connection health check/i)).toBeInTheDocument();
    expect(screen.queryByText(/refresh completed/i)).not.toBeInTheDocument();
  });

  it("renders editable title on click", async () => {
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));

    const input = screen.getByPlaceholderText("Session title...");
    expect(input.tagName).toBe("INPUT");
  });

	it("shows planner model and thinking effort controls", () => {
		render(<PlanningSessionPage />);

		expect(screen.getByRole("combobox", { name: "Planner Model" })).toBeInTheDocument();
		expect(screen.getByRole("button", { name: "Thinking Effort" })).toBeInTheDocument();
	});

  it("does not revive a stale model draft after canonical A to B to A changes", async () => {
    const user = userEvent.setup();
    const sessionA = mockUsePlanningSession().data;
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "fresh",
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
          {
            id: "github-copilot/canonical-b",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "Canonical B",
            thinking_effort_options: [{ value: "low", label: "Low" }],
          },
          {
            id: "github-copilot/local-draft",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "Local Draft",
            thinking_effort_options: [{ value: "high", label: "High" }],
          },
        ],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    const { rerender } = render(<PlanningSessionPage />);

    await user.click(screen.getByRole("combobox", { name: "Planner Model" }));
    await user.click(screen.getByRole("option", { name: "GitHub Copilot - Local Draft" }));
    expect(screen.getByRole("combobox", { name: "Planner Model" })).toHaveTextContent("Local Draft");
    expect(screen.getByRole("button", { name: "Thinking Effort" })).toHaveTextContent("High");

    mockUsePlanningSession.mockReturnValue({
      data: {
        ...sessionA,
        model_selection: {
          model: "github-copilot/canonical-b",
          thinking_effort: "low",
        },
      },
      error: null,
      refetch: refetchPlanningSession,
    });
    rerender(<PlanningSessionPage />);
    expect(screen.getByRole("combobox", { name: "Planner Model" })).toHaveTextContent("Canonical B");
    expect(screen.getByRole("button", { name: "Thinking Effort" })).toHaveTextContent("Low");

    mockUsePlanningSession.mockReturnValue({
      data: sessionA,
      error: null,
      refetch: refetchPlanningSession,
    });
    rerender(<PlanningSessionPage />);
    expect(screen.getByRole("combobox", { name: "Planner Model" })).toHaveTextContent("GPT-5.4");
    expect(screen.getByRole("button", { name: "Thinking Effort" })).toHaveTextContent("Medium");
  });

  it("does not restore a stale thinking draft after navigating away and back", async () => {
    const user = userEvent.setup();
    const sessionOne = mockUsePlanningSession().data;
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "fresh",
        items: [{
          id: "github-copilot/gpt-5.4",
          provider_id: "github-copilot",
          provider_name: "GitHub Copilot",
          model_name: "GPT-5.4",
          thinking_effort_options: [
            { value: "medium", label: "Medium" },
            { value: "high", label: "High" },
          ],
        }],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    const { rerender } = render(<PlanningSessionPage />);

    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "High" }));
    expect(screen.getByRole("button", { name: "Thinking Effort" })).toHaveTextContent("High");

    mockUseParams.mockReturnValue({ sessionId: "planning-session-2" });
    mockUsePlanningSession.mockReturnValue({
      data: {
        ...sessionOne,
        planning_session_id: "planning-session-2",
        title: "Second session",
      },
      error: null,
      refetch: refetchPlanningSession,
    });
    rerender(<PlanningSessionPage />);
    expect(screen.getByRole("button", { name: "Thinking Effort" })).toHaveTextContent("Medium");

    mockUseParams.mockReturnValue({ sessionId: "planning-session-1" });
    mockUsePlanningSession.mockReturnValue({
      data: sessionOne,
      error: null,
      refetch: refetchPlanningSession,
    });
    rerender(<PlanningSessionPage />);
    expect(screen.getByRole("button", { name: "Thinking Effort" })).toHaveTextContent("Medium");
  });

  it("starts title catch-up once for an eligible placeholder session", async () => {
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "plan_1",
        title_state: "placeholder",
        auto_title_eligible: true,
        lifecycle_state: "active",
        conversation_state: "ready",
        current_question_message_id: "assistant-1",
        current_operation_id: undefined,
        current_title_operation_id: undefined,
        current_draft_id: undefined,
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4" },
      },
      error: null,
    });

    const { rerender } = render(<PlanningSessionPage />);

    await waitFor(() => {
      expect(generateTitleMutateAsync).toHaveBeenCalledTimes(1);
    });

    rerender(<PlanningSessionPage />);

    await waitFor(() => {
      expect(generateTitleMutateAsync).toHaveBeenCalledTimes(1);
    });
  });

  it("keeps a headless subscription to the background title operation", () => {
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "plan_1",
        title_state: "placeholder",
        auto_title_eligible: false,
        lifecycle_state: "active",
        conversation_state: "ready",
        current_question_message_id: "assistant-1",
        current_operation_id: undefined,
        current_title_run_id: "title-operation-1",
        current_draft_id: undefined,
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4" },
      },
      error: null,
    });
    mockUsePlanningOperation.mockReturnValue({
      data: {
        run_id: "title-operation-1",
        session_id: "8cfa85f2-f95a-53ec-ae58-2c46f21495dc",
        planning_session_id: "planning-session-1",
        agent_name: "planner-generate-title",
        status: "in-progress",
        created_at: "2026-04-21T10:05:00Z",
      },
    });

    render(<PlanningSessionPage />);

    expect(screen.getByRole("button", { name: "Edit session title" })).toHaveTextContent("New session");
    expect(mockUsePlanningOperation.mock.calls.some(([value]) => value === "title-operation-1")).toBe(true);
    expect(
      mockUsePlannerOperation.mock.calls.some(
        ([value]) =>
          typeof value === "object" &&
          value !== null &&
          "run_id" in (value as Record<string, unknown>) &&
          (value as { run_id?: string }).run_id === "title-operation-1",
      ),
    ).toBe(true);
  });

  it("shows disabled generating button when a formalize operation is active", () => {
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "Planner kickoff",
        title_state: "custom",
        lifecycle_state: "active",
        conversation_state: "processing",
        current_question_message_id: "assistant-1",
        current_operation_id: "operation-1",
        current_draft_id: undefined,
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4" },
      },
      error: null,
      refetch: vi.fn(),
    });
    mockUsePlanningOperation.mockReturnValue({
      data: {
        operation_id: "operation-1",
        planning_session_id: "planning-session-1",
        type: "formalize",
        status: "running",
        created_at: "2026-04-21T10:05:00Z",
        request: {},
      },
    });
    mockUsePlannerOperation.mockReturnValue({
      operation: {
        kind: "formalize",
        operationId: "operation-1",
        status: "streaming",
        lastEventSeq: 1,
        pendingReply: null,
        replyText: "",
        formalizeText: "drafting",
        error: null,
        activityItems: [],
      },
      connection: { state: "open", attempt: 1 },
    });

    render(<PlanningSessionPage />);

    expect(screen.getByRole("button", { name: /Generating Plan/ })).toBeDisabled();
  });

  it("re-enables Generate Plan when an active formalize operation finalises as cancelled", () => {
    // Regression guard for the cancelled-formalize gap: before the
    // isPlannerOperationTerminal helper landed, a cancelled formalize
    // left activeOperation.status === "cancelled" but the page only
    // cleared activeOperationId on done|error — so "Generating Plan…"
    // stayed stuck on screen indefinitely after a graceful shutdown.
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "Planner kickoff",
        title_state: "custom",
        lifecycle_state: "active",
        conversation_state: "ready",
        current_question_message_id: "assistant-1",
        current_operation_id: undefined, // server already cleared on cancel
        current_draft_id: undefined,
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4" },
      },
      error: null,
      refetch: vi.fn(),
    });
    mockUsePlanningOperation.mockReturnValue({
      data: {
        operation_id: "operation-1",
        planning_session_id: "planning-session-1",
        type: "formalize",
        status: "cancelled",
        error: "operation cancelled during shutdown",
        created_at: "2026-04-21T10:05:00Z",
        request: {},
      },
    });
    mockUsePlannerOperation.mockReturnValue({
      operation: {
        kind: "formalize",
        operationId: "operation-1",
        status: "cancelled",
        lastEventSeq: 5,
        pendingReply: null,
        replyText: "",
        formalizeText: "partial draft",
        error: "operation cancelled during shutdown",
        activityItems: [],
      },
      connection: { state: "idle", attempt: 0 },
    });
    // Simulate at least one user message so the Generate Plan button
    // is allowed to be enabled (its other gates are conversation_state
    // and hasUserMessages).
    mockUsePlannerMessagePages.mockReturnValue({
      messages: [
        { role: "assistant", content_markdown: "Opening" },
        { role: "user", content_markdown: "First answer" },
      ],
      firstItemIndex: 0,
      totalCount: 2,
      hasPreviousPage: false,
      isFetchingPreviousPage: false,
      fetchPreviousPage: vi.fn(),
      error: null,
      isLoading: false,
    });

    render(<PlanningSessionPage />);

    // Button shows "Generate Plan" (not "Generating Plan...") and is
    // not disabled by formalizePending.
    expect(screen.getByRole("button", { name: "Generate Plan" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Generating Plan/ })).not.toBeInTheDocument();
  });

	it("re-enables Generate Plan when an active formalize operation finalises as error", () => {
		mockUsePlanningSession.mockReturnValue({
			data: {
				planning_session_id: "planning-session-1",
				title: "Planner kickoff",
				title_state: "custom",
				lifecycle_state: "active",
				conversation_state: "ready",
				current_question_message_id: "assistant-1",
				current_operation_id: undefined,
				current_draft_id: undefined,
				context_snapshot_id: "planning-context-1",
				created_at: "2026-04-21T10:00:00Z",
				updated_at: "2026-04-21T10:05:00Z",
				project_root: "/tmp/project",
				model_selection: { model: "github-copilot/gpt-5.4" },
			},
			error: null,
			refetch: vi.fn(),
		});
		mockUsePlanningOperation.mockReturnValue({
			data: {
				operation_id: "operation-1",
				planning_session_id: "planning-session-1",
				type: "formalize",
				status: "error",
				error: "formalize failed",
				created_at: "2026-04-21T10:05:00Z",
				request: {},
			},
		});
		mockUsePlannerOperation.mockReturnValue({
			operation: {
				kind: "formalize",
				operationId: "operation-1",
				status: "error",
				lastEventSeq: 5,
				pendingReply: null,
				replyText: "",
				formalizeText: "partial draft",
				error: "formalize failed",
				activityItems: [],
			},
			connection: { state: "idle", attempt: 0 },
		});
		mockUsePlannerMessagePages.mockReturnValue({
			messages: [
				{ role: "assistant", content_markdown: "Opening" },
				{ role: "user", content_markdown: "First answer" },
			],
			firstItemIndex: 0,
			totalCount: 2,
			hasPreviousPage: false,
			isFetchingPreviousPage: false,
			fetchPreviousPage: vi.fn(),
			error: null,
			isLoading: false,
		});

		render(<PlanningSessionPage />);

		expect(screen.getByRole("button", { name: "Generate Plan" })).toBeInTheDocument();
		expect(screen.queryByRole("button", { name: /Generating Plan/ })).not.toBeInTheDocument();
	});

  it("commits the title on blur with a single PATCH", async () => {
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "Planner kickoff updated");
    // Blur the input by tabbing out — commitTitle should fire exactly
    // once with the trimmed value. Previously the optimistic cache write
    // could collapse the dirty flag mid-debounce and suppress the PATCH.
    await user.tab();

    await waitFor(() => {
      expect(titleMutateAsync).toHaveBeenCalledWith({
        title: "Planner kickoff updated",
        key: "planning-update-title-test",
      });
    });
    expect(titleMutateAsync).toHaveBeenCalledTimes(1);
    expect(modelMutateAsync).not.toHaveBeenCalled();
  });

  it("writes optimistically to the cache before the server PATCH completes", async () => {
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "Optimistic title");
    await user.tab();

    // The optimistic write uses a functional updater so concurrent cache
    // updates survive. Find the updater that produces the expected patch.
    await waitFor(() => {
      const detailCalls = setQueryData.mock.calls.filter((call: unknown[]) => {
        const key = call[0] as unknown[] | undefined;
        return Array.isArray(key) && key[0] === "planning-sessions" && key[1] === "planning-session-1";
      });
      const optimistic = detailCalls.find((call) => {
        if (typeof call[1] !== "function") return false;
        const updater = call[1] as (current: { title?: string; title_state?: string } | undefined) =>
          { title?: string; title_state?: string } | undefined;
        const next = updater({ title: "Planner kickoff", title_state: "custom" });
        return next?.title === "Optimistic title" && next?.title_state === "custom";
      });
      expect(optimistic).toBeDefined();
    });
  });

  it("rolls back the cache and shows a retry chip when the title PATCH fails", async () => {
    const user = userEvent.setup();
    titleMutateAsync.mockRejectedValueOnce(new Error("Server unreachable"));
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "Doomed title");
    await user.tab();

    // Title mutation was attempted exactly once. The model mutation
    // must remain untouched — they are independent hooks now.
    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(1));
    expect(modelMutateAsync).not.toHaveBeenCalled();

    // Both the optimistic write and the CAS rollback are functional
    // updaters now. The optimistic patches title/title_state forward;
    // the rollback (CAS) patches them back. Distinguish by their
    // effect when applied to a representative current cache.
    await waitFor(() => {
      const detailCalls = setQueryData.mock.calls.filter((call: unknown[]) => {
        const key = call[0] as unknown[] | undefined;
        return Array.isArray(key) && key[0] === "planning-sessions" && key[1] === "planning-session-1";
      });
      const optimistic = detailCalls.find((call) => {
        if (typeof call[1] !== "function") return false;
        const updater = call[1] as (current: { title?: string; title_state?: string } | undefined) =>
          { title?: string; title_state?: string } | undefined;
        const next = updater({ title: "Planner kickoff", title_state: "custom" });
        return next?.title === "Doomed title" && next?.title_state === "custom";
      });
      const rollback = detailCalls.find((call) => {
        if (typeof call[1] !== "function") return false;
        const updater = call[1] as (current: { title?: string; title_state?: string } | undefined) =>
          { title?: string; title_state?: string } | undefined;
        // CAS rollback condition: cache must currently hold the
        // failed optimistic title; the updater then reverts to the
        // canonical value.
        const next = updater({ title: "Doomed title", title_state: "custom" });
        return next?.title === "Planner kickoff" && next?.title_state === "custom";
      });
      expect(optimistic).toBeDefined();
      expect(rollback).toBeDefined();
    });

    // The list-prefix CAS fans out via setQueriesData so every cached
    // list slice is covered, regardless of limit.
    expect(setQueriesData).toHaveBeenCalled();

    // Inline error chip is rendered with retry + discard buttons.
    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Discard" })).toBeInTheDocument();

    // Failed draft stays visible: the input remains rendered and still
    // holds the user's typed value. Previously the page collapsed back
    // to the header on blur, hiding the failed draft until the user
    // clicked into the header again. Professional UX keeps it visible.
    const stillEditing = screen.getByDisplayValue("Doomed title");
    expect(stillEditing.tagName).toBe("INPUT");
  });

  it("retry refires the same PATCH after a failure when the draft is unchanged", async () => {
    const user = userEvent.setup();
    titleMutateAsync
      .mockRejectedValueOnce(new Error("Server unreachable"))
      .mockResolvedValueOnce({});
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "Retry me");
    await user.tab();

    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(1));
    await user.click(await screen.findByRole("button", { name: "Retry" }));

    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(2));
    // The retry now reads the *live* titleInput at click time. With no
    // intervening edits, that's the same failed value — but the next
    // test confirms it's not the *captured* failed value either.
    expect(titleMutateAsync).toHaveBeenLastCalledWith(
      expect.objectContaining({ title: "Retry me" }),
    );
  });

  it("retry uses the latest draft, not the originally failed value", async () => {
    const user = userEvent.setup();
    titleMutateAsync
      .mockRejectedValueOnce(new Error("Server unreachable"))
      .mockResolvedValueOnce({});
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "v1");
    await user.tab();

    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(1));

    // Failed input is still visible. Edit further, then retry.
    const stillEditing = screen.getByDisplayValue("v1");
    await user.clear(stillEditing);
    await user.type(stillEditing, "v2 final");

    await user.click(await screen.findByRole("button", { name: "Retry" }));

    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(2));
    expect(titleMutateAsync).toHaveBeenLastCalledWith(
      expect.objectContaining({ title: "v2 final" }),
    );
  });

  it("discard never re-fires the abandoned commit", async () => {
    const user = userEvent.setup();
    titleMutateAsync.mockRejectedValueOnce(new Error("Server unreachable"));
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "Throwaway");
    await user.tab();

    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(1));

    await user.click(await screen.findByRole("button", { name: "Discard" }));

    // Error alert is gone.
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    // Header reflects the server-confirmed canonical title again.
    expect(screen.getByText("Planner kickoff")).toBeInTheDocument();
    // Crucially: the discard click did NOT cause a second mutation.
    // The abandonEdit reducer action atomically resets draft + exits
    // edit mode, so no follow-up commit fires for the abandoned value.
    expect(titleMutateAsync).toHaveBeenCalledTimes(1);
  });

  it("commits the title on Enter with a single PATCH", async () => {
    const user = userEvent.setup();
    render(<PlanningSessionPage />);

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "Planner kickoff via Enter{Enter}");

    await waitFor(() => {
      expect(titleMutateAsync).toHaveBeenCalledWith({
        title: "Planner kickoff via Enter",
        key: "planning-update-title-test",
      });
    });
    expect(titleMutateAsync).toHaveBeenCalledTimes(1);
  });

  it("persists model selection even while the title is still placeholder/blank", async () => {
    const user = userEvent.setup();
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "plan_1",
        title_state: "placeholder",
        lifecycle_state: "active",
        conversation_state: "ready",
        current_question_message_id: "assistant-1",
        current_operation_id: undefined,
        current_draft_id: undefined,
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:00Z",
        project_root: "/tmp/project",
        model_selection: {
          model: "github-copilot/gpt-5.4",
        },
      },
      error: null,
      refetch: vi.fn(),
    });

    render(<PlanningSessionPage />);

    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "Medium" }));

    // Autosave fires through .mutate() (fire-and-forget) — not
    // mutateAsync — so unhandled rejections don't leak. .mutateAsync
    // would also work but routes errors to a returned promise the
    // caller has to handle; .mutate routes them to hook.error and
    // the dedicated chip, which is the surface we want.
    await waitFor(() => {
      expect(modelMutate).toHaveBeenCalledWith(
        {
          modelSelection: {
            model: "github-copilot/gpt-5.4",
            thinking_effort: "medium",
          },
          key: "planning-update-model-test",
          editGeneration: 1,
        },
        expect.objectContaining({ onSuccess: expect.any(Function) }),
      );
    });
    expect(modelMutateAsync).not.toHaveBeenCalled();
    expect(titleMutateAsync).not.toHaveBeenCalled();
  });

	it("consumes ?show_plan=true per session instead of once per component lifetime", async () => {
		mockUseSearch.mockReturnValue({ show_plan: true });
		mockUsePlanningSession.mockReturnValue({
			data: {
				planning_session_id: "planning-session-1",
				title: "Planner kickoff",
				title_state: "custom",
				lifecycle_state: "active",
				conversation_state: "ready",
				current_question_message_id: "assistant-1",
				current_operation_id: undefined,
				current_draft_id: "draft-1",
				context_snapshot_id: "planning-context-1",
				created_at: "2026-04-21T10:00:00Z",
				updated_at: "2026-04-21T10:05:00Z",
				project_root: "/tmp/project",
				model_selection: { model: "github-copilot/gpt-5.4", thinking_effort: "medium" },
			},
			error: null,
			refetch: vi.fn(),
		});
		mockUsePlanningSessionDraft.mockReturnValue({ data: { draft_id: "draft-1" } });

		const { rerender } = render(<PlanningSessionPage />);

		await waitFor(() => {
			expect(mockNavigate).toHaveBeenCalledTimes(1);
		});
		expect(screen.getByText("Plan Modal")).toBeInTheDocument();

		mockUseParams.mockReturnValue({ sessionId: "planning-session-2" });
		mockUsePlanningSession.mockReturnValue({
			data: {
				planning_session_id: "planning-session-2",
				title: "Planner follow-up",
				title_state: "custom",
				lifecycle_state: "active",
				conversation_state: "ready",
				current_question_message_id: "assistant-2",
				current_operation_id: undefined,
				current_draft_id: "draft-2",
				context_snapshot_id: "planning-context-2",
				created_at: "2026-04-21T11:00:00Z",
				updated_at: "2026-04-21T11:05:00Z",
				project_root: "/tmp/project",
				model_selection: { model: "github-copilot/gpt-5.4", thinking_effort: "medium" },
			},
			error: null,
			refetch: vi.fn(),
		});
		mockUsePlanningSessionDraft.mockReturnValue({ data: { draft_id: "draft-2" } });

		rerender(<PlanningSessionPage />);

		await waitFor(() => {
			expect(mockNavigate).toHaveBeenCalledTimes(2);
		});
	});

  it("model-selection failure shows a dedicated chip and never the title chip", async () => {
    const user = userEvent.setup();
    // Multi-option mock so the user can dirty the dropdown to a
    // value distinct from canonical "medium". The chip render gate
    // (`hasModelSelectionChanges && modelError`) requires real dirty
    // state — a stale error against canonical no longer renders,
    // because the Retry button would just resend the canonical value.
    mockUseProviderModels.mockReturnValue({
      data: {
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [
              { value: "low", label: "Low" },
              { value: "medium", label: "Medium" },
              { value: "high", label: "High" },
            ],
          },
        ],
      },
      isLoading: false,
    });
    mockUseUpdatePlanningSessionModelSelection.mockReturnValue({
      mutate: modelMutate,
      mutateAsync: modelMutateAsync,
      reset: modelReset,
      error: new Error("Server unreachable"),
    });

    render(<PlanningSessionPage />);

    // Dirty the dropdown so the render gate opens.
    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "High" }));

    // The dedicated model chip is rendered with a Retry button; the
    // title flow remains untouched because the two hooks have
    // independent error slots now.
    const chip = await screen.findByRole("alert");
    expect(chip).toHaveTextContent(/model selection/i);
    expect(chip).toHaveTextContent(/Server unreachable/i);

    // Only the model chip is rendered — no title chip alongside.
    expect(screen.queryAllByRole("alert")).toHaveLength(1);

    // Clicking Retry inside the chip re-fires the model mutation
    // through .mutate(). The retry button switched away from
    // mutateAsync alongside the autosave path so unhandled rejections
    // can't leak from a fire-and-forget call. The dirty state means
    // the autosave debounce ALSO queued a call, but both calls go
    // through .mutate() and the supersede pattern in useSlicedMutation
    // already handles ordering — the assertion only requires that the
    // retry click was the LAST call.
    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(modelMutate).toHaveBeenLastCalledWith(
      expect.objectContaining({
        modelSelection: expect.objectContaining({ model: "github-copilot/gpt-5.4" }),
      }),
		expect.objectContaining({ onSuccess: expect.any(Function) }),
    );
    expect(titleMutateAsync).not.toHaveBeenCalled();
  });

  it("shows planner model catalog unavailable state distinctly", () => {
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

    render(<PlanningSessionPage />);

    expect(screen.getByText("provider model catalog unavailable")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "Planner Model" })).toBeDisabled();
  });

  it("preserves the canonical model label and does not autosave away thinking effort when the catalog is unavailable", async () => {
    vi.useFakeTimers();
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

    render(<PlanningSessionPage />);

    expect(screen.getByRole("combobox", { name: "Planner Model" })).toHaveTextContent("github-copilot/gpt-5.4");

    await vi.advanceTimersByTimeAsync(750);

    expect(modelMutate).not.toHaveBeenCalled();
  });

  it("says nothing about a stale catalog while its models are still selectable", () => {
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "stale",
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
        failure_reason: "command_failed",
        failure_message: "Showing the models Kingdom already had.",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<PlanningSessionPage />);

    // The planner header is always on screen above the transcript, so a notice
    // here costs a full row for the life of the session. It has to earn that by
    // telling the operator something they can act on.
    expect(screen.queryByText("Showing the models Kingdom already had.")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry" })).not.toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "Planner Model" })).not.toBeDisabled();
  });

  it("stays silent when a background refresh fails but cached planner models remain", () => {
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "fresh",
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
      },
      isLoading: false,
      error: new Error("provider model catalog refresh failed"),
      refetch: vi.fn(),
    });

    render(<PlanningSessionPage />);

    expect(screen.queryByText("provider model catalog refresh failed")).not.toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "Planner Model" })).not.toBeDisabled();
  });

  it("uses a live refresh when retrying an unreachable planner catalog", async () => {
    const user = userEvent.setup();
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "unavailable",
        items: [],
        failure_reason: "timeout",
        failure_message: "planner model catalog unavailable",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<PlanningSessionPage />);
    await user.click(screen.getByRole("button", { name: "Retry" }));

    expect(refreshProviderModelsMutate).toHaveBeenCalledTimes(1);
  });

  it("shows a planner retry failure in place of the copy it replaced", async () => {
    const user = userEvent.setup();
    mockUseProviderModels.mockReturnValue({
      data: {
        catalog_status: "unavailable",
        items: [],
        failure_reason: "timeout",
        failure_message: "planner model catalog unavailable",
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      dataUpdatedAt: 1,
    });
    refreshProviderModelsMutate.mockImplementation((_vars, callbacks) => {
      callbacks?.onError?.(new Error("planner refresh failed"), undefined, undefined);
    });

    render(<PlanningSessionPage />);
    await user.click(screen.getByRole("button", { name: "Retry" }));

    expect(await screen.findByText("planner refresh failed")).toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "Planner Model" })).toBeDisabled();
  });

  it("concurrent model edit + title save preserves the dirty model selection", async () => {
    // Load-bearing regression test. Before the field-scoped + narrow-
    // deps fix, two compounding bugs would erase the user's pending
    // model edit when they committed a title change before the 500ms
    // model autosave fired:
    //
    //   1. commitTitle did `{...session, title, title_state}` from a
    //      closure-captured snapshot → clobbered concurrent SSE
    //      updates AND mutated the cache reference.
    //   2. The model-sync useEffect depended on the whole `session`
    //      object → fired on every cache update, including the title
    //      save's onSuccess writing the server-canonical response
    //      (still carrying the pre-edit model_selection because the
    //      autosave hadn't run yet) into the cache.
    //
    // The fix: field-scoped optimistic write, narrow string deps on
    // the reseed effect. This test pins the user-visible outcome —
    // local "high" must survive a title save whose response carries
    // server-canonical "medium".
    const user = userEvent.setup();

    // Multi-option mock so the user can actually pick a value
    // different from the canonical "medium". The previous version
    // of this test only had one option, so clicking it produced no
    // dirty state and the test passed vacuously.
    mockUseProviderModels.mockReturnValue({
      data: {
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [
              { value: "low", label: "Low" },
              { value: "medium", label: "Medium" },
              { value: "high", label: "High" },
            ],
          },
        ],
      },
      isLoading: false,
    });

    // Title save's server response echoes the SERVER-CANONICAL
    // model_selection (still "medium" because the user's autosave
    // for "high" hasn't fired). Without the narrow deps fix, this
    // would reseed setThinkingEffort to "medium" and snap the UI
    // back, blowing away the user's pending "high".
    titleMutateAsync.mockResolvedValueOnce({
      planning_session_id: "planning-session-1",
      title: "fresh title",
      title_state: "custom",
      lifecycle_state: "active",
      conversation_state: "ready",
      current_question_message_id: "assistant-1",
      current_operation_id: undefined,
      current_draft_id: undefined,
      context_snapshot_id: "planning-context-1",
      created_at: "2026-04-21T10:00:00Z",
      updated_at: "2026-04-21T10:05:00Z",
      project_root: "/tmp/project",
      model_selection: { model: "github-copilot/gpt-5.4", thinking_effort: "medium" },
    });

    render(<PlanningSessionPage />);

    // Sanity: dropdown starts at canonical "Medium".
    const initialButton = screen.getByRole("button", { name: "Thinking Effort" });
    expect(initialButton).toHaveTextContent(/Medium/i);

    // User picks "High" — the divergent value. The 500ms autosave
    // debounce hasn't fired yet, so this lives only in local state.
    await user.click(initialButton);
    await user.click(screen.getByRole("option", { name: "High" }));

    // Pre-save assertion: the local state diverged from canonical.
    // The dropdown shows "High" while the cache (and any onSuccess
    // response) still carries server-canonical "medium". This proves
    // the test is actually exercising the regression — without this
    // assertion the test would pass even if local state was reset
    // immediately on click.
    const dirtyButton = screen.getByRole("button", { name: "Thinking Effort" });
    expect(dirtyButton).toHaveTextContent(/High/i);

    // Commit a title edit immediately, before the 500ms autosave
    // fires. titleMutateAsync resolves; api/hooks's onSuccess
    // applies field-scoped { title, title_state } only — model
    // fields untouched. Narrow deps mean the model-sync useEffect
    // doesn't re-fire setThinkingEffort.
    await user.click(screen.getByText("Planner kickoff"));
    const titleInput = screen.getByDisplayValue("Planner kickoff");
    await user.clear(titleInput);
    await user.type(titleInput, "fresh title");
    await user.tab();

    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(1));

    // Post-save assertion: dropdown still shows "High". The dirty
    // local state survived the title save's onSuccess.
    const finalButton = screen.getByRole("button", { name: "Thinking Effort" });
    expect(finalButton).toHaveTextContent(/High/i);
    expect(finalButton).not.toHaveTextContent(/Medium/i);
  });

  it("model autosave timer survives an unrelated session-reference rerender", async () => {
    // Regression guard for the autosave-debounce restart bug. Before
    // narrowing the effect deps off `session`, any cache patch that
    // produced a new session object reference (SSE updates, sibling
    // reconciliation, terminal events, even the title save's own
    // onSuccess writing the response back into the cache) would tear
    // down the 500ms timer and start a new one — at <500ms cadence
    // the autosave could be starved indefinitely. The fix: deps are
    // [model, thinkingEffort, hasModelSelectionChanges, editGeneration,
    // submitModelSelection]; the session reference no longer participates.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

    mockUseProviderModels.mockReturnValue({
      data: {
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [
              { value: "low", label: "Low" },
              { value: "medium", label: "Medium" },
              { value: "high", label: "High" },
            ],
          },
        ],
      },
      isLoading: false,
    });

    const { rerender } = render(<PlanningSessionPage />);

    // Dirty the dropdown — autosave timer is now armed for 500ms.
    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "High" }));

    // Advance 250ms — halfway through the debounce. No autosave yet.
    act(() => vi.advanceTimersByTime(250));
    expect(modelMutate).not.toHaveBeenCalled();

    // Sibling cache update lands: a NEW session object (different
    // reference, same canonical model_selection — e.g. an SSE patch
    // updating current_operation_id from a parallel formalize). Before
    // the fix, this rerender's new `session` reference would have
    // cleared the 250ms-deep timer and started a fresh 500ms one.
    mockUsePlanningSession.mockReturnValue({
      data: {
        planning_session_id: "planning-session-1",
        title: "Planner kickoff",
        title_state: "custom",
        lifecycle_state: "active",
        conversation_state: "processing",
        current_question_message_id: "assistant-1",
        current_operation_id: "op-from-sibling",
        current_draft_id: undefined,
        context_snapshot_id: "planning-context-1",
        created_at: "2026-04-21T10:00:00Z",
        updated_at: "2026-04-21T10:05:30Z",
        project_root: "/tmp/project",
        model_selection: { model: "github-copilot/gpt-5.4", thinking_effort: "medium" },
      },
      error: null,
      refetch: vi.fn(),
    });
    rerender(<PlanningSessionPage />);

    // Advance another 250ms — total 500ms since the dropdown change.
    // The original timer must still resolve at 500ms; if the rerender
    // had restarted it, the autosave would not yet have fired.
    await act(async () => vi.advanceTimersByTime(250));

    expect(modelMutate).toHaveBeenCalledTimes(1);
    expect(modelMutate).toHaveBeenCalledWith(
      {
        modelSelection: {
          model: "github-copilot/gpt-5.4",
          thinking_effort: "high",
        },
        key: "planning-update-model-test",
        editGeneration: 1,
      },
      expect.objectContaining({ onSuccess: expect.any(Function) }),
    );
  });

  it("preserves a later local model edit when an earlier autosave resolves during its debounce", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    const initialSession = mockUsePlanningSession().data;
    mockUseProviderModels.mockReturnValue({
      data: {
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [
              { value: "low", label: "Low" },
              { value: "medium", label: "Medium" },
              { value: "high", label: "High" },
            ],
          },
        ],
      },
      isLoading: false,
    });
    const { rerender } = render(<PlanningSessionPage />);

    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "High" }));
    await act(async () => vi.advanceTimersByTime(500));
    expect(modelMutate).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "Low" }));
    await act(async () => vi.advanceTimersByTime(250));
    const firstAutosaveCallbacks = modelMutate.mock.calls[0]?.[1] as {
      onSuccess?: (result: typeof initialSession) => void;
    };
    act(() => firstAutosaveCallbacks.onSuccess?.({
      ...initialSession,
      model_selection: {
        model: "github-copilot/gpt-5.4",
        thinking_effort: "high",
      },
    }));

    mockUsePlanningSession.mockReturnValue({
      data: {
        ...initialSession,
        model_selection: {
          model: "github-copilot/gpt-5.4",
          thinking_effort: "high",
        },
      },
      error: null,
      refetch: refetchPlanningSession,
    });
    rerender(<PlanningSessionPage />);

    expect(screen.getByRole("button", { name: "Thinking Effort" })).toHaveTextContent("Low");

    await act(async () => vi.advanceTimersByTime(250));
    expect(modelMutate).toHaveBeenCalledTimes(2);
    expect(modelMutate).toHaveBeenLastCalledWith(
      {
        modelSelection: {
          model: "github-copilot/gpt-5.4",
          thinking_effort: "low",
        },
        key: "planning-update-model-test",
        editGeneration: 2,
      },
      expect.objectContaining({ onSuccess: expect.any(Function) }),
    );
  });

  it("model chip disappears and reset() fires when the user reverts to canonical", async () => {
    // Reverting to the canonical value should hide the stale error chip and
    // reset mutation state before the next dirty edit starts autosaving.
    const user = userEvent.setup();
    mockUseProviderModels.mockReturnValue({
      data: {
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [
              { value: "low", label: "Low" },
              { value: "medium", label: "Medium" },
              { value: "high", label: "High" },
            ],
          },
        ],
      },
      isLoading: false,
    });
    mockUseUpdatePlanningSessionModelSelection.mockReturnValue({
      mutate: modelMutate,
      mutateAsync: modelMutateAsync,
      reset: modelReset,
      error: new Error("Server unreachable"),
    });

    render(<PlanningSessionPage />);

    // Dirty the dropdown so the chip renders.
    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "High" }));

    // Chip is visible while dirty.
    expect(await screen.findByRole("alert")).toHaveTextContent(/model selection/i);

    // User reverts to canonical "Medium". Chip render gate slams
    // shut, AND the reset effect fires — the mutation hook becomes
    // a clean slate for the next dirty edit.
    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "Medium" }));

    await waitFor(() => {
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    });
    expect(modelReset).toHaveBeenCalled();
  });
  it("title and model errors render independently with separate retry buttons", async () => {
    const user = userEvent.setup();
    titleMutateAsync.mockRejectedValueOnce(new Error("Title patch failed"));
    // Multi-option model so the model chip's render gate
    // (`hasModelSelectionChanges`) can be opened by a dropdown change.
    mockUseProviderModels.mockReturnValue({
      data: {
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [
              { value: "low", label: "Low" },
              { value: "medium", label: "Medium" },
              { value: "high", label: "High" },
            ],
          },
        ],
      },
      isLoading: false,
    });
    mockUseUpdatePlanningSessionModelSelection.mockReturnValue({
      mutate: modelMutate,
      mutateAsync: modelMutateAsync,
      reset: modelReset,
      error: new Error("Model patch failed"),
    });

    render(<PlanningSessionPage />);

    // Dirty the dropdown so the model chip's render gate opens, then
    // trigger the title failure path independently.
    await user.click(screen.getByRole("button", { name: "Thinking Effort" }));
    await user.click(screen.getByRole("option", { name: "High" }));

    await user.click(screen.getByText("Planner kickoff"));
    const input = screen.getByDisplayValue("Planner kickoff");
    await user.clear(input);
    await user.type(input, "doomed");
    await user.tab();

    await waitFor(() => expect(titleMutateAsync).toHaveBeenCalledTimes(1));

    // Two distinct chips on screen — one for title, one for model.
    const chips = await screen.findAllByRole("alert");
    expect(chips).toHaveLength(2);
    const messages = chips.map((c) => c.textContent ?? "");
    expect(messages.some((m) => /Title patch failed/i.test(m))).toBe(true);
    expect(messages.some((m) => /Model patch failed/i.test(m))).toBe(true);
  });

});
