import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { DecreePanel } from "./decree-panel";

const useDecreeMock = vi.fn();
const useProviderModelsMock = vi.fn();
const refetchDecree = vi.fn();

vi.mock("@/api/hooks", () => ({
  useDecree: (...args: unknown[]) => useDecreeMock(...args),
  useProviderModels: (...args: unknown[]) => useProviderModelsMock(...args),
  newIdempotencyKey: () => "refresh-key",
}));

vi.mock("@/features/connections/external-source-comparison", () => ({
  ExternalSourceComparison: ({ baselineId, candidateId }: { baselineId: string; candidateId: string }) => <div aria-label="GitHub source comparison" data-baseline={baselineId} data-candidate={candidateId}>Source comparison</div>,
}));

const baseRun = {
  run_id: "run-1",
  decree_id: "decree-1",
  decree_title: "Royal Landing Page",
  workflow_id: "royal-decree-v1",
  status: "running",
  display_state: "running",
  plan_state: "approved",
  created_at: "2026-03-06T10:00:00.000Z",
  updated_at: "2026-03-06T10:01:00.000Z",
  last_activity_at: "2026-03-06T10:01:00.000Z",
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
      "builder-berserker": {
        model: "opencode/gpt-5-nano",
      },
    },
  },
} as const;

const baseRoles = [
  {
    role_id: "builder-berserker",
    display_name: "Berserker",
    description: "",
    can_spawn: false,
    agent_model_selector_visible: true,
    agent_model_selector_section_id: "builder",
    agent_model_selector_section_label: "Builder",
    agent_model_selector_section_order: 50,
    agent_model_selector_role_order: 10,
  },
  {
    role_id: "chancellor",
    display_name: "Chancellor",
    description: "",
    can_spawn: true,
    agent_model_selector_visible: true,
    agent_model_selector_section_id: "chancellor",
    agent_model_selector_section_label: "Chancellor",
    agent_model_selector_section_order: 30,
    agent_model_selector_role_order: 10,
  },
];

describe("decree panel", () => {
  afterEach(() => vi.unstubAllGlobals());
  beforeEach(() => {
    useDecreeMock.mockReset();
    useProviderModelsMock.mockReset();
    refetchDecree.mockReset();
    useDecreeMock.mockReturnValue({
      data: {
        decree_id: "decree-1",
        decree_title: "Royal Landing Page",
        decree_text: "Build the new experience.",
        metadata: {
          planning_session_id: "planning-session-1",
        },
        created_at: "2026-03-06T10:00:00.000Z",
      },
      isLoading: false,
      error: null,
      refetch: refetchDecree,
    });
    useProviderModelsMock.mockReturnValue({
      data: undefined,
      isLoading: false,
    });
  });

  it("shows the decree title inline and opens the full decree in a read modal", async () => {
    const user = userEvent.setup();
    render(
      <DecreePanel
        run={baseRun}
        roles={baseRoles}
      />,
    );

    // The sidebar stays compact: title is shown, but the body lives behind a modal.
    expect(screen.getByText("Royal Landing Page")).toBeInTheDocument();
    expect(screen.queryByText("Build the new experience.")).not.toBeInTheDocument();
    expect(screen.queryByText("Metadata")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Open decree settings" })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Read royal decree" }));

    const dialog = screen.getByRole("dialog", { name: "Royal decree" });
    expect(within(dialog).getByText("Build the new experience.")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Close dialog" }));
    expect(screen.queryByRole("dialog", { name: "Royal decree" })).not.toBeInTheDocument();
  });

  it("opens a decree settings modal with structured run configuration", async () => {
    const user = userEvent.setup();
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

    render(<DecreePanel run={baseRun} roles={baseRoles} />);

    await user.click(screen.getByRole("button", { name: "Open decree settings" }));

	const dialog = screen.getByRole("dialog", { name: "Decree settings" });
	const scrollRegion = within(dialog).getByTestId("decree-settings-scroll-region");

	expect(dialog).toBeInTheDocument();
	expect(within(dialog).getByText("Decree Settings")).toBeInTheDocument();
	expect(within(dialog).getByText("Royal Landing Page")).toBeInTheDocument();
    expect(within(dialog).getByText("Workflow, approval mode, review flow, and publish target.")).toBeInTheDocument();
    expect(within(dialog).getByText("Default model selection and any role-specific overrides recorded for this run.")).toBeInTheDocument();
    expect(scrollRegion).toHaveClass("min-h-0");
    expect(scrollRegion).toHaveClass("flex-1");
    expect(scrollRegion).toHaveClass("overflow-y-auto");
    expect(screen.getByText("Git worktree")).toBeInTheDocument();
    expect(within(dialog).getByText("Default for all agents")).toBeInTheDocument();
    expect(screen.getByText("GitHub Copilot - GPT-5.3-Codex")).toBeInTheDocument();
    expect(screen.getByText("OpenCode - GPT-5 Nano")).toBeInTheDocument();
    expect(within(dialog).getByText("Role Overrides")).toBeInTheDocument();
    expect(screen.getByText("Berserker")).toBeInTheDocument();
    expect(screen.getByText("Thinking: High")).toBeInTheDocument();
    expect(within(dialog).queryByText("Global Model")).not.toBeInTheDocument();
    expect(within(dialog).queryByText("Builder")).not.toBeInTheDocument();
    expect(within(dialog).queryByText("builder-berserker")).not.toBeInTheDocument();
    expect(within(dialog).queryByText("run-1")).not.toBeInTheDocument();
    expect(within(dialog).queryByText("royal-decree-v1")).not.toBeInTheDocument();

	await user.click(screen.getByRole("button", { name: "Close dialog" }));

    expect(screen.queryByRole("dialog", { name: "Decree settings" })).not.toBeInTheDocument();
  });

  it("falls back to raw ids and skip-review wording when lookup data is unavailable", async () => {
    const user = userEvent.setup();
    useProviderModelsMock.mockReturnValue({
      data: undefined,
      isLoading: true,
    });

    render(
      <DecreePanel
        run={{
          ...baseRun,
          workflow_id: "royal-decree-skip-review-validation-v1",
          publish_mode: "direct",
          model_selection: {
            global: { model: "github-copilot/gpt-5.4-mini" },
            role_overrides: {
              "unknown-role": { model: "custom/model-id", thinking_effort: "low" },
            },
          },
        }}
        roles={[]}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Open decree settings" }));

    expect(screen.getByText("Skip reviews and validations")).toBeInTheDocument();
    expect(screen.queryByText("Limits")).not.toBeInTheDocument();
    expect(screen.getByText("Direct to project")).toBeInTheDocument();
    expect(screen.getByText("github-copilot/gpt-5.4-mini")).toBeInTheDocument();
    expect(screen.getByText("Unknown Role")).toBeInTheDocument();
    expect(screen.queryByText("unknown-role")).not.toBeInTheDocument();
    expect(screen.getByText("custom/model-id")).toBeInTheDocument();
    expect(screen.getByText("Thinking: Low")).toBeInTheDocument();
    expect(screen.getByText("Resolving model labels...")).toBeInTheDocument();
  });

	  it("falls back to standard review wording without exposing unknown workflow ids", async () => {
	    const user = userEvent.setup();

    render(
      <DecreePanel
        run={{
          ...baseRun,
          workflow_id: "my-custom-workflow-v7",
          max_execution_review_cycles: 0,
          max_validation_rework_cycles: 0,
        }}
        roles={baseRoles}
      />,
    );

	    await user.click(screen.getByRole("button", { name: "Open decree settings" }));

	    const dialog = screen.getByRole("dialog", { name: "Decree settings" });
	    expect(within(dialog).getByText("Standard review flow")).toBeInTheDocument();
	    expect(within(dialog).queryByText("my-custom-workflow-v7")).not.toBeInTheDocument();
	  });

  it("shows every issued source and refreshes their comparisons independently without changing decree text", async () => {
    useDecreeMock.mockReturnValue({
      data: {
        decree_id: "decree-1", decree_title: "Royal Landing Page", decree_text: "Immutable operator text.", created_at: "2026-03-06T10:00:00.000Z",
        external_sources: [
          { link_id: "link-1", provider: "github", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "unchanged" },
          { link_id: "link-2", provider: "github", repository: "owner/other", issue_number: 8, canonical_url: "https://github.com/owner/other/issues/8", used_snapshot_id: "snapshot-c", latest_snapshot_id: "snapshot-c", status: "unchanged" },
        ],
      },
      isLoading: false, error: null, refetch: refetchDecree,
    });
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const second = String(input).includes("link-2");
      return new Response(JSON.stringify({
        link: { link_id: second ? "link-2" : "link-1", target_kind: "decree", target_id: "decree-1", source_id: second ? "source-2" : "source-1", used_snapshot_id: second ? "snapshot-c" : "snapshot-a", latest_snapshot_id: second ? "snapshot-d" : "snapshot-b", status: "changed", row_version: 2, provider: "github", repository: second ? "owner/other" : "owner/repo", canonical_url: second ? "https://github.com/owner/other/issues/8" : "https://github.com/owner/repo/issues/7", issue_number: second ? 8 : 7, created_at: "2026-03-06T10:00:00Z" },
        outcome: "changed", snapshot_created: true, changes: { title: true, body: true, state: false, repository: false, issue_number: false, canonical_url: false, created_at: false, updated_at: true, comment_count: false, comments: false }, message: second ? "Second source changed." : "First source changed.",
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<DecreePanel run={baseRun} roles={baseRoles} />);
    await user.click(screen.getByRole("button", { name: "Read royal decree" }));
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
    await waitFor(() => expect(refetchDecree).toHaveBeenCalledTimes(2));
  });

  it("retains revoked source recovery guidance after decree detail reload", async () => {
    useDecreeMock.mockReturnValue({
      data: {
        decree_id: "decree-1", decree_title: "Royal Landing Page", decree_text: "Immutable operator text.", created_at: "2026-03-06T10:00:00.000Z",
        external_sources: [{ link_id: "link-1", provider: "github", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "revoked" }],
      },
      isLoading: false, error: null, refetch: refetchDecree,
    });
    const user = userEvent.setup();
    render(<DecreePanel run={baseRun} roles={baseRoles} />);

    await user.click(screen.getByRole("button", { name: "Read royal decree" }));

    expect(screen.getByText(/GitHub App access was revoked/i)).toBeInTheDocument();
    expect(screen.getByText(/run the connection health check/i)).toBeInTheDocument();
    expect(screen.getByText("Immutable operator text.")).toBeInTheDocument();
  });

  it("retries an issued-source pending reservation with the same key", async () => {
    useDecreeMock.mockReturnValue({
      data: {
        decree_id: "decree-1", decree_title: "Royal Landing Page", decree_text: "Immutable operator text.", created_at: "2026-03-06T10:00:00.000Z",
        external_sources: [{ link_id: "link-1", provider: "github", repository: "owner/repo", issue_number: 7, canonical_url: "https://github.com/owner/repo/issues/7", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "unchanged" }],
      },
      isLoading: false, error: null, refetch: refetchDecree,
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
        link: { link_id: "link-1", target_kind: "decree", target_id: "decree-1", source_id: "source-1", used_snapshot_id: "snapshot-a", latest_snapshot_id: "snapshot-a", status: "unchanged", row_version: 2, provider: "github", repository: "owner/repo", canonical_url: "https://github.com/owner/repo/issues/7", issue_number: 7, created_at: "2026-03-06T10:00:00Z" },
        outcome: "unchanged", snapshot_created: false, changes: {}, message: "Source is current.",
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    render(<DecreePanel run={baseRun} roles={baseRoles} />);
    await user.click(screen.getByRole("button", { name: "Read royal decree" }));
    await user.click(screen.getByRole("button", { name: "Check GitHub source owner/repo#7" }));

    await user.click(await screen.findByRole("button", { name: "Retry GitHub source owner/repo#7" }));

    expect(await screen.findByText("Source is current.")).toBeInTheDocument();
    expect(keys).toHaveLength(2);
    expect(keys[1]).toBe(keys[0]);
  });
});
