import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { DecreePanel } from "./decree-panel";

const useDecreeMock = vi.fn();
const useProviderModelsMock = vi.fn();

vi.mock("@/api/hooks", () => ({
  useDecree: (...args: unknown[]) => useDecreeMock(...args),
  useProviderModels: (...args: unknown[]) => useProviderModelsMock(...args),
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
  beforeEach(() => {
    useDecreeMock.mockReset();
    useProviderModelsMock.mockReset();
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
    });
    useProviderModelsMock.mockReturnValue({
      data: undefined,
      isLoading: false,
    });
  });

  it("does not render decree metadata in the run sidebar", () => {
    render(
      <DecreePanel
        run={baseRun}
        roles={baseRoles}
      />,
    );

    expect(screen.getByText("Royal Landing Page")).toBeInTheDocument();
    expect(screen.getByText("Build the new experience.")).toBeInTheDocument();
    expect(screen.queryByText("Metadata")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Open decree settings" })).toBeInTheDocument();
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
});
