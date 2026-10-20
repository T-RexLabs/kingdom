import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import userEvent from "@testing-library/user-event";
import { AgentsPage } from "./agents-page";

const useAgentsMock = vi.fn();
const useRolesMock = vi.fn();
const useAgentConfigurationMock = vi.fn();
const updateRoleMock = vi.fn();
const resetRoleMock = vi.fn();
const updateWorkspaceVoiceMock = vi.fn();
const refreshThemeCatalogMock = vi.fn();
const themeCatalogItemsMock = vi.fn();
const promptCatalogDigest = "a".repeat(64);
vi.mock("@/api/hooks", () => ({
  useAgents: () => useAgentsMock(),
  useRoles: () => useRolesMock(),
  useAgentConfiguration: () => useAgentConfigurationMock(),
  useUpdateAgentRoleConfiguration: () => ({ mutateAsync: updateRoleMock, isPending: false }),
  useResetAgentRoleGuidance: () => {
    const [isPending, setPending] = useState(false);
    return {
      isPending,
      mutateAsync: async (...args: unknown[]) => {
        setPending(true);
        try {
          return await resetRoleMock(...args);
        } finally {
          setPending(false);
        }
      },
    };
  },
  useUpdateWorkspaceAgentVoice: () => ({ mutateAsync: updateWorkspaceVoiceMock, isPending: false }),
}));
vi.mock("@/theme", () => ({
  ThemeArtwork: () => null,
  ThemeIcon: () => <span aria-hidden="true" />,
  RolePortrait: () => null,
  useThemeCopy: () => "The Court",
  useTheme: () => ({ catalog: { items: themeCatalogItemsMock() }, refreshCatalog: refreshThemeCatalogMock }),
  useRoleIdentity: (role: string) => ({
    name: role === "governance" ? "Regent" : "Operator",
    canonicalName: role === "governance" ? "Governance" : "Operator",
    description: "Coordinates the work.",
    lore: "A clear path forward.",
  }),
}));

describe("agents page", () => {
  beforeEach(() => {
    updateRoleMock.mockReset().mockResolvedValue({});
    resetRoleMock.mockReset().mockResolvedValue({ row_version: 1, effective_guidance: "Default role guidance." });
    updateWorkspaceVoiceMock.mockReset().mockResolvedValue({});
    refreshThemeCatalogMock.mockReset();
    themeCatalogItemsMock.mockReturnValue([]);
    useAgentConfigurationMock.mockReturnValue({
      data: {
        prompt_catalog_digest: promptCatalogDigest,
        workspace: { voice: { source: { kind: "neutral-v1" } }, row_version: 1 },
        roles: [{
          role_id: "governance",
          default_guidance: "Default role guidance.",
          guidance_override: null,
          effective_guidance: "Default role guidance.",
          effective_voice: { source: { kind: "neutral-v1" } },
          row_version: 0,
        }],
      },
      isLoading: false,
      refetch: vi.fn(),
    });
    useAgentsMock.mockReturnValue({
      data: {
        items: [
          { agent_id: "g1", role_id: "governance", status: "busy" },
          { agent_id: "g2", role_id: "governance", status: "busy" },
          { agent_id: "human", role_id: "operator", status: "idle" },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    });
    useRolesMock.mockReturnValue({
      data: {
        items: [
          {
            role_id: "governance",
            description: "Approves plans and records decisions.",
          },
          { role_id: "operator", description: "Human operator." },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    });
  });
  it("shows one themed role with aggregate status and no arbitrary run link", () => {
    render(<AgentsPage />);
    expect(
      screen.getByText("Meet the agents and their responsibilities", { exact: true }),
    ).toBeInTheDocument();
    expect(
      screen.getAllByRole("button", { name: "About Regent" }),
    ).toHaveLength(1);
    expect(screen.getByText("Running")).toHaveAttribute("title", "2 Running");
    expect(screen.queryByText("Operator")).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /Run/ })).not.toBeInTheDocument();
    expect(screen.queryByText("actions=")).not.toBeInTheDocument();
  });
  it("shows workspace save actions only while the selected style has unsaved changes", async () => {
    themeCatalogItemsMock.mockReturnValue([{
      id: "star-wars",
      version: "1.0.0",
      name: "Star Wars",
      resolvedDigest: "a".repeat(64),
      agentVoice: "Use measured, cinematic language.",
      builtin: false,
    }]);
    render(<AgentsPage />);
    expect(screen.queryByRole("button", { name: /^Save$/ })).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Communication Style" }));
    await userEvent.click(screen.getByRole("option", { name: "Star Wars · 1.0.0" }));
    expect(screen.getByRole("button", { name: /^Save$/ })).toBeEnabled();

    await userEvent.click(screen.getByRole("button", { name: "Communication Style" }));
    await userEvent.click(screen.getByRole("option", { name: "Neutral · neutral-v1" }));
    expect(screen.queryByRole("button", { name: /^Save$/ })).not.toBeInTheDocument();
  });
  it("hides workspace save actions again when custom guidance is restored", async () => {
    const query = useAgentConfigurationMock();
    query.data.workspace.voice = {
      source: { kind: "custom" },
      instruction: "Use a steady workspace tone.",
    };
    render(<AgentsPage />);
    const instruction = screen.getByLabelText("Workspace Custom Communication Style");
    expect(instruction).toHaveValue("Use a steady workspace tone.");
    expect(screen.queryByRole("button", { name: /^Save$/ })).not.toBeInTheDocument();

    await userEvent.clear(instruction);
    await userEvent.type(instruction, "Use a brighter workspace tone.");
    expect(screen.getByRole("button", { name: /^Save$/ })).toBeEnabled();

    await userEvent.clear(instruction);
    await userEvent.type(instruction, "Use a steady workspace tone.");
    expect(screen.queryByRole("button", { name: /^Save$/ })).not.toBeInTheDocument();
  });
  it("shows the built-in neutral voice while configuration loads", () => {
    useAgentConfigurationMock.mockReturnValue({ data: undefined, isLoading: true, refetch: vi.fn() });
    render(<AgentsPage />);
    expect(screen.getByRole("button", { name: "Communication Style" })).toHaveTextContent("Neutral · neutral-v1");
  });
  it("keeps the canonical responsibility available alongside lore", async () => {
    render(<AgentsPage />);
    await userEvent.click(screen.getByRole("button", { name: "About Regent" }));
    expect(screen.getByRole("dialog")).toHaveTextContent(
      "Approves plans and records decisions.",
    );
    expect(screen.getByRole("dialog")).toHaveTextContent(
      "A clear path forward.",
    );
    expect(screen.queryByRole("heading", { name: "Agent Settings" })).not.toBeInTheDocument();
    const guidanceTooltip = screen.getByRole("button", { name: "About agent guidance" });
    await userEvent.hover(guidanceTooltip);
    expect(screen.getByRole("tooltip")).toHaveTextContent(
      "These settings do not change plan approval or final acceptance.",
    );
    expect(screen.queryByText("Current Work")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Open Run" }),
    ).not.toBeInTheDocument();
  });
  it("prefills default guidance, resets an unsaved edit in place, and saves nullable overrides", async () => {
    render(<AgentsPage />);
    await userEvent.click(screen.getByRole("button", { name: "About Regent" }));
    const guidance = screen.getByLabelText("Agent Guidance");
    expect(guidance).toHaveValue("Default role guidance.");
    await userEvent.clear(guidance);
    await userEvent.type(guidance, "Keep decisions concise.");
    await userEvent.click(screen.getByRole("button", { name: "Reset Agent Guidance" }));
    expect(guidance).toHaveValue("Default role guidance.");
    expect(resetRoleMock).not.toHaveBeenCalled();
    await userEvent.clear(guidance);
    await userEvent.type(guidance, "Keep decisions concise.");
    await userEvent.click(screen.getByRole("button", { name: "Save Settings" }));
    expect(updateRoleMock).toHaveBeenCalledWith(expect.objectContaining({
      roleId: "governance",
      body: {
        expected_row_version: 0,
        expected_prompt_catalog_digest: promptCatalogDigest,
        guidance_override: "Keep decisions concise.",
        voice_override: null,
      },
    }));
  });
  it("applies a theme-authored communication style only after an explicit workspace save", async () => {
    themeCatalogItemsMock.mockReturnValue([{
      id: "star-wars",
      version: "1.0.0",
      name: "Star Wars",
      resolvedDigest: "a".repeat(64),
      agentVoice: "Use measured, cinematic language.",
      builtin: false,
    }]);
    render(<AgentsPage />);
    expect(screen.getByLabelText("Communication Style")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Communication Style" }));
    await userEvent.click(screen.getByRole("option", { name: "Star Wars · 1.0.0" }));
    await userEvent.click(screen.getByRole("button", { name: /^Save$/ }));
    expect(updateWorkspaceVoiceMock).toHaveBeenCalledWith(expect.objectContaining({
      body: {
        expected_row_version: 1,
        voice: { kind: "theme", theme_id: "star-wars", theme_digest: "a".repeat(64) },
      },
    }));
  });
  it("stores workspace and per-agent custom communication styles separately", async () => {
    render(<AgentsPage />);
    await userEvent.click(screen.getByRole("button", { name: "Communication Style" }));
    await userEvent.click(screen.getByRole("option", { name: "Custom" }));
    await userEvent.type(screen.getByLabelText("Workspace Custom Communication Style"), "Use a steady, practical tone.");
    await userEvent.click(screen.getByRole("button", { name: /^Save$/ }));
    expect(updateWorkspaceVoiceMock).toHaveBeenCalledWith(expect.objectContaining({
      body: expect.objectContaining({ voice: { kind: "custom", instruction: "Use a steady, practical tone." } }),
    }));

    await userEvent.click(screen.getByRole("button", { name: "About Regent" }));
    const dialog = within(screen.getByRole("dialog"));
    expect(dialog.getByRole("button", { name: "Communication Style" })).toHaveTextContent("Neutral · neutral-v1");
    expect(dialog.getByText("Using workspace style")).toBeInTheDocument();
    await userEvent.click(dialog.getByRole("button", { name: "Communication Style" }));
    expect(screen.getByRole("option", { name: "Use Workspace Style" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("option", { name: "Custom" }));
    const agentStyle = dialog.getByLabelText("Custom communication style for this agent");
    expect(dialog.getByRole("button", { name: "Communication Style" })).toHaveTextContent("Custom");
    expect(dialog.queryByText("Agent Custom Communication Style")).not.toBeInTheDocument();
    await userEvent.type(agentStyle, "Offer careful, independent review.");
    await userEvent.click(dialog.getByRole("button", { name: "Communication Style" }));
    await userEvent.click(screen.getByRole("option", { name: "Custom" }));
    expect(agentStyle).toHaveValue("Offer careful, independent review.");
    await userEvent.click(dialog.getByRole("button", { name: "Save Settings" }));
    expect(updateRoleMock).toHaveBeenCalledWith(expect.objectContaining({
      body: expect.objectContaining({
        guidance_override: null,
        voice_override: { kind: "custom", instruction: "Offer careful, independent review." },
      }),
    }));
  });
  it("distinguishes an inherited workspace custom style from an agent custom override", async () => {
    const query = useAgentConfigurationMock();
    query.data.workspace.voice = {
      source: { kind: "custom" },
      instruction: "Use a steady workspace tone.",
    };
    query.data.roles[0].effective_voice = {
      source: { kind: "custom" },
      instruction: "Use a steady workspace tone.",
    };
    render(<AgentsPage />);
    await userEvent.click(screen.getByRole("button", { name: "About Regent" }));
    const dialog = within(screen.getByRole("dialog"));
    expect(dialog.getByRole("button", { name: "Communication Style" })).toHaveTextContent("Custom - Workspace");

    await userEvent.click(dialog.getByRole("button", { name: "Communication Style" }));
    expect(screen.getByRole("option", { name: "Custom" })).toBeInTheDocument();
  });
  it("resets saved guidance directly without clearing a saved communication style override", async () => {
    const query = useAgentConfigurationMock();
    query.data.roles[0] = {
      role_id: "governance",
      default_guidance: "Default role guidance.",
      guidance_override: "Current role guidance.",
      effective_guidance: "Current role guidance.",
      effective_voice: { source: { kind: "neutral-v1" } },
      voice_override: { source: { kind: "neutral-v1" } },
      row_version: 4,
    };
    resetRoleMock.mockResolvedValue({ row_version: 5, effective_guidance: "Default role guidance." });
    render(<AgentsPage />);
    await userEvent.click(screen.getByRole("button", { name: "About Regent" }));
    await userEvent.click(screen.getByRole("button", { name: "Reset Agent Guidance" }));
    expect(resetRoleMock).toHaveBeenCalledWith(expect.objectContaining({
      roleId: "governance",
      body: { expected_row_version: 4, expected_prompt_catalog_digest: promptCatalogDigest },
    }));
    expect(updateRoleMock).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Agent Guidance")).toHaveValue("Default role guidance.");
  });
  it("prevents guidance edits while a saved reset is pending", async () => {
    const query = useAgentConfigurationMock();
    query.data.roles[0] = {
      role_id: "governance",
      default_guidance: "Default role guidance.",
      guidance_override: "Current role guidance.",
      effective_guidance: "Current role guidance.",
      effective_voice: { source: { kind: "neutral-v1" } },
      voice_override: null,
      row_version: 4,
    };
    let finishReset!: (value: { row_version: number; effective_guidance: string }) => void;
    resetRoleMock.mockImplementation(() => new Promise((resolve) => { finishReset = resolve; }));
    render(<AgentsPage />);
    await userEvent.click(screen.getByRole("button", { name: "About Regent" }));
    const guidance = screen.getByLabelText("Agent Guidance");
    await userEvent.click(screen.getByRole("button", { name: "Reset Agent Guidance" }));
    expect(guidance).toBeDisabled();
    await act(async () => {
      finishReset({ row_version: 5, effective_guidance: "Default role guidance." });
    });
    expect(guidance).toHaveValue("Default role guidance.");
  });
  it.each([true, false])(
    "does not turn missing registry data into unassigned roles (loading=%s)",
    (isLoading) => {
      useAgentsMock.mockReturnValue({
        data: undefined,
        error: isLoading ? undefined : new Error("offline"),
        isLoading,
        refetch: vi.fn(),
      });
      render(<AgentsPage />);
      expect(
        screen.getByText(isLoading ? "Loading" : "Unknown"),
      ).toBeInTheDocument();
      expect(screen.queryByText("Unassigned")).not.toBeInTheDocument();
      expect(
        screen.queryByText(/Showing the last known/),
      ).not.toBeInTheDocument();
    },
  );
  it("reports an unavailable registry instead of implying nobody is working", () => {
    useAgentsMock.mockReturnValue({
      error: new Error("offline"),
      isLoading: false,
      refetch: vi.fn(),
    });
    useRolesMock.mockReturnValue({
      error: new Error("offline"),
      isLoading: false,
      refetch: vi.fn(),
    });
    render(<AgentsPage />);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Agent availability could not be refreshed",
    );
    expect(screen.queryByText("No Agents Yet")).not.toBeInTheDocument();
  });
});
