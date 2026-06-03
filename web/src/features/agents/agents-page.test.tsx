import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { AgentsPage } from "./agents-page";

const useAgentsMock = vi.fn();
const useRolesMock = vi.fn();
const useRunsMock = vi.fn();
const usePendingGovernanceRunsMock = vi.fn();

vi.mock("@/api/hooks", () => ({
  useAgents: (...args: unknown[]) => useAgentsMock(...args),
  useRoles: (...args: unknown[]) => useRolesMock(...args),
  useRuns: (...args: unknown[]) => useRunsMock(...args),
  usePendingGovernanceRuns: (...args: unknown[]) => usePendingGovernanceRunsMock(...args),
}));

describe("agents page", () => {
  beforeEach(() => {
    useAgentsMock.mockReset();
    useRolesMock.mockReset();
    useRunsMock.mockReset();
    usePendingGovernanceRunsMock.mockReset();

    useAgentsMock.mockReturnValue({
      data: {
        items: [
          {
            agent_id: "agent-king",
            role_id: "king",
            display_name: "King",
            status: "idle",
            spawn_depth: 0,
            created_at: "2026-03-22T10:00:00Z",
          },
          {
            agent_id: "agent-allfather",
            role_id: "allfather",
            display_name: "Allfather",
            status: "idle",
            spawn_depth: 0,
            created_at: "2026-03-22T10:01:00Z",
          },
        ],
      },
      isLoading: false,
    });
    useRolesMock.mockReturnValue({
      data: {
        items: [
          {
            role_id: "king",
            display_name: "King",
            description: "Supreme governance authority that approves plans, requests rework when needed, and closes decrees.",
            can_spawn: true,
            agent_model_selector_visible: true,
            agent_model_selector_section_id: "king",
            agent_model_selector_section_label: "King",
            agent_model_selector_section_order: 10,
            agent_model_selector_role_order: 10,
          },
          {
            role_id: "allfather",
            display_name: "Allfather",
            description: "Human operator who issues decrees and makes plan and final approval decisions when manual approvals are enabled.",
            can_spawn: false,
            agent_model_selector_visible: false,
            agent_model_selector_section_id: "operators",
            agent_model_selector_section_label: "Operators",
            agent_model_selector_section_order: 0,
            agent_model_selector_role_order: 10,
          },
        ],
      },
      isLoading: false,
    });
    useRunsMock.mockReturnValue({
      data: { items: [] },
    });
    usePendingGovernanceRunsMock.mockReturnValue({
      data: { items: [] },
    });
  });

  it("renders canonical role descriptions and excludes allfather from the registry", () => {
    render(<AgentsPage />);

    expect(screen.getByText("King")).toBeInTheDocument();
    expect(
      screen.getByText("Supreme governance authority that approves plans, requests rework when needed, and closes decrees."),
    ).toBeInTheDocument();
    expect(screen.queryByText("Allfather")).not.toBeInTheDocument();
    expect(
      screen.queryByText("Human operator who issues decrees and makes plan and final approval decisions when manual approvals are enabled."),
    ).not.toBeInTheDocument();
  });
});
