import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import type { RunParticipant } from "@/api/hooks";
import { AgentDetailPanel } from "./agent-detail-panel";

vi.mock("@/theme", async () => {
  const actual = await vi.importActual<typeof import("@/theme")>("@/theme");
  return { RoleIdentity: ({ role }: { role: string }) => <span>{actual.canonicalRoleName(role)}</span> };
});

const agents: RunParticipant[] = [
  {
    agent_id: "agent_governance",
    role_id: "governance",
    display_name: "Governance",
    status: "busy",
    spawn_depth: 0,
    first_seen_at: "2026-03-22T10:00:00Z",
  },
  {
    agent_id: "agent_nightraven",
    role_id: "security-validator",
    display_name: "Security Validator",
    status: "blocked",
    spawn_depth: 2,
    first_seen_at: "2026-03-22T10:02:00Z",
  },
  {
    agent_id: "agent_reviewer",
    role_id: "reviewer",
    display_name: "Reviewer",
    status: "idle",
    spawn_depth: 1,
    first_seen_at: "2026-03-22T10:03:00Z",
  },
];

const roleDescriptions = {
  governance: "Supreme governance authority that approves plans, requests rework when needed, and closes work orders.",
  "reviewer": "Reviews builder output against the work order and approved plan, checks scope and correctness before validation begins.",
  "security-validator": "Validates security posture, security findings, and risk concerns before royal closure.",
};

describe("agent detail panel", () => {
  it("renders compact rows and toggles role descriptions on click", () => {
    render(<AgentDetailPanel agents={agents} roleDescriptions={roleDescriptions} />);

    expect(screen.getByText("1 Active")).toBeInTheDocument();
    expect(screen.getByText("1 Blocked")).toBeInTheDocument();
    expect(screen.queryByText("Active: 1")).not.toBeInTheDocument();
    expect(screen.queryByText("Blocked: 1")).not.toBeInTheDocument();

    expect(screen.queryByText("agent_governance")).not.toBeInTheDocument();
    expect(screen.queryByText("agent_nightraven")).not.toBeInTheDocument();
    expect(screen.queryByText(/Agent:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Depth/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Session:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Created:/)).not.toBeInTheDocument();
    expect(screen.getByTestId("role-description-governance")).toHaveClass("grid-rows-[0fr]");

    fireEvent.click(screen.getByRole("button", { name: /Governance/i }));
    expect(screen.getByText(roleDescriptions.governance)).toBeInTheDocument();
    expect(screen.getByTestId("role-description-governance")).toHaveClass("grid-rows-[1fr]");

    fireEvent.click(screen.getByRole("button", { name: /Reviewer/i }));
    expect(screen.getByText(roleDescriptions["reviewer"])).toBeInTheDocument();
    expect(screen.getByTestId("role-description-governance")).toHaveClass("grid-rows-[0fr]");

    fireEvent.click(screen.getByRole("button", { name: /Reviewer/i }));
    expect(screen.getByTestId("role-description-reviewer")).toHaveClass("grid-rows-[0fr]");

    expect(screen.getAllByText("Active").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Blocked").length).toBeGreaterThan(0);
  });

  it("shows empty state when no agents are present", () => {
    render(<AgentDetailPanel agents={[]} />);
    expect(screen.getByText("No agents spawned yet.")).toBeInTheDocument();
  });

  it("filters operator from the run agents surface", () => {
    render(
      <AgentDetailPanel
        agents={[
          {
            agent_id: "agent_operator",
            role_id: "operator",
            display_name: "Operator",
            status: "idle",
            spawn_depth: 0,
            first_seen_at: "2026-03-22T10:04:00Z",
          },
        ]}
        roleDescriptions={{
          operator: "Human operator who issues work orders and makes plan and final approval decisions when manual approvals are enabled.",
        }}
      />,
    );

    expect(screen.getByText("No agents spawned yet.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Operator/i })).not.toBeInTheDocument();
  });
});
