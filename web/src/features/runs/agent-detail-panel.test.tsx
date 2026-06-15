import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import type { RunParticipant } from "@/api/hooks";
import { AgentDetailPanel } from "./agent-detail-panel";

const agents: RunParticipant[] = [
  {
    agent_id: "agent_king",
    role_id: "king",
    display_name: "King",
    status: "busy",
    spawn_depth: 0,
    first_seen_at: "2026-03-22T10:00:00Z",
  },
  {
    agent_id: "agent_nightraven",
    role_id: "intelligence-nightraven",
    display_name: "Nightraven",
    status: "blocked",
    spawn_depth: 2,
    first_seen_at: "2026-03-22T10:02:00Z",
  },
  {
    agent_id: "agent_reviewer",
    role_id: "reviewer-shieldmaiden",
    display_name: "Shieldmaiden Reviewer",
    status: "idle",
    spawn_depth: 1,
    first_seen_at: "2026-03-22T10:03:00Z",
  },
];

const roleDescriptions = {
  king: "Supreme governance authority that approves plans, requests rework when needed, and closes decrees.",
  "reviewer-shieldmaiden": "Reviews builder output against the decree and approved plan, checks scope and correctness before validation begins.",
  "intelligence-nightraven": "Validates security posture, security findings, and risk concerns before royal closure.",
};

describe("agent detail panel", () => {
  it("renders compact rows and toggles role descriptions on click", () => {
    render(<AgentDetailPanel agents={agents} roleDescriptions={roleDescriptions} />);

    expect(screen.getByText("1 Active")).toBeInTheDocument();
    expect(screen.getByText("1 Blocked")).toBeInTheDocument();
    expect(screen.queryByText("Active: 1")).not.toBeInTheDocument();
    expect(screen.queryByText("Blocked: 1")).not.toBeInTheDocument();

    expect(screen.queryByText("agent_king")).not.toBeInTheDocument();
    expect(screen.queryByText("agent_nightraven")).not.toBeInTheDocument();
    expect(screen.queryByText(/Agent:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Depth/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Session:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Created:/)).not.toBeInTheDocument();
    expect(screen.getByTestId("role-description-king")).toHaveClass("grid-rows-[0fr]");

    fireEvent.click(screen.getByRole("button", { name: /King/i }));
    expect(screen.getByText(roleDescriptions.king)).toBeInTheDocument();
    expect(screen.getByTestId("role-description-king")).toHaveClass("grid-rows-[1fr]");

    fireEvent.click(screen.getByRole("button", { name: /Reviewer Shieldmaiden/i }));
    expect(screen.getByText(roleDescriptions["reviewer-shieldmaiden"])).toBeInTheDocument();
    expect(screen.getByTestId("role-description-king")).toHaveClass("grid-rows-[0fr]");

    fireEvent.click(screen.getByRole("button", { name: /Reviewer Shieldmaiden/i }));
    expect(screen.getByTestId("role-description-reviewer-shieldmaiden")).toHaveClass("grid-rows-[0fr]");

    expect(screen.getAllByText("Active").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Blocked").length).toBeGreaterThan(0);
  });

  it("shows empty state when no agents are present", () => {
    render(<AgentDetailPanel agents={[]} />);
    expect(screen.getByText("No agents spawned yet.")).toBeInTheDocument();
  });

  it("filters allfather from the run agents surface", () => {
    render(
      <AgentDetailPanel
        agents={[
          {
            agent_id: "agent_allfather",
            role_id: "allfather",
            display_name: "Allfather",
            status: "idle",
            spawn_depth: 0,
            first_seen_at: "2026-03-22T10:04:00Z",
          },
        ]}
        roleDescriptions={{
          allfather: "Human operator who issues decrees and makes plan and final approval decisions when manual approvals are enabled.",
        }}
      />,
    );

    expect(screen.getByText("No agents spawned yet.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Allfather/i })).not.toBeInTheDocument();
  });
});
