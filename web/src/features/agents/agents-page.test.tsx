import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { AgentsPage } from "./agents-page";

const useAgentsMock = vi.fn();
const useRolesMock = vi.fn();
vi.mock("@/api/hooks", () => ({
  useAgents: () => useAgentsMock(),
  useRoles: () => useRolesMock(),
}));
vi.mock("@/theme", () => ({
  ThemeArtwork: () => null,
  ThemeIcon: () => <span aria-hidden="true" />,
  RolePortrait: () => null,
  useThemeCopy: () => "The Court",
  useRoleIdentity: (role: string) => ({
    name: role === "governance" ? "Regent" : "Operator",
    canonicalName: role === "governance" ? "Governance" : "Operator",
    description: "Coordinates the work.",
    lore: "A clear path forward.",
  }),
}));

describe("agents page", () => {
  beforeEach(() => {
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
      screen.getAllByRole("button", { name: "About Regent" }),
    ).toHaveLength(1);
    expect(screen.getByText("Running")).toHaveAttribute("title", "2 Running");
    expect(screen.queryByText("Operator")).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /Run/ })).not.toBeInTheDocument();
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
    expect(screen.queryByText("Current Work")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Open Run" }),
    ).not.toBeInTheDocument();
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
