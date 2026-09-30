import { type ReactNode, useState } from "react";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { PlanResultModal } from "./planner-draft-panel";

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, search }: { children: ReactNode; search: unknown }) => <a href="#" data-search={JSON.stringify(search)}>{children}</a>,
}));

vi.mock("@/lib/use-text-reveal", () => ({
  useTextReveal: (text: string) => ({
    displayText: text,
    isStreamActive: false,
  }),
}));

function ModalHarness() {
  const [open, setOpen] = useState(false);

  return (
    <div>
      <button type="button" onClick={() => setOpen(true)}>
        Open plan result
      </button>
      <PlanResultModal
        open={open}
        draft={{
          planning_draft_id: "draft-1",
          planning_session_id: "session-1",
          decree_title: "Royal decree draft",
          decree_text: "Plan body",
          created_at: "2026-04-29T12:00:00Z",
        }}
        planningLineage={{
          planning_session_id: "session-1",
          planning_draft_id: "draft-1",
        }}
        operation={null}
        onClose={() => setOpen(false)}
      />
    </div>
  );
}

describe("plan result modal", () => {
  it("renders the plan inside the shared modal content card", async () => {
    const user = userEvent.setup();

    render(<ModalHarness />);

    await user.click(screen.getByRole("button", { name: "Open plan result" }));

    const dialog = screen.getByRole("dialog", { name: "Royal decree draft" });
    const contentCard = within(dialog).getByTestId("plan-result-card");

    expect(contentCard).toHaveClass("border-border");
    expect(contentCard).toHaveClass("bg-background");
    expect(within(contentCard).getByText("Plan body")).toBeInTheDocument();
  });

  it("moves focus into the dialog and restores it to the opener on close", async () => {
    const user = userEvent.setup();

    render(<ModalHarness />);

    const opener = screen.getByRole("button", { name: "Open plan result" });
    await user.click(opener);

	const closeButton = screen.getByRole("button", { name: "Close dialog" });
    await waitFor(() => expect(closeButton).toHaveFocus());

    await user.click(closeButton);
    await waitFor(() => expect(opener).toHaveFocus());
  });

  it("routes only one typed planning lineage", async () => {
    const user = userEvent.setup();
    render(<ModalHarness />);

    await user.click(screen.getByRole("button", { name: "Open plan result" }));

    expect(screen.getByRole("link", { name: /issue decree from plan/i })).toHaveAttribute("data-search", JSON.stringify({
      planning_session_id: "session-1",
      planning_draft_id: "draft-1",
    }));
  });
});
