import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { ActivityChecklist } from "./activity-stream";
import type { TodoList } from "./activity-view";

const list: TodoList = {
  items: [
    { content: "Confirm module root", status: "completed" },
    { content: "Add greeter.go", status: "in_progress" },
    { content: "Run go test", status: "pending" },
  ],
  done: 1,
  total: 3,
};

describe("ActivityChecklist", () => {
  it("shows progress and reveals items with strikethrough on completion when expanded", async () => {
    const user = userEvent.setup();
    render(<ActivityChecklist list={list} />);

    // Collapsed by default: progress count shown, items hidden.
    const toggle = screen.getByRole("button", { name: /Checklist/ });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByText("1/3")).toBeInTheDocument();
    expect(screen.queryByText("Add greeter.go")).not.toBeInTheDocument();

    await user.click(toggle);
    expect(screen.getByText("Add greeter.go")).toBeInTheDocument();
    const completed = screen.getByText("Confirm module root");
    expect(completed.className).toContain("line-through");
  });
});
