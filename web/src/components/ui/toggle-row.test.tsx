import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ToggleRow } from "./toggle-row";

describe("ToggleRow", () => {
  it("connects visible help to its switch without adding a redundant tooltip", async () => {
    const change = vi.fn();
    render(<ToggleRow label="Require Human Approval" description="Pause for your decision before execution and final completion." checked={false} onChange={change} />);
    const control = screen.getByRole("switch", { name: "Require Human Approval" });
    expect(control).toHaveAccessibleDescription("Pause for your decision before execution and final completion.");
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    await userEvent.click(control);
    expect(change).toHaveBeenCalledWith(true);
  });
  it("preserves optional detailed help and disabled behavior", async () => {
    const change = vi.fn();
    render(<ToggleRow label="Advanced Control" info="An explanation of this setting." checked disabled onChange={change} />);
    expect(screen.getByRole("button", { name: "Advanced Control definition" })).toBeInTheDocument();
    const control = screen.getByRole("switch", { name: "Advanced Control" });
    expect(control).toBeDisabled();
    await userEvent.click(control);
    expect(change).not.toHaveBeenCalled();
  });
});
