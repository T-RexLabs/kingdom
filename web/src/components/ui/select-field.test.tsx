import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { SelectField } from "./select-field";

describe("SelectField", () => {
  it("shows a decorative dropdown affordance without changing its accessible name", () => {
    render(<SelectField id="source" label="Work Source" value="manual" options={[{ label: "Write a Brief", value: "manual" }]} onChange={vi.fn()} />);
    const trigger = screen.getByRole("button", { name: "Work Source" });
    expect(trigger).toHaveAccessibleDescription("Write a Brief");
    expect(trigger.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
  });

  it("recognizes an explicitly selected empty value and keeps selection keyboard accessible", async () => {
    const user = userEvent.setup();
    const change = vi.fn();
    render(<SelectField id="plan" label="Plan" value="" options={[{ label: "No Plan", value: "" }, { label: "Current Plan", value: "plan-1" }]} onChange={change} />);
    const trigger = screen.getByRole("button", { name: "Plan" });
    await user.click(trigger);
    const selected = screen.getByRole("option", { name: "No Plan" });
    expect(selected).toHaveAttribute("aria-selected", "true");
    expect(selected.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    await waitFor(() => expect(selected).toHaveFocus());
    await user.keyboard("{End}{Enter}");
    expect(change).toHaveBeenCalledWith("plan-1");
    await waitFor(() => expect(trigger).toHaveFocus());
  });
});
