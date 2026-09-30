import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { Button } from "./button";
import { Card } from "./card";
import { Modal } from "./modal";
import { SelectField } from "./select-field";
import { ToggleRow } from "./toggle-row";

describe("structural theme hooks", () => {
  it("exposes control variants without changing click or disabled behavior", async () => {
    const click = vi.fn();
    render(
      <>
        <Button onClick={click}>Save</Button>
        <Button variant="outline" disabled onClick={click}>
          Unavailable
        </Button>
        <Card>Content</Card>
      </>,
    );
    expect(screen.getByRole("button", { name: "Save" })).toHaveClass(
      "ui-button",
      "ui-button--default",
    );
    expect(screen.getByRole("button", { name: "Unavailable" })).toHaveClass(
      "ui-button--outline",
    );
    expect(screen.getByText("Content")).toHaveClass("ui-card");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await userEvent.click(screen.getByRole("button", { name: "Unavailable" }));
    expect(click).toHaveBeenCalledOnce();
  });
  it("themes the portalled dropdown and retains canonical selection", async () => {
    const change = vi.fn();
    render(
      <SelectField
        id="choice"
        label="Choice"
        value="a"
        options={[
          { value: "a", label: "First" },
          { value: "b", label: "Second" },
        ]}
        onChange={change}
      />,
    );
    expect(screen.getByRole("button", { name: "Choice" })).toHaveClass(
      "ui-field",
      "ui-select-trigger",
    );
    await userEvent.click(screen.getByRole("button", { name: "Choice" }));
    expect(screen.getByRole("option", { name: "First" })).toHaveClass(
      "ui-menu-option",
    );
    expect(screen.getByRole("option", { name: "First" }).closest(".ui-popover")).not.toBeNull();
    await userEvent.click(screen.getByRole("option", { name: "Second" }));
    expect(change).toHaveBeenCalledWith("b");
  });
  it("exposes window and switch parts without changing their accessible controls", async () => {
    const change = vi.fn();
    render(
      <Modal
        open
        onClose={vi.fn()}
        title="Settings"
        description="These options apply to this workspace."
      >
        <ToggleRow label="Enabled" checked onChange={change} />
      </Modal>,
    );
    const dialog = screen.getByRole("dialog", { name: "Settings" });
    expect(dialog.querySelector(".ui-window-titlebar")).not.toBeNull();
    expect(dialog.querySelector(".ui-window-body")).not.toBeNull();
    expect(dialog.querySelector(".ui-window-titlebar")).not.toContainElement(
      screen.getByText("These options apply to this workspace."),
    );
    const toggle = screen.getByRole("switch", { name: "Enabled" });
    expect(toggle).toHaveClass("ui-switch");
    expect(toggle).toHaveAttribute("aria-checked", "true");
    await userEvent.click(toggle);
    expect(change).toHaveBeenCalledWith(false);
  });
});
