import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { Badge } from "./badge";
import { Button } from "./button";
import { Card, CardContent } from "./card";
import { InfoTooltip } from "./info-tooltip";
import { SelectField } from "./select-field";

// These assertions protect the intrinsic-size contract. Actual border/font
// geometry is checked in the installed browser, which jsdom cannot measure.
describe("shared control spacing", () => {
  it.each([
    ["default", "min-h-9", "py-1.5"],
    ["sm", "min-h-8", "py-1.5"],
    ["lg", "min-h-11", "py-2.5"],
    ["icon", "min-h-9", "p-2"],
  ] as const)(
    "lets %s buttons grow around content and theme borders",
    (size, minimum, padding) => {
      render(
        <Button size={size} aria-label="Action">
          Action
        </Button>,
      );
      const button = screen.getByRole("button", { name: "Action" });
      expect(button).toHaveClass(minimum, padding);
      expect(
        button.className.split(/\s+/).some((name) => /^h-\d/.test(name)),
      ).toBe(false);
      if (size === "icon") expect(button).toHaveClass("min-w-9");
    },
  );

  it("insets compact labels and tooltip text without changing their content", async () => {
    render(
      <>
        <Badge>Running</Badge>
        <InfoTooltip label="About timing" message="The exact recorded time." />
      </>,
    );
    expect(screen.getByText("Running")).toHaveClass("px-2.5", "py-1");
    await userEvent.hover(screen.getByRole("button", { name: "About timing" }));
    expect(screen.getByRole("tooltip")).toHaveClass("px-3", "py-2");
    expect(screen.getByRole("tooltip")).toHaveTextContent(
      "The exact recorded time.",
    );
  });

  it("keeps selected option highlights inset from the dropdown frame", async () => {
    render(
      <SelectField
        id="spacing-choice"
        label="Choice"
        value="one"
        options={[{ value: "one", label: "A selected option" }]}
        onChange={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Choice" }));
    const option = screen.getByRole("option", { name: "A selected option" });
    expect(option.parentElement).toHaveClass("p-1.5");
    expect(option).toHaveClass("px-3", "py-2");
  });

  it("preserves intentional full-bleed cards and explicit control padding", () => {
    render(
      <Card data-testid="frame" className="p-0">
        <CardContent>Full Width</CardContent>
        <Button className="px-2 py-1">Compact</Button>
      </Card>,
    );
    expect(screen.getByTestId("frame")).toHaveClass("p-0");
    expect(screen.getByTestId("frame")).not.toHaveClass("p-4");
    expect(screen.getByText("Full Width").className).not.toMatch(
      /(?:^|\s)p[xyltrb]?-\d/,
    );
    const button = screen.getByRole("button", { name: "Compact" });
    expect(button).toHaveClass("px-2", "py-1");
    expect(button).not.toHaveClass("px-4", "py-1.5");
  });
});
