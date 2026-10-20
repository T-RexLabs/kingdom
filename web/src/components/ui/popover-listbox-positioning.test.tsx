import { useState } from "react";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PopoverListbox } from "./popover-listbox";

function viewportGeometry(triggerY: number, viewportHeight = 800, panelHeight = 300, responsiveHeight = false, transientZeroOnReopen = false) {
  const measured = new WeakSet<HTMLElement>();
  let mountedPanelCount = 0;
  const measuredHeight = (element: HTMLElement) => {
    if (!measured.has(element)) {
      measured.add(element);
      mountedPanelCount++;
      // A reopening can be measured before layout; later reads see the
      // content constrained by whatever height the middleware applied.
      if (transientZeroOnReopen && mountedPanelCount > 1) return 0;
    }
    return responsiveHeight && element.style.maxHeight !== ""
      ? Math.min(panelHeight, Math.max(0, Number.parseFloat(element.style.maxHeight)))
      : panelHeight;
  };
  vi.spyOn(document.documentElement, "clientWidth", "get").mockReturnValue(1200);
  vi.spyOn(document.documentElement, "clientHeight", "get").mockReturnValue(viewportHeight);
  vi.spyOn(HTMLElement.prototype, "offsetWidth", "get").mockImplementation(function (this: HTMLElement) {
    return this.hasAttribute("data-popover-placement") ? 280 : this.id === "edge-model" ? 240 : 1200;
  });
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockImplementation(function (this: HTMLElement) {
    return this.hasAttribute("data-popover-placement") ? measuredHeight(this) : this.id === "edge-model" ? 32 : viewportHeight;
  });
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (this: HTMLElement) {
    if (this.id === "edge-model") return new DOMRect(100, triggerY, 240, 32);
    if (this.hasAttribute("data-popover-placement")) return new DOMRect(0, 0, 280, measuredHeight(this));
    return new DOMRect(0, 0, 1200, viewportHeight);
  });
}

function ModelListbox({ role = "button", onSelect = vi.fn(), itemCount = 1 }: { role?: "button" | "combobox"; onSelect?: (value: string) => void; itemCount?: number }) {
  const [query, setQuery] = useState("");
  const items = Array.from({ length: itemCount }, (_, index) => ({ id: index === 0 ? "luna" : `model-${index}`, label: index === 0 ? "GPT-6 Luna" : `Model ${index}` }));
  return (
    <PopoverListbox
      triggerId="edge-model"
      triggerRole={role}
      triggerAriaLabel="Choose model"
      triggerContent="Choose model"
      items={items.filter((item) => item.label.toLowerCase().includes(query.toLowerCase()))}
      selectedId={null}
      onSelect={onSelect}
      search={role === "combobox" ? { value: query, onChange: setQuery, placeholder: "Search models" } : undefined}
    />
  );
}

afterEach(() => vi.restoreAllMocks());

describe("listbox viewport placement", () => {
  it.each(["combobox", "button"] as const)("keeps a %s above the footer on repeated openings with real size constraints", async (role) => {
    const panelHeight = role === "combobox" ? 5000 : 300;
    const itemCount = role === "combobox" ? 130 : 7;
    viewportGeometry(750, 800, panelHeight, true, true);
    const user = userEvent.setup();
    const onSelect = vi.fn();
    render(<ModelListbox role={role} itemCount={itemCount} onSelect={onSelect} />);
    const trigger = screen.getByRole(role, { name: "Choose model" });
    for (let opening = 0; opening < 3; opening++) {
      await user.click(trigger);
      await waitFor(() => {
        const panel = document.querySelector<HTMLElement>("[data-popover-placement]");
        expect(panel).toHaveAttribute("data-popover-placement", "top-start");
        expect(panel?.getBoundingClientRect().height).toBe(Math.min(panelHeight, 320));
      });
      await user.click(trigger);
      await waitFor(() => expect(screen.queryByRole("listbox")).not.toBeInTheDocument());
    }
    await user.click(trigger);
    if (role === "combobox") {
      await user.type(screen.getByRole("textbox", { name: "Search models" }), "Model 129");
      expect(screen.getAllByRole("option")).toHaveLength(1);
    }
    await user.keyboard("{End}{Enter}");
    expect(onSelect).toHaveBeenCalledWith(`model-${itemCount - 1}`);
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it.each(["combobox", "button"] as const)("opens a %s above a bottom-edge trigger and preserves keyboard selection", async (role) => {
    viewportGeometry(750);
    const user = userEvent.setup();
    const onSelect = vi.fn();
    render(<ModelListbox role={role} onSelect={onSelect} />);
    const trigger = screen.getByRole(role, { name: "Choose model" });
    await user.click(trigger);
    await waitFor(() => expect(document.querySelector("[data-popover-placement]")).toHaveAttribute("data-popover-placement", "top-start"));
    await user.keyboard("{Home}{Enter}");
    await waitFor(() => expect(screen.queryByRole("listbox")).not.toBeInTheDocument());
    expect(onSelect).toHaveBeenCalledWith("luna");
    expect(trigger).toHaveFocus();
  });

  it("retains bottom placement when the viewport has room", async () => {
    viewportGeometry(50);
    const user = userEvent.setup();
    render(<ModelListbox />);
    await user.click(screen.getByRole("button", { name: "Choose model" }));
    await waitFor(() => expect(document.querySelector("[data-popover-placement]")).toHaveAttribute("data-popover-placement", "bottom-start"));
  });

  it.each([
    { triggerY: 250, placement: "top-start", availableHeight: 238 },
    { triggerY: 20, placement: "bottom-start", availableHeight: 236 },
  ])("limits a tall menu to available space at $placement", async ({ triggerY, placement, availableHeight }) => {
    viewportGeometry(triggerY, 300, 500);
    const user = userEvent.setup();
    render(<ModelListbox />);
    await user.click(screen.getByRole("button", { name: "Choose model" }));
    await waitFor(() => {
      const panel = document.querySelector<HTMLElement>("[data-popover-placement]");
      expect(panel).toHaveAttribute("data-popover-placement", placement);
      expect(Number.parseFloat(panel?.style.maxHeight ?? "")).toBe(availableHeight);
    });
  });
});
