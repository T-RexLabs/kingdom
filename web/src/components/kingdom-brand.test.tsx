import type { ComponentProps } from "react";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KingdomBrand } from "./kingdom-brand";

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, to, ...props }: ComponentProps<"a"> & { to: string }) => <a href={to} {...props} onClick={(event) => { props.onClick?.(event); event.preventDefault(); }}>{children}</a>,
}));
vi.mock("@/theme", () => ({
  ThemeIcon: ({ slot }: { slot: string }) => <span aria-hidden="true" data-icon={slot} />,
}));

// jsdom has no PointerEvent constructor. Preserve the pointer fields that
// distinguish a hovering mouse from a touch or pen contact.
class TestPointerEvent extends MouseEvent {
  readonly pointerType: string;
  constructor(type: string, init: PointerEventInit = {}) {
    super(type, init);
    this.pointerType = init.pointerType ?? "";
  }
}

function advance(milliseconds: number) {
  act(() => vi.advanceTimersByTime(milliseconds));
}
function hover(brand: HTMLElement) {
  fireEvent.pointerEnter(brand, { pointerType: "mouse", buttons: 0 });
}

describe("Kingdom brand hover", () => {
  let previousOrnamentation: string | null;
  beforeEach(() => {
    previousOrnamentation = document.documentElement.getAttribute("data-ornamentation");
    vi.useFakeTimers();
    vi.stubGlobal("PointerEvent", TestPointerEvent);
  });
  afterEach(() => {
    cleanup();
    if (previousOrnamentation === null) document.documentElement.removeAttribute("data-ornamentation");
    else document.documentElement.setAttribute("data-ornamentation", previousOrnamentation);
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it.each(["minimal", "balanced", "high", "ludicrous"])("stays still until activating Rexie at exactly three seconds in %s ornamentation", (ornamentation) => {
    document.documentElement.setAttribute("data-ornamentation", ornamentation);
    render(<KingdomBrand />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    expect(brand).toHaveAttribute("href", "/");
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    hover(brand);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    advance(2999);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    advance(1);
    expect(brand).toHaveAttribute("data-brand-state", "rex");
    expect(brand.querySelector(".kingdom-brand-rex")?.closest("[aria-hidden]")).toHaveAttribute("aria-hidden", "true");
    // The roar belongs to the shared collision in every mode, and must never
    // replace the home link's accessible name with the decorative sound effect.
    expect(brand.querySelector(".kingdom-brand-roar")).toHaveTextContent("RAAWR!");
    expect(brand.querySelector(".kingdom-brand-roar")?.closest("[aria-hidden]")).toHaveAttribute("aria-hidden", "true");
    expect(brand).toHaveAccessibleName("Kingdom");
    expect(brand).toHaveAttribute("href", "/");
  });

  it("counts a continuous hover while the mouse moves across the logo and title", () => {
    render(<KingdomBrand />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    hover(brand);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    advance(1500);
    fireEvent.pointerMove(brand.querySelector(".kingdom-brand-icon")!, { pointerType: "mouse", buttons: 0 });
    advance(1499);
    fireEvent.pointerMove(brand.querySelector(".kingdom-brand-title")!, { pointerType: "mouse", buttons: 0 });
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    advance(1);
    expect(brand).toHaveAttribute("data-brand-state", "rex");
  });

  it.each([false, true])("keeps the collision fragments decorative and the home link named when collapsed is %s", (collapsed) => {
    render(<KingdomBrand collapsed={collapsed} />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    const title = brand.querySelector(".kingdom-brand-title");
    const letters = Array.from(brand.querySelectorAll(".kingdom-brand-letter"));
    expect(letters.map((letter) => letter.textContent)).toEqual(Array.from("Kingdom"));
    expect(title).toHaveTextContent(/^Kingdom$/);
    for (const letter of letters) {
      expect(letter.closest('[aria-hidden="true"]')).toBe(title);
    }

    hover(brand);
    advance(3000);
    expect(brand).toHaveAttribute("data-brand-state", "rex");
    expect(screen.getAllByRole("link", { name: "Kingdom" })).toHaveLength(1);
    expect(brand).toHaveAccessibleName("Kingdom");
    expect(title).toHaveTextContent(/^Kingdom$/);
    fireEvent.pointerLeave(brand, { pointerType: "mouse" });
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    expect(title).toHaveTextContent(/^Kingdom$/);
  });

  it.each([
    ["pointer leave", (brand: HTMLElement) => fireEvent.pointerLeave(brand, { pointerType: "mouse" })],
    ["pointer cancellation", (brand: HTMLElement) => fireEvent.pointerCancel(brand, { pointerType: "mouse" })],
    ["pointer press", (brand: HTMLElement) => fireEvent.pointerDown(brand, { pointerType: "mouse", buttons: 1 })],
    ["home navigation", (brand: HTMLElement) => fireEvent.click(brand)],
    ["window blur", () => fireEvent(window, new Event("blur"))],
  ] as const)("requires a fresh uninterrupted hover after %s", (_name, cancel) => {
    render(<KingdomBrand />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    hover(brand);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    advance(2500);
    cancel(brand);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    expect(vi.getTimerCount()).toBe(0);
    advance(1000);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    hover(brand);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    advance(2999);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    advance(1);
    expect(brand).toHaveAttribute("data-brand-state", "rex");
    cancel(brand);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
  });

  it("resets when the document becomes hidden and clears pending work on unmount", () => {
    const hidden = vi.spyOn(document, "hidden", "get").mockReturnValue(false);
    const view = render(<KingdomBrand />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    hover(brand);
    advance(2999);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    hidden.mockReturnValue(true);
    fireEvent(document, new Event("visibilitychange"));
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    expect(vi.getTimerCount()).toBe(0);
    hidden.mockReturnValue(false);
    fireEvent(document, new Event("visibilitychange"));
    advance(3000);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    hover(brand);
    expect(vi.getTimerCount()).toBe(1);
    view.unmount();
    expect(vi.getTimerCount()).toBe(0);
    advance(3000);
  });

  it.each(["touch", "pen"])("does not schedule a reveal for a %s pointer", (pointerType) => {
    render(<KingdomBrand />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    fireEvent.pointerEnter(brand, { pointerType, buttons: 0 });
    advance(3000);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not treat a mouse drag or keyboard focus as a hover", () => {
    render(<KingdomBrand />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    fireEvent.pointerEnter(brand, { pointerType: "mouse", buttons: 1 });
    fireEvent.focus(brand);
    advance(3000);
    expect(brand).toHaveAttribute("data-brand-state", "idle");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("preserves the named home link and reveal when the navigation is collapsed", () => {
    render(<KingdomBrand collapsed />);
    const brand = screen.getByRole("link", { name: "Kingdom" });
    expect(brand).toHaveAttribute("title", "Kingdom");
    expect(brand).toHaveAttribute("href", "/");
    expect(brand.querySelector(".kingdom-brand-title")).toHaveClass("sr-only");
    hover(brand);
    advance(3000);
    expect(brand).toHaveAttribute("data-brand-state", "rex");
    expect(brand).toHaveAccessibleName("Kingdom");
  });
});
