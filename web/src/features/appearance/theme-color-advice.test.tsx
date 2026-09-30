import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ThemeColorAdvice } from "./theme-color-advice";

const warnings = [{ path: "/tokens/text.primary", code: "contrast", message: "Some text may be difficult to read." }];
afterEach(() => vi.useRealTimers());
it("dismisses advisory feedback after eight seconds and lets the user reopen it", () => {
  vi.useFakeTimers();
  render(<ThemeColorAdvice warnings={warnings} />);
  expect(screen.getByRole("status")).toHaveTextContent(/still preview, save and publish/);
  act(() => vi.advanceTimersByTime(8000));
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Color Advice (1)" }));
  expect(screen.getByRole("status")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Dismiss color advice" }));
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
});
it("does not repeat the same advice on unrelated edits and clears it when resolved", () => {
  vi.useFakeTimers();
  const view = render(<ThemeColorAdvice warnings={warnings} />);
  act(() => vi.advanceTimersByTime(8000));
  view.rerender(<ThemeColorAdvice warnings={[...warnings]} />);
  expect(screen.queryByRole("status")).not.toBeInTheDocument();
  view.rerender(<ThemeColorAdvice warnings={[]} />);
  expect(screen.queryByRole("button", { name: /Color Advice/ })).not.toBeInTheDocument();
});
it("keeps advice open while someone reads its details", () => {
  vi.useFakeTimers();
  render(<ThemeColorAdvice warnings={warnings} />);
  fireEvent.click(screen.getByRole("button", { name: "Show Details" }));
  act(() => vi.advanceTimersByTime(20000));
  expect(screen.getByText(warnings[0].message)).toBeVisible();
});
