import { useState } from "react";
import { act, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { useScrollViewportHeight } from "./use-scroll-viewport-height";

let resize: (() => void) | undefined;
const original = globalThis.ResizeObserver;
afterEach(() => {
  globalThis.ResizeObserver = original;
  vi.restoreAllMocks();
  resize = undefined;
});

function Probe({ enabled }: { enabled: boolean }) {
  const [element, setElement] = useState<HTMLDivElement | null>(null);
  const height = useScrollViewportHeight(element, enabled);
  return (
    <div ref={setElement} style={{ paddingBottom: "28px" }}>
      <output>{height ?? "none"}</output>
    </div>
  );
}

it("measures the viewport less its bottom padding and follows resizes only while enabled", () => {
  globalThis.ResizeObserver = class {
    constructor(callback: ResizeObserverCallback) {
      resize = () => callback([], this as unknown as ResizeObserver);
    }
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
  let clientHeight = 800;
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockImplementation(() => clientHeight);

  const view = render(<Probe enabled />);
  expect(screen.getByRole("status")).toHaveTextContent("772");
  clientHeight = 600;
  act(() => resize?.());
  expect(screen.getByRole("status")).toHaveTextContent("572");
  view.rerender(<Probe enabled={false} />);
  expect(screen.getByRole("status")).toHaveTextContent("none");
});
