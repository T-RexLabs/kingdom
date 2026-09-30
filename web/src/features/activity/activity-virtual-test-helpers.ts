import { fireEvent } from "@testing-library/react";
import { vi } from "vitest";

// jsdom has no layout or scrolling. Keep the real Virtuoso renderer while
// supplying the viewport and row geometry specified by VirtuosoMockContext.
export function mockActivityScrollGeometry() {
  const height = (node: HTMLElement) =>
    node.hasAttribute("data-virtuoso-scroller") ? 260 : 52;
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockImplementation(
    function (this: HTMLElement) {
      return height(this);
    },
  );
  vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockImplementation(
    function (this: HTMLElement) {
      const list = this.querySelector<HTMLElement>(
        '[data-testid="virtuoso-item-list"]',
      );
      return list
        ? Number.parseFloat(list.style.marginTop || "0") +
            Number.parseFloat(list.style.paddingTop || "0") +
            Number.parseFloat(list.style.paddingBottom || "0") +
            list.childElementCount * 52
        : height(this);
    },
  );
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(
    function (this: HTMLElement) {
      return new DOMRect(0, 0, 800, height(this));
    },
  );
  const original = Object.getOwnPropertyDescriptor(
    HTMLElement.prototype,
    "scrollTo",
  );
  const originalBy = Object.getOwnPropertyDescriptor(
    HTMLElement.prototype,
    "scrollBy",
  );
  Object.defineProperty(HTMLElement.prototype, "scrollTo", {
    configurable: true,
    value(this: HTMLElement, options: ScrollToOptions) {
      this.scrollTop = Math.max(
        0,
        Math.min(options.top ?? 0, this.scrollHeight - this.offsetHeight),
      );
      fireEvent.scroll(this);
    },
  });
  Object.defineProperty(HTMLElement.prototype, "scrollBy", {
    configurable: true,
    value(this: HTMLElement, options: ScrollToOptions) {
      this.scrollTo({ top: this.scrollTop + (options.top ?? 0) });
    },
  });
  return () => {
    vi.restoreAllMocks();
    if (original)
      Object.defineProperty(HTMLElement.prototype, "scrollTo", original);
    else Reflect.deleteProperty(HTMLElement.prototype, "scrollTo");
    if (originalBy)
      Object.defineProperty(HTMLElement.prototype, "scrollBy", originalBy);
    else Reflect.deleteProperty(HTMLElement.prototype, "scrollBy");
  };
}
