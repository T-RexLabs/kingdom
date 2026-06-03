import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PlannerMessageEditor } from "./planner-message-editor";

let mockScrollHeight = 80;
const originalScrollHeightDescriptor = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "scrollHeight");
const resizeObservers: MockResizeObserver[] = [];

class MockResizeObserver {
  readonly elements = new Set<Element>();
  private callback: ResizeObserverCallback;

  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
    resizeObservers.push(this);
  }

  disconnect = () => {
    this.elements.clear();
  };

  observe = (element: Element) => {
    this.elements.add(element);
  };

  unobserve = (element: Element) => {
    this.elements.delete(element);
  };

  trigger(element: Element) {
    this.callback(
      [
        {
          target: element,
          contentRect: element.getBoundingClientRect(),
        } as ResizeObserverEntry,
      ],
      this as unknown as ResizeObserver,
    );
  }
}

describe("PlannerMessageEditor", () => {
  beforeEach(() => {
    mockScrollHeight = 80;
    resizeObservers.length = 0;
    vi.stubGlobal("ResizeObserver", MockResizeObserver);
    Object.defineProperty(HTMLTextAreaElement.prototype, "scrollHeight", {
      configurable: true,
      get() {
        return mockScrollHeight;
      },
    });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    if (originalScrollHeightDescriptor) {
      Object.defineProperty(HTMLTextAreaElement.prototype, "scrollHeight", originalScrollHeightDescriptor);
      return;
    }
    Reflect.deleteProperty(HTMLTextAreaElement.prototype, "scrollHeight");
  });

  it("keeps the editor scrollbar hidden while the draft fits", () => {
    render(<PlannerMessageEditor initialText="Short draft" onSubmit={() => {}} onCancel={() => {}} />);

    const textarea = screen.getByRole("textbox");
    expect(textarea).toHaveClass("overflow-hidden");
    expect(textarea).not.toHaveClass("overflow-y-auto");
    expect(textarea).toHaveStyle({ height: "80px" });
  });

  it("switches to internal scrolling when the editor reaches its max height", () => {
    mockScrollHeight = 420;

    render(
      <PlannerMessageEditor
        initialText={"Long draft\n".repeat(40)}
        onSubmit={() => {}}
        onCancel={() => {}}
      />
    );

    const textarea = screen.getByRole("textbox");
    expect(textarea).toHaveClass("overflow-y-auto");
    expect(textarea).not.toHaveClass("overflow-hidden");
    expect(textarea).toHaveStyle({ height: "320px" });
  });

  it("remeasures the editor when layout changes without editing the draft", () => {
    render(<PlannerMessageEditor initialText="Wrapped draft" onSubmit={() => {}} onCancel={() => {}} />);

    const textarea = screen.getByRole("textbox");
    expect(textarea).toHaveClass("overflow-hidden");

    mockScrollHeight = 420;

    act(() => {
      resizeObservers[0]?.trigger(textarea);
    });

    expect(textarea).toHaveClass("overflow-y-auto");
    expect(textarea).toHaveStyle({ height: "320px" });
  });
});
