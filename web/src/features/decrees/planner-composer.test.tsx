import { act, fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PlannerComposer } from "./planner-composer";

let mockScrollHeight = 40;
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

describe("PlannerComposer", () => {
  beforeEach(() => {
    mockScrollHeight = 40;
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

  it("hides the internal scrollbar while the content fits within the max height", () => {
    render(<PlannerComposer value="Short reply" onChange={() => {}} onSend={() => {}} />);

    const textarea = screen.getByPlaceholderText("Type your response...");
    expect(textarea).toHaveClass("overflow-hidden");
    expect(textarea).not.toHaveClass("overflow-y-auto");
    expect(textarea).toHaveStyle({ height: "40px" });
  });

  it("enables internal scrolling only once the composer hits its height cap", () => {
    const { rerender } = render(<PlannerComposer value="Short reply" onChange={() => {}} onSend={() => {}} />);

    mockScrollHeight = 240;
    rerender(<PlannerComposer value={"Long reply\n".repeat(24)} onChange={() => {}} onSend={() => {}} />);

    const textarea = screen.getByPlaceholderText("Type your response...");
    expect(textarea).toHaveClass("overflow-y-auto");
    expect(textarea).not.toHaveClass("overflow-hidden");
    expect(textarea).toHaveStyle({ height: "160px" });
  });

  it("remeasures overflow when layout changes without a new keystroke", () => {
    render(<PlannerComposer value="Wrapped reply" onChange={() => {}} onSend={() => {}} />);

    const textarea = screen.getByPlaceholderText("Type your response...");
    expect(textarea).toHaveClass("overflow-hidden");

    mockScrollHeight = 240;

    act(() => {
      resizeObservers[0]?.trigger(textarea);
    });

    expect(textarea).toHaveClass("overflow-y-auto");
    expect(textarea).toHaveStyle({ height: "160px" });
  });

  it("submits on Enter without Shift", () => {
    let currentValue = "Reply";
    let sendCount = 0;

    render(
      <PlannerComposer
        value={currentValue}
        onChange={(next) => {
          currentValue = next;
        }}
        onSend={() => {
          sendCount += 1;
        }}
      />
    );

    fireEvent.keyDown(screen.getByPlaceholderText("Type your response..."), { key: "Enter" });
    expect(sendCount).toBe(1);
  });

  it("accepts an external textarea ref for programmatic focus", () => {
    const textareaRef = createRef<HTMLTextAreaElement>();

    render(
      <PlannerComposer value="Reply" textareaRef={textareaRef} onChange={() => {}} onSend={() => {}} />
    );

    textareaRef.current?.focus();

    expect(screen.getByPlaceholderText("Type your response...")).toHaveFocus();
  });
});
