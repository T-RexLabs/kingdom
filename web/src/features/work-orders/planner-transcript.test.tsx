import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { VirtuosoMockContext } from "react-virtuoso";
import React from "react";
import { PlannerTranscript } from "./planner-transcript";
import type { TranscriptItem } from "./planner-transcript-view-model";

let capturedVirtuosoProps: Record<string, unknown> | null = null;
const scrollIntoViewSpy = vi.fn();
const scrollToIndexSpy = vi.fn();
const resizeObservers: MockResizeObserver[] = [];

class MockResizeObserver {
  readonly elements = new Set<Element>();
  private callback: ResizeObserverCallback;

  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
    resizeObservers.push(this);
  }

  disconnect = vi.fn(() => {
    this.elements.clear();
  });

  observe = vi.fn((element: Element) => {
    this.elements.add(element);
  });

  unobserve = vi.fn((element: Element) => {
    this.elements.delete(element);
  });

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

vi.mock("react-virtuoso", async () => {
  const actual = await vi.importActual<typeof import("react-virtuoso")>("react-virtuoso");
  const ReactMod = await import("react");
  return {
    ...actual,
    Virtuoso: ReactMod.forwardRef(function MockVirtuoso(
      props: {
        data: TranscriptItem[];
        itemContent: (index: number, item: TranscriptItem) => React.ReactNode;
        startReached?: () => void;
        computeItemKey: (index: number, item: TranscriptItem) => string;
        followOutput: ((isAtBottom: boolean) => "auto" | false) | "auto" | false;
        firstItemIndex: number;
        totalCount: number;
        initialTopMostItemIndex?: number;
        atBottomThreshold?: number;
        atBottomStateChange?: (isAtBottom: boolean) => void;
        scrollerRef?: (ref: HTMLElement | null | Window) => void;
      },
      ref: React.Ref<{
        scrollIntoView: typeof scrollIntoViewSpy;
        scrollToIndex: typeof scrollToIndexSpy;
      }>,
    ) {
      const scrollerRef = ReactMod.useRef<HTMLDivElement | null>(null);
      ReactMod.useImperativeHandle(ref, () => ({
        scrollIntoView: scrollIntoViewSpy,
        scrollToIndex: scrollToIndexSpy,
      }));
      ReactMod.useEffect(() => {
        props.scrollerRef?.(scrollerRef.current);
        return () => props.scrollerRef?.(null);
      }, [props.scrollerRef]);
      capturedVirtuosoProps = props;
      return (
        <div data-testid="virtuoso-scroller" ref={scrollerRef} tabIndex={0}>
          {props.data.map((item, index) => (
            <div key={props.computeItemKey(index, item)}>{props.itemContent(index, item)}</div>
          ))}
        </div>
      );
    }),
  };
});

vi.mock("./planner-message", () => ({
  PlannerMessageCard: ({ message, children }: { message: { content_markdown: string }; children?: React.ReactNode }) => (
    <div>
      <div>{message.content_markdown}</div>
      {children}
    </div>
  ),
}));

vi.mock("./planner-question-choices", () => ({
  PlannerQuestionChoices: () => null,
}));

function assistantItem(id: string, content: string): TranscriptItem {
  return {
    kind: "message",
    key: `message-${id}`,
    message: {
      planning_message_id: id,
      planning_session_id: "session-1",
      role: "assistant",
      content_markdown: content,
      created_at: "2026-04-22T10:00:00Z",
    },
    isInteractive: false,
    isCanonical: true,
  };
}

function userItem(id: string, content: string): TranscriptItem {
  return {
    kind: "message",
    key: `message-${id}`,
    message: {
      planning_message_id: id,
      planning_session_id: "session-1",
      role: "user",
      content_markdown: content,
      created_at: "2026-04-22T10:00:00Z",
    },
    isInteractive: false,
    isCanonical: true,
  };
}

describe("PlannerTranscript virtualization wiring", () => {
  beforeEach(() => {
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      callback(0);
      return 1;
    });
    vi.stubGlobal("cancelAnimationFrame", () => {});
    vi.stubGlobal("ResizeObserver", MockResizeObserver);
    capturedVirtuosoProps = null;
    resizeObservers.length = 0;
    scrollIntoViewSpy.mockClear();
    scrollToIndexSpy.mockClear();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  function resolveFollowOutput(isAtBottom = false) {
    const followOutput = capturedVirtuosoProps?.followOutput;
    return typeof followOutput === "function" ? followOutput(isAtBottom) : followOutput;
  }

  function setScrollerScrollTop(value: number) {
    const scroller = screen.getByTestId("virtuoso-scroller");
    Object.defineProperty(scroller, "scrollTop", {
      configurable: true,
      writable: true,
      value,
    });
    return scroller;
  }

  // The transcript registers a single ResizeObserver over the scroller and the
  // tail item's content box. Firing it simulates the tail growing as reply
  // text / thinking / ACP events / sources stream in.
  function triggerTailResize() {
    const observer = resizeObservers.at(-1);
    const element = observer ? Array.from(observer.elements).at(-1) : null;
    if (!observer || !element) {
      throw new Error("Tail content resize observer was not registered");
    }

    act(() => {
      observer.trigger(element);
    });
  }

  it("loads older pages via startReached and preserves ascending rendered order after prepend", () => {
    const onLoadPrevious = vi.fn();

    const { rerender } = render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-3", "m-3"), assistantItem("m-4", "m-4")]}
          firstItemIndex={2}
          totalCount={4}
          hasPreviousPage
          isFetchingPreviousPage={false}
          onLoadPrevious={onLoadPrevious}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    act(() => {
      (capturedVirtuosoProps?.startReached as (() => void) | undefined)?.();
    });
    expect(onLoadPrevious).toHaveBeenCalledTimes(1);

    rerender(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[
            assistantItem("m-1", "m-1"),
            assistantItem("m-2", "m-2"),
            assistantItem("m-3", "m-3"),
            assistantItem("m-4", "m-4"),
          ]}
          firstItemIndex={0}
          totalCount={4}
          hasPreviousPage={false}
          isFetchingPreviousPage={false}
          onLoadPrevious={onLoadPrevious}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    expect(screen.getAllByText(/m-[1-4]/).map((node) => node.textContent)).toEqual(["m-1", "m-2", "m-3", "m-4"]);
    expect(capturedVirtuosoProps?.firstItemIndex).toBe(0);
    expect(capturedVirtuosoProps?.totalCount).toBe(4);
  });

  it("keeps follow mode pinned when bottom geometry changes without user scroll intent", () => {
    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1")]}
          firstItemIndex={0}
          totalCount={1}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    expect(resolveFollowOutput()).toBe("auto");
    expect(capturedVirtuosoProps?.atBottomThreshold).toBe(80);

    act(() => {
      (capturedVirtuosoProps?.atBottomStateChange as ((isAtBottom: boolean) => void) | undefined)?.(false);
    });

    expect(resolveFollowOutput()).toBe("auto");
  });

  it("re-pins to the bottom when the tail content grows while following", async () => {
    // Regression: live thinking / ACP events / sources grow the tail bubble
    // without changing the `items` array, so `followOutput` never fires. The
    // tail-content ResizeObserver is what keeps the view glued to the bottom.
    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "older"), assistantItem("m-2", "latest")]}
          firstItemIndex={0}
          totalCount={2}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    scrollToIndexSpy.mockClear();

    triggerTailResize();

    await waitFor(() => {
      expect(scrollToIndexSpy).toHaveBeenCalledWith({ index: "LAST", align: "end", behavior: "auto" });
    });
    expect(resolveFollowOutput()).toBe("auto");
  });

  it("does not re-pin on tail growth after the operator has scrolled away", () => {
    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1"), assistantItem("m-2", "m-2")]}
          firstItemIndex={0}
          totalCount={2}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    const scroller = setScrollerScrollTop(240);
    fireEvent.scroll(scroller);
    fireEvent.pointerDown(scroller);
    setScrollerScrollTop(120);
    fireEvent.scroll(scroller);

    expect(resolveFollowOutput()).toBe(false);

    scrollToIndexSpy.mockClear();
    triggerTailResize();

    expect(scrollToIndexSpy).not.toHaveBeenCalled();
  });

  it("detaches only when the operator actually scrolls away from the bottom", () => {
    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1"), assistantItem("m-2", "m-2")]}
          firstItemIndex={0}
          totalCount={2}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    const scroller = setScrollerScrollTop(240);
    fireEvent.scroll(scroller);
    fireEvent.pointerDown(scroller);
    setScrollerScrollTop(120);
    fireEvent.scroll(scroller);

    expect(resolveFollowOutput()).toBe(false);
  });

  it("repins to latest when an explicit follow request arrives", async () => {
    const { rerender } = render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1"), assistantItem("m-2", "m-2")]}
          firstItemIndex={0}
          totalCount={2}
          followLatestRequest={0}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    const scroller = setScrollerScrollTop(240);
    fireEvent.scroll(scroller);
    fireEvent.pointerDown(scroller);
    setScrollerScrollTop(120);
    fireEvent.scroll(scroller);

    act(() => {
      (capturedVirtuosoProps?.atBottomStateChange as ((isAtBottom: boolean) => void) | undefined)?.(false);
    });

    expect(resolveFollowOutput()).toBe(false);

    rerender(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1"), assistantItem("m-2", "m-2")]}
          firstItemIndex={0}
          totalCount={2}
          followLatestRequest={1}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    await waitFor(() => {
      expect(scrollToIndexSpy).toHaveBeenCalledWith({
        index: "LAST",
        align: "end",
        behavior: "auto",
      });
    });
    expect(resolveFollowOutput()).toBe("auto");
  });

  it("keeps the tail pinned through later transcript updates after an explicit follow request", async () => {
    const { rerender } = render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1")]}
          firstItemIndex={0}
          totalCount={1}
          followLatestRequest={0}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    rerender(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1")]}
          firstItemIndex={0}
          totalCount={1}
          followLatestRequest={1}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    scrollToIndexSpy.mockClear();

    rerender(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1"), assistantItem("m-2", "streaming reply") ]}
          firstItemIndex={0}
          totalCount={2}
          followLatestRequest={1}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    // Still pinned after the append (Virtuoso's followOutput stays active), and
    // the tail bubble growing as the reply streams rides the bottom.
    expect(resolveFollowOutput()).toBe("auto");

    triggerTailResize();

    await waitFor(() => {
      expect(scrollToIndexSpy).toHaveBeenCalledWith({ index: "LAST", align: "end", behavior: "auto" });
    });
  });

  it("shows a scroll to latest affordance when the operator scrolls away", async () => {
    const user = userEvent.setup();

    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-1", "m-1"), assistantItem("m-2", "m-2")]}
          firstItemIndex={0}
          totalCount={2}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    expect(screen.queryByRole("button", { name: "Scroll to latest" })).not.toBeInTheDocument();

    const scroller = setScrollerScrollTop(240);
    fireEvent.scroll(scroller);
    fireEvent.pointerDown(scroller);
    setScrollerScrollTop(120);
    fireEvent.scroll(scroller);

    act(() => {
      (capturedVirtuosoProps?.atBottomStateChange as ((isAtBottom: boolean) => void) | undefined)?.(false);
    });

    await user.click(screen.getByRole("button", { name: "Scroll to latest" }));

    await waitFor(() => {
      expect(scrollToIndexSpy).toHaveBeenCalledWith({
        index: "LAST",
        align: "end",
        behavior: "auto",
      });
    });
  });

  it("loads missing history before restoring a saved message anchor", async () => {
    const loadPrevious = vi.fn();
    const initialPosition = { messageId: "old-message", offset: -24, pinned: false };
    const { rerender } = render(<PlannerTranscript items={[assistantItem("recent", "Recent")]} firstItemIndex={50} totalCount={51} hasPreviousPage onLoadPrevious={loadPrevious} initialPosition={initialPosition} callbacks={{}} />);
    await waitFor(() => expect(loadPrevious).toHaveBeenCalledTimes(1));
    expect(screen.getByRole("status")).toHaveTextContent("Restoring your place");
    expect(capturedVirtuosoProps?.followOutput && (capturedVirtuosoProps.followOutput as () => false)()).toBe(false);
    rerender(<PlannerTranscript items={[assistantItem("old-message", "Older"), assistantItem("recent", "Recent")]} firstItemIndex={49} totalCount={51} hasPreviousPage onLoadPrevious={loadPrevious} initialPosition={initialPosition} callbacks={{}} />);
    await waitFor(() => expect(scrollToIndexSpy).toHaveBeenCalledWith({ index: 49, align: "start", offset: 24, behavior: "auto" }));
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("sets initialTopMostItemIndex to the virtual index of the last item", () => {
    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-3", "m-3"), assistantItem("m-4", "m-4")]}
          firstItemIndex={5}
          totalCount={7}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    // Virtual index = firstItemIndex + (data.length - 1) = 5 + 1 = 6
    expect(capturedVirtuosoProps?.initialTopMostItemIndex).toBe(6);
  });

  it("sets initialTopMostItemIndex to 0 for empty data", () => {
    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[]}
          firstItemIndex={5}
          totalCount={5}
          callbacks={{}}
        />
      </VirtuosoMockContext.Provider>
    );

    expect(capturedVirtuosoProps?.initialTopMostItemIndex).toBe(0);
  });

  it("scrolls to the correct virtual index when editing a message", () => {
    const items = [
      userItem("m-1", "m-1"),
      assistantItem("m-2", "m-2"),
      userItem("m-3", "m-3"),
    ];

    render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={items}
          firstItemIndex={10}
          totalCount={13}
          callbacks={{ editingMessageId: "m-2" }}
        />
      </VirtuosoMockContext.Provider>
    );

    // m-2 is at local index 1, virtual index = 10 + 1 = 11
    expect(scrollIntoViewSpy).toHaveBeenCalledWith({
      index: 11,
      align: "center",
      behavior: "auto",
    });
  });

  it("retries edit scroll when the target message arrives in a later data update", () => {
    // Initial render: only m-3 and m-4 loaded, editing m-1 which is NOT in data yet
    const { rerender } = render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[assistantItem("m-3", "m-3"), assistantItem("m-4", "m-4")]}
          firstItemIndex={2}
          totalCount={4}
          callbacks={{ editingMessageId: "m-1" }}
        />
      </VirtuosoMockContext.Provider>
    );

    // m-1 not in data — scroll should NOT have been called
    expect(scrollIntoViewSpy).not.toHaveBeenCalled();

    // Rerender with m-1 now loaded (older page prepended)
    rerender(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[
            userItem("m-1", "m-1"),
            assistantItem("m-2", "m-2"),
            assistantItem("m-3", "m-3"),
            assistantItem("m-4", "m-4"),
          ]}
          firstItemIndex={0}
          totalCount={4}
          callbacks={{ editingMessageId: "m-1" }}
        />
      </VirtuosoMockContext.Provider>
    );

    // Now m-1 is at local index 0, virtual index = 0 + 0 = 0
    expect(scrollIntoViewSpy).toHaveBeenCalledWith({
      index: 0,
      align: "center",
      behavior: "auto",
    });
  });

  it("does not repeat scroll after first success when data changes", () => {
    const items = [
      userItem("m-1", "m-1"),
      assistantItem("m-2", "m-2"),
    ];

    const { rerender } = render(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={items}
          firstItemIndex={0}
          totalCount={2}
          callbacks={{ editingMessageId: "m-1" }}
        />
      </VirtuosoMockContext.Provider>
    );

    expect(scrollIntoViewSpy).toHaveBeenCalledTimes(1);
    scrollIntoViewSpy.mockClear();

    // Rerender with same editingMessageId but new streaming data appended
    rerender(
      <VirtuosoMockContext.Provider value={{ viewportHeight: 500, itemHeight: 40 }}>
        <PlannerTranscript
          items={[...items, assistantItem("m-3", "m-3")]}
          firstItemIndex={0}
          totalCount={3}
          callbacks={{ editingMessageId: "m-1" }}
        />
      </VirtuosoMockContext.Provider>
    );

    // Latch should prevent re-scroll
    expect(scrollIntoViewSpy).not.toHaveBeenCalled();
  });
});
