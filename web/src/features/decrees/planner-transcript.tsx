import { ArrowDown } from "lucide-react";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Virtuoso, type VirtuosoHandle } from "react-virtuoso";
import { PlannerMessageCard } from "./planner-message";
import { PlannerQuestionChoices } from "./planner-question-choices";
import type { AnswerPayload } from "./planner-operation-controller";
import type { TranscriptItem } from "./planner-transcript-view-model";

function normalizeSelectedValues(answerPayload?: AnswerPayload): string[] | null {
  if (!answerPayload) return null;
  const values = answerPayload.values?.filter(Boolean) ?? [];
  return values.length > 0 ? values : null;
}

export type TranscriptCallbacks = {
  onSelect?: (questionId: string, option: { label: string; value: string }) => void;
  onSubmitMultiSelect?: (questionId: string, options: Array<{ label: string; value: string }>) => void;
  onTypeYourAnswer?: (questionId: string) => void;
  onEditMessage?: (messageId: string) => void;
  onEditSubmit?: (text: string) => void;
  onEditCancel?: () => void;
  onRegenerate?: (messageId: string, parentMessageId?: string) => void;
  onActivateVariant?: (messageId: string) => void;
  sessionId?: string;
  isProcessing?: boolean;
  editingMessageId?: string | null;
  editText?: string;
  onEditChange?: (text: string) => void;
};

const LATEST_VISIBILITY_THRESHOLD_PX = 80;

function renderMessage(item: TranscriptItem, callbacks: TranscriptCallbacks, isFirstAssistant: boolean) {
  const question = item.message.question_payload;
  const isEditing = callbacks.editingMessageId === item.message.planning_message_id;
  return (
    <PlannerMessageCard
      message={item.message}
      isActive={item.isInteractive}
      isStreaming={item.isStreaming}
      contentMarkdown={item.contentMarkdown}
      onEdit={callbacks.onEditMessage ? () => callbacks.onEditMessage?.(item.message.planning_message_id) : undefined}
      onRegenerate={callbacks.onRegenerate ? () => callbacks.onRegenerate?.(item.message.planning_message_id, item.message.parent_message_id) : undefined}
      isEditing={isEditing}
      onEditSubmit={callbacks.onEditSubmit}
      editText={callbacks.editText}
      onEditChange={callbacks.onEditChange}
      onEditCancel={callbacks.onEditCancel}
      isProcessing={callbacks.isProcessing}
      isFirstAssistant={isFirstAssistant}
      variantCount={item.variantCount}
      currentVariantIndex={item.currentVariantIndex}
      sessionId={callbacks.sessionId}
      onActivateVariant={callbacks.onActivateVariant}
      activityRunId={item.activityRunId}
      liveActivityItems={item.liveActivityItems}
    >
      {item.message.role === "assistant" && question ? (
        <PlannerQuestionChoices
          question={question}
          disabled={!item.isInteractive}
          selectedValue={item.resolvedAnswer?.value}
          selectedValues={normalizeSelectedValues(item.resolvedAnswer)}
          onTypeYourAnswer={
            item.isInteractive
              ? () => callbacks.onTypeYourAnswer?.(item.message.planning_message_id)
              : undefined
          }
          onSelect={
            item.isInteractive
              ? (option) => callbacks.onSelect?.(item.message.planning_message_id, option)
              : undefined
          }
          onSubmitMultiSelect={
            item.isInteractive
              ? (options) => callbacks.onSubmitMultiSelect?.(item.message.planning_message_id, options)
              : undefined
          }
        />
      ) : null}
    </PlannerMessageCard>
  );
}

export function PlannerTranscript({
  items,
  firstItemIndex,
  totalCount,
  hasPreviousPage,
  isFetchingPreviousPage,
  onLoadPrevious,
  followLatestRequest,
  callbacks,
}: {
  items: TranscriptItem[];
  firstItemIndex: number;
  totalCount: number;
  hasPreviousPage?: boolean;
  isFetchingPreviousPage?: boolean;
  onLoadPrevious?: () => void;
  followLatestRequest?: number;
  callbacks: TranscriptCallbacks;
}) {
  const [isAtBottom, setIsAtBottom] = useState(true);
  const [isPinned, setIsPinned] = useState(true);
  const [scrollerElement, setScrollerElement] = useState<HTMLElement | null>(null);
  const [tailContentElement, setTailContentElement] = useState<HTMLDivElement | null>(null);
  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const isPinnedRef = useRef(true);
  const lastFollowLatestRequestRef = useRef<number | null>(followLatestRequest ?? null);
  const pinFrameRef = useRef<number | null>(null);
  const lastScrollTopRef = useRef(0);
  const pointerInteractionRef = useRef(false);
  const touchInteractionRef = useRef(false);
  const wheelDetachPendingRef = useRef(false);
  const keyboardDetachPendingRef = useRef(false);

  const firstAssistantId = useMemo(() => {
    const first = items.find((item) => item.message.role === "assistant");
    return first?.message.planning_message_id;
  }, [items]);

  const setVirtuosoScroller = useCallback((node: HTMLElement | null | Window) => {
    setScrollerElement(node instanceof HTMLElement ? node : null);
  }, []);

  const setTailContent = useCallback((node: HTMLDivElement | null) => {
    setTailContentElement(node);
  }, []);

  const clearOneShotDetachIntent = useCallback(() => {
    wheelDetachPendingRef.current = false;
    keyboardDetachPendingRef.current = false;
  }, []);

  const hasDetachIntent = useCallback(
    () =>
      pointerInteractionRef.current ||
      touchInteractionRef.current ||
      wheelDetachPendingRef.current ||
      keyboardDetachPendingRef.current,
    [],
  );

  const updateAtBottom = useCallback((next: boolean) => {
    setIsAtBottom((current) => (current === next ? current : next));
  }, []);

  const updatePinned = useCallback((next: boolean) => {
    isPinnedRef.current = next;
    setIsPinned((current) => (current === next ? current : next));
  }, []);

  // --- Entrance animation ---
  // Track the highest virtual index seen so far. Items above this watermark are
  // "new". Prepended items (older pages) always have lower indices, so they
  // never animate.
  const maxSeenIndexRef = useRef(firstItemIndex + items.length - 1);

  // Persist which keys have been marked for animation. Once added, a key stays
  // permanently — the CSS animation plays once (animation-fill-mode: both) and
  // the final state (opacity: 1, transform: none) is neutral. This prevents
  // streaming re-renders from stripping the animation class mid-flight.
  const animatedKeysRef = useRef(new Set<string>());

  // Compute new-item membership in a layout effect rather than mutating refs
  // inside the Virtuoso `itemContent` render function. React's render contract
  // requires render functions to be pure; mutating refs there breaks
  // concurrent-mode invariants and risks the animation class flickering when
  // Virtuoso re-runs `itemContent` during reconciliation.
  useLayoutEffect(() => {
    for (let i = 0; i < items.length; i++) {
      const idx = firstItemIndex + i;
      if (idx > maxSeenIndexRef.current) {
        animatedKeysRef.current.add(items[i].key);
      }
    }
    // Drop animation tracking for keys that have left the dataset (e.g. after
    // a regenerate truncates the transcript) so the Set does not grow without
    // bound across long sessions.
    const keep = new Set(items.map((item) => item.key));
    for (const key of animatedKeysRef.current) {
      if (!keep.has(key)) animatedKeysRef.current.delete(key);
    }
    maxSeenIndexRef.current = firstItemIndex + items.length - 1;
  }, [firstItemIndex, items]);

  // Glue the viewport to the tail by aligning the last item's bottom edge with
  // the viewport bottom. Unlike `autoscrollToBottom()` (which is gated on
  // Virtuoso already considering itself at-bottom and silently no-ops once
  // streaming content has pushed the tail below the fold), `scrollToIndex`
  // scrolls unconditionally — so it keeps following as the bubble grows with
  // streamed reply text, thinking, ACP events, and sources. Instant positioning
  // ("auto") is used because smooth scrolling can't track a target whose height
  // is changing mid-animation and produces jitter on virtualized lists.
  //
  // Requests are coalesced into a single rAF so a burst of resize/append ticks
  // within one frame produces exactly one scroll.
  const pinToLatest = useCallback(() => {
    if (items.length === 0 || pinFrameRef.current != null) {
      return;
    }

    let ranSynchronously = false;
    const frameId = requestAnimationFrame(() => {
      ranSynchronously = true;
      pinFrameRef.current = null;
      virtuosoRef.current?.scrollToIndex({ index: "LAST", align: "end", behavior: "auto" });
    });

    if (!ranSynchronously) {
      pinFrameRef.current = frameId;
    }
  }, [items.length]);

  // --- Scroll-to-edit ---
  // Scroll the edited item into view using Virtuoso's imperative API.
  // Uses behavior: "auto" (instant) because smooth scrolling can't work when
  // the item height changes simultaneously — the scroll target becomes stale
  // mid-animation, causing jitter. Instant positioning is the industry standard
  // for virtualized lists with dynamic item sizes.
  const lastScrolledEditIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (!callbacks.editingMessageId) {
      lastScrolledEditIdRef.current = null;
      return;
    }
    // Only scroll once per edit session — don't re-scroll on streaming data changes
    if (lastScrolledEditIdRef.current === callbacks.editingMessageId) return;

    const itemIndex = items.findIndex(
      (item) => item.message.planning_message_id === callbacks.editingMessageId
    );
    const handle = virtuosoRef.current;
    if (itemIndex < 0 || !handle) return;

    handle.scrollIntoView({
      index: firstItemIndex + itemIndex,
      align: "center",
      behavior: "auto",
    });
    lastScrolledEditIdRef.current = callbacks.editingMessageId;
  }, [callbacks.editingMessageId, items, firstItemIndex]);

  useEffect(() => {
    if (followLatestRequest == null) {
      return;
    }
    if (lastFollowLatestRequestRef.current === followLatestRequest) {
      return;
    }

    lastFollowLatestRequestRef.current = followLatestRequest;
    const frameId = requestAnimationFrame(() => {
      updatePinned(true);
      pinToLatest();
    });
    return () => cancelAnimationFrame(frameId);
  }, [followLatestRequest, pinToLatest, updatePinned]);

  // Single growth detector. We observe the tail item's content box — the element
  // that actually grows as the reply text, live thinking, ACP events, and sources
  // stream in — plus the scroller for viewport-height changes. A zero-height
  // anchor never resizes, so observing the real content box is what keeps the
  // view glued to the bottom during a thinking/ACP burst (which never changes the
  // `items` array and so never triggers `followOutput`).
  useEffect(() => {
    if (typeof ResizeObserver === "undefined" || !scrollerElement) {
      return;
    }

    const observer = new ResizeObserver(() => {
      if (isPinnedRef.current && !hasDetachIntent()) {
        pinToLatest();
      }
    });

    observer.observe(scrollerElement);
    if (tailContentElement) {
      observer.observe(tailContentElement);
    }

    return () => {
      observer.disconnect();
    };
  }, [hasDetachIntent, pinToLatest, scrollerElement, tailContentElement]);

  useEffect(() => {
    if (!scrollerElement) {
      return;
    }

    lastScrollTopRef.current = scrollerElement.scrollTop;

    const clearPointerInteraction = () => {
      pointerInteractionRef.current = false;
    };

    const clearTouchInteraction = () => {
      touchInteractionRef.current = false;
    };

    const handlePointerDown = () => {
      pointerInteractionRef.current = true;
    };

    const handleTouchStart = () => {
      touchInteractionRef.current = true;
    };

    const handleWheel = (event: WheelEvent) => {
      wheelDetachPendingRef.current = event.deltaY < 0;
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      keyboardDetachPendingRef.current = ["ArrowUp", "PageUp", "Home"].includes(event.key);
    };

    const handleScroll = () => {
      const currentScrollTop = scrollerElement.scrollTop;
      const previousScrollTop = lastScrollTopRef.current;
      lastScrollTopRef.current = currentScrollTop;

      if (!isPinnedRef.current) {
        clearOneShotDetachIntent();
        return;
      }

      // Only an upward scroll backed by a real user gesture detaches. A
      // programmatic scroll-to-bottom moves downward and carries no detach
      // intent, so it can never be misread as the user leaving the tail.
      if (currentScrollTop < previousScrollTop - 1 && hasDetachIntent()) {
        updatePinned(false);
      }

      clearOneShotDetachIntent();
    };

    scrollerElement.addEventListener("pointerdown", handlePointerDown, { passive: true });
    window.addEventListener("pointerup", clearPointerInteraction, { passive: true });
    window.addEventListener("pointercancel", clearPointerInteraction, { passive: true });
    scrollerElement.addEventListener("touchstart", handleTouchStart, { passive: true });
    window.addEventListener("touchend", clearTouchInteraction, { passive: true });
    window.addEventListener("touchcancel", clearTouchInteraction, { passive: true });
    scrollerElement.addEventListener("wheel", handleWheel, { passive: true });
    scrollerElement.addEventListener("keydown", handleKeyDown);
    scrollerElement.addEventListener("scroll", handleScroll, { passive: true });

    return () => {
      scrollerElement.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("pointerup", clearPointerInteraction);
      window.removeEventListener("pointercancel", clearPointerInteraction);
      scrollerElement.removeEventListener("touchstart", handleTouchStart);
      window.removeEventListener("touchend", clearTouchInteraction);
      window.removeEventListener("touchcancel", clearTouchInteraction);
      scrollerElement.removeEventListener("wheel", handleWheel);
      scrollerElement.removeEventListener("keydown", handleKeyDown);
      scrollerElement.removeEventListener("scroll", handleScroll);
    };
  }, [clearOneShotDetachIntent, hasDetachIntent, scrollerElement, updatePinned]);

  useEffect(() => {
    return () => {
      if (pinFrameRef.current != null) {
        cancelAnimationFrame(pinFrameRef.current);
      }
    };
  }, []);

  const showScrollToLatest = items.length > 0 && !isPinned && !isAtBottom;

  return (
    <div className="relative flex-1 min-h-0">
      <Virtuoso
        ref={virtuosoRef}
        scrollerRef={setVirtuosoScroller}
        className="flex-1 min-h-0 overflow-x-hidden py-4"
        alignToBottom
        data={items}
        totalCount={Math.max(totalCount, firstItemIndex + items.length)}
        firstItemIndex={Math.max(firstItemIndex, 0)}
        initialTopMostItemIndex={items.length > 0 ? firstItemIndex + items.length - 1 : 0}
        followOutput={() => (isPinned ? "auto" : false)}
        atBottomThreshold={LATEST_VISIBILITY_THRESHOLD_PX}
        atBottomStateChange={(atBottom) => {
          updateAtBottom(atBottom);
          if (atBottom) {
            updatePinned(true);
          }
        }}
        startReached={() => {
          if (!hasPreviousPage || isFetchingPreviousPage) {
            return;
          }
          onLoadPrevious?.();
        }}
        computeItemKey={(_index, item) => item.key}
        itemContent={(index, item) => {
          // Pure read — animation membership is computed in the layout effect
          // above. itemContent stays a pure render function so React 19 strict
          // mode and Virtuoso reconciliation never corrupt the Set.
          const shouldAnimate = animatedKeysRef.current.has(item.key);
          // The tail item's content box is what the ResizeObserver watches: it
          // grows as reply text, thinking, ACP events, and sources stream in,
          // which is exactly when we need to re-pin to the bottom.
          const isTailItem = index === items.length - 1;
          return (
            <div
              ref={isTailItem ? setTailContent : undefined}
              className={`px-4 ${index === 0 ? "pt-1" : "pt-4"} ${shouldAnimate ? "animate-chat-bubble-in" : ""}`}
            >
              {renderMessage(item, callbacks, item.message.planning_message_id === firstAssistantId)}
            </div>
          );
        }}
      />

      {showScrollToLatest ? (
        <button
          type="button"
          onClick={() => {
            updatePinned(true);
            pinToLatest();
          }}
          className="absolute right-4 bottom-4 z-20 flex items-center gap-1.5 rounded-full border border-border-strong bg-surface-overlay px-3 py-1.5 text-xs font-medium text-foreground shadow-lg transition-colors hover:bg-surface-raised hover:text-gold"
          style={{ bottom: "max(1rem, env(safe-area-inset-bottom))" }}
        >
          <ArrowDown className="h-3.5 w-3.5" />
          Scroll to latest
        </button>
      ) : null}
    </div>
  );
}
