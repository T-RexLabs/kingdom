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
  onRegenerate?: (turnIndex: number) => void;
  onActivateVariant?: (messageId: string) => void;
  sessionId?: string;
  isProcessing?: boolean;
  editingMessageId?: string | null;
};

type PinSyncMode = "force" | "maintain";
const LATEST_VISIBILITY_THRESHOLD_PX = 80;
const PROGRAMMATIC_SYNC_SETTLE_FRAMES = 8;

function isLatestAnchorVisible(scroller: HTMLElement | null, anchor: HTMLDivElement | null) {
  if (!scroller || !anchor) {
    return true;
  }

  const scrollerRect = scroller.getBoundingClientRect();
  const anchorRect = anchor.getBoundingClientRect();
  return anchorRect.top <= scrollerRect.bottom + LATEST_VISIBILITY_THRESHOLD_PX;
}

function buildTailVersion(item: TranscriptItem | undefined) {
  if (!item) {
    return "empty";
  }

  const question = item.message.question_payload;
  const questionOptions = question?.options?.map((option) => `${option.label}:${option.value}`).join("|") ?? "";
  const resolvedValues = item.resolvedAnswer?.values?.join(",") ?? "";

  return [
    item.key,
    item.message.planning_message_id,
    item.contentMarkdown ?? item.message.content_markdown,
    item.isStreaming ? "streaming" : "stable",
    item.isInteractive ? "interactive" : "static",
    question?.prompt ?? "",
    question?.type ?? "",
    questionOptions,
    item.resolvedAnswer?.value ?? "",
    resolvedValues,
    item.variantCount ?? "",
    item.currentVariantIndex ?? "",
  ].join("::");
}

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
      onRegenerate={callbacks.onRegenerate ? () => callbacks.onRegenerate?.(item.message.turn_index) : undefined}
      isEditing={isEditing}
      onEditSubmit={callbacks.onEditSubmit}
      onEditCancel={callbacks.onEditCancel}
      isProcessing={callbacks.isProcessing}
      isFirstAssistant={isFirstAssistant}
      variantCount={item.variantCount}
      currentVariantIndex={item.currentVariantIndex}
      sessionId={callbacks.sessionId}
      onActivateVariant={callbacks.onActivateVariant}
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
  const [isFollowingLatest, setIsFollowingLatest] = useState(true);
  const [isLatestVisible, setIsLatestVisible] = useState(true);
  const [scrollerElement, setScrollerElement] = useState<HTMLElement | null>(null);
  const [latestAnchorElement, setLatestAnchorElement] = useState<HTMLDivElement | null>(null);
  const virtuosoRef = useRef<VirtuosoHandle>(null);
  const isAtBottomRef = useRef(true);
  const isFollowingLatestRef = useRef(true);
  const isLatestVisibleRef = useRef(true);
  const lastFollowLatestRequestRef = useRef<number | null>(followLatestRequest ?? null);
  const pinFrameRef = useRef<number | null>(null);
  const pendingPinModeRef = useRef<PinSyncMode | null>(null);
  const lastScrollTopRef = useRef(0);
  const programmaticSyncFrameRef = useRef<number | null>(null);
  const pendingProgrammaticSyncFramesRef = useRef(0);
  const pointerInteractionRef = useRef(false);
  const touchInteractionRef = useRef(false);
  const wheelDetachPendingRef = useRef(false);
  const keyboardDetachPendingRef = useRef(false);

  const firstAssistantId = useMemo(() => {
    const first = items.find((item) => item.message.role === "assistant");
    return first?.message.planning_message_id;
  }, [items]);
  const tailVersion = useMemo(() => buildTailVersion(items[items.length - 1]), [items]);

  const setVirtuosoScroller = useCallback((node: HTMLElement | null | Window) => {
    setScrollerElement(node instanceof HTMLElement ? node : null);
  }, []);

  const setLatestAnchor = useCallback((node: HTMLDivElement | null) => {
    setLatestAnchorElement(node);
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

  const clearProgrammaticSync = useCallback(() => {
    pendingProgrammaticSyncFramesRef.current = 0;
    if (programmaticSyncFrameRef.current != null) {
      cancelAnimationFrame(programmaticSyncFrameRef.current);
      programmaticSyncFrameRef.current = null;
    }
  }, []);

  const updateAtBottom = useCallback(
    (next: boolean) => {
      isAtBottomRef.current = next;
      setIsAtBottom((current) => (current === next ? current : next));
      if (next) {
        clearProgrammaticSync();
      }
    },
    [clearProgrammaticSync],
  );

  const updateFollowingLatest = useCallback((next: boolean) => {
    isFollowingLatestRef.current = next;
    setIsFollowingLatest((current) => (current === next ? current : next));
  }, []);

  const updateLatestVisible = useCallback(
    (next: boolean) => {
      isLatestVisibleRef.current = next;
      setIsLatestVisible((current) => (current === next ? current : next));
      if (next) {
        clearProgrammaticSync();
      }
    },
    [clearProgrammaticSync],
  );

  const refreshLatestVisibility = useCallback(() => {
    const next = items.length === 0 || isLatestAnchorVisible(scrollerElement, latestAnchorElement);
    updateLatestVisible(next);
    return next;
  }, [items.length, latestAnchorElement, scrollerElement, updateLatestVisible]);

  const beginProgrammaticSync = useCallback(() => {
    pendingProgrammaticSyncFramesRef.current = PROGRAMMATIC_SYNC_SETTLE_FRAMES;
    if (programmaticSyncFrameRef.current != null) {
      return;
    }

    let scheduleSettleFrame = () => {};

    const settle = () => {
      if (
        pendingProgrammaticSyncFramesRef.current <= 0 ||
        isAtBottomRef.current ||
        isLatestVisibleRef.current
      ) {
        clearProgrammaticSync();
        return;
      }

      pendingProgrammaticSyncFramesRef.current -= 1;
      scheduleSettleFrame();
    };

    scheduleSettleFrame = () => {
      let ranSynchronously = false;
      const frameId = requestAnimationFrame(() => {
        ranSynchronously = true;
        settle();
      });

      if (!ranSynchronously) {
        programmaticSyncFrameRef.current = frameId;
      }
    };

    scheduleSettleFrame();
  }, [clearProgrammaticSync]);

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

  const syncToLatest = useCallback(
    (mode: PinSyncMode) => {
      const handle = virtuosoRef.current;
      if (!handle || items.length === 0) {
        return;
      }

      beginProgrammaticSync();
      if (mode === "force") {
        handle.scrollToIndex({
          index: "LAST",
          align: "end",
          behavior: "auto",
        });
        return;
      }

      handle.autoscrollToBottom();
    },
    [beginProgrammaticSync, items.length],
  );

  const schedulePinnedSync = useCallback(
    (mode: PinSyncMode) => {
      if (items.length === 0) {
        return;
      }

      if (mode === "force") {
        pendingPinModeRef.current = null;
        syncToLatest("force");
        return;
      }

      pendingPinModeRef.current = pendingPinModeRef.current === "force" ? "force" : "maintain";
      if (pinFrameRef.current != null) {
        return;
      }

      let ranSynchronously = false;
      const frameId = requestAnimationFrame(() => {
        ranSynchronously = true;
        pinFrameRef.current = null;
        const nextMode = pendingPinModeRef.current ?? "maintain";
        pendingPinModeRef.current = null;
        if (nextMode !== "force" && !isFollowingLatestRef.current) {
          return;
        }
        syncToLatest(nextMode);
      });

      if (!ranSynchronously) {
        pinFrameRef.current = frameId;
      }
    },
    [items.length, syncToLatest],
  );

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
    updateFollowingLatest(true);
    schedulePinnedSync("force");
  }, [followLatestRequest, schedulePinnedSync, updateFollowingLatest]);

  useEffect(() => {
    if (!isFollowingLatest) {
      return;
    }
    schedulePinnedSync("maintain");
  }, [isFollowingLatest, schedulePinnedSync, tailVersion]);

  useEffect(() => {
    if (items.length === 0) {
      updateLatestVisible(true);
      return;
    }

    if (!scrollerElement || !latestAnchorElement) {
      return;
    }

    if (typeof IntersectionObserver === "undefined") {
      refreshLatestVisibility();
      return;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        const nextVisible = entries[0]?.isIntersecting ?? false;
        updateLatestVisible(nextVisible);
        if (isFollowingLatestRef.current && !nextVisible && !hasDetachIntent()) {
          schedulePinnedSync("maintain");
        }
      },
      {
        root: scrollerElement,
        rootMargin: `0px 0px ${LATEST_VISIBILITY_THRESHOLD_PX}px 0px`,
        threshold: 0,
      },
    );

    observer.observe(latestAnchorElement);
    refreshLatestVisibility();

    return () => {
      observer.disconnect();
    };
  }, [
    hasDetachIntent,
    items.length,
    latestAnchorElement,
    refreshLatestVisibility,
    schedulePinnedSync,
    scrollerElement,
    updateLatestVisible,
  ]);

  useEffect(() => {
    if (typeof ResizeObserver === "undefined" || !scrollerElement) {
      return;
    }

    const observer = new ResizeObserver(() => {
      const nextVisible = refreshLatestVisibility();
      if (isFollowingLatestRef.current && !nextVisible && !hasDetachIntent()) {
        schedulePinnedSync("maintain");
      }
    });

    observer.observe(scrollerElement);
    if (latestAnchorElement) {
      observer.observe(latestAnchorElement);
    }

    return () => {
      observer.disconnect();
    };
  }, [hasDetachIntent, latestAnchorElement, refreshLatestVisibility, schedulePinnedSync, scrollerElement]);

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

      if (typeof IntersectionObserver === "undefined") {
        refreshLatestVisibility();
      }

      if (!isFollowingLatestRef.current) {
        clearOneShotDetachIntent();
        return;
      }

      if (pendingProgrammaticSyncFramesRef.current > 0) {
        clearOneShotDetachIntent();
        return;
      }

      if (currentScrollTop < previousScrollTop - 1 && hasDetachIntent()) {
        updateFollowingLatest(false);
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
  }, [clearOneShotDetachIntent, hasDetachIntent, refreshLatestVisibility, scrollerElement, updateFollowingLatest]);

  useEffect(() => {
    return () => {
      if (pinFrameRef.current != null) {
        cancelAnimationFrame(pinFrameRef.current);
      }
      clearProgrammaticSync();
    };
  }, [clearProgrammaticSync]);

  const showScrollToLatest = items.length > 0 && !isFollowingLatest && (!isAtBottom || !isLatestVisible);

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
        followOutput={() => (isFollowingLatest ? "auto" : false)}
        atBottomThreshold={LATEST_VISIBILITY_THRESHOLD_PX}
        atBottomStateChange={(atBottom) => {
          updateAtBottom(atBottom);
          if (atBottom) {
            updateFollowingLatest(true);
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
          const isTailItem = index === items.length - 1;
          return (
            <div className={`px-4 ${index === 0 ? "pt-1" : "pt-4"} ${shouldAnimate ? "animate-chat-bubble-in" : ""}`}>
              {renderMessage(item, callbacks, item.message.planning_message_id === firstAssistantId)}
              {isTailItem ? <div ref={setLatestAnchor} aria-hidden="true" className="h-px w-full pointer-events-none" /> : null}
            </div>
          );
        }}
      />

      {showScrollToLatest ? (
        <button
          type="button"
          onClick={() => {
            updateFollowingLatest(true);
            schedulePinnedSync("force");
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
