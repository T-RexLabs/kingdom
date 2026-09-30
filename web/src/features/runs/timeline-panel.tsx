import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowDown, ArrowUp } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { RunExecution, RunGraphEdge, RunGraphNode, TimelineEvent } from "@/api/hooks";
import { buildTimelineLookups, synthesizeMilestones } from "./timeline-milestones";
import { MilestoneRow } from "./timeline-milestone-row";
import { RawEventRow } from "./timeline-raw-event-row";
import { orderMilestonesForDisplay } from "./timeline-ordering";

export function TimelinePanel({
  events,
  graph,
  executions = [],
  hasMore,
  onLoadMore,
  isLoadingMore = false,
}: {
  events: TimelineEvent[];
  graph?: { nodes: RunGraphNode[]; edges: RunGraphEdge[] };
  executions?: RunExecution[];
  hasMore: boolean;
  onLoadMore?: () => void;
  isLoadingMore?: boolean;
}) {
	const [expandedId, setExpandedId] = useState<string | null>(null);
	const [showRawEvents, setShowRawEvents] = useState(false);
	const [userScrolledAwayFromLatest, setUserScrolledAwayFromLatest] = useState(false);
	const scrollRef = useRef<HTMLDivElement>(null);
	const prevRangeRef = useRef({ count: 0, firstSeq: 0, lastSeq: 0 });
	const inspectionLocked = userScrolledAwayFromLatest || expandedId !== null;
	const currentRange = {
		count: events.length,
		firstSeq: events[0]?.run_event_seq ?? 0,
		lastSeq: events[events.length - 1]?.run_event_seq ?? 0,
	};
	const [timelineUpdateState, setTimelineUpdateState] = useState({
		...currentRange,
		hasHiddenLatestUpdate: false,
	});
	if (!sameTimelineRange(timelineUpdateState, currentRange)) {
		const initialLoad = timelineUpdateState.count === 0 && currentRange.count > 0;
		const receivedNewer = timelineUpdateState.count > 0
			&& currentRange.lastSeq > timelineUpdateState.lastSeq;
		setTimelineUpdateState({
			...currentRange,
			hasHiddenLatestUpdate: receivedNewer && inspectionLocked
				? true
				: (initialLoad || receivedNewer) && !inspectionLocked
					? false
					: timelineUpdateState.hasHiddenLatestUpdate,
		});
	}
	const hasHiddenLatestUpdate = timelineUpdateState.hasHiddenLatestUpdate;

  const lookups = useMemo(
		() => buildTimelineLookups(events, graph?.nodes ?? [], executions),
		[events, executions, graph?.nodes],
  );

	const milestones = useMemo(
		() => synthesizeMilestones(events, graph?.nodes ?? [], graph?.edges ?? [], lookups),
		[events, graph?.edges, graph?.nodes, lookups],
	);

	const orderedMilestones = useMemo(
		() => orderMilestonesForDisplay(milestones),
		[milestones],
	);
	const orderedRawEvents = useMemo(
		() => [...events].sort(compareTimelineEventDisplayTime).reverse(),
		[events],
	);

  const toggleExpand = useCallback((id: string) => {
    setExpandedId((previous) => (previous === id ? null : id));
  }, []);

	const handleScroll = useCallback(() => {
		const element = scrollRef.current;
		if (!element) {
			return;
		}
		setUserScrolledAwayFromLatest(element.scrollTop > 80);
	}, []);

	useEffect(() => {
		const firstSeq = events[0]?.run_event_seq ?? 0;
		const lastSeq = events[events.length - 1]?.run_event_seq ?? 0;
		const previous = prevRangeRef.current;
		const initialLoad = previous.count === 0 && events.length > 0;
		const receivedNewer = previous.count > 0
			&& lastSeq > previous.lastSeq;
		if ((initialLoad || receivedNewer) && !inspectionLocked) {
			scrollContainerToTop(scrollRef.current, "auto");
		}
		prevRangeRef.current = { count: events.length, firstSeq, lastSeq };
	}, [events, inspectionLocked]);

	const scrollToLatest = () => {
		scrollContainerToTop(scrollRef.current, "smooth");
		setUserScrolledAwayFromLatest(false);
		setTimelineUpdateState((current) => ({ ...current, hasHiddenLatestUpdate: false }));
		setExpandedId(null);
	};

  if (events.length === 0) {
    return (
      <div className="flex h-64 items-center justify-center text-sm text-foreground-subtle">
        No events recorded yet.
      </div>
    );
  }

  return (
    <div className="relative flex h-full min-h-0 flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-3">
        <div className="space-y-1.5">
          <h3 className="text-sm font-medium text-foreground">Audit Trail</h3>
          <p className="text-xs leading-relaxed text-foreground-subtle">
            {showRawEvents
              ? `${events.length} loaded ${events.length === 1 ? "event" : "events"}`
              : `${orderedMilestones.length} ${orderedMilestones.length === 1 ? "milestone" : "milestones"} · ${events.length} loaded ${events.length === 1 ? "event" : "events"}`}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-1" role="group" aria-label="Audit view">
          {([false, true] as const).map((raw) => (
            <Button
              key={String(raw)}
              type="button"
              size="sm"
              variant={showRawEvents === raw ? "secondary" : "ghost"}
              aria-pressed={showRawEvents === raw}
              onClick={() => { setShowRawEvents(raw); setExpandedId(null); }}
            >
              {raw ? "Raw Events" : "Activity"}
            </Button>
          ))}
        </div>
      </div>
      <div className="flex items-center gap-1.5 text-xs text-foreground-subtle">
        <ArrowDown aria-hidden="true" className="h-3.5 w-3.5" />
        Newest First
      </div>
      <div ref={scrollRef} onScroll={handleScroll} className="relative min-h-0 flex-1 overflow-y-auto pr-1">
        <ol aria-label={showRawEvents ? "Raw events" : "Run milestones"} className="min-w-0 border-y border-border">
          {showRawEvents
            ? orderedRawEvents.map((event) => (
              <RawEventRow key={event.event_id} event={event} isExpanded={expandedId === event.event_id} onToggle={() => toggleExpand(event.event_id)} lookups={lookups} />
            ))
            : orderedMilestones.map((milestone) => (
              <MilestoneRow key={milestone.id} milestone={milestone} isExpanded={expandedId === milestone.id} onToggle={() => toggleExpand(milestone.id)} lookups={lookups} />
            ))}
        </ol>
        {hasMore && onLoadMore && (
          <div className="flex justify-center py-4">
            <Button type="button" variant="outline" size="sm" onClick={onLoadMore} disabled={isLoadingMore}>
              {isLoadingMore ? "Loading older activity..." : "Load Older Activity"}
            </Button>
          </div>
        )}
      </div>
      {(userScrolledAwayFromLatest || hasHiddenLatestUpdate) && (
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={scrollToLatest}
          className="absolute right-4 z-20 bg-surface-overlay shadow-lg sm:right-6"
          style={{ bottom: "max(1rem, env(safe-area-inset-bottom))" }}
        >
          <ArrowUp aria-hidden="true" className="h-3.5 w-3.5" />
          Jump to Latest
        </Button>
      )}
    </div>
  );
}

function compareTimelineEventDisplayTime(left: TimelineEvent, right: TimelineEvent): number {
	if (left.run_event_seq !== right.run_event_seq) {
		return left.run_event_seq - right.run_event_seq;
	}
	if (left.attempt_seq !== right.attempt_seq) {
		return left.attempt_seq - right.attempt_seq;
	}
	if (left.emitted_at > right.emitted_at) return 1;
	if (left.emitted_at < right.emitted_at) return -1;
	return left.event_id.localeCompare(right.event_id);
}

function sameTimelineRange(
	left: { count: number; firstSeq: number; lastSeq: number },
	right: { count: number; firstSeq: number; lastSeq: number },
): boolean {
	return left.count === right.count
		&& left.firstSeq === right.firstSeq
		&& left.lastSeq === right.lastSeq;
}

function scrollContainerToTop(element: HTMLDivElement | null, behavior: ScrollBehavior): void {
	if (!element) {
		return;
	}
	if (typeof element.scrollTo === "function") {
		element.scrollTo({ top: 0, behavior });
		return;
	}
	element.scrollTop = 0;
}
