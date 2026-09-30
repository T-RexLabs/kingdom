import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowUp, Eye, EyeOff } from "lucide-react";
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
    <div className="relative flex h-full min-h-0 flex-col gap-3">
		<div className="flex items-center justify-between">
			<span className="text-xs text-foreground-subtle">
				{showRawEvents
					? `${events.length} raw events`
					: `${orderedMilestones.length} milestones from ${events.length} events`}
			</span>
        <button
          onClick={() => {
            setShowRawEvents(!showRawEvents);
            setExpandedId(null);
          }}
          className="inline-flex items-center gap-1.5 rounded-md border border-border px-2 py-1 text-[11px] font-medium text-foreground-subtle transition-colors hover:border-border-strong hover:text-foreground"
        >
          {showRawEvents ? (
            <>
              <EyeOff className="h-3 w-3" />
              Milestones
            </>
          ) : (
            <>
              <Eye className="h-3 w-3" />
              Raw Events
            </>
          )}
        </button>
      </div>

			<div
				ref={scrollRef}
				onScroll={handleScroll}
        className="relative min-h-0 flex-1 overflow-y-auto pr-2"
      >
				<div className="pl-1">
					{showRawEvents ? (
						orderedRawEvents.map((event, index) => (
							<RawEventRow
								key={event.event_id}
								event={event}
								isLast={index === orderedRawEvents.length - 1}
								isExpanded={expandedId === event.event_id}
								onToggle={() => toggleExpand(event.event_id)}
								lookups={lookups}
							/>
						))
					) : (
						orderedMilestones.map((milestone, index) => (
							<MilestoneRow
								key={milestone.id}
								milestone={milestone}
								isLast={index === orderedMilestones.length - 1}
								isExpanded={expandedId === milestone.id}
								onToggle={() => toggleExpand(milestone.id)}
								lookups={lookups}
							/>
						))
					)}

					{hasMore && onLoadMore && (
						<div className="flex justify-center pt-3">
							<button
								type="button"
								onClick={onLoadMore}
								disabled={isLoadingMore}
								className="rounded-md border border-border bg-surface px-4 py-1.5 text-xs font-medium text-foreground-muted transition-colors hover:border-border-strong hover:bg-surface-raised hover:text-foreground disabled:cursor-not-allowed disabled:opacity-60"
							>
								{isLoadingMore ? "Loading older activity..." : "Load older activity"}
							</button>
						</div>
					)}
				</div>
			</div>

			{(userScrolledAwayFromLatest || hasHiddenLatestUpdate) && (
				<button
					onClick={scrollToLatest}
					className="absolute right-4 z-20 flex items-center gap-1.5 rounded-full border border-border-strong bg-surface-overlay px-3 py-1.5 text-xs font-medium text-foreground shadow-lg transition-colors hover:bg-surface-raised hover:text-gold sm:right-6"
					style={{ bottom: "max(1rem, env(safe-area-inset-bottom))" }}
				>
					<ArrowUp className="h-3.5 w-3.5" />
					Jump to latest
				</button>
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
