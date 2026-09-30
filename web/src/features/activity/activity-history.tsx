import { useCallback, useMemo, useRef, useState } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { Virtuoso, type ListRange, type VirtuosoHandle } from "react-virtuoso";
import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import type { ActivityGap } from "@/api/activity-history";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { formatExactTime } from "@/lib/date-time";
import { activityToolSummary } from "./activity-tool-summary";
import { ActivityChecklist } from "./activity-checklist";
import type { ActivityContent, ActivityItem } from "./activity-view";
import {
  initialActivityPosition,
  reconcileActivityPosition,
} from "./activity-virtual-state";

export type ActivityHistoryControls = {
  gaps: ActivityGap[];
  isLoadingMore: boolean;
  loadError: Error | null;
  loadMore: (cursor: string) => Promise<unknown>;
};

type HistoryProps = {
  scopeKey: string;
  items: ActivityItem[];
} & ActivityHistoryControls;
type HistoryRow = {
  key: string;
  item?: ActivityItem;
  gap?: ActivityGap;
  sequence: number;
};
type Expansion = {
  row?: boolean;
  input?: boolean;
  output?: boolean;
  metadata?: boolean;
};

export function ActivityHistory(props: HistoryProps) {
  return <VirtualActivityHistory key={props.scopeKey} {...props} />;
}

function VirtualActivityHistory({
  items,
  gaps,
  isLoadingMore,
  loadError,
  loadMore,
}: HistoryProps) {
  const rows = useMemo<HistoryRow[]>(
    () =>
      [
        ...items.map((item) => ({
          key: item.eventId,
          item,
          sequence: item.sequence,
        })),
        ...gaps.map((gap) => ({
          key: gap.cursor,
          gap,
          sequence: gap.through + 0.5,
        })),
      ].sort((left, right) => left.sequence - right.sequence),
    [items, gaps],
  );
  const list = useRef<VirtuosoHandle>(null);
  const [contentHeight, setContentHeight] = useState<number | null>(null);
  const [atBottom, setAtBottom] = useState(true);
  const [expanded, setExpanded] = useState<Record<string, Expansion>>({});
  const [savedPosition, setSavedPosition] = useState(() =>
    initialActivityPosition(rows),
  );
  let position = savedPosition;
  // Commit data and inverse-list coordinates together, before rendering the
  // list. An effect would briefly render the new rows at the old coordinates.
  if (savedPosition.rows !== rows) {
    position = reconcileActivityPosition(savedPosition, rows);
    setSavedPosition(position);
  }
  const rangeChanged = useCallback((range: ListRange) => {
    setSavedPosition((current) => {
      const anchorKey =
        current.rows[range.startIndex - current.firstItemIndex]?.key ?? null;
      return anchorKey === current.anchorKey
        ? current
        : { ...current, anchorKey };
    });
  }, []);
  const setDisclosure = (
    key: string,
    field: keyof Expansion,
    open: boolean,
  ) => {
    setExpanded((current) => ({
      ...current,
      [key]: { ...current[key], [field]: open },
    }));
  };
  return (
    <div
      className="space-y-2"
      role="region"
      aria-label="Provider activity history"
    >
      {loadError ? (
        <p role="alert" className="text-xs text-status-failed">
          Activity could not be loaded. Try again.
        </p>
      ) : null}
      {!atBottom && <div className="flex min-h-9 flex-wrap items-center justify-between gap-3 text-xs text-foreground-muted">
        <div className="ml-auto flex items-start gap-3">
          {!atBottom && (
            <button
              type="button"
              className="min-h-9 font-medium text-accent"
              onClick={() =>
                list.current?.scrollToIndex({
                  index: "LAST",
                  align: "end",
                  behavior: "auto",
                })
              }
            >
              Jump to Latest
            </button>
          )}
        </div>
      </div>}
      {rows.length > 0 ? (
        <Virtuoso
          ref={list}
          style={{
            height: Math.min(520, Math.max(52, contentHeight ?? rows.length * 52)),
            maxHeight: "65vh",
          }}
          totalListHeightChanged={setContentHeight}
          aria-label="Scrollable provider activity"
          data={rows}
          firstItemIndex={position.firstItemIndex}
          computeItemKey={(_, row) => row.key}
          defaultItemHeight={52}
          initialTopMostItemIndex={{ index: "LAST", align: "end" }}
          followOutput={(isAtBottom) => (isAtBottom ? "auto" : false)}
          atBottomStateChange={setAtBottom}
          atBottomThreshold={20}
          rangeChanged={rangeChanged}
          increaseViewportBy={100}
          itemContent={(_, row) => (
            <div className="pb-2">
              {row.gap ? (
                <div className="rounded-lg border border-dashed border-border px-3 py-2 text-xs text-foreground-subtle">
                  <span>
                    {row.gap.count.toLocaleString()}{" "}
                    {row.gap.direction === "earlier"
                      ? "earlier"
                      : "intervening"}{" "}
                    events are not loaded.
                  </span>{" "}
                  <button
                    type="button"
                    disabled={isLoadingMore}
                    className="min-h-9 font-medium text-accent disabled:opacity-50"
                    onClick={() =>
                      void loadMore(row.gap!.cursor).catch(() => {})
                    }
                  >
                    {isLoadingMore ? "Loading…" : "Load Events"}
                  </button>
                </div>
              ) : row.item ? (
                <ActivityHistoryRow
                  item={row.item}
                  expanded={
                    expanded[
                      JSON.stringify([row.item.invocationId, row.item.id])
                    ] ?? {}
                  }
                  setExpanded={(field, open) =>
                    setDisclosure(
                      JSON.stringify([row.item!.invocationId, row.item!.id]),
                      field,
                      open,
                    )
                  }
                />
              ) : null}
            </div>
          )}
        />
      ) : (
        <p className="text-xs text-foreground-muted">
          No provider activity has been recorded.
        </p>
      )}
    </div>
  );
}

function ActivityHistoryRow({
  item,
  expanded,
  setExpanded,
}: {
  item: ActivityItem;
  expanded: Expansion;
  setExpanded: (field: keyof Expansion, open: boolean) => void;
}) {
  const checklistTool = item.kind === "tool" ? item.toolName?.toLowerCase() : undefined;
  const isChecklist = checklistTool === "todowrite" || checklistTool === "todoread";
  const checklistContent = checklistTool === "todoread" ? item.outputContent : item.inputContent;
  const checklistExpanded = checklistTool === "todoread" ? expanded.output : expanded.input;
  return (
    <>
    <details
      data-activity-event={item.eventId}
      open={expanded.row ?? false}
      onToggle={(event) => {
        if (event.target === event.currentTarget)
          setExpanded("row", event.currentTarget.open);
      }}
      className="py-1 text-foreground-muted"
    >
      <summary className="min-h-9 cursor-pointer py-2 text-[13px] marker:text-foreground-subtle">
        <span className="inline-block max-w-[90%] truncate align-bottom">
          {item.kind === "tool"
            ? activityToolSummary(item)
            : item.kind === "thinking"
              ? "Thinking"
              : item.label}
        </span>
        {item.kind === "tool" && item.toolStatus === "running" && (
          <span aria-label="Tool running" className="ml-2 text-xs text-accent">
            Running
          </span>
        )}
        {item.toolStatus &&
          ["failed", "error", "cancelled"].includes(item.toolStatus) && (
            <span className="ml-2 text-xs text-status-failed">
              {
                { failed: "Failed", error: "Error", cancelled: "Cancelled" }[
                  item.toolStatus
                ]
              }
            </span>
          )}
        {item.kind === "thinking" && !expanded.row && (
          <span className="mt-2 block line-clamp-3 whitespace-pre-wrap border-l-2 border-border pl-4 text-sm leading-6 text-foreground-muted">
            {item.reasoningContent?.preview ?? item.detail}
          </span>
        )}
      </summary>
      {expanded.row && (
        <div className="space-y-3 py-3 pl-4">
          {item.kind === "message" && (
            <ActivityContentReader
              label="Observed Message"
              presentation="message"
              content={item.messageContent ?? { availability: "not_provided" }}
            />
          )}
          {item.kind === "tool" ? (
            <>
              <ActivityContentReader
                label="Input"
                open={expanded.input ?? false}
                onOpenChange={(open) => setExpanded("input", open)}
                content={item.inputContent ?? { availability: "not_provided" }}
                checklist={checklistTool === "todowrite"}
              />
              <ActivityContentReader
                label="Output"
                open={expanded.output ?? false}
                onOpenChange={(open) => setExpanded("output", open)}
                content={item.outputContent ?? { availability: "not_provided" }}
                checklist={checklistTool === "todoread"}
              />
            </>
          ) : null}
          {item.kind === "thinking" ? (
            <ActivityContentReader
              label="Provider Reasoning"
              presentation="reasoning"
              content={
                item.reasoningContent ?? { availability: "not_provided" }
              }
            />
          ) : null}
          {item.kind === "progress" && item.detail ? (
            <p className="whitespace-pre-wrap text-xs text-foreground-subtle">
              {item.detail}
            </p>
          ) : null}
          <details
            open={expanded.metadata ?? false}
            onToggle={(event) => {
              if (event.target === event.currentTarget)
                setExpanded("metadata", event.currentTarget.open);
            }}
          >
            <summary className="w-fit cursor-pointer py-1 text-[11px] text-foreground-subtle">
              Event Details
            </summary>
            {expanded.metadata && <ActivityMetadata item={item} />}
          </details>
        </div>
      )}
    </details>
    {isChecklist && (!expanded.row || !checklistExpanded) && checklistContent && <ActivityChecklist content={checklistContent} />}
    </>
  );
}

function ActivityMetadata({ item }: { item: ActivityItem }) {
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-[10px] text-foreground-muted">
      {item.kind === "message" && (
        <>
          <dt>Origin</dt>
          <dd>Provider Observation</dd>
          {item.messageContent?.byte_length !== undefined && (
            <>
              <dt>Content</dt>
              <dd>{item.messageContent.byte_length.toLocaleString()} Bytes</dd>
            </>
          )}
        </>
      )}
      <dt>Activity</dt>
      <dd>{item.label}</dd>
      {item.toolName && (
        <>
          <dt>Tool</dt>
          <dd>{item.toolName}</dd>
        </>
      )}
      {item.toolTitle && (
        <>
          <dt>Title</dt>
          <dd className="whitespace-pre-wrap break-words">{item.toolTitle}</dd>
        </>
      )}
      {item.toolStatus && (
        <>
          <dt>Status</dt>
          <dd>{item.toolStatus}</dd>
        </>
      )}
      {item.occurredAt && (
        <>
          <dt>Occurred</dt>
          <dd>
            <time dateTime={item.occurredAt}>
              {formatExactTime(item.occurredAt)}
            </time>
          </dd>
        </>
      )}
      {item.recordedAt && (
        <>
          <dt>Recorded</dt>
          <dd>
            <time dateTime={item.recordedAt}>
              {formatExactTime(item.recordedAt)}
            </time>
          </dd>
        </>
      )}
      {item.toolCallId && (
        <>
          <dt>Tool Call ID</dt>
          <dd className="break-all font-mono">{item.toolCallId}</dd>
        </>
      )}
      {item.providerPartId && (
        <>
          <dt>Provider Part ID</dt>
          <dd className="break-all font-mono">{item.providerPartId}</dd>
        </>
      )}
      {item.invocationId && (
        <>
          <dt>Invocation ID</dt>
          <dd className="break-all font-mono">{item.invocationId}</dd>
        </>
      )}
      <dt>Event ID</dt>
      <dd className="break-all font-mono">{item.eventId}</dd>
    </dl>
  );
}

type ContentPage = components["schemas"]["ActivityContentPage"];

export function ActivityContentReader({
  label,
  content,
  open: controlledOpen,
  onOpenChange,
  presentation = "disclosure",
  checklist = false,
}: {
  checklist?: boolean;
  presentation?: "disclosure" | "message" | "reasoning";
  label: string;
  content: ActivityContent;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const [localOpen, setLocalOpen] = useState(false);
  const open = presentation !== "disclosure" || (controlledOpen ?? localOpen);
  const pages = useInfiniteQuery({
    queryKey: ["activity-content", content.content_url, content.sha256],
    queryFn: ({ pageParam, signal }) =>
      apiFetch<ContentPage>(content.content_url!, {
        params: { cursor: pageParam, limit: 16_384 },
        signal,
      }),
    enabled:
      open &&
      content.availability === "available" &&
      Boolean(content.content_url) &&
      (content.byte_length ?? 0) > 0,
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
  });
  const text =
    pages.data?.pages.map((page) => page.text).join("") ??
    content.preview ??
    "";
  const lastPage = pages.data?.pages.at(-1);
  const loadedBytes = lastPage
    ? lastPage.offset + new TextEncoder().encode(lastPage.text).length
    : new TextEncoder().encode(content.preview ?? "").length;
  const body = (
    <div
      className={
        presentation !== "disclosure"
          ? "space-y-2"
          : "space-y-2 border-t border-border/70 p-2.5"
      }
    >
      {content.availability === "not_provided" ? (
        <p className="text-xs text-foreground-muted">
          Not supplied by the provider.
        </p>
      ) : content.availability === "unavailable" ? (
        <p className="text-xs text-foreground-muted">
          The provider did not expose this content.
        </p>
      ) : content.byte_length === 0 ? (
        <p className="text-xs text-foreground-muted">
          The provider supplied empty content.
        </p>
      ) : (
        <>
          {checklist && <ActivityChecklist content={{ ...content, preview: text }} />}
          {presentation !== "disclosure" ? (
            <div>
              <MarkdownBlock
                content={text}
                className={
                  presentation === "reasoning"
                    ? "border-l-2 border-border pl-4 text-sm leading-6 text-foreground-muted"
                    : "text-[15px] leading-7 text-foreground"
                }
              />
            </div>
          ) : (
            <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-words font-mono text-xs leading-relaxed text-foreground">
              {text}
            </pre>
          )}
          {(presentation === "disclosure" ||
            loadedBytes < (content.byte_length ?? 0)) && (
            <p className="text-[10px] text-foreground-muted">
              {loadedBytes.toLocaleString()} of{" "}
              {content.byte_length?.toLocaleString()} bytes loaded.
            </p>
          )}
          {pages.isError ? (
            <div role="alert" className="text-xs text-status-failed">
              Content could not be loaded.{" "}
              <button
                type="button"
                className="min-h-9 underline"
                onClick={() => void pages.refetch()}
              >
                Retry
              </button>
            </div>
          ) : null}
          {pages.isFetching ? (
            <p className="text-xs text-foreground-muted">Loading content…</p>
          ) : null}
          {pages.hasNextPage ? (
            <button
              type="button"
              disabled={pages.isFetchingNextPage}
              className="min-h-9 text-xs font-medium text-accent"
              onClick={() => void pages.fetchNextPage()}
            >
              Load More Content
            </button>
          ) : null}
        </>
      )}
    </div>
  );
  if (presentation !== "disclosure") return body;
  return (
    <details
      open={open}
      onToggle={(event) => {
        if (event.target === event.currentTarget) {
          setLocalOpen(event.currentTarget.open);
          onOpenChange?.(event.currentTarget.open);
        }
      }}
      className="rounded border border-border/70"
    >
      <summary className="min-h-10 cursor-pointer px-2.5 py-2 text-xs font-medium text-foreground-subtle">
        {label}
        {content.availability === "available" ? (
          <span className="ml-2 font-normal text-foreground-muted">
            {content.byte_length?.toLocaleString()} Bytes
          </span>
        ) : null}
      </summary>
      {body}
    </details>
  );
}
