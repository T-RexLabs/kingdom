import { useCallback, useMemo, useRef, useState, type ReactNode } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { Virtuoso, type ListRange, type VirtuosoHandle } from "react-virtuoso";
import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import type { ActivityGap } from "@/api/activity-history";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { RelativeTime } from "@/components/ui/relative-time";
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
  messageDisplay?: "conversation" | "inspection";
  headerActions?: ReactNode;
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
  reasoning?: boolean;
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
  messageDisplay = "conversation",
  headerActions,
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
      <div className="flex min-h-9 flex-wrap items-center justify-between gap-3 text-xs text-foreground-muted">
        <span>
          {rows.length > 0 &&
            (atBottom ? "Following Latest" : "Reading Earlier Activity")}
        </span>
        {gaps.length > 0 && (
          <span>
            {gaps.reduce((count, gap) => count + gap.count, 0).toLocaleString()}{" "}
            Events Not Loaded
          </span>
        )}
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
          {headerActions}
        </div>
      </div>
      {rows.length > 0 ? (
        <Virtuoso
          ref={list}
          style={{ height: 520, maxHeight: "65vh" }}
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
                  messageDisplay={messageDisplay}
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
      ) : !headerActions ? (
        <p className="text-xs text-foreground-muted">
          No provider activity has been recorded.
        </p>
      ) : null}
    </div>
  );
}

function ActivityHistoryRow({
  item,
  expanded,
  setExpanded,
  messageDisplay,
}: {
  item: ActivityItem;
  messageDisplay: "conversation" | "inspection";
  expanded: Expansion;
  setExpanded: (field: keyof Expansion, open: boolean) => void;
}) {
  if (item.kind === "message" && messageDisplay === "conversation") {
    return (
      <article
        data-activity-event={item.eventId}
        className="rounded-xl border border-border bg-surface/60 px-4 py-3"
      >
        <ActivityContentReader
          label="Message"
          presentation="message"
          content={item.messageContent ?? { availability: "not_provided" }}
        />
        <div className="mt-2 flex flex-wrap items-start gap-x-4 text-xs text-foreground-muted">
          {item.occurredAt && (
            <span className="py-2">
              <RelativeTime iso={item.occurredAt} />
            </span>
          )}
          <details
            open={expanded.row ?? false}
            onToggle={(event) => {
              if (event.target === event.currentTarget)
                setExpanded("row", event.currentTarget.open);
            }}
            className="min-w-0 flex-1"
          >
            <summary className="min-h-9 cursor-pointer py-2">
              Message Details
            </summary>
            <ActivityMetadata item={item} />
          </details>
        </div>
      </article>
    );
  }
  return (
    <details
      data-activity-event={item.eventId}
      open={expanded.row ?? false}
      onToggle={(event) => {
        if (event.target === event.currentTarget)
          setExpanded("row", event.currentTarget.open);
      }}
      className="rounded-lg border border-border bg-surface/50"
    >
      <summary className="min-h-11 cursor-pointer px-3 py-2 text-xs marker:text-foreground-muted">
        <span className="font-medium text-foreground">{item.label}</span>
        {item.toolStatus ? (
          <span className="ml-2 rounded bg-background px-1.5 py-0.5 text-[10px] capitalize text-foreground-muted">
            {item.toolStatus.replaceAll("_", " ")}
          </span>
        ) : null}
        {item.occurredAt ? (
          <span className="ml-2 text-foreground-muted">
            <RelativeTime iso={item.occurredAt} />
          </span>
        ) : null}
      </summary>
      {expanded.row && (
        <div className="space-y-2 border-t border-border px-3 py-3">
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
              />
              <ActivityContentReader
                label="Output"
                open={expanded.output ?? false}
                onOpenChange={(open) => setExpanded("output", open)}
                content={item.outputContent ?? { availability: "not_provided" }}
              />
            </>
          ) : null}
          {item.kind === "thinking" ? (
            <ActivityContentReader
              label="Provider Reasoning"
              open={expanded.reasoning ?? false}
              onOpenChange={(open) => setExpanded("reasoning", open)}
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
          <ActivityMetadata item={item} />
        </div>
      )}
    </details>
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
}: {
  presentation?: "disclosure" | "message";
  label: string;
  content: ActivityContent;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const [localOpen, setLocalOpen] = useState(false);
  const open = presentation === "message" || (controlledOpen ?? localOpen);
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
        presentation === "message"
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
          {presentation === "message" ? (
            <div className="max-h-[32rem] overflow-auto">
              <MarkdownBlock
                content={text}
                className="text-sm leading-relaxed text-foreground"
              />
            </div>
          ) : (
            <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-words font-mono text-xs leading-relaxed text-foreground">
              {text}
            </pre>
          )}
          {(presentation !== "message" ||
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
  if (presentation === "message") return body;
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
