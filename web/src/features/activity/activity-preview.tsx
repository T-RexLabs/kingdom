import { useLayoutEffect, useMemo, useRef, useState } from "react";
import { Wrench } from "lucide-react";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { useTextReveal } from "@/lib/use-text-reveal";
import { ActivityChecklist } from "./activity-checklist";
import { activityToolSummary } from "./activity-tool-summary";
import type { ActivityItem } from "./activity-view";

const REASONING_PREVIEW_CHARACTERS = 2_400;

/** A compact reading surface; complete observations remain in exact history. */
export function ActivityPreview({
  items,
  isLive,
  hasGaps,
  onReadMore,
}: {
  items: ActivityItem[];
  isLive: boolean;
  hasGaps: boolean;
  onReadMore: () => void;
}) {
  const preview = useMemo(() => selectPreview(items), [items]);
  const reveal = useTextReveal(preview.reasoning, isLive);
  // Settled previews are immediately readable and make no live-state claims.
  const reasoning = isLive ? reveal.displayText : preview.reasoning;
  const checklist = preview.checklist;
  const checklistContent =
    checklist?.toolName?.toLowerCase() === "todoread"
      ? checklist.outputContent
      : checklist?.inputContent;
  if (!reasoning && !preview.action && !checklist && !isLive) return null;

  return (
    <div
      role="region"
      aria-label="Provider activity preview"
      className="space-y-2 text-left"
    >
      {reasoning && <ReasoningWindow content={reasoning} />}
      {preview.reasoning && (preview.incomplete || hasGaps) && (
        <p className="text-xs text-foreground-muted">
          Reasoning preview.{" "}
          <button
            type="button"
            onClick={onReadMore}
            className="min-h-9 font-medium text-accent"
          >
            Read More
          </button>
        </p>
      )}
      {preview.tools.length > 0 && (
        <ul aria-label="Recent tool calls" className="space-y-1">
          {preview.tools.map((item) => (
            <li
              key={JSON.stringify([item.invocationId, item.id])}
              className="flex min-w-0 items-start gap-2 py-1 text-xs text-foreground-muted"
            >
              <Wrench aria-hidden="true" className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              <p
                role={isLive && preview.actionItem === item ? "status" : undefined}
                title={activityToolSummary(item)}
                className="min-w-0 flex-1 truncate"
              >
                {activityToolSummary(item)}
              </p>
              {item.toolStatus && (
                <span className={`shrink-0 text-[11px] ${["failed", "error"].includes(item.toolStatus) ? "text-status-failed" : "text-foreground-subtle"}`}>
                  {toolStateLabel(item.toolStatus, isLive)}
                </span>
              )}
            </li>
          ))}
        </ul>
      )}
      {preview.action && !(preview.actionItem && preview.tools.includes(preview.actionItem)) ? (
        <p
          role={isLive ? "status" : undefined}
          className="truncate text-xs font-medium text-foreground-muted"
        >
          {!isLive && <span className="font-normal">Last Activity: </span>}
          {preview.action}
        </p>
      ) : isLive && !preview.action ? (
        <p role="status" className="text-xs text-foreground-muted">
          Waiting for provider activity.
        </p>
      ) : null}
      {checklistContent && (
        <ActivityChecklist content={checklistContent} collapsible />
      )}
    </div>
  );
}

function ReasoningWindow({ content }: { content: string }) {
  const viewport = useRef<HTMLDivElement>(null);
  const body = useRef<HTMLDivElement>(null);
  const [overflowing, setOverflowing] = useState(false);
  useLayoutEffect(() => {
    const frame = viewport.current;
    const text = body.current;
    if (!frame || !text) return;
    const measure = () =>
      setOverflowing(text.scrollHeight > frame.clientHeight + 1);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(frame);
    observer.observe(text);
    return () => observer.disconnect();
  }, [content]);

  return (
    <div
      ref={viewport}
      role="group"
      aria-label="Reasoning preview"
      className="flex max-h-[120px] flex-col justify-end overflow-hidden"
      style={{
        maskImage: overflowing
          ? "linear-gradient(to bottom, transparent, black 24px)"
          : undefined,
      }}
    >
      <div ref={body} data-reasoning-content className="shrink-0">
        <MarkdownBlock
          content={content}
          interactiveLinks={false}
          className="text-xs leading-relaxed text-foreground-muted [&_h1]:text-xs [&_h2]:text-xs [&_h3]:text-xs [&_h4]:text-xs [&_h5]:text-xs [&_h6]:text-xs [&_pre]:overflow-hidden [&_pre]:whitespace-pre-wrap"
        />
      </div>
    </div>
  );
}

function selectPreview(items: ActivityItem[]) {
  const reasoning: string[] = [];
  let remaining = REASONING_PREVIEW_CHARACTERS;
  let incomplete = false;
  let action: string | undefined;
  let progress: string | undefined;
  let actionItem: ActivityItem | undefined;
  const tools: ActivityItem[] = [];
  let checklist: ActivityItem | undefined;
  for (let index = items.length - 1; index >= 0; index -= 1) {
    const item = items[index];
    if (!progress && item.kind === "progress") progress = item.label;
    if (!action && (item.kind === "tool" || item.kind === "thinking")) {
      action = item.kind === "tool" ? activityToolSummary(item) : item.label;
      actionItem = item;
    }
    if (item.kind === "tool" && tools.length < 3 &&
      !["todowrite", "todoread"].includes(item.toolName?.toLowerCase() ?? "")) {
      tools.unshift(item);
    }
    if (
      !checklist &&
      item.kind === "tool" &&
      ["todowrite", "todoread"].includes(item.toolName?.toLowerCase() ?? "")
    ) {
      // Select before parsing: a partial new snapshot must not resurrect an older checklist.
      checklist = item;
    }
    if (item.kind !== "thinking") continue;
    const content = item.reasoningContent;
    const text =
      content?.availability === "available"
        ? (content.preview ?? "")
        : content?.availability === "unavailable"
          ? "The provider did not expose this reasoning."
          : "Reasoning was not supplied by the provider.";
    if (!text) continue;
    if (
      content?.availability === "available" &&
      new TextEncoder().encode(text).length < (content.byte_length ?? 0)
    )
      incomplete = true;
    if (remaining <= 0) {
      incomplete = true;
      continue;
    }
    let excerpt = text.slice(-remaining);
    // Keep the display boundary on a complete Unicode code point.
    if (excerpt.length < text.length && /^[\uDC00-\uDFFF]/.test(excerpt))
      excerpt = excerpt.slice(1);
    if (excerpt.length < text.length) incomplete = true;
    reasoning.unshift(excerpt);
    remaining -= excerpt.length + 2;
  }
  return {
    reasoning: reasoning.join("\n\n"),
    incomplete,
    action: action ?? progress,
    actionItem,
    tools,
    checklist,
  };
}

function toolStateLabel(status: string, isLive: boolean): string {
  const label = ({ pending: "Pending", running: "Running", completed: "Done", failed: "Failed", error: "Error", cancelled: "Cancelled" } as Record<string, string>)[status] ?? status;
  return !isLive && ["pending", "running"].includes(status) ? `Last Reported: ${label}` : label;
}
