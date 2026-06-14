import type { PlanningMessage } from "@/api/hooks";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { formatTimestamp } from "@/lib/utils";
import { useState } from "react";
import { ChevronDown, ExternalLink, Feather, RefreshCw, ScrollText, User } from "lucide-react";
import { PlannerEditAction } from "./planner-message-actions";
import { usePlannerMessageActivity } from "./planner-message-activity";
import type { PlannerActivityItem } from "./planner-operation-controller";
import { PlannerMessageEditor } from "./planner-message-editor";
import { PlannerVariantNavigator } from "./planner-variant-navigator";
import { useTextReveal } from "./use-text-reveal";

export function PlannerMessageCard({
  message,
  children,
  isActive,
  isStreaming,
  contentMarkdown,
  onEdit,
  onRegenerate,
  isEditing,
  onEditSubmit,
  onEditCancel,
  isProcessing,
  isFirstAssistant,
  variantCount,
  currentVariantIndex,
  sessionId,
  onActivateVariant,
  activityRunId,
  liveActivityItems,
}: {
  message: PlanningMessage;
  children?: React.ReactNode;
  isActive?: boolean;
  isStreaming?: boolean;
  contentMarkdown?: string;
  onEdit?: () => void;
  onRegenerate?: () => void;
  isEditing?: boolean;
  onEditSubmit?: (text: string) => void;
  onEditCancel?: () => void;
  isProcessing?: boolean;
  isFirstAssistant?: boolean;
  variantCount?: number;
  currentVariantIndex?: number;
  sessionId?: string;
  onActivateVariant?: (messageId: string) => void;
  activityRunId?: string;
  liveActivityItems?: PlannerActivityItem[];
}) {
  const isAssistant = message.role === "assistant";
  const renderedContent = contentMarkdown ?? message.content_markdown;
  const isAssistantStreaming = isAssistant && (isStreaming ?? false);
  // useTextReveal does no-op work for non-assistant messages, but its effect
  // and ref machinery still cost cycles on every render. Pass an empty string
  // when we know the hook will never animate so the no-op path stays cheap;
  // user messages then render their content_markdown verbatim.
  const reveal = useTextReveal(isAssistant ? renderedContent : "", isAssistantStreaming);
  const displayText = isAssistant ? reveal.displayText : renderedContent;
  const isStreamActive = isAssistant ? reveal.isStreamActive : false;
  const messageActivity = usePlannerMessageActivity({
    activityRunId,
    isAssistant,
    isStreaming: isAssistantStreaming,
    liveActivityItems,
  });
  // Drive the reveal off `isAssistantStreaming` directly (not gated on thought
  // text existing). That captures the empty baseline the moment streaming
  // starts, so the FIRST reasoning block types in from zero like later ones.
  // Gating on text length used to capture the baseline only once the first
  // block had already arrived — making it snap in fully. Baseline mode still
  // shows replayed/reconnected backlog instantly instead of re-typing it.
  const liveThoughtReveal = useTextReveal(
    messageActivity.liveThoughtText,
    isAssistantStreaming,
  );
  const [thoughtsOpen, setThoughtsOpen] = useState(false);
  const [sourcesOpen, setSourcesOpen] = useState(false);

  const hasVariants = isAssistant && variantCount != null && variantCount > 1 && currentVariantIndex != null && sessionId && onActivateVariant;
  const showRegenerate = isAssistant && !isFirstAssistant && !(isProcessing ?? false) && onRegenerate;
  const showStreamingActivity = isAssistantStreaming && Boolean(messageActivity.currentEvent || liveThoughtReveal.displayText.trim());
  const showThoughts = isAssistant && !isAssistantStreaming && messageActivity.thoughts.length > 0;
  const showSources = isAssistant && !isAssistantStreaming && messageActivity.sources.length > 0;
  // A short handful of sources reads cleanly inline; collapse 5+ behind a
  // disclosure so a long research burst doesn't dominate the bubble.
  const collapseSources = messageActivity.sources.length >= 5;
  const sourceChips = messageActivity.sources.map((source) => (
    <a
      key={source.id}
      href={source.url}
      target="_blank"
      rel="noreferrer"
      title={source.url}
      className="inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-border/40 bg-background/10 px-2.5 py-1 text-[11px] text-foreground-subtle/85 transition-colors hover:border-gold/40 hover:text-foreground"
    >
      <ExternalLink className="h-3 w-3 shrink-0 opacity-70" />
      <span className="truncate">{sourceHostLabel(source.url)}</span>
    </a>
  ));

  return (
    <div className={`group flex min-w-0 ${isAssistant ? "justify-start" : "justify-end"}`}>
      <div className={`flex max-w-[85%] gap-2.5 ${!isAssistant && isEditing ? "w-full" : ""}`}>
        {isAssistant ? (
          <div className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-gold/15 text-gold">
            <ScrollText className="h-3.5 w-3.5" />
          </div>
        ) : null}
        <div className={`min-w-0 space-y-1.5 ${isAssistant ? "" : "text-right"} ${!isAssistant && isEditing ? "flex-1" : ""}`}>
          <div
            className={`overflow-hidden rounded-2xl px-4 py-3 text-sm text-left space-y-3 ${
              isAssistant
                ? `w-fit rounded-tl-md border text-foreground ${
                    isActive
                      ? "border-gold/40 bg-surface-raised/55 shadow-[0_0_0_1px_rgba(212,175,55,0.08)]"
                      : "border-border/50 bg-surface-raised/40"
                  }`
                : `${isEditing ? "w-full" : "w-fit ml-auto"} rounded-tr-md bg-gold/10 border border-gold/20 text-foreground`
            }`}
          >
            {isEditing && onEditSubmit && onEditCancel ? (
              <PlannerMessageEditor
                initialText={message.content_markdown}
                onSubmit={onEditSubmit}
                onCancel={onEditCancel}
              />
            ) : (
              <>
                {showStreamingActivity ? (
                  <div className="space-y-2 text-left">
                    {liveThoughtReveal.displayText.trim() ? (
                      <p className="whitespace-pre-wrap text-xs leading-relaxed text-foreground-subtle/80 text-wrap-anywhere">
                        {liveThoughtReveal.displayText}
                      </p>
                    ) : null}
                    {/* Status line sits below the reasoning so the live activity
                        the user is tracking stays anchored at the bottom of the
                        bubble — no need to lift their gaze as thoughts grow. */}
                    {messageActivity.currentEvent ? (
                      <div className="flex items-center justify-between gap-3 text-xs font-medium text-foreground-subtle/90">
                        <span className="truncate">{messageActivity.currentEvent.label}</span>
                        <Feather className="h-3.5 w-3.5 shrink-0 animate-quill-write text-gold/75" />
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {showThoughts ? (
                  <PlannerDisclosure
                    title="Thoughts"
                    count={messageActivity.thoughts.length}
                    open={thoughtsOpen}
                    onToggle={() => setThoughtsOpen((current) => !current)}
                  >
                    <div className="space-y-3 text-xs leading-relaxed text-foreground-subtle/78">
                      {messageActivity.thoughts.map((thought) => (
                        <p key={thought.id} className="whitespace-pre-wrap text-wrap-anywhere">
                          {thought.detail}
                        </p>
                      ))}
                    </div>
                  </PlannerDisclosure>
                ) : null}
                {displayText.trim() ? (
                  isStreamActive ? (
                    <div data-streaming>
                      <p className="text-sm leading-relaxed whitespace-pre-wrap text-wrap-anywhere">{displayText}</p>
                    </div>
                  ) : (
                      <MarkdownBlock content={renderedContent} className="text-sm leading-relaxed" />
                    )
                  ) : null}
                {children}
                {showSources ? (
                  collapseSources ? (
                    <PlannerDisclosure
                      title="Sources"
                      count={messageActivity.sources.length}
                      open={sourcesOpen}
                      onToggle={() => setSourcesOpen((current) => !current)}
                    >
                      <div className="flex flex-wrap items-center gap-1.5">{sourceChips}</div>
                    </PlannerDisclosure>
                  ) : (
                    <div className="space-y-2">
                      <span className="block text-[11px] font-medium text-foreground-subtle/82">Sources</span>
                      <div className="flex flex-wrap items-center gap-1.5">{sourceChips}</div>
                    </div>
                  )
                ) : null}
              </>
            )}
          </div>
          {isAssistant ? (
            (hasVariants || showRegenerate) ? (
              <div className="flex items-center gap-1.5 opacity-0 transition-opacity group-hover:opacity-100 focus-within:opacity-100">
                {hasVariants ? (
                  <PlannerVariantNavigator
                    sessionId={sessionId}
                    messageId={message.planning_message_id}
                    currentVariantIndex={currentVariantIndex}
                    variantCount={variantCount}
                    onActivateVariant={onActivateVariant}
                  />
                ) : null}
                {showRegenerate ? (
                  <button
                    type="button"
                    onClick={onRegenerate}
                    className="flex h-6 w-6 items-center justify-center rounded-md border border-border/40 bg-surface-raised/80 text-foreground-subtle transition-colors hover:border-gold/40 hover:text-gold"
                  >
                    <RefreshCw className="h-3 w-3" />
                  </button>
                ) : null}
              </div>
            ) : null
          ) : (
            !isProcessing && onEdit ? <PlannerEditAction onEdit={onEdit} /> : null
          )}
          <span className="block text-[10px] text-foreground-subtle/0 transition-colors group-hover:text-foreground-subtle/70">
            {formatTimestamp(message.created_at)}
          </span>
        </div>
        {!isAssistant ? (
          <div className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-gold/10 text-gold/70">
            <User className="h-3.5 w-3.5" />
          </div>
        ) : null}
      </div>
    </div>
  );
}

// Compact, clickable label for a source chip: the hostname (sans leading
// "www."), falling back to the raw URL when it can't be parsed. Keeps a row of
// 10+ fetched sources scannable instead of hiding them behind a disclosure.
function sourceHostLabel(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

function PlannerDisclosure({
  title,
  count,
  open,
  onToggle,
  children,
}: {
  title: string;
  count: number;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  return (
    <div className="overflow-hidden rounded-xl border border-border/35 bg-background/6">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left text-[11px] font-medium text-foreground-subtle/82 transition-colors hover:text-foreground-subtle"
        aria-expanded={open}
      >
        <span>{title}</span>
        <span className="flex items-center gap-2">
          <span>{count}</span>
          <ChevronDown className={`h-3.5 w-3.5 transition-transform ${open ? "rotate-180" : ""}`} />
        </span>
      </button>
      {open ? <div className="border-t border-border/35 p-3">{children}</div> : null}
    </div>
  );
}
