import { useState } from "react";
import type { PlanningMessage } from "@/api/hooks";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { formatTimestamp } from "@/lib/utils";
import { RefreshCw, User } from "lucide-react";
import { ActivityHistory } from "@/features/activity/activity-history";
import { ActivityLiveStream, ActivitySources, ActivityThoughts } from "@/features/activity/activity-stream";
import type { ActivityItem } from "@/features/activity/activity-view";
import { PlannerEditAction } from "./planner-message-actions";
import { usePlannerMessageActivity } from "./planner-message-activity";
import { PlannerMessageEditor } from "./planner-message-editor";
import { PlannerVariantNavigator } from "./planner-variant-navigator";
import { useTextReveal } from "@/lib/use-text-reveal";

export function PlannerMessageCard({
  message,
  children,
  isStreaming,
  contentMarkdown,
  onEdit,
  onRegenerate,
  isEditing,
  onEditSubmit,
  editText,
  onEditChange,
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
  editText?: string;
  onEditChange?: (text: string) => void;
  onEditCancel?: () => void;
  isProcessing?: boolean;
  isFirstAssistant?: boolean;
  variantCount?: number;
  currentVariantIndex?: number;
  sessionId?: string;
  onActivateVariant?: (messageId: string) => void;
  activityRunId?: string;
  liveActivityItems?: ActivityItem[];
}) {
  const [historyOpen, setHistoryOpen] = useState(false);
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

  const hasVariants = isAssistant && variantCount != null && variantCount > 1 && currentVariantIndex != null && sessionId && onActivateVariant;
  const showRegenerate = isAssistant && !isFirstAssistant && !(isProcessing ?? false) && onRegenerate;
  const showThoughts = isAssistant && !isAssistantStreaming;
  const showSources = isAssistant && !isAssistantStreaming;

  return (
    <div className={`group flex min-w-0 ${isAssistant ? "justify-start" : "justify-end"}`}>
      <div className={`flex w-full max-w-3xl gap-2.5 ${!isAssistant && isEditing ? "w-full" : ""}`}>

        <div className={`min-w-0 space-y-1.5 ${isAssistant ? "" : "text-right"} ${!isAssistant && isEditing ? "flex-1" : ""}`}>
          <div
            className={`overflow-hidden rounded-2xl px-4 py-3 text-sm text-left space-y-3 ${
              isAssistant
                ? "w-full text-foreground"
                : `${isEditing ? "w-full" : "w-fit ml-auto"} rounded-tr-md bg-accent/10 border border-accent/20 text-foreground`
            }`}
          >
            {isEditing && onEditSubmit && onEditCancel ? (
              <PlannerMessageEditor
                initialText={message.content_markdown}
                value={editText}
                onChange={onEditChange}
                onSubmit={onEditSubmit}
                onCancel={onEditCancel}
              />
            ) : (
              <>
                <ActivityLiveStream state={messageActivity} isStreaming={isAssistantStreaming} />
                {showThoughts ? <ActivityThoughts thoughts={messageActivity.thoughts} /> : null}
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
                {showSources ? <ActivitySources sources={messageActivity.sources} /> : null}
                {isAssistant && (messageActivity.items.length > 0 || messageActivity.gaps.length > 0) ? <details className="border-t border-border/60 pt-2" onToggle={(event) => setHistoryOpen(event.currentTarget.open)}><summary className="cursor-pointer py-2 text-xs text-foreground-muted">Provider Activity</summary>{historyOpen && <ActivityHistory messageDisplay="inspection" scopeKey={activityRunId ?? message.planning_message_id} items={messageActivity.items} gaps={messageActivity.gaps} isLoadingMore={messageActivity.isLoadingMore} loadError={messageActivity.loadError} loadMore={messageActivity.loadMore} />}</details> : null}
              </>
            )}
          </div>
          {isAssistant ? (
            (hasVariants || showRegenerate) ? (
              <div className="flex items-center gap-1.5 opacity-70 transition-opacity sm:opacity-0 sm:group-hover:opacity-100 focus-within:opacity-100">
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
                    aria-label="Regenerate reply"
                    className="flex h-6 w-6 items-center justify-center rounded-md border border-border/40 bg-surface-raised/80 text-foreground-subtle transition-colors hover:border-accent/40 hover:text-accent"
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
          <div className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-accent/10 text-accent/70">
            <User className="h-3.5 w-3.5" />
          </div>
        ) : null}
      </div>
    </div>
  );
}

