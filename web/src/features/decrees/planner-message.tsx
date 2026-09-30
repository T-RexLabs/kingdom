import type { PlanningMessage } from "@/api/hooks";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { formatTimestamp } from "@/lib/utils";
import { RefreshCw, ScrollText, User } from "lucide-react";
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
  isActive,
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

