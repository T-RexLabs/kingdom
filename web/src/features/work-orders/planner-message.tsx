import type { PlanningMessage } from "@/api/hooks";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { formatTimestamp } from "@/lib/utils";
import { RefreshCw } from "lucide-react";
import { ThemeIcon } from "@/theme";
import { ActivitySources } from "@/features/activity/activity-sources";
import type { ActivityItem } from "@/features/activity/activity-view";
import { PlannerEditAction } from "./planner-message-actions";
import { PlannerMessageActivity } from "./planner-message-activity";
import { usePlannerMessageActivity } from "./use-planner-message-activity";
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
  isCanonical,
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
  isCanonical?: boolean;
}) {
  const isAssistant = message.role === "assistant";
  const renderedContent = contentMarkdown ?? message.content_markdown;
  const isAssistantStreaming = isAssistant && (isStreaming ?? false);
  const hasFinalResponse = isAssistant && (isCanonical ?? !isAssistantStreaming);
  // useTextReveal does no-op work for non-assistant messages, but its effect
  // and ref machinery still cost cycles on every render. Pass an empty string
  // when we know the hook will never animate so the no-op path stays cheap;
  // user messages then render their content_markdown verbatim.
  const reveal = useTextReveal(
    isAssistant ? renderedContent : "",
    isAssistantStreaming,
  );
  const displayText = isAssistant ? reveal.displayText : renderedContent;
  const isStreamActive = isAssistant ? reveal.isStreamActive : false;
  const resolvedActivityRunId = activityRunId ?? message.source_run_id;
  const messageActivity = usePlannerMessageActivity({
    activityRunId: resolvedActivityRunId,
    isAssistant,
    isStreaming: isAssistantStreaming,
    liveActivityItems,
  });

  const hasVariants =
    isAssistant &&
    variantCount != null &&
    variantCount > 1 &&
    currentVariantIndex != null &&
    sessionId &&
    onActivateVariant;
  const showRegenerate =
    isAssistant &&
    !isFirstAssistant &&
    !(isProcessing ?? false) &&
    onRegenerate;
  const showSources = isAssistant;

  return (
    <div
      className={`group flex min-w-0 ${isAssistant ? "justify-start" : "justify-end"}`}
    >
      <div
        className={`flex w-full max-w-3xl gap-2.5 ${isAssistant ? "" : "justify-end"}`}
      >
        <div
          className={`min-w-0 space-y-1.5 ${isAssistant ? "flex-1" : "text-right"} ${!isAssistant && isEditing ? "flex-1" : ""}`}
        >
          <div
            data-message-kind={isAssistant ? "assistant" : "user"}
            className={`ui-chat-message overflow-hidden rounded-2xl px-4 py-3 text-sm text-left space-y-3 ${
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
                {isAssistant && (
                  <PlannerMessageActivity
                    key={resolvedActivityRunId ?? message.planning_message_id}
                    scopeKey={
                      resolvedActivityRunId ?? message.planning_message_id
                    }
                    activity={messageActivity}
                    isLive={isAssistantStreaming && !hasFinalResponse}
                  />
                )}
                {displayText.trim() ? (
                  isStreamActive ? (
                    <div data-streaming>
                      <p className="text-sm leading-relaxed whitespace-pre-wrap text-wrap-anywhere">
                        {displayText}
                      </p>
                    </div>
                  ) : (
                    <MarkdownBlock
                      content={renderedContent}
                      className="text-sm leading-relaxed"
                    />
                  )
                ) : null}
                {children}
                {showSources ? (
                  <ActivitySources sources={messageActivity.sources} />
                ) : null}
              </>
            )}
          </div>
          {!isAssistantStreaming && (
            <div className="flex min-h-6 items-center gap-2 text-[10px] text-foreground-subtle opacity-70 transition-opacity sm:opacity-0 sm:group-hover:opacity-100 focus-within:opacity-100">
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
                  className="ui-button ui-button--ghost flex h-6 w-6 items-center justify-center rounded-md border border-border/40 bg-surface-raised/80 text-foreground-subtle transition-colors hover:border-accent/40 hover:text-accent"
                >
                  <RefreshCw className="h-3 w-3" />
                </button>
              ) : null}
              {!isAssistant && !isProcessing && onEdit ? <PlannerEditAction onEdit={onEdit} /> : null}
              <time dateTime={message.created_at}>{formatTimestamp(message.created_at)}</time>
            </div>
          )}
        </div>
        {!isAssistant ? (
          <div className="ui-operator-avatar mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-accent/10 text-accent/70">
            <ThemeIcon slot="role.operator" size="var(--ui-operator-avatar-icon-size, 14px)" />
          </div>
        ) : null}
      </div>
    </div>
  );
}
