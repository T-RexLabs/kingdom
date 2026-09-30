import { useEffect, useId, useMemo, useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { SelectField } from "@/components/ui/select-field";
import { Badge } from "@/components/ui/badge";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import {
  MODAL_BODY_TEXT_CLASS,
  Modal,
  ModalContentCard,
  MODAL_SUPPORTING_TEXT_CLASS,
} from "@/components/ui/modal-shell";
import { cn, formatTimestamp } from "@/lib/utils";
import { ArrowDownRight, ArrowUpRight, Gavel, Reply, Send } from "lucide-react";
import type {
  MailboxFilters,
  MailboxMessage,
  MailboxMessageDetail,
} from "@/api/hooks";
import { runStatusBadgeClass, selectionCardClass } from "./run-detail-tones";
import type { MailboxPresentation } from "./mailbox-lanes";

function agentDisplayName(name: string | undefined, id: string | undefined): string {
  if (name && name.trim()) {
    return name;
  }
  return id ?? "Unknown Agent";
}

interface VerdictResult {
  type: "review" | "validation";
  verdict: "PASS" | "REWORK";
}

interface LaneOutcome {
  verdict?: VerdictResult;
  label?: string;
  className?: string;
}

interface MailboxVerdictCarrier {
  node_id: string;
  verdict_type?: MailboxMessage["verdict_type"] | MailboxMessageDetail["verdict_type"];
  verdict_status?: MailboxMessage["verdict_status"] | MailboxMessageDetail["verdict_status"];
}

function verdictTypeForNode(nodeId: string | undefined | null): VerdictResult["type"] | null {
  const trimmed = (nodeId ?? "").trim();
  if (trimmed.startsWith("execution_review")) {
    return "review";
  }
  if (trimmed.startsWith("validation_gates")) {
    return "validation";
  }
  return null;
}

function verdictBadgeClasses(verdict: VerdictResult): string {
		switch (verdict.verdict) {
		case "PASS":
			return runStatusBadgeClass("succeeded", "strong");
		case "REWORK":
			return runStatusBadgeClass("rework", "strong");
	}
}

function verdictLabel(verdict: VerdictResult, full = false): string {
  const v = verdict.verdict.charAt(0) + verdict.verdict.slice(1).toLowerCase();
  if (!full) return v;
  const typeLabel = verdict.type === "review" ? "Review" : "Validation";
  return `${typeLabel}: ${v}`;
}

function communicationLabel(type: string): string {
  switch (type) {
    case "instruction_dispatched":
      return "Instruction";
    case "response_received":
      return "Response";
    case "approval_request":
      return "Approval Request";
    case "approval_response":
      return "Approval Response";
    default:
      return type.split("_").join(" ");
  }
}

function laneOutcomeFromMessage(message: MailboxVerdictCarrier | undefined): LaneOutcome | null {
  if (!message) {
    return null;
  }
  const verdictType = message.verdict_type ?? verdictTypeForNode(message.node_id);
  const verdictStatus = message.verdict_status ?? null;
  if (verdictType && verdictStatus) {
    if (verdictStatus === "pass") {
      return { verdict: { type: verdictType, verdict: "PASS" } };
    }
    if (verdictStatus === "rework") {
      return { verdict: { type: verdictType, verdict: "REWORK" } };
    }
		if (verdictStatus === "invalid") {
			return { label: "Invalid Response", className: runStatusBadgeClass("failed", "strong") };
		}
		if (verdictStatus === "auto_succeeded") {
			return { label: "Auto-Succeeded at Cap", className: runStatusBadgeClass("running", "strong") };
		}
	}
  return null;
}

function laneOutcomeFromMessages(messages: MailboxMessage[]): LaneOutcome | null {
  if (messages.length === 0) {
    return null;
  }
  const latestByThread = new Map<string, MailboxMessage>();
  for (const message of messages) {
    const threadKey = (message.thread_id ?? "").trim() || "_no_thread";
    const existing = latestByThread.get(threadKey);
    if (!existing || new Date(message.created_at).getTime() >= new Date(existing.created_at).getTime()) {
      latestByThread.set(threadKey, message);
    }
  }
  const outcomes = Array.from(latestByThread.values())
    .map((message) => laneOutcomeFromMessage(message))
    .filter((outcome): outcome is LaneOutcome => outcome !== null);
  if (outcomes.length === 0) {
    return null;
  }
  const signatures = new Set(outcomes.map((outcome) => JSON.stringify(outcome)));
  if (signatures.size === 1) {
    return outcomes[0] ?? null;
  }
	return { label: "Mixed Outcomes", className: runStatusBadgeClass("rework", "strong") };
}

function MessageDetailView({ detail }: { detail: MailboxMessageDetail }) {
  const payload = (detail.payload_json ?? {}) as Record<string, unknown>;
  const instruction = typeof payload.instruction_full === "string" ? payload.instruction_full : "";
  const response = typeof payload.response_full === "string" ? payload.response_full : "";
  const detailOutcome = laneOutcomeFromMessage(detail);
  const body = preferredMessageBody(detail.communication_type, instruction, response, detail.content);

  return (
    <div className="space-y-3">
	      <div className="flex items-start justify-between gap-2">
	        <div>
	          <p className="text-xs text-foreground-subtle">
	            {agentDisplayName(detail.from_agent_name, detail.from_agent_id)} {" -> "}
	            {agentDisplayName(detail.to_agent_name, detail.to_agent_id)}
	          </p>
	          {detailOutcome?.verdict && (
	            <span className={cn("mt-1 inline-flex items-center rounded px-2 py-0.5 text-xs font-semibold", verdictBadgeClasses(detailOutcome.verdict))}>
	              {verdictLabel(detailOutcome.verdict, true)}
	            </span>
	          )}
	          {detailOutcome?.label && detailOutcome.className && (
	            <span className={cn("mt-1 inline-flex items-center rounded px-2 py-0.5 text-xs font-semibold", detailOutcome.className)}>
	              {detailOutcome.label}
	            </span>
	          )}
	        </div>
	      </div>

	      <ModalContentCard data-testid="message-detail-card">
	        {body ? (
	          <MarkdownBlock content={body} className={MODAL_BODY_TEXT_CLASS} />
	        ) : (
	          <p className={MODAL_SUPPORTING_TEXT_CLASS}>No message content recorded.</p>
	        )}
	      </ModalContentCard>
	    </div>
  );
}

function preferredMessageBody(
  communicationType: string,
  instruction: string,
  response: string,
  fallback: string,
): string {
  switch (communicationType) {
    case "instruction_dispatched":
    case "approval_request":
      return firstNonEmptyMessageBody(instruction, response, fallback);
    case "response_received":
    case "approval_response":
      return firstNonEmptyMessageBody(response, instruction, fallback);
    default:
      return firstNonEmptyMessageBody(instruction, response, fallback);
  }
}

function firstNonEmptyMessageBody(...values: string[]): string {
  for (const value of values) {
    if (value.trim() !== "") {
      return value;
    }
  }
  return "";
}

function MessageDetailModal({ detail, onClose }: { detail: MailboxMessageDetail | null; onClose: () => void }) {
  if (!detail) {
    return null;
  }

  const description = `${communicationLabel(detail.communication_type)} from ${agentDisplayName(detail.from_agent_name, detail.from_agent_id)}`;

  return (
    <Modal
      open
      onClose={onClose}
      size="lg"
      height="workbench"
      title="Message Detail"
      caption="Mailbox"
      description={description}
    >
	        <MessageDetailView detail={detail} />
    </Modal>
  );
}

export function MailboxPanel({
  presentation,
  isLoading = false,
  errorMessage = null,
  onRetry,
  onSelectMessage,
  selectedDetail,
  filters,
  onChangeFilters,
  hasMore = false,
  isLoadingMore = false,
  loadMoreError = false,
  onLoadMore,
}: {
  presentation: MailboxPresentation;
  isLoading?: boolean;
  errorMessage?: string | null;
  onRetry?: (() => void) | undefined;
  onSelectMessage: (messageId: string | null) => void;
  selectedDetail?: MailboxMessageDetail | null;
  filters: MailboxFilters;
  onChangeFilters: (filters: MailboxFilters) => void;
  hasMore?: boolean;
  isLoadingMore?: boolean;
  loadMoreError?: boolean;
  onLoadMore?: () => void;
}) {
  const laneConversations = presentation.lanes;
  const filterId = useId();
  const [selectedLane, setSelectedLane] = useState<string | null>(() => laneConversations[0]?.laneKey ?? null);
  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null);

  const activeLane = useMemo(() => {
    if (laneConversations.length === 0) {
      return null;
    }
    const current = selectedLane
      ? laneConversations.find((lane) => lane.laneKey === selectedLane)
      : null;
    return current ?? laneConversations[0];
  }, [laneConversations, selectedLane]);

  const visibleSelectedMessageId = useMemo(() => {
    if (!selectedMessageId || !activeLane) {
      return null;
    }
    return activeLane.messages.some((message) => message.message_id === selectedMessageId)
      ? selectedMessageId
      : null;
  }, [activeLane, selectedMessageId]);

  useEffect(() => {
    if (!activeLane && selectedMessageId) {
      onSelectMessage(null);
      return;
    }
    if (!activeLane || !selectedMessageId) {
      return;
    }
    const containsSelected = activeLane.messages.some((message) => message.message_id === selectedMessageId);
    if (!containsSelected) {
      onSelectMessage(null);
    }
  }, [activeLane, onSelectMessage, selectedMessageId]);

  const hasFilters = Boolean(filters.communicationType);
  const showLoadingState = isLoading && presentation.visibleMessageCount === 0;
  const showErrorState = !showLoadingState && errorMessage !== null && presentation.visibleMessageCount === 0;

  return (
    <div data-testid="mailbox-panel" className="flex h-full min-h-0 flex-col gap-3">
      <div className="flex items-center gap-2">
        <SelectField
          id={filterId}
          label="Message type"
          labelClassName="sr-only"
          className="w-[10.5rem] shrink-0"
          triggerClassName="py-1 text-xs"
          value={filters.communicationType ?? ""}
          options={[
            { label: "All messages", value: "" },
            { label: "Instruction", value: "instruction_dispatched" },
            { label: "Response", value: "response_received" },
            { label: "Approval Request", value: "approval_request" },
            { label: "Approval Response", value: "approval_response" },
          ]}
          onChange={(next) => onChangeFilters({ communicationType: next || undefined })}
        />
        {hasFilters && (
          <button
            onClick={() => onChangeFilters({})}
            className="rounded-md border border-border px-2 py-1 text-xs text-foreground-subtle transition-colors hover:bg-surface-raised hover:text-foreground"
          >
            Clear filter
          </button>
        )}
        <span className="text-xs text-foreground-subtle">
          Grouped by message lane with live markdown previews.
        </span>
      </div>

      <div
        data-testid="mailbox-panel-layout"
        className="flex min-h-0 flex-1 flex-col gap-3 xl:grid xl:grid-cols-[minmax(15rem,22rem)_minmax(0,1fr)] xl:grid-rows-[minmax(0,1fr)]"
      >
        <div data-testid="mailbox-lane-list" className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
          {showLoadingState ? (
            <Card className="border-border">
              <CardContent className="space-y-2 py-6 text-center text-sm text-foreground-subtle">
                <p>Loading messages...</p>
              </CardContent>
            </Card>
          ) : showErrorState ? (
            <Card className="border-status-failed/30 bg-status-failed/5">
              <CardContent className="space-y-3 py-6 text-center text-sm text-foreground-subtle">
                <p>Unable to load messages.</p>
                <p className="text-xs text-foreground-muted">{errorMessage}</p>
                {onRetry && (
                  <button
                    onClick={onRetry}
                    className="mx-auto rounded-md border border-border px-2 py-1 text-xs text-foreground-subtle transition-colors hover:bg-surface-raised hover:text-foreground"
                  >
                    Retry
                  </button>
                )}
              </CardContent>
            </Card>
          ) : laneConversations.length === 0 ? (
            <Card className="border-border">
              <CardContent className="space-y-2 py-6 text-center text-sm text-foreground-subtle">
                <p>{hasFilters ? "No messages match this filter." : "No messages yet."}</p>
                {hasFilters && (
                  <button
                    onClick={() => onChangeFilters({})}
                    className="mx-auto rounded-md border border-border px-2 py-1 text-xs text-foreground-subtle transition-colors hover:bg-surface-raised hover:text-foreground"
                  >
                    Clear filter
                  </button>
                )}
              </CardContent>
            </Card>
          ) : (
            laneConversations.map((lane) => {
              const latest = lane.messages[lane.messages.length - 1];
              const isActive = activeLane?.laneKey === lane.laneKey;
              const laneOutcome = laneOutcomeFromMessages(lane.messages);
              return (
                <button
                  key={lane.laneKey}
                  aria-pressed={isActive}
                  onClick={() => {
                    setSelectedLane(lane.laneKey);
                    onSelectMessage(null);
                  }}
                  className={cn(
                    "w-full rounded-md border px-3 py-2 text-left transition-colors",
                    selectionCardClass(isActive),
                  )}
                >
                  <div className="flex items-start justify-between gap-2">
                    <p className="text-wrap-anywhere text-xs font-medium text-foreground">{lane.laneLabel}</p>
                    <div className="text-right">
                      <p className="shrink-0 text-[11px] text-foreground-subtle">{formatTimestamp(lane.latestAt)}</p>
                      <p className="mt-0.5 whitespace-nowrap text-[11px] text-foreground-subtle">{lane.messages.length} messages{hasMore ? " loaded" : ""}</p>
                    </div>
                  </div>
                  <div className="mt-1 max-h-12 overflow-hidden text-wrap-anywhere text-xs text-foreground-muted">
                    <MarkdownBlock content={latest?.preview || "No preview"} />
                  </div>
                  <div className="mt-2 flex items-center gap-2">
                    {laneOutcome?.verdict && (
                      <Badge className={cn("font-medium", verdictBadgeClasses(laneOutcome.verdict))}>
                        {verdictLabel(laneOutcome.verdict, true)}
                      </Badge>
                    )}
                    {!laneOutcome?.verdict && laneOutcome?.label && (
                      <Badge className={cn("font-medium", laneOutcome.className)}>
                        {laneOutcome.label}
                      </Badge>
                    )}
                    {lane.awaitingResponseCount > 0 && (
                      <Badge className={runStatusBadgeClass("blocked", "strong")}>
                        {lane.awaitingResponseCount} awaiting response
                      </Badge>
                    )}
                  </div>
                </button>
              );
            })
          )}
          {hasMore && onLoadMore ? (
            <div className="space-y-2 py-2">
              {loadMoreError ? <p role="status" className="text-xs text-status-failed">Older messages could not be loaded.</p> : null}
              <Button variant="ghost" size="sm" className="w-full" disabled={isLoadingMore} onClick={onLoadMore}>
                {isLoadingMore ? "Loading messages..." : loadMoreError ? "Retry Older Messages" : "Load Older Messages"}
              </Button>
            </div>
          ) : null}
        </div>

        <div className="flex min-h-0 flex-1 flex-col gap-3">
          <div className="rounded-md border border-border px-3 py-2">
            <p className="text-xs uppercase tracking-wide text-foreground-subtle">Conversation</p>
            <p className="text-wrap-anywhere text-sm font-medium text-foreground">{activeLane?.laneLabel ?? "No lane selected"}</p>
            {activeLane && (
              <p className="mt-1 text-xs text-foreground-subtle">
                {activeLane.messages.length} messages{hasMore ? " loaded" : ""} across {new Set(activeLane.messages.map((message) => message.thread_id || "_none")).size} thread(s)
              </p>
            )}
          </div>

          <div data-testid="mailbox-message-list" className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
            {(activeLane?.messages ?? []).map((message) => {
              const isResponse = message.communication_type === "response_received" || message.communication_type === "approval_response";
              const isSelected = visibleSelectedMessageId === message.message_id;
              const messageOutcome = laneOutcomeFromMessage(message);
              return (
                <button
                  key={message.message_id}
                  aria-pressed={isSelected}
                  onClick={() => {
                    setSelectedMessageId(message.message_id);
                    onSelectMessage(message.message_id);
                  }}
                  className={cn(
                    "w-full rounded-md border px-3 py-2 text-left transition-colors",
                    selectionCardClass(isSelected),
                  )}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex min-w-0 items-center gap-2">
                      {isResponse ? (
                        <Reply className="h-3.5 w-3.5 shrink-0 text-status-succeeded" />
                      ) : message.communication_type.startsWith("approval") ? (
                        <Gavel className="h-3.5 w-3.5 shrink-0 text-gold" />
                      ) : (
                        <Send className="h-3.5 w-3.5 shrink-0 text-status-running" />
                      )}
                      <span className="truncate text-xs font-medium text-foreground">
                        {communicationLabel(message.communication_type)}
                      </span>
                    </div>
                    <span className="shrink-0 text-[11px] text-foreground-subtle">
                      {formatTimestamp(message.created_at)}
                    </span>
                  </div>
                  <div className="mt-1 flex items-center gap-1 text-xs text-foreground-subtle">
                    <span>{agentDisplayName(message.from_agent_name, message.from_agent_id)}</span>
                    {isResponse ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
                    <span>{agentDisplayName(message.to_agent_name, message.to_agent_id)}</span>
                  </div>
                  {messageOutcome?.verdict && (
                    <div className="mt-1 flex items-center gap-2">
                      <Badge className={cn("font-medium", verdictBadgeClasses(messageOutcome.verdict))}>
                        {verdictLabel(messageOutcome.verdict, true)}
                      </Badge>
                    </div>
                  )}
                  {!messageOutcome?.verdict && messageOutcome?.label && (
                    <div className="mt-1 flex items-center gap-2">
                      <Badge className={cn("font-medium", messageOutcome.className)}>
                        {messageOutcome.label}
                      </Badge>
                    </div>
                  )}
                  <div className="mt-1 max-h-16 overflow-hidden text-wrap-anywhere text-xs text-foreground-muted">
                    <MarkdownBlock content={message.preview} />
                  </div>
                </button>
              );
            })}
            {!activeLane && (
              <Card className="border-border">
                <CardContent className="flex min-h-[14rem] items-center justify-center text-sm text-foreground-subtle">
                  Select a lane to inspect messages.
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>

      <MessageDetailModal
        detail={selectedDetail ?? null}
        onClose={() => {
          onSelectMessage(null);
        }}
      />
    </div>
  );
}
