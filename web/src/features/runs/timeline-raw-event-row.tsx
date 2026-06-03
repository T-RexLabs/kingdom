import { Fragment, useMemo, useState, type ReactNode } from "react";
import { Activity, AlertTriangle, CheckCircle2, ChevronDown, ChevronRight, Flag, Play, RotateCw, XCircle } from "lucide-react";
import { formatTimestamp, cn } from "@/lib/utils";
import type { TimelineEvent } from "@/api/hooks";
import { accentTextClass, runStatusBadgeClass, runStatusDotClass, runStatusTextClass } from "./run-detail-tones";
import { buildRawEventLabel, isNoiseReasonCode, specialReasonLabel } from "./timeline-milestones";
import { attemptEventIconKey, badgeToneForLabel, isReworkTimelineEvent, timelineEventTone, type AttemptEventIconKey } from "./run-status-semantics";
import type { TimelineLookups } from "./timeline-types";

const RAW_EVENT_ICONS: Record<string, typeof Play> = {
  activity: Activity,
  play: Play,
  check: CheckCircle2,
  x: XCircle,
  rotate: RotateCw,
  alert: AlertTriangle,
  "node.state_changed": Activity,
  "run.state_changed": Flag,
  "approval.recorded": Flag,
  "escalation.raised": AlertTriangle,
};

export function RawEventsDetail({
  events,
  lookups,
}: {
  events: TimelineEvent[];
  lookups: TimelineLookups;
}) {
  const [expandedEventId, setExpandedEventId] = useState<string | null>(null);
  const orderedEvents = useMemo(
    () => [...events].sort(compareTimelineEventDisplayTime).reverse(),
    [events],
  );

  if (orderedEvents.length === 0) {
    return null;
  }

  return (
    <div className="ml-2 min-w-0 space-y-0.5 border-l border-border pl-2">
	      {orderedEvents.map((event) => {
	        const isOpen = expandedEventId === event.event_id;
	        const hasPayload = !!event.payload_json && Object.keys(event.payload_json).length > 0;
	        const payloadId = `${event.event_id}-payload`;
	        const relative = formatTimestamp(event.emitted_at);
	        const { label, detail } = buildRawEventLabel(event, lookups);
	        const reasonCode = event.reason_code ?? "";
	        const showReason = !detail && reasonCode && !isNoiseReasonCode(reasonCode);

	        return (
	          <div key={event.event_id}>
	            {hasPayload ? (
	              <button
	                type="button"
	                onClick={() => setExpandedEventId(isOpen ? null : event.event_id)}
	                aria-expanded={isOpen}
	                aria-controls={payloadId}
	                className={cn(
	                  "min-w-0 w-full rounded-md px-2.5 py-1 text-left transition-colors",
	                  "cursor-pointer hover:bg-surface-raised",
	                  isOpen && "bg-surface-raised",
	                )}
	              >
	                <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
	                  <span className="min-w-0 flex-1 text-wrap-anywhere text-[10px] text-foreground-subtle">
	                    {label}
	                    {detail ? ` · ${detail}` : ""}
	                    {showReason ? ` · ${reasonCode.replace(/_/g, " ")}` : ""}
	                  </span>
	                  <span className="shrink-0 text-[10px] text-foreground-subtle">{relative}</span>
	                  {isOpen
	                    ? <ChevronDown className="h-3 w-3 shrink-0 text-foreground-subtle" />
	                    : <ChevronRight className="h-3 w-3 shrink-0 text-foreground-subtle" />}
	                </div>
	              </button>
	            ) : (
	              <div className="min-w-0 w-full rounded-md px-2.5 py-1 text-left">
	                <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
	                  <span className="min-w-0 flex-1 text-wrap-anywhere text-[10px] text-foreground-subtle">
	                    {label}
	                    {detail ? ` · ${detail}` : ""}
	                    {showReason ? ` · ${reasonCode.replace(/_/g, " ")}` : ""}
	                  </span>
	                  <span className="shrink-0 text-[10px] text-foreground-subtle">{relative}</span>
	                </div>
	              </div>
	            )}
	            {isOpen && hasPayload && <JsonPayloadCard id={payloadId} payload={event.payload_json} className="mt-0.5" />}
	          </div>
	        );
	      })}
    </div>
  );
}

export function RawEventRow({
  event,
  isLast,
  isExpanded,
  onToggle,
	lookups,
}: {
  event: TimelineEvent;
  isLast: boolean;
  isExpanded: boolean;
  onToggle: () => void;
	lookups: TimelineLookups;
}) {
	const eventTone = timelineEventTone(event);
	const reworkEvent = isReworkTimelineEvent(event);
	const Icon = reworkEvent
		? RotateCw
		: RAW_EVENT_ICONS[(attemptEventIconKey(event) ?? event.event_type) as AttemptEventIconKey | string] ?? Activity;
	const dotClass = event.event_type === "attempt.retrying"
		? "bg-role-loki"
		: runStatusDotClass(eventTone);
	const iconClass = event.event_type === "attempt.retrying"
		? accentTextClass("loki")
		: runStatusTextClass(eventTone);
	const hasPayload = !!event.payload_json && Object.keys(event.payload_json).length > 0;
	const payloadId = `${event.event_id}-payload`;
	const relative = formatTimestamp(event.emitted_at);
	const badge = specialReasonLabel(event);
  const { label, detail } = buildRawEventLabel(event, lookups);
  const reasonCode = event.reason_code ?? "";
  const showReason = !detail && reasonCode && !isNoiseReasonCode(reasonCode);

  return (
    <div className="group relative flex min-w-0 gap-3">
      <div className="relative flex flex-col items-center">
        <div
          className={cn(
            "z-10 mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full border-2 border-background",
            dotClass,
          )}
        />
        {!isLast && <div className="w-px flex-1 bg-border" />}
      </div>
	  <div className="min-w-0 flex-1 pb-2">
		{hasPayload ? (
		  <button
			type="button"
			onClick={onToggle}
			aria-expanded={isExpanded}
			aria-controls={payloadId}
			className={cn(
			  "min-w-0 w-full rounded-md border border-border bg-surface px-2.5 py-1.5 text-left transition-colors",
			  "hover:border-border-strong hover:bg-surface-raised",
			  isExpanded && "border-border-strong bg-surface-raised",
			)}
		  >
			<div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
			  <Icon className={cn("h-3.5 w-3.5 shrink-0", iconClass)} />
			  <span className="min-w-0 flex-1 text-wrap-anywhere text-xs text-foreground">{label}</span>
			  {detail && <span className="text-wrap-anywhere text-[10px] text-foreground-subtle">{detail}</span>}
			  {showReason && <span className="text-wrap-anywhere text-[10px] text-foreground-subtle">{reasonCode.replace(/_/g, " ")}</span>}
	            {badge && (
	              <span className={cn("inline-flex items-center rounded-md border border-border px-1.5 py-0.5 text-[10px] font-medium", runStatusBadgeClass(badgeToneForLabel(badge, eventTone)))}>
	                {badge}
	              </span>
	            )}
			  <span className="shrink-0 text-[10px] text-foreground-subtle">{relative}</span>
			  {isExpanded
				? <ChevronDown className="h-3 w-3 shrink-0 text-foreground-subtle" />
				: <ChevronRight className="h-3 w-3 shrink-0 text-foreground-subtle" />}
			</div>
		  </button>
		) : (
		  <div className="min-w-0 w-full rounded-md border border-border bg-surface px-2.5 py-1.5 text-left">
			<div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
			  <Icon className={cn("h-3.5 w-3.5 shrink-0", iconClass)} />
			  <span className="min-w-0 flex-1 text-wrap-anywhere text-xs text-foreground">{label}</span>
			  {detail && <span className="text-wrap-anywhere text-[10px] text-foreground-subtle">{detail}</span>}
			  {showReason && <span className="text-wrap-anywhere text-[10px] text-foreground-subtle">{reasonCode.replace(/_/g, " ")}</span>}
	            {badge && (
	              <span className={cn("inline-flex items-center rounded-md border border-border px-1.5 py-0.5 text-[10px] font-medium", runStatusBadgeClass(badgeToneForLabel(badge, eventTone)))}>
	                {badge}
	              </span>
	            )}
			  <span className="shrink-0 text-[10px] text-foreground-subtle">{relative}</span>
			</div>
		  </div>
		)}
		{isExpanded && hasPayload && <JsonPayloadCard id={payloadId} payload={event.payload_json} className="mt-1" />}
	  </div>
    </div>
  );
}

function JsonPayloadCard({ payload, className, id }: { payload: unknown; className?: string; id?: string }) {
  const normalizedPayload = useMemo(() => normalizeJsonValue(payload), [payload]);

  return (
    <div
		id={id}
      data-testid="timeline-json-payload"
      className={cn(
        "w-full max-w-full overflow-x-auto rounded-md border border-border bg-background px-3 py-2",
        className,
      )}
    >
      <pre className="text-[11px] font-mono leading-relaxed">{renderJsonValue(normalizedPayload, 0)}</pre>
    </div>
  );
}

function renderJsonValue(value: JsonValue, indent: number): ReactNode {
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "[]";
    }
    return (
      <>
        [
        {"\n"}
        {value.map((entry, index) => (
          <Fragment key={`array-${indent}-${index}`}>
            {indentText(indent + 2)}
            {renderJsonValue(entry, indent + 2)}
            {index < value.length - 1 ? "," : ""}
            {"\n"}
          </Fragment>
        ))}
        {indentText(indent)}]
      </>
    );
  }

  if (isJsonObject(value)) {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      return "{}";
    }
    return (
      <>
        {"{"}
        {"\n"}
        {entries.map(([key, entryValue], index) => (
          <Fragment key={`object-${indent}-${key}`}>
            {indentText(indent + 2)}
            <span className="text-role-council">{JSON.stringify(key)}</span>
            {": "}
            {renderJsonValue(entryValue, indent + 2)}
            {index < entries.length - 1 ? "," : ""}
            {"\n"}
          </Fragment>
        ))}
        {indentText(indent)}{"}"}
      </>
    );
  }

  if (typeof value === "string") {
    return <span className="text-status-succeeded">{JSON.stringify(value)}</span>;
  }

  if (typeof value === "number") {
    return <span className="text-status-blocked">{String(value)}</span>;
  }

  if (typeof value === "boolean") {
    return <span className="text-role-loki">{String(value)}</span>;
  }

  return <span className="text-foreground-subtle">null</span>;
}

function normalizeJsonValue(value: unknown): JsonValue {
  const serialized = JSON.stringify(value);
  if (!serialized) {
    return null;
  }
  return JSON.parse(serialized) as JsonValue;
}

function indentText(indent: number): string {
  return " ".repeat(indent);
}

function isJsonObject(value: JsonValue): value is Record<string, JsonValue> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

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
