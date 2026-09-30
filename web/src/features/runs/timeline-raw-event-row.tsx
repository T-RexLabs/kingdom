import { Fragment, useMemo, useState, type ReactNode } from "react";
import { Activity, AlertTriangle, CheckCircle2, ChevronDown, ChevronRight, Flag, Play, RotateCw, XCircle } from "lucide-react";
import { formatTimestamp, cn } from "@/lib/utils";
import type { TimelineEvent } from "@/api/hooks";
import { accentTextClass, runStatusTextClass } from "./run-detail-tones";
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

export function RawEventsDetail({ events, lookups }: { events: TimelineEvent[]; lookups: TimelineLookups }) {
  const [expandedEventId, setExpandedEventId] = useState<string | null>(null);
  const orderedEvents = useMemo(() => [...events].sort(compareTimelineEventDisplayTime).reverse(), [events]);
  if (orderedEvents.length === 0) return null;

  return (
    <ol aria-label="Milestone events" className="min-w-0 border-l border-border pl-2">
      {orderedEvents.map((event) => (
        <RawEventRow
          key={event.event_id}
          event={event}
          isExpanded={expandedEventId === event.event_id}
          onToggle={() => setExpandedEventId(expandedEventId === event.event_id ? null : event.event_id)}
          lookups={lookups}
        />
      ))}
    </ol>
  );
}

export function RawEventRow({ event, isExpanded, onToggle, lookups }: {
  event: TimelineEvent;
  isExpanded: boolean;
  onToggle: () => void;
  lookups: TimelineLookups;
}) {
  const eventTone = timelineEventTone(event);
  const Icon = isReworkTimelineEvent(event) ? RotateCw : RAW_EVENT_ICONS[(attemptEventIconKey(event) ?? event.event_type) as AttemptEventIconKey | string] ?? Activity;
  const iconClass = event.event_type === "attempt.retry_scheduled" ? accentTextClass("loki") : runStatusTextClass(eventTone);
  const detailId = `${event.event_id}-detail`;
  const badge = specialReasonLabel(event);
  const { label, detail } = buildRawEventLabel(event, lookups);
  const reasonCode = event.reason_code ?? "";
  const showReason = !detail && !badge && reasonCode && !isNoiseReasonCode(reasonCode);

  return (
    <li className="min-w-0 border-b border-border last:border-b-0">
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={isExpanded}
        aria-controls={detailId}
        className={cn(
          "grid w-full min-w-0 grid-cols-[1rem_minmax(0,1fr)_auto] items-start gap-x-3 px-3 py-3.5 text-left transition-colors sm:px-4",
          "hover:bg-surface-raised focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent",
          isExpanded && "bg-surface-raised/60",
        )}
      >
        <Icon aria-hidden="true" className={cn("mt-0.5 h-4 w-4", iconClass)} />
        <span className="min-w-0 space-y-1.5">
          <span className="block text-wrap-anywhere text-sm text-foreground">{label}</span>
          {(detail || showReason || badge) && (
            <span className="flex flex-wrap gap-x-3 gap-y-1 text-xs leading-relaxed">
              {(detail || showReason) && <span className="text-wrap-anywhere text-foreground-muted">{detail || reasonCode.replace(/_/g, " ")}</span>}
              {badge && <span className={cn("text-wrap-anywhere", runStatusTextClass(badgeToneForLabel(badge, eventTone)))}>{badge}</span>}
            </span>
          )}
        </span>
        <span className="flex items-start gap-2.5 pl-1 text-xs text-foreground-subtle">
          <span className="flex flex-col items-end gap-1.5">
            <time dateTime={event.emitted_at} title={event.emitted_at} className="whitespace-nowrap">{formatTimestamp(event.emitted_at)}</time>
            <span className="font-mono tabular-nums" title="Event sequence">#{event.run_event_seq}</span>
          </span>
          {isExpanded ? <ChevronDown aria-hidden="true" className="mt-0.5 h-3.5 w-3.5" /> : <ChevronRight aria-hidden="true" className="mt-0.5 h-3.5 w-3.5" />}
        </span>
      </button>
      {isExpanded && (
        <section id={detailId} aria-label="Event record" className="min-w-0 space-y-4 px-3 pb-4 pt-2 sm:px-4">
          <dl className="grid min-w-0 grid-cols-1 gap-x-6 gap-y-3 text-xs sm:grid-cols-2">
            <RecordField label="Event Type" value={event.event_type} />
            <RecordField label="Recorded At" value={event.emitted_at} />
            <RecordField label="Event ID" value={event.event_id} />
            <RecordField label="Reason Code" value={event.reason_code || "—"} />
            <RecordField label="Run Sequence" value={event.run_event_seq} />
            <RecordField label="Attempt Sequence" value={event.attempt_seq} />
          </dl>
          <div className="min-w-0 space-y-2">
            <h4 className="text-xs font-medium text-foreground-muted">Payload</h4>
            <JsonPayloadCard payload={event.payload_json} />
          </div>
        </section>
      )}
    </li>
  );
}

function RecordField({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="min-w-0 space-y-1">
      <dt className="text-foreground-subtle">{label}</dt>
      <dd className="text-wrap-anywhere font-mono text-foreground">{value}</dd>
    </div>
  );
}

function JsonPayloadCard({ payload }: { payload: unknown }) {
  const normalizedPayload = useMemo(() => normalizeJsonValue(payload), [payload]);

  return (
    <div
      data-testid="timeline-json-payload"
      className="w-full max-w-full overflow-x-auto rounded-md border border-border bg-background p-4"
    >
      <pre className="text-xs font-mono leading-relaxed">{renderJsonValue(normalizedPayload, 0)}</pre>
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
            <span className="text-accent">{JSON.stringify(key)}</span>
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
