import { useState } from "react";
import { Activity, Ban, CheckCircle2, ChevronDown, ChevronRight, Clock3, Layers, MinusCircle, PauseCircle, Play, RotateCw, XCircle } from "lucide-react";
import { formatTimestamp, cn } from "@/lib/utils";
import type { Milestone, TimelineLookups } from "./timeline-types";
import { formatDuration } from "./timeline-milestones";
import { runStatusTextClass } from "./run-detail-tones";
import { badgeToneForLabel } from "./run-status-semantics";
import { RawEventsDetail } from "./timeline-raw-event-row";

const STATUS_ICONS: Record<string, typeof Play> = {
  succeeded: CheckCircle2,
  failed: XCircle,
  running: Play,
  ready: Clock3,
  pending: Activity,
  blocked: PauseCircle,
  cancelled: Ban,
  skipped: MinusCircle,
  rework: RotateCw,
  neutral: Activity,
};

export function MilestoneRow({
  milestone,
  isExpanded,
  onToggle,
  lookups,
}: {
  milestone: Milestone;
  isExpanded: boolean;
  onToggle: () => void;
  lookups: TimelineLookups;
}) {
  const Icon = milestone.kind === "fan_out" ? Layers : STATUS_ICONS[milestone.status] ?? Activity;
  const timestamp = milestone.endTimestamp ?? milestone.timestamp;
  const duration = milestone.endTimestamp ? formatDuration(milestone.timestamp, milestone.endTimestamp) : null;
  const detailPanelId = `${milestone.id}-detail`;

  return (
    <li className="min-w-0 border-b border-border last:border-b-0">
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={isExpanded}
        aria-controls={detailPanelId}
        className={cn(
          "grid w-full min-w-0 grid-cols-[1rem_minmax(0,1fr)_auto] items-start gap-x-3 px-3 py-4 text-left transition-colors sm:px-4",
          "hover:bg-surface-raised focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent",
          isExpanded && "bg-surface-raised/60",
        )}
      >
        <Icon aria-hidden="true" className={cn("mt-0.5 h-4 w-4", runStatusTextClass(milestone.status))} />
        <span className="min-w-0 space-y-1.5">
          <span className="block text-wrap-anywhere text-sm font-medium text-foreground">{milestone.label}</span>
          {(milestone.detail || milestone.badge) && (
            <span className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs leading-relaxed">
              {milestone.detail && <span className="text-wrap-anywhere text-foreground-muted">{milestone.detail}</span>}
              {milestone.badge && <span className={cn("text-wrap-anywhere", runStatusTextClass(badgeToneForLabel(milestone.badge, milestone.status)))}>{milestone.badge}</span>}
            </span>
          )}
          {milestone.kind === "fan_out" && !isExpanded && milestone.children && (
            <span className="flex flex-wrap gap-x-4 gap-y-1 text-xs leading-relaxed">
              {milestone.children.map((child) => (
                <span key={child.id} className={runStatusTextClass(child.status)}>
                  {child.label.startsWith(milestone.label) ? child.label.slice(milestone.label.length).trim() || child.label : child.label}
                </span>
              ))}
            </span>
          )}
        </span>
        <span className="flex items-start gap-2.5 pl-1 text-xs text-foreground-subtle">
          <span className="flex flex-col items-end gap-1.5">
            <time dateTime={timestamp} title={timestamp} className="whitespace-nowrap">{formatTimestamp(timestamp)}</time>
            {duration && <span className="font-mono tabular-nums" title="Duration">{duration}</span>}
          </span>
          {isExpanded ? <ChevronDown aria-hidden="true" className="mt-0.5 h-3.5 w-3.5" /> : <ChevronRight aria-hidden="true" className="mt-0.5 h-3.5 w-3.5" />}
        </span>
      </button>
      {isExpanded && (
        <div id={detailPanelId} className="min-w-0 pb-4 pl-7 pr-3 sm:pl-11 sm:pr-4">
          {milestone.kind === "fan_out" && milestone.children ? (
            <FanOutExpandedDetail children={[...milestone.children].reverse()} parentLabel={milestone.label} lookups={lookups} />
          ) : (
            <RawEventsDetail events={milestone.events} lookups={lookups} />
          )}
        </div>
      )}
    </li>
  );
}

function FanOutExpandedDetail({ children, parentLabel, lookups }: { children: Milestone[]; parentLabel: string; lookups: TimelineLookups }) {
  const [expandedChild, setExpandedChild] = useState<string | null>(null);

  return (
    <ol aria-label="Parallel tasks" className="min-w-0 border-l border-border pl-2">
      {children.map((child) => {
        const isOpen = expandedChild === child.id;
        const childSuffix = child.label.startsWith(parentLabel) ? child.label.slice(parentLabel.length).trim() : child.label;
        return (
          <MilestoneRow
            key={child.id}
            milestone={{ ...child, label: childSuffix || child.label }}
            isExpanded={isOpen}
            onToggle={() => setExpandedChild(isOpen ? null : child.id)}
            lookups={lookups}
          />
        );
      })}
    </ol>
  );
}
