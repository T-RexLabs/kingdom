import { useState } from "react";
import { Activity, Ban, CheckCircle2, ChevronDown, ChevronRight, Clock3, Layers, MinusCircle, PauseCircle, Play, RotateCw, XCircle } from "lucide-react";
import { formatTimestamp, cn } from "@/lib/utils";
import type { Milestone, TimelineLookups } from "./timeline-types";
import { formatDuration } from "./timeline-milestones";
import { runStatusBadgeClass, runStatusBorderTintClass, runStatusDotClass, runStatusTextClass } from "./run-detail-tones";
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
  isLast,
  isExpanded,
  onToggle,
  lookups,
}: {
  milestone: Milestone;
  isLast: boolean;
  isExpanded: boolean;
  onToggle: () => void;
  lookups: TimelineLookups;
}) {
  const Icon = milestone.kind === "fan_out" ? Layers : STATUS_ICONS[milestone.status] ?? Activity;
	const iconClass = runStatusTextClass(milestone.status);
	const dotClass = runStatusDotClass(milestone.status);
	const borderClass = runStatusBorderTintClass(milestone.status);
	const relative = formatTimestamp(milestone.endTimestamp ?? milestone.timestamp);
	const duration = milestone.endTimestamp
		? formatDuration(milestone.timestamp, milestone.endTimestamp)
		: null;
	const detailPanelId = `${milestone.id}-detail`;
	const orderedChildren = milestone.children ? [...milestone.children].reverse() : null;

  return (
    <div className="group relative flex min-w-0 gap-3">
      <div className="relative flex flex-col items-center">
        <div
          className={cn(
            "z-10 mt-2 h-3 w-3 shrink-0 rounded-full border-2 border-background",
            dotClass,
          )}
        />
        {!isLast && <div className="w-px flex-1 bg-border" />}
      </div>

      <div className="min-w-0 flex-1 pb-3">
        <button
			type="button"
          onClick={onToggle}
			aria-expanded={isExpanded}
			aria-controls={detailPanelId}
          className={cn(
            "min-w-0 w-full rounded-lg border bg-surface px-3 py-2.5 text-left transition-colors",
            "hover:border-border-strong hover:bg-surface-raised",
            isExpanded ? "border-border-strong bg-surface-raised" : "border-border",
            borderClass && !isExpanded && borderClass,
          )}
        >
	          <div className="flex min-w-0 flex-wrap items-center gap-x-2.5 gap-y-1.5">
            <Icon className={cn("h-4 w-4 shrink-0", iconClass)} />
	            <span className={cn("min-w-0 flex-1 text-wrap-anywhere text-sm text-foreground", milestone.status === "failed" && "font-medium")}>
              {milestone.label}
            </span>
            {milestone.detail && (
	              <span className="text-wrap-anywhere text-xs text-foreground-subtle">{milestone.detail}</span>
            )}
				{milestone.badge && (
	              <span className={cn("inline-flex items-center rounded-md border border-border px-1.5 py-0.5 text-[10px] font-medium", runStatusBadgeClass(badgeToneForLabel(milestone.badge, milestone.status)))}>
                {milestone.badge}
              </span>
            )}
            {duration && (
              <span className="shrink-0 rounded bg-surface-raised px-1.5 py-0.5 text-[10px] font-mono text-foreground-subtle">
                {duration}
              </span>
            )}
            <span className="shrink-0 text-[11px] text-foreground-subtle">{relative}</span>
            {isExpanded ? (
              <ChevronDown className="h-3.5 w-3.5 shrink-0 text-foreground-subtle" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5 shrink-0 text-foreground-subtle" />
            )}
          </div>

          {milestone.kind === "fan_out" && !isExpanded && milestone.children && (
            <div className="mt-1.5 flex flex-wrap gap-1.5 pl-6.5">
              {milestone.children.map((child) => {
                const childSuffix = child.label.startsWith(milestone.label)
                  ? child.label.slice(milestone.label.length).trim()
                  : child.label;
                const pillLabel = childSuffix || child.label;
                return (
                  <span
                    key={child.id}
                    className={cn(
                      "inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-medium",
                      runStatusBadgeClass(child.status),
                    )}
                  >
                    <div className={cn("h-1.5 w-1.5 rounded-full", runStatusDotClass(child.status))} />
                    {pillLabel}
                  </span>
                );
              })}
            </div>
          )}
        </button>

        {isExpanded && (
		  <div id={detailPanelId} className="mt-1.5">
	            {milestone.kind === "fan_out" && orderedChildren ? (
	              <FanOutExpandedDetail children={orderedChildren} parentLabel={milestone.label} lookups={lookups} />
	            ) : (
	              <RawEventsDetail events={milestone.events} lookups={lookups} />
	            )}
          </div>
        )}
      </div>
    </div>
  );
}

function FanOutExpandedDetail({
  children,
  parentLabel,
  lookups,
}: {
  children: Milestone[];
  parentLabel: string;
  lookups: TimelineLookups;
}) {
  const [expandedChild, setExpandedChild] = useState<string | null>(null);

  return (
    <div className="ml-2 min-w-0 space-y-1 border-l border-border pl-2">
		{children.map((child) => {
			const Icon = STATUS_ICONS[child.status] ?? Activity;
			const relative = formatTimestamp(child.endTimestamp ?? child.timestamp);
			const duration = child.endTimestamp
				? formatDuration(child.timestamp, child.endTimestamp)
				: null;
        const isOpen = expandedChild === child.id;
			const detailPanelId = `${child.id}-detail`;
        const childSuffix = child.label.startsWith(parentLabel)
          ? child.label.slice(parentLabel.length).trim()
          : child.label;
        const displayLabel = childSuffix || child.label;

        return (
          <div key={child.id}>
            <button
				type="button"
              onClick={() => setExpandedChild(isOpen ? null : child.id)}
				aria-expanded={isOpen}
				aria-controls={detailPanelId}
              className={cn(
                "min-w-0 w-full cursor-pointer rounded-md px-2.5 py-1.5 text-left transition-colors hover:bg-surface-raised",
                isOpen && "bg-surface-raised",
              )}
            >
	              <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
                <Icon className={cn("h-3.5 w-3.5 shrink-0", runStatusTextClass(child.status))} />
	                <span className={cn("min-w-0 flex-1 text-wrap-anywhere text-xs text-foreground", child.status === "failed" && "font-medium")}>
                  {displayLabel}
                </span>
	                {child.detail && <span className="text-wrap-anywhere text-[10px] text-foreground-subtle">{child.detail}</span>}
				{child.badge && (
	                  <span className={cn("inline-flex items-center rounded-md border border-border px-1.5 py-0.5 text-[10px] font-medium", runStatusBadgeClass(badgeToneForLabel(child.badge, child.status)))}>
                    {child.badge}
                  </span>
                )}
                {duration && (
                  <span className="shrink-0 rounded bg-surface-raised px-1 py-0.5 text-[10px] font-mono text-foreground-subtle">
                    {duration}
                  </span>
                )}
                <span className="shrink-0 text-[10px] text-foreground-subtle">{relative}</span>
              </div>
            </button>
			{isOpen && <div id={detailPanelId}><RawEventsDetail events={child.events} lookups={lookups} /></div>}
          </div>
        );
      })}
    </div>
  );
}
