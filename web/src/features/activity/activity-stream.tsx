import { useState } from "react";
import { CheckCircle2, ChevronDown, Circle, CircleDot, CircleSlash, ExternalLink } from "lucide-react";
import { ThemeIcon } from "@/theme";
import { cn } from "@/lib/utils";
import { useTextReveal } from "@/lib/use-text-reveal";
import type { ActivityState, ActivitySourceEntry, ActivityThoughtEntry, TodoList, TodoStatus } from "./activity-view";

// ActivityLiveStream renders the in-flight activity: the agent's reasoning revealed
// at a natural pace, with the current action (thinking / tool call) anchored beneath
// it and the themed planner icon. Renders nothing when there is no live activity to show.
export function ActivityLiveStream({
  state,
  isStreaming,
}: {
  state: ActivityState;
  isStreaming: boolean;
}) {
  // Drive the reveal off `isStreaming` directly so the first reasoning block types
  // in from zero, while replayed/reconnected backlog appears instantly.
  const reveal = useTextReveal(state.liveThoughtText, isStreaming);
  const show = isStreaming && Boolean(state.currentEvent || reveal.displayText.trim());
  if (!show) {
    return null;
  }
  return (
    <div className="space-y-2 text-left">
      {reveal.displayText.trim() ? (
        <p className="whitespace-pre-wrap text-xs leading-relaxed text-foreground-subtle/80 text-wrap-anywhere">
          {reveal.displayText}
        </p>
      ) : null}
      {state.currentEvent ? (
        <div className="flex items-center justify-between gap-3 text-xs font-medium text-foreground-subtle/90">
          <span className="truncate">{state.currentEvent.label}</span>
          <ThemeIcon slot="role.planning-assistant" size={14} className="shrink-0 motion-safe:animate-pulse text-accent/75" />
        </div>
      ) : null}
    </div>
  );
}

// ActivityThoughts renders the settled reasoning trail behind a disclosure.
export function ActivityThoughts({ thoughts }: { thoughts: ActivityThoughtEntry[] }) {
  const [open, setOpen] = useState(false);
  if (thoughts.length === 0) {
    return null;
  }
  return (
    <ActivityDisclosure title="Thoughts" count={thoughts.length} open={open} onToggle={() => setOpen((current) => !current)}>
      <div className="space-y-3 text-xs leading-relaxed text-foreground-subtle/78">
        {thoughts.map((thought) => (
          <p key={thought.id} className="whitespace-pre-wrap text-wrap-anywhere">
            {thought.detail}
          </p>
        ))}
      </div>
    </ActivityDisclosure>
  );
}

// ActivityChecklist renders the agent's current todo list (from its latest todowrite)
// as a foldable checklist with live progress, collapsed by default like Thoughts.
export function ActivityChecklist({ list }: { list: TodoList }) {
  const [open, setOpen] = useState(false);
  return (
    <ActivityDisclosure title="Checklist" count={`${list.done}/${list.total}`} open={open} onToggle={() => setOpen((current) => !current)}>
      <ul className="space-y-1.5 text-xs leading-relaxed">
        {list.items.map((item, index) => {
          const struck = item.status === "completed" || item.status === "cancelled";
          return (
            <li key={index} className="flex items-start gap-2">
              <TodoStatusIcon status={item.status} />
              <span className={cn("min-w-0 text-wrap-anywhere", struck ? "text-foreground-subtle/55 line-through" : "text-foreground-subtle/85")}>
                {item.content}
              </span>
            </li>
          );
        })}
      </ul>
    </ActivityDisclosure>
  );
}

function TodoStatusIcon({ status }: { status: TodoStatus }) {
  const base = "mt-0.5 h-3.5 w-3.5 shrink-0";
  switch (status) {
    case "completed":
      return <CheckCircle2 className={cn(base, "text-accent")} aria-label="completed" />;
    case "in_progress":
      return <CircleDot className={cn(base, "animate-pulse text-accent/80")} aria-label="in progress" />;
    case "cancelled":
      return <CircleSlash className={cn(base, "text-foreground-subtle/50")} aria-label="cancelled" />;
    default:
      return <Circle className={cn(base, "text-foreground-subtle/40")} aria-label="pending" />;
  }
}

// ActivitySources renders fetched web sources as chips. A short handful read cleanly
// inline; 5+ collapse behind a disclosure so a long research burst stays compact.
export function ActivitySources({ sources }: { sources: ActivitySourceEntry[] }) {
  const [open, setOpen] = useState(false);
  if (sources.length === 0) {
    return null;
  }
  const chips = sources.map((source) => (
    <a
      key={source.id}
      href={source.url}
      target="_blank"
      rel="noreferrer"
      title={source.url}
      className="inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-border/40 bg-background/10 px-2.5 py-1 text-[11px] text-foreground-subtle/85 transition-colors hover:border-accent/40 hover:text-foreground"
    >
      <ExternalLink className="h-3 w-3 shrink-0 opacity-70" />
      <span className="truncate">{sourceHostLabel(source.url)}</span>
    </a>
  ));
  if (sources.length >= 5) {
    return (
      <ActivityDisclosure title="Sources" count={sources.length} open={open} onToggle={() => setOpen((current) => !current)}>
        <div className="flex flex-wrap items-center gap-1.5">{chips}</div>
      </ActivityDisclosure>
    );
  }
  return (
    <div className="space-y-2">
      <span className="block text-[11px] font-medium text-foreground-subtle/82">Sources</span>
      <div className="flex flex-wrap items-center gap-1.5">{chips}</div>
    </div>
  );
}

export function ActivityDisclosure({
  title,
  count,
  open,
  onToggle,
  children,
}: {
  title: string;
  count: number | string;
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

// Compact, clickable label for a source chip: the hostname (sans leading "www."),
// falling back to the raw URL when it can't be parsed.
function sourceHostLabel(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}
