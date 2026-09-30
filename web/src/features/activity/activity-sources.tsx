import { useState } from "react";
import { ChevronDown, ExternalLink } from "lucide-react";
import type { ActivitySourceEntry } from "./activity-view";

// ActivitySources renders fetched web sources as chips. A short handful read cleanly
// inline; 5+ collapse behind a disclosure so a long research burst stays compact.
export function ActivitySources({
  sources,
}: {
  sources: ActivitySourceEntry[];
}) {
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
      className="ui-source-chip inline-flex max-w-[14rem] items-center gap-1 rounded-full border border-border/40 bg-background/10 px-2.5 py-1 text-[11px] text-foreground-subtle/85 transition-colors hover:border-accent/40 hover:text-foreground"
    >
      <ExternalLink className="h-3 w-3 shrink-0 opacity-70" />
      <span className="truncate">{sourceHostLabel(source.url)}</span>
    </a>
  ));
  if (sources.length >= 5) {
    return (
      <ActivityDisclosure
        title="Sources"
        count={sources.length}
        open={open}
        onToggle={() => setOpen((current) => !current)}
      >
        <div className="flex flex-wrap items-center gap-1.5">{chips}</div>
      </ActivityDisclosure>
    );
  }
  return (
    <div className="space-y-2">
      <span className="block text-[11px] font-medium text-foreground-subtle/82">
        Sources
      </span>
      <div className="flex flex-wrap items-center gap-1.5">{chips}</div>
    </div>
  );
}

function ActivityDisclosure({
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
    <div className="ui-inset-panel overflow-hidden rounded-xl border border-border/35 bg-background/6">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left text-[11px] font-medium text-foreground-subtle/82 transition-colors hover:text-foreground-subtle"
        aria-expanded={open}
      >
        <span>{title}</span>
        <span className="flex items-center gap-2">
          <span>{count}</span>
          <ChevronDown
            className={`h-3.5 w-3.5 transition-transform ${open ? "rotate-180" : ""}`}
          />
        </span>
      </button>
      {open ? (
        <div className="border-t border-border/35 p-3">{children}</div>
      ) : null}
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
