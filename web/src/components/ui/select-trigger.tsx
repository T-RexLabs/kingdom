import type { ReactNode } from "react";
import { ChevronDown } from "lucide-react";

/** Shared content for plain and searchable choices; labels remain application-owned. */
export function SelectTriggerContent({ children }: { children: ReactNode }) {
  return <span className="flex min-w-0 flex-1 items-center justify-between gap-3">
    <span className="min-w-0 truncate">{children}</span>
    <ChevronDown size={16} aria-hidden="true" className="ui-select-arrow shrink-0 text-foreground-muted transition-transform motion-reduce:transition-none group-aria-expanded:rotate-180" />
  </span>;
}
