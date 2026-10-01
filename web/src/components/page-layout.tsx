import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { LudicrousRegalia } from "./ludicrous-regalia";

export function PageHeader({ children, className }: { children: ReactNode; className?: string }) {
  return <><header className={cn("ui-page-header flex h-12 shrink-0 items-center sm:h-[76px] border-b border-border px-page", className)}>{children}</header><LudicrousRegalia /></>;
}

/**
 * The one scroll viewport of a page.
 *
 * `relative` is load-bearing, not decoration. A scroll viewport only clips the
 * descendants it is a containing block for, so an absolutely positioned
 * descendant whose nearest positioned ancestor sits *above* this element
 * escapes the clip entirely and grows the document's scroll height instead —
 * which is how a second, page-level scrollbar appears next to this one. The
 * `sr-only` utility is exactly such a descendant: it is `position: absolute`,
 * so every visually hidden label inside a page would otherwise leak out.
 */
export function PageContent({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("ui-page-content relative min-h-0 flex-1 overflow-auto px-page py-4", className)}>{children}</div>;
}
