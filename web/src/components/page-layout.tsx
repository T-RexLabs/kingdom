import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function PageHeader({ children, className }: { children: ReactNode; className?: string }) {
  return <header className={cn("flex h-[76px] items-center border-b border-border px-page", className)}>{children}</header>;
}

export function PageContent({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("flex-1 min-h-0 overflow-auto px-page py-4", className)}>{children}</div>;
}
