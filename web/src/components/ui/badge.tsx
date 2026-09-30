import { cn, getStatusBg } from "@/lib/utils";
import type { HTMLAttributes } from "react";

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: "default" | "status";
  status?: string;
}

export function Badge({ className, variant = "default", status, children, ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        "ui-badge inline-flex items-center rounded-md px-2.5 py-1 text-xs font-medium",
        variant === "status" && status ? getStatusBg(status) : "bg-surface-raised text-foreground-muted",
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
}
