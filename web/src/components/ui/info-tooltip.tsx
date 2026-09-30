import { CircleHelp } from "lucide-react";
import { useCallback, useLayoutEffect, useRef, useState, useId, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { resolveOverlayContainer, tooltipZIndexForOwner } from "@/lib/overlay-system";
import { cn } from "@/lib/utils";

export function InfoTooltip({
  label,
  message,
  icon,
  className,
  tooltipClassName,
}: {
  label: string;
  message: string;
  icon?: ReactNode;
  className?: string;
  tooltipClassName?: string;
}) {
  const tooltipID = useId();
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const tooltipRef = useRef<HTMLSpanElement | null>(null);
  const [open, setOpen] = useState(false);
  const [position, setPosition] = useState<{ top: number; left: number; maxWidth: number; zIndex: number }>({
    top: 0,
    left: 0,
    maxWidth: 320,
    zIndex: 120,
  });

  const updatePosition = useCallback(() => {
    if (!triggerRef.current || !tooltipRef.current || typeof window === "undefined") {
      return;
    }

    const triggerRect = triggerRef.current.getBoundingClientRect();
    const tooltipRect = tooltipRef.current.getBoundingClientRect();

    const viewportPadding = 12;
    const offset = 8;
    const maxWidth = Math.max(220, Math.min(512, window.innerWidth - viewportPadding * 2));

    let left = triggerRect.left + triggerRect.width / 2 - tooltipRect.width / 2;
    if (left < viewportPadding) {
      left = viewportPadding;
    }
    if (left + tooltipRect.width > window.innerWidth - viewportPadding) {
      left = Math.max(viewportPadding, window.innerWidth - viewportPadding - tooltipRect.width);
    }

    let top = triggerRect.bottom + offset;
    if (top + tooltipRect.height > window.innerHeight - viewportPadding) {
      top = triggerRect.top - tooltipRect.height - offset;
    }
    if (top < viewportPadding) {
      top = viewportPadding;
    }

    setPosition({ top, left, maxWidth, zIndex: tooltipZIndexForOwner(triggerRef.current) });
  }, []);

  useLayoutEffect(() => {
    if (!open) {
      return;
    }

    updatePosition();

    const onViewportChange = () => updatePosition();
    window.addEventListener("resize", onViewportChange);
    window.addEventListener("scroll", onViewportChange, true);
    return () => {
      window.removeEventListener("resize", onViewportChange);
      window.removeEventListener("scroll", onViewportChange, true);
    };
  }, [open, updatePosition]);

  return (
    <span className={cn("group relative inline-flex", className)}>
      <button
        ref={triggerRef}
        type="button"
        className="inline-flex items-center text-foreground-subtle hover:text-foreground-muted"
        aria-label={label}
        aria-describedby={open ? tooltipID : undefined}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
      >
        {icon ?? <CircleHelp className="h-3.5 w-3.5" />}
      </button>
      {open && typeof document !== "undefined" &&
        createPortal(
          <span
            ref={tooltipRef}
            id={tooltipID}
            role="tooltip"
            className={cn(
              "ui-tooltip pointer-events-none fixed z-[120] rounded-md border border-border bg-surface-overlay px-2 py-1 text-[11px] leading-snug text-foreground shadow-lg",
              tooltipClassName,
            )}
            style={{ top: position.top, left: position.left, maxWidth: position.maxWidth, zIndex: position.zIndex }}
          >
            {message}
          </span>,
          resolveOverlayContainer() ?? document.body,
        )}
    </span>
  );
}
