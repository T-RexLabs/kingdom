import { createPortal } from "react-dom";
import { CheckCircle2, Info, X, XCircle } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { OVERLAY_Z_INDEX, resolveOverlayContainer } from "@/lib/overlay-system";
import { useNotifications, type Notification } from "./notification-context";

export function NotificationStack() {
  const { notifications, removeNotification } = useNotifications();
  const items = Array.from(notifications.values());
  const overlayContainer = resolveOverlayContainer();

  if (items.length === 0 || !overlayContainer) return null;

  return createPortal(
    <div className="fixed right-4 bottom-4 flex flex-col gap-3" style={{ zIndex: OVERLAY_Z_INDEX.notification }} role="log" aria-label="Notifications">
      {items.map((n) => (
        <NotificationCard key={n.id} notification={n} onRemove={removeNotification} />
      ))}
    </div>,
    overlayContainer,
  );
}

function NotificationCard({ notification, onRemove }: { notification: Notification; onRemove: (id: string) => void }) {
  const [phase, setPhase] = useState<"entering" | "visible" | "exiting">("entering");
  const autoDismissRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const animEndRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const dismissedRef = useRef(false);

  const startDismiss = useCallback(() => {
    if (dismissedRef.current) {
      return;
    }
    dismissedRef.current = true;
    notification.onDismiss?.();
    setPhase("exiting");
    animEndRef.current = setTimeout(() => onRemove(notification.id), 260);
  }, [notification, onRemove]);

  // Transition from entering to visible after slide-in completes
  useEffect(() => {
    if (phase === "entering") {
      const t = setTimeout(() => setPhase("visible"), 310);
      return () => clearTimeout(t);
    }
  }, [phase]);

  // Auto-dismiss when the notification specifies a timeout
  useEffect(() => {
    if (notification.autoDismissMs && notification.autoDismissMs > 0 && phase !== "exiting") {
      clearTimeout(autoDismissRef.current);
      autoDismissRef.current = setTimeout(startDismiss, notification.autoDismissMs);
    }
    return () => {
      clearTimeout(autoDismissRef.current);
    };
  }, [notification.autoDismissMs, phase, startDismiss]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      clearTimeout(animEndRef.current);
    };
  }, []);

  const animClass = phase === "entering" ? "animate-notification-in" : phase === "exiting" ? "animate-notification-out" : "";

  return (
    <div
      className={`w-80 rounded-lg border border-border/80 bg-surface-raised shadow-xl ${animClass}`}
      role="status"
      aria-live="polite"
    >
      <div className="flex items-start gap-3 px-3.5 py-3">
        <StatusIcon status={notification.status} />
        <div className="min-w-0 flex-1">
          <p className="text-sm font-medium text-foreground">{notification.title}</p>
          {notification.message ? (
            <p className="mt-0.5 truncate text-xs text-foreground-muted">{notification.message}</p>
          ) : null}
        </div>
        <div className="flex shrink-0 items-center gap-1.5">
          {notification.action ? (
            <button
              type="button"
              className="text-xs font-medium text-gold transition-colors hover:text-gold-bright"
              onClick={() => {
                notification.action?.onClick();
                startDismiss();
              }}
            >
              {notification.action.label}
            </button>
          ) : null}
          <button
            type="button"
            aria-label="Dismiss notification"
            onClick={() => startDismiss()}
            className="rounded p-0.5 text-foreground-subtle transition-colors hover:text-foreground"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
    </div>
  );
}

function StatusIcon({ status }: { status: Notification["status"] }) {
  switch (status) {
    case "loading":
      return <div className="mt-0.5 h-4 w-4 shrink-0 animate-spin rounded-full border-2 border-gold border-t-transparent" />;
    case "success":
      return <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-status-succeeded" />;
    case "error":
      return <XCircle className="mt-0.5 h-4 w-4 shrink-0 text-status-failed" />;
    case "info":
      return <Info className="mt-0.5 h-4 w-4 shrink-0 text-status-running" />;
  }
}
