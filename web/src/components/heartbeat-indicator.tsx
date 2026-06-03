import { Heart } from "lucide-react";
import { cn, formatTimestamp, getHeartbeatState } from "@/lib/utils";

/**
 * Compact heartbeat indicator showing a heart icon with relative timestamp.
 * Displays "No heartbeat" when no timestamp is available.
 */
export function HeartbeatIndicator({
  lastHeartbeatAt,
}: {
  lastHeartbeatAt?: string;
}) {
  const state = getHeartbeatState(lastHeartbeatAt);

  const colorClass =
    state === "alive"
      ? "text-status-succeeded"
      : state === "stale"
        ? "text-status-blocked"
        : "text-foreground-subtle";

  return (
    <span className={cn("inline-flex items-center gap-1", colorClass)}>
      <Heart
        className={cn("h-3 w-3", state === "alive" && "animate-pulse")}
      />
      {lastHeartbeatAt ? (
        <span className="text-[10px]">{formatTimestamp(lastHeartbeatAt)}</span>
      ) : (
        <span className="text-[10px]">No heartbeat</span>
      )}
    </span>
  );
}
