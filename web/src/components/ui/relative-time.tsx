import { formatTimestampAt } from "@/lib/utils";
import { useRelativeTimeNow } from "@/lib/relative-time-store";

export function RelativeTime({ iso }: { iso: string }) {
  const nowMs = useRelativeTimeNow();
  return <time dateTime={iso}>{formatTimestampAt(iso, nowMs)}</time>;
}
