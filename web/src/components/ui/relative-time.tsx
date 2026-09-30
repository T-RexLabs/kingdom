import { formatTimestampAt } from "@/lib/utils";
import { useRelativeTimeNow } from "@/lib/relative-time-store";
import { timestampDescription } from "@/lib/date-time";

export function RelativeTime({ iso }: { iso: string }) {
  const nowMs = useRelativeTimeNow();
  return <time dateTime={iso} title={timestampDescription(iso)}>{formatTimestampAt(iso, nowMs)}</time>;
}
