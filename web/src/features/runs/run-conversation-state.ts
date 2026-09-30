import type { RunExecutionDetail } from "@/api/hooks";
import type { ActivityItem } from "@/features/activity/activity-view";

export function hasSettledResponse(
  detail?: RunExecutionDetail | null,
): detail is RunExecutionDetail & { output_text: string } {
  return (
    !!detail?.output_text &&
    detail.output_status === "settled" &&
    ["succeeded", "failed", "cancelled"].includes(detail.settlement_status)
  );
}

// A preview may be incomplete. Only complete, identical text from this exact
// invocation can make the canonical copy a disclosure; observations stay intact.
export function repeatsSettledResponse(
  detail: RunExecutionDetail | null | undefined,
  items: ActivityItem[],
): boolean {
  if (!hasSettledResponse(detail) || !detail.provider_invocation_id)
    return false;
  const message = [...items]
    .reverse()
    .find(
      (item) =>
        item.kind === "message" &&
        item.invocationId === detail.provider_invocation_id,
    );
  const content = message?.messageContent;
  return (
    content?.availability === "available" &&
    content.preview === detail.output_text &&
    content.byte_length === new TextEncoder().encode(content.preview).length
  );
}
