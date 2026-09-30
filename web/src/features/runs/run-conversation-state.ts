import type { RunExecutionDetail } from "@/api/hooks";

export function hasSettledResponse(
  detail?: RunExecutionDetail | null,
): detail is RunExecutionDetail & { output_text: string } {
  return (
    !!detail?.output_text &&
    detail.output_status === "settled" &&
    ["succeeded", "failed", "cancelled"].includes(detail.settlement_status)
  );
}
