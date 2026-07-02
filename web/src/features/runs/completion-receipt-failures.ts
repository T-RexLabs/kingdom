export const COMPLETION_RECEIPT_CONFLICT_FAILURE_TEXT = "Kingdom detected conflicting completion receipts for this step and could not safely accept the result.";

export function isCompletionReceiptConflictDetail(value: unknown): boolean {
	return typeof value === "string" && value.toLowerCase().includes("completion_receipt_conflict");
}
