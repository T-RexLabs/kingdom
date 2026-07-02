export const COMPLETION_RECEIPT_CONFLICT_FAILURE_TEXT = "Kingdom detected conflicting completion receipts for this step and could not safely accept the result.";
export const PROVIDER_INCOMPLETE_TURN_FAILURE_TEXT = "Kingdom could not confirm a clean finish for this step.";

export function isCompletionReceiptConflictDetail(value: unknown): boolean {
	return typeof value === "string" && value.toLowerCase().includes("completion_receipt_conflict");
}

export function operatorFacingExecutionFailureText(failureCode: unknown, ...details: unknown[]): string | null {
	const code = typeof failureCode === "string" ? failureCode.trim().toLowerCase() : "";
	if (code === "provider_incomplete_turn") {
		return details.some(isCompletionReceiptConflictDetail)
			? COMPLETION_RECEIPT_CONFLICT_FAILURE_TEXT
			: PROVIDER_INCOMPLETE_TURN_FAILURE_TEXT;
	}
	if (code === "provider_cancelled") {
		return "This step was stopped before it could finish.";
	}
	for (const detail of details) {
		if (typeof detail !== "string") {
			continue;
		}
		const trimmed = detail.trim();
		if (trimmed) {
			return trimmed;
		}
	}
	return null;
}
