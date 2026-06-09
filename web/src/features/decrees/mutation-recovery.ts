import { ApiError } from "@/api/client";

export function isAmbiguousMutationFailure(error: unknown): boolean {
	if (!(error instanceof ApiError)) {
		return true;
	}
	return error.retryable || error.status === 408 || error.status >= 500;
}
