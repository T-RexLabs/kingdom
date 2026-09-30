import { ApiError } from "@/api/client";
import type { components, operations } from "@/api/schema";

export type OutcomeOperation = operations["getMutationOutcome"]["parameters"]["query"]["operation"];

type DecodedMutationOutcome =
  | { state: "unresolved" }
  | { state: "succeeded"; value: unknown }
  | { state: "rejected"; error: ApiError };

/** Only a complete original response can release an unresolved mutation guard. */
export function decodeMutationOutcome(outcome: components["schemas"]["MutationOutcome"]): DecodedMutationOutcome {
  const status = outcome.response_status;
  const body = outcome.response_body;
  if (outcome.state !== "completed" || !status || !Number.isInteger(status)
    || status < 200 || status >= 600 || (status >= 300 && status < 400)
    || (status !== 204 && (!body || typeof body !== "object" || Array.isArray(body)))) {
    return { state: "unresolved" };
  }
  if (status >= 400) {
    const error = body?.error as components["schemas"]["ErrorEnvelope"]["error"] | undefined;
    return { state: "rejected", error: new ApiError(status, {
      message: error?.message ?? "The original request was rejected.",
      code: error?.code ?? "unknown",
      category: error?.category ?? "core",
      details: error?.details ?? {},
      correlationId: error?.correlation_id ?? "mutation-outcome",
      retryable: error?.retryable ?? false,
      occurredAt: error?.occurred_at,
      retryAt: error?.retry_at,
    }) };
  }
  return { state: "succeeded", value: status === 204 ? undefined : body };
}
