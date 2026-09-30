import { skipToken, useQuery } from "@tanstack/react-query";
import { ApiError, apiFetch, shouldRetryApiQuery } from "./client";

/** Immutable prompt bytes are loaded only while the selected attempt is inspected. */
export function useRunExecutionInstructions(
  runId: string,
  attemptId: string | null,
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["runs", runId, "executions", "instructions", attemptId],
    queryFn:
      enabled && attemptId
        ? ({ signal }) =>
            apiFetch<string>(
              `/runs/${encodeURIComponent(runId)}/executions/${encodeURIComponent(attemptId)}/instructions`,
              { responseType: "text", signal },
            )
        : skipToken,
    staleTime: Infinity,
    gcTime: 0,
    retry: (count, error) =>
      !(error instanceof ApiError && error.status === 404) &&
      shouldRetryApiQuery(count, error, 2),
    refetchOnWindowFocus: false,
  });
}
