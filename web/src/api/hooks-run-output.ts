import { useInfiniteQuery, useQueryClient } from "@tanstack/react-query";
import { apiFetch, ApiError } from "./client";
import type { components } from "./schema";

type ArtifactsPage = components["schemas"]["RunArtifactsResponse"];
type PublicationPage = components["schemas"]["RunPublicationEntriesResponse"];

export function useRunArtifacts(runId: string) {
  const client = useQueryClient();
  const queryKey = ["runs", runId, "artifacts"] as const;
  const query = useInfiniteQuery({
    queryKey,
    queryFn: ({ pageParam, signal }) =>
      apiFetch<ArtifactsPage>(`/runs/${runId}/artifacts`, {
        signal,
        params: { limit: 50, cursor: pageParam },
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor,
    retry: false,
    refetchOnWindowFocus: false,
  });
  return {
    ...query,
    refresh: () => client.resetQueries({ queryKey, exact: true }),
  };
}

export function useRunPublicationEntries(
  runId: string,
  attemptId: string,
  publication: components["schemas"]["RunExecutionPublication"],
) {
  const client = useQueryClient();
  const queryKey = [
    "runs",
    runId,
    "executions",
    attemptId,
    "publication",
    publication.batch_id,
    publication.entries_revision,
  ] as const;
  const query = useInfiniteQuery({
    queryKey,
    queryFn: ({ pageParam, signal }) =>
      apiFetch<PublicationPage>(
        `/runs/${runId}/executions/${attemptId}/publication/${publication.batch_id}/entries`,
        {
          signal,
          params: { limit: 50, cursor: pageParam },
        },
      ),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor,
    retry: false,
    refetchOnWindowFocus: false,
  });
  return {
    ...query,
    refresh: () => client.resetQueries({ queryKey, exact: true }),
  };
}

export function outputSnapshotChanged(error: unknown): boolean {
  return (
    error instanceof ApiError &&
    error.status === 409 &&
    error.details.refresh_required === true
  );
}
