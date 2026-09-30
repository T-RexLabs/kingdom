import {
  skipToken,
  useMutation,
  useQuery,
  useQueryClient,
  type QueryKey,
} from "@tanstack/react-query";
import { apiFetch, shouldRetryApiQuery } from "./client";
import type { AcpRunActivityResponse } from "./hooks-types";
import { mergeActivityPage, type ActivityHistory } from "./activity-history";

export function useActivityHistory(
  path: string | undefined,
  queryKey: QueryKey,
  options?: { enabled?: boolean; refetchIntervalMs?: number | false },
) {
  const qc = useQueryClient();
  const historyKey = [...queryKey, "loaded-ranges"];
  const history = useQuery<ActivityHistory>({
    queryKey: historyKey,
    queryFn: skipToken,
    enabled: false,
    staleTime: Infinity,
  });
  const enabled = Boolean(path) && (options?.enabled ?? true);
  const query = useQuery({
    queryKey,
    enabled,
    queryFn: path
      ? async ({ signal }) => {
          const current = qc.getQueryData<ActivityHistory>(historyKey);
          // Catch up a bounded forward delta page before opening a newer range.
          // Earlier history is loaded only by an explicit operator request.
          const cursor =
            current?.gaps.find((gap) => gap.direction === "later")?.cursor ??
            current?.resumeCursor;
          const page = await apiFetch<AcpRunActivityResponse>(path, {
            params: { cursor, limit: 50 },
            signal,
          });
          if (signal.aborted) throw new DOMException("Aborted", "AbortError");
          qc.setQueryData<ActivityHistory>(historyKey, (latest) =>
            mergeActivityPage(latest, page, cursor),
          );
          return page;
        }
      : skipToken,
    staleTime: 0,
    refetchOnWindowFocus: false,
    refetchInterval: options?.refetchIntervalMs ?? false,
    retry: (count, error) => shouldRetryApiQuery(count, error, 2),
  });
  const load = useMutation({
    mutationFn: async (request: {
      path: string;
      cursor: string;
      historyKey: QueryKey;
    }) =>
      apiFetch<AcpRunActivityResponse>(request.path, {
        params: { cursor: request.cursor, limit: 50 },
      }),
    onSuccess: (page, request) =>
      qc.setQueryData<ActivityHistory>(request.historyKey, (current) =>
        mergeActivityPage(current, page, request.cursor),
      ),
  });
  return {
    ...query,
    data: history.data,
    gaps: history.data?.gaps ?? [],
    isLoadingMore: load.isPending,
    loadError: load.error,
    loadMore: (cursor: string) =>
      path
        ? load.mutateAsync({ path, cursor, historyKey })
        : Promise.resolve(undefined),
  };
}
