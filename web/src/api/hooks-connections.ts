import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "./client";
import { manualRefetchQueryOptions } from "./query-freshness";
import type { components } from "./schema";

type Connection = components["schemas"]["Connection"];

export const connectionKeys = {
  list: () => ["connections"] as const,
};

/**
 * The operator's configured external connections.
 *
 * Refetch is manual because connections only change through the Connections
 * page, which reloads them itself. Sharing one cache entry means a surface
 * that merely asks "is anything connected?" — the issue picker — costs nothing
 * on top of the pages that list them.
 */
export function useConnections(options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: connectionKeys.list(),
    queryFn: () => apiFetch<{ items: Connection[] }>("/connections"),
    enabled: options?.enabled ?? true,
    ...manualRefetchQueryOptions,
  });
}
