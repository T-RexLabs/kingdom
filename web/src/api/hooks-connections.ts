import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "./client";
import { refetchOnMountQueryOptions } from "./query-freshness";
import type { components } from "./schema";

type Connection = components["schemas"]["Connection"];

export const connectionKeys = {
  list: () => ["connections"] as const,
};

/**
 * The operator's configured external connections.
 *
 * This entry is the only copy of the list. Every surface reads it — the
 * Connections page that manages them and the issue picker that merely asks
 * "is anything connected?" — and every mutation invalidates it, so no surface
 * can hold a private view that disagrees with another.
 *
 * Mounting re-reads on top of that, because a connection's health can regress
 * on GitHub's side without any local mutation to invalidate against.
 */
export function useConnections(options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: connectionKeys.list(),
    queryFn: () => apiFetch<{ items: Connection[] }>("/connections"),
    enabled: options?.enabled ?? true,
    ...refetchOnMountQueryOptions,
  });
}
