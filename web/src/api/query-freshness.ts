export const manualRefetchQueryOptions = {
  staleTime: Number.POSITIVE_INFINITY,
  refetchOnMount: false,
  refetchOnReconnect: false,
  refetchOnWindowFocus: false,
} as const;

export const streamOwnedCollectionQueryOptions = {
	staleTime: Number.POSITIVE_INFINITY,
	refetchOnMount: true,
	refetchOnReconnect: true,
	refetchOnWindowFocus: false,
} as const;

/**
 * Re-read whenever a surface mounts.
 *
 * For collections whose contents can change between visits without this tab
 * hearing about it — a filtered server-side search, or a resource another page
 * mutates. Mutations should still invalidate the key; this only guarantees that
 * arriving at a surface never shows it something the server has moved past.
 */
export const refetchOnMountQueryOptions = {
	staleTime: 0,
	refetchOnMount: true,
	refetchOnReconnect: true,
	refetchOnWindowFocus: false,
} as const;

export const pollingRefetchQueryOptions = {
  refetchOnMount: true,
  refetchOnReconnect: true,
  refetchOnWindowFocus: true,
} as const;
