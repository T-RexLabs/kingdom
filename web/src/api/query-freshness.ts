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

export const searchDrivenCollectionQueryOptions = {
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
