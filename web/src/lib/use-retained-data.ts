import { useState } from "react";

interface RetainedDataOptions<TIdentity, TData> {
  identity: TIdentity;
  retentionScope?: string;
  data: TData | undefined;
  isPending: boolean;
  isFetching: boolean;
}

interface RetainedDataState<TData> {
  displayData: TData | undefined;
  isInitialLoading: boolean;
  isRetainingPreviousData: boolean;
}

/**
 * Keep the last successful payload visible while a new keyed request is still in flight.
 * Once the new request settles without data, the retained payload is dropped so the UI
 * does not misrepresent the active selection. An optional retentionScope prevents
 * carrying values across boundaries such as analytics date ranges.
 */
export function useRetainedData<TIdentity, TData>({
  identity,
  retentionScope,
  data,
  isPending,
  isFetching,
}: RetainedDataOptions<TIdentity, TData>): RetainedDataState<TData> {
  const [lastSuccessful, setLastSuccessful] = useState<{ identity: TIdentity; retentionScope?: string; data: TData } | null>(null);

  if (
    data !== undefined &&
    (lastSuccessful === null ||
      !Object.is(lastSuccessful.identity, identity) ||
      lastSuccessful.retentionScope !== retentionScope ||
      !Object.is(lastSuccessful.data, data))
  ) {
    setLastSuccessful({ identity, retentionScope, data });
  }

  const hasRetainedMatch = lastSuccessful !== null && !Object.is(lastSuccessful.identity, identity);
  const shouldRetainPrevious = data === undefined && isFetching && hasRetainedMatch && lastSuccessful.retentionScope === retentionScope;
  const displayData = shouldRetainPrevious ? lastSuccessful.data : data;

  return {
    displayData,
    isInitialLoading: displayData === undefined && (isPending || isFetching),
    isRetainingPreviousData: shouldRetainPrevious,
  };
}
