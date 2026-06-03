import { useEffect, useRef } from "react";

interface RetainedDataOptions<TIdentity, TData> {
  identity: TIdentity;
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
 * does not misrepresent the active selection.
 */
export function useRetainedData<TIdentity, TData>({
  identity,
  data,
  isPending,
  isFetching,
}: RetainedDataOptions<TIdentity, TData>): RetainedDataState<TData> {
  const lastSuccessfulRef = useRef<{ identity: TIdentity; data: TData } | null>(null);

  useEffect(() => {
    if (data !== undefined) {
      lastSuccessfulRef.current = { identity, data };
    }
  }, [data, identity]);

  const hasRetainedMatch = lastSuccessfulRef.current !== null && !Object.is(lastSuccessfulRef.current.identity, identity);
  const shouldRetainPrevious = data === undefined && isFetching && hasRetainedMatch;
  const displayData = shouldRetainPrevious ? lastSuccessfulRef.current?.data : data;

  return {
    displayData,
    isInitialLoading: displayData === undefined && (isPending || isFetching),
    isRetainingPreviousData: shouldRetainPrevious,
  };
}
