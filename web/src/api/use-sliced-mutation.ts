import { useMutation } from "@tanstack/react-query";
import { useRef } from "react";

/**
 * Sentinel returned by superseded mutation invocations. mutationFn
 * resolves with this value (rather than throwing) so the hook's
 * `error` slot stays clean - without it, a transport-level abort
 * would surface as `AbortError` on the inline retry chip every
 * time a newer request superseded an older one.
 */
export const SUPERSEDED = Symbol("planner-mutation-superseded");
export type SupersededResult = typeof SUPERSEDED;

/**
 * Latest-request-wins sequencing for slice-scoped planner mutations.
 *
 * Field-scoped onSuccess reconciliation fixes the cross-slice clobber class.
 * This adds the same-slice ordering guarantee: when two title (or two model)
 * PATCHes are in flight, only the latest one's response is applied. Older
 * in-flight requests are aborted at the transport layer AND skipped at the
 * application layer via a generation token so an older response that's already
 * on the wire can't overwrite a newer one.
 *
 * Returning SUPERSEDED instead of throwing AbortError keeps the mutation hook's
 * `error` state clean - superseded requests are not failures from the user's
 * perspective.
 */
export function useSlicedMutation<TVars, TResponse>(
  mutationFn: (vars: TVars, signal: AbortSignal) => Promise<TResponse>,
  onLatestSuccess: (result: TResponse) => void,
) {
  const abortRef = useRef<AbortController | null>(null);
  const genRef = useRef(0);

  return useMutation<TResponse | SupersededResult, Error, TVars>({
    mutationFn: async (vars: TVars) => {
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;
      const myGen = ++genRef.current;
      try {
        const result = await mutationFn(vars, controller.signal);
        if (myGen !== genRef.current) {
          return SUPERSEDED;
        }
        return result;
      } catch (err) {
        if (myGen !== genRef.current || (err instanceof Error && err.name === "AbortError")) {
          return SUPERSEDED;
        }
        throw err;
      }
    },
    onSuccess: (result) => {
      if (result === SUPERSEDED) {
        return;
      }
      onLatestSuccess(result as TResponse);
    },
  });
}
