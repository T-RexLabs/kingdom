import { useMutation } from "@tanstack/react-query";
import { useRef, useState } from "react";
import { shouldRetainMutationRequest } from "@/lib/use-recoverable-mutation";

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
  identity = "default",
) {
  const genRef = useRef(0);
  const queueRef = useRef<Promise<void>>(Promise.resolve());
  const recoveryRef = useRef<{ identity: string; variables: TVars } | null>(null);
  const [recoveryVariables, setRecoveryVariables] = useState<{ identity: string; variables: TVars } | null>(null);

  const mutation = useMutation<TResponse | SupersededResult, Error, TVars>({
    mutationFn: async (vars: TVars) => {
      const myGen = ++genRef.current;
      const previous = queueRef.current;
      let release!: () => void;
      queueRef.current = new Promise<void>((resolve) => { release = resolve; });
      await previous;
      try {
        const recovery = recoveryRef.current?.identity === identity ? recoveryRef.current.variables : null;
        const attempts = recovery && recovery !== vars ? [recovery, vars] : [vars];
        let result: TResponse | undefined;
        for (const attemptedVars of attempts) {
          try {
            const controller = new AbortController();
            result = await mutationFn(attemptedVars, controller.signal);
            recoveryRef.current = null;
            setRecoveryVariables(null);
          } catch (err) {
            recoveryRef.current = shouldRetainMutationRequest(err) ? { identity, variables: attemptedVars } : null;
            setRecoveryVariables(recoveryRef.current);
            if (myGen !== genRef.current) return SUPERSEDED;
            throw err;
          }
          if (myGen !== genRef.current) return SUPERSEDED;
        }
        return result as TResponse;
      } finally {
        release();
      }
    },
    onSuccess: (result) => {
      if (result === SUPERSEDED) {
        return;
      }
      onLatestSuccess(result as TResponse);
    },
  });

  const retryRecovery = () => {
    const recovery = recoveryRef.current;
    if (!recovery || recovery.identity !== identity) return Promise.reject(new Error("mutation recovery request is not available"));
    return mutation.mutateAsync(recovery.variables);
  };

  return {
    ...mutation,
    hasPendingRecovery: recoveryVariables?.identity === identity,
    pendingVariables: recoveryVariables?.identity === identity ? recoveryVariables.variables : null,
    retryRecovery,
  };
}
