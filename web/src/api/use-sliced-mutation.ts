import { useMutation } from "@tanstack/react-query";
import { useRef } from "react";
import { usePlanningMutationJournal } from "@/features/work-orders/use-planning-mutation-journal";
import type { OutcomeOperation } from "@/lib/mutation-outcome";

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
 * Serialize field-scoped saves and reconcile only the latest invocation.
 * A superseding edit first resolves any uncertain earlier request with its
 * original body/key. Aborting a fetch does not prove that a PATCH was not
 * accepted, so the durable journal owns recovery across route changes/reloads.
 * Canonical responses are applied by the caller to its own Query fields only.
 */
export function useSlicedMutation<TVars, TResponse>(
  mutationFn: (vars: TVars, signal: AbortSignal) => Promise<TResponse>,
  onLatestSuccess: (result: TResponse) => void,
  identity = "default",
  operation?: OutcomeOperation,
) {
  const queues = useRef(new Map<string, { generation: number; tail: Promise<void> }>());
  const recovery = usePlanningMutationJournal<TVars, TResponse>(
    "planning-field", (vars) => mutationFn(vars, new AbortController().signal), identity, operation,
  );

  const mutation = useMutation<TResponse | SupersededResult, Error, TVars>({
    mutationFn: async (vars: TVars) => {
      let queue = queues.current.get(identity);
      if (!queue) { queue = { generation: 0, tail: Promise.resolve() }; queues.current.set(identity, queue); }
      const myGen = ++queue.generation;
      const previous = queue.tail;
      let release!: () => void;
      queue.tail = new Promise<void>((resolve) => { release = resolve; });
      await previous;
      try {
        const retained = recovery.getPendingRequest();
        const attempts = retained && retained !== vars ? [retained, vars] : [vars];
        let result: TResponse | undefined;
        for (const attemptedVars of attempts) {
          try {
            const key = (attemptedVars as { key?: string }).key;
            result = await recovery.mutateAsync(attemptedVars, key);
          } catch (err) {
            if (myGen !== queue.generation) return SUPERSEDED;
            throw err;
          }
          if (myGen !== queue.generation) return SUPERSEDED;
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
    const retained = recovery.getPendingRequest();
    return retained ? mutation.mutateAsync(retained) : Promise.reject(new Error("The recovery request is no longer available."));
  };

  return {
    ...mutation,
    hasPendingRecovery: recovery.hasPendingRecovery,
    pendingVariables: recovery.pendingRequest,
    pendingRequestKey: recovery.pendingKey,
    recoveryBlocked: recovery.recoveryBlocked,
    retryRecovery,
  };
}
