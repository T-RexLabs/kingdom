import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { useCallback, useSyncExternalStore } from "react";
import { newIdempotencyKey } from "@/lib/idempotency";
import { PlanningMutationJournal, type OutcomeOperationSelector, type RequestBinding } from "./planning-mutation-journal";

const journals = new WeakMap<QueryClient, PlanningMutationJournal>();
export function planningMutationJournal(client: QueryClient) {
  let journal = journals.get(client);
  if (!journal) { journal = new PlanningMutationJournal(); journals.set(client, journal); }
  return journal;
}
export function usePlanningMutationJournal<TRequest, TResult>(scope: string, mutation: (body: TRequest, key: string) => Promise<TResult>, identity = scope, operation?: OutcomeOperationSelector<TRequest>, onRecovered?: (result: TResult, request: TRequest) => void) {
  const journal = planningMutationJournal(useQueryClient());
  const id = JSON.stringify([scope, identity]);
  const subscribe = useCallback((listener: () => void) => journal.subscribe(id, listener), [journal, id]);
  const snapshot = useCallback(() => journal.get<TRequest>(id), [journal, id]);
  const pending = useSyncExternalStore(subscribe, snapshot, snapshot);
  const mutateAsync = useCallback((body: TRequest, key?: string, origin?: RequestBinding) => journal.execute(id, body, key ?? newIdempotencyKey(scope), mutation, operation, origin, onRecovered), [journal, id, scope, mutation, operation, onRecovered]);
  const retry = useCallback(() => {
    const request = journal.get<TRequest>(id);
    return request ? journal.execute(id, request.body, request.key, mutation, operation, undefined, onRecovered) : Promise.reject(new Error("The recovery request is no longer available."));
  }, [journal, id, mutation, operation, onRecovered]);
  return { mutateAsync, retry, getPendingRequest: () => journal.get<TRequest>(id)?.body ?? null, pendingRequest: pending?.body ?? null, pendingKey: pending?.key, hasPendingRecovery: Boolean(pending), recoveryBlocked: pending ? !journal.canReplay(pending) : false };
}
