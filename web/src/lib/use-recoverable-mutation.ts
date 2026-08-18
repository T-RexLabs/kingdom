import { useCallback, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import { newIdempotencyKey } from "./idempotency";

type RetainedRequest<TRequest> = {
  identity: string;
  key: string;
  request: TRequest;
};

export function shouldRetainMutationRequest(error: unknown): boolean {
  if (!(error instanceof ApiError)) return true;
  if (error.code === "idempotency_pending") return true;
  if (error.code === "unknown" && (error.status === 408 || error.status >= 500)) return true;
  return error.code === "internal_error" && error.correlationId === "idempotency-middleware";
}

export function useRecoverableMutation<TRequest, TResult>(
  scope: string,
  mutation: (request: TRequest, key: string) => Promise<TResult>,
  identity = scope,
) {
  const retainedRef = useRef<RetainedRequest<TRequest> | null>(null);
  const sequenceRef = useRef(0);
  const [retainedRequest, setRetainedRequest] = useState<RetainedRequest<TRequest> | null>(null);

  const mutateAsync = useCallback(async (request: TRequest): Promise<TResult> => {
    const sequence = ++sequenceRef.current;
    const attempted = retainedRef.current?.identity === identity ? retainedRef.current : {
      identity,
      key: newIdempotencyKey(scope),
      request,
    };

    try {
      const result = await mutation(attempted.request, attempted.key);
      if (sequence === sequenceRef.current && retainedRef.current === attempted) {
        retainedRef.current = null;
        setRetainedRequest(null);
      }
      return result;
    } catch (error) {
      if (sequence !== sequenceRef.current) throw error;
      if (shouldRetainMutationRequest(error)) {
        retainedRef.current = attempted;
        setRetainedRequest(attempted);
      } else if (retainedRef.current === attempted) {
        retainedRef.current = null;
        setRetainedRequest(null);
      }
      throw error;
    }
  }, [identity, mutation, scope]);

  const retry = useCallback(() => {
    const retained = retainedRef.current;
    if (!retained || retained.identity !== identity) {
      return Promise.reject(new Error("mutation recovery request is not available"));
    }
    return mutateAsync(retained.request);
  }, [identity, mutateAsync]);

  return {
    hasPendingRecovery: retainedRequest?.identity === identity,
    mutateAsync,
    pendingRequest: retainedRequest?.identity === identity ? retainedRequest.request : null,
    retry,
  };
}
