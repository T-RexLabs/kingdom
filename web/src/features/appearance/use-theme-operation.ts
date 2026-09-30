import { useSyncExternalStore } from "react";
import { ApiError, currentAuthBinding } from "@/api/client";
import { newIdempotencyKey } from "@/lib/idempotency";
import { shouldRetainMutationRequest } from "@/lib/use-recoverable-mutation";
import { themeAPI, themeError, type SharedThemeRequest } from "./theme-api";
import {
  validSharedRequest,
  validStoredAttempt,
  type ThemeAttempt,
} from "./recovery-contract";
export { validStoredAttempt } from "./recovery-contract";

type OperationState = {
  busy: boolean;
  pending: ThemeAttempt | null;
  error: unknown;
  storageWarning: boolean;
  blockedRecovery: boolean;
};
const unresolvedMessage =
  "The original request outcome remains unresolved. No shared change was sent. Keep this draft while the original outcome is verified.";
function isRetryBoundaryError(error: unknown) {
  return (
    error instanceof ApiError &&
    (error.status === 401 ||
      error.correlationId === "api.auth.csrf" ||
      error.code === "idempotency_conflict" ||
      error.code === "idempotency_key_expired")
  );
}
function sameAssets(
  left: Record<string, string>,
  right: Record<string, string>,
) {
  const names = Object.keys(left);
  return (
    names.length === Object.keys(right).length &&
    names.every((name) => left[name] === right[name])
  );
}
export class ThemeOperation {
  private state: OperationState = {
    busy: false,
    pending: null,
    error: null,
    storageWarning: false,
    blockedRecovery: false,
  };
  private listeners = new Set<() => void>();
  private workspace: string;
  constructor(workspace: string) {
    this.workspace = workspace;
    try {
      const raw = sessionStorage.getItem(this.storageKey());
      if (raw) {
        const saved: unknown = JSON.parse(raw);
        if (validStoredAttempt(saved)) this.state.pending = saved;
        else
          this.state = {
            ...this.state,
            blockedRecovery: true,
            error: new Error(
              "The stored recovery record is invalid. Shared theme changes are blocked so an unresolved request cannot be replaced. Keep a draft backup and review the original request.",
            ),
          };
      }
    } catch {
      this.state = {
        ...this.state,
        storageWarning: true,
        blockedRecovery: true,
        error: new Error(
          "Theme recovery storage could not be read. Shared changes are blocked until the original request can be checked.",
        ),
      };
    }
  }
  private storageKey() {
    return `kingdom:theme-operation:v1:${this.workspace}`;
  }
  getSnapshot = () => this.state;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private update(
    state: Pick<OperationState, "busy" | "pending" | "error">,
    requirePersistence = false,
  ) {
    let storageWarning = false;
    try {
      if (state.pending)
        sessionStorage.setItem(
          this.storageKey(),
          JSON.stringify(state.pending),
        );
      else if (!this.state.blockedRecovery)
        sessionStorage.removeItem(this.storageKey());
    } catch {
      storageWarning = true;
      if (requirePersistence) {
        const error = new Error(
          "The recovery request could not be saved. No shared change was sent. Free browser storage and try again.",
        );
        this.state = { ...this.state, busy: false, storageWarning, error };
        this.listeners.forEach((listener) => listener());
        throw error;
      }
    }
    this.state = { ...this.state, ...state, storageWarning };
    this.listeners.forEach((listener) => listener());
  }
  clearError = () => {
    if (!this.state.pending && !this.state.blockedRecovery)
      this.update({ ...this.state, error: null });
  };
  run = async (request: SharedThemeRequest): Promise<unknown> => {
    if (this.state.blockedRecovery)
      throw new Error(
        "Resolve the invalid recovery record before starting another shared change.",
      );
    if (this.state.busy)
      throw new Error("A theme operation is already in progress.");
    if (!validSharedRequest(request))
      throw new Error("The shared theme request is invalid.");
    const retained = this.state.pending;
    if (
      retained &&
      JSON.stringify(retained.request) !== JSON.stringify(request)
    )
      throw new Error(
        "Recover the previous theme operation before starting another.",
      );
    const binding = currentAuthBinding();
    if (!binding)
      throw new Error(
        "Restore the local browser session before changing shared themes.",
      );
    const attempt = retained ?? {
      request: structuredClone(request),
      key: newIdempotencyKey("theme-authoring"),
      origin: window.location.origin,
      authBinding: binding,
    };
    if (
      attempt.origin !== window.location.origin ||
      attempt.authBinding !== binding
    )
      throw new Error(unresolvedMessage);
    this.update({ busy: true, pending: attempt, error: null }, true);
    try {
      const result = await themeAPI.mutate(attempt.request, attempt.key);
      this.update({ busy: false, pending: null, error: null });
      return result;
    } catch (error) {
      this.update({
        busy: false,
        pending:
          shouldRetainMutationRequest(error) ||
          (!!retained && isRetryBoundaryError(error))
            ? attempt
            : null,
        error,
      });
      throw error;
    }
  };
  private async desiredStateMatches(
    request: SharedThemeRequest,
  ): Promise<boolean> {
    const id = request.path.split("/")[2];
    if (request.method === "PUT" && request.path.endsWith("/source")) {
      const expected = request.body as {
        source: string;
        assets: Record<string, string>;
      };
      const current = await themeAPI.source(id);
      return (
        current.source === expected.source &&
        sameAssets(current.assets, expected.assets)
      );
    }
    if (request.method === "DELETE" && request.path.endsWith("/source")) {
      try {
        await themeAPI.source(id);
        return false;
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) return true;
        throw error;
      }
    }
    if (request.verify) {
      await themeAPI.revision(request.verify);
      return true;
    }
    if (request.method === "DELETE")
      return !(await themeAPI.catalog()).items.some((item) => item.id === id);
    return false;
  }
  retry = async () => {
    const pending = this.state.pending;
    if (!pending) throw new Error("No theme operation needs recovery.");
    if (this.state.busy)
      throw new Error("A theme operation is already in progress.");
    if (
      pending.origin === window.location.origin &&
      pending.authBinding === currentAuthBinding()
    )
      return {
        request: pending.request,
        result: await this.run(pending.request),
      };
    // Current desired state cannot prove an older request has finished. Keep
    // its guard until an exact outcome is available; never replay in a renewed
    // browser binding or convert a matching readback into mutation success.
    this.update({ busy: true, pending, error: null });
    try {
      if (pending.origin !== window.location.origin || !currentAuthBinding())
        throw new Error(unresolvedMessage);
      const matches = await this.desiredStateMatches(pending.request);
      throw new Error(
        matches
          ? `The current shared state matches this change, but ${unresolvedMessage[0].toLowerCase()}${unresolvedMessage.slice(1)}`
          : unresolvedMessage,
      );
    } catch (error) {
      this.update({ busy: false, pending, error });
      throw error;
    }
  };
}
const operations = new Map<string, ThemeOperation>();
export function useThemeOperation(workspace: string) {
  let operation = operations.get(workspace);
  if (!operation) {
    operation = new ThemeOperation(workspace);
    operations.set(workspace, operation);
  }
  const state = useSyncExternalStore(
    operation.subscribe,
    operation.getSnapshot,
  );
  return {
    ...state,
    run: operation.run,
    retry: operation.retry,
    clearError: operation.clearError,
    message: state.error ? themeError(state.error) : null,
  };
}
