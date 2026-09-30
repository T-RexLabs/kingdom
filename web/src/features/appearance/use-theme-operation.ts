import { useSyncExternalStore } from "react";
import { ApiError, apiFetch, currentAuthBinding } from "@/api/client";
import type { components } from "@/api/schema";
import { newIdempotencyKey } from "@/lib/idempotency";
import {
  decodeMutationOutcome,
  type OutcomeOperation,
} from "@/lib/mutation-outcome";
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
const themeOutcomeRoutes = {
  "PUT /themes/{id}/source": "theme-source-save",
  "DELETE /themes/{id}/source": "theme-source-delete",
  "POST /themes/{id}/publish": "theme-publish",
  "DELETE /themes/{id}": "theme-unpublish",
  "POST /themes/import": "theme-import",
} satisfies Record<string, OutcomeOperation>;

function themeOutcomeOperation(request: SharedThemeRequest) {
  if (!validSharedRequest(request)) throw new Error(unresolvedMessage);
  const path =
    request.path === "/themes/import"
      ? request.path
      : request.path.replace(/^\/themes\/[^/]+/, "/themes/{id}");
  const operation =
    themeOutcomeRoutes[
      `${request.method} ${path}` as keyof typeof themeOutcomeRoutes
    ];
  if (!operation) throw new Error(unresolvedMessage);
  return operation;
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
    // Only the retained request's exact durable outcome can clear this guard.
    // A renewed browser binding may read that outcome, but cannot replay it.
    this.update({ busy: true, pending, error: null });
    let authoritative = false;
    try {
      if (pending.origin !== window.location.origin || !currentAuthBinding())
        throw new Error(unresolvedMessage);
      const outcome = await apiFetch<components["schemas"]["MutationOutcome"]>(
        `/mutation-outcomes/${encodeURIComponent(pending.key)}`,
        { params: { operation: themeOutcomeOperation(pending.request) } },
      );
      const decoded = decodeMutationOutcome(outcome);
      if (decoded.state === "unresolved") throw new Error(unresolvedMessage);
      authoritative = true;
      if (decoded.state === "rejected") throw decoded.error;
      this.update({ busy: false, pending: null, error: null });
      return { request: pending.request, result: decoded.value };
    } catch (error) {
      this.update({
        busy: false,
        pending: authoritative ? null : pending,
        error,
      });
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
