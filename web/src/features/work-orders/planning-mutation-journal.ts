import { API_BASE, ApiError, apiFetch, currentAuthBinding, currentAuthGeneration } from "@/api/client";
import type { components } from "@/api/schema";
import { shouldRetainMutationRequest } from "@/lib/use-recoverable-mutation";

export type JournalRequest<T> = {
  key: string;
  body: T;
  binding: string;
  generation: number;
  restored: boolean;
};
export type RequestBinding = Pick<JournalRequest<unknown>, "binding" | "generation" | "restored">;
export type OutcomeOperation = "planning-session-create" | "planning-operation-create" | "work-order-create" | "run-create";
const prefix = `kingdom:planning-mutations:v1:${API_BASE}:`;

/** This contains user intent and a nonsecret binding ID, never credentials or domain caches. */
export class PlanningMutationJournal {
  private requests = new Map<string, JournalRequest<unknown> | null>();
  private listeners = new Map<string, Set<() => void>>();
  private executing = new Map<string, Promise<unknown>>();

  get<T>(identity: string): JournalRequest<T> | null {
    if (!this.requests.has(identity)) {
      const raw = sessionStorage.getItem(prefix + identity);
      const value = raw ? JSON.parse(raw) as JournalRequest<T> : null;
      if (value && (typeof value.key !== "string" || !("body" in value) || typeof value.binding !== "string")) {
        throw new Error("The saved request could not be read. Its outcome must be resolved before sending another request.");
      }
      this.requests.set(identity, value ? { ...value, restored: true } : null);
    }
    return this.requests.get(identity) as JournalRequest<T> | null;
  }
  subscribe(identity: string, listener: () => void) {
    const listeners = this.listeners.get(identity) ?? new Set();
    this.listeners.set(identity, listeners);
    listeners.add(listener);
    return () => { listeners.delete(listener); };
  }
  private set<T>(identity: string, request: JournalRequest<T> | null) {
    // Persist before a request can be dispatched. Storage failure must not create
    // an unjournaled mutation that a reload could silently send again.
    if (request) sessionStorage.setItem(prefix + identity, JSON.stringify(request));
    else sessionStorage.removeItem(prefix + identity);
    this.requests.set(identity, request);
    this.listeners.get(identity)?.forEach((listener) => listener());
  }
  canReplay(request: JournalRequest<unknown>) {
    return request.binding
      ? request.binding === currentAuthBinding()
      : !request.restored && request.generation === currentAuthGeneration();
  }
  async execute<T, R>(identity: string, body: T, key: string, send: (body: T, key: string) => Promise<R>, operation?: OutcomeOperation, origin?: RequestBinding): Promise<R> {
    const running = this.executing.get(identity);
    if (running) return running as Promise<R>;
    const retained = this.get<T>(identity);
    const request = retained ?? { body, key, binding: currentAuthBinding(), generation: currentAuthGeneration(), restored: false, ...origin };
    if (!retained) this.set(identity, request);
    const execute = async () => {
      let authoritative = false;
      try {
        let result: R;
        if (this.canReplay(request)) {
          result = await send(request.body, request.key);
        } else {
          if (!operation) throw new Error("This request belongs to an earlier browser session. Its outcome is unresolved; another request cannot safely be sent.");
          const outcome = await apiFetch<components["schemas"]["MutationOutcome"]>(`/mutation-outcomes/${encodeURIComponent(request.key)}`, { params: { operation } });
          if (outcome.state !== "completed" || !outcome.response_status) {
            throw new Error(`The original request outcome is ${outcome.state}. Keep this recovery request; a new request could duplicate the action.`);
          }
          authoritative = true;
          if (outcome.response_status >= 400) {
            const error = outcome.response_body?.error as components["schemas"]["ErrorEnvelope"]["error"] | undefined;
            throw new ApiError(outcome.response_status, { message: error?.message ?? "The original request was rejected.", code: error?.code ?? "unknown", category: error?.category ?? "core", details: error?.details ?? {}, correlationId: error?.correlation_id ?? "mutation-outcome", retryable: error?.retryable ?? false });
          }
          result = outcome.response_body as R;
        }
        this.set(identity, null);
        return result;
      } catch (error) {
        if (authoritative || !shouldRetainMutationRequest(error, Boolean(retained))) this.set(identity, null);
        throw error;
      }
    };
    const promise = execute().finally(() => { this.executing.delete(identity); });
    this.executing.set(identity, promise);
    return promise;
  }
}
