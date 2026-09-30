import type { components } from "./schema";

export function resolveAPIBase(documentRoot: Document = document): string {
  return documentRoot.querySelector<HTMLMetaElement>('meta[name="kingdom-api-base"]')?.content.trim() || "/api/v1";
}

export const API_BASE = resolveAPIBase();

type CanonicalErrorEnvelope = components["schemas"]["ErrorEnvelope"];
type CanonicalErrorPayload = CanonicalErrorEnvelope["error"];

export type ApiErrorCode = CanonicalErrorPayload["code"] | "unknown";
export type ApiErrorCategory = CanonicalErrorPayload["category"] | "core";

type ApiErrorOptions = {
  message: string;
  code: ApiErrorCode;
  retryable: boolean;
  category: ApiErrorCategory;
  details: Record<string, unknown>;
  correlationId: string;
  occurredAt?: string;
  retryAt?: string;
};

type ParsedApiErrorOptions = Omit<ApiErrorOptions, "retryable"> & {
  retryable?: boolean;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseCanonicalErrorPayload(body: unknown): ParsedApiErrorOptions | null {
  if (!isRecord(body) || !isRecord(body.error)) {
    return null;
  }

  const { error } = body;
  if (typeof error.message !== "string" || typeof error.code !== "string") {
    return null;
  }

  const details = isRecord(error.details) ? error.details : {};

  const retryAt = typeof error.retry_at === "string" && !Number.isNaN(Date.parse(error.retry_at)) ? error.retry_at : undefined;

  return {
    message: error.message,
    code: error.code as ApiErrorCode,
    retryable: typeof error.retryable === "boolean" ? error.retryable : undefined,
    category: typeof error.category === "string" ? (error.category as ApiErrorCategory) : "core",
    details,
    correlationId: typeof error.correlation_id === "string" ? error.correlation_id : "api",
    occurredAt: typeof error.occurred_at === "string" ? error.occurred_at : undefined,
    retryAt,
  };
}

function inferRetryableFromStatus(status: number): boolean {
  return status === 408 || status >= 500;
}

function normalizeApiErrorOptions(status: number, statusText: string, parsed: ParsedApiErrorOptions | null): ApiErrorOptions {
  if (parsed) {
    return {
      ...parsed,
      retryable: parsed.retryable ?? inferRetryableFromStatus(status),
    };
  }

  return {
    message: statusText || "Request failed",
    code: "unknown",
    retryable: inferRetryableFromStatus(status),
    category: "core",
    details: {},
    correlationId: "api",
  };
}

let csrfToken = "";
let authGeneration = 0;
let signaledGeneration = -1;

export type UnauthorizedEvent = {
  generation: number;
  source: "read" | "mutation" | "stream";
};

const unauthorizedListeners = new Set<(event: UnauthorizedEvent) => void>();

export function setCSRFToken(token: string): void {
  csrfToken = token;
  authGeneration += 1;
  signaledGeneration = -1;
}

export function currentAuthGeneration(): number {
  return authGeneration;
}

export function subscribeToUnauthorized(listener: (event: UnauthorizedEvent) => void): () => void {
  unauthorizedListeners.add(listener);
  return () => unauthorizedListeners.delete(listener);
}

export function signalUnauthorized(source: UnauthorizedEvent["source"] = "read", generation = authGeneration): void {
  if (generation !== authGeneration || signaledGeneration === generation) return;
  csrfToken = "";
  signaledGeneration = generation;
  const event = { generation, source } as const;
  for (const listener of unauthorizedListeners) listener(event);
}

export function buildAuthHeaders(
  headers?: HeadersInit
): Record<string, string> {
  return {
    ...(headers as Record<string, string>),
  };
}

/**
 * Typed fetch wrapper for the Kingdom API.
 * Throws on non-2xx responses with the canonical error envelope.
 */
export async function apiFetch<T>(
  path: string,
  options?: RequestInit & {
    params?: Record<string, string | number | boolean | undefined>;
  }
): Promise<T> {
  const { params, ...fetchOpts } = options ?? {};

  let url = `${API_BASE}${path}`;
  if (params) {
    const searchParams = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) {
        searchParams.set(key, String(value));
      }
    }
    const qs = searchParams.toString();
    if (qs) url += `?${qs}`;
  }

  const headers = buildAuthHeaders(fetchOpts.headers);
  const requestGeneration = authGeneration;

  if (fetchOpts.body && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }

  const method = (fetchOpts.method ?? "GET").toUpperCase();
  const mutation = ["POST", "PUT", "PATCH", "DELETE"].includes(method);
  if (mutation && csrfToken) {
    headers["X-CSRF-Token"] = csrfToken;
  }

  const res = await fetch(url, {
    ...fetchOpts,
    cache: fetchOpts.cache ?? "no-store",
    credentials: "same-origin",
    headers,
  });

  if (!res.ok) {
    const errorBody = await res.json().catch(() => null);
    const parsedError = parseCanonicalErrorPayload(errorBody);
    if (res.status === 401 || (res.status === 403 && parsedError?.correlationId === "api.auth.csrf")) {
      signalUnauthorized(mutation ? "mutation" : "read", requestGeneration);
    }
    throw new ApiError(res.status, normalizeApiErrorOptions(res.status, res.statusText, parsedError));
  }

  if (res.status === 204) return undefined as T;

  return res.json();
}

export class ApiError extends Error {
  status: number;
  code: ApiErrorCode;
  retryable: boolean;
  category: ApiErrorCategory;
  details: Record<string, unknown>;
  correlationId: string;
  occurredAt?: string;
  retryAt?: string;

  constructor(status: number, options: ApiErrorOptions) {
    super(options.message);
    this.name = "ApiError";
    this.status = status;
    this.code = options.code;
    this.retryable = options.retryable;
    this.category = options.category;
    this.details = options.details;
    this.correlationId = options.correlationId;
    this.occurredAt = options.occurredAt;
    this.retryAt = options.retryAt;
  }
}

export function shouldRetryApiQuery(failureCount: number, error: unknown, maxRetries: number): boolean {
  return !(error instanceof ApiError && error.status === 401) && failureCount < maxRetries;
}

export function shouldRetryApiMutation(failureCount: number, error: unknown, maxRetries: number): boolean {
  return (!(error instanceof ApiError) || error.retryable) && failureCount < maxRetries;
}

/** Types extracted from generated schema for convenience */
export type { paths, components } from "./schema";
