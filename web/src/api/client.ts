import type { components } from "./schema";

export const API_BASE = "/api/v1";

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

  return {
    message: error.message,
    code: error.code as ApiErrorCode,
    retryable: typeof error.retryable === "boolean" ? error.retryable : undefined,
    category: typeof error.category === "string" ? (error.category as ApiErrorCategory) : "core",
    details,
    correlationId: typeof error.correlation_id === "string" ? error.correlation_id : "api",
    occurredAt: typeof error.occurred_at === "string" ? error.occurred_at : undefined,
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

const DEFAULT_ACTOR_ID = "allfather";
const DEFAULT_ROLE_ID = "king";

export function buildAuthHeaders(
  headers?: HeadersInit
): Record<string, string> {
  return {
    "X-Actor-Id": DEFAULT_ACTOR_ID,
    "X-Role-Id": DEFAULT_ROLE_ID,
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

  if (fetchOpts.body && !headers["Content-Type"]) {
    headers["Content-Type"] = "application/json";
  }

  const res = await fetch(url, {
    ...fetchOpts,
    cache: fetchOpts.cache ?? "no-store",
    headers,
  });

  if (!res.ok) {
    const errorBody = await res.json().catch(() => null);
    throw new ApiError(res.status, normalizeApiErrorOptions(res.status, res.statusText, parseCanonicalErrorPayload(errorBody)));
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
  }
}

/** Types extracted from generated schema for convenience */
export type { paths, components } from "./schema";
