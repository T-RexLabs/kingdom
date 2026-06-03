/**
 * Generate a single, stable Idempotency-Key for a logical mutation.
 *
 * The key is created at the call site (not inside `mutationFn`) so that
 * automatic retries from TanStack Query reuse the same key — that is the
 * entire point of idempotency: a retry should be deduplicated against the
 * original attempt, not treated as a new write. Generating fresh randomness
 * in `mutationFn` defeats the mechanism.
 *
 * Crypto-strength UUID where available; falls back to a randomised suffix in
 * legacy environments (jsdom, very old browsers) so test runs still produce
 * unique keys.
 */
export function newIdempotencyKey(scope: string): string {
  const suffix = (typeof globalThis.crypto?.randomUUID === "function"
    ? globalThis.crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`);
  return `${scope}-${suffix}`;
}
