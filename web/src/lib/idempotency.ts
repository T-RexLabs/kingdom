/**
 * Generate a single, stable Idempotency-Key for a logical mutation.
 *
 * The key is created at the call site (not inside `mutationFn`) so that
 * automatic retries from TanStack Query reuse the same key — that is the
 * entire point of idempotency: a retry should be deduplicated against the
 * original attempt, not treated as a new write. Generating fresh randomness
 * in `mutationFn` defeats the mechanism.
 *
 * Keys carry an explicit issue time and TTL so the server can reject them
 * permanently after bounded durable retention.
 */
const version = "kd1";
const ttlSeconds = 24 * 60 * 60;
const scopePattern = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;

export function newIdempotencyKey(scope: string): string {
  if (!scopePattern.test(scope) || scope.length > 40) {
    throw new Error("idempotency scope must be lowercase alphanumeric with optional hyphens");
  }
  if (typeof globalThis.crypto?.getRandomValues !== "function") {
    throw new Error("secure randomness is unavailable");
  }
  const nonce = globalThis.crypto.getRandomValues(new Uint8Array(16));
  const encodedNonce = btoa(String.fromCharCode(...nonce))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/, "");
  const issuedAt = Math.floor(Date.now() / 1000);
  return `${version}.${issuedAt}.${ttlSeconds}.${scope}.${encodedNonce}`;
}
