import { describe, expect, it } from "vitest";
import { newIdempotencyKey } from "./idempotency";

describe("newIdempotencyKey", () => {
  it("emits a versioned 24-hour key with the supplied scope", () => {
    const key = newIdempotencyKey("planning-operation");
    const [version, issuedAt, ttl, scope, nonce] = key.split(".");
    expect(version).toBe("kd1");
    expect(Number(issuedAt)).toBe(Math.floor(Date.now() / 1000));
    expect(ttl).toBe("86400");
    expect(scope).toBe("planning-operation");
    expect(nonce).toMatch(/^[A-Za-z0-9_-]{22}$/);
  });

  it("returns a unique key per call so distinct logical actions never collide", () => {
    const seen = new Set<string>();
    for (let i = 0; i < 1000; i += 1) {
      const key = newIdempotencyKey("test");
      expect(seen.has(key)).toBe(false);
      seen.add(key);
    }
    expect(seen.size).toBe(1000);
  });

  it("rejects scopes outside the protocol alphabet", () => {
    expect(() => newIdempotencyKey("Planning Operation")).toThrow(/scope/);
  });
});
