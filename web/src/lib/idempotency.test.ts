import { describe, expect, it } from "vitest";
import { newIdempotencyKey } from "./idempotency";

describe("newIdempotencyKey", () => {
  it("prefixes with the supplied scope so server logs stay readable", () => {
    const key = newIdempotencyKey("planning-operation");
    expect(key.startsWith("planning-operation-")).toBe(true);
  });

  it("returns a unique key per call so distinct logical actions never collide", () => {
    // 1000 invocations is small but well above any plausible UUID birthday
    // collision; if this ever fails, randomUUID is broken.
    const seen = new Set<string>();
    for (let i = 0; i < 1000; i += 1) {
      const key = newIdempotencyKey("test");
      expect(seen.has(key)).toBe(false);
      seen.add(key);
    }
    expect(seen.size).toBe(1000);
  });
});
