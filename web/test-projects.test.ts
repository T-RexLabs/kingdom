import { describe, expect, it } from "vitest";

describe("Node test environment", () => {
  it("does not load browser globals or DOM setup", () => {
    expect(typeof window).toBe("undefined");
    expect(typeof document).toBe("undefined");
    expect("IntersectionObserver" in globalThis).toBe(false);
    expect("ResizeObserver" in globalThis).toBe(false);
  });
});
