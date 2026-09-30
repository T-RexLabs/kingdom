import { describe, expect, it } from "vitest";
import { ApiError } from "@/api/client";
import type { components } from "@/api/schema";
import { decodeMutationOutcome } from "./mutation-outcome";

type Outcome = components["schemas"]["MutationOutcome"];

describe("mutation outcome decoding", () => {
  it.each([
    { state: "unknown" }, { state: "pending" }, { state: "completed" },
    { state: "completed", response_status: 200 },
    { state: "completed", response_status: 201.5, response_body: {} },
    { state: "completed", response_status: 302, response_body: {} },
    { state: "completed", response_status: 600, response_body: {} },
    { state: "completed", response_status: 400 },
    { state: "completed", response_status: 200, response_body: [] },
    { state: "completed", response_status: 200, response_body: "invalid" },
  ])("does not treat an incomplete or malformed projection as authoritative: %j", (outcome) => {
    expect(decodeMutationOutcome(outcome as Outcome)).toEqual({ state: "unresolved" });
  });

  it("recognizes completed deletion without manufacturing a response body", () => {
    expect(decodeMutationOutcome({ state: "completed", response_status: 204 })).toEqual({ state: "succeeded", value: undefined });
  });

  it("returns the original complete success response", () => {
    const body = { planning_session_id: "session-a", title: "Saved" };
    expect(decodeMutationOutcome({ state: "completed", response_status: 200, response_body: body })).toEqual({ state: "succeeded", value: body });
  });

  it("distinguishes the original rejection from failure to read its outcome", () => {
    const decoded = decodeMutationOutcome({ state: "completed", response_status: 409, response_body: { error: {
      message: "Original conflict", code: "idempotency_conflict", category: "core", details: { field: "title" },
      correlation_id: "original-correlation", retryable: false, occurred_at: "2026-09-30T10:00:00Z", retry_at: "2026-09-30T10:05:00Z",
    } } });
    expect(decoded.state).toBe("rejected");
    if (decoded.state !== "rejected") throw new Error("Expected authoritative rejection");
    expect(decoded.error).toBeInstanceOf(ApiError);
    expect(decoded.error).toMatchObject({ status: 409, message: "Original conflict", code: "idempotency_conflict", details: { field: "title" }, correlationId: "original-correlation", retryable: false, occurredAt: "2026-09-30T10:00:00Z", retryAt: "2026-09-30T10:05:00Z" });
  });
});
