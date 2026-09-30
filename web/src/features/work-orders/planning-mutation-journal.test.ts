import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, setCSRFToken } from "@/api/client";
import { PlanningMutationJournal } from "./planning-mutation-journal";

beforeEach(() => { sessionStorage.clear(); setCSRFToken("token", "binding-a"); vi.unstubAllGlobals(); });
describe("ordinary planning mutation journal", () => {
  it("persists before send and recovers the exact request after a reload", async () => {
    const journal = new PlanningMutationJournal();
    const send = vi.fn(async () => { expect(new PlanningMutationJournal().get("title")?.body).toEqual({ title: "Original" }); throw new Error("lost response"); });
    await expect(journal.execute("title", { title: "Original" }, "key-original", send)).rejects.toThrow();
    const retry = vi.fn(async () => ({ title: "Original" }));
    await new PlanningMutationJournal().execute("title", { title: "Later edit" }, "key-new", retry);
    expect(retry).toHaveBeenCalledWith({ title: "Original" }, "key-original");
    expect(new PlanningMutationJournal().get("title")).toBeNull();
  });
  it("keeps independent sessions and slices isolated", async () => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("A:title", { title: "A" }, "A", async () => { throw new Error("lost"); })).rejects.toThrow();
    await journal.execute("B:title", { title: "B" }, "B", async () => ({}));
    await journal.execute("A:model", { model: "model" }, "M", async () => ({}));
    expect(journal.get("A:title")?.key).toBe("A");
  });
  it("blocks replacement-binding replay of an unresolved ordinary update", async () => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("title", { title: "A" }, "key", async () => { throw new Error("lost"); })).rejects.toThrow();
    setCSRFToken("replacement", "binding-b");
    const send = vi.fn();
    await expect(new PlanningMutationJournal().execute("title", { title: "B" }, "new", send)).rejects.toThrow("earlier browser session");
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("title")?.key).toBe("key");
  });
  it.each(["unknown", "pending"])("does not fresh-send a %s creation outcome", async (state) => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("create", {}, "key", async () => { throw new Error("lost"); })).rejects.toThrow();
    setCSRFToken("replacement", "binding-b");
    const send = vi.fn();
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ state }), { status: 200 })));
    await expect(new PlanningMutationJournal().execute("create", {}, "new", send, "planning-session-create")).rejects.toThrow(state);
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("create")?.key).toBe("key");
  });
  it.each([
    { status: 401, code: "unknown", correlationId: "api.auth" },
    { status: 403, code: "policy_violation", correlationId: "api.auth.csrf" },
    { status: 409, code: "idempotency_conflict", correlationId: "idempotency-middleware" },
    { status: 409, code: "idempotency_key_expired", correlationId: "idempotency-middleware" },
  ] as const)("keeps an unknown original request after a replay boundary rejects with $code/$status", async (rejection) => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("title", { title: "Original" }, "original-key", async () => { throw new TypeError("response lost"); })).rejects.toThrow();
    const retry = vi.fn(async () => { throw new ApiError(rejection.status, { message: "Replay rejected", code: rejection.code, correlationId: rejection.correlationId, category: "core", retryable: false, details: {} }); });
    await expect(new PlanningMutationJournal().execute("title", { title: "Later" }, "replacement-key", retry)).rejects.toThrow("Replay rejected");
    expect(retry).toHaveBeenCalledWith({ title: "Original" }, "original-key");
    expect(new PlanningMutationJournal().get("title")).toMatchObject({ key: "original-key", body: { title: "Original" } });
  });
  it("reconciles an authoritative creation response without cross-binding mutation", async () => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("create", {}, "key", async () => { throw new Error("lost"); })).rejects.toThrow();
    setCSRFToken("replacement", "binding-b");
    const send = vi.fn();
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ state: "completed", response_status: 201, response_body: { session: { planning_session_id: "created" } } }), { status: 200 })));
    await expect(new PlanningMutationJournal().execute("create", {}, "new", send, "planning-session-create")).resolves.toEqual({ session: { planning_session_id: "created" } });
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("create")).toBeNull();
  });
});
