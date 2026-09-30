import { beforeEach, describe, expect, it, vi } from "vitest";
import { PlannerWorkspaceState, type PlannerPendingRequest } from "./planner-workspace-state";
import { setCSRFToken, currentAuthGeneration } from "@/api/client";

const scope = { workspace: "/tmp/project", sessionId: "session-a" };
beforeEach(() => { sessionStorage.clear(); setCSRFToken("", ""); });

describe("planner workspace intent", () => {
  it("restores composer and edit drafts only within their workspace, session and branch", () => {
    const store = new PlannerWorkspaceState();
    store.setText(scope, "branch-a", "Unsent A");
    store.updateDraft(scope, "branch-a", (draft) => ({ ...draft, editing: { messageId: "user-a", text: "Edited A", generation: 2 } }));
    store.setText(scope, "branch-b", "Unsent B");
    const restored = new PlannerWorkspaceState();
    expect(restored.draft(scope, "branch-a")).toMatchObject({ text: "Unsent A", editing: { text: "Edited A" } });
    expect(restored.draft(scope, "branch-b").text).toBe("Unsent B");
    expect(restored.draft({ ...scope, workspace: "/tmp/another-project" }, "branch-a").text).toBe("");
    expect(restored.draft({ ...scope, sessionId: "session-b" }, "branch-a").text).toBe("");
  });

  it("clears only the submitted generation and preserves newer text and edit changes", () => {
    const store = new PlannerWorkspaceState();
    store.setText(scope, "branch", "Submitted");
    const submitted = store.draft(scope, "branch").generation;
    store.setText(scope, "branch", "Typed later");
    store.clearSubmittedDraft(scope, "branch", submitted);
    expect(store.draft(scope, "branch").text).toBe("Typed later");
    store.clearSubmittedDraft(scope, "branch", store.draft(scope, "branch").generation);
    expect(store.draft(scope, "branch").text).toBe("");
  });

  it("keeps an exact unresolved journal on reload but never infers replay authority", () => {
    const request: PlannerPendingRequest = { kind: "reply", key: "original-key", body: { agent_name: "planner-reply", planning_session_id: "session-a", reply: { content_markdown: "Original text" } }, authGeneration: currentAuthGeneration(), restored: false, phase: "submitting" };
    const store = new PlannerWorkspaceState();
    store.updateSession(scope, (state) => ({ ...state, request }));
    expect(store.canRetry(request)).toBe(true);
    const restored = new PlannerWorkspaceState();
    expect(restored.getSession(scope).request).toMatchObject({ key: "original-key", body: request.body, phase: "recovery", restored: true });
    expect(restored.canRetry(restored.getSession(scope).request!)).toBe(false);
  });

  it("retains independent operation registrations and removes only the completed one", () => {
    const store = new PlannerWorkspaceState();
    store.trackOperation({ ...scope, operationId: "op-a", kind: "formalize", sessionTitle: "A" });
    store.trackOperation({ ...scope, sessionId: "session-b", operationId: "op-b", kind: "formalize", sessionTitle: "B" });
    store.markNotified("op-a");
    expect(new PlannerWorkspaceState().getOperations()).toHaveLength(2);
    store.finishOperation("op-a");
    expect(store.getOperations().map((entry) => entry.operationId)).toEqual(["op-b"]);
  });

  it("keeps text in memory and exposes storage failure", () => {
    const write = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new DOMException("Quota exceeded", "QuotaExceededError"); });
    try {
      const store = new PlannerWorkspaceState();
      store.setText(scope, "branch", "Do not lose this");
      expect(store.draft(scope, "branch").text).toBe("Do not lose this");
      expect(store.getSession(scope).storageUnavailable).toBe(true);
    } finally { write.mockRestore(); }
  });

  it("clears deleted-session drafts without clearing another session", () => {
    const store = new PlannerWorkspaceState();
    store.setText(scope, "branch", "A");
    store.setText({ ...scope, sessionId: "session-b" }, "branch", "B");
    store.removeSession("session-a");
    const restored = new PlannerWorkspaceState();
    expect(restored.draft(scope, "branch").text).toBe("");
    expect(restored.draft({ ...scope, sessionId: "session-b" }, "branch").text).toBe("B");
  });
});


it("permits exact-key retry after reload only when the nonsecret browser binding matches", () => {
  setCSRFToken("secret-never-stored", "binding-a");
  const store = new PlannerWorkspaceState();
  const request: PlannerPendingRequest = { kind: "formalize", body: { agent_name: "planner-formalize", planning_session_id: scope.sessionId }, key: "key", authBinding: "binding-a", authGeneration: currentAuthGeneration(), restored: false, phase: "recovery" };
  store.updateSession(scope, (state) => ({ ...state, request }));
  const restored = new PlannerWorkspaceState();
  expect(restored.canRetry(restored.getSession(scope).request!)).toBe(true);
  setCSRFToken("another-secret", "binding-b");
  expect(restored.canRetry(restored.getSession(scope).request!)).toBe(false);
  expect(JSON.stringify(Object.values(sessionStorage))).not.toContain("secret");
});


it("keeps later typing on the same conversation as a reply advances its canonical leaf", () => {
  const store = new PlannerWorkspaceState();
  store.setText(scope, "old-leaf", "Next thought");
  const entry = { ...scope, operationId: "reply-op", kind: "reply" as const, sessionTitle: "A", draftBranch: "old-leaf" };
  store.trackOperation(entry);
  store.observeOperation(entry, { kind: "reply", operationId: "reply-op", status: "done", lastEventSeq: 4, userMessageId: "new-user", assistantMessageId: "new-assistant", pendingReply: null, replyText: "Reply", formalizeText: "", error: null, activityReconciled: false, activityItems: [] });
  expect(store.draft(scope, store.branch(scope, "new-assistant")).text).toBe("Next thought");
  expect(store.draft(scope, store.branch(scope, "other-sibling")).text).toBe("");
});


it("persists transcript positions independently of other sessions, branches, and submitted text", () => {
  const store = new PlannerWorkspaceState();
  store.setText(scope, "main", "Unsent");
  const generation = store.draft(scope, "main").generation;
  store.updateDraft(scope, "main", (draft) => ({ ...draft, scroll: { messageId: "old-message", offset: -44, pinned: false } }));
  store.updateSession(scope, (state) => ({ ...state, selectedTab: "plan" }));
  store.clearSubmittedDraft(scope, "main", generation);
  const restored = new PlannerWorkspaceState();
  expect(restored.draft(scope, "main")).toMatchObject({ text: "", scroll: { messageId: "old-message", offset: -44, pinned: false } });
  expect(restored.draft(scope, "other").scroll).toBeUndefined();
  expect(restored.draft({ ...scope, sessionId: "another" }, "main").scroll).toBeUndefined();
  expect(restored.getSession(scope).selectedTab).toBe("plan");
});
