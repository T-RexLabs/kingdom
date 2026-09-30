import type { QueryClient } from "@tanstack/react-query";
import { API_BASE, currentAuthBinding, currentAuthGeneration } from "@/api/client";
import type { CreateAcpRunRequest, PlanningSessionDetail } from "@/api/hooks";
import type { PlannerOperationKind, PlannerOperationState } from "./planner-operation-controller";

export type PlannerScope = { workspace: string; sessionId: string };
export type PlannerScrollPosition = { messageId: string; offset: number; pinned: boolean };
export type PlannerDraft = {
  scroll?: PlannerScrollPosition;
  text: string;
  generation: number;
  editing?: { messageId: string; text: string; generation: number };
};
export type PlannerDraftSubmission = { branch: string; generation: number; kind: "composer" | "edit" };
export type PlannerPendingRequest = {
  kind: "reply" | "regenerate" | "formalize" | "stop";
  key: string;
  body: CreateAcpRunRequest | { runId: string };
  authGeneration: number;
  authBinding?: string;
  draft?: PlannerDraftSubmission;
  draftBranch?: string;
  parentMessageId?: string;
  restored: boolean;
  phase: "submitting" | "recovery";
};
export type TrackedPlannerOperation = PlannerScope & {
  operationId: string;
  kind: PlannerOperationKind;
  sessionTitle: string;
  draftBranch?: string;
  notified?: boolean;
};
export type PlannerSessionUI = {
  selectedTab?: "chat" | "plan" | "context";
  drafts: Record<string, PlannerDraft>;
  leafBranches: Record<string, string>;
  trackedOperationId?: string;
  optimisticOperation: PlannerOperationState | null;
  request?: PlannerPendingRequest;
  error?: string;
  checkingRecovery: boolean;
  sequence: number;
  followLatestRequest: number;
  storageUnavailable: boolean;
};

const EMPTY_DRAFT: PlannerDraft = Object.freeze({ text: "", generation: 0 });
const prefix = `kingdom:planner-ui:v1:${API_BASE}:`;
const operationStorageKey = `${prefix}operations`;
const stores = new WeakMap<QueryClient, PlannerWorkspaceState>();

export function plannerScope(session: Pick<PlanningSessionDetail, "planning_session_id" | "project_root">): PlannerScope {
  return { workspace: session.project_root, sessionId: session.planning_session_id };
}
export function plannerScopeKey(scope: PlannerScope): string {
  return JSON.stringify([scope.workspace, scope.sessionId]);
}
function sessionStorageKey(scope: PlannerScope) { return `${prefix}session:${plannerScopeKey(scope)}`; }
function emptySession(): PlannerSessionUI {
  return { drafts: {}, leafBranches: {}, optimisticOperation: null, checkingRecovery: false, sequence: 0, followLatestRequest: 0, storageUnavailable: false };
}
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function readSession(scope: PlannerScope): PlannerSessionUI {
  const state = emptySession();
  try {
    const raw = sessionStorage.getItem(sessionStorageKey(scope));
    if (!raw) return state;
    const saved: unknown = JSON.parse(raw);
    if (!record(saved)) return state;
    if (record(saved.drafts)) {
      for (const [branch, value] of Object.entries(saved.drafts)) {
        if (!record(value) || typeof value.text !== "string" || typeof value.generation !== "number") continue;
        const edit = value.editing;
        state.drafts[branch] = {
          text: value.text, generation: value.generation,
          ...(record(value.scroll) && typeof value.scroll.messageId === "string" && typeof value.scroll.offset === "number" && Number.isFinite(value.scroll.offset) && typeof value.scroll.pinned === "boolean" ? { scroll: value.scroll as PlannerScrollPosition } : {}),
          ...(record(edit) && typeof edit.messageId === "string" && typeof edit.text === "string" && typeof edit.generation === "number"
            ? { editing: { messageId: edit.messageId, text: edit.text, generation: edit.generation } } : {}),
        };
      }
    }
    if (record(saved.leafBranches)) {
      for (const [leaf, branch] of Object.entries(saved.leafBranches)) {
        if (typeof branch === "string") state.leafBranches[leaf] = branch;
      }
    }
    if (["chat", "plan", "context"].includes(String(saved.selectedTab))) state.selectedTab = saved.selectedTab as PlannerSessionUI["selectedTab"];
    if (typeof saved.trackedOperationId === "string") state.trackedOperationId = saved.trackedOperationId;
    const request = saved.request;
    if (record(request) && ["reply", "regenerate", "formalize", "stop"].includes(String(request.kind)) && typeof request.key === "string" && record(request.body)) {
      // This journal is evidence of an unresolved request, never authority to
      // replay under a newly bootstrapped browser session. No auth secrets are stored.
      state.request = { ...request, restored: true, phase: "recovery" } as PlannerPendingRequest;
    }
  } catch { state.storageUnavailable = true; }
  return state;
}
function readOperations(): TrackedPlannerOperation[] {
  try {
    const saved: unknown = JSON.parse(sessionStorage.getItem(operationStorageKey) ?? "[]");
    if (!Array.isArray(saved)) return [];
    return saved.filter((item): item is TrackedPlannerOperation => record(item)
      && typeof item.workspace === "string" && typeof item.sessionId === "string" && typeof item.operationId === "string"
      && typeof item.sessionTitle === "string" && ["reply", "regenerate", "formalize", "generate_title"].includes(String(item.kind)));
  } catch { return []; }
}

/** Client-owned intent and operation identities only. Canonical domain data stays in Query. */
export class PlannerWorkspaceState {
  private sessions = new Map<string, PlannerSessionUI>();
  private listeners = new Map<string, Set<() => void>>();
  private operationListeners = new Set<() => void>();
  private operations = readOperations();

  getSession = (scope: PlannerScope): PlannerSessionUI => {
    const key = plannerScopeKey(scope);
    let state = this.sessions.get(key);
    if (!state) { state = readSession(scope); this.sessions.set(key, state); }
    return state;
  };
  subscribeSession = (scope: PlannerScope, listener: () => void) => {
    const key = plannerScopeKey(scope);
    let listeners = this.listeners.get(key);
    if (!listeners) { listeners = new Set(); this.listeners.set(key, listeners); }
    listeners.add(listener);
    return () => { listeners.delete(listener); if (!listeners.size) this.listeners.delete(key); };
  };
  getOperations = () => this.operations;
  subscribeOperations = (listener: () => void) => { this.operationListeners.add(listener); return () => { this.operationListeners.delete(listener); }; };

  private persistSession(scope: PlannerScope, state: PlannerSessionUI): boolean {
    try {
      sessionStorage.setItem(sessionStorageKey(scope), JSON.stringify({
        selectedTab: state.selectedTab, drafts: state.drafts, leafBranches: state.leafBranches, trackedOperationId: state.trackedOperationId, request: state.request,
      }));
      return true;
    } catch { return false; }
  }
  private publishSession(scope: PlannerScope, state: PlannerSessionUI) {
    this.sessions.set(plannerScopeKey(scope), state);
    this.listeners.get(plannerScopeKey(scope))?.forEach((listener) => listener());
  }
  updateSession(scope: PlannerScope, update: (state: PlannerSessionUI) => PlannerSessionUI) {
    const current = this.getSession(scope);
    const next = update(current);
    if (next === current) return;
    const persisted = this.persistSession(scope, next);
    this.publishSession(scope, { ...next, storageUnavailable: !persisted });
  }
  /** Mutation intent must cross this durable boundary before any network send. */
  journalRequest(scope: PlannerScope, request: PlannerPendingRequest, update: (state: PlannerSessionUI) => PlannerSessionUI = (state) => state) {
    const current = this.getSession(scope);
    const next = { ...update(current), request, error: undefined };
    if (!this.persistSession(scope, next)) {
      const error = new Error("The recovery request could not be saved. No planner request was sent. Free browser storage and try again.");
      this.publishSession(scope, { ...current, storageUnavailable: true, error: error.message });
      throw error;
    }
    this.publishSession(scope, { ...next, storageUnavailable: false });
  }
  branch(scope: PlannerScope, leaf?: string): string {
    const anchor = leaf ?? "root";
    return this.getSession(scope).leafBranches[anchor] ?? anchor;
  }
  draft(scope: PlannerScope, branch: string): PlannerDraft { return this.getSession(scope).drafts[branch] ?? EMPTY_DRAFT; }
  updateDraft(scope: PlannerScope, branch: string, update: (draft: PlannerDraft) => PlannerDraft) {
    this.updateSession(scope, (state) => {
      const current = state.drafts[branch] ?? EMPTY_DRAFT;
      const next = update(current);
      return next === current ? state : { ...state, drafts: { ...state.drafts, [branch]: next } };
    });
  }
  setText(scope: PlannerScope, branch: string, text: string) {
    this.updateDraft(scope, branch, (draft) => ({ ...draft, text, generation: draft.generation + 1 }));
  }
  clearSubmittedDraft(scope: PlannerScope, branch: string, generation: number, editing = false) {
    this.updateDraft(scope, branch, (draft) => editing
      ? draft.editing?.generation === generation ? { ...draft, editing: undefined } : draft
      : draft.generation === generation ? { ...draft, text: "", generation: draft.generation + 1 } : draft);
  }
  canRetry(request: PlannerPendingRequest): boolean {
    const binding = currentAuthBinding();
    return request.authBinding ? Boolean(binding) && request.authBinding === binding : !request.restored && request.authGeneration === currentAuthGeneration();
  }
  trackOperation(entry: TrackedPlannerOperation) {
    const existing = this.operations.find((item) => item.operationId === entry.operationId);
    if (existing && Object.entries(entry).every(([key, value]) => existing[key as keyof TrackedPlannerOperation] === value)) return;
    this.setOperations([...this.operations.filter((item) => item.operationId !== entry.operationId), { ...existing, ...entry }]);
  }
  markNotified(operationId: string) {
    this.setOperations(this.operations.map((entry) => entry.operationId === operationId ? { ...entry, notified: true } : entry));
  }
  finishOperation(operationId: string) {
    this.setOperations(this.operations.filter((entry) => entry.operationId !== operationId));
  }
  observeOperation(entry: TrackedPlannerOperation, operation: PlannerOperationState) {
    if (entry.draftBranch && (operation.userMessageId || operation.assistantMessageId)) {
      this.updateSession(entry, (state) => {
        const leaves = [operation.userMessageId, operation.assistantMessageId].filter((id): id is string => Boolean(id));
        if (leaves.every((id) => state.leafBranches[id] === entry.draftBranch)) return state;
        return { ...state, leafBranches: { ...state.leafBranches, ...Object.fromEntries(leaves.map((id) => [id, entry.draftBranch!])) } };
      });
    }
    const terminal = operation.status === "error" || operation.status === "cancelled" || (operation.status === "done" && operation.activityReconciled);
    if (!terminal) return;
    this.updateSession(entry, (state) => {
      const stopResolved = state.request?.kind === "stop" && (state.request.body as { runId: string }).runId === operation.operationId;
      const completedCurrent = state.trackedOperationId === operation.operationId && operation.status === "done";
      if (!stopResolved && !completedCurrent) return state;
      return { ...state, ...(completedCurrent ? { trackedOperationId: undefined } : {}), ...(stopResolved ? { request: undefined, error: undefined } : {}) };
    });
  }
  removeSession(sessionId: string) {
    const knownKeys = new Set(this.sessions.keys());
    try {
      for (let index = 0; index < sessionStorage.length; index += 1) {
        const key = sessionStorage.key(index);
        if (key?.startsWith(`${prefix}session:`)) knownKeys.add(key.slice(`${prefix}session:`.length));
      }
    } catch { /* loaded in-memory drafts can still be cleared */ }
    const scopes = [...knownKeys].map((key) => JSON.parse(key) as [string, string]).filter(([, id]) => id === sessionId);
    for (const [workspace] of scopes) {
      const scope = { workspace, sessionId };
      this.updateSession(scope, () => emptySession());
      try { sessionStorage.removeItem(sessionStorageKey(scope)); } catch { /* memory was cleared */ }
    }
    this.setOperations(this.operations.filter((entry) => entry.sessionId !== sessionId));
  }
  private setOperations(operations: TrackedPlannerOperation[]) {
    this.operations = operations;
    try { sessionStorage.setItem(operationStorageKey, JSON.stringify(operations)); } catch {
      for (const entry of operations) this.updateSession(entry, (state) => ({ ...state, storageUnavailable: true }));
    }
    this.operationListeners.forEach((listener) => listener());
  }
}

export function plannerWorkspaceState(client: QueryClient): PlannerWorkspaceState {
  let state = stores.get(client);
  if (!state) { state = new PlannerWorkspaceState(); stores.set(client, state); }
  return state;
}
