import { Button } from "@/components/ui/button";
import { API_BASE, ApiError, apiFetch, currentAuthBinding, currentAuthGeneration } from "@/api/client";
import type { components } from "@/api/schema";
import {
  type CreateWorkOrderRequest,
  newIdempotencyKey,
  usePlanningSessionDraft,
  usePlanningSessions,
  useCreateWorkOrder,
  useCreateRun,
  useRoles,
  useWorkspaceCapabilities,
  type CreateRunRequest,
  type ProviderModel,
  type RunModelSelectionEntry,
  type RunSummary,
  type Role,
} from "@/api/hooks";
import { ChevronDown, CircleAlert, Loader2, RefreshCw, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { SelectField } from "@/components/ui/select-field";
import { ModelSelectField } from "./model-select-field";
import { findProviderModel, normalizeThinkingEffortSelection, selectedModelPayload } from "./provider-model-selection";
import { ThinkingEffortField } from "./thinking-effort-field";
import { ToggleRow } from "@/components/ui/toggle-row";
import { ProviderModelCatalogNotice } from "./provider-model-catalog-state";
import { useProviderModelCatalogController } from "./use-provider-model-catalog";
import { IssuePicker, type IssueSelection } from "@/features/connections/issue-picker";
import { ExternalSourceComparison } from "@/features/connections/external-source-comparison";
import { ExternalSourceCoordinate, ExternalSourceStrip } from "@/features/connections/external-source-strip";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import type { WorkOrderPlanningLineageSearch } from "./work-order-route-search";
import { usePlanningMutationJournal } from "./use-planning-mutation-journal";
import { useTheme, useThemeCopy, roleIdentity } from "@/theme";
import { isAmbiguousMutationFailure } from "./mutation-recovery";

function parseNonNegativeCycleInput(rawValue: string, fallback: number): number {
  const trimmed = rawValue.trim();
  if (trimmed === "") {
    return fallback;
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    return fallback;
  }
  return Math.max(0, parsed);
}

function normalizeNonNegativeCycleInput(rawValue: string, fallback: number): string {
  return String(parseNonNegativeCycleInput(rawValue, fallback));
}

type RoleGroup = {
  key: string;
  label: string;
  groupOrder: number;
  roles: Role[];
};

type ModelSelectionDraft = {
  model: string;
  thinkingEffort: string;
};

function normalizeModelSelectionDraft(models: ProviderModel[], draft: ModelSelectionDraft): ModelSelectionDraft {
  const model = draft.model.trim();
  const providerModel = findProviderModel(models, model);
  const thinkingEffort = normalizeThinkingEffortSelection(providerModel, draft.thinkingEffort, { preserveWhenModelMissing: true });
  return { model, thinkingEffort };
}

function sameModelSelection(left: ModelSelectionDraft, right: ModelSelectionDraft): boolean {
  return left.model === right.model && left.thinkingEffort === right.thinkingEffort;
}

function toRunModelSelectionEntry(models: ProviderModel[], draft: ModelSelectionDraft): RunModelSelectionEntry | undefined {
  const normalized = normalizeModelSelectionDraft(models, draft);
  if (normalized.model === "") {
    return undefined;
  }
  return selectedModelPayload(models, normalized.model, normalized.thinkingEffort);
}

function ErrorBanner({ message }: { message: string }) {
  return <div className="rounded-md border border-status-failed/30 bg-status-failed/10 px-3 py-2 text-sm text-status-failed">{message}</div>;
}

type CapturedExternalSource = {
  snapshotId: string;
  issue: components["schemas"]["NormalizedIssue"];
  /** The issue body plus its imported comments, as Kingdom renders it. */
  workOrderText: string;
};

type WorkOrderOrigin =
  | { kind: "manual" }
  | { kind: "planning"; sessionId: string; routeLineage?: WorkOrderPlanningLineageSearch }
  | {
      kind: "github";
      snapshotId: string;
      issue: components["schemas"]["NormalizedIssue"];
      workOrderText: string;
      intake: components["schemas"]["ExternalIntake"];
      candidate?: CapturedExternalSource;
    };

export type WorkOrderDispatchResult = {
  workOrderId: string;
  run: RunSummary;
};

type DispatchPhase = "submitting_work_order" | "work_order_recovery" | "submitting_run" | "run_recovery" | "completed";

type RecoveryMode = "fresh" | "replay" | "pending";

type DispatchSession = {
  draftScope: string;
  workOrderBinding: string;
  workOrderGeneration: number;
  runBinding?: string;
  runGeneration?: number;
  restored?: boolean;
  phase: DispatchPhase;
  workOrderKey: string;
  workOrderBody: CreateWorkOrderRequest;
  workOrderRecoveryMode?: RecoveryMode;
  workOrderId?: string;
  runKey?: string;
  runRequest?: Omit<CreateRunRequest, "work_order_id">;
  runRecoveryMode?: RecoveryMode;
  completedResult?: WorkOrderDispatchResult;
};

const creationStorageKeyFor = (workspace: string, scope: string) => `kingdom:work-order-draft:v1:${API_BASE}:${JSON.stringify([workspace, scope])}`;
const dispatchStorageKeyFor = (workspace: string) => `kingdom:work-order-dispatch:v1:${API_BASE}:${workspace}`;
type CreationDraft = {
  title: string; text: string; origin: WorkOrderOrigin; appliedDraftKey: string;
  skipReviewsAndValidations: boolean; runUntilAllApprove: boolean;
  maxExecutionReviewCyclesInput: string; maxValidationReworkCyclesInput: string;
  operatorApprovalRequired: boolean; useGlobalModel: boolean; globalSelection: ModelSelectionDraft;
  agentSelectionsByRole: Record<string, ModelSelectionDraft>; useGitWorktree: boolean; publishModeInitialized: boolean;
};
function savedCreationDraft(workspace: string, scope: string): Partial<CreationDraft> {
  try { return JSON.parse(sessionStorage.getItem(creationStorageKeyFor(workspace, scope)) ?? "{}") as Partial<CreationDraft>; } catch { return {}; }
}
function savedDispatch(workspace: string): DispatchSession | null {
  const saved = sessionStorage.getItem(dispatchStorageKeyFor(workspace));
  if (!saved) return null;
  const request = { ...JSON.parse(saved), restored: true } as DispatchSession;
  if (!request.workOrderKey || !request.workOrderBody) throw new Error("The saved dispatch request is unreadable. Resolve its outcome before issuing another work order.");
  if (request.phase === "completed" && !request.completedResult?.run?.run_id) throw new Error("The confirmed run result is unreadable. Keep this dispatch guarded before issuing another work order.");
  if (request.phase === "submitting_work_order") return { ...request, phase: "work_order_recovery", workOrderRecoveryMode: "replay" };
  if (request.phase === "submitting_run") return { ...request, phase: "run_recovery", runRecoveryMode: "replay" };
  return request;
}

function buildRunStartRequest(workOrderId: string | undefined, runRequest: Omit<CreateRunRequest, "work_order_id"> | undefined): CreateRunRequest | null {
  if (!workOrderId || !runRequest) {
    return null;
  }
  return {
    work_order_id: workOrderId,
    ...runRequest,
  };
}

function classifyRecoveryMode(error: unknown): RecoveryMode {
  if (error instanceof ApiError && error.code === "idempotency_pending") return "pending";
  return isAmbiguousMutationFailure(error) ? "replay" : "fresh";
}

type WorkOrderFormProps = {
  initialPlanningLineage?: Partial<WorkOrderPlanningLineageSearch>;
  onDispatched?: (result: WorkOrderDispatchResult) => void;
};
export function WorkOrderForm(props: WorkOrderFormProps) {
  const capabilities = useWorkspaceCapabilities();
  if (!capabilities.data) return <div role={capabilities.error ? "alert" : "status"} className="space-y-3 py-8 text-sm text-foreground-muted">{capabilities.error ? <>The project could not be loaded.<Button variant="outline" onClick={() => void capabilities.refetch()}>Retry</Button></> : "Loading project settings..."}</div>;
  return <WorkOrderFormContent key={JSON.stringify([capabilities.data.project_root, props.initialPlanningLineage?.planning_session_id, props.initialPlanningLineage?.planning_draft_id])} workspace={capabilities.data.project_root} {...props} />;
}
function WorkOrderFormContent({ initialPlanningLineage, onDispatched, workspace }: WorkOrderFormProps & { workspace: string }) {
  const [restoredDispatch] = useState(() => savedDispatch(workspace));
  const draftScope = restoredDispatch?.draftScope ?? (initialPlanningLineage?.planning_session_id && initialPlanningLineage.planning_draft_id ? JSON.stringify([initialPlanningLineage.planning_session_id, initialPlanningLineage.planning_draft_id]) : "manual");
  const creationStorageKey = creationStorageKeyFor(workspace, draftScope);
  const dispatchStorageKey = dispatchStorageKeyFor(workspace);
  const { theme } = useTheme();
  const workName = useThemeCopy("workOrder.singular");
  const [savedDraft] = useState(() => savedCreationDraft(workspace, draftScope));
  const [dispatchError, setDispatchError] = useState("");
  const [title, setTitle] = useState(savedDraft.title ?? "");
  const [text, setText] = useState(savedDraft.text ?? "");
  const [origin, setOrigin] = useState<WorkOrderOrigin>(() => savedDraft.origin ?? (initialPlanningLineage?.planning_session_id && initialPlanningLineage.planning_draft_id
    ? { kind: "planning", sessionId: initialPlanningLineage.planning_session_id, routeLineage: initialPlanningLineage as WorkOrderPlanningLineageSearch }
    : { kind: "manual" }));
  const [externalBusy, setExternalBusy] = useState(false);
  const [externalError, setExternalError] = useState("");
  const [skipReviewsAndValidations, setSkipReviewsAndValidations] = useState(savedDraft.skipReviewsAndValidations ?? false);
  const [runUntilAllApprove, setRunUntilAllApprove] = useState(savedDraft.runUntilAllApprove ?? false);
  const [maxExecutionReviewCyclesInput, setMaxExecutionReviewCyclesInput] = useState(savedDraft.maxExecutionReviewCyclesInput ?? "5");
  const [maxValidationReworkCyclesInput, setMaxValidationReworkCyclesInput] = useState(savedDraft.maxValidationReworkCyclesInput ?? "5");
  const [operatorApprovalRequired, setOperatorApprovalRequired] = useState(savedDraft.operatorApprovalRequired ?? false);
  const [useGlobalModel, setUseGlobalModel] = useState(savedDraft.useGlobalModel ?? true);
  const [globalSelection, setGlobalSelection] = useState<ModelSelectionDraft>(savedDraft.globalSelection ?? { model: "", thinkingEffort: "" });
  const [showAgentModelOverrides, setShowAgentModelOverrides] = useState(false);
  const [agentSelectionsByRole, setAgentSelectionsByRole] = useState<Record<string, ModelSelectionDraft>>(savedDraft.agentSelectionsByRole ?? {});
  const [dispatchSession, setDispatchSession] = useState<DispatchSession | null>(restoredDispatch);
  const dispatchSessionRef = useRef<DispatchSession | null>(dispatchSession);
  const [sourceMode, setSourceMode] = useState(origin.kind);

  const createWorkOrder = useCreateWorkOrder();
  const createRun = useCreateRun();
  const workOrderRecovery = usePlanningMutationJournal("work-order-dispatch", (body: CreateWorkOrderRequest, key) => createWorkOrder.mutateAsync({ body, key }), workspace, "work-order-create");
  const runRecovery = usePlanningMutationJournal("run-dispatch", (body: CreateRunRequest, key) => createRun.mutateAsync({ body, key }), workspace, "run-create");
  const recoverableExternalCapture = useRecoverableMutation(
    "external-capture",
    async (intake: components["schemas"]["ExternalIntake"], key) => ({
      captured: await apiFetch<components["schemas"]["CaptureResult"]>("/external-source-captures", {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify(intake),
      }),
      intake,
    }),
  );

  // The work order text is rendered from the snapshot rather than copied from the
  // issue body, so an import that selected comments carries them. Kingdom
  // renders it: the bounds that make it safe are enforced at capture, and the
  // planner already answers the same question about the same snapshot.
  const captureExternalSnapshot = async (intake: components["schemas"]["ExternalIntake"]) => {
    const { captured, intake: capturedIntake } = await recoverableExternalCapture.mutateAsync(intake);
    const [snapshot, workOrderText] = await Promise.all([
      apiFetch<components["schemas"]["ExternalSnapshotResponse"]>(`/external-source-snapshots/${captured.snapshot_id}`),
      apiFetch<components["schemas"]["ExternalWorkOrderText"]>(`/external-source-snapshots/${captured.snapshot_id}/work-order-text`),
    ]);
    return { snapshotId: captured.snapshot_id, issue: snapshot.issue, workOrderText: workOrderText.markdown, intake: capturedIntake };
  };

  const prefillExternalIssue = async (selection: IssueSelection) => {
    setExternalBusy(true);
    setExternalError("");
    try {
      const captured = await captureExternalSnapshot(selection.intake);
      setSourceMode("github");
      setOrigin({ kind: "github", snapshotId: captured.snapshotId, issue: captured.issue, workOrderText: captured.workOrderText, intake: captured.intake });
      setTitle(captured.issue.title);
      setText(captured.workOrderText);
    } catch (error) {
      setExternalError(error instanceof Error ? error.message : "Could not capture GitHub issue");
      throw error;
    } finally {
      setExternalBusy(false);
    }
  };

  const refreshExternalCandidate = async () => {
    if (origin.kind !== "github") return;
    setExternalBusy(true);
    setExternalError("");
    try {
      const candidate = await captureExternalSnapshot(origin.intake);
      setOrigin((current) => current.kind === "github" ? { ...current, candidate } : current);
    } catch (error) {
      setExternalError(error instanceof Error ? error.message : "Could not refresh GitHub issue");
    } finally {
      setExternalBusy(false);
    }
  };
  const selectedPlanSessionId = origin.kind === "planning" ? origin.sessionId : undefined;
  const planningSessionId = selectedPlanSessionId;
  const currentPlanningDraft = usePlanningSessionDraft(planningSessionId);
  const { data: planningSessionsData, hasNextPage: hasMoreSessions, fetchNextPage: loadMoreSessions, isFetchingNextPage: loadingMoreSessions, isError: sessionsError } = usePlanningSessions();
  const sessionsWithPlans = useMemo(
    () => (planningSessionsData?.items ?? []).filter((s) => Boolean(s.current_draft_id)),
    [planningSessionsData?.items]
  );
	const providerModelCatalogController = useProviderModelCatalogController();
	const providerModelCatalog = providerModelCatalogController.state;
	const { data: roles } = useRoles();
	const workspaceCapabilities = useWorkspaceCapabilities();
	const allModels = providerModelCatalog.items;
  const normalizedGlobalSelection = useMemo(() => normalizeModelSelectionDraft(allModels, globalSelection), [allModels, globalSelection]);
  const defaultUseGitWorktree = Boolean(
    workspaceCapabilities.data?.default_publish_mode === "git_worktree" && workspaceCapabilities.data?.git_worktree_available
  );
  const gitWorktreeAvailable = workspaceCapabilities.data?.git_worktree_available ?? false;
  const [useGitWorktree, setUseGitWorktree] = useState(savedDraft.useGitWorktree ?? false);
  const [publishModeInitialized, setPublishModeInitialized] = useState(savedDraft.publishModeInitialized ?? false);

  const orderedRoleGroups = useMemo(() => {
    const grouped = new Map<string, RoleGroup>();
    const eligibleRoles = (roles?.items ?? [])
      .filter((role) => role.agent_model_selector_visible)
      .sort((left, right) => {
        if (left.agent_model_selector_section_order !== right.agent_model_selector_section_order) {
          return left.agent_model_selector_section_order - right.agent_model_selector_section_order;
        }
        if (left.agent_model_selector_role_order !== right.agent_model_selector_role_order) {
          return left.agent_model_selector_role_order - right.agent_model_selector_role_order;
        }
        return left.display_name.localeCompare(right.display_name);
      });

    for (const role of eligibleRoles) {
      const groupKey = role.agent_model_selector_section_id;
      const existing = grouped.get(groupKey);
      if (!existing) {
        grouped.set(groupKey, {
          key: groupKey,
          label: role.agent_model_selector_section_label,
          groupOrder: role.agent_model_selector_section_order,
          roles: [role],
        });
        continue;
      }
      existing.roles.push(role);
    }

    return Array.from(grouped.values()).sort((left, right) => {
      if (left.groupOrder !== right.groupOrder) {
        return left.groupOrder - right.groupOrder;
      }
      return left.label.localeCompare(right.label);
    });
  }, [roles?.items]);

  const orderedRoleIDs = useMemo(() => orderedRoleGroups.flatMap((group) => group.roles.map((role) => role.role_id)), [orderedRoleGroups]);
  const selectedPlanningDraft = currentPlanningDraft.data ?? undefined;
  const selectedDraftKey = [planningSessionId ?? "", selectedPlanningDraft?.planning_draft_id ?? ""].join("::");
  const [appliedDraftKey, setAppliedDraftKey] = useState(savedDraft.appliedDraftKey ?? "");

  const creationDraft = useMemo<CreationDraft>(() => ({
    title, text, origin, appliedDraftKey, skipReviewsAndValidations, runUntilAllApprove,
    maxExecutionReviewCyclesInput, maxValidationReworkCyclesInput, operatorApprovalRequired,
    useGlobalModel, globalSelection, agentSelectionsByRole, useGitWorktree, publishModeInitialized,
  }), [title, text, origin, appliedDraftKey, skipReviewsAndValidations, runUntilAllApprove, maxExecutionReviewCyclesInput, maxValidationReworkCyclesInput, operatorApprovalRequired, useGlobalModel, globalSelection, agentSelectionsByRole, useGitWorktree, publishModeInitialized]);

  useEffect(() => {
    if (dispatchSession?.phase === "completed") return;
    try { sessionStorage.setItem(creationStorageKey, JSON.stringify(creationDraft)); }
    catch { setDispatchError("This browser could not save your draft. Keep this page open until storage is available."); }
  }, [creationStorageKey, creationDraft, dispatchSession?.phase]);

  const setCurrentDispatchSession = (next: DispatchSession | null) => {
    if (next) sessionStorage.setItem(dispatchStorageKey, JSON.stringify(next));
    else sessionStorage.removeItem(dispatchStorageKey);
    dispatchSessionRef.current = next;
    setDispatchSession(next);
  };

  useEffect(() => {
    if (sameModelSelection(globalSelection, normalizedGlobalSelection)) {
      return;
    }
    setGlobalSelection(normalizedGlobalSelection);
  }, [globalSelection, normalizedGlobalSelection]);

  useEffect(() => {
    if (orderedRoleIDs.length === 0) {
      return;
    }
    if (normalizedGlobalSelection.model === "") {
      return;
    }
    setAgentSelectionsByRole((current) => {
      const next = { ...current };
      let changed = false;
      for (const roleID of orderedRoleIDs) {
        if (useGlobalModel) {
          if (!sameModelSelection(next[roleID] ?? { model: "", thinkingEffort: "" }, normalizedGlobalSelection)) {
            next[roleID] = normalizedGlobalSelection;
            changed = true;
          }
          continue;
        }
        const normalizedCurrent = normalizeModelSelectionDraft(allModels, next[roleID] ?? normalizedGlobalSelection);
        if (!sameModelSelection(next[roleID] ?? { model: "", thinkingEffort: "" }, normalizedCurrent)) {
          next[roleID] = normalizedCurrent;
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [allModels, normalizedGlobalSelection, orderedRoleIDs, useGlobalModel]);

  useEffect(() => {
    if (!workspaceCapabilities.data) {
      return;
    }
    if (!publishModeInitialized) {
      setUseGitWorktree(defaultUseGitWorktree);
      setPublishModeInitialized(true);
      return;
    }
    if (!workspaceCapabilities.data.git_worktree_available) {
      setUseGitWorktree(false);
    }
  }, [defaultUseGitWorktree, publishModeInitialized, workspaceCapabilities.data]);

  useEffect(() => {
    if (!planningSessionId || !selectedPlanningDraft || selectedDraftKey === "::") {
      return;
    }
    if (selectedDraftKey === appliedDraftKey) {
      return;
    }
    setTitle(selectedPlanningDraft.work_order_title);
    setText(selectedPlanningDraft.work_order_text);
    setAppliedDraftKey(selectedDraftKey);
  }, [appliedDraftKey, planningSessionId, selectedDraftKey, selectedPlanningDraft]);

  const buildWorkOrderBody = (): CreateWorkOrderRequest | null => {
    if (!title.trim() || !text.trim() || normalizedGlobalSelection.model === "") {
      return null;
    }

		const activeLineage: components["schemas"]["PlanningLineage"] | undefined = origin.kind === "planning" && selectedPlanningDraft
			? {
					planning_session_id: origin.sessionId,
					planning_draft_id: selectedPlanningDraft.planning_draft_id,
				}
      : undefined;

    return {
      work_order_title: title.trim(),
      work_order_text: text.trim(),
      ...(activeLineage ? { planning_lineage: activeLineage } : {}),
      ...(origin.kind === "github" ? { external_source_snapshot_id: origin.snapshotId } : {}),
    };
  };

  const buildRunRequest = (): Omit<CreateRunRequest, "work_order_id"> | null => {
    if (Object.values(agentSelectionsByRole).some(selection => selection.model.trim() && !toRunModelSelectionEntry(allModels, selection))) return null;
    const cleanedRoleSelections = Object.entries(agentSelectionsByRole).reduce<Record<string, RunModelSelectionEntry>>((acc, [roleId, selection]) => {
      const trimmedRole = roleId.trim();
      const entry = toRunModelSelectionEntry(allModels, selection);
      if (trimmedRole !== "" && entry) {
        acc[trimmedRole] = entry;
      }
      return acc;
    }, {});

    const globalEntry = toRunModelSelectionEntry(allModels, normalizedGlobalSelection);
    if (!globalEntry) {
      return null;
    }

    const runRequest: Omit<CreateRunRequest, "work_order_id"> = {
      autonomous_approvals: !operatorApprovalRequired,
      publish_mode: useGitWorktree ? "git_worktree" : "direct",
      model_selection: {
        global: globalEntry,
      },
    };

    if (skipReviewsAndValidations) {
      runRequest.skip_reviews_and_validations = true;
    }

    if (!skipReviewsAndValidations) {
      runRequest.run_until_all_approve = runUntilAllApprove;
      runRequest.max_execution_review_cycles = parseNonNegativeCycleInput(maxExecutionReviewCyclesInput, 5);
      runRequest.max_validation_rework_cycles = parseNonNegativeCycleInput(maxValidationReworkCyclesInput, 5);
    }

    if (!useGlobalModel && Object.keys(cleanedRoleSelections).length > 0) {
      const concreteRoleOverrides = Object.entries(cleanedRoleSelections).reduce<Record<string, RunModelSelectionEntry>>((acc, [roleID, entry]) => {
        if (entry.model !== globalEntry.model || (entry.thinking_effort ?? "") !== (globalEntry.thinking_effort ?? "")) {
          acc[roleID] = entry;
        }
        return acc;
      }, {});
      if (Object.keys(concreteRoleOverrides).length > 0) {
        runRequest.model_selection.role_overrides = concreteRoleOverrides;
      }
    }

    return runRequest;
  };

  const prepareForNextWorkOrder = () => {
    setCurrentDispatchSession(null);
    setTitle("");
    setText("");
    setOrigin({ kind: "manual" });
    setSourceMode("manual");
    setExternalError("");
    setAppliedDraftKey("");
    createWorkOrder.reset();
    createRun.reset();
  };

  const finishDispatch = (session: DispatchSession) => {
    const result = session.completedResult;
    if (!result) return;
    // Retain known success even if storage fails. Retrying this phase only
    // clears local state; it must never enter the mutation recovery path.
    dispatchSessionRef.current = session;
    setDispatchSession(session);
    try {
      sessionStorage.setItem(dispatchStorageKey, JSON.stringify(session));
      sessionStorage.setItem(
        creationStorageKey,
        JSON.stringify({
          ...creationDraft,
          title: "",
          text: "",
          origin: { kind: "manual" },
          appliedDraftKey: "",
        } satisfies CreationDraft),
      );
      prepareForNextWorkOrder();
    } catch {
      setDispatchError("The run was created, but this browser could not clear its saved draft. Keep this page open and retry opening the run.");
      return;
    }
    onDispatched?.(result);
  };

  const issueAndRun = async (event: React.FormEvent) => {
    event.preventDefault();
    setDispatchError("");

    const activeSession = dispatchSessionRef.current;
    if (activeSession?.phase === "completed") {
      finishDispatch(activeSession);
      return;
    }
    if (activeSession?.phase === "submitting_work_order" || activeSession?.phase === "submitting_run") {
      return;
    }

    let session = activeSession;
    let nextRunRequest: Omit<CreateRunRequest, "work_order_id"> | undefined;
    let nextRunKey: string | undefined;

    if (session?.phase === "run_recovery") {
      createRun.reset();
      if (session.runRecoveryMode !== "fresh") {
        nextRunRequest = session.runRequest;
        nextRunKey = session.runKey;
      } else {
        const recoveryRunRequest = buildRunRequest();
        if (!recoveryRunRequest) {
          return;
        }
        nextRunRequest = recoveryRunRequest;
        nextRunKey = newIdempotencyKey("run");
      }
      session = {
        ...session,
        phase: "submitting_run",
        runRequest: nextRunRequest,
        runKey: nextRunKey,
        ...(session.runRecoveryMode === "fresh" ? { runBinding: currentAuthBinding(), runGeneration: currentAuthGeneration() } : {}),
        runRecoveryMode: undefined,
      };
      setCurrentDispatchSession(session);
    } else {
      const initialRunRequest = session?.runRequest ?? buildRunRequest();
      if (!initialRunRequest) {
        return;
      }
      nextRunRequest = initialRunRequest;
    }

    if (!session) {
      const workOrderBody = buildWorkOrderBody();
      if (!workOrderBody) {
        return;
      }
      createWorkOrder.reset();
      createRun.reset();
      session = {
        phase: "submitting_work_order",
        draftScope,
        workOrderKey: newIdempotencyKey("work-order"),
        workOrderBinding: currentAuthBinding(),
        workOrderGeneration: currentAuthGeneration(),
        workOrderBody,
        runRequest: nextRunRequest,
      };
      setCurrentDispatchSession(session);
    } else if (session.phase === "work_order_recovery") {
      createWorkOrder.reset();
      createRun.reset();
      if (session.workOrderRecoveryMode === "fresh") {
        const workOrderBody = buildWorkOrderBody();
        if (!workOrderBody) {
          return;
        }
        session = {
          phase: "submitting_work_order",
          draftScope,
          workOrderKey: newIdempotencyKey("work-order"),
        workOrderBinding: currentAuthBinding(),
        workOrderGeneration: currentAuthGeneration(),
          workOrderBody,
        };
      } else {
        session = {
          ...session,
          phase: "submitting_work_order",
          workOrderRecoveryMode: undefined,
        };
      }
      setCurrentDispatchSession(session);
    } else {
      createRun.reset();
    }

    if (!session.workOrderId) {
      try {
        const workOrder = await workOrderRecovery.mutateAsync(session.workOrderBody, session.workOrderKey, { binding: session.workOrderBinding, generation: session.workOrderGeneration, restored: session.restored ?? false });
        session = {
          ...session,
          phase: "submitting_run",
          workOrderId: workOrder.work_order_id,
          workOrderRecoveryMode: undefined,
        };
        setCurrentDispatchSession(session);
      } catch (error) {
        setDispatchError(error instanceof Error ? error.message : "The work order outcome could not be confirmed.");
        setCurrentDispatchSession({
          ...session,
          phase: "work_order_recovery",
          workOrderRecoveryMode: classifyRecoveryMode(error),
          workOrderId: undefined,
          runKey: undefined,
          runRequest: nextRunRequest,
          runRecoveryMode: undefined,
        });
        return;
      }
    }

    if (!nextRunRequest) {
      const latestRunRequest = session.runRequest ?? buildRunRequest();
      if (!latestRunRequest) {
        setCurrentDispatchSession(null);
        return;
      }
      nextRunRequest = latestRunRequest;
    }
    if (!nextRunKey) {
      nextRunKey = session.runKey ?? newIdempotencyKey("run");
    }

    const workOrderId = session.workOrderId;
    const runRequest = buildRunStartRequest(workOrderId, nextRunRequest);
    if (!workOrderId || !runRequest || !nextRunKey) {
      setCurrentDispatchSession(null);
      return;
    }

    session = {
      ...session,
      phase: "submitting_run",
      runRequest: nextRunRequest,
      runKey: nextRunKey,
      runBinding: session.runBinding ?? currentAuthBinding(),
      runGeneration: session.runGeneration ?? currentAuthGeneration(),
      runRecoveryMode: undefined,
    };
    setCurrentDispatchSession(session);

    let run: RunSummary;
    try {
      run = await runRecovery.mutateAsync(runRequest, nextRunKey, { binding: session.runBinding ?? "", generation: session.runGeneration ?? -1, restored: session.restored ?? false });
    } catch (error) {
      setDispatchError(error instanceof Error ? error.message : "The run outcome could not be confirmed.");
      const recoveryMode = classifyRecoveryMode(error);
      setCurrentDispatchSession({
        ...session,
        phase: "run_recovery",
        runRequest: nextRunRequest,
        runKey: nextRunKey,
        runRecoveryMode: recoveryMode,
      });
      return;
    }
    finishDispatch({ ...session, phase: "completed", completedResult: { workOrderId, run } });
  };

  const handleReset = () => {
    setCurrentDispatchSession(null);
	setTitle("");
	setText("");
	setOrigin({ kind: "manual" });
    setSourceMode("manual");
	setExternalError("");
	setAppliedDraftKey("");
	setSkipReviewsAndValidations(false);
	setRunUntilAllApprove(false);
    setMaxExecutionReviewCyclesInput("5");
    setMaxValidationReworkCyclesInput("5");
    setOperatorApprovalRequired(false);
    setUseGitWorktree(defaultUseGitWorktree);
    setPublishModeInitialized(Boolean(workspaceCapabilities.data));
    setUseGlobalModel(true);
    setGlobalSelection({ model: "", thinkingEffort: "" });
    setShowAgentModelOverrides(false);
    setAgentSelectionsByRole({});
    createWorkOrder.reset();
    createRun.reset();
  };

  const handleClearWorkOrder = () => {
    if (dispatchSessionRef.current?.workOrderId) {
      return;
    }
    setTitle("");
    setText("");
	setOrigin({ kind: "manual" });
    setSourceMode("manual");
	setExternalError("");
    setAppliedDraftKey("");
  };

  const hasWorkOrderContent = title.trim() !== "" || text.trim() !== "" || origin.kind !== "manual";

	const dispatchPhase = dispatchSession?.phase ?? null;
	const isCompleted = dispatchPhase === "completed";
	const isSubmitting = dispatchPhase === "submitting_work_order" || dispatchPhase === "submitting_run";
	const canIssueWorkOrder = title.trim() !== "" && text.trim() !== "" && normalizedGlobalSelection.model.trim() !== "";
	const workOrderRecoveryMode = dispatchSession?.workOrderRecoveryMode ?? null;
	const isRecoveringWorkOrder = dispatchPhase === "work_order_recovery";
	const runRecoveryMode = dispatchSession?.runRecoveryMode ?? null;
	const isRecoveringRun = dispatchPhase === "run_recovery" && Boolean(dispatchSession?.workOrderId);
	const isReplayRecovery = (isRecoveringWorkOrder && workOrderRecoveryMode === "replay") || (isRecoveringRun && runRecoveryMode === "replay");
	const isPendingRecovery = (isRecoveringWorkOrder && workOrderRecoveryMode === "pending") || (isRecoveringRun && runRecoveryMode === "pending");
	const canRetryRunStart = isRecoveringRun && (runRecoveryMode !== "fresh"
	  ? Boolean(dispatchSession?.runKey && dispatchSession?.runRequest)
	  : normalizedGlobalSelection.model.trim() !== "");
	const hasIssuedWorkOrder = Boolean(dispatchSession?.workOrderId);
	const workOrderLocked = isSubmitting || hasIssuedWorkOrder || (isRecoveringWorkOrder && workOrderRecoveryMode !== "fresh");
	const runControlsLocked = isCompleted || isSubmitting || isRecoveringWorkOrder || (isRecoveringRun && runRecoveryMode !== "fresh");
	const gitWorktreeToggleDisabled = runControlsLocked || workspaceCapabilities.isLoading || !gitWorktreeAvailable;
	const reviewControlsDisabled = runControlsLocked || skipReviewsAndValidations || runUntilAllApprove;
	const skipToggleDisabled = runControlsLocked;
	const canSubmit = isCompleted || (isRecoveringRun ? canRetryRunStart : isRecoveringWorkOrder && workOrderRecoveryMode !== "fresh" ? true : canIssueWorkOrder);
	const submitDisabled = isSubmitting || !canSubmit;
	const submitLabel = isCompleted ? "Open Run" : isSubmitting
	  ? "Dispatching..."
	  : isRecoveringWorkOrder
	    ? workOrderRecoveryMode !== "fresh"
	      ? `Resolve ${workName} Request`
	      : `Issue ${workName}`
	  : isRecoveringRun
	    ? runRecoveryMode !== "fresh"
	      ? "Retry Run Start"
	      : "Start Run"
	    : `Issue ${workName}`;
  const secondaryActionLabel = hasIssuedWorkOrder ? `Start New ${workName}` : "Reset";
  const secondaryActionHandler = handleReset;
  const secondaryActionVariant = "ghost";
	const gitWorktreeStatus = workspaceCapabilities.isLoading
	  ? "Checking whether Git worktree is available..."
	  : workspaceCapabilities.error
	    ? "Could not check Git worktree availability right now."
	    : workspaceCapabilities.data?.git_worktree_reason?.trim() || "";

	const handleSkipReviewsAndValidations = (next: boolean) => {
	  if (next) {
	    setRunUntilAllApprove(false);
	  }
	  setSkipReviewsAndValidations(next);
	};

  const promoteExternalCandidate = ({ applyTitle = false, applyBody = false }: { applyTitle?: boolean; applyBody?: boolean } = {}) => {
    if (origin.kind !== "github" || !origin.candidate) return;
    const candidate = origin.candidate;
    setOrigin({
      kind: "github",
      snapshotId: candidate.snapshotId,
      issue: candidate.issue,
      workOrderText: candidate.workOrderText,
      intake: origin.intake,
    });
    if (applyTitle) setTitle(candidate.issue.title);
    if (applyBody) setText(candidate.workOrderText);
  };

  // Removing the source leaves the form as if nothing had been imported. The
  // plan selector already clears title and text when it drops back to "No
  // plan"; a half-cleared form carrying text the operator believes they
  // discarded is worse than an empty one.
  const clearExternalSource = () => {
    setOrigin({ kind: "manual" });
    setTitle("");
    setText("");
    setExternalError("");
  };

  return (
    <div className="space-y-8">
      <header><h1 className="font-display text-4xl">New {workName}</h1><p className="mt-3 max-w-2xl text-sm leading-relaxed text-foreground-muted">Give the team a clear brief, choose how it should run, and follow the work from start to finish.</p></header>
        <div>
          <form onSubmit={(event) => { void issueAndRun(event).catch((error: unknown) => setDispatchError(error instanceof Error ? error.message : "The dispatch request could not be saved.")); }} className="space-y-7">
            <div className="grid gap-4 sm:grid-cols-[180px_minmax(0,1fr)] sm:items-start">
              <SelectField id="work-source" label="Work Source" value={sourceMode} disabled={workOrderLocked} options={[{ label: "Write a Brief", value: "manual" }, { label: "Saved Plan", value: "planning" }, { label: "GitHub Issue", value: "github" }]} onChange={(value) => { setSourceMode(value as typeof sourceMode); if (value !== origin.kind) { setOrigin({ kind: "manual" }); setAppliedDraftKey(""); } }} />
              {sourceMode === "github" && !workOrderLocked ? <div className="min-w-0 sm:pt-6"><IssuePicker actionLabel={`Prefill ${workName}`} onSelect={prefillExternalIssue} /></div> : null}
              {sourceMode === "planning" && <div className="min-w-0">              <div className="min-w-0">
                  <SelectField
                    id="plan-selector"
                    label="From Plan"
                    value={selectedPlanSessionId ?? ""}
                    placeholder="No plan"
                    disabled={workOrderLocked || sessionsWithPlans.length === 0}
                    options={[
                      { label: "No plan", value: "" },
                      ...sessionsWithPlans.map((s) => ({
                        label: s.title,
                        value: s.planning_session_id,
                      })),
                    ]}
                    onChange={(next) => {
                      if (next) {
                        setOrigin({ kind: "planning", sessionId: next });
                        setAppliedDraftKey("");
                        setExternalError("");
                      } else {
                        setOrigin({ kind: "manual" });
                        setTitle("");
                        setText("");
                        setAppliedDraftKey("");
                      }
                    }}
                  />
                  {hasMoreSessions ? (
                    <Button type="button" size="sm" variant="ghost" className="mt-1" disabled={loadingMoreSessions || workOrderLocked} onClick={() => void loadMoreSessions()}>
                      {loadingMoreSessions ? "Loading sessions..." : sessionsError ? "Retry Loading Sessions" : "Load Older Sessions"}
                    </Button>
                  ) : null}
              </div>
</div>}
            </div>
            {origin.kind === "github" ? (
              <ExternalSourceStrip
                label="Selected GitHub source"
                identity={<>Imported from <ExternalSourceCoordinate canonicalURL={origin.issue.canonical_url} repository={origin.issue.repository} issueNumber={origin.issue.issue_number} /></>}
                actions={
                  <>
                    <Button type="button" variant="ghost" size="sm" disabled={externalBusy || workOrderLocked} onClick={() => void refreshExternalCandidate()}>{externalBusy ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}Check for Updates</Button>
                    <Button type="button" variant="ghost" size="sm" disabled={workOrderLocked} onClick={clearExternalSource}>Remove Source</Button>
                  </>
                }
              >
                {origin.candidate?.snapshotId === origin.snapshotId ? <p role="status" className="text-foreground-muted">GitHub is up to date with the selected source.</p> : null}
                {origin.candidate && origin.candidate.snapshotId !== origin.snapshotId ? (
                  <div className="border-t border-accent/15 pt-3">
                    <p className="font-medium text-foreground">An update is available. Choose which refreshed fields to apply; every choice also promotes its provenance.</p>
                    <ExternalSourceComparison baselineId={origin.snapshotId} candidateId={origin.candidate.snapshotId} />
                    <div className="mt-3 flex flex-wrap gap-2">
                      <Button type="button" size="sm" variant="outline" onClick={() => promoteExternalCandidate()}>Use Source, Keep Edits</Button>
                      <Button type="button" size="sm" variant="outline" onClick={() => promoteExternalCandidate({ applyTitle: true })}>Use Refreshed Title</Button>
                      <Button type="button" size="sm" variant="outline" onClick={() => promoteExternalCandidate({ applyBody: true })}>Use Refreshed Body</Button>
                      <Button type="button" size="sm" onClick={() => promoteExternalCandidate({ applyTitle: true, applyBody: true })}>Use All Refreshed Fields</Button>
                    </div>
                  </div>
                ) : null}
              </ExternalSourceStrip>
            ) : null}
            {externalError ? <p role="alert" className="text-xs text-status-failed">{externalError}</p> : null}
            <div className="grid min-w-0 gap-3 md:grid-cols-[minmax(0,1fr)_auto] md:items-end">
              <div className="min-w-0">
                <label htmlFor="work-order-title" className="mb-1.5 block text-sm font-medium">
                  Title
                </label>
                <input data-ui-field=""
                  id="work-order-title"
                  type="text"
                  value={title}
                  disabled={workOrderLocked}
                  onChange={(e) => setTitle(e.target.value)}
                  placeholder={`Name your ${workName.toLowerCase()}...`}
                  className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-foreground-subtle focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
                />
              </div>
                {hasWorkOrderContent ? (
                  <div className="flex items-center justify-end md:flex-col md:gap-0.5">
                    <span className="mr-1 text-[10px] text-foreground-subtle md:mr-0">Clear</span>
                    <button
                      type="button"
                      onClick={handleClearWorkOrder}
                      aria-label="Clear title and brief"
                      disabled={workOrderLocked}
                      className="rounded-md p-2 text-foreground-subtle transition-colors hover:bg-surface-raised hover:text-foreground disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      <X className="h-4 w-4" />
                    </button>
                  </div>
                ) : <span />}
            </div>

            <div>
              <label htmlFor="work-order-text" className="mb-1.5 block text-sm font-medium">
                Work Brief
              </label>
              <textarea data-ui-field=""
                id="work-order-text"
                value={text}
                disabled={workOrderLocked}
                onChange={(e) => setText(e.target.value)}
                placeholder="Describe the outcome, constraints, and how you’ll know the work is done…"
                rows={10}
                className="w-full resize-y rounded-xl border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-foreground-subtle focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
              />
            </div>

            <section aria-labelledby="work-model-heading" className="space-y-5 border-t border-border pt-7">
              <div>
                <h2 id="work-model-heading" className="font-display text-2xl">Model</h2>
                <p className="mt-1 text-sm text-foreground-muted">Choose the model the team will use.</p>
              </div>
              <ProviderModelCatalogNotice state={providerModelCatalog} onRetry={providerModelCatalogController.retry} retryPending={providerModelCatalogController.retryPending} />
              <div className="grid gap-4 sm:grid-cols-2">
                  <ModelSelectField
                    id="global-model"
                    label="Global Model"
                    value={normalizedGlobalSelection.model}
					loading={providerModelCatalog.isLoading}
                    disabled={runControlsLocked || providerModelCatalog.disableSelection}
                    options={allModels}
                    triggerPlaceholder={providerModelCatalog.triggerPlaceholder}
                    emptyStateLabel={providerModelCatalog.emptyStateLabel}
                    onChange={(next) => {
                      const nextSelection = normalizeModelSelectionDraft(allModels, { ...normalizedGlobalSelection, model: next });
                      setGlobalSelection(nextSelection);
                      if (useGlobalModel) {
                        setAgentSelectionsByRole((current) => {
                          const merged = { ...current };
                          for (const roleID of orderedRoleIDs) {
                            merged[roleID] = nextSelection;
                          }
                          return merged;
                        });
                      }
                    }}
                  />
                  <ThinkingEffortField
                    id="global-thinking-effort"
                    label="Thinking Effort"
                    value={normalizedGlobalSelection.thinkingEffort}
                    model={findProviderModel(allModels, normalizedGlobalSelection.model)}
                    disabled={runControlsLocked || providerModelCatalog.disableSelection}
                    onChange={(next) => {
                      const nextSelection = normalizeModelSelectionDraft(allModels, { ...normalizedGlobalSelection, thinkingEffort: next });
                      setGlobalSelection(nextSelection);
                      if (useGlobalModel) {
                        setAgentSelectionsByRole((current) => {
                          const merged = { ...current };
                          for (const roleID of orderedRoleIDs) {
                            merged[roleID] = nextSelection;
                          }
                          return merged;
                        });
                      }
                    }}
                  />
              </div>
              <button type="button" aria-expanded={showAgentModelOverrides} aria-controls="role-model-options" disabled={runControlsLocked} onClick={() => setShowAgentModelOverrides((value) => !value)} className="flex min-h-10 items-center gap-2 text-sm text-foreground-muted hover:text-foreground disabled:opacity-50">
                <ChevronDown aria-hidden="true" className={`h-3.5 w-3.5 transition-transform ${showAgentModelOverrides ? "rotate-180" : ""}`} />
                Customize Role Models{!useGlobalModel ? " · Custom" : ""}
              </button>
              {showAgentModelOverrides && <div id="role-model-options" className="space-y-6 border-l border-border pl-5">
                  <ToggleRow
                    label="Use for All Agents"
                    checked={useGlobalModel}
                    onChange={(next) => {
                      setUseGlobalModel(next);
                      if (next) {
                        setAgentSelectionsByRole((current) => {
                          const merged = { ...current };
                          for (const roleID of orderedRoleIDs) {
                            merged[roleID] = normalizedGlobalSelection;
                          }
                          return merged;
                        });
                      }
                    }}
                    disabled={runControlsLocked}
                    info="When enabled, every visible role inherits the same model selection unless you diverge a role below."
                  />
                  {orderedRoleGroups.map((group) => (
                      <div key={group.key} className="space-y-2">
                        <h4 className="text-sm font-medium text-foreground-muted">{group.label}</h4>
                        <div className="space-y-2">
                          {group.roles.map((role) => {
                            const roleSelection = normalizeModelSelectionDraft(allModels, agentSelectionsByRole[role.role_id] ?? normalizedGlobalSelection);
                            return (
                              <div key={role.role_id} className="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:items-end">
                                <ModelSelectField
                                  id={`role-model-${role.role_id}`}
                                  label={roleIdentity(theme, role.role_id).name}
                                  value={roleSelection.model}
								  loading={providerModelCatalog.isLoading}
                                  disabled={runControlsLocked || providerModelCatalog.disableSelection}
                                  options={allModels}
                                  triggerPlaceholder={providerModelCatalog.triggerPlaceholder}
                                  emptyStateLabel={providerModelCatalog.emptyStateLabel}
                                  onChange={(next) => {
                                    const nextSelection = normalizeModelSelectionDraft(allModels, { ...roleSelection, model: next });
                                    setAgentSelectionsByRole((current) => ({ ...current, [role.role_id]: nextSelection }));
                                    if (useGlobalModel && !sameModelSelection(nextSelection, normalizedGlobalSelection)) {
                                      setUseGlobalModel(false);
                                    }
                                  }}
                                />
                                <ThinkingEffortField
                                  id={`role-thinking-effort-${role.role_id}`}
                                  label="Thinking Effort"
                                  value={roleSelection.thinkingEffort}
                                  model={findProviderModel(allModels, roleSelection.model)}
                                  disabled={runControlsLocked || providerModelCatalog.disableSelection}
                                  onChange={(next) => {
                                    const nextSelection = normalizeModelSelectionDraft(allModels, { ...roleSelection, thinkingEffort: next });
                                    setAgentSelectionsByRole((current) => ({ ...current, [role.role_id]: nextSelection }));
                                    if (useGlobalModel && !sameModelSelection(nextSelection, normalizedGlobalSelection)) {
                                      setUseGlobalModel(false);
                                    }
                                  }}
                                />
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    ))}
              </div>}
            </section>
            <section aria-labelledby="work-options-heading" className="space-y-5 border-t border-border pt-7">
              <h2 id="work-options-heading" className="font-display text-2xl">Run Options</h2>
              <ToggleRow label="Require Human Approval" checked={operatorApprovalRequired} onChange={setOperatorApprovalRequired} disabled={runControlsLocked} description="Pause for your decision before implementation and final completion." />
              <div className="space-y-2">
                <ToggleRow label="Use Git Worktree" checked={useGitWorktree} onChange={setUseGitWorktree} disabled={gitWorktreeToggleDisabled} description="Keep changes in a separate Git working copy. When off, publish into this project." />
                {gitWorktreeStatus ? <p className="text-xs text-foreground-muted">{gitWorktreeStatus}</p> : null}
              </div>
              <details className="text-sm">
                <summary className="min-h-10 cursor-pointer text-foreground-muted">
                  <span>Review Options</span>
                  <span className="ml-2 text-foreground">{skipReviewsAndValidations ? "Skipped" : runUntilAllApprove ? "Until All Approve" : `${maxExecutionReviewCyclesInput} Review / ${maxValidationReworkCyclesInput} Validation Cycles`}</span>
                </summary>
                <div className="space-y-4 border-l border-border pl-5 pt-2">
                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
	                  <ToggleRow
	                    label="Skip Reviews and Validations"
	                    checked={skipReviewsAndValidations}
	                    onChange={handleSkipReviewsAndValidations}
	                    disabled={skipToggleDisabled}
	                    info="Skip review and validation for this work order."
	                  />
	                  <ToggleRow
	                    label="Run Until All Approve"
	                    checked={runUntilAllApprove}
	                    onChange={setRunUntilAllApprove}
	                    disabled={runControlsLocked || skipReviewsAndValidations}
	                    info="Continue review and validation loops until they pass."
	                  />
	                </div>

	                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 lg:items-end">
	                  <div className={reviewControlsDisabled ? "opacity-55" : ""}>
	                    <label htmlFor="max-review-cycles" className="mb-1.5 block text-sm font-medium">
	                      Max Review Cycles
	                    </label>
                    <input data-ui-field=""
                      id="max-review-cycles"
                      type="number"
	                      min={0}
                       value={maxExecutionReviewCyclesInput}
                       onChange={(e) => setMaxExecutionReviewCyclesInput(e.target.value)}
	                      onBlur={() => setMaxExecutionReviewCyclesInput(normalizeNonNegativeCycleInput(maxExecutionReviewCyclesInput, 5))}
	                      disabled={reviewControlsDisabled}
	                      className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
	                    />
	                  </div>
	                  <div className={reviewControlsDisabled ? "opacity-55" : ""}>
	                    <label htmlFor="max-validation-cycles" className="mb-1.5 block text-sm font-medium">
	                      Max Validation Cycles
	                    </label>
                    <input data-ui-field=""
                      id="max-validation-cycles"
                      type="number"
	                      min={0}
                       value={maxValidationReworkCyclesInput}
                       onChange={(e) => setMaxValidationReworkCyclesInput(e.target.value)}
	                      onBlur={() => setMaxValidationReworkCyclesInput(normalizeNonNegativeCycleInput(maxValidationReworkCyclesInput, 5))}
	                      disabled={reviewControlsDisabled}
	                      className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
	                    />
	                  </div>
	                </div>

	                {!skipReviewsAndValidations && !runUntilAllApprove && (
	                  <p className="text-xs text-foreground-subtle">
	                    Set either cap to 0 to auto-succeed the first ready review or validation wave without provider dispatch.
	                  </p>
	                )}

	                {skipReviewsAndValidations && (
	                  <div className="flex items-start gap-2 rounded-md border border-accent/35 bg-accent/10 px-3 py-2 text-xs text-accent">
	                    <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                    <span>Review and validation are skipped. Builder output proceeds directly to completion for this run.</span>
	                  </div>
	                )}

	                {runUntilAllApprove && !skipReviewsAndValidations && (
	                  <div className="flex items-start gap-2 rounded-md border border-accent/35 bg-accent/10 px-3 py-2 text-xs text-accent">
	                    <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                    <span>Uses more tokens: review and validation keep running until they pass.</span>
                  </div>
                )}

                </div>
              </details>
            </section>
	            {isRecoveringWorkOrder ? (
	              <div className="flex items-start gap-2 rounded-md border border-accent/35 bg-accent/10 px-3 py-2 text-xs text-accent">
	                <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                <span>
	                  {workOrderRecoveryMode === "pending"
	                    ? <>Kingdom is still resolving this work order request. Retry will reuse the same request and idempotency key; starting over with a new key is unsafe while the original outcome remains pending.</>
	                    : workOrderRecoveryMode === "replay"
	                      ? <>Kingdom may have accepted this work order, but the connection dropped before the response arrived. Retry will reuse the same work order request and idempotency key so Kingdom can recover without duplicating work orders.</>
	                      : <>Kingdom rejected the work order request before it was accepted. Update the work order fields if needed, then issue the work order again.</>}
	                </span>
	              </div>
	            ) : null}
	            {isRecoveringRun && dispatchSession?.workOrderId ? (
	              <div className="flex items-start gap-2 rounded-md border border-accent/35 bg-accent/10 px-3 py-2 text-xs text-accent">
	                <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                <span>
	                  {runRecoveryMode === "pending"
	                    ? <>Work Order <span className="font-mono">{dispatchSession.workOrderId}</span> was issued and the run start remains pending. Retry will reuse the same request and idempotency key; starting over with a new key is unsafe.</>
	                    : runRecoveryMode === "replay"
	                      ? <>Work Order <span className="font-mono">{dispatchSession.workOrderId}</span> was issued, but the connection dropped before Kingdom could confirm whether the run started. Retry will reuse the same run request and idempotency key so Kingdom can recover without duplicating work.</>
	                      : <>Work Order <span className="font-mono">{dispatchSession.workOrderId}</span> was issued, but Kingdom rejected the run start request before it was accepted. Update the run controls if needed, or start a new work order.</>}
	                </span>
	              </div>
	            ) : null}
	            {(createWorkOrder.error || createRun.error) && (
	              <ErrorBanner message={(createWorkOrder.error ?? createRun.error)?.message ?? "Request failed"} />
	            )}

            {dispatchSession && (isRecoveringWorkOrder || isRecoveringRun) && <details className="rounded-lg border border-border px-4 py-3 text-xs text-foreground-muted"><summary className="cursor-pointer font-medium">Recovery Details</summary><dl className="mt-3 grid gap-2"><dt>Request</dt><dd className="break-all font-mono">{isRecoveringRun ? dispatchSession.runKey : dispatchSession.workOrderKey}</dd><dt>Work Title</dt><dd>{dispatchSession.workOrderBody.work_order_title}</dd><dt>Original Model</dt><dd className="break-all font-mono">{dispatchSession.runRequest?.model_selection?.global?.model}</dd></dl></details>}
            {isCompleted && <p role="status" className="text-sm text-foreground-muted">The run has already been created. Opening it will only clear this saved draft.</p>}
            {dispatchError && <div role="alert"><ErrorBanner message={dispatchError} /></div>}
            <div className="flex flex-wrap items-center gap-2 border-t border-border pt-6">
	              <Button type="submit" disabled={submitDisabled}>
	                {submitLabel}
              </Button>
	              {!isCompleted && !isPendingRecovery && !isReplayRecovery ? (
	                <Button type="button" variant={secondaryActionVariant} onClick={secondaryActionHandler} disabled={isSubmitting}>
	                  {secondaryActionLabel}
	                </Button>
	              ) : null}
            </div>
          </form>
        </div>
    </div>
  );
}
