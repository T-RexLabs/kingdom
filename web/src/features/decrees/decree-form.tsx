import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError, apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import {
  type CreateDecreeRequest,
  newIdempotencyKey,
  usePlanningSessionDraft,
  usePlanningSessions,
  useCreateDecree,
  useCreateRun,
  useRoles,
  useWorkspaceCapabilities,
  type CreateRunRequest,
  type ProviderModel,
  type RunModelSelectionEntry,
  type RunSummary,
  type Role,
} from "@/api/hooks";
import { ChevronDown, ChevronUp, CircleAlert, Loader2, Play, RefreshCw, Scroll, Send, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { SelectField } from "@/components/ui/select-field";
import { ModelSelectField } from "./model-select-field";
import { findProviderModel, normalizeThinkingEffortSelection } from "./provider-model-selection";
import { ThinkingEffortField } from "./thinking-effort-field";
import { ToggleRow } from "@/components/ui/toggle-row";
import { ProviderModelCatalogNotice } from "./provider-model-catalog-state";
import { useProviderModelCatalogController } from "./use-provider-model-catalog";
import { IssuePicker, type IssueSelection } from "@/features/connections/issue-picker";
import { ExternalSourceComparison } from "@/features/connections/external-source-comparison";
import { ExternalSourceCoordinate, ExternalSourceStrip } from "@/features/connections/external-source-strip";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import type { DecreePlanningLineageSearch } from "./decree-route-search";
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
  if (normalized.thinkingEffort === "") {
    return { model: normalized.model };
  }
  return { model: normalized.model, thinking_effort: normalized.thinkingEffort };
}

function ErrorBanner({ message }: { message: string }) {
  return <div className="rounded-md border border-status-failed/30 bg-status-failed/10 px-3 py-2 text-sm text-status-failed">{message}</div>;
}

type CapturedExternalSource = {
  snapshotId: string;
  issue: components["schemas"]["NormalizedIssue"];
  /** The issue body plus its imported comments, as Kingdom renders it. */
  decreeText: string;
};

type DecreeOrigin =
  | { kind: "manual" }
  | { kind: "planning"; sessionId: string; routeLineage?: DecreePlanningLineageSearch }
  | {
      kind: "github";
      snapshotId: string;
      issue: components["schemas"]["NormalizedIssue"];
      decreeText: string;
      intake: components["schemas"]["ExternalIntake"];
      candidate?: CapturedExternalSource;
    };

export type DecreeDispatchResult = {
  decreeId: string;
  run: RunSummary;
};

type DispatchPhase = "submitting_decree" | "decree_recovery" | "submitting_run" | "run_recovery";

type RecoveryMode = "fresh" | "replay" | "pending";

type DispatchSession = {
  phase: DispatchPhase;
  decreeKey: string;
  decreeBody: CreateDecreeRequest;
  decreeRecoveryMode?: RecoveryMode;
  decreeId?: string;
  runKey?: string;
  runRequest?: Omit<CreateRunRequest, "decree_id">;
  runRecoveryMode?: RecoveryMode;
};

function buildRunStartRequest(decreeId: string | undefined, runRequest: Omit<CreateRunRequest, "decree_id"> | undefined): CreateRunRequest | null {
  if (!decreeId || !runRequest) {
    return null;
  }
  return {
    decree_id: decreeId,
    ...runRequest,
  };
}

function classifyRecoveryMode(error: unknown): RecoveryMode {
  if (error instanceof ApiError && error.code === "idempotency_pending") return "pending";
  return isAmbiguousMutationFailure(error) ? "replay" : "fresh";
}

export function DecreeForm({
  initialPlanningLineage,
  onDispatched,
}: {
  initialPlanningLineage?: Partial<DecreePlanningLineageSearch>;
  onDispatched?: (result: DecreeDispatchResult) => void;
}) {
  const [title, setTitle] = useState("");
  const [text, setText] = useState("");
  const [origin, setOrigin] = useState<DecreeOrigin>(() => initialPlanningLineage?.planning_session_id && initialPlanningLineage.planning_draft_id
    ? { kind: "planning", sessionId: initialPlanningLineage.planning_session_id, routeLineage: initialPlanningLineage as DecreePlanningLineageSearch }
    : { kind: "manual" });
  const [externalBusy, setExternalBusy] = useState(false);
  const [externalError, setExternalError] = useState("");
  const [skipReviewsAndValidations, setSkipReviewsAndValidations] = useState(false);
  const [runUntilAllApprove, setRunUntilAllApprove] = useState(false);
  const [maxExecutionReviewCyclesInput, setMaxExecutionReviewCyclesInput] = useState("5");
  const [maxValidationReworkCyclesInput, setMaxValidationReworkCyclesInput] = useState("5");
  const [allfatherApprovalRequired, setAllfatherApprovalRequired] = useState(false);
  const [useGlobalModel, setUseGlobalModel] = useState(true);
  const [globalSelection, setGlobalSelection] = useState<ModelSelectionDraft>({ model: "", thinkingEffort: "" });
  const [showAgentModelOverrides, setShowAgentModelOverrides] = useState(false);
  const [agentSelectionsByRole, setAgentSelectionsByRole] = useState<Record<string, ModelSelectionDraft>>({});
  const [dispatchSession, setDispatchSession] = useState<DispatchSession | null>(null);
  const dispatchSessionRef = useRef<DispatchSession | null>(null);

  const createDecree = useCreateDecree();
  const createRun = useCreateRun();
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

  // The decree text is rendered from the snapshot rather than copied from the
  // issue body, so an import that selected comments carries them. Kingdom
  // renders it: the bounds that make it safe are enforced at capture, and the
  // planner already answers the same question about the same snapshot.
  const captureExternalSnapshot = async (intake: components["schemas"]["ExternalIntake"]) => {
    const { captured, intake: capturedIntake } = await recoverableExternalCapture.mutateAsync(intake);
    const [snapshot, decreeText] = await Promise.all([
      apiFetch<components["schemas"]["ExternalSnapshotResponse"]>(`/external-source-snapshots/${captured.snapshot_id}`),
      apiFetch<components["schemas"]["ExternalDecreeText"]>(`/external-source-snapshots/${captured.snapshot_id}/decree-text`),
    ]);
    return { snapshotId: captured.snapshot_id, issue: snapshot.issue, decreeText: decreeText.markdown, intake: capturedIntake };
  };

  const prefillExternalIssue = async (selection: IssueSelection) => {
    setExternalBusy(true);
    setExternalError("");
    try {
      const captured = await captureExternalSnapshot(selection.intake);
      setOrigin({ kind: "github", snapshotId: captured.snapshotId, issue: captured.issue, decreeText: captured.decreeText, intake: captured.intake });
      setTitle(captured.issue.title);
      setText(captured.decreeText);
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
  const { data: planningSessionsData } = usePlanningSessions();
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
  const [useGitWorktree, setUseGitWorktree] = useState(false);
  const [publishModeInitialized, setPublishModeInitialized] = useState(false);

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
  const [appliedDraftKey, setAppliedDraftKey] = useState("");

  const setCurrentDispatchSession = (next: DispatchSession | null) => {
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
    setTitle(selectedPlanningDraft.decree_title);
    setText(selectedPlanningDraft.decree_text);
    setAppliedDraftKey(selectedDraftKey);
  }, [appliedDraftKey, planningSessionId, selectedDraftKey, selectedPlanningDraft]);

  const buildDecreeBody = (): CreateDecreeRequest | null => {
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
      decree_title: title.trim(),
      decree_text: text.trim(),
      ...(activeLineage ? { planning_lineage: activeLineage } : {}),
      ...(origin.kind === "github" ? { external_source_snapshot_id: origin.snapshotId } : {}),
    };
  };

  const buildRunRequest = (): Omit<CreateRunRequest, "decree_id"> | null => {
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

    const runRequest: Omit<CreateRunRequest, "decree_id"> = {
      autonomous_approvals: !allfatherApprovalRequired,
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

  const prepareForNextDecree = () => {
    setCurrentDispatchSession(null);
    setTitle("");
    setText("");
    setOrigin({ kind: "manual" });
    setExternalError("");
    setAppliedDraftKey("");
    createDecree.reset();
    createRun.reset();
  };

  const issueAndRun = async (event: React.FormEvent) => {
    event.preventDefault();

    const activeSession = dispatchSessionRef.current;
    if (activeSession?.phase === "submitting_decree" || activeSession?.phase === "submitting_run") {
      return;
    }

    let session = activeSession;
    let nextRunRequest: Omit<CreateRunRequest, "decree_id"> | undefined;
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
        runRecoveryMode: undefined,
      };
      setCurrentDispatchSession(session);
    } else {
      const initialRunRequest = buildRunRequest();
      if (!initialRunRequest) {
        return;
      }
      nextRunRequest = initialRunRequest;
    }

    if (!session) {
      const decreeBody = buildDecreeBody();
      if (!decreeBody) {
        return;
      }
      createDecree.reset();
      createRun.reset();
      session = {
        phase: "submitting_decree",
        decreeKey: newIdempotencyKey("decree"),
        decreeBody,
      };
      setCurrentDispatchSession(session);
    } else if (session.phase === "decree_recovery") {
      createDecree.reset();
      createRun.reset();
      if (session.decreeRecoveryMode === "fresh") {
        const decreeBody = buildDecreeBody();
        if (!decreeBody) {
          return;
        }
        session = {
          phase: "submitting_decree",
          decreeKey: newIdempotencyKey("decree"),
          decreeBody,
        };
      } else {
        session = {
          ...session,
          phase: "submitting_decree",
          decreeRecoveryMode: undefined,
        };
      }
      setCurrentDispatchSession(session);
    } else {
      createRun.reset();
    }

    if (!session.decreeId) {
      try {
        const decree = await createDecree.mutateAsync({
          body: session.decreeBody,
          key: session.decreeKey,
        });
        session = {
          ...session,
          phase: "submitting_run",
          decreeId: decree.decree_id,
          decreeRecoveryMode: undefined,
        };
        setCurrentDispatchSession(session);
      } catch (error) {
        setCurrentDispatchSession({
          ...session,
          phase: "decree_recovery",
          decreeRecoveryMode: classifyRecoveryMode(error),
          decreeId: undefined,
          runKey: undefined,
          runRequest: undefined,
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

    const decreeId = session.decreeId;
    const runRequest = buildRunStartRequest(decreeId, nextRunRequest);
    if (!decreeId || !runRequest || !nextRunKey) {
      setCurrentDispatchSession(null);
      return;
    }

    session = {
      ...session,
      phase: "submitting_run",
      runRequest: nextRunRequest,
      runKey: nextRunKey,
      runRecoveryMode: undefined,
    };
    setCurrentDispatchSession(session);

    try {
      const run = await createRun.mutateAsync({
        body: runRequest,
        key: nextRunKey,
      });
      prepareForNextDecree();
      onDispatched?.({ decreeId, run });
    } catch (error) {
      const recoveryMode = classifyRecoveryMode(error);
      setCurrentDispatchSession({
        ...session,
        phase: "run_recovery",
        runRequest: nextRunRequest,
        runKey: nextRunKey,
        runRecoveryMode: recoveryMode,
      });
    }
  };

  const handleReset = () => {
    setCurrentDispatchSession(null);
	setTitle("");
	setText("");
	setOrigin({ kind: "manual" });
	setExternalError("");
	setAppliedDraftKey("");
	setSkipReviewsAndValidations(false);
	setRunUntilAllApprove(false);
    setMaxExecutionReviewCyclesInput("5");
    setMaxValidationReworkCyclesInput("5");
    setAllfatherApprovalRequired(false);
    setUseGitWorktree(defaultUseGitWorktree);
    setPublishModeInitialized(Boolean(workspaceCapabilities.data));
    setUseGlobalModel(true);
    setGlobalSelection({ model: "", thinkingEffort: "" });
    setShowAgentModelOverrides(false);
    setAgentSelectionsByRole({});
    createDecree.reset();
    createRun.reset();
  };

  const handleDiscardRecovery = () => {
    setCurrentDispatchSession(null);
    createDecree.reset();
    createRun.reset();
  };

  const handleClearDecree = () => {
    if (dispatchSessionRef.current?.decreeId) {
      return;
    }
    setTitle("");
    setText("");
	setOrigin({ kind: "manual" });
	setExternalError("");
    setAppliedDraftKey("");
  };

  const hasDecreeContent = title.trim() !== "" || text.trim() !== "" || origin.kind !== "manual";

	const dispatchPhase = dispatchSession?.phase ?? null;
	const isSubmitting = dispatchPhase === "submitting_decree" || dispatchPhase === "submitting_run";
	const canIssueDecree = title.trim() !== "" && text.trim() !== "" && normalizedGlobalSelection.model.trim() !== "";
	const decreeRecoveryMode = dispatchSession?.decreeRecoveryMode ?? null;
	const isRecoveringDecree = dispatchPhase === "decree_recovery";
	const runRecoveryMode = dispatchSession?.runRecoveryMode ?? null;
	const isRecoveringRun = dispatchPhase === "run_recovery" && Boolean(dispatchSession?.decreeId);
	const isReplayRecovery = (isRecoveringDecree && decreeRecoveryMode === "replay") || (isRecoveringRun && runRecoveryMode === "replay");
	const isPendingRecovery = (isRecoveringDecree && decreeRecoveryMode === "pending") || (isRecoveringRun && runRecoveryMode === "pending");
	const canRetryRunStart = isRecoveringRun && (runRecoveryMode !== "fresh"
	  ? Boolean(dispatchSession?.runKey && dispatchSession?.runRequest)
	  : normalizedGlobalSelection.model.trim() !== "");
	const hasIssuedDecree = Boolean(dispatchSession?.decreeId);
	const decreeLocked = isSubmitting || hasIssuedDecree || (isRecoveringDecree && decreeRecoveryMode !== "fresh");
	const runControlsLocked = isSubmitting || (isRecoveringRun && runRecoveryMode !== "fresh");
	const gitWorktreeToggleDisabled = runControlsLocked || workspaceCapabilities.isLoading || !gitWorktreeAvailable;
	const reviewControlsDisabled = runControlsLocked || skipReviewsAndValidations || runUntilAllApprove;
	const skipToggleDisabled = runControlsLocked;
	const canSubmit = isRecoveringRun ? canRetryRunStart : canIssueDecree;
	const submitDisabled = isSubmitting || !canSubmit;
	const submitLabel = isSubmitting
	  ? "Dispatching..."
	  : isRecoveringDecree
	    ? decreeRecoveryMode !== "fresh"
	      ? "Retry Decree"
	      : "Issue Decree"
	  : isRecoveringRun
	    ? runRecoveryMode !== "fresh"
	      ? "Retry Run Start"
	      : "Start Run"
	    : "Issue Decree";
	const secondaryActionLabel = isReplayRecovery
	  ? "Discard Recovery"
	  : hasIssuedDecree
	    ? "Start New Decree"
	    : "Reset";
	const secondaryActionHandler = isReplayRecovery ? handleDiscardRecovery : handleReset;
	const secondaryActionVariant = isReplayRecovery ? "destructive" : "ghost";
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
      decreeText: candidate.decreeText,
      intake: origin.intake,
    });
    if (applyTitle) setTitle(candidate.issue.title);
    if (applyBody) setText(candidate.decreeText);
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
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Scroll className="h-4 w-4" />
          Issue Decree
        </CardTitle>
      </CardHeader>
        <CardContent>
          <form onSubmit={issueAndRun} className="space-y-4">
            {!decreeLocked ? <IssuePicker actionLabel="Prefill Decree" onSelect={prefillExternalIssue} /> : null}
            {origin.kind === "github" ? (
              <ExternalSourceStrip
                label="Selected GitHub source"
                identity={<>Imported from <ExternalSourceCoordinate canonicalURL={origin.issue.canonical_url} repository={origin.issue.repository} issueNumber={origin.issue.issue_number} /></>}
                actions={
                  <>
                    <Button type="button" variant="ghost" size="sm" disabled={externalBusy || decreeLocked} onClick={() => void refreshExternalCandidate()}>{externalBusy ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}Check for Updates</Button>
                    <Button type="button" variant="ghost" size="sm" disabled={decreeLocked} onClick={clearExternalSource}>Remove Source</Button>
                  </>
                }
              >
                {origin.candidate?.snapshotId === origin.snapshotId ? <p role="status" className="text-foreground-muted">GitHub is up to date with the selected source.</p> : null}
                {origin.candidate && origin.candidate.snapshotId !== origin.snapshotId ? (
                  <div className="border-t border-gold/15 pt-3">
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
            <div className="grid min-w-0 gap-3 md:grid-cols-[minmax(0,1fr)_minmax(12rem,15rem)_auto] md:items-end">
              <div className="min-w-0">
                <label htmlFor="decree-title" className="mb-1.5 block text-sm font-medium">
                  Decree Title
                </label>
                <input
                  id="decree-title"
                  type="text"
                  value={title}
                  disabled={decreeLocked}
                  onChange={(e) => setTitle(e.target.value)}
                  placeholder="Name your decree..."
                  className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-foreground-subtle focus:border-gold focus:outline-none focus:ring-1 focus:ring-gold disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
                />
              </div>
              <div className="min-w-0">
                  <SelectField
                    id="plan-selector"
                    label="From Plan"
                    value={selectedPlanSessionId ?? ""}
                    placeholder="No plan"
                    disabled={decreeLocked || sessionsWithPlans.length === 0}
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
              </div>
                {hasDecreeContent ? (
                  <div className="flex items-center justify-end md:flex-col md:gap-0.5">
                    <span className="mr-1 text-[10px] text-foreground-subtle md:mr-0">Clear</span>
                    <button
                      type="button"
                      onClick={handleClearDecree}
                      disabled={decreeLocked}
                      className="rounded-md p-2 text-foreground-subtle transition-colors hover:bg-surface-raised hover:text-foreground disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      <X className="h-4 w-4" />
                    </button>
                  </div>
                ) : <span />}
            </div>

            <div>
              <label htmlFor="decree-text" className="mb-1.5 block text-sm font-medium">
                Decree Text
              </label>
              <textarea
                id="decree-text"
                value={text}
                disabled={decreeLocked}
                onChange={(e) => setText(e.target.value)}
                placeholder="Describe what must be done..."
                rows={8}
                className="w-full resize-none rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-foreground-subtle focus:border-gold focus:outline-none focus:ring-1 focus:ring-gold disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
              />
            </div>

            <section className="space-y-4 rounded-md border border-border bg-surface-raised/30 p-4">
              <div className="space-y-3">
                <header className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-sm font-semibold text-foreground">Models</p>
                    <p className="text-xs text-foreground-subtle">
                      Pick a global model and thinking effort. Expand role controls to override per agent.
                    </p>
                  </div>
                  <button
                    type="button"
                    disabled={runControlsLocked}
                    onClick={() => setShowAgentModelOverrides((value) => !value)}
                    className="flex shrink-0 items-center gap-2 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-surface-raised disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    <span>{showAgentModelOverrides ? "Hide role controls" : "Show role controls"}</span>
                    {showAgentModelOverrides ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                  </button>
                </header>

                <ProviderModelCatalogNotice
                  state={providerModelCatalog}
					onRetry={providerModelCatalogController.retry}
					retryPending={providerModelCatalogController.retryPending}
                />

                <div className="grid grid-cols-1 gap-3 lg:grid-cols-3 lg:items-end">
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
                  <ToggleRow
                    label="Use for all Agents"
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
                </div>

                {showAgentModelOverrides && (
                  <div className="space-y-4 border-t border-border/60 pt-3">
                    {orderedRoleGroups.map((group) => (
                      <div key={group.key} className="space-y-2">
                        <h4 className="text-xs font-semibold uppercase tracking-wide text-foreground-subtle">{group.label}</h4>
                        <div className="space-y-2">
                          {group.roles.map((role) => {
                            const roleSelection = normalizeModelSelectionDraft(allModels, agentSelectionsByRole[role.role_id] ?? normalizedGlobalSelection);
                            return (
                              <div key={role.role_id} className="grid grid-cols-1 gap-3 lg:grid-cols-3 lg:items-end">
                                <ModelSelectField
                                  id={`role-model-${role.role_id}`}
                                  label={role.display_name}
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
                  </div>
                )}
              </div>

              <div className="space-y-3 border-t border-border/60 pt-4">
                <header>
                  <p className="text-sm font-semibold text-foreground">Run Controls</p>
                  <p className="text-xs text-foreground-subtle">
                    Configure this run's behavior, including review limits, Git worktree use, and approval requirements.
                  </p>
                </header>

                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
	                  <ToggleRow
	                    label="Skip Reviews and Validations"
	                    checked={skipReviewsAndValidations}
	                    onChange={handleSkipReviewsAndValidations}
	                    disabled={skipToggleDisabled}
	                    info="Skip review and validation for this decree."
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
                    <input
                      id="max-review-cycles"
                      type="number"
	                      min={0}
                       value={maxExecutionReviewCyclesInput}
                       onChange={(e) => setMaxExecutionReviewCyclesInput(e.target.value)}
	                      onBlur={() => setMaxExecutionReviewCyclesInput(normalizeNonNegativeCycleInput(maxExecutionReviewCyclesInput, 5))}
	                      disabled={reviewControlsDisabled}
	                      className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-gold focus:outline-none focus:ring-1 focus:ring-gold disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
	                    />
	                  </div>
	                  <div className={reviewControlsDisabled ? "opacity-55" : ""}>
	                    <label htmlFor="max-validation-cycles" className="mb-1.5 block text-sm font-medium">
	                      Max Validation Cycles
	                    </label>
                    <input
                      id="max-validation-cycles"
                      type="number"
	                      min={0}
                       value={maxValidationReworkCyclesInput}
                       onChange={(e) => setMaxValidationReworkCyclesInput(e.target.value)}
	                      onBlur={() => setMaxValidationReworkCyclesInput(normalizeNonNegativeCycleInput(maxValidationReworkCyclesInput, 5))}
	                      disabled={reviewControlsDisabled}
	                      className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-gold focus:outline-none focus:ring-1 focus:ring-gold disabled:cursor-not-allowed disabled:bg-surface-raised disabled:text-foreground-subtle"
	                    />
	                  </div>
	                </div>

	                {!skipReviewsAndValidations && !runUntilAllApprove && (
	                  <p className="text-xs text-foreground-subtle">
	                    Set either cap to 0 to auto-succeed the first ready review or validation wave without provider dispatch.
	                  </p>
	                )}

	                {skipReviewsAndValidations && (
	                  <div className="flex items-start gap-2 rounded-md border border-gold/35 bg-gold/10 px-3 py-2 text-xs text-gold">
	                    <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                    <span>Review and validation are skipped. Builder output proceeds directly to royal closure for this run.</span>
	                  </div>
	                )}

	                {runUntilAllApprove && !skipReviewsAndValidations && (
	                  <div className="flex items-start gap-2 rounded-md border border-gold/35 bg-gold/10 px-3 py-2 text-xs text-gold">
	                    <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                    <span>Uses more tokens: review and validation keep running until they pass.</span>
                  </div>
                )}

                <div className="space-y-1">
                  <ToggleRow
                    label="Use Git Worktree"
                    checked={useGitWorktree}
                    onChange={setUseGitWorktree}
                    disabled={gitWorktreeToggleDisabled}
                    info="On: Kingdom keeps builder changes in a separate Git working copy, so your main project folder stays untouched. That working copy is created in a sibling folder outside the repository: kingdom-worktrees/<repository>/<run-id>/. Off: builder changes are written directly into your project folder. Only available in a Git repository."
                  />
                  {gitWorktreeStatus ? <p className="text-xs text-foreground-subtle">{gitWorktreeStatus}</p> : null}
                </div>

                <ToggleRow
                  label="Allfather (Human) in the Loop"
                  checked={allfatherApprovalRequired}
                  onChange={setAllfatherApprovalRequired}
                  disabled={runControlsLocked}
                  info="On: Kingdom pauses for human Allfather approval at the plan stage and again before final completion. Off: Kingdom proceeds autonomously."
                />
              </div>
	            </section>
	            {isRecoveringDecree ? (
	              <div className="flex items-start gap-2 rounded-md border border-gold/35 bg-gold/10 px-3 py-2 text-xs text-gold">
	                <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                <span>
	                  {decreeRecoveryMode === "pending"
	                    ? <>Kingdom is still resolving this decree request. Retry will reuse the same request and idempotency key; starting over with a new key is unsafe while the original outcome remains pending.</>
	                    : decreeRecoveryMode === "replay"
	                      ? <>Kingdom may have accepted this decree, but the connection dropped before the response arrived. Retry will reuse the same decree request and idempotency key so Kingdom can recover without duplicating decrees. Discard recovery only if you intentionally want to abandon that replay path.</>
	                      : <>Kingdom rejected the decree request before it was accepted. Update the decree fields if needed, then issue the decree again.</>}
	                </span>
	              </div>
	            ) : null}
	            {isRecoveringRun && dispatchSession?.decreeId ? (
	              <div className="flex items-start gap-2 rounded-md border border-gold/35 bg-gold/10 px-3 py-2 text-xs text-gold">
	                <CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
	                <span>
	                  {runRecoveryMode === "pending"
	                    ? <>Decree <span className="font-mono">{dispatchSession.decreeId}</span> was issued and the run start remains pending. Retry will reuse the same request and idempotency key; starting over with a new key is unsafe.</>
	                    : runRecoveryMode === "replay"
	                      ? <>Decree <span className="font-mono">{dispatchSession.decreeId}</span> was issued, but the connection dropped before Kingdom could confirm whether the run started. Retry will reuse the same run request and idempotency key so Kingdom can recover without duplicating work. Discard recovery only if you intentionally want to abandon that replay path.</>
	                      : <>Decree <span className="font-mono">{dispatchSession.decreeId}</span> was issued, but Kingdom rejected the run start request before it was accepted. Update the run controls if needed, or start a new decree.</>}
	                </span>
	              </div>
	            ) : null}
	            {(createDecree.error || createRun.error) && (
	              <ErrorBanner message={(createDecree.error ?? createRun.error)?.message ?? "Request failed"} />
	            )}

            <div className="flex flex-wrap items-center gap-2">
	              <Button type="submit" disabled={submitDisabled}>
                {isSubmitting ? <Send className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
	                {submitLabel}
              </Button>
	              {!isPendingRecovery ? (
	                <Button type="button" variant={secondaryActionVariant} onClick={secondaryActionHandler} disabled={isSubmitting}>
	                  {secondaryActionLabel}
	                </Button>
	              ) : null}
            </div>
          </form>
        </CardContent>
    </Card>
  );
}
