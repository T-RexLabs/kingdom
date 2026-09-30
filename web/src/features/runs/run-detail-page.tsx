import { useParams, useSearch, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import {
  useRun,
  useRunExecution,
  useRunExecutions,
  useInfiniteRunNodeAttempts,
  useRunGraph,
  useTimeline,
  useRoles,
  useCancelRun,
  useRetryExecutionStop,
  type CancelRunRequest,
} from "@/api/hooks";
import {
  invalidateRunDetailRecoveryQueries,
  invalidateRunQuerySurfaces,
  type RunQuerySurface,
} from "@/api/hooks-runs";
import { useRunEventStream } from "./use-run-event-stream";
import {
  isDegradedRunStreamState,
  isTerminalRunStatus,
  RUN_SURFACE_DEGRADED_REFRESH_MS,
} from "./run-freshness";
import { ApiError } from "@/api/client";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatExactTime, timestampDescription } from "@/lib/date-time";
import {
  ConfirmDialog,
  Modal,
  MODAL_BODY_TEXT_CLASS,
} from "@/components/ui/modal-shell";
import { RelativeTime } from "@/components/ui/relative-time";
import { cn } from "@/lib/utils";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { ArrowLeft, Shield, AlertTriangle, XCircle } from "lucide-react";
import { Link } from "@tanstack/react-router";
import { DAGVisualization } from "./dag-visualization";
import { RunExecutionSurface } from "./run-execution-surface";
import { selectRunExecutionNode } from "./run-execution-selection";
import { RunExecutionPicker } from "./run-execution-picker";
import { RunDiagnostics } from "./run-diagnostics";
import { RunOutput, RunPublicationPanel } from "./run-output";
import { RunLiveStatus } from "./run-live-status";
import { RunReworkSummary } from "./run-rework-summary";
import { isActiveProviderBackedExecution } from "./run-status-semantics";
import { GovernanceActions } from "./governance-actions";
import { CurrentPlanPanel } from "./current-plan-panel";
import { WorkOrderPanel } from "./work-order-panel";
import {
  mergeTimelineEvents,
  readCanonicalTimelineCoverage,
  reconcileLiveTimelineEvents,
  settleLiveTimelineReconciliationAttempt,
  startLiveTimelineReconciliationAttempt,
  useLiveTimelineEvents,
  useLiveTimelineSyncState,
} from "./run-live-timeline";
import { accentBannerClass } from "./run-detail-tones";
import { describeManualGovernanceState } from "./manual-governance-state";
import {
  runDisplayBadgeStatus,
  runDisplayStateLabel,
} from "./run-display-state";
import { PageHeader } from "@/components/page-layout";
import { TimelinePanel } from "./timeline-panel";
import type {
  RunDetailTab,
  RunDetailPanel,
  RunDetailSection,
} from "./run-detail-tabs";
import { readRunStreamBootstrapState } from "./run-cache-updates";
import {
  clearRunQuerySurfacesDirty,
  markRunQuerySurfacesDirty,
  useRunSurfaceFreshness,
} from "./run-surface-freshness";
import {
  mergeRunNodeExecutionItems,
  overlayExecutionDetail,
  useLiveExecutionProjection,
} from "./run-live-execution-projection";

const RUN_DETAIL_TIMELINE_PAGE_SIZE = 50;

type SelectedExecutionState = {
  runId: string;
  nodeId: string;
  attemptId: string | null;
};

const tabs: { id: RunDetailTab; label: string }[] = [
  { id: "workflow", label: "Workflow" },
  { id: "execution", label: "Agent Activity" },
];
export function RunDetailPage() {
  const { runId } = useParams({ from: "/runs/$runId" });
  const search = useSearch({ from: "/runs/$runId" });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const runMountStartedAtRef = useRef(0);
  const activeTab: RunDetailTab = search.tab ?? "workflow";
  const panel = search.panel;
  const detailSection = search.section ?? "context";
  const [diagnosticsRunId, setDiagnosticsRunId] = useState<string | null>(null);
  const diagnosticsOpen =
    panel === "details" &&
    detailSection === "audit" &&
    diagnosticsRunId === runId;
  const runQuery = useRun(runId);
  const run = runQuery.data;
  const runReady = runQuery.isSuccess;
  const executionVisible =
    activeTab === "execution" || panel === "files" || diagnosticsOpen;
  const graphVisible = true;
  const timelineVisible = panel === "details" && detailSection === "audit";
  const executionsQueryActive = graphVisible;
  const contextVisible = panel === "details" && detailSection === "context";
  const hiddenRunQuerySurfaces = useMemo(
    () =>
      hiddenRunDetailSurfaces({
        graphVisible,
        executionsQueryActive,
        timelineVisible,
      }),
    [executionsQueryActive, graphVisible, timelineVisible],
  );
  const graphQuery = useRunGraph(runId, { enabled: runReady && graphVisible });
  const graph = graphQuery.data;
  const graphIsFetching = graphQuery.isFetching;
  const refetchGraph = graphQuery.refetch;
  const executionsQuery = useRunExecutions(runId, {
    enabled: runReady && executionsQueryActive,
  });
  const nodes = useMemo(
    () => executionsQuery.data?.items ?? [],
    [executionsQuery.data?.items],
  );
  const executionsAreFetching = executionsQuery.isFetching;
  const refetchExecutions = executionsQuery.refetch;
  const liveExecutionProjectionQuery = useLiveExecutionProjection(runId);
  const liveExecutionProjection = useMemo(
    () => liveExecutionProjectionQuery.data ?? {},
    [liveExecutionProjectionQuery.data],
  );
  const presentedNodes = useMemo(
    () => mergeRunNodeExecutionItems(nodes, liveExecutionProjection),
    [liveExecutionProjection, nodes],
  );
  const timelineQuery = useTimeline(runId, RUN_DETAIL_TIMELINE_PAGE_SIZE, {
    enabled: runReady && timelineVisible,
  });
  const timelineIsPending = timelineQuery.isPending;
  const refetchTimeline = timelineQuery.refetch;
  const fetchNextTimelinePage = timelineQuery.fetchNextPage;
  const liveTimelineQuery = useLiveTimelineEvents(runId);
  const liveTimelineSyncQuery = useLiveTimelineSyncState(runId);
  const runSurfaceFreshnessQuery = useRunSurfaceFreshness(runId);
  const [selectedExecution, setSelectedExecution] =
    useState<SelectedExecutionState | null>(null);
  const storedSelectedExecution =
    selectedExecution?.runId === runId ? selectedExecution : null;
  const selectedNode = selectRunExecutionNode(
    presentedNodes,
    storedSelectedExecution?.nodeId,
  );
  const selectedExecutionNodeId = selectedNode?.node_id ?? null;
  const nodeAttemptsQuery = useInfiniteRunNodeAttempts(
    runId,
    selectedExecutionNodeId,
    {
      enabled:
        runReady &&
        executionVisible &&
        ((selectedNode?.attempt_count ?? 0) > 1 ||
          ((selectedNode?.attempt_count ?? 0) > 0 &&
            !selectedNode?.current_execution)),
    },
  );
  const nodeAttempts = useMemo(
    () => nodeAttemptsQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [nodeAttemptsQuery.data?.pages],
  );
  const selectedExecutionId =
    storedSelectedExecution?.attemptId ??
    selectedNode?.current_execution?.attempt_id ??
    null;
  const executionDetailQuery = useRunExecution(runId, selectedExecutionId, {
    enabled: runReady && executionVisible && !!selectedExecutionId,
  });
  const selectedExecutionDetailQuery = executionDetailQuery;
  const executionDetailIsFetching = selectedExecutionDetailQuery.isFetching;
  const refetchExecutionDetail = selectedExecutionDetailQuery.refetch;
  const presentedExecutionDetail = useMemo(
    () =>
      selectedExecutionDetailQuery.data?.attempt_id === selectedExecutionId
        ? overlayExecutionDetail(
            selectedExecutionDetailQuery.data,
            liveExecutionProjection[
              selectedExecutionDetailQuery.data.node_run_id
            ],
          )
        : null,
    [
      liveExecutionProjection,
      selectedExecutionDetailQuery.data,
      selectedExecutionId,
    ],
  );
  const { data: roles } = useRoles();
  const cancelRun = useCancelRun(runId);
  const retryExecutionStop = useRetryExecutionStop(runId);
  const recoverableCancel = useRecoverableMutation(
    "cancel",
    (body: CancelRunRequest, key) => cancelRun.mutateAsync({ body, key }),
    runId,
  );
  const recoverableStopRetry = useRecoverableMutation(
    "retry-execution-stop",
    (attemptId: string, key) =>
      retryExecutionStop.mutateAsync({ value: attemptId, key }),
    `${runId}:${selectedExecutionId ?? ""}`,
  );
  const [cancelDialogOpen, setCancelDialogOpen] = useState(false);
  const planApprovalTargetRef = useRef<HTMLDivElement | null>(null);
  const finalApprovalTargetRef = useRef<HTMLDivElement | null>(null);
  const activeTabPanelRef = useRef<HTMLDivElement | null>(null);
  const pendingGovernanceFocus = useRef(false);

  const baseTimelineItems = useMemo(
    () =>
      [...(timelineQuery.data?.pages ?? [])]
        .reverse()
        .flatMap((page) => page.items ?? []),
    [timelineQuery.data?.pages],
  );
  const liveTimelineItems = useMemo(
    () => liveTimelineQuery.data ?? [],
    [liveTimelineQuery.data],
  );
  const timelineItems = useMemo(
    () => mergeTimelineEvents(baseTimelineItems, liveTimelineItems),
    [baseTimelineItems, liveTimelineItems],
  );
  const baseOldestTimelineSeq = baseTimelineItems[0]?.run_event_seq ?? 0;
  const baseLatestTimelineSeq =
    baseTimelineItems[baseTimelineItems.length - 1]?.run_event_seq ?? 0;
  const hiddenTimelineDirtyStartSeq =
    liveTimelineSyncQuery.data?.dirtyStartSeq ?? null;
  const hiddenTimelineDirtyEndSeq =
    liveTimelineSyncQuery.data?.dirtyEndSeq ?? null;
  const hiddenTimelineReconciliationAttempt =
    liveTimelineSyncQuery.data?.reconciliationAttempt ?? null;
  const hiddenTimelineRetryAtMs =
    liveTimelineSyncQuery.data?.nextRetryAtMs ?? null;
  const hasEarlierTimelinePages = timelineQuery.hasNextPage === true;
  const isTimelineFetchInFlight =
    timelineQuery.isFetching || timelineQuery.isFetchingNextPage;
  const [timelineRetryWakeTick, setTimelineRetryWakeTick] = useState(0);
  const timelineErrorMessage = timelineQuery.isError
    ? queryErrorMessage(
        timelineQuery.error,
        "The activity timeline could not be loaded right now.",
      )
    : null;
  const executionsErrorMessage = executionsQuery.isError
    ? queryErrorMessage(
        executionsQuery.error,
        "The execution steps could not be loaded right now.",
      )
    : null;
  const setActiveTab = useCallback(
    (tab: RunDetailTab) => {
      navigate({
        to: "/runs/$runId",
        params: { runId },
        search: { tab },
        replace: true,
      });
    },
    [navigate, runId],
  );

  const openPanel = useCallback(
    (next: RunDetailPanel, section?: RunDetailSection) => {
      if (next === "files" && selectedExecutionNodeId && selectedExecutionId)
        setSelectedExecution({
          runId,
          nodeId: selectedExecutionNodeId,
          attemptId: selectedExecutionId,
        });
      void navigate({
        to: "/runs/$runId",
        params: { runId },
        search: {
          tab: activeTab,
          panel: next,
          ...(section ? { section } : {}),
        },
      });
    },
    [activeTab, navigate, runId, selectedExecutionId, selectedExecutionNodeId],
  );
  const closePanel = useCallback(() => {
    setDiagnosticsRunId(null);
    void navigate({
      to: "/runs/$runId",
      params: { runId },
      search: { tab: activeTab },
    });
  }, [activeTab, navigate, runId]);

  const canCancelRun =
    run?.status === "running" ||
    run?.status === "pending" ||
    run?.status === "awaiting_human_confirmation";
  const cancelDialogVisible = cancelDialogOpen && canCancelRun;
  const dirtyRunSurfaces = useMemo(
    () => runSurfaceFreshnessQuery.data?.dirtySurfaces ?? [],
    [runSurfaceFreshnessQuery.data?.dirtySurfaces],
  );
  const dirtyRunSurfaceSet = useMemo(
    () => new Set(dirtyRunSurfaces),
    [dirtyRunSurfaces],
  );
  const graphDirty = dirtyRunSurfaceSet.has("graph");
  const executionsDirty = dirtyRunSurfaceSet.has("executions");
  const timelineDirty = dirtyRunSurfaceSet.has("timeline");

  const runStreamState = useRunEventStream(
    runId,
    runReady,
    !timelineVisible,
    hiddenRunQuerySurfaces,
  );

  useEffect(() => {
    runMountStartedAtRef.current = Date.now();
  }, [runId]);

  useEffect(() => {
    if (!runReady || !runId) {
      return;
    }
    if (readRunStreamBootstrapState(queryClient, runId).lastEventId) {
      return;
    }
    if (
      runQuery.dataUpdatedAt === 0 ||
      runQuery.dataUpdatedAt >= runMountStartedAtRef.current
    ) {
      return;
    }

    invalidateRunDetailRecoveryQueries(queryClient, runId);
  }, [queryClient, runId, runQuery.dataUpdatedAt, runReady]);

  useEffect(() => {
    if (
      !runId ||
      !timelineVisible ||
      baseOldestTimelineSeq < 1 ||
      baseLatestTimelineSeq < baseOldestTimelineSeq
    ) {
      return;
    }
    reconcileLiveTimelineEvents(
      queryClient,
      runId,
      baseOldestTimelineSeq,
      baseLatestTimelineSeq,
    );
  }, [
    baseLatestTimelineSeq,
    baseOldestTimelineSeq,
    queryClient,
    runId,
    timelineVisible,
  ]);

  useEffect(() => {
    if (activeTab !== "execution") {
      return;
    }
    const panel = activeTabPanelRef.current;
    if (!panel) {
      return;
    }
    panel.scrollTop = 0;
  }, [activeTab, runId]);

  useEffect(() => {
    if (!runId || !graphVisible || !graphDirty || graphIsFetching) {
      return;
    }
    void Promise.resolve(refetchGraph())
      .then((result) => {
        if (isSuccessfulQueryResult(result)) {
          clearRunQuerySurfacesDirty(queryClient, runId, ["graph"]);
        }
      })
      .catch(() => {
        // leave dirty so the next visible retry can reconcile it
      });
  }, [
    graphDirty,
    graphIsFetching,
    graphVisible,
    queryClient,
    refetchGraph,
    runId,
  ]);

  useEffect(() => {
    if (
      !runId ||
      !executionsQueryActive ||
      !executionsDirty ||
      executionsAreFetching ||
      (executionVisible && executionDetailIsFetching)
    ) {
      return;
    }
    const refreshes: Array<Promise<unknown>> = [
      Promise.resolve(refetchExecutions()),
    ];
    if (executionVisible && selectedExecutionId) {
      refreshes.push(Promise.resolve(refetchExecutionDetail()));
    }
    void Promise.all(refreshes)
      .then((results) => {
        if (results.every((result) => isSuccessfulQueryResult(result))) {
          clearRunQuerySurfacesDirty(queryClient, runId, ["executions"]);
        }
      })
      .catch(() => {
        // leave dirty so the next retry can reconcile it
      });
  }, [
    executionDetailIsFetching,
    executionsDirty,
    executionsQueryActive,
    executionsAreFetching,
    executionVisible,
    queryClient,
    refetchExecutionDetail,
    refetchExecutions,
    runId,
    selectedExecutionId,
  ]);

  useEffect(() => {
    if (!runId || !timelineVisible) {
      return;
    }
    if (
      hiddenTimelineDirtyStartSeq === null ||
      hiddenTimelineDirtyEndSeq === null
    ) {
      return;
    }
    if (
      timelineIsPending ||
      isTimelineFetchInFlight ||
      hiddenTimelineReconciliationAttempt !== null
    ) {
      return;
    }
    if (
      hiddenTimelineRetryAtMs !== null &&
      hiddenTimelineRetryAtMs > Date.now()
    ) {
      return;
    }

    const settleReconciliationAttempt = (success: boolean) => {
      settleLiveTimelineReconciliationAttempt(
        queryClient,
        runId,
        success,
        readCanonicalTimelineCoverage(
          queryClient,
          runId,
          RUN_DETAIL_TIMELINE_PAGE_SIZE,
        ),
      );
    };

    if (baseLatestTimelineSeq < hiddenTimelineDirtyEndSeq) {
      const started = startLiveTimelineReconciliationAttempt(
        queryClient,
        runId,
        {
          kind: "refetch-tail",
          dirtyStartSeq: hiddenTimelineDirtyStartSeq,
          dirtyEndSeq: hiddenTimelineDirtyEndSeq,
          baseOldestSeq: baseOldestTimelineSeq,
          baseLatestSeq: baseLatestTimelineSeq,
        },
      );
      if (!started) {
        return;
      }
      void Promise.resolve(refetchTimeline())
        .then((result) => {
          settleReconciliationAttempt(isSuccessfulQueryResult(result));
        })
        .catch(() => {
          settleReconciliationAttempt(false);
        });
      return;
    }
    if (
      baseOldestTimelineSeq > hiddenTimelineDirtyStartSeq &&
      hasEarlierTimelinePages
    ) {
      const started = startLiveTimelineReconciliationAttempt(
        queryClient,
        runId,
        {
          kind: "fetch-older",
          dirtyStartSeq: hiddenTimelineDirtyStartSeq,
          dirtyEndSeq: hiddenTimelineDirtyEndSeq,
          baseOldestSeq: baseOldestTimelineSeq,
          baseLatestSeq: baseLatestTimelineSeq,
        },
      );
      if (!started) {
        return;
      }
      void Promise.resolve(fetchNextTimelinePage())
        .then((result) => {
          settleReconciliationAttempt(isSuccessfulQueryResult(result));
        })
        .catch(() => {
          settleReconciliationAttempt(false);
        });
    }
  }, [
    baseLatestTimelineSeq,
    baseOldestTimelineSeq,
    hasEarlierTimelinePages,
    hiddenTimelineDirtyEndSeq,
    hiddenTimelineDirtyStartSeq,
    hiddenTimelineReconciliationAttempt,
    hiddenTimelineRetryAtMs,
    isTimelineFetchInFlight,
    runId,
    queryClient,
    fetchNextTimelinePage,
    refetchTimeline,
    timelineRetryWakeTick,
    timelineIsPending,
    timelineVisible,
  ]);

  useEffect(() => {
    if (!runId || !timelineVisible || !timelineDirty) {
      return;
    }
    if (
      hiddenTimelineDirtyStartSeq !== null ||
      hiddenTimelineDirtyEndSeq !== null
    ) {
      return;
    }
    if (timelineIsPending || isTimelineFetchInFlight) {
      return;
    }
    void Promise.resolve(refetchTimeline())
      .then((result) => {
        if (isSuccessfulQueryResult(result)) {
          clearRunQuerySurfacesDirty(queryClient, runId, ["timeline"]);
        }
      })
      .catch(() => {
        // leave dirty so the next visible retry can reconcile it
      });
  }, [
    hiddenTimelineDirtyEndSeq,
    hiddenTimelineDirtyStartSeq,
    isTimelineFetchInFlight,
    queryClient,
    refetchTimeline,
    runId,
    timelineDirty,
    timelineIsPending,
    timelineVisible,
  ]);

  useEffect(() => {
    if (!timelineVisible || hiddenTimelineRetryAtMs === null) {
      return;
    }
    const delayMs = Math.max(0, hiddenTimelineRetryAtMs - Date.now());
    const timer = window.setTimeout(() => {
      setTimelineRetryWakeTick((current) => current + 1);
    }, delayMs);
    return () => {
      window.clearTimeout(timer);
    };
  }, [hiddenTimelineRetryAtMs, timelineVisible]);

  useEffect(() => {
    if (!runReady || !runId || !run) {
      return;
    }
    if (!isDegradedRunStreamState(runStreamState)) {
      return;
    }
    if (isTerminalRunStatus(run.status)) {
      return;
    }

    const refreshActiveRunQueries = () => {
      markRunQuerySurfacesDirty(
        queryClient,
        runId,
        hiddenRunDetailSurfaces({
          graphVisible,
          executionsQueryActive,
          timelineVisible,
        }),
      );
      invalidateRunQuerySurfaces(
        queryClient,
        runId,
        degradedRunRefreshSurfaces({
          graphVisible,
          executionsQueryActive,
          timelineVisible,
        }),
      );
    };

    refreshActiveRunQueries();
    const timer = window.setInterval(
      refreshActiveRunQueries,
      RUN_SURFACE_DEGRADED_REFRESH_MS,
    );
    return () => {
      window.clearInterval(timer);
    };
  }, [
    executionsQueryActive,
    graphVisible,
    queryClient,
    run,
    runId,
    runReady,
    runStreamState,
    timelineVisible,
  ]);

  const handleSelectExecutionStep = useCallback(
    (nodeId: string) => {
      const node = presentedNodes.find((item) => item.node_id === nodeId);
      if (!node) {
        return;
      }
      setSelectedExecution({ runId, nodeId: node.node_id, attemptId: null });
    },
    [presentedNodes, runId],
  );

  const handleViewNodeActivity = useCallback(
    (nodeId: string) => {
      const node = presentedNodes.find((item) => item.node_id === nodeId);
      if (!node) {
        return;
      }
      setSelectedExecution({ runId, nodeId: node.node_id, attemptId: null });
      setActiveTab("execution");
    },
    [presentedNodes, runId, setActiveTab],
  );

  const handleSelectExecutionAttempt = useCallback(
    (attemptId: string) => {
      const nodeId = selectedExecutionNodeId;
      if (!nodeId) {
        return;
      }
      if (
        selectedNode?.current_execution?.attempt_id !== attemptId &&
        !nodeAttempts.some((item) => item.attempt_id === attemptId)
      ) {
        return;
      }
      setSelectedExecution({ runId, nodeId, attemptId });
    },
    [
      nodeAttempts,
      runId,
      selectedExecutionNodeId,
      selectedNode?.current_execution?.attempt_id,
    ],
  );

  const handleSelectCurrentExecutionAttempt = useCallback(() => {
    if (!selectedExecutionNodeId) {
      return;
    }
    setSelectedExecution({
      runId,
      nodeId: selectedExecutionNodeId,
      attemptId: null,
    });
  }, [runId, selectedExecutionNodeId]);

  const handleCloseCancelDialog = () => {
    if (!cancelRun.isPending && !recoverableCancel.hasPendingRecovery) {
      setCancelDialogOpen(false);
    }
  };

  const handleConfirmCancelRun = async () => {
    try {
      await recoverableCancel.mutateAsync({
        reason: "manual cancellation from run detail",
      });
      setCancelDialogOpen(false);
    } catch {
      // surfaced via mutation error UI below
    }
  };

  const focusGovernanceAction = useCallback(() => {
    const isAwaitingPlanApproval =
      run?.autonomous_approvals === false &&
      run.display_state === "awaiting_plan_decision";
    const isAwaitingFinalApproval =
      run?.autonomous_approvals === false &&
      run.display_state === "awaiting_final_decision";
    const target = isAwaitingPlanApproval
      ? planApprovalTargetRef.current
      : isAwaitingFinalApproval
        ? finalApprovalTargetRef.current
        : null;
    if (!target) {
      return;
    }

    const prefersReducedMotion =
      typeof window !== "undefined" &&
      typeof window.matchMedia === "function" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    target.scrollIntoView({
      behavior: prefersReducedMotion ? "auto" : "smooth",
      block: "nearest",
      inline: "nearest",
    });

    const firstInteractive = target.querySelector<HTMLElement>(
      'textarea, button:not([disabled]), input:not([disabled]), select:not([disabled]), [href], [tabindex]:not([tabindex="-1"])',
    );
    (firstInteractive ?? target).focus({ preventScroll: true });
  }, [run]);

  const jumpToGovernanceAction = useCallback(() => {
    if (panel === "plan") focusGovernanceAction();
    else {
      pendingGovernanceFocus.current = true;
      openPanel("plan");
    }
  }, [panel, focusGovernanceAction, openPanel]);

  if (runQuery.isPending) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="animate-pulse text-foreground-muted">
          Loading run...
        </div>
      </div>
    );
  }

  if (runQuery.isError) {
    const isNotFound =
      runQuery.error instanceof ApiError && runQuery.error.status === 404;
    const loadErrorMessage =
      runQuery.error instanceof Error
        ? runQuery.error.message
        : "The run detail view could not be loaded right now.";
    return (
      <div className="flex h-full items-center justify-center px-page py-8">
        <div className="w-full max-w-lg rounded-xl border border-border bg-surface-raised/40 p-6 shadow-elevation-2">
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-foreground-muted">
            Run Detail
          </p>
          <h1 className="mt-2 font-display text-2xl font-semibold text-foreground">
            {isNotFound ? "Run not found" : "Unable to load run"}
          </h1>
          <p className="mt-3 text-sm text-foreground-subtle">
            {isNotFound
              ? "The requested run is unavailable or may have been removed."
              : loadErrorMessage}
          </p>
          <div className="mt-5 flex items-center gap-3">
            <Link
              to="/runs"
              className="inline-flex items-center rounded-md border border-border bg-background px-3 py-2 text-sm font-medium text-foreground hover:bg-surface-raised"
            >
              Back to runs
            </Link>
          </div>
        </div>
      </div>
    );
  }

  if (!run) {
    return null;
  }

  const governanceState = describeManualGovernanceState(run);
  const failedNodeCount = run.failed_nodes.length;
  const cancelledNodes = run.status === "cancelled" ? (graph?.nodes ?? []) : [];
  const governanceJumpLabel = governanceState.isAwaitingPlanApproval
    ? "Review Plan"
    : governanceState.isAwaitingFinalApproval
      ? "Review Final Decision"
      : null;

  const connected = runStreamState === "open";
  const startedLabel = run.started_at
    ? formatExactTime(run.started_at)
    : run.start_state === "unrecorded"
      ? "Observation Not Recorded"
      : "Not Started";

  const attemptPicker = (
    <RunExecutionPicker
      nodes={presentedNodes}
      attempts={nodeAttempts}
      selectedNodeId={selectedExecutionNodeId}
      selectedAttemptId={selectedExecutionId}
      detail={presentedExecutionDetail}
      onSelectStep={handleSelectExecutionStep}
      onSelectAttempt={handleSelectExecutionAttempt}
      onSelectCurrentAttempt={handleSelectCurrentExecutionAttempt}
      hasMoreAttempts={nodeAttemptsQuery.hasNextPage === true}
      onLoadMoreAttempts={() => void nodeAttemptsQuery.fetchNextPage()}
      isLoadingMoreAttempts={nodeAttemptsQuery.isFetchingNextPage}
      isHistoryLoading={nodeAttemptsQuery.isFetching && !nodeAttemptsQuery.data}
      historyErrorMessage={
        nodeAttemptsQuery.isError
          ? queryErrorMessage(
              nodeAttemptsQuery.error,
              "Attempt history could not be loaded.",
            )
          : null
      }
      onRetryHistory={() => void nodeAttemptsQuery.refetch()}
    />
  );

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader>
        <Link
          to="/runs"
          className="inline-flex min-h-9 items-center gap-2 text-sm text-foreground-muted hover:text-foreground"
        >
          <ArrowLeft size={15} />
          Runs
        </Link>
        <span className="ml-3 flex min-w-0 items-center gap-2 text-xs text-foreground-subtle">
          <span className="shrink-0">Run ID</span>
          <span className="truncate font-mono">{run.run_id}</span>
        </span>
      </PageHeader>
      <div className="min-h-0 flex-1 overflow-y-auto px-page py-7">
        <div className="mx-auto w-full max-w-5xl space-y-5">
          <header className="space-y-4">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="min-w-0 flex-1">
                <h1 className="text-wrap-anywhere font-display text-3xl leading-tight sm:text-4xl">
                  {run.work_order_title}
                </h1>
                <dl className="mt-4 flex flex-wrap items-center gap-x-6 gap-y-2 text-xs">
                  <div className="flex items-center gap-2">
                    <dt className="text-foreground-muted">Started</dt>
                    <dd>
                      <time
                        dateTime={run.started_at}
                        title={
                          run.started_at
                            ? timestampDescription(run.started_at)
                            : undefined
                        }
                      >
                        {startedLabel}
                      </time>
                    </dd>
                  </div>
                  <div className="flex items-center gap-2">
                    <dt className="text-foreground-muted">Updated</dt>
                    <dd>
                      <RelativeTime iso={run.last_activity_at} />
                    </dd>
                  </div>
                </dl>
              </div>
              <div
                data-testid="run-primary-controls"
                className="flex items-center gap-2"
              >
                {(!presentedNodes.some((node) =>
                  isActiveProviderBackedExecution(node.current_execution),
                ) ||
                  run.display_state !== "running") && (
                  <Badge
                    data-testid="run-status-pill"
                    variant="status"
                    status={runDisplayBadgeStatus(run)}
                  >
                    {runDisplayStateLabel(run)}
                  </Badge>
                )}
                {canCancelRun && (
                  <Button
                    data-testid="run-cancel-pill"
                    size="sm"
                    variant="ghost"
                    onClick={() => setCancelDialogOpen(true)}
                    disabled={cancelRun.isPending}
                  >
                    <XCircle size={15} />
                    {cancelRun.isPending ? "Cancelling…" : "Cancel Run"}
                  </Button>
                )}
              </div>
            </div>
            <nav
              aria-label="Run resources"
              className="flex flex-wrap gap-x-5 text-xs text-foreground-muted"
            >
              <button
                type="button"
                className="min-h-9 hover:text-foreground"
                onClick={() => openPanel("plan")}
              >
                View Plan
              </button>
              <button
                type="button"
                className="min-h-9 hover:text-foreground"
                onClick={() => openPanel("files")}
              >
                Run Files
              </button>
              <button
                type="button"
                className="min-h-9 hover:text-foreground"
                onClick={() => openPanel("details")}
              >
                Run Details
              </button>
            </nav>
            <RunReworkSummary run={run} />
            <RunLiveStatus
              runId={runId}
              nodes={presentedNodes}
              connected={connected}
              onSelect={handleViewNodeActivity}
            />
          </header>
          {!connected && !isTerminalRunStatus(run.status) && (
            <p role="status" className="text-xs text-foreground-muted">
              Live updates{" "}
              {runStreamState === "failed"
                ? "are unavailable"
                : "are reconnecting"}
              . Showing the latest recorded state.
            </p>
          )}
          {governanceState.noticeTitle && (
            <div
              data-testid="run-governance-notice"
              className={cn(
                "flex flex-wrap items-center justify-between gap-4 rounded-xl border p-4",
                accentBannerClass("accent"),
              )}
            >
              <div className="flex items-start gap-3">
                <Shield size={18} />
                <div>
                  <p className="text-sm font-medium">
                    {governanceState.noticeTitle}
                  </p>
                  <p className="mt-1 text-xs">
                    {governanceState.noticeMessage}
                  </p>
                </div>
              </div>
              {governanceJumpLabel && (
                <Button
                  data-testid="run-governance-jump"
                  size="sm"
                  variant="outline"
                  onClick={jumpToGovernanceAction}
                >
                  {governanceJumpLabel}
                </Button>
              )}
            </div>
          )}
          {cancelRun.error && (
            <p role="alert" className="text-sm text-status-failed">
              Failed to cancel run: {cancelRun.error.message}
            </p>
          )}
          {(failedNodeCount > 0 || cancelledNodes.length > 0) && (
            <GovernanceActions
              run={run}
              failedNodes={run.failed_nodes}
              cancelledNodes={cancelledNodes}
              showFinalApproval={false}
            />
          )}
          <section className="min-w-0">
            <div className="flex flex-wrap items-center justify-between gap-x-4 border-b border-border">
              <nav
                role="tablist"
                aria-label="Run detail sections"
                className="flex min-w-0 gap-1 overflow-x-auto"
                onKeyDown={(event) => {
                  const index = tabs.findIndex(
                    (tab) =>
                      `run-tab-${tab.id}` === (event.target as HTMLElement).id,
                  );
                  const next =
                    event.key === "ArrowRight"
                      ? (index + 1) % tabs.length
                      : event.key === "ArrowLeft"
                        ? (index - 1 + tabs.length) % tabs.length
                        : event.key === "Home"
                          ? 0
                          : event.key === "End"
                            ? tabs.length - 1
                            : null;
                  if (next === null) return;
                  event.preventDefault();
                  setActiveTab(tabs[next].id);
                  document.getElementById(`run-tab-${tabs[next].id}`)?.focus();
                }}
              >
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    id={`run-tab-${tab.id}`}
                    role="tab"
                    aria-selected={activeTab === tab.id}
                    aria-controls={`run-tabpanel-${tab.id}`}
                    tabIndex={
                      activeTab === tab.id ||
                      (!tabs.some((primary) => primary.id === activeTab) &&
                        tab.id === "workflow")
                        ? 0
                        : -1
                    }
                    onClick={() => setActiveTab(tab.id)}
                    className={cn(
                      "flex min-h-12 shrink-0 items-center gap-2 border-b-2 px-4 text-sm font-medium",
                      activeTab === tab.id
                        ? "border-accent text-accent"
                        : "border-transparent text-foreground-muted hover:text-foreground",
                    )}
                  >
                    {tab.label}
                  </button>
                ))}
              </nav>
            </div>
            <div
              ref={activeTabPanelRef}
              id={`run-tabpanel-${activeTab}`}
              role="tabpanel"
              aria-label={tabs.find((tab) => tab.id === activeTab)?.label}
              className="min-w-0 py-5"
            >
              {activeTab === "workflow" &&
                (graph ? (
                  <DAGVisualization
                    graph={graph}
                    nodes={presentedNodes}
                    selectedNodeId={selectedExecutionNodeId}
                    connected={connected}
                    onViewActivity={handleViewNodeActivity}
                    executionsErrorMessage={executionsErrorMessage}
                    onRetryExecutions={() => void executionsQuery.refetch()}
                  />
                ) : graphQuery.isError ? (
                  <TabError
                    message={queryErrorMessage(
                      graphQuery.error,
                      "The workflow could not be loaded.",
                    )}
                    onRetry={() => void graphQuery.refetch()}
                  />
                ) : (
                  <TabLoading />
                ))}
              {activeTab === "execution" &&
                (executionsQuery.isPending && !nodes.length ? (
                  <TabLoading />
                ) : executionsErrorMessage && !nodes.length ? (
                  <TabError
                    message={executionsErrorMessage}
                    onRetry={() => void executionsQuery.refetch()}
                  />
                ) : (
                  <RunExecutionSurface
                    run={run}
                    nodes={presentedNodes}
                    attempts={nodeAttempts}
                    selectedNodeId={selectedExecutionNodeId}
                    selectedAttemptId={selectedExecutionId}
                    onSelectStep={handleSelectExecutionStep}
                    onSelectAttempt={handleSelectExecutionAttempt}
                    onSelectCurrentAttempt={handleSelectCurrentExecutionAttempt}
                    hasMoreAttempts={nodeAttemptsQuery.hasNextPage === true}
                    onLoadMoreAttempts={() =>
                      void nodeAttemptsQuery.fetchNextPage()
                    }
                    isLoadingMoreAttempts={nodeAttemptsQuery.isFetchingNextPage}
                    isHistoryLoading={
                      nodeAttemptsQuery.isFetching && !nodeAttemptsQuery.data
                    }
                    historyErrorMessage={
                      nodeAttemptsQuery.isError
                        ? queryErrorMessage(
                            nodeAttemptsQuery.error,
                            "Attempt history could not be loaded.",
                          )
                        : null
                    }
                    onRetryHistory={() => void nodeAttemptsQuery.refetch()}
                    detail={presentedExecutionDetail}
                    isDetailLoading={
                      executionDetailIsFetching &&
                      !selectedExecutionDetailQuery.data
                    }
                    detailErrorMessage={
                      selectedExecutionDetailQuery.isError
                        ? queryErrorMessage(
                            selectedExecutionDetailQuery.error,
                            "The execution detail could not be loaded.",
                          )
                        : null
                    }
                    onRetryDetail={() => void executionDetailQuery.refetch()}
                    onRetryStop={(attemptId) =>
                      void recoverableStopRetry.mutateAsync(attemptId)
                    }
                    isStopRetryPending={
                      retryExecutionStop.isPending ||
                      recoverableStopRetry.hasPendingRecovery
                    }
                    stopRetryErrorMessage={
                      retryExecutionStop.error
                        ? queryErrorMessage(
                            retryExecutionStop.error,
                            "The stop request could not be retried.",
                          )
                        : null
                    }
                  />
                ))}
            </div>
          </section>
        </div>
      </div>
      {panel && (
        <Modal
          open
          onClose={closePanel}
          title={
            panel === "files"
              ? "Run Files"
              : panel === "plan"
                ? "Plan Review"
                : "Run Details"
          }
          size="xl"
          height="workbench"
        >
          {panel === "details" && (
            <nav
              aria-label="Run detail sections"
              className="mb-6 flex flex-wrap gap-2 border-b border-border pb-3"
            >
              {(
                [
                  ["context", "Work Order and Settings"],
                  ["audit", "Audit"],
                ] as const
              ).map(([id, label]) => (
                <Button
                  key={id}
                  variant="ghost"
                  size="sm"
                  aria-pressed={detailSection === id}
                  onClick={() => openPanel("details", id)}
                >
                  {label}
                </Button>
              ))}
            </nav>
          )}
          {panel === "files" && (
            <>
              <RunOutput key={runId} runId={runId} />
              <RunPublicationPanel
                runId={runId}
                attemptId={presentedExecutionDetail?.attempt_id}
                publication={presentedExecutionDetail?.publication}
                picker={attemptPicker}
                isLoading={
                  executionDetailIsFetching && !presentedExecutionDetail
                }
                error={
                  selectedExecutionDetailQuery.isError
                    ? new Error(
                        queryErrorMessage(
                          selectedExecutionDetailQuery.error,
                          "Publication could not be loaded.",
                        ),
                      )
                    : null
                }
                onRefresh={() => executionDetailQuery.refetch()}
              />
            </>
          )}
          {panel === "plan" && (
            <div className="space-y-5">
              <GovernanceFocus
                pendingRef={pendingGovernanceFocus}
                onFocus={focusGovernanceAction}
              />
              <CurrentPlanPanel
                run={run}
                approvalSectionRef={planApprovalTargetRef}
              />
              {governanceState.isAwaitingFinalApproval && (
                <GovernanceActions
                  run={run}
                  failedNodes={run.failed_nodes}
                  showNodeActions={false}
                  finalApprovalCardRef={finalApprovalTargetRef}
                />
              )}
            </div>
          )}
          {contextVisible && (
            <WorkOrderPanel run={run} roles={roles?.items ?? []} />
          )}

          {timelineVisible && (
            <div className="space-y-3">
              {timelineErrorMessage && (
                <TabInlineError
                  message={timelineErrorMessage}
                  onRetry={() => void timelineQuery.refetch()}
                />
              )}
              <div className="h-[34rem]">
                <TimelinePanel
                  events={timelineItems}
                  graph={graph}
                  executions={presentedNodes.flatMap((node) =>
                    node.current_execution ? [node.current_execution] : [],
                  )}
                  hasMore={hasEarlierTimelinePages}
                  onLoadMore={() => void timelineQuery.fetchNextPage()}
                  isLoadingMore={timelineQuery.isFetchingNextPage}
                />
              </div>
            </div>
          )}
          {timelineVisible && (
            <details
              open={diagnosticsOpen}
              className="mt-6 border-t border-border pt-3"
              onToggle={(event) => {
                if (event.target !== event.currentTarget) return;
                const isOpen = event.currentTarget.open;
                setDiagnosticsRunId(isOpen ? runId : null);
                if (isOpen && selectedExecutionNodeId && selectedExecutionId) {
                  setSelectedExecution({
                    runId,
                    nodeId: selectedExecutionNodeId,
                    attemptId: selectedExecutionId,
                  });
                }
              }}
            >
              <summary className="min-h-10 cursor-pointer py-2 text-sm text-foreground-muted hover:text-foreground">
                Attempt Record
              </summary>
              {diagnosticsOpen && (
                <div className="pt-3">
                  {attemptPicker}
                  <RunDiagnostics
                    detail={presentedExecutionDetail}
                    history={nodeAttempts.find(
                      (item) => item.attempt_id === selectedExecutionId,
                    )}
                    currentAttemptId={
                      selectedNode?.current_execution?.attempt_id
                    }
                    isLoading={
                      executionDetailIsFetching && !presentedExecutionDetail
                    }
                    errorMessage={
                      selectedExecutionDetailQuery.isError
                        ? queryErrorMessage(
                            selectedExecutionDetailQuery.error,
                            "The execution detail could not be loaded.",
                          )
                        : null
                    }
                    onRetry={() => void executionDetailQuery.refetch()}
                    failure={
                      selectedExecutionId ===
                      selectedNode?.current_execution?.attempt_id
                        ? selectedNode?.failure
                        : nodeAttempts.find(
                            (item) => item.attempt_id === selectedExecutionId,
                          )?.failure
                    }
                  />
                </div>
              )}
            </details>
          )}
        </Modal>
      )}
      <CancelRunDialog
        open={cancelDialogVisible}
        runTitle={run.work_order_title}
        isPending={cancelRun.isPending}
        recoveryPending={recoverableCancel.hasPendingRecovery}
        onClose={handleCloseCancelDialog}
        onConfirm={() => void handleConfirmCancelRun()}
      />
    </div>
  );
}

function queryErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message;
  }
  return fallback;
}

function isSuccessfulQueryResult(result: unknown): boolean {
  if (result && typeof result === "object") {
    const status = (result as { status?: unknown }).status;
    if (status === "error") {
      return false;
    }
    const isError = (result as { isError?: unknown }).isError;
    if (typeof isError === "boolean") {
      return !isError;
    }
  }
  return true;
}

function degradedRunRefreshSurfaces({
  graphVisible,
  executionsQueryActive,
  timelineVisible,
}: {
  graphVisible: boolean;
  executionsQueryActive: boolean;
  timelineVisible: boolean;
}) {
  const surfaces: RunQuerySurface[] = [
    "detail",
    "plan",
    "governanceSummary",
    "governanceHistory",
  ];
  if (executionsQueryActive) {
    surfaces.push("executions");
  }
  if (graphVisible) {
    surfaces.push("graph");
  }
  if (timelineVisible) {
    surfaces.push("timeline");
  }
  return surfaces;
}

function hiddenRunDetailSurfaces({
  graphVisible,
  executionsQueryActive,
  timelineVisible,
}: {
  graphVisible: boolean;
  executionsQueryActive: boolean;
  timelineVisible: boolean;
}) {
  const surfaces: RunQuerySurface[] = [];
  if (!graphVisible) {
    surfaces.push("graph");
  }
  if (!executionsQueryActive) {
    surfaces.push("executions");
  }
  if (!timelineVisible) {
    surfaces.push("timeline");
  }
  return surfaces;
}

function TabLoading() {
  return (
    <div className="flex h-64 items-center justify-center text-sm text-foreground-muted animate-pulse">
      Loading...
    </div>
  );
}

function TabError({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div className="flex h-64 items-center justify-center p-4">
      <div className="flex max-w-md flex-col items-center gap-3 rounded-lg border border-status-failed/30 bg-status-failed/5 px-4 py-5 text-center">
        <AlertTriangle className="h-5 w-5 text-status-failed" />
        <p className="text-sm text-foreground">{message}</p>
        <Button type="button" variant="outline" onClick={onRetry}>
          Retry
        </Button>
      </div>
    </div>
  );
}

function TabInlineError({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div className="mb-3 flex items-center justify-between gap-3 rounded-lg border border-status-failed/20 bg-status-failed/5 px-3 py-2 text-sm">
      <div className="flex items-center gap-2 text-status-failed">
        <AlertTriangle className="h-4 w-4 shrink-0" />
        <span>{message}</span>
      </div>
      <Button type="button" variant="outline" size="sm" onClick={onRetry}>
        Retry
      </Button>
    </div>
  );
}

function CancelRunDialog({
  open,
  runTitle,
  isPending,
  recoveryPending,
  onClose,
  onConfirm,
}: {
  open: boolean;
  runTitle: string;
  isPending: boolean;
  recoveryPending: boolean;
  onClose: () => void;
  onConfirm: () => void;
}) {
  return (
    <ConfirmDialog
      open={open}
      onClose={onClose}
      onConfirm={onConfirm}
      title={runTitle}
      caption="Cancel Run"
      confirmLabel={
        isPending
          ? "Cancelling..."
          : recoveryPending
            ? "Retry Cancel Request"
            : "Cancel Run"
      }
      // The only confirm that overrides the default dismiss verb, because
      // here "Cancel" is the destructive action's own name.
      dismissLabel="Keep Run Active"
      confirmDisabled={isPending}
      dismissDisabled={isPending || recoveryPending}
    >
      <p className={MODAL_BODY_TEXT_CLASS}>
        This stops further loop scheduling for this run. Existing work remains
        visible in the run timeline and workflow.
      </p>
    </ConfirmDialog>
  );
}

function GovernanceFocus({
  pendingRef,
  onFocus,
}: {
  pendingRef: { current: boolean };
  onFocus: () => void;
}) {
  // Mount inside the portal: the plan targets do not exist when the page's
  // effect first sees the route change.
  useEffect(() => {
    if (pendingRef.current) {
      pendingRef.current = false;
      onFocus();
    }
  }, [pendingRef, onFocus]);
  return null;
}
