import { useParams, useSearch, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import {
	useRun,
	useRunExecution,
	useRunExecutions,
	useInfiniteRunNodeAttempts,
	useRunGraph,
  useTimeline,
  useRunAgents,
  useRoles,
  useMailbox,
  useMailboxMessage,
	useCancelRun,
	useRetryExecutionStop,
	type MailboxFilters,
	type CancelRunRequest,
} from "@/api/hooks";
import { invalidateRunDetailRecoveryQueries, invalidateRunQuerySurfaces, type RunQuerySurface } from "@/api/hooks-runs";
import { useRunEventStream } from "./use-run-event-stream";
import { isDegradedRunStreamState, isTerminalRunStatus, RUN_SURFACE_DEGRADED_REFRESH_MS } from "./run-freshness";
import { ApiError } from "@/api/client";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { InfoTooltip } from "@/components/ui/info-tooltip";
import { ConfirmDialog, MODAL_BODY_TEXT_CLASS } from "@/components/ui/modal-shell";
import { RelativeTime } from "@/components/ui/relative-time";
import { isSkipReviewValidationWorkflowId } from "@/lib/workflows";
import { cn } from "@/lib/utils";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import {
  Activity,
  GitBranch,
  Users,
  Mail,
  ArrowLeft,
  Shield,
  AlertTriangle,
  XCircle,
  CheckCircle2,
} from "lucide-react";
import { Link } from "@tanstack/react-router";
import { DAGVisualization } from "./dag-visualization";
import { AgentDetailPanel } from "./agent-detail-panel";
import { MailboxPanel } from "./mailbox-panel";
import { RunExecutionSurface } from "./run-execution-surface";
import { RunLiveStatus } from "./run-live-status";
import { GovernanceActions } from "./governance-actions";
import { CurrentPlanPanel } from "./current-plan-panel";
import { DecreePanel } from "./decree-panel";
import { buildMailboxPresentation } from "./mailbox-lanes";
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
import { runDisplayBadgeStatus, runDisplayStateLabel } from "./run-display-state";
import { PageHeader } from "@/components/page-layout";
import type { RunDetailTab } from "./run-detail-tabs";
import { readRunStreamBootstrapState } from "./run-cache-updates";
import { clearRunQuerySurfacesDirty, markRunQuerySurfacesDirty, useRunSurfaceFreshness } from "./run-surface-freshness";
import { mergeRunNodeExecutionItems, overlayExecutionDetail, useLiveExecutionProjection } from "./run-live-execution-projection";

const RUN_DETAIL_TIMELINE_PAGE_SIZE = 50;

type SelectedExecutionState = {
	runId: string;
	nodeId: string;
	attemptId: string | null;
};

const tabs: { id: RunDetailTab; label: string; icon: typeof GitBranch }[] = [
  { id: "graph", label: "DAG", icon: GitBranch },
	{ id: "execution", label: "Activity", icon: Activity },
	{ id: "agents", label: "Agents", icon: Users },
	{ id: "mailbox", label: "Messages", icon: Mail },
];
export function RunDetailPage() {
	const { runId } = useParams({ from: "/runs/$runId" });
	const search = useSearch({ from: "/runs/$runId" });
	const navigate = useNavigate();
	const queryClient = useQueryClient();
	const runMountStartedAtRef = useRef(0);
	const activeTab: RunDetailTab = search.tab ?? "graph";
	const runQuery = useRun(runId);
	const run = runQuery.data;
	const runReady = runQuery.isSuccess;
	const executionVisible = activeTab === "execution";
	const graphVisible = activeTab === "graph" || executionVisible || run?.status === "cancelled";
	const timelineVisible = executionVisible;
	const executionsQueryActive = graphVisible;
	const agentsVisible = activeTab === "agents";
	const mailboxVisible = activeTab === "mailbox";
	const hiddenRunQuerySurfaces = useMemo(
		() => hiddenRunDetailSurfaces({ graphVisible, executionsQueryActive, timelineVisible, agentsVisible, mailboxVisible }),
		[agentsVisible, executionsQueryActive, graphVisible, mailboxVisible, timelineVisible],
	);
	const graphQuery = useRunGraph(runId, { enabled: runReady && graphVisible });
	const graph = graphQuery.data;
	const graphIsFetching = graphQuery.isFetching;
	const refetchGraph = graphQuery.refetch;
	const executionsQuery = useRunExecutions(runId, { enabled: runReady && executionsQueryActive });
	const nodes = useMemo(() => executionsQuery.data?.items ?? [], [executionsQuery.data?.items]);
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
	const timelineQuery = useTimeline(runId, RUN_DETAIL_TIMELINE_PAGE_SIZE, { enabled: runReady && timelineVisible });
	const timelineIsPending = timelineQuery.isPending;
	const refetchTimeline = timelineQuery.refetch;
	const fetchNextTimelinePage = timelineQuery.fetchNextPage;
	const liveTimelineQuery = useLiveTimelineEvents(runId);
	const liveTimelineSyncQuery = useLiveTimelineSyncState(runId);
	const runSurfaceFreshnessQuery = useRunSurfaceFreshness(runId);
	const agentsQuery = useRunAgents(runId, { enabled: runReady && agentsVisible });
	const agents = agentsQuery.data;
	const agentsAreFetching = agentsQuery.isFetching;
	const refetchAgents = agentsQuery.refetch;
	const [selectedExecution, setSelectedExecution] = useState<SelectedExecutionState | null>(null);
	const [attemptHistoryDisclosure, setAttemptHistoryDisclosure] = useState<Record<string, boolean>>({});
	const storedSelectedExecution = selectedExecution?.runId === runId ? selectedExecution : null;
	const storedSelectedNode = presentedNodes.find((node) => node.node_id === storedSelectedExecution?.nodeId) ?? null;
	const selectedNode = storedSelectedNode ?? presentedNodes[0] ?? null;
	const selectedExecutionNodeId = selectedNode?.node_id ?? null;
	const attemptHistoryKey = selectedExecutionNodeId ? `${runId}:${selectedExecutionNodeId}` : "";
	const attemptHistoryOpen = !!attemptHistoryKey && attemptHistoryDisclosure[attemptHistoryKey] === true;
	const nodeAttemptsQuery = useInfiniteRunNodeAttempts(runId, selectedExecutionNodeId, { enabled: runReady && executionVisible && attemptHistoryOpen && !!selectedExecutionNodeId });
	const nodeAttempts = useMemo(
		() => nodeAttemptsQuery.data?.pages.flatMap((page) => page.items) ?? [],
		[nodeAttemptsQuery.data?.pages],
	);
	const selectedExecutionId = storedSelectedExecution?.attemptId ?? selectedNode?.current_execution?.attempt_id ?? null;
	const executionDetailQuery = useRunExecution(runId, selectedExecutionId, {
		enabled: runReady && executionVisible && !!selectedExecutionId,
	});
	const selectedExecutionDetailQuery = executionDetailQuery;
	const executionDetailIsFetching = selectedExecutionDetailQuery.isFetching;
	const refetchExecutionDetail = selectedExecutionDetailQuery.refetch;
	const presentedExecutionDetail = useMemo(
		() => selectedExecutionDetailQuery.data?.attempt_id === selectedExecutionId
			? overlayExecutionDetail(selectedExecutionDetailQuery.data, liveExecutionProjection[selectedExecutionDetailQuery.data.node_run_id])
			: null,
		[liveExecutionProjection, selectedExecutionDetailQuery.data, selectedExecutionId],
	);
	const detailPlaceholderMessage = selectedExecutionId && !executionDetailIsFetching ? "The selected attempt could not be loaded." : null;
	const { data: roles } = useRoles();
	const [mailboxFilters, setMailboxFilters] = useState<MailboxFilters>({});
	const mailboxQuery = useMailbox(runId, mailboxFilters, { enabled: runReady && mailboxVisible });
	const mailbox = mailboxQuery.data;
	const mailboxIsFetching = mailboxQuery.isFetching;
	const refetchMailbox = mailboxQuery.refetch;
	const cancelRun = useCancelRun(runId);
	const retryExecutionStop = useRetryExecutionStop(runId);
	const recoverableCancel = useRecoverableMutation(
		"cancel",
		(body: CancelRunRequest, key) => cancelRun.mutateAsync({ body, key }),
		runId,
	);
	const recoverableStopRetry = useRecoverableMutation(
		"retry-execution-stop",
		(attemptId: string, key) => retryExecutionStop.mutateAsync({ value: attemptId, key }),
		`${runId}:${selectedExecutionId ?? ""}`,
	);
	const [cancelDialogOpen, setCancelDialogOpen] = useState(false);
	const planApprovalTargetRef = useRef<HTMLDivElement | null>(null);
	const finalApprovalTargetRef = useRef<HTMLDivElement | null>(null);
	const activeTabPanelRef = useRef<HTMLDivElement | null>(null);

  const baseTimelineItems = useMemo(
	    () => [...(timelineQuery.data?.pages ?? [])].reverse().flatMap((page) => page.items ?? []),
	    [timelineQuery.data?.pages],
	  );
  const liveTimelineItems = useMemo(() => liveTimelineQuery.data ?? [], [liveTimelineQuery.data]);
  const timelineItems = useMemo(
	    () => mergeTimelineEvents(baseTimelineItems, liveTimelineItems),
	    [baseTimelineItems, liveTimelineItems],
	  );
  const hasTimelineItems = timelineItems.length > 0;
  const baseOldestTimelineSeq = baseTimelineItems[0]?.run_event_seq ?? 0;
  const baseLatestTimelineSeq = baseTimelineItems[baseTimelineItems.length - 1]?.run_event_seq ?? 0;
  const hiddenTimelineDirtyStartSeq = liveTimelineSyncQuery.data?.dirtyStartSeq ?? null;
  const hiddenTimelineDirtyEndSeq = liveTimelineSyncQuery.data?.dirtyEndSeq ?? null;
  const hiddenTimelineReconciliationAttempt = liveTimelineSyncQuery.data?.reconciliationAttempt ?? null;
  const hiddenTimelineRetryAtMs = liveTimelineSyncQuery.data?.nextRetryAtMs ?? null;
  const hasEarlierTimelinePages = timelineQuery.hasNextPage === true;
  const isTimelineFetchInFlight = timelineQuery.isFetching || timelineQuery.isFetchingNextPage;
  const [timelineRetryWakeTick, setTimelineRetryWakeTick] = useState(0);
	const mailboxPresentation = useMemo(
    () => buildMailboxPresentation(mailbox?.items ?? []),
    [mailbox?.items],
  );
	const timelineErrorMessage = timelineQuery.isError ? queryErrorMessage(timelineQuery.error, "The activity timeline could not be loaded right now.") : null;
	const executionsErrorMessage = executionsQuery.isError ? queryErrorMessage(executionsQuery.error, "The execution steps could not be loaded right now.") : null;
	const activitySurfaceState = useMemo(
		() => resolveActivitySurfaceState({
			timelinePending: timelineQuery.isPending,
			executionsPending: executionsQuery.isPending,
			hasTimelineItems,
			hasExecutionsResponse: executionsQuery.data !== undefined,
			timelineErrorMessage,
			executionsErrorMessage,
		}),
		[executionsErrorMessage, executionsQuery.data, executionsQuery.isPending, hasTimelineItems, timelineErrorMessage, timelineQuery.isPending],
	);
	const executionSummaryLoading = hasTimelineItems && executionsQuery.isPending && executionsQuery.data === undefined && !executionsErrorMessage;

  const setActiveTab = useCallback((tab: RunDetailTab) => {
    navigate({
      to: "/runs/$runId",
      params: { runId },
      search: { tab },
      replace: true,
    });
  }, [navigate, runId]);

  const roleDescriptions = useMemo(() => {
    const map: Record<string, string> = {};
    for (const role of roles?.items ?? []) {
      map[role.role_id] = role.description;
    }
    return map;
  }, [roles?.items]);

  // Mailbox detail state
  const [selectedMessageId, setSelectedMessageId] = useState<string | null>(null);
  const mailboxMessageQuery = useMailboxMessage(runId, selectedMessageId, {
    enabled: runReady && mailboxVisible,
  });
  const messageDetail = mailboxMessageQuery.data;
	const mailboxMessageIsFetching = mailboxMessageQuery.isFetching;
	const refetchMailboxMessage = mailboxMessageQuery.refetch;
  const canCancelRun =
    run?.status === "running" ||
    run?.status === "pending" ||
    run?.status === "awaiting_human_confirmation";
	  const cancelDialogVisible = cancelDialogOpen && canCancelRun;
	  const dirtyRunSurfaces = useMemo(
	    () => runSurfaceFreshnessQuery.data?.dirtySurfaces ?? [],
	    [runSurfaceFreshnessQuery.data?.dirtySurfaces],
	  );
	  const dirtyRunSurfaceSet = useMemo(() => new Set(dirtyRunSurfaces), [dirtyRunSurfaces]);
	const graphDirty = dirtyRunSurfaceSet.has("graph");
	const executionsDirty = dirtyRunSurfaceSet.has("executions");
	const timelineDirty = dirtyRunSurfaceSet.has("timeline");
	  const agentsDirty = dirtyRunSurfaceSet.has("agents");
	  const mailboxDirty = dirtyRunSurfaceSet.has("mailbox");

	const runStreamState = useRunEventStream(runId, runReady, !timelineVisible, hiddenRunQuerySurfaces);

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
	    if (runQuery.dataUpdatedAt === 0 || runQuery.dataUpdatedAt >= runMountStartedAtRef.current) {
	      return;
	    }

	    invalidateRunDetailRecoveryQueries(queryClient, runId);
	  }, [queryClient, runId, runQuery.dataUpdatedAt, runReady]);

	  useEffect(() => {
	    if (!runId || !timelineVisible || baseOldestTimelineSeq < 1 || baseLatestTimelineSeq < baseOldestTimelineSeq) {
	      return;
	    }
	    reconcileLiveTimelineEvents(queryClient, runId, baseOldestTimelineSeq, baseLatestTimelineSeq);
	  }, [baseLatestTimelineSeq, baseOldestTimelineSeq, queryClient, runId, timelineVisible]);

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
	}, [graphDirty, graphIsFetching, graphVisible, queryClient, refetchGraph, runId]);

	useEffect(() => {
		if (!runId || !executionsQueryActive || !executionsDirty || executionsAreFetching || (executionVisible && executionDetailIsFetching)) {
			return;
		}
		const refreshes: Array<Promise<unknown>> = [Promise.resolve(refetchExecutions())];
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
	    if (hiddenTimelineDirtyStartSeq === null || hiddenTimelineDirtyEndSeq === null) {
	      return;
	    }
	    if (timelineIsPending || isTimelineFetchInFlight || hiddenTimelineReconciliationAttempt !== null) {
	      return;
	    }
	    if (hiddenTimelineRetryAtMs !== null && hiddenTimelineRetryAtMs > Date.now()) {
	      return;
	    }

	    const settleReconciliationAttempt = (success: boolean) => {
	      settleLiveTimelineReconciliationAttempt(
	        queryClient,
	        runId,
	        success,
	        readCanonicalTimelineCoverage(queryClient, runId, RUN_DETAIL_TIMELINE_PAGE_SIZE),
	      );
	    };

	    if (baseLatestTimelineSeq < hiddenTimelineDirtyEndSeq) {
	      const started = startLiveTimelineReconciliationAttempt(queryClient, runId, {
	        kind: "refetch-tail",
	        dirtyStartSeq: hiddenTimelineDirtyStartSeq,
	        dirtyEndSeq: hiddenTimelineDirtyEndSeq,
	        baseOldestSeq: baseOldestTimelineSeq,
	        baseLatestSeq: baseLatestTimelineSeq,
	      });
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
	    if (baseOldestTimelineSeq > hiddenTimelineDirtyStartSeq && hasEarlierTimelinePages) {
	      const started = startLiveTimelineReconciliationAttempt(queryClient, runId, {
	        kind: "fetch-older",
	        dirtyStartSeq: hiddenTimelineDirtyStartSeq,
	        dirtyEndSeq: hiddenTimelineDirtyEndSeq,
	        baseOldestSeq: baseOldestTimelineSeq,
	        baseLatestSeq: baseLatestTimelineSeq,
	      });
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
		if (hiddenTimelineDirtyStartSeq !== null || hiddenTimelineDirtyEndSeq !== null) {
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
		if (!runId || !agentsVisible || !agentsDirty || agentsAreFetching) {
			return;
		}
		void Promise.resolve(refetchAgents())
			.then((result) => {
				if (isSuccessfulQueryResult(result)) {
					clearRunQuerySurfacesDirty(queryClient, runId, ["agents"]);
				}
			})
			.catch(() => {
				// leave dirty so the next visible retry can reconcile it
			});
	}, [agentsAreFetching, agentsDirty, agentsVisible, queryClient, refetchAgents, runId]);

	useEffect(() => {
		if (!runId || !mailboxVisible || !mailboxDirty || mailboxIsFetching || mailboxMessageIsFetching) {
			return;
		}
		const refreshes: Array<Promise<unknown>> = [Promise.resolve(refetchMailbox())];
		if (selectedMessageId) {
			refreshes.push(Promise.resolve(refetchMailboxMessage()));
		}
		void Promise.all(refreshes)
			.then((results) => {
				if (results.every((result) => isSuccessfulQueryResult(result))) {
					clearRunQuerySurfacesDirty(queryClient, runId, ["mailbox"]);
				}
			})
			.catch(() => {
				// leave dirty so the next visible retry can reconcile it
			});
	}, [
		mailboxDirty,
		mailboxIsFetching,
		mailboxMessageIsFetching,
		mailboxVisible,
		queryClient,
		refetchMailbox,
		refetchMailboxMessage,
		runId,
		selectedMessageId,
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
	      markRunQuerySurfacesDirty(queryClient, runId, hiddenRunDetailSurfaces({
	      	graphVisible,
	      	executionsQueryActive,
	      	timelineVisible,
	      	agentsVisible,
	      	mailboxVisible,
	      }));
	      invalidateRunQuerySurfaces(queryClient, runId, degradedRunRefreshSurfaces({
	      	graphVisible,
	      	executionsQueryActive,
	      	timelineVisible,
	      	agentsVisible,
	      	mailboxVisible,
	      }));
	    };

    refreshActiveRunQueries();
    const timer = window.setInterval(refreshActiveRunQueries, RUN_SURFACE_DEGRADED_REFRESH_MS);
    return () => {
      window.clearInterval(timer);
    };
	  	}, [agentsVisible, executionsQueryActive, graphVisible, mailboxVisible, queryClient, run, runId, runReady, runStreamState, timelineVisible]);

	const handleSelectExecutionStep = useCallback((nodeId: string) => {
		const node = presentedNodes.find((item) => item.node_id === nodeId);
		if (!node) {
			return;
		}
		setSelectedExecution({ runId, nodeId: node.node_id, attemptId: null });
	}, [presentedNodes, runId]);

	const handleViewNodeActivity = useCallback((nodeId: string) => {
		const node = presentedNodes.find((item) => item.node_id === nodeId);
		if (!node) {
			return;
		}
		setSelectedExecution({ runId, nodeId: node.node_id, attemptId: null });
		setActiveTab("execution");
	}, [presentedNodes, runId, setActiveTab]);

	const handleSelectExecutionAttempt = useCallback((attemptId: string) => {
		const nodeId = selectedExecutionNodeId;
		if (!nodeId) {
			return;
		}
		if (!nodeAttempts.some((item) => item.attempt_id === attemptId)) {
			return;
		}
		setSelectedExecution({ runId, nodeId, attemptId });
	}, [nodeAttempts, runId, selectedExecutionNodeId]);

	const handleSelectCurrentExecutionAttempt = useCallback(() => {
		if (!selectedExecutionNodeId) {
			return;
		}
		setSelectedExecution({ runId, nodeId: selectedExecutionNodeId, attemptId: null });
	}, [runId, selectedExecutionNodeId]);

	const toggleAttemptHistory = useCallback(() => {
		if (!attemptHistoryKey) {
			return;
		}
		setAttemptHistoryDisclosure((current) => ({ ...current, [attemptHistoryKey]: !current[attemptHistoryKey] }));
	}, [attemptHistoryKey]);

  const handleSelectMessage = useCallback((messageId: string | null) => {
    if (!messageId) {
      setSelectedMessageId(null);
      return;
    }
    setSelectedMessageId((prev) => (prev === messageId ? null : messageId));
  }, []);

  const handleCloseCancelDialog = () => {
    if (!cancelRun.isPending && !recoverableCancel.hasPendingRecovery) {
      setCancelDialogOpen(false);
    }
  };

  const handleConfirmCancelRun = async () => {
    try {
      await recoverableCancel.mutateAsync({ reason: "manual cancellation from run detail" });
      setCancelDialogOpen(false);
    } catch {
      // surfaced via mutation error UI below
    }
  };

	const jumpToGovernanceAction = useCallback(() => {
		const isAwaitingPlanApproval = run?.autonomous_approvals === false && run.display_state === "awaiting_plan_decision";
		const isAwaitingFinalApproval = run?.autonomous_approvals === false && run.display_state === "awaiting_final_decision";
		const target = isAwaitingPlanApproval
			? planApprovalTargetRef.current
			: isAwaitingFinalApproval
				? finalApprovalTargetRef.current
				: null;
		if (!target) {
			return;
		}

		const prefersReducedMotion = typeof window !== "undefined"
			&& typeof window.matchMedia === "function"
			&& window.matchMedia("(prefers-reduced-motion: reduce)").matches;
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

  if (runQuery.isPending) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="animate-pulse text-foreground-muted">Loading run...</div>
      </div>
    );
  }

	if (runQuery.isError) {
    const isNotFound = runQuery.error instanceof ApiError && runQuery.error.status === 404;
    const loadErrorMessage = runQuery.error instanceof Error
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
  const untilAllApprove = !!run.run_until_all_approve;
  const skipReviewAndValidation = isSkipReviewValidationWorkflowId(run.workflow_id);
  const governanceJumpLabel = governanceState.isAwaitingPlanApproval
    ? "Review Plan"
    : governanceState.isAwaitingFinalApproval
      ? "Review Final Decision"
      : null;

	const reviewReworkCycles = run.review_rework_cycles;
	const validationReworkWaves = run.validation_rework_waves;
	const reviewAutoSucceededCount = run.review_auto_succeeded_count;
	const validationAutoSucceededCount = run.validation_auto_succeeded_count;
	const reviewAutoSucceededLabel = "Review Auto-Succeeded at Cap";
	const validationAutoSucceededLabel = "Validation Auto-Succeeded at Cap";
  const mailboxErrorMessage = !mailboxQuery.data && mailboxQuery.isError
    ? queryErrorMessage(mailboxQuery.error, "The messages could not be loaded right now.")
    : null;
	const showHeaderNotices =
		untilAllApprove ||
		governanceState.noticeTitle !== null ||
		!!cancelRun.error;

	const activeTabId = `run-tab-${activeTab}`;
	const activePanelId = `run-tabpanel-${activeTab}`;
	// `relative` for the same reason PageContent carries it: a scroll viewport
	// must be the containing block for its absolutely positioned descendants,
	// or they escape the clip and grow the document instead.
	const managedTabPanelClass = activeTab === "graph"
		? "min-h-0 overflow-y-auto lg:flex lg:flex-col lg:overflow-hidden"
		: activeTab === "mailbox"
			? "flex flex-col overflow-hidden"
			: "relative overflow-auto";

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader>
        <div className="flex w-full items-center gap-3">
          <div className="flex min-w-0 flex-1 items-center gap-3">
            <Link to="/runs" className="text-foreground-subtle hover:text-foreground">
              <ArrowLeft className="h-4 w-4" />
            </Link>
	            <h1 className="truncate text-wrap-anywhere font-display text-xl font-bold text-gold">
	              {run.decree_title}
	            </h1>
          </div>

          <div data-testid="run-header-command-strip" className="ml-auto flex shrink-0 items-center gap-2">
            <div data-testid="run-primary-controls" className="flex items-center gap-2">
              <Badge data-testid="run-status-pill" className="h-7 px-2.5 leading-none" variant="status" status={runDisplayBadgeStatus(run)}>
                {runDisplayStateLabel(run)}
              </Badge>
              {canCancelRun && (
                <button
                  data-testid="run-cancel-pill"
                  type="button"
                  onClick={() => setCancelDialogOpen(true)}
                  disabled={cancelRun.isPending}
                  className="inline-flex h-7 items-center gap-1 rounded-md border border-status-failed/50 bg-status-failed/10 px-2.5 text-xs font-medium leading-none text-status-failed hover:bg-status-failed/20 disabled:opacity-60"
                >
                  <XCircle className="h-3.5 w-3.5" />
                  {cancelRun.isPending ? "Cancelling..." : "Cancel Run"}
                </button>
              )}
            </div>
          </div>

          <div data-testid="run-header-meta-group" className="hidden flex-none items-center gap-2 sm:flex">
            <span data-testid="run-header-divider" aria-hidden className="h-6 w-px bg-border/80" />

            <div data-testid="run-header-meta" className="flex min-w-0 items-center gap-2 text-xs text-foreground-subtle xl:gap-3">
	              <span data-testid="run-created-meta" className="inline-flex items-center gap-1 whitespace-nowrap leading-none tabular-nums">
	                <span className="hidden xl:inline">Created:</span>
	                <span className="xl:hidden">C:</span>
	                <span data-testid="run-created-meta-value" className="inline-block min-w-[7ch]">
	                  <RelativeTime iso={run.created_at} />
	                </span>
	              </span>
	              <span data-testid="run-last-activity-meta" className="inline-flex items-center gap-1 whitespace-nowrap leading-none tabular-nums">
	                <span className="hidden xl:inline">Last Activity:</span>
	                <span className="xl:hidden">A:</span>
	                <span data-testid="run-last-activity-meta-value" className="inline-block min-w-[7ch]">
	                  <RelativeTime iso={run.last_activity_at} />
	                </span>
	              </span>
            </div>
          </div>
        </div>
      </PageHeader>

		<RunLiveStatus runId={runId} nodes={presentedNodes} />

      {showHeaderNotices && (
        <div data-testid="run-header-notices" className="border-b border-border bg-surface-raised/20 px-page py-2">
          <div className="flex flex-wrap items-center gap-2">
            {governanceState.noticeTitle && governanceState.noticeMessage && (
              <div
                data-testid="run-governance-notice"
                className={cn(
                  "inline-flex items-center gap-2 rounded-md border px-2.5 py-2 text-xs",
                  accentBannerClass("gold"),
                )}
              >
                <Shield className="h-3.5 w-3.5 shrink-0" />
                <div className="space-y-0.5">
                  <p className="font-medium leading-none">{governanceState.noticeTitle}</p>
                  <p className="leading-snug">{governanceState.noticeMessage}</p>
                </div>
                {governanceJumpLabel && (
                  <Button
                    data-testid="run-governance-jump"
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={jumpToGovernanceAction}
                    className="ml-1 h-7 border-gold/35 bg-background/80 text-gold hover:bg-gold/10 hover:text-gold-bright"
                  >
                    {governanceJumpLabel}
                  </Button>
                )}
              </div>
            )}
	            {untilAllApprove && (
              <span data-testid="run-unbounded-warning" className="inline-flex items-center gap-1 rounded-md border border-status-blocked/30 bg-status-blocked/10 px-2 py-1 text-xs text-status-blocked">
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                <span>Unbounded review/validation mode: run continues until PASS or manual cancel.</span>
              </span>
	            )}
	            {cancelRun.error && (
	              <div className="rounded-md border border-status-failed/30 bg-status-failed/10 px-2.5 py-2 text-xs text-status-failed">
	                Failed to cancel run: {cancelRun.error.message}
              </div>
            )}
          </div>
        </div>
      )}

      <div className="flex-1 min-h-0 overflow-hidden px-page py-3">
        <div data-testid="run-detail-layout" className="grid h-full min-h-0 gap-3 lg:grid-cols-2">
          <section className="order-2 flex min-h-0 flex-col rounded-md border border-border bg-background lg:order-1">
	            {(failedNodeCount > 0 || cancelledNodes.length > 0) && (
	              <div className="border-b border-border bg-surface-raised/50 px-3 py-3">
	                 <GovernanceActions run={run} failedNodes={run.failed_nodes} cancelledNodes={cancelledNodes} showFinalApproval={false} />
              </div>
            )}

            <div className="border-b border-border bg-surface-raised/30 px-3 py-2.5 text-xs text-foreground-subtle">
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
                {skipReviewAndValidation ? (
                  <span className="inline-flex items-center gap-1 rounded-md border border-gold/35 bg-gold/10 px-2 py-0.5 text-gold">
                    <CheckCircle2 className="h-3.5 w-3.5" />
                    Review and validation stages were skipped for this decree.
                  </span>
                ) : (
                  <>
	                    <span className="inline-flex items-center gap-1">
						Review Rework Requests: <strong className="text-foreground">{reviewReworkCycles}</strong>
                      <CycleInfo
                        label="Review cycles info"
	                        message="How many times review requested rework."
                      />
                    </span>
                    <span className="inline-flex items-center gap-1">
						Validation Rework Requests: <strong className="text-foreground">{validationReworkWaves}</strong>
	                      <CycleInfo
	                        label="Validation cycles info"
	                        message="How many times validation requested rework."
	                      />
	                    </span>
                    <span>Review Cap: {run.max_execution_review_cycles ?? 5}</span>
                    <span>Validation Cap: {run.max_validation_rework_cycles ?? 5}</span>
	                    {!untilAllApprove && reviewAutoSucceededCount > 0 && (
	                      <Badge variant="status" status="running" className="gap-1 border border-status-running/40">
	                        <CheckCircle2 className="h-3.5 w-3.5" />
	                        {reviewAutoSucceededLabel}
							<CycleInfo label="Review auto-succeeded count" message="Review auto-succeeded after the review cap was reached." />
	                      </Badge>
	                    )}
	                    {!untilAllApprove && validationAutoSucceededCount > 0 && (
	                      <Badge variant="status" status="running" className="gap-1 border border-status-running/40">
	                        <CheckCircle2 className="h-3.5 w-3.5" />
	                        {validationAutoSucceededLabel}
							<CycleInfo label="Validation auto-succeeded count" message="Validation auto-succeeded after the validation cap was reached." />
	                      </Badge>
	                    )}
                    {untilAllApprove && <span className="text-status-blocked">Cap Enforcement Disabled</span>}
                  </>
                )}
              </div>
            </div>

            <div className="border-b border-border px-2 md:px-3">
              <nav
                data-testid="run-tab-nav"
                className="flex gap-0 overflow-x-auto"
                role="tablist"
                aria-label="Run detail sections"
              >
				{tabs.map((tab) => {
					const count =
						tab.id === "execution"
							? undefined
							: tab.id === "agents"
								? run.tab_counts.agents
								: tab.id === "mailbox"
									? run.tab_counts.messages
		                            : run.tab_counts.dag_nodes;

                  return (
                    <button
                      key={tab.id}
                      id={`run-tab-${tab.id}`}
                      onClick={() => setActiveTab(tab.id)}
                      role="tab"
                      aria-selected={activeTab === tab.id}
                      aria-controls={`run-tabpanel-${tab.id}`}
                      tabIndex={activeTab === tab.id ? 0 : -1}
                      className={cn(
                        "flex shrink-0 items-center gap-2 border-b-2 px-3 py-2.5 text-sm font-medium transition-colors",
                        activeTab === tab.id
                          ? "border-gold text-gold"
                          : "border-transparent text-foreground-muted hover:text-foreground"
                      )}
                    >
                      <tab.icon className="h-3.5 w-3.5" />
                      {tab.label}
                      {count !== undefined && count > 0 && (
                        <span className="rounded-full bg-surface-raised px-1.5 text-[10px]">
                          {count}
                        </span>
                      )}
                    </button>
                  );
                })}
              </nav>
            </div>

			<div
				ref={activeTabPanelRef}
				id={activePanelId}
				role="tabpanel"
				aria-labelledby={activeTabId}
				className={cn(
					"min-h-0 flex-1 p-3",
					managedTabPanelClass,
				)}
			>
	              {activeTab === "graph" && graph && (
					<DAGVisualization
						graph={graph}
						nodes={presentedNodes}
						onViewActivity={handleViewNodeActivity}
						executionsErrorMessage={executionsErrorMessage}
						onRetryExecutions={() => {
							void executionsQuery.refetch();
						}}
					/>
				)}
	              {activeTab === "graph" && graphQuery.isPending && !graph && <TabLoading />}
	              {activeTab === "graph" && graphQuery.isError && !graph && (
	                <TabError
	                  message={queryErrorMessage(graphQuery.error, "The DAG could not be loaded right now.")}
	                  onRetry={() => {
	                    void graphQuery.refetch();
	                  }}
	                />
	              )}

	              {activeTab === "execution" && activitySurfaceState === "loading" && <TabLoading />}

				  {activeTab === "execution" && activitySurfaceState === "error" && (
	                <TabError
	                  message={timelineErrorMessage ?? executionsErrorMessage ?? "The activity surface could not be loaded right now."}
	                  onRetry={() => {
						if (timelineErrorMessage) {
							void timelineQuery.refetch();
							return;
						}
						void executionsQuery.refetch();
	                  }}
	                />
	              )}

	              {activeTab === "execution" && timelineErrorMessage && activitySurfaceState !== "error" && (
					<TabInlineError
						message={timelineErrorMessage}
						onRetry={() => {
							void timelineQuery.refetch();
						}}
					/>
				)}

	              {(activeTab === "execution" && (activitySurfaceState === "ready" || activitySurfaceState === "ready_with_execution_error")) && (
					<RunExecutionSurface
						run={run}
						nodes={presentedNodes}
						attempts={nodeAttempts}
						executionsErrorMessage={executionsErrorMessage}
					showPrimaryExecutionError={activitySurfaceState === "ready_with_execution_error"}
					isSummaryLoading={executionSummaryLoading}
					onRetryExecutions={() => {
							void executionsQuery.refetch();
						}}
						selectedNodeId={selectedExecutionNodeId}
						selectedAttemptId={selectedExecutionId}
						onSelectStep={handleSelectExecutionStep}
						onSelectAttempt={handleSelectExecutionAttempt}
						onSelectCurrentAttempt={handleSelectCurrentExecutionAttempt}
						historyOpen={attemptHistoryOpen}
						onToggleHistory={toggleAttemptHistory}
						hasMoreAttempts={nodeAttemptsQuery.hasNextPage === true}
						onLoadMoreAttempts={() => {
							void nodeAttemptsQuery.fetchNextPage();
						}}
						isLoadingMoreAttempts={nodeAttemptsQuery.isFetchingNextPage}
						isHistoryLoading={nodeAttemptsQuery.isPending && !nodeAttemptsQuery.data}
						detail={presentedExecutionDetail}
						isDetailLoading={executionDetailIsFetching && !selectedExecutionDetailQuery.data}
						detailErrorMessage={selectedExecutionDetailQuery.isError ? queryErrorMessage(selectedExecutionDetailQuery.error, "The execution detail could not be loaded right now.") : null}
						detailPlaceholderMessage={detailPlaceholderMessage}
							onRetryDetail={() => {
								void executionDetailQuery.refetch();
							}}
							onRetryStop={(attemptId) => {
								void recoverableStopRetry.mutateAsync(attemptId);
							}}
							isStopRetryPending={retryExecutionStop.isPending || recoverableStopRetry.hasPendingRecovery}
							stopRetryErrorMessage={retryExecutionStop.error ? queryErrorMessage(retryExecutionStop.error, "Kingdom could not retry the stop request.") : null}
						events={timelineItems}
						graph={graph}
						hasMoreEvents={hasEarlierTimelinePages}
						onLoadMoreEvents={() => {
							void timelineQuery.fetchNextPage();
						}}
						isLoadingMoreEvents={timelineQuery.isFetchingNextPage}
					/>
	              )}

              {activeTab === "agents" && (
                <AgentDetailPanel agents={agents?.items ?? []} roleDescriptions={roleDescriptions} />
              )}

              {activeTab === "mailbox" && (
                <MailboxPanel
                  key={mailboxPresentation.visibleMessageCount === 0 ? "empty" : "populated"}
                  presentation={mailboxPresentation}
                  isLoading={mailboxQuery.isPending && !mailboxQuery.data}
                  errorMessage={mailboxErrorMessage}
                  onRetry={() => {
                    void mailboxQuery.refetch();
                  }}
                  onSelectMessage={handleSelectMessage}
                  selectedDetail={selectedMessageId ? messageDetail : null}
                  filters={mailboxFilters}
                  onChangeFilters={setMailboxFilters}
                />
              )}
            </div>
          </section>

          <aside className="order-1 min-h-0 lg:order-2">
            <div className="h-full lg:sticky lg:top-3">
              <div data-testid="run-sidebar-scroll" className="h-full overflow-y-auto pr-1 [scrollbar-gutter:stable]">
                <div className="space-y-3">
                  <DecreePanel run={run} roles={roles?.items ?? []} />
                  <CurrentPlanPanel run={run} approvalSectionRef={planApprovalTargetRef} />
                  {governanceState.isAwaitingFinalApproval && (
                    <GovernanceActions run={run} failedNodes={run.failed_nodes} showNodeActions={false} finalApprovalCardRef={finalApprovalTargetRef} />
                  )}
                </div>
              </div>
            </div>
          </aside>
        </div>
      </div>

      <CancelRunDialog
        open={cancelDialogVisible}
        runTitle={run.decree_title}
        isPending={cancelRun.isPending}
        recoveryPending={recoverableCancel.hasPendingRecovery}
        onClose={handleCloseCancelDialog}
        onConfirm={() => {
          void handleConfirmCancelRun();
        }}
      />
    </div>
  );
}

function CycleInfo({ label, message }: { label: string; message: string }) {
  return (
    <InfoTooltip label={label} message={message} />
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

function resolveActivitySurfaceState({
	timelinePending,
	executionsPending,
	hasTimelineItems,
	hasExecutionsResponse,
	timelineErrorMessage,
	executionsErrorMessage,
}: {
	timelinePending: boolean;
	executionsPending: boolean;
	hasTimelineItems: boolean;
	hasExecutionsResponse: boolean;
	timelineErrorMessage: string | null;
	executionsErrorMessage: string | null;
}): "loading" | "error" | "ready" | "ready_with_execution_error" {
	if (!hasTimelineItems && timelinePending) {
		return "loading";
	}
	if (!hasTimelineItems && !hasExecutionsResponse) {
		if (executionsPending) {
			return "loading";
		}
		if (timelineErrorMessage || executionsErrorMessage) {
			return "error";
		}
	}
	if (hasTimelineItems && !hasExecutionsResponse && executionsErrorMessage) {
		return "ready_with_execution_error";
	}
	return "ready";
}

function degradedRunRefreshSurfaces({
	graphVisible,
	executionsQueryActive,
	timelineVisible,
	agentsVisible,
	mailboxVisible,
}: {
	graphVisible: boolean;
	executionsQueryActive: boolean;
	timelineVisible: boolean;
	agentsVisible: boolean;
	mailboxVisible: boolean;
}) {
	const surfaces: RunQuerySurface[] = ["detail", "plan", "governanceSummary", "governanceHistory"];
	if (executionsQueryActive) {
		surfaces.push("executions");
	}
	if (graphVisible) {
		surfaces.push("graph");
	}
	if (timelineVisible) {
		surfaces.push("timeline");
	}
	if (agentsVisible) {
		surfaces.push("agents");
	}
	if (mailboxVisible) {
		surfaces.push("mailbox");
	}
	return surfaces;
}

function hiddenRunDetailSurfaces({
	graphVisible,
	executionsQueryActive,
	timelineVisible,
	agentsVisible,
	mailboxVisible,
}: {
	graphVisible: boolean;
	executionsQueryActive: boolean;
	timelineVisible: boolean;
	agentsVisible: boolean;
	mailboxVisible: boolean;
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
	if (!agentsVisible) {
		surfaces.push("agents");
	}
	if (!mailboxVisible) {
		surfaces.push("mailbox");
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
			confirmLabel={isPending ? "Cancelling..." : recoveryPending ? "Retry Cancel Request" : "Cancel Run"}
			// The only confirm that overrides the default dismiss verb, because
			// here "Cancel" is the destructive action's own name.
			dismissLabel="Keep Run Active"
			confirmDisabled={isPending}
			dismissDisabled={isPending || recoveryPending}
		>
			<p className={MODAL_BODY_TEXT_CLASS}>
				This stops further loop scheduling for this run. Existing work remains visible in the run timeline,
				DAG, and mailbox.
			</p>
		</ConfirmDialog>
	);
}
