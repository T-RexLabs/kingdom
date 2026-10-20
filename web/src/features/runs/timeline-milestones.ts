import type { RunExecution, RunGraphEdge, RunGraphNode, TimelineEvent } from "@/api/hooks";
import { formatElapsedDuration, nodeLabel } from "@/lib/utils";
import type { RunStatusTone } from "./run-detail-tones";
import { attemptEventLabelFragment, executionDisplayStatus, isReworkTimelineEvent, normalizedRunStatusTone, reworkRequestLabel } from "./run-status-semantics";
import { operatorFacingExecutionFailureText } from "./execution-failure-copy";
import { presentRecoveryTimeline, type RecoveryLike } from "./run-execution-presentation";
import type { Milestone, RawEventLabelResult, TimelineLookups } from "./timeline-types";

const SPECIAL_REASON_LABELS: Record<string, string> = {
  execution_review_cap_auto_succeeded: "Review Cap Auto-Succeeded",
  validation_gate_cap_auto_succeeded: "Validation Cap Auto-Succeeded",
  run_cancelled_by_user: "Run Cancelled by User",
};

const REVIEW_CAP_AUTO_SUCCEEDED_BADGE = "Review Cap Auto-Succeeded";
const VALIDATION_CAP_AUTO_SUCCEEDED_BADGE = "Validation Cap Auto-Succeeded";
const LOOP_CAP_AUTO_SUCCEEDED_BADGE = "Cap Auto-Succeeded";

const NOISE_REASON_CODES = new Set(["orchestration_progress"]);

const NODE_LIFECYCLE_EVENT_TYPES = new Set(["node.state_changed"]);
const RECOVERY_QUEUE_EVENT_TYPES = new Set(["attempt.retry_queued", "attempt.rerun_queued"]);
const RECOVERY_RUN_REOPEN_EVENT_TYPES = new Set(["run.retry_reopened", "run.rerun_reopened"]);

interface FanOutGroup {
  parentNodeId: string;
  childNodeIds: string[];
}

function fanOutLoopCapBadge(milestones: Milestone[]): string | undefined {
  const hasReview = milestones.some((milestone) => milestone.badge === REVIEW_CAP_AUTO_SUCCEEDED_BADGE);
  const hasValidation = milestones.some((milestone) => milestone.badge === VALIDATION_CAP_AUTO_SUCCEEDED_BADGE);
  if (hasReview && hasValidation) {
    return LOOP_CAP_AUTO_SUCCEEDED_BADGE;
  }
  if (hasReview) {
    return REVIEW_CAP_AUTO_SUCCEEDED_BADGE;
  }
  if (hasValidation) {
    return VALIDATION_CAP_AUTO_SUCCEEDED_BADGE;
  }
  return undefined;
}

export function isNoiseReasonCode(reasonCode?: string | null): boolean {
  return !!reasonCode && NOISE_REASON_CODES.has(reasonCode);
}

export function specialReasonLabel(event: TimelineEvent): string {
  const reasonCode = typeof event.reason_code === "string" ? event.reason_code.trim() : "";
  if (reasonCode !== "") {
    return SPECIAL_REASON_LABELS[reasonCode] ?? "";
  }
  const payload = event.payload_json ?? {};
  const action = typeof payload.action === "string" ? payload.action.trim().toLowerCase() : "";
  if (event.event_type === "run.state_changed" && action === "cancel") {
    return "Run Cancelled by User";
  }
  return "";
}

export function buildTimelineLookups(events: TimelineEvent[], nodes: RunGraphNode[], executions: RunExecution[] = []): TimelineLookups {
  const nridToNodeId = new Map<string, string>();
  for (const event of events) {
    const payload = event.payload_json ?? {};
    const nodeId = typeof payload.node_id === "string" ? payload.node_id : "";
    const nodeRunID = typeof payload.node_run_id === "string" ? payload.node_run_id : "";
    if (nodeId && nodeRunID) {
      nridToNodeId.set(nodeRunID, nodeId);
    }
  }

  const nodeStateMap = new Map<string, RunGraphNode>();
  const currentNodeRunIDMap = new Map<string, string>();
  for (const node of nodes) {
    nodeStateMap.set(node.node_id, node);
    if (typeof node.node_run_id === "string" && node.node_run_id.trim() !== "") {
      currentNodeRunIDMap.set(node.node_id, node.node_run_id.trim());
    }
  }

  const executionStateMap = new Map<string, RunExecution>();
  for (const execution of executions) {
    const existing = executionStateMap.get(execution.node_run_id);
    if (!existing || execution.activity_at > existing.activity_at) {
      executionStateMap.set(execution.node_run_id, execution);
    }
  }

  return { nridToNodeId, nodeStateMap, currentNodeRunIDMap, executionStateMap };
}

export function buildRawEventLabel(
  event: TimelineEvent,
  lookups: TimelineLookups,
): RawEventLabelResult {
  const payload = event.payload_json ?? {};

  let resolvedNodeId = typeof payload.node_id === "string" ? payload.node_id : "";
  if (!resolvedNodeId) {
    const nodeRunID = typeof payload.node_run_id === "string" ? payload.node_run_id : "";
    if (nodeRunID && lookups.nridToNodeId.has(nodeRunID)) {
      resolvedNodeId = lookups.nridToNodeId.get(nodeRunID) ?? "";
    }
  }

  const name = resolvedNodeId ? nodeLabel(resolvedNodeId) : "";
	const recoveryTimeline = recoveryTimelineFromEvent(event);
	if (recoveryTimeline) {
		return { label: recoveryTimeline.label, detail: name || undefined };
	}

	if (event.event_type === "node.state_changed") {
		const state = typeof payload.state === "string" ? payload.state : "";
		if (state === "starting") {
			return { label: `${name || "Node"} Launching` };
		}
		if (state === "ready") {
		  return { label: `${name || "Node"} Queued` };
		}
    const humanState = state ? titleCaseEventFragment(state.replace(/_/g, " ")) : "Updated";
		return { label: `${name || "Node"} ${humanState}` };
  }

  if (event.event_type === "run.state_changed") {
    const status = typeof payload.status === "string" ? payload.status : "";
    const planState = typeof payload.plan_state === "string" ? payload.plan_state : "";

    if (status === "running") return { label: "Run Started" };
    if (status === "failed") {
      const reason = event.reason_code && !specialReasonLabel(event) && !isNoiseReasonCode(event.reason_code)
        ? event.reason_code.replace(/_/g, " ")
        : undefined;
      return {
        label: "Run Failed",
        detail: reason ? reason.charAt(0).toUpperCase() + reason.slice(1) : undefined,
      };
    }
	    if (status === "succeeded" || status === "completed") return { label: "Run Succeeded" };
    if (status === "cancelled") return { label: "Run Cancelled" };
    if (planState) {
      return { label: `Plan Entered ${titleCaseEventFragment(planState.replace(/_/g, " "))}` };
    }
    return { label: "Run State Changed" };
  }

	if (event.event_type === "attempt.started") {
		const attempt = typeof payload.attempt === "number" ? payload.attempt : 0;
		const parts: string[] = [];
		if (attempt > 1) parts.push(`attempt ${attempt}`);
		return {
			label: `${name || "Node"} Started`,
			detail: parts.length > 0 ? parts.join(" · ") : undefined,
		};
	}

	if (event.event_type === "attempt.progress" && typeof payload.text === "string" && payload.text.trim() !== "") {
		return {
			label: `${name ? `${name} ` : ""}Provisional Stream Text`,
			detail: "Not Canonical",
		};
	}

	if (event.event_type === "attempt.succeeded") {
		if (resolvedNodeId) {
		  return { label: `${name} Succeeded` };
		}
		const reason = event.reason_code && !specialReasonLabel(event) && !isNoiseReasonCode(event.reason_code)
		  ? event.reason_code.replace(/_/g, " ")
		  : undefined;
		return { label: "Node Succeeded", detail: reason || undefined };
	}

	if (event.event_type === "attempt.failed") {
		if (isReworkTimelineEvent(event)) {
			return {
				label: resolvedNodeId ? `${name} Requested Rework` : titleCaseEventFragment(reworkRequestLabel(event.reason_code)),
			};
		}
		const detail = failureDetailFromEvent(event, executionForEvent(event, lookups));
		if (resolvedNodeId) {
		  return { label: `${name} Failed`, detail: detail || undefined };
    }
    return { label: "Node Failed", detail: detail || undefined };
  }

  if (event.event_type === "attempt.recovery_blocked") {
    const operatorText = typeof payload.operator_text === "string" ? payload.operator_text : "";
    return { label: `${name || "Node"} Recovery Blocked`, detail: operatorText || undefined };
  }

  if (event.event_type === "attempt.timeout_warning") {
    const elapsedMs = typeof payload.elapsed_ms === "number" ? payload.elapsed_ms : 0;
    const timeoutMs = typeof payload.timeout_ms === "number" ? payload.timeout_ms : 0;
    return {
      label: `${name || "Node"} Approaching Timeout`,
      detail: elapsedMs > 0 && timeoutMs > 0
        ? `${Math.round(elapsedMs / 1000)}s / ${Math.round(timeoutMs / 1000)}s`
        : undefined,
    };
  }

	const attemptLabelFragment = attemptEventLabelFragment(event);
	if (attemptLabelFragment) {
		return {
			label: `${name || "Node"} ${titleCaseEventFragment(attemptLabelFragment)}`,
		};
	}

  if (event.event_type === "mailbox.message_recorded") {
    const communicationType = typeof payload.communication_type === "string"
      ? payload.communication_type.trim().toLowerCase()
      : "";
    switch (communicationType) {
      case "approval_request":
        return {
          label: name ? `${name} Awaiting Approval` : "Approval Requested",
        };
      case "approval_response":
        return {
          label: name ? `${name} Approval Recorded` : "Approval Recorded",
        };
      case "instruction_dispatched":
        return {
				label: `${name || "Node"} Instruction Sent`,
        };
      case "response_received":
        return {
				label: `${name || "Node"} Response Received`,
        };
      default:
        return {
				label: name ? `${name} Activity Recorded` : "Mailbox Activity Recorded",
        };
    }
  }

  if (event.event_type === "approval.recorded") {
    const decision = typeof payload.decision === "string" ? payload.decision : "";
    const mode = typeof payload.mode === "string" ? payload.mode : "";
    return {
      label: decision === "rework" ? "Rework Requested" : "Approval Recorded",
      detail: mode || undefined,
    };
  }

  if (event.event_type === "escalation.raised") {
    const reason = event.reason_code && !specialReasonLabel(event) && !isNoiseReasonCode(event.reason_code)
      ? event.reason_code.replace(/_/g, " ")
      : undefined;
    return {
      label: "Escalation Raised",
      detail: reason ? reason.charAt(0).toUpperCase() + reason.slice(1) : undefined,
    };
  }

  const fallback = titleCaseEventFragment(event.event_type.replace(/[._]/g, " "));
  return { label: name ? `${name} - ${fallback}` : fallback };
}

// Only application-owned event fragments pass through here, never node names or payload text.
function titleCaseEventFragment(fragment: string): string {
  return fragment.replace(/\b[a-z]/g, (letter) => letter.toUpperCase());
}

export function synthesizeMilestones(
  events: TimelineEvent[],
  nodes: RunGraphNode[],
  edges: RunGraphEdge[],
  lookups: TimelineLookups,
): Milestone[] {
  const nodeEvents = new Map<string, TimelineEvent[]>();
   const runEvents: TimelineEvent[] = [];
   const orphanedAttempts: TimelineEvent[] = [];
	const suppressedResponseMilestoneEventIDs = buildSuppressedResponseMilestoneEventIDs(events);
	const recoveryMilestones: Array<{ event: TimelineEvent; label: string; status: Milestone["status"] }> = [];

	  for (const event of events) {
	    if (suppressedResponseMilestoneEventIDs.has(event.event_id)) {
	      continue;
	    }
		const recoveryTimeline = recoveryTimelineFromEvent(event);
		if (recoveryTimeline) {
			recoveryMilestones.push({ event, label: recoveryTimeline.label, status: recoveryTimeline.status });
			if (RECOVERY_RUN_REOPEN_EVENT_TYPES.has(event.event_type)) {
				continue;
			}
		}
	    const payload = event.payload_json ?? {};
    let nodeId = typeof payload.node_id === "string" ? payload.node_id : "";
    if (!nodeId) {
      const nodeRunID = typeof payload.node_run_id === "string" ? payload.node_run_id : "";
      if (nodeRunID && lookups.nridToNodeId.has(nodeRunID)) {
        nodeId = lookups.nridToNodeId.get(nodeRunID) ?? "";
      }
    }

    if (event.event_type === "run.state_changed") {
      runEvents.push(event);
    } else if (nodeId && isNodeLifecycleEventType(event.event_type)) {
      const existing = nodeEvents.get(nodeId);
      if (existing) {
        existing.push(event);
      } else {
        nodeEvents.set(nodeId, [event]);
      }
		} else if (event.event_type.startsWith("attempt.")) {
			orphanedAttempts.push(event);
		} else {
			if (shouldIncludeRunLevelMilestoneEvent(event)) {
				runEvents.push(event);
			}
		}
	}

  const fanOutGroups = detectFanOutGroups(edges);

  function buildNodeMilestone(nodeId: string): Milestone {
    const nodeEventList = nodeEvents.get(nodeId) ?? [];
    const graphNode = lookups.nodeStateMap.get(nodeId);
    const currentNodeRunID = lookups.currentNodeRunIDMap.get(nodeId);
		const execution = currentNodeRunID ? lookups.executionStateMap.get(currentNodeRunID) : undefined;

    const graphState = graphNode?.state ?? "";
		const visibleLineageState = executionLifecycleStatus(execution)
			?? latestNodeLifecycleState(nodeEventList, currentNodeRunID)
			?? graphNodeLifecycleStatus(graphState)
			?? "neutral";
		const sorted = [...selectMilestoneEventsForVisibleLineage(nodeEventList, currentNodeRunID, visibleLineageState)].sort(compareTimelineEventSequence);
    const latestSucceededEvent = latestNodeLifecycleEvent(sorted, "attempt.succeeded");
    const latestFailedEvent = latestNodeLifecycleEvent(sorted, "attempt.failed");
		const effectiveState = executionLifecycleStatus(execution)
			?? latestNodeLifecycleState(sorted, currentNodeRunID)
			?? graphNodeLifecycleStatus(graphState)
			?? "neutral";
	    const earliest = milestoneStartTimestamp(sorted, execution, effectiveState)
	      ?? sorted[0]?.emitted_at
	      ?? execution?.launch_started_at
	      ?? execution?.started_at
	      ?? execution?.activity_at
	      ?? "";
	    const latest = sorted[sorted.length - 1]?.emitted_at ?? execution?.activity_at ?? earliest;

    let status: Milestone["status"];
    let kind: Milestone["kind"];
    let detail: string | undefined;
    let badge: string | undefined;

	if (effectiveState === "failed") {
	  status = "failed";
	  kind = "node_failed";
	  const failedEvent = latestFailedEvent;
	  const reason = failedEvent ? failureDetailFromEvent(failedEvent, execution) : failureDetailFromExecution(execution);
	  detail = reason;
	} else if (effectiveState === "rework") {
	  status = "rework";
	  kind = "node_failed";
	  detail = reworkRequestLabel(latestFailedEvent?.reason_code);
	} else if (effectiveState === "succeeded") {
	  status = "succeeded";
      kind = "node_completed";
      if (latestSucceededEvent) {
        badge = specialReasonLabel(latestSucceededEvent);
      }
	} else if (effectiveState === "starting") {
		status = "starting";
		kind = "node_running";
	} else if (effectiveState === "running") {
		status = "running";
		kind = "node_running";
    } else if (effectiveState === "ready" || effectiveState === "pending" || effectiveState === "neutral") {
      status = effectiveState;
      kind = "node_queued";
	} else if (effectiveState === "blocked" || effectiveState === "paused") {
		status = "blocked";
		if (effectiveState === "paused") {
			status = "paused";
		}
		kind = "node_blocked";
    } else if (effectiveState === "cancelled" || effectiveState === "skipped") {
      status = effectiveState;
      kind = "node_cancelled";
    } else {
      status = "neutral";
      kind = "node_queued";
    }

		let detailText: string | undefined;
	if (kind === "node_failed") {
	  detailText = status === "rework"
			? detail ?? "Rework requested"
			: detail ? `Failed - ${detail}` : "Failed";
		} else if (kind === "node_completed") {
		  detailText = "Succeeded";
		} else if (kind === "node_running") {
			const lead = status === "starting"
				? "Launching"
				: execution?.status === "verifying"
					? "Verifying settlement"
					: "In progress";
			detailText = lead;
	} else if (kind === "node_queued") {
		  const lead = latestNodeLifecycleEvent(sorted, "attempt.final_rework_queued")
			? "Final Governance Rework Queued"
			: effectiveState === "ready" ? "Queued" : effectiveState === "pending" ? "Pending" : "Awaiting update";
		  detailText = lead;
	} else if (kind === "node_blocked") {
		  detailText = status === "paused"
			? execution?.handoff_state === "workspace_restore_failed"
				? "Step Paused - Trusted Workspace Restoration Failed"
				: execution?.handoff_state === "awaiting_workspace_quiescence"
					? "Retry Paused - Shared Workspace Still in Use"
				: "Retry Paused - Previous Attempt Still Running"
			: "Blocked";
    } else if (kind === "node_cancelled") {
		  const lead = effectiveState === "skipped" ? "Skipped" : "Cancelled";
		  detailText = lead;
    }

    return {
      id: `node-${nodeId}`,
      kind,
      label: nodeLabel(nodeId),
      detail: detailText,
      status,
      timestamp: earliest,
      endTimestamp: earliest !== latest ? latest : undefined,
      events: sorted,
      badge,
    };
  }

  const processedNodes = new Set<string>();
  const milestones: Milestone[] = [];
  const processedFanOutParents = new Set<string>();

  for (const group of fanOutGroups) {
    if (processedFanOutParents.has(group.parentNodeId)) {
      continue;
    }
    processedFanOutParents.add(group.parentNodeId);

    const activeChildren = group.childNodeIds.filter((nodeId) => nodeEvents.has(nodeId));
    if (activeChildren.length < 2) {
      continue;
    }

    const childMilestones = activeChildren.map((nodeId) => buildNodeMilestone(nodeId));
    for (const nodeId of activeChildren) {
      processedNodes.add(nodeId);
    }

	    const statusCounts = countMilestoneStatuses(childMilestones);
	    const succeeded = statusCounts.succeeded;
	    const failed = statusCounts.failed;
	    const starting = statusCounts.starting;
	    const running = statusCounts.running;
	    const active = starting + running;
	    const ready = statusCounts.ready;
    const pending = statusCounts.pending;
    const blocked = statusCounts.blocked;
		const paused = statusCounts.paused;
    const cancelled = statusCounts.cancelled;
    const skipped = statusCounts.skipped;
	    const rework = statusCounts.rework;
	    const autoSucceededAtCap = childMilestones.filter((milestone) => milestone.badge === VALIDATION_CAP_AUTO_SUCCEEDED_BADGE || milestone.badge === REVIEW_CAP_AUTO_SUCCEEDED_BADGE).length;
	    const total = childMilestones.length;
	    const loopCapBadge = fanOutLoopCapBadge(childMilestones);

		const earliest = childMilestones.reduce((value, milestone) => {
			if (value === "" || milestone.timestamp < value) {
				return milestone.timestamp;
			}
			return value;
		}, "");
		const latest = childMilestones.reduce((value, milestone) => {
			const candidate = milestoneDisplayTimestamp(milestone);
			if (value === "" || candidate > value) {
				return candidate;
			}
			return value;
		}, earliest);
		childMilestones.sort(compareMilestoneDisplaySequence);

	    let status: Milestone["status"];
	    let summaryDetail: string;
	    let badge: string | undefined;
	    if (failed > 0) {
	      status = "failed";
	      summaryDetail = `${succeeded} of ${total} succeeded, ${failed} failed${active > 0 ? `, ${active} active` : ""}`;
		} else if (paused > 0 && blocked === 0) {
			status = "paused";
			summaryDetail = `${succeeded} of ${total} succeeded, ${paused} ${paused === 1 ? "retry" : "retries"} paused`;
	    } else if (blocked > 0) {
	      status = "blocked";
	      summaryDetail = `${succeeded} of ${total} succeeded, ${blocked} blocked${paused > 0 ? `, ${paused} ${paused === 1 ? "retry" : "retries"} paused` : ""}`;
	    } else if (rework > 0) {
	      status = "rework";
	      summaryDetail = `${total - rework} of ${total} completed, ${rework} requested rework`;
	    } else if (active > 0) {
	      status = running > 0 ? "running" : "starting";
	      if (running > 0 && starting > 0) {
	        summaryDetail = `${succeeded} of ${total} succeeded, ${running} running, ${starting} launching`;
	      } else if (running > 0) {
	        summaryDetail = `${succeeded} of ${total} succeeded, ${running} running`;
	      } else {
	        summaryDetail = `${succeeded} of ${total} succeeded, ${starting} launching`;
	      }
	    } else if (ready > 0) {
	      status = "ready";
	      summaryDetail = `${succeeded} of ${total} succeeded, ${ready} queued`;
    } else if (pending > 0) {
      status = "pending";
	      summaryDetail = `${succeeded} of ${total} succeeded, ${pending} pending`;
    } else if (cancelled > 0) {
      status = "cancelled";
	      summaryDetail = `${succeeded} of ${total} succeeded, ${cancelled} cancelled`;
    } else if (skipped > 0) {
      status = "skipped";
	      summaryDetail = `${succeeded} of ${total} succeeded, ${skipped} skipped`;
    } else {
      status = "succeeded";
	      summaryDetail = autoSucceededAtCap > 0
	        ? `All ${total} succeeded, ${autoSucceededAtCap} auto-succeeded at cap`
	        : `All ${total} succeeded`;
	      if (autoSucceededAtCap > 0) {
	        badge = loopCapBadge;
	      }
    }

    const allEvents = childMilestones.flatMap((milestone) => milestone.events);
		allEvents.sort(compareTimelineEventSequence);

    milestones.push({
      id: `fanout-${group.parentNodeId}`,
      kind: "fan_out",
      label: findCommonPrefix(activeChildren) ? nodeLabel(findCommonPrefix(activeChildren)) : "Parallel Tasks",
      detail: summaryDetail,
      status,
      timestamp: earliest,
      endTimestamp: earliest !== latest ? latest : undefined,
      events: allEvents,
      children: childMilestones,
	      badge,
    });
  }

  for (const [nodeId] of nodeEvents) {
    if (processedNodes.has(nodeId)) {
      continue;
    }
    milestones.push(buildNodeMilestone(nodeId));
    processedNodes.add(nodeId);
  }

  for (const event of runEvents) {
    const { label, detail, status, badge } = buildRunEventMilestone(event, lookups);

    milestones.push({
      id: `run-${event.event_id}`,
      kind: "run_event",
      label,
      detail,
      status,
      timestamp: event.emitted_at,
      events: [event],
      badge,
    });
  }

	for (const recovery of deduplicateRecoveryMilestones(recoveryMilestones, lookups)) {
		milestones.push({
			id: `recovery-${recovery.event.event_id}`,
			kind: "run_event",
			label: recovery.label,
			detail: nodeLabelForEvent(recovery.event, lookups),
			status: recovery.status,
			timestamp: recovery.event.emitted_at,
			events: recovery.events,
		});
	}

  if (orphanedAttempts.length > 0) {
    const specialOrphans: TimelineEvent[] = [];
    const plainOrphans: TimelineEvent[] = [];

    for (const event of orphanedAttempts) {
      if (specialReasonLabel(event)) {
        specialOrphans.push(event);
      } else {
        plainOrphans.push(event);
      }
    }

    for (const event of specialOrphans) {
      const badge = specialReasonLabel(event);
	      const isFailed = event.event_type === "attempt.failed";
	      const rework = isReworkTimelineEvent(event);
	      milestones.push({
	        id: `orphan-${event.event_id}`,
	        kind: isFailed ? "node_failed" : "node_completed",
	        label: badge || event.event_type,
	        status: rework ? "rework" : isFailed ? "failed" : "succeeded",
	        timestamp: event.emitted_at,
	        events: [event],
	      });
    }

    const hasGraphData = nodes.length > 0;
    if (plainOrphans.length > 0 && !hasGraphData) {
			const sorted = [...plainOrphans].sort(compareTimelineEventSequence);
      const succeeded = sorted.filter((event) => event.event_type === "attempt.succeeded").length;
	      const failed = sorted.filter((event) => event.event_type === "attempt.failed").length;
	      const rework = sorted.filter((event) => isReworkTimelineEvent(event)).length;
	      const hardFailures = failed - rework;
	      const earliest = sorted[0].emitted_at;
	      const latest = sorted[sorted.length - 1].emitted_at;

	      let label: string;
	      let status: Milestone["status"];
	      if (hardFailures > 0 && succeeded > 0) {
	        label = `${succeeded + failed} Background Attempts - ${hardFailures} Failed`;
	        status = "failed";
	      } else if (hardFailures > 0) {
	        label = `${hardFailures} Background Attempt${hardFailures > 1 ? "s" : ""} Failed`;
	        status = "failed";
	      } else if (rework > 0 && succeeded > 0) {
	        label = `${succeeded + rework} Background Attempts - ${rework} Requested Rework`;
	        status = "rework";
	      } else if (rework > 0) {
	        label = `${rework} Background Attempt${rework > 1 ? "s" : ""} Requested Rework`;
	        status = "rework";
	      } else {
	        label = `${succeeded} Background Completion${succeeded > 1 ? "s" : ""}`;
	        status = "succeeded";
      }

      milestones.push({
        id: "orphaned-attempts",
        kind: "run_event",
        label,
        status,
        timestamp: earliest,
        endTimestamp: earliest !== latest ? latest : undefined,
        events: sorted,
      });
    }
  }

	milestones.sort(compareMilestoneDisplaySequence);

  const merged: Milestone[] = [];
  for (let index = 0; index < milestones.length; index += 1) {
    const current = milestones[index];
    const next = milestones[index + 1];
    if (
      next &&
      current.kind === "run_event" &&
      next.kind === "run_event" &&
      current.label.startsWith("Plan Entered") &&
      next.label === "Run Started" &&
      Math.abs(new Date(next.timestamp).getTime() - new Date(current.timestamp).getTime()) < 2000
    ) {
      merged.push({
        ...next,
        id: `merged-${current.id}-${next.id}`,
        label: "Run Started",
        detail: current.label,
        events: [...current.events, ...next.events],
      });
      index += 1;
      continue;
    }

    merged.push(current);
  }

  return merged;
}

function latestNodeLifecycleEvent(events: TimelineEvent[], eventType: string): TimelineEvent | undefined {
	for (let index = events.length - 1; index >= 0; index -= 1) {
		if (events[index]?.event_type === eventType) {
			return events[index];
		}
	}
	return undefined;
}

function selectMilestoneEventsForVisibleLineage(events: TimelineEvent[], currentNodeRunID?: string, visibleLineageState?: string): TimelineEvent[] {
	const trimmedNodeRunID = typeof currentNodeRunID === "string" ? currentNodeRunID.trim() : "";
	if (trimmedNodeRunID === "") {
		return events;
	}
	const lineageEvents = events.filter((event) => eventNodeRunID(event) === trimmedNodeRunID);
	if ((visibleLineageState ?? "").trim() !== "blocked" && (visibleLineageState ?? "").trim() !== "paused") {
		return lineageEvents;
	}
	const blockedRunEventSeq = latestBlockedRunEventSeq(lineageEvents);
	if (blockedRunEventSeq === undefined) {
		return lineageEvents;
	}
	return events.filter((event) => {
		if (eventNodeRunID(event) === trimmedNodeRunID) {
			return true;
		}
		return RECOVERY_QUEUE_EVENT_TYPES.has(event.event_type) && event.run_event_seq >= blockedRunEventSeq;
	});
}

function latestBlockedRunEventSeq(events: TimelineEvent[]): number | undefined {
	for (let index = events.length - 1; index >= 0; index -= 1) {
		const event = events[index];
		if (!event) {
			continue;
		}
		if (event.event_type === "node.state_changed" && normalizedRunStatusTone(event.payload_json?.state) === "blocked") {
			return event.run_event_seq;
		}
	}
	return undefined;
}

function eventNodeRunID(event: TimelineEvent): string {
  const nodeRunID = event.payload_json?.node_run_id;
  return typeof nodeRunID === "string" ? nodeRunID.trim() : "";
}

// Timeline events remain generic JSON at the API boundary. Decode only the
// durable recovery payload here, then delegate all operator wording to the mapper.
function recoveryTimelineFromEvent(event: TimelineEvent) {
	const payload = event.payload_json;
	if (!payload || typeof payload !== "object") {
		return undefined;
	}
	const fields = payload as Record<string, unknown>;
	const phase = fields.recovery_phase;
	const trigger = fields.recovery_trigger;
	const reasonCode = fields.error_code ?? event.reason_code;
	const scheduledAt = fields.scheduled_at;
	const retryIndex = fields.retry_index;
	const retryMax = fields.retry_max;
	if (!isRecoveryPhase(phase) || !isRecoveryTrigger(trigger) || typeof reasonCode !== "string" || typeof scheduledAt !== "string" || typeof retryIndex !== "number" || typeof retryMax !== "number") {
		return undefined;
	}
	return presentRecoveryTimeline({ phase, trigger, reason_code: reasonCode, scheduled_at: scheduledAt, retry_index: retryIndex, retry_max: retryMax } satisfies RecoveryLike);
}

function nodeLabelForEvent(event: TimelineEvent, lookups: TimelineLookups): string | undefined {
	const nodeID = recoveryLogicalNodeID(event, lookups);
	return nodeID ? nodeLabel(nodeID) : undefined;
}

function deduplicateRecoveryMilestones(
	recoveryMilestones: Array<{ event: TimelineEvent; label: string; status: Milestone["status"] }>,
	lookups: TimelineLookups,
): Array<{ event: TimelineEvent; label: string; status: Milestone["status"]; events: TimelineEvent[] }> {
	const deduplicated = new Map<string, { event: TimelineEvent; label: string; status: Milestone["status"]; events: TimelineEvent[] }>();
	for (const recovery of recoveryMilestones) {
		const identity = recoveryMilestoneIdentity(recovery.event, lookups) ?? `event:${recovery.event.event_id}`;
		const existing = deduplicated.get(identity);
		if (!existing || shouldPreferRecoveryEvent(recovery.event, existing.event)) {
			deduplicated.set(identity, {
				...recovery,
				events: existing ? [...existing.events, recovery.event].sort(compareTimelineEventSequence) : [recovery.event],
			});
			continue;
		}
		existing.events.push(recovery.event);
		existing.events.sort(compareTimelineEventSequence);
	}
	return [...deduplicated.values()];
}

function recoveryMilestoneIdentity(event: TimelineEvent, lookups: TimelineLookups): string | undefined {
	const payload = event.payload_json;
	if (!payload || typeof payload !== "object") {
		return undefined;
	}
	const fields = payload as Record<string, unknown>;
	const trigger = fields.recovery_trigger;
	const phase = fields.recovery_phase;
	const code = fields.error_code ?? event.reason_code;
	const retryIndex = fields.retry_index;
	const scheduledAt = fields.scheduled_at;
	const logicalNodeID = recoveryLogicalNodeID(event, lookups);
	if (
		typeof trigger !== "string"
		|| typeof phase !== "string"
		|| typeof code !== "string"
		|| typeof retryIndex !== "number"
		|| typeof scheduledAt !== "string"
		|| !logicalNodeID
	) {
		return undefined;
	}
	return JSON.stringify([trigger, phase, code, logicalNodeID, retryIndex, scheduledAt]);
}

function recoveryLogicalNodeID(event: TimelineEvent, lookups: TimelineLookups): string | undefined {
	const nodeID = event.payload_json?.node_id;
	if (typeof nodeID === "string" && nodeID.trim() !== "") {
		return nodeID.trim();
	}
	const nodeRunID = eventNodeRunID(event);
	return nodeRunID ? lookups.nridToNodeId.get(nodeRunID) : undefined;
}

function shouldPreferRecoveryEvent(candidate: TimelineEvent, current: TimelineEvent): boolean {
	return RECOVERY_QUEUE_EVENT_TYPES.has(candidate.event_type) && !RECOVERY_QUEUE_EVENT_TYPES.has(current.event_type);
}

function isRecoveryPhase(value: unknown): value is RecoveryLike["phase"] {
	return value === "scheduled" || value === "paused" || value === "running" || value === "recovered" || value === "exhausted";
}

function isRecoveryTrigger(value: unknown): value is RecoveryLike["trigger"] {
	return value === "automatic_retry" || value === "operator_retry" || value === "operator_rerun";
}

function executionForEvent(event: TimelineEvent, lookups: TimelineLookups): RunExecution | undefined {
	const nodeRunID = eventNodeRunID(event);
	return nodeRunID ? lookups.executionStateMap.get(nodeRunID) : undefined;
}

function milestoneStartTimestamp(
	events: TimelineEvent[],
	execution: RunExecution | undefined,
	status: RunStatusTone,
): string | undefined {
	switch (status) {
		case "starting":
			return firstNonEmptyTimestamp(
				firstNodeStateTimestamp(events, "starting"),
				execution?.launch_started_at,
				execution?.started_at,
				execution?.activity_at,
			);
		case "running":
			return firstNonEmptyTimestamp(
				firstAttemptEventTimestamp(events, "attempt.started"),
				firstAttemptProgressReasonTimestamp(events, "provider_invoked"),
				execution?.started_at,
				execution?.progress_at,
				execution?.activity_at,
			);
		default:
			return undefined;
	}
}

function firstAttemptProgressReasonTimestamp(events: TimelineEvent[], reasonCode: string): string {
	for (const event of events) {
		if (event.event_type !== "attempt.progress") {
			continue;
		}
		if (event.reason_code === reasonCode) {
			return event.emitted_at;
		}
	}
	return "";
}

function firstNodeStateTimestamp(events: TimelineEvent[], state: string): string {
	for (const event of events) {
		if (event.event_type !== "node.state_changed") {
			continue;
		}
		const eventState = event.payload_json?.state;
		if (typeof eventState === "string" && eventState.trim() === state) {
			return event.emitted_at;
		}
	}
	return "";
}

function firstAttemptEventTimestamp(events: TimelineEvent[], eventType: string): string {
	for (const event of events) {
		if (event.event_type === eventType) {
			return event.emitted_at;
		}
	}
	return "";
}

function firstNonEmptyTimestamp(...values: Array<string | undefined>): string | undefined {
	for (const value of values) {
		if (typeof value === "string" && value.trim() !== "") {
			return value;
		}
	}
	return undefined;
}

function latestNodeLifecycleState(events: TimelineEvent[], currentNodeRunID?: string): RunStatusTone | undefined {
	const trimmedNodeRunID = typeof currentNodeRunID === "string" ? currentNodeRunID.trim() : "";
	for (let index = events.length - 1; index >= 0; index -= 1) {
		const event = events[index];
		if (!event) {
			continue;
		}
		const eventNodeRun = eventNodeRunID(event);
		if (trimmedNodeRunID && eventNodeRun && eventNodeRun !== trimmedNodeRunID) {
			continue;
		}
			switch (event.event_type) {
				case "attempt.retry_queued":
				case "attempt.rerun_queued":
					return "ready";
				case "attempt.started":
					return "running";
				case "attempt.succeeded":
					return "succeeded";
				case "attempt.failed":
					return isReworkTimelineEvent(event) ? "rework" : "failed";
				case "attempt.cancelled":
					return "cancelled";
				case "node.state_changed": {
					const state = normalizedRunStatusTone(event.payload_json?.state);
					if (state) {
						return state;
					}
				break;
			}
		}
	}
	return undefined;
}

function graphNodeLifecycleStatus(state: string): RunStatusTone | undefined {
	return normalizedRunStatusTone(state);
}

function executionLifecycleStatus(execution?: RunExecution): RunStatusTone | undefined {
	if (!execution) {
		return undefined;
	}
	if (isExecutionCompletionWorkPending(execution)) {
		return "running";
	}
		switch (executionDisplayStatus(execution)) {
			case "queued":
				return "pending";
			case "starting":
				return "starting";
			case "blocked":
				return "blocked";
			case "paused":
				return "paused";
			case "running":
			case "verifying":
				return "running";
		case "succeeded":
			return "succeeded";
		case "rework":
			return "rework";
		case "failed":
			return "failed";
		case "cancelled":
			return "cancelled";
		default:
			return undefined;
	}
}

function isExecutionCompletionWorkPending(execution?: RunExecution): boolean {
	if (!execution) {
		return false;
	}
	return execution.status === "verifying";
}

function countMilestoneStatuses(milestones: Milestone[]): Record<RunStatusTone, number> {
	const counts: Record<RunStatusTone, number> = {
		succeeded: 0,
		failed: 0,
		starting: 0,
		running: 0,
		ready: 0,
		pending: 0,
		blocked: 0,
		paused: 0,
		cancelled: 0,
		skipped: 0,
		rework: 0,
		neutral: 0,
	};
	for (const milestone of milestones) {
		counts[milestone.status] += 1;
	}
	return counts;
}

function buildSuppressedResponseMilestoneEventIDs(events: TimelineEvent[]): Set<string> {
	const terminalAttemptKeys = new Set<string>();
	for (const event of events) {
		if (!isTerminalAttemptEventType(event.event_type)) {
			continue;
		}
		for (const key of milestoneSuppressionKeys(event)) {
			terminalAttemptKeys.add(key);
		}
	}

	const suppressed = new Set<string>();
	for (const event of events) {
		if (!isResponseReceivedMailboxEvent(event)) {
			continue;
		}
		if (milestoneSuppressionKeys(event).some((key) => terminalAttemptKeys.has(key))) {
			suppressed.add(event.event_id);
		}
	}
	return suppressed;
}

function isTerminalAttemptEventType(eventType: string): boolean {
	return eventType === "attempt.succeeded" || eventType === "attempt.failed" || eventType === "attempt.cancelled";
}

function isResponseReceivedMailboxEvent(event: TimelineEvent): boolean {
	if (event.event_type !== "mailbox.message_recorded") {
		return false;
	}
	return mailboxCommunicationType(event) === "response_received";
}

function milestoneSuppressionKeys(event: TimelineEvent): string[] {
	const payload = event.payload_json ?? {};
	const reasonCode = typeof event.reason_code === "string" ? event.reason_code.trim() : "";
	if (!reasonCode) {
		return [];
	}
	const nodeRunID = typeof payload.node_run_id === "string" ? payload.node_run_id.trim() : "";
	if (nodeRunID) {
		return [`node_run:${nodeRunID}|reason:${reasonCode}`];
	}
	return [];
}

function mailboxCommunicationType(event: TimelineEvent): string {
	const communicationType = event.payload_json?.communication_type;
	return typeof communicationType === "string" ? communicationType.trim().toLowerCase() : "";
}

function shouldIncludeRunLevelMilestoneEvent(event: TimelineEvent): boolean {
	if (event.event_type !== "mailbox.message_recorded") {
		return true;
	}
	const communicationType = mailboxCommunicationType(event);
	return communicationType === "approval_request" || communicationType === "approval_response";
}

function failureDetailFromEvent(event: TimelineEvent, execution?: RunExecution): string | undefined {
	const reasonCode = typeof event.reason_code === "string" ? event.reason_code.trim() : "";
	const error = typeof event.payload_json?.error === "string" ? event.payload_json.error.trim() : "";
	const operatorText = operatorFacingExecutionFailureText(
		reasonCode || execution?.failure_code,
		error,
		execution?.failure_text,
	);
	if (operatorText) {
		return operatorText;
	}
	if (error) {
		return error;
	}
	if (reasonCode && !isNoiseReasonCode(reasonCode)) {
		return reasonCode.replace(/_/g, " ");
	}
	return undefined;
}

function failureDetailFromExecution(execution?: RunExecution): string | undefined {
	if (!execution) {
		return undefined;
	}
	return operatorFacingExecutionFailureText(
		execution.failure_code,
		execution.failure_text,
	) ?? undefined;
}

function buildRunEventMilestone(
  event: TimelineEvent,
  lookups: TimelineLookups,
): Pick<Milestone, "label" | "detail" | "status" | "badge"> {
  if (event.event_type === "run.state_changed") {
    const payload = event.payload_json ?? {};
    const statusValue = typeof payload.status === "string" ? payload.status : "";
    const planState = typeof payload.plan_state === "string" ? payload.plan_state : "";

    if (statusValue === "running") {
      return { label: "Run Started", status: "neutral" };
    }
    if (statusValue === "failed") {
      const reason = event.reason_code ? event.reason_code.replace(/_/g, " ") : undefined;
      const badge = specialReasonLabel(event) || undefined;
      return {
        label: "Run Failed",
        detail: reason && !badge ? reason.charAt(0).toUpperCase() + reason.slice(1) : undefined,
        status: "failed",
        badge,
      };
    }
    if (statusValue === "succeeded" || statusValue === "completed") {
      return { label: "Run Succeeded", status: "succeeded" };
    }
    if (statusValue === "cancelled") {
      return {
        label: "Run Cancelled",
        status: "cancelled",
        badge: specialReasonLabel(event) || undefined,
      };
    }
    if (planState) {
      return {
        label: `Plan Entered ${titleCaseEventFragment(planState.replace(/_/g, " "))}`,
        status: "neutral",
      };
    }
    return {
      label: "Run State Updated",
      status: "neutral",
      badge: specialReasonLabel(event) || undefined,
    };
  }

  const { label, detail } = buildRawEventLabel(event, lookups);
  const badge = specialReasonLabel(event) || undefined;

  if (event.event_type === "approval.recorded") {
    return { label, detail, status: "succeeded", badge };
  }
  if (event.event_type === "escalation.raised") {
    return { label, detail, status: "failed", badge };
  }
  return { label, detail, status: "neutral", badge };
}

function isNodeLifecycleEventType(eventType: string): boolean {
  return NODE_LIFECYCLE_EVENT_TYPES.has(eventType) || eventType.startsWith("attempt.");
}

function compareTimelineEventSequence(left: TimelineEvent, right: TimelineEvent): number {
	if (left.run_event_seq !== right.run_event_seq) {
		return left.run_event_seq - right.run_event_seq;
	}
	if (left.attempt_seq !== right.attempt_seq) {
		return left.attempt_seq - right.attempt_seq;
	}
	if (left.emitted_at > right.emitted_at) return 1;
	if (left.emitted_at < right.emitted_at) return -1;
	if (left.event_id > right.event_id) return 1;
	if (left.event_id < right.event_id) return -1;
	return 0;
}

function milestoneStartSequence(milestone: Milestone): number {
	return milestone.events[0]?.run_event_seq ?? Number.MAX_SAFE_INTEGER;
}

function milestoneDisplayTimestamp(milestone: Milestone): string {
	return milestone.endTimestamp ?? milestone.timestamp;
}

function compareMilestoneDisplaySequence(left: Milestone, right: Milestone): number {
	const leftEvent = left.events[left.events.length - 1];
	const rightEvent = right.events[right.events.length - 1];
	if (leftEvent && rightEvent) {
		return compareTimelineEventSequence(leftEvent, rightEvent);
	}
	if (leftEvent) {
		return 1;
	}
	if (rightEvent) {
		return -1;
	}
	const leftTimestamp = milestoneDisplayTimestamp(left);
	const rightTimestamp = milestoneDisplayTimestamp(right);
	if (leftTimestamp !== rightTimestamp) {
		return leftTimestamp > rightTimestamp ? 1 : -1;
	}
	return milestoneStartSequence(left) - milestoneStartSequence(right);
}

export function formatDuration(startIso: string, endIso: string): string | null {
  return formatElapsedDuration(startIso, endIso);
}

function detectFanOutGroups(edges: RunGraphEdge[]): FanOutGroup[] {
  const outgoing = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (!outgoing.has(edge.from)) {
      outgoing.set(edge.from, new Set());
    }
    outgoing.get(edge.from)?.add(edge.to);
  }

  const groups: FanOutGroup[] = [];
  for (const [parentNodeId, children] of outgoing) {
    if (children.size >= 2) {
      groups.push({ parentNodeId, childNodeIds: Array.from(children) });
    }
  }
  return groups;
}

function findCommonPrefix(ids: string[]): string {
  if (ids.length === 0) return "";
  const parts = ids.map((id) => id.split("_"));
  const minLength = Math.min(...parts.map((part) => part.length));
  const common: string[] = [];
  for (let index = 0; index < minLength; index += 1) {
    const segment = parts[0][index];
    if (parts.every((part) => part[index] === segment)) {
      common.push(segment);
    } else {
      break;
    }
  }
  if (common.length > 0 && common.join("_") !== ids[0]) {
    return common.join("_");
  }
  return "";
}
