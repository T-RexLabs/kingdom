import type { RunAttemptRetryRelationship, RunExecution, RunExecutionFailure, RunNodeAttemptHistoryItem, RunRecoveryState } from "@/api/hooks-types";
import { executionDisplayStatus, isExecutionReworkOutcome } from "./run-status-semantics";
import type { RunStatusTone } from "./run-detail-tones";

export type RecoveryIconKey = "activity" | "play" | "check" | "x" | "rotate" | "alert";

export type RecoveryTimelineInput = {
	label: string;
	status: RunStatusTone;
	iconKey: RecoveryIconKey;
};

export type RunNodeExecutionPresentation = {
	status: string;
	tone: RunStatusTone;
	iconKey: RecoveryIconKey;
	heading: string;
	description: string;
	announcement?: { role: "status" | "alert"; ariaLive: "polite" | "assertive" };
	recoveryKind?: "Automatic Recovery" | "Manual Retry" | "Manual Rerun";
	retryDetail?: string;
	failureDetail?: string;
	recoveryReason?: string;
	scheduledAt?: string;
	pauseReason?: string;
	requiredAction?: string;
	currentAttemptLabel: "Current Attempt";
	historicalAttemptLabel: "Historical Attempt";
	timeline?: RecoveryTimelineInput;
};

export type RecoveryLike = Pick<RunRecoveryState, "phase" | "trigger" | "reason_code" | "retry_index" | "retry_max" | "scheduled_at">;

// This is the sole Phase 9 translation of canonical failure and recovery state
// into operator copy. Presentation consumers must not infer recovery from attempt order.
export function presentRunNodeExecution(
	node: { status: string; failure?: RunExecutionFailure; recovery?: RunRecoveryState },
	execution?: Pick<RunExecution, "status" | "failure_code" | "failure_text" | "handoff_state"> & { output_verdict?: string } | null,
): RunNodeExecutionPresentation {
	const recovery = node.recovery;
	const failure = node.failure ?? failureFromExecution(execution);
	const status = executionDisplayStatus(execution ?? { status: node.status });
	const labels = attemptLabels();

	if (recovery) {
		return { ...labels, status, ...presentRecovery(recovery, execution?.handoff_state) };
	}
	if (status === "rework" || isExecutionReworkOutcome(execution)) {
		return { ...labels, status: "rework", tone: "rework", iconKey: "rotate", heading: "Rework Requested", description: "Changes were requested for this step." };
	}
	if (failure) {
		const nonRetryable = !failure.retryable;
		return {
			...labels,
			status,
			tone: "failed",
			iconKey: "x",
			heading: "Step Failed",
			description: failure.operator_text,
			failureDetail: `${failure.classification.replaceAll("_", " ")} (${failure.code})`,
			announcement: nonRetryable ? { role: "alert", ariaLive: "assertive" } : undefined,
			requiredAction: nonRetryable ? "Required Action: Resolve the issue, then retry this step." : undefined,
		};
	}
	return { ...labels, status, ...presentStatus(status, execution?.handoff_state) };
}

export function presentRecoveryTimeline(recovery: RecoveryLike): RecoveryTimelineInput {
	return presentRecovery(recovery).timeline ?? { label: "Recovery Updated", status: "neutral", iconKey: "activity" };
}

export function presentAttemptHistoryItem(attempt: RunNodeAttemptHistoryItem, currentAttemptID?: string): {
	attemptLabel: string;
	outcome: string;
	error?: string;
	retryRelationships: string[];
} {
	const presentation = presentRunNodeExecution({ status: attempt.status, failure: attempt.failure }, attempt);
	return {
		attemptLabel: `${attempt.attempt_id === currentAttemptID ? presentation.currentAttemptLabel : presentation.historicalAttemptLabel} ${attempt.attempt}`,
		outcome: presentation.heading,
		error: attempt.failure?.operator_text ?? attempt.failure_text,
		retryRelationships: attempt.retry_relationships.map(presentRetryRelationship),
	};
}

function presentRecovery(
	recovery: RecoveryLike,
	handoffState?: RunExecution["handoff_state"],
): Omit<RunNodeExecutionPresentation, "status" | "currentAttemptLabel" | "historicalAttemptLabel"> {
	const automatic = recovery.trigger === "automatic_retry";
	const recoveryKind = automatic ? "Automatic Recovery" : recovery.trigger === "operator_rerun" ? "Manual Rerun" : "Manual Retry";
	const retryDetail = recovery.retry_max > 0 ? `${automatic ? "Automatic retry" : recoveryKind} ${recovery.retry_index} of ${recovery.retry_max}` : undefined;
	const reason = recoveryReasonLabel(recovery.reason_code);
	const attempt = recovery.retry_index + 1;
	const base = { recoveryKind, retryDetail, scheduledAt: recovery.scheduled_at, recoveryReason: reason } as const;

	if (isUsageLimit(recovery.reason_code)) {
		return {
			...base,
			tone: "failed", iconKey: "alert", heading: "Usage Limit Reached",
			description: "Restore provider capacity or select another model, then retry this step.",
			announcement: { role: "alert", ariaLive: "assertive" },
			requiredAction: "Required Action: Restore provider capacity or select another model, then retry this step.",
			timeline: { label: "Usage Limit Reached", status: "failed", iconKey: "alert" },
		};
	}
	if (recovery.reason_code === "budget_exceeded") {
		return {
			...base,
			tone: "failed", iconKey: "alert", heading: "Budget Exceeded",
			description: "This run reached its configured Kingdom budget.",
			announcement: { role: "alert", ariaLive: "assertive" },
			requiredAction: "Required Action: Increase the Kingdom budget before retrying this step.",
			timeline: { label: "Budget Exceeded", status: "failed", iconKey: "alert" },
		};
	}
	if (recovery.phase === "exhausted") {
		return {
			...base,
			tone: "failed", iconKey: "alert", heading: "Retries Exhausted",
			description: `Kingdom could not complete this step after ${recovery.retry_index} automatic retries.`,
			announcement: { role: "alert", ariaLive: "assertive" },
			requiredAction: "Required Action: Resolve the issue, then retry this step.",
			timeline: { label: `Retries Exhausted - ${reason}`, status: "failed", iconKey: "alert" },
		};
	}
	if (recovery.phase === "paused") {
		const unsafe = handoffState === "workspace_restore_failed" || recovery.reason_code === "agent_unresponsive";
		return {
			...base,
			tone: "paused", iconKey: "alert", heading: unsafe ? "Step Paused" : "Retry Paused",
			description: unsafe
				? "Kingdom could not confirm that the previous agent stopped. Resolve the process before retrying."
				: "Kingdom is waiting for the previous attempt to stop before starting the retry.",
			announcement: { role: "status", ariaLive: "polite" },
			pauseReason: unsafe ? "Previous agent process requires resolution." : "Previous attempt is still running.",
			requiredAction: unsafe ? "Required Action: Resolve the previous agent process before retrying." : undefined,
			timeline: { label: unsafe ? "Step Paused - Previous Agent Still Running" : "Retry Paused - Previous Attempt Still Running", status: "paused", iconKey: "alert" },
		};
	}
	if (recovery.phase === "running") {
		return {
			...base,
			tone: "running", iconKey: "rotate", heading: "Retry Running",
			description: automatic ? "Kingdom is trying this step again now." : `Kingdom is running the ${recoveryKind.toLowerCase()} now.`,
			announcement: { role: "status", ariaLive: "polite" },
			timeline: { label: automatic ? `Retry Started - Attempt ${attempt}` : `${recoveryKind} Started - Attempt ${attempt}`, status: "running", iconKey: "rotate" },
		};
	}
	if (recovery.phase === "recovered") {
		return {
			...base,
			tone: "succeeded", iconKey: "check", heading: "Retry Recovered",
			description: "The retry succeeded, and this step resumed.",
			announcement: { role: "status", ariaLive: "polite" },
			timeline: { label: automatic ? `Retry Recovered - Attempt ${attempt}` : `${recoveryKind} Recovered - Attempt ${attempt}`, status: "succeeded", iconKey: "check" },
		};
	}
	const rateLimited = recovery.reason_code === "llm_rate_limited";
	return {
		...base,
		tone: "ready", iconKey: "rotate", heading: automatic ? "Retry Scheduled" : recoveryKind,
		description: rateLimited
			? `The provider rate limit was reached. Kingdom will retry this step at ${recovery.scheduled_at}.`
			: recovery.reason_code === "timeout"
				? `This step timed out before work began. Kingdom will retry it at ${recovery.scheduled_at}.`
				: `Kingdom will retry this step at ${recovery.scheduled_at}.`,
		announcement: { role: "status", ariaLive: "polite" },
		timeline: { label: automatic ? `Retry Scheduled - ${reason}` : `${recoveryKind} Scheduled - Attempt ${attempt}`, status: "ready", iconKey: "rotate" },
	};
}

function presentStatus(status: string, handoffState?: RunExecution["handoff_state"]): Omit<RunNodeExecutionPresentation, "status" | "currentAttemptLabel" | "historicalAttemptLabel"> {
	if (status === "paused") {
		return {
			tone: "paused", iconKey: "alert", heading: "Step Paused",
			description: handoffState === "workspace_restore_failed"
				? "Kingdom stopped the previous attempt but could not restore the trusted workspace."
				: "Kingdom is waiting for the previous attempt to stop before starting the retry.",
			announcement: { role: "status", ariaLive: "polite" },
		};
	}
	if (status === "verifying") {
		return {
			tone: "running",
			iconKey: "activity",
			heading: "Verifying",
			description: "Kingdom is still confirming the final result for this step.",
			announcement: { role: "status", ariaLive: "polite" },
		};
	}
	if (status === "running" || status === "starting") {
		return { tone: "running", iconKey: "activity", heading: "In Progress", description: "This step is still running." };
	}
	if (status === "queued" || status === "pending" || status === "ready") {
		return { tone: "ready", iconKey: "play", heading: "Queued", description: "This step is queued to start." };
	}
	return { tone: status === "succeeded" ? "succeeded" : status === "cancelled" ? "cancelled" : "neutral", iconKey: status === "succeeded" ? "check" : "activity", heading: status === "succeeded" ? "Succeeded" : status === "cancelled" ? "Cancelled" : "Waiting", description: status === "succeeded" ? "This step completed." : status === "cancelled" ? "This step stopped before completion." : "This step is waiting for workflow progress." };
}

function failureFromExecution(execution?: Pick<RunExecution, "failure_code" | "failure_text"> | null): RunExecutionFailure | undefined {
	if (!execution?.failure_code || !execution.failure_text) return undefined;
	return { code: execution.failure_code, classification: "unknown", category: "runtime", retryable: false, operator_text: execution.failure_text };
}

function recoveryReasonLabel(reasonCode: string): string {
	if (reasonCode === "llm_rate_limited") return "Rate Limit";
	if (reasonCode === "timeout") return "Timeout";
	return reasonCode.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function isUsageLimit(reasonCode: string): boolean {
	return reasonCode === "llm_usage_limit_reached";
}

function attemptLabels(): Pick<RunNodeExecutionPresentation, "currentAttemptLabel" | "historicalAttemptLabel"> {
	return { currentAttemptLabel: "Current Attempt", historicalAttemptLabel: "Historical Attempt" };
}

function presentRetryRelationship(relationship: RunAttemptRetryRelationship): string {
	const direction = relationship.direction === "source" ? "Recovery from this attempt" : "Recovery to this attempt";
	const trigger = relationship.trigger === "automatic_retry" ? "Automatic retry" : relationship.trigger === "operator_rerun" ? "Manual Rerun" : "Manual Retry";
	return `${direction}: ${trigger}${relationship.retry_max > 0 ? ` ${relationship.retry_index}/${relationship.retry_max}` : ""}`;
}
