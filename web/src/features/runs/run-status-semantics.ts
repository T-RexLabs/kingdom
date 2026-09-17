import type { TimelineEvent } from "@/api/hooks";
import type { RunStatusTone } from "./run-detail-tones";

export type AttemptEventIconKey = "activity" | "play" | "check" | "x" | "rotate" | "alert";

const REWORK_REASON_CODES = new Set(["execution_review_rework", "validation_gate_rework"]);

const CAP_AUTO_SUCCEEDED_LABELS = new Set([
	"Auto-Succeeded at Cap",
	"Review Auto-Succeeded at Cap",
	"Validation Auto-Succeeded at Cap",
	"Cap Auto-Succeeded",
	"Review Cap Auto-Succeeded",
	"Validation Cap Auto-Succeeded",
]);

export function normalizedRunStatusTone(state: unknown): RunStatusTone | undefined {
	switch (normalizedToken(state)) {
		case "starting":
		case "running":
		case "failed":
		case "succeeded":
		case "ready":
		case "pending":
		case "blocked":
		case "cancelled":
		case "skipped":
		case "rework":
			return normalizedToken(state) as RunStatusTone;
		case "paused":
			return "paused";
		default:
			return undefined;
	}
}

export function isReworkReasonCode(reasonCode?: string | null): boolean {
	return REWORK_REASON_CODES.has(normalizedToken(reasonCode));
}

export function isExecutionReworkOutcome(outcome?: { failure_code?: string | null; output_verdict?: string | null } | null): boolean {
	if (!outcome) {
		return false;
	}
	return normalizedToken(outcome.output_verdict) === "rework" || isReworkReasonCode(outcome.failure_code);
}

export function executionDisplayStatus(outcome?: { status?: string | null; failure_code?: string | null; output_verdict?: string | null } | null): string {
	const status = typeof outcome?.status === "string" ? outcome.status.trim() : "";
	if (status.toLowerCase() === "failed" && isExecutionReworkOutcome(outcome)) {
		return "rework";
	}
	return status || "pending";
}

export function isQueuedExecutionStatus(status?: string | null): boolean {
	switch (normalizedToken(status)) {
		case "queued":
		case "pending":
			return true;
		default:
			return false;
	}
}

export function isBlockedExecutionStatus(status?: string | null): boolean {
	const normalized = normalizedToken(status);
	return normalized === "blocked" || normalized === "paused";
}

export function isActiveExecutionStatus(status?: string | null): boolean {
	switch (normalizedToken(status)) {
		case "starting":
		case "running":
		case "verifying":
			return true;
		default:
			return false;
	}
}

export function isActiveProviderBackedExecution(execution?: {
	status?: string | null;
	execution_mode?: string | null;
} | null): boolean {
	return execution?.execution_mode === "provider_backed" && isActiveExecutionStatus(execution.status);
}

export function isCurrentActivityExecutionStatus(status?: string | null): boolean {
	return isQueuedExecutionStatus(status) || isActiveExecutionStatus(status) || isBlockedExecutionStatus(status);
}

export function operatorLiveExecutionLabel(label?: string | null): string | null {
		switch ((label ?? "").trim()) {
			case "Workspace prepared":
				return "This step is getting ready.";
			case "Launching":
				return "This step is launching now.";
			case "Blocked":
			case "Blocked for retry":
				return "This step is blocked while Kingdom decides or performs recovery.";
			case "Provider running":
			case "Execution started":
				return "This step is still running.";
		case "Completion observed":
			return "This step is finishing up.";
		case "Retry queued":
			return "This step is queued to retry.";
		case "Rerun queued":
			return "This step is queued to rerun.";
		case "Recovered for retry":
			return "This step recovered and is continuing through retry handling.";
		case "Retry exhausted":
			return "Kingdom exhausted provider retries for this step.";
		default: {
			const trimmed = (label ?? "").trim();
			return trimmed === "" ? null : trimmed;
		}
	}
}

export function operatorExecutionStatusText(status?: string | null, liveLabel?: string | null, handoffState?: string | null): string | null {
	if (normalizedToken(status) === "paused") {
		switch (normalizedToken(handoffState)) {
			case "stop_retry_requested":
				return "Kingdom is sending another stop request before retrying this step.";
			case "cancellation_dispatched":
			case "stop_retry_dispatched":
				return "Kingdom is confirming that the previous stop request completed.";
			case "workspace_restore_failed":
				return "Kingdom stopped the previous attempt but could not restore the trusted workspace.";
			case "awaiting_workspace_quiescence":
				return "Kingdom is waiting for another active step to release the shared workspace.";
			default:
				return "Kingdom is waiting for the previous attempt to stop before starting the retry.";
		}
	}
	const liveBody = operatorLiveExecutionLabel(liveLabel);
	if (liveBody) {
		return liveBody;
	}
		switch (normalizedToken(status)) {
			case "starting":
				return "This step is launching now.";
			case "running":
				return "This step is still running.";
		case "blocked":
			return "This step is blocked while Kingdom decides or performs recovery.";
			case "verifying":
				return "Kingdom is still confirming the final result for this step.";
		case "queued":
		case "pending":
			return "This step is queued to start.";
		default:
			return null;
	}
}

export function executionProgressLabel(
	status?: string | null,
	progressReasonCode?: string | null,
	progressEventType?: string | null,
	liveLabel?: string | null,
): string | undefined {
	const trimmedLiveLabel = typeof liveLabel === "string" ? liveLabel.trim() : "";
	if (trimmedLiveLabel !== "") {
		return trimmedLiveLabel;
	}
	return liveExecutionLabel(progressReasonCode ?? undefined, progressEventType ?? undefined, status ?? undefined);
}

function isPreLaunchProgressReasonCode(reasonCode?: string | null): boolean {
	switch (normalizedToken(reasonCode)) {
		case "workspace_prepared":
		case "provider_launching":
			return true;
		default:
			return false;
	}
}

export function liveExecutionLabel(
	reasonCode?: string,
	eventType?: string,
	nodeState?: string,
): string | undefined {
	switch ((reasonCode ?? "").trim()) {
		case "workspace_prepared":
			return "Workspace prepared";
		case "provider_launching":
			return "Launching";
		case "provider_invoked":
			return "Provider running";
		case "runtime_completion_observation":
			return "Completion observed";
		case "runtime_observation":
			return "Runtime observation";
	}
		switch ((eventType ?? "").trim()) {
			case "attempt.started":
				return "Execution started";
			case "node.state_changed":
				switch ((nodeState ?? "").trim()) {
					case "starting":
						return "Launching";
					case "running":
						return "Execution started";
					case "blocked":
						return "Blocked";
					default:
						return undefined;
				}
		case "attempt.retry_queued":
			return "Retry queued";
		case "attempt.rerun_queued":
			return "Rerun queued";
		case "attempt.final_rework_queued":
			return "Final Governance Rework Queued";
		case "attempt.retry_exhausted":
			return "Retry exhausted";
		case "attempt.recovered":
			if ((nodeState ?? "").trim() === "blocked") {
				return "Blocked for retry";
			}
			return "Recovered for retry";
		case "attempt.failed":
			return "Execution failed";
		case "attempt.succeeded":
			return "Execution succeeded";
		case "attempt.cancelled":
			return "Execution cancelled";
		case "attempt.progress":
			return "Execution update";
		default:
			return undefined;
	}
}

export function attemptEventIconKey(event: Pick<TimelineEvent, "event_type" | "reason_code" | "payload_json">): AttemptEventIconKey | undefined {
	if (isReworkTimelineEvent(event)) {
		return "rotate";
	}
		switch (normalizedToken(event.event_type)) {
			case "attempt.started":
				return "play";
		case "attempt.succeeded":
		case "attempt.retry_recovered":
			return "check";
		case "attempt.failed":
		case "attempt.cancelled":
			return "x";
		case "attempt.retrying":
		case "attempt.retry_queued":
		case "attempt.rerun_queued":
		case "attempt.final_rework_queued":
		case "attempt.recovered":
			return "rotate";
			case "attempt.retry_exhausted":
			case "attempt.timeout_warning":
				return "alert";
			case "attempt.progress":
				return isPreLaunchProgressReasonCode(event.reason_code) ? "play" : "activity";
			default:
				return undefined;
		}
}

export function attemptEventLabelFragment(event: Pick<TimelineEvent, "event_type" | "reason_code" | "payload_json">): string | undefined {
	if (isReworkTimelineEvent(event)) {
		return undefined;
	}
	const nodeState = readPayloadField(event.payload_json, "state");
	const liveLabel = liveExecutionLabel(
		event.reason_code,
		event.event_type,
		typeof nodeState === "string" ? nodeState : undefined,
	);
		switch (normalizedToken(event.event_type)) {
			case "attempt.started":
				return "started";
		case "attempt.succeeded":
			return "succeeded";
		case "attempt.failed":
			return "failed";
		case "attempt.cancelled":
			return "cancelled";
		case "attempt.retrying":
			return "retrying";
		case "attempt.retry_recovered":
			return "recovered after retry";
		case "attempt.retry_queued":
			return "retry queued";
		case "attempt.rerun_queued":
			return "rerun queued";
		case "attempt.final_rework_queued":
			return "final governance rework queued";
		case "attempt.retry_exhausted":
			return "retry exhausted";
		case "attempt.recovered":
			return liveLabel ? liveLabel.toLowerCase() : "recovered for retry";
		case "attempt.progress":
			return liveLabel ?? "execution update";
		case "attempt.timeout_warning":
			return "approaching timeout";
		default:
			return undefined;
	}
}

export function isReworkTimelineEvent(event?: Pick<TimelineEvent, "event_type" | "reason_code"> | null): boolean {
	return normalizedToken(event?.event_type) === "attempt.failed" && isReworkReasonCode(event?.reason_code);
}

export function reworkRequestLabel(reasonCode?: string | null): string {
	switch (normalizedToken(reasonCode)) {
		case "execution_review_rework":
			return "Review requested rework";
		case "validation_gate_rework":
			return "Validation requested rework";
		default:
			return "Rework requested";
	}
}

export function badgeToneForLabel(label: string | undefined, fallbackTone: RunStatusTone): RunStatusTone {
	return isCapAutoSucceededLabel(label) ? "running" : fallbackTone;
}

	export function timelineEventTone(event: Pick<TimelineEvent, "event_type" | "reason_code" | "payload_json">): RunStatusTone {
	if (isReworkTimelineEvent(event)) {
		return "rework";
	}
	if (normalizedToken(event.event_type) === "attempt.progress" && isPreLaunchProgressReasonCode(event.reason_code)) {
		return "starting";
	}
	const attemptIcon = attemptEventIconKey(event);
	if (attemptIcon) {
		switch (attemptIcon) {
			case "play":
			case "activity":
				return "running";
			case "check":
				return "succeeded";
			case "x":
				return normalizedToken(event.event_type) === "attempt.cancelled" ? "cancelled" : "failed";
			case "rotate":
				return normalizedToken(event.event_type) === "attempt.retry_queued" || normalizedToken(event.event_type) === "attempt.rerun_queued" || normalizedToken(event.event_type) === "attempt.final_rework_queued"
					? "ready"
					: "blocked";
			case "alert":
				return "blocked";
		}
	}

	switch (normalizedToken(event.event_type)) {
		case "approval.recorded":
			return "succeeded";
		case "escalation.raised":
			return "failed";
		case "node.state_changed":
			return normalizedRunStatusTone(readPayloadField(event.payload_json, "state")) ?? "neutral";
		case "run.state_changed": {
			const action = normalizedToken(readPayloadField(event.payload_json, "action"));
			if (action === "cancel") {
				return "cancelled";
			}
			const status = normalizedToken(readPayloadField(event.payload_json, "status"));
			if (status === "running") {
				return "running";
			}
			if (status === "cancelled") {
				return "cancelled";
			}
			if (status === "failed") {
				return "failed";
			}
			if (status === "succeeded" || status === "completed") {
				return "succeeded";
			}
			return "neutral";
		}
		default:
			return "neutral";
	}
}

function isCapAutoSucceededLabel(label?: string | null): boolean {
	return CAP_AUTO_SUCCEEDED_LABELS.has((label ?? "").trim());
}

function normalizedToken(value: unknown): string {
	return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function readPayloadField(payload: TimelineEvent["payload_json"], key: string): unknown {
	if (!payload || typeof payload !== "object") {
		return undefined;
	}
	return (payload as Record<string, unknown>)[key];
}
