import type { RunExecution } from "@/api/hooks";
import {
	isActiveExecutionStatus,
	isBlockedExecutionStatus,
	isCurrentActivityExecutionStatus,
	isQueuedExecutionStatus,
} from "./run-status-semantics";

export type ActivityStepGroup = {
	nodeId: string;
	attempts: RunExecution[];
	preferredAttemptId: string;
	activityAt: string;
};

export function buildActivityStepGroups(executions: RunExecution[]): ActivityStepGroup[] {
	const groups = new Map<string, RunExecution[]>();
	for (const execution of executions) {
		const existing = groups.get(execution.node_id);
		if (existing) {
			existing.push(execution);
			continue;
		}
		groups.set(execution.node_id, [execution]);
	}
	return [...groups.entries()]
		.map(([nodeId, attempts]) => {
			const attemptsByRecency = [...attempts].sort(compareExecutionActivityTimeDesc);
			const preferredAttempt = preferredActivityAttempt(attemptsByRecency) ?? attemptsByRecency[0];
			return {
				nodeId,
				attempts: attemptsByRecency,
				preferredAttemptId: preferredAttempt?.attempt_id ?? "",
				activityAt: preferredAttempt?.activity_at ?? attemptsByRecency[0]?.activity_at ?? "",
			};
		})
		.sort(compareActivityStepGroupPriority);
}

export function preferredActivityAttempt(executions: RunExecution[]): RunExecution | null {
	const [preferred] = [...executions].sort(compareActivityAttemptPriority);
	return preferred ?? null;
}

export function isInFlightExecutionStatus(status: string): boolean {
	return isCurrentActivityExecutionStatus(status);
}

function compareActivityStepGroupPriority(left: ActivityStepGroup, right: ActivityStepGroup): number {
	const leftPreferred = preferredActivityAttempt(left.attempts);
	const rightPreferred = preferredActivityAttempt(right.attempts);
	if (!leftPreferred || !rightPreferred) {
		return compareExecutionActivityTimeDesc(left.attempts[0], right.attempts[0]);
	}
	return compareActivityAttemptPriority(leftPreferred, rightPreferred);
}

function compareActivityAttemptPriority(left: RunExecution, right: RunExecution): number {
	const leftPriority = activityExecutionPriority(left.status);
	const rightPriority = activityExecutionPriority(right.status);
	if (leftPriority !== rightPriority) {
		return leftPriority - rightPriority;
	}
	return compareExecutionActivityTimeDesc(left, right);
}

function activityExecutionPriority(status?: string | null): number {
	if (isActiveExecutionStatus(status)) {
		return 0;
	}
	if (isBlockedExecutionStatus(status)) {
		return 1;
	}
	if (isQueuedExecutionStatus(status)) {
		return 2;
	}
	return 3;
}

function compareExecutionActivityTimeDesc(
	left: Pick<RunExecution, "activity_at" | "attempt" | "attempt_id">,
	right: Pick<RunExecution, "activity_at" | "attempt" | "attempt_id">,
): number {
	if (left.activity_at !== right.activity_at) {
		return left.activity_at > right.activity_at ? -1 : 1;
	}
	if (left.attempt !== right.attempt) {
		return right.attempt - left.attempt;
	}
	return left.attempt_id.localeCompare(right.attempt_id);
}
