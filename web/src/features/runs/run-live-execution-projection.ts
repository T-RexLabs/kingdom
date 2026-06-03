import { useQuery, type QueryClient } from "@tanstack/react-query";
import type { RunExecution, RunExecutionDetail } from "@/api/hooks";

export { liveExecutionLabel } from "./run-status-semantics";

export const liveExecutionProjectionKey = (runId: string) => ["runs", runId, "__live_execution_projection"] as const;

export interface LiveExecutionProjectionEntry {
	node_run_id: string;
	activity_at: string;
	run_event_seq?: number;
	attempt_seq?: number;
	attempt_id?: string;
	node_id?: string;
	attempt?: number;
	agent_id?: string;
	required_role?: string;
	execution_mode?: RunExecution["execution_mode"];
	status?: RunExecution["status"];
	live_label?: string;
	live_reason_code?: string;
	started_at?: string;
	terminal_observed_at?: string;
}

export type LiveExecutionProjection = Record<string, LiveExecutionProjectionEntry>;

const TERMINAL_LIVE_EXECUTION_STATUSES = new Set<NonNullable<RunExecution["status"]>>([
	"succeeded",
	"failed",
	"cancelled",
]);

export function readLiveExecutionProjection(qc: QueryClient, runId: string): LiveExecutionProjection {
	return qc.getQueryData<LiveExecutionProjection>(liveExecutionProjectionKey(runId)) ?? {};
}

export function useLiveExecutionProjection(runId: string) {
	return useQuery({
		queryKey: liveExecutionProjectionKey(runId),
		queryFn: async () => ({} as LiveExecutionProjection),
		enabled: false,
		initialData: {} as LiveExecutionProjection,
	});
}

export function patchLiveExecutionProjection(
	qc: QueryClient,
	runId: string,
	entry: LiveExecutionProjectionEntry,
) {
	qc.setQueryData<LiveExecutionProjection>(liveExecutionProjectionKey(runId), (current) => {
		const existing = current?.[entry.node_run_id];
		if (existing && shouldRetainExistingLiveProjection(existing, entry)) {
			return current ?? {};
		}
		return {
			...(current ?? {}),
			[entry.node_run_id]: {
				...existing,
				...entry,
			},
		};
	});
}

function liveExecutionStatusIsTerminal(status?: RunExecution["status"]): boolean {
	return typeof status === "string" && TERMINAL_LIVE_EXECUTION_STATUSES.has(status);
}

function shouldRetainExistingLiveProjection(
	existing: LiveExecutionProjectionEntry,
	entry: LiveExecutionProjectionEntry,
): boolean {
	if (liveExecutionStatusIsTerminal(existing.status) && !liveExecutionStatusIsTerminal(entry.status)) {
		return true;
	}
	const freshness = compareLiveExecutionFreshness(existing, entry);
	if (freshness > 0) {
		return true;
	}
	if (freshness < 0) {
		return false;
	}
	return liveExecutionStatusPriority(existing.status) >= liveExecutionStatusPriority(entry.status);
}

function compareLiveExecutionFreshness(
	left: LiveExecutionProjectionEntry,
	right: LiveExecutionProjectionEntry,
): number {
	if (left.activity_at !== right.activity_at) {
		return left.activity_at > right.activity_at ? 1 : -1;
	}
	const runEventComparison = compareOptionalNumber(left.run_event_seq, right.run_event_seq);
	if (runEventComparison !== 0) {
		return runEventComparison;
	}
	return compareOptionalNumber(left.attempt_seq, right.attempt_seq);
}

function compareOptionalNumber(left?: number, right?: number): number {
	if (typeof left === "number" && typeof right === "number") {
		if (left === right) {
			return 0;
		}
		return left > right ? 1 : -1;
	}
	if (typeof left === "number") {
		return 1;
	}
	if (typeof right === "number") {
		return -1;
	}
	return 0;
}

function liveExecutionStatusPriority(status?: RunExecution["status"]): number {
	if (liveExecutionStatusIsTerminal(status)) {
		return 2;
	}
	if (typeof status === "string" && status.trim() !== "") {
		return 1;
	}
	return 0;
}

function shouldIgnoreLiveExecutionOverlay(baseStatus: RunExecution["status"], liveStatus?: RunExecution["status"]): boolean {
	return liveExecutionStatusIsTerminal(baseStatus) && !liveExecutionStatusIsTerminal(liveStatus);
}

export function overlayExecutionItem(item: RunExecution, live: LiveExecutionProjectionEntry | undefined): RunExecution {
	if (!live) {
		return item;
	}
	if (shouldIgnoreLiveExecutionOverlay(item.status, live.status)) {
		return item;
	}
	return {
		...item,
		activity_at: live.activity_at > item.activity_at ? live.activity_at : item.activity_at,
		...(live.status ? { status: live.status } : {}),
	};
}

export function mergeExecutionItems(executions: RunExecution[], liveProjection: LiveExecutionProjection): RunExecution[] {
	const merged = executions.map((item) => overlayExecutionItem(item, liveProjection[item.node_run_id]));
	const seenNodeRunIDs = new Set(merged.map((item) => item.node_run_id));
	for (const live of Object.values(liveProjection)) {
		if (seenNodeRunIDs.has(live.node_run_id)) {
			continue;
		}
		const synthetic = syntheticExecutionItem(live);
		if (!synthetic) {
			continue;
		}
		merged.push(synthetic);
	}
	merged.sort(compareExecutionRecencyDesc);
	return merged;
}

export function overlayExecutionDetail(
	detail: RunExecutionDetail | null,
	live: LiveExecutionProjectionEntry | undefined,
): (RunExecutionDetail & { live_signal?: LiveExecutionProjectionEntry }) | null {
	if (!detail) {
		return null;
	}
	if (!live) {
		return detail;
	}
	if (shouldIgnoreLiveExecutionOverlay(detail.status, live.status)) {
		return detail;
	}
	return {
		...detail,
		activity_at: live.activity_at > detail.activity_at ? live.activity_at : detail.activity_at,
		...(live.status ? { status: live.status } : {}),
		...(live.started_at && (!detail.started_at || live.started_at > detail.started_at) ? { started_at: live.started_at } : {}),
		...(live.terminal_observed_at && (!detail.terminal_observed_at || live.terminal_observed_at > detail.terminal_observed_at)
			? { terminal_observed_at: live.terminal_observed_at }
			: {}),
		live_signal: live,
	};
}

function syntheticExecutionItem(live: LiveExecutionProjectionEntry): RunExecution | null {
	if (!live.attempt_id || !live.node_id || typeof live.attempt !== "number") {
		return null;
	}
	return {
		attempt_id: live.attempt_id,
		node_run_id: live.node_run_id,
		node_id: live.node_id,
		attempt: live.attempt,
		agent_id: live.agent_id ?? "",
		required_role: live.required_role ?? "",
		execution_mode: live.execution_mode ?? "provider_backed",
		status: live.status ?? "queued",
		activity_at: live.activity_at,
		updated_at: live.activity_at,
		...(live.started_at ? { started_at: live.started_at } : {}),
		...(live.terminal_observed_at ? { terminal_observed_at: live.terminal_observed_at } : {}),
	};
}

function compareExecutionRecencyDesc(
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
