import { useNavigate, useRouterState } from "@tanstack/react-router";
import { useEffect } from "react";
import { usePendingGovernanceRuns, type RunSummary } from "@/api/hooks";
import { useNotifications } from "@/components/notifications";
import { describeManualGovernanceState } from "./manual-governance-state";

const STORAGE_KEY = "kingdom:dismissed-governance-notices";
const SUMMARY_NOTIFICATION_ID = "governance-pending-summary";

interface PendingGovernanceItem {
	run: RunSummary;
	stage: "plan" | "final";
}

interface PendingGovernanceSummary {
	items: PendingGovernanceItem[];
	planCount: number;
	finalCount: number;
	latestRun: RunSummary;
}

function notificationId() {
	return SUMMARY_NOTIFICATION_ID;
}

function stageForRun(run: RunSummary): "plan" | "final" | null {
	if (run.autonomous_approvals !== false) {
		return null;
	}
	const state = describeManualGovernanceState(run);
	if (state.isAwaitingPlanApproval) {
		return "plan";
	}
	if (state.isAwaitingFinalApproval) {
		return "final";
	}
	return null;
}

function titleForStage(stage: "plan" | "final") {
	return stage === "plan" ? "Plan Approval Required" : "Final Approval Required";
}

function activeRunIDFromPath(pathname: string): string | null {
	const match = /^\/runs\/([^/]+)$/.exec(pathname);
	if (!match) {
		return null;
	}
	try {
		return decodeURIComponent(match[1]);
	} catch {
		return match[1];
	}
}

function pendingApprovalInstanceKey(run: RunSummary, stage: "plan" | "final") {
	const stableKey = typeof run.pending_approval_key === "string" && run.pending_approval_key.trim() !== ""
		? run.pending_approval_key.trim()
		: stage;
	return `${run.run_id}:${stage}:${stableKey}`;
}

function pluralize(count: number, singular: string, plural: string) {
	return `${count} ${count === 1 ? singular : plural}`;
}

function collectPendingGovernanceItems(runs: RunSummary[], activeRunID: string | null): PendingGovernanceItem[] {
	return runs.flatMap((run) => {
		if (activeRunID !== null && run.run_id === activeRunID) {
			return [];
		}
		const stage = stageForRun(run);
		return stage === null ? [] : [{ run, stage }];
	});
}

function summarizePendingGovernance(items: PendingGovernanceItem[]): PendingGovernanceSummary | null {
	if (items.length === 0) {
		return null;
	}

	let latest = items[0];
	let planCount = 0;
	let finalCount = 0;
	for (const item of items) {
		if (item.stage === "plan") {
			planCount += 1;
		} else {
			finalCount += 1;
		}
		if (item.run.updated_at > latest.run.updated_at) {
			latest = item;
		}
	}

	return {
		items,
		planCount,
		finalCount,
		latestRun: latest.run,
	};
}

function summaryInstanceKey(summary: PendingGovernanceSummary) {
	return summary.items
		.map(({ run, stage }) => pendingApprovalInstanceKey(run, stage))
		.sort()
		.join("|");
}

function summaryTitle(summary: PendingGovernanceSummary) {
	if (summary.items.length === 1) {
		return titleForStage(summary.items[0].stage);
	}
	return "Pending Manual Approvals";
}

function removeStaleGovernanceNotifications<TNotification>(
	notifications: Map<string, TNotification>,
	removeNotification: (id: string) => void,
	activeID?: string,
) {
	for (const existingID of notifications.keys()) {
		if (existingID.startsWith("governance-") && existingID !== activeID) {
			removeNotification(existingID);
		}
	}
}

function summaryMessage(summary: PendingGovernanceSummary) {
	const latestLabel = summary.latestRun.decree_title || summary.latestRun.run_id;
	if (summary.items.length === 1) {
		return latestLabel;
	}
	const parts: string[] = [];
	if (summary.planCount > 0) {
		parts.push(pluralize(summary.planCount, "plan approval", "plan approvals"));
	}
	if (summary.finalCount > 0) {
		parts.push(pluralize(summary.finalCount, "final approval", "final approvals"));
	}
	return `${parts.join(", ")} across ${summary.items.length} runs. Latest: ${latestLabel}.`;
}

function itemInstanceKeys(items: PendingGovernanceItem[]) {
	return items.map(({ run, stage }) => pendingApprovalInstanceKey(run, stage));
}

function readDismissed(): Record<string, true> {
	try {
		const raw = sessionStorage.getItem(STORAGE_KEY);
		if (!raw) {
			return {};
		}
		const parsed = JSON.parse(raw) as Record<string, unknown>;
		if (!parsed || typeof parsed !== "object") {
			return {};
		}
		const dismissed: Record<string, true> = {};
		for (const [key, value] of Object.entries(parsed)) {
			if (typeof key === "string" && key.trim() !== "" && value === true) {
				dismissed[key] = true;
			}
		}
		return dismissed;
	} catch {
		return {};
	}
}

function writeDismissed(value: Record<string, true>) {
	sessionStorage.setItem(STORAGE_KEY, JSON.stringify(value));
}

function pruneDismissedKeys(dismissed: Record<string, true>, activeKeys: string[]) {
	if (activeKeys.length === 0) {
		return {};
	}
	const active = new Set(activeKeys);
	const next: Record<string, true> = {};
	for (const key of Object.keys(dismissed)) {
		if (active.has(key)) {
			next[key] = true;
		}
	}
	return next;
}

export function GovernanceNotifierMount() {
	const { data } = usePendingGovernanceRuns();
	const { notifications, addNotification, updateNotification, removeNotification } = useNotifications();
	const navigate = useNavigate();
	const pathname = useRouterState({ select: (state) => state.location.pathname });
	const activeRunID = activeRunIDFromPath(pathname);
	const runs = data?.items ?? [];

	useEffect(() => {
		const id = notificationId();
		const allPendingItems = collectPendingGovernanceItems(runs, null);
		const visibleItems = collectPendingGovernanceItems(runs, activeRunID);
		const allPendingItemKeys = itemInstanceKeys(allPendingItems);
		const dismissed = pruneDismissedKeys(readDismissed(), allPendingItemKeys);
		const undisclosedItems = visibleItems.filter(({ run, stage }) => dismissed[pendingApprovalInstanceKey(run, stage)] !== true);
		const undisclosedItemKeys = itemInstanceKeys(undisclosedItems);
		const summary = summarizePendingGovernance(undisclosedItems);
		if (summary === null) {
			writeDismissed(dismissed);
			removeStaleGovernanceNotifications(notifications, removeNotification);
			return;
		}

		const currentInstanceKey = summaryInstanceKey(summary);
		const title = summaryTitle(summary);
		const message = summaryMessage(summary);
		const existing = notifications.get(id);
		const onDismiss = () => {
			const latest = pruneDismissedKeys(readDismissed(), allPendingItemKeys);
			for (const itemKey of undisclosedItemKeys) {
				latest[itemKey] = true;
			}
			writeDismissed(latest);
		};
		const action = {
			label: "Open Runs",
			onClick: () => {
				void navigate({ to: "/runs" });
			},
		};
		if (!existing) {
			addNotification({
				id,
				title,
				message,
				instanceKey: currentInstanceKey,
				status: "info",
				onDismiss,
				action,
			});
			writeDismissed(dismissed);
			return;
		}
		if (existing.title !== title || existing.message !== message || existing.status !== "info" || existing.instanceKey !== currentInstanceKey) {
			updateNotification(id, {
				title,
				message,
				instanceKey: currentInstanceKey,
				status: "info",
				onDismiss,
				action,
			});
		}
		writeDismissed(dismissed);
		removeStaleGovernanceNotifications(notifications, removeNotification, id);
	}, [runs, activeRunID, notifications, addNotification, updateNotification, removeNotification, navigate]);

	return null;
}
