export type StatusTone = "running" | "attention" | "succeeded" | "failed" | "cancelled" | "rework" | "neutral";

export const STATUS_TONE_HEX: Record<StatusTone, string> = {
	running: "#3b82f6",
	attention: "#f59e0b",
	succeeded: "#22c55e",
	failed: "#ef4444",
	cancelled: "#8b5cf6",
	rework: "#ec4899",
	neutral: "#6b7280",
};

export function statusToneForKey(status: string): StatusTone {
		switch (status.trim()) {
			case "busy":
			case "running":
			case "starting":
			case "verifying":
			case "terminal_observed":
			case "auto_succeeded":
			case "cap_auto_succeeded":
				return "running";
		case "idle":
		case "ready":
		case "queued":
		case "awaiting_human_confirmation":
		case "awaiting_plan_decision":
		case "awaiting_final_decision":
		case "blocked":
			return "attention";
		case "succeeded":
		case "committed_success":
		case "pass":
			return "succeeded";
		case "failed":
		case "committed_failure":
		case "error":
			return "failed";
		case "rework":
		case "requested_changes":
			return "rework";
		case "cancelled":
		case "committed_cancelled":
			return "cancelled";
		case "skipped":
		case "offline":
		case "pending":
		default:
			return "neutral";
	}
}

export function statusTextClass(status: string): string {
	switch (statusToneForKey(status)) {
		case "running":
			return "text-status-running";
		case "attention":
			return "text-status-blocked";
		case "succeeded":
			return "text-status-succeeded";
		case "failed":
			return "text-status-failed";
		case "cancelled":
			return "text-status-cancelled";
		case "rework":
			return "text-status-rework";
		case "neutral":
			return "text-foreground-subtle";
	}
}

export function statusDotClass(status: string): string {
	switch (statusToneForKey(status)) {
		case "running":
			return "bg-status-running";
		case "attention":
			return "bg-status-blocked";
		case "succeeded":
			return "bg-status-succeeded";
		case "failed":
			return "bg-status-failed";
		case "cancelled":
			return "bg-status-cancelled";
		case "rework":
			return "bg-status-rework";
		case "neutral":
			return "bg-foreground-subtle";
	}
}

export function statusBorderTintClass(status: string): string {
	switch (statusToneForKey(status)) {
		case "running":
			return "border-status-running/30";
		case "attention":
			return "border-status-blocked/30";
		case "succeeded":
			return "border-status-succeeded/30";
		case "failed":
			return "border-status-failed/30";
		case "cancelled":
			return "border-status-cancelled/30";
		case "rework":
			return "border-status-rework/30";
		case "neutral":
			return "";
	}
}

export function statusBadgeClass(status: string, emphasis: "soft" | "strong" = "soft"): string {
	const intensity = emphasis === "strong" ? "/15" : "/10";
	switch (statusToneForKey(status)) {
		case "running":
			return `bg-status-running${intensity} text-status-running`;
		case "attention":
			return `bg-status-blocked${intensity} text-status-blocked`;
		case "succeeded":
			return `bg-status-succeeded${intensity} text-status-succeeded`;
		case "failed":
			return `bg-status-failed${intensity} text-status-failed`;
		case "cancelled":
			return `bg-status-cancelled${intensity} text-status-cancelled`;
		case "rework":
			return `bg-status-rework${intensity} text-status-rework`;
		case "neutral":
			return emphasis === "strong"
				? "bg-foreground-subtle/15 text-foreground-subtle"
				: "bg-foreground-subtle/10 text-foreground-subtle";
	}
}

// Canonical human label for any agent / node / run status. Single source of truth:
// the working state ("busy", "running") reads as "Active" everywhere, sharing the one
// blue "running" tone from statusToneForKey. Unknown statuses fall back to title case.
const STATUS_LABELS: Record<string, string> = {
	busy: "Active",
	running: "Active",
	starting: "Launching",
	verifying: "Verifying",
	queued: "Queued",
	ready: "Ready",
	blocked: "Blocked",
	idle: "Idle",
	offline: "Offline",
	pending: "Pending",
	skipped: "Skipped",
	rework: "Rework",
	cancelled: "Cancelled",
	succeeded: "Succeeded",
	failed: "Failed",
	awaiting_plan_decision: "Awaiting Plan Decision",
	awaiting_final_decision: "Awaiting Final Decision",
	awaiting_human_confirmation: "Awaiting Confirmation",
};

export function statusLabel(status: string): string {
	const key = status.trim();
	const known = STATUS_LABELS[key];
	if (known) {
		return known;
	}
	return key
		.replace(/_/g, " ")
		.replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function statusBannerClass(status: string): string {
	switch (statusToneForKey(status)) {
		case "running":
			return "border-status-running/30 bg-status-running/10 text-status-running";
		case "attention":
			return "border-status-blocked/30 bg-status-blocked/10 text-status-blocked";
		case "succeeded":
			return "border-status-succeeded/30 bg-status-succeeded/10 text-status-succeeded";
		case "failed":
			return "border-status-failed/30 bg-status-failed/10 text-status-failed";
		case "cancelled":
			return "border-status-cancelled/30 bg-status-cancelled/10 text-status-cancelled";
		case "rework":
			return "border-status-rework/30 bg-status-rework/10 text-status-rework";
		case "neutral":
			return "border-status-pending/30 bg-status-pending/10 text-status-pending";
	}
}
