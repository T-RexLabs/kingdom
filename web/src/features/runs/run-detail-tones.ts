import {
	STATUS_TONE_HEX,
	statusBadgeClass as sharedStatusBadgeClass,
	statusBannerClass as sharedStatusBannerClass,
	statusBorderTintClass as sharedStatusBorderTintClass,
	statusDotClass as sharedStatusDotClass,
	statusTextClass as sharedStatusTextClass,
} from "@/lib/status-system";

export type RunStatusTone =
  | "succeeded"
  | "failed"
  | "starting"
  | "running"
  | "ready"
  | "pending"
  | "blocked"
  | "cancelled"
  | "skipped"
  | "rework"
  | "neutral";

export const RUN_DETAIL_NEUTRAL_HEX = {
  background: "#0a0e17",
  surface: "#111827",
  border: "#1f2937",
  text: "#e8e6e1",
  muted: "#9ca3af",
  subtle: "#6b7280",
  gold: "#c9a84c",
} as const;

export const RUN_STATUS_HEX: Record<Exclude<RunStatusTone, "neutral">, string> = {
	succeeded: STATUS_TONE_HEX.succeeded,
	failed: STATUS_TONE_HEX.failed,
	starting: STATUS_TONE_HEX.running,
	running: STATUS_TONE_HEX.running,
	ready: STATUS_TONE_HEX.attention,
	pending: STATUS_TONE_HEX.neutral,
	blocked: STATUS_TONE_HEX.attention,
	cancelled: STATUS_TONE_HEX.cancelled,
	skipped: STATUS_TONE_HEX.neutral,
	rework: STATUS_TONE_HEX.rework,
};

export const RUN_ROLE_ACCENT_HEX: Record<string, string> = {
  king: "#c9a84c",
  chancellor: "#8b5cf6",
  "council-member": "#3b82f6",
  "noble-delegate": "#14b8a6",
  "clergy-delegate": "#f472b6",
  "burgher-delegate": "#f59e0b",
  "peasant-delegate": "#22c55e",
  "builder-berserker": "#ef4444",
  "builder-shieldmaiden": "#06b6d4",
  "reviewer-shieldmaiden": "#06b6d4",
  "intelligence-qa-scout": "#a3e635",
  "intelligence-nightraven": "#8b5cf6",
  "intelligence-loki-chaos": "#f97316",
};

export function runStatusTextClass(status: RunStatusTone): string {
	return sharedStatusTextClass(status);
}

export function runStatusDotClass(status: RunStatusTone): string {
	return sharedStatusDotClass(status);
}

export function runStatusBorderTintClass(status: RunStatusTone): string {
	return sharedStatusBorderTintClass(status);
}

export function runStatusBadgeClass(status: RunStatusTone, emphasis: "soft" | "strong" = "soft"): string {
	return sharedStatusBadgeClass(status, emphasis);
}

export function runStatusBannerClass(status: Exclude<RunStatusTone, "neutral">): string {
	return sharedStatusBannerClass(status);
}

export function accentTextClass(accent: "gold" | "loki"): string {
  return accent === "gold" ? "text-gold" : "text-role-loki";
}

export function accentBannerClass(accent: "gold" | "loki"): string {
  return accent === "gold"
    ? "border-gold/30 bg-gold/10 text-gold"
    : "border-role-loki/40 bg-role-loki/10 text-role-loki";
}

export function selectionCardClass(selected: boolean): string {
  return selected
    ? "border-gold/40 bg-surface-raised"
    : "border-border hover:bg-surface-raised";
}
