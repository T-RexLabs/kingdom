import {
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
	| "paused"
  | "cancelled"
  | "skipped"
  | "rework"
  | "neutral";

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

export function accentTextClass(accent: "accent" | "loki"): string {
  return accent === "accent" ? "text-accent" : "text-role-loki";
}

export function accentBannerClass(accent: "accent" | "loki"): string {
  return accent === "accent"
    ? "border-accent/30 bg-accent/10 text-accent"
    : "border-role-loki/40 bg-role-loki/10 text-role-loki";
}

export function selectionCardClass(selected: boolean): string {
  return selected
    ? "border-accent/40 bg-surface-raised"
    : "border-border hover:bg-surface-raised";
}
