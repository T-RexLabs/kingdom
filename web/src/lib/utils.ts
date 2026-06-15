import { type ClassValue, clsx } from "clsx";
import type { LucideIcon } from "lucide-react";
import {
  Axe,
  Bird,
  ClipboardList,
  Crown,
  Flame,
  Landmark,
  ScrollText,
  Search,
  Shield,
  ShieldCheck,
  Store,
  Wheat,
} from "lucide-react";
import { twMerge } from "tailwind-merge";
import { statusBadgeClass } from "./status-system";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Format a UTC timestamp string into a human-readable relative or absolute time.
 */
export function formatTimestamp(iso: string): string {
	return formatTimestampAt(iso, Date.now());
}

export function formatTimestampAt(iso: string, nowMs: number): string {
  const date = new Date(iso);
  const diffMs = nowMs - date.getTime();
  const diffSec = Math.max(0, Math.floor(diffMs / 1000));

  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function formatElapsedDuration(startIso: string, endIso: string): string | null {
	const start = new Date(startIso).getTime();
	const end = new Date(endIso).getTime();
	const diffMs = end - start;
	if (diffMs < 0 || Number.isNaN(diffMs)) return null;
	if (diffMs < 1000) return "<1s";
	const seconds = Math.floor(diffMs / 1000);
	if (seconds < 60) return `${seconds}s`;
	const minutes = Math.floor(seconds / 60);
	const remainingSeconds = seconds % 60;
	return remainingSeconds > 0 ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`;
}

/**
 * Capitalize the first letter of a string.
 */
export function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// ---------------------------------------------------------------------------
// Agent status constants
// ---------------------------------------------------------------------------

export type AgentStatus = "busy" | "idle" | "blocked" | "offline" | "failed";

/** Canonical display order for agent statuses. */
export const AGENT_STATUS_ORDER: AgentStatus[] = [
  "busy",
  "idle",
  "blocked",
  "offline",
  "failed",
];

/** Tailwind dot-color classes for each agent status. */
export const AGENT_STATUS_DOT_COLORS: Record<AgentStatus, string> = {
	busy: "bg-status-running",
	idle: "bg-status-blocked",
	blocked: "bg-status-blocked",
	offline: "bg-status-pending",
	failed: "bg-status-failed",
};

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

/**
 * Classify a heartbeat timestamp as alive (< 30 s), stale (≥ 30 s), or none.
 */
export function getHeartbeatState(
  lastHeartbeatAt?: string,
): "alive" | "stale" | "none" {
  if (!lastHeartbeatAt) return "none";
  const diff = Date.now() - new Date(lastHeartbeatAt).getTime();
  if (diff < 30_000) return "alive";
  return "stale";
}

// ---------------------------------------------------------------------------
// Role helpers
// ---------------------------------------------------------------------------

/**
 * Map a role_id to its display-friendly color class.
 */
export function getRoleColor(roleId: string): string {
  const map: Record<string, string> = {
    allfather: "text-gold",
    king: "text-role-king",
    chancellor: "text-role-chancellor",
    "council-member": "text-role-council",
    "noble-delegate": "text-role-noble",
    "clergy-delegate": "text-role-clergy",
    "burgher-delegate": "text-role-burgher",
    "peasant-delegate": "text-role-peasant",
    "builder-berserker": "text-role-berserker",
    "reviewer-shieldmaiden": "text-role-shieldmaiden",
    "intelligence-qa-scout": "text-role-scout",
    "intelligence-nightraven": "text-role-nightraven",
    "intelligence-loki-chaos": "text-role-loki",
  };
  return map[roleId] ?? "text-foreground-muted";
}

/**
 * Inline style object setting --glow-color to the role's CSS variable.
 * Use with .animate-glow-pulse or .animate-glow-pulse-subtle.
 */
export function getRoleGlowStyle(roleId: string): Record<string, string> {
  const map: Record<string, string> = {
    allfather: "var(--color-gold)",
    king: "var(--color-role-king)",
    chancellor: "var(--color-role-chancellor)",
    "council-member": "var(--color-role-council)",
    "noble-delegate": "var(--color-role-noble)",
    "clergy-delegate": "var(--color-role-clergy)",
    "burgher-delegate": "var(--color-role-burgher)",
    "peasant-delegate": "var(--color-role-peasant)",
    "builder-berserker": "var(--color-role-berserker)",
    "reviewer-shieldmaiden": "var(--color-role-shieldmaiden)",
    "intelligence-qa-scout": "var(--color-role-scout)",
    "intelligence-nightraven": "var(--color-role-nightraven)",
    "intelligence-loki-chaos": "var(--color-role-loki)",
  };
  return { "--glow-color": map[roleId] ?? "var(--color-foreground-muted)" };
}

export function getRoleIcon(roleId: string): LucideIcon {
  const map: Record<string, LucideIcon> = {
    allfather: Crown,
    king: Crown,
    chancellor: ScrollText,
    "council-member": ClipboardList,
    "noble-delegate": Landmark,
    "clergy-delegate": ShieldCheck,
    "burgher-delegate": Store,
    "peasant-delegate": Wheat,
    "builder-berserker": Axe,
    "reviewer-shieldmaiden": Shield,
    "intelligence-qa-scout": Search,
    "intelligence-nightraven": Bird,
    "intelligence-loki-chaos": Flame,
  };
  return map[roleId] ?? Crown;
}

export function isAgentRole(roleId: string): boolean {
  return (roleId ?? "").trim() !== "allfather";
}

/**
 * Map a status to its background color class for badges.
 */
export function getStatusBg(status: string): string {
  return statusBadgeClass(status, "strong");
}

/**
 * Role hierarchy for sorting — lower number = higher rank.
 */
export const ROLE_HIERARCHY_ORDER: Record<string, number> = {
  king: 0,
  chancellor: 1,
  "council-member": 2,
  "noble-delegate": 3,
  "clergy-delegate": 4,
  "burgher-delegate": 5,
  "peasant-delegate": 6,
  "builder-berserker": 7,
  "reviewer-shieldmaiden": 8,
  "intelligence-qa-scout": 9,
  "intelligence-nightraven": 10,
  "intelligence-loki-chaos": 11,
};

// ---------------------------------------------------------------------------
// Text formatting helpers
// ---------------------------------------------------------------------------

/** Words that should be fully uppercased when formatting IDs. */
export const UPPERCASE_WORDS = new Set(["qa", "api", "ui", "id"]);

const EXECUTION_MODE_LABELS = {
  runtime_owned: "Crown-handled",
  provider_backed: "Agent-executed",
} as const;

export interface NodeExecutionSemantics {
  executionModeLabel: string;
  roleLabel: string;
}

/**
 * Convert a role ID to a display-friendly title
 * (e.g. "builder-berserker" → "Builder Berserker", "intelligence-qa-scout" → "Intelligence QA Scout").
 */
export function roleTitle(roleId: string): string {
  const trimmed = (roleId ?? "").trim();
  if (!trimmed) return "Unknown Role";
  return trimmed
    .split("-")
    .map((w) => {
      const lower = w.toLowerCase();
      if (UPPERCASE_WORDS.has(lower)) return w.toUpperCase();
      return w.charAt(0).toUpperCase() + w.slice(1);
    })
    .join(" ");
}

export function nodeExecutionSemantics(ownership: string, roleId: string): NodeExecutionSemantics {
  const normalizedOwnership = (ownership ?? "").trim();

  return {
    executionModeLabel:
      EXECUTION_MODE_LABELS[normalizedOwnership as keyof typeof EXECUTION_MODE_LABELS] ?? "Execution mode unknown",
    roleLabel: roleTitle(roleId),
  };
}

/**
 * Convert an underscore-delimited node ID to a human-readable label
 * (e.g. "plan_qa_review" → "Plan QA Review").
 */
export function nodeLabel(nodeId: string): string {
  return nodeId
    .split("_")
    .map((w) => {
      const lower = w.toLowerCase();
      if (UPPERCASE_WORDS.has(lower)) return w.toUpperCase();
      return w.charAt(0).toUpperCase() + w.slice(1);
    })
    .join(" ");
}

/**
 * Friendly name for the agent behind an execution: the role title once it is known,
 * falling back to the node label before the role resolves. Avoids surfacing the
 * "Unknown Role" placeholder at the very start of a run, when execution records can
 * briefly lack a required_role.
 */
export function agentLabelForExecution(requiredRole: string | undefined, nodeId: string): string {
  const role = (requiredRole ?? "").trim();
  return role ? roleTitle(role) : nodeLabel(nodeId);
}
