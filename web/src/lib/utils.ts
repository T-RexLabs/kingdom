import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";
import { statusBadgeClass } from "./status-system";
import { formatRelativeTime } from "./date-time";

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
  return formatRelativeTime(iso, nowMs);
}

export function formatElapsedDuration(
  startIso: string,
  endIso: string,
): string | null {
  const start = new Date(startIso).getTime();
  const end = new Date(endIso).getTime();
  const diffMs = end - start;
  if (diffMs < 0 || Number.isNaN(diffMs)) return null;
  if (diffMs < 1000) return "<1s";
  const seconds = Math.floor(diffMs / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return remainingSeconds > 0
    ? `${minutes}m ${remainingSeconds}s`
    : `${minutes}m`;
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

export function isAgentRole(roleId: string): boolean {
  return (roleId ?? "").trim() !== "operator";
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
  governance: 0,
  coordinator: 1,
  "plan-author": 2,
  "architecture-advisor": 3,
  "risk-advisor": 4,
  "systems-advisor": 5,
  "usability-advisor": 6,
  builder: 7,
  reviewer: 8,
  "qa-validator": 9,
  "security-validator": 10,
  "resilience-validator": 11,
};

// ---------------------------------------------------------------------------
// Text formatting helpers
// ---------------------------------------------------------------------------

/** Words that should be fully uppercased when formatting IDs. */
export const UPPERCASE_WORDS = new Set(["qa", "api", "ui", "id"]);

const EXECUTION_MODE_LABELS = {
  runtime_owned: "Runtime-Handled",
  provider_backed: "Agent-Executed",
} as const;

export interface NodeExecutionSemantics {
  executionModeLabel: string;
  roleLabel: string;
}

/**
 * Convert a role ID to a display-friendly title
 * (e.g. "builder" → "Builder", "qa-validator" → "QA Validator").
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

export function nodeExecutionSemantics(
  ownership: string,
  roleId: string,
): NodeExecutionSemantics {
  const normalizedOwnership = (ownership ?? "").trim();

  return {
    executionModeLabel:
      EXECUTION_MODE_LABELS[
        normalizedOwnership as keyof typeof EXECUTION_MODE_LABELS
      ] ?? "Unknown Execution Mode",
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
export function agentLabelForExecution(
  requiredRole: string | undefined,
  nodeId: string,
): string {
  const role = (requiredRole ?? "").trim();
  return role ? roleTitle(role) : nodeLabel(nodeId);
}
