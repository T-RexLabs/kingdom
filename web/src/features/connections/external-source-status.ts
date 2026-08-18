const failureGuidance: Record<string, string> = {
  revoked: "GitHub App access was revoked or is invalid. Restore the installation, then run the connection health check.",
  permission_mismatch: "GitHub App configuration no longer matches. Configure exactly repository metadata read and Issues read, with zero subscribed events and no additional permissions, then run the connection health check.",
  timeout: "The GitHub refresh timed out. Check connectivity and retry.",
  rate_limited: "GitHub rate limited the refresh. Retry after the reported reset time.",
  inaccessible_or_missing: "The GitHub issue is inaccessible or missing. Check installation repository access.",
  deleted: "The linked GitHub issue was deleted. Historical Kingdom provenance remains available.",
  connection_disabled: "The GitHub connection is disabled. Enable and health-check it before refreshing.",
  malformed_response: "GitHub returned an invalid response. Retry later and check connection health if the problem continues.",
  oversized_response: "The GitHub response exceeded Kingdom's safe size limit. Reduce the source content before retrying.",
  service_unavailable: "GitHub is currently unavailable. Retry later.",
};

export function externalSourceFailureGuidance(status: string): string | undefined {
  return failureGuidance[status];
}

// Status values are Title Case like the rest of Kingdom's status vocabulary
// (see status-system.ts); the guidance above is prose and stays sentence case.
const statusLabels: Record<string, string> = {
  never_checked: "Not Checked",
  unchanged: "Up to Date",
  changed: "Update Available",
  inaccessible_or_missing: "Issue Unavailable",
  deleted: "Issue Deleted",
  rate_limited: "Check Delayed",
  connection_disabled: "Connection Disabled",
  service_unavailable: "GitHub Unavailable",
  revoked: "Access Revoked",
  permission_mismatch: "Permissions Changed",
  timeout: "Check Timed Out",
  malformed_response: "Check Failed",
  oversized_response: "Source Too Large",
  superseded: "Checked Elsewhere",
};

export function externalSourceStatusLabel(status: string, updateAvailable = false): string {
  if (updateAvailable) {
    return "Update Available";
  }
  return statusLabels[status] ?? "Check Failed";
}
