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

const statusLabels: Record<string, string> = {
  never_checked: "Not checked",
  unchanged: "Up to date",
  changed: "Update available",
  inaccessible_or_missing: "Issue unavailable",
  deleted: "Issue deleted",
  rate_limited: "Check delayed",
  connection_disabled: "Connection disabled",
  service_unavailable: "GitHub unavailable",
  revoked: "Access revoked",
  permission_mismatch: "Permissions changed",
  timeout: "Check timed out",
  malformed_response: "Check failed",
  oversized_response: "Source too large",
  superseded: "Checked elsewhere",
};

export function externalSourceStatusLabel(status: string, updateAvailable = false): string {
  if (updateAvailable) {
    return "Update available";
  }
  return statusLabels[status] ?? "Check failed";
}
