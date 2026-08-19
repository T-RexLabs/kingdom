const failureGuidance: Record<string, string> = {
  revoked: "GitHub App access was revoked or is invalid. Restore the installation, then run the connection health check.",
  permission_mismatch: "GitHub App configuration no longer matches. Configure exactly repository metadata read and Issues read, with zero subscribed events and no additional permissions, then run the connection health check.",
  timeout: "The GitHub refresh timed out. Check connectivity and retry.",
  rate_limited: "GitHub rate limited the refresh. Retry after the reported reset time.",
  inaccessible_or_missing: "The GitHub issue is inaccessible or missing. Check installation repository access.",
  deleted: "The linked GitHub issue was deleted. Historical Kingdom provenance remains available.",
  connection_disabled: "The GitHub connection is disabled. Enable and health-check it before refreshing.",
  repository_excluded: "This repository is not included in the GitHub connection, so the source was left as it is. Include the repository to resume refreshing.",
  malformed_response: "GitHub returned an invalid response. Retry later and check connection health if the problem continues.",
  oversized_response: "The GitHub response exceeded Kingdom's safe size limit. Reduce the source content before retrying.",
  service_unavailable: "GitHub is currently unavailable. Retry later.",
};

export function externalSourceFailureGuidance(status: string): string | undefined {
  return failureGuidance[status];
}

/**
 * Statuses a linked source has no reason to announce.
 *
 * A source that was just captured has not been checked, a check that found
 * nothing found nothing, and a check another surface already performed is
 * someone else's news. None of them is an update to adopt or a problem to fix,
 * so the honest render is silence — saying "Not Checked" or "Up to Date" costs
 * the operator a read and tells them only that the system is behaving.
 */
const QUIET_STATUSES = new Set(["never_checked", "unchanged", "superseded"]);

// Status values are Title Case like the rest of Kingdom's status vocabulary
// (see status-system.ts); the guidance above is prose and stays sentence case.
// Only statuses worth announcing live here, so this map and QUIET_STATUSES
// cannot drift into disagreeing about the same status.
const statusLabels: Record<string, string> = {
  changed: "Update Available",
  inaccessible_or_missing: "Issue Unavailable",
  deleted: "Issue Deleted",
  rate_limited: "Check Delayed",
  connection_disabled: "Connection Disabled",
  repository_excluded: "Repository Not Included",
  service_unavailable: "GitHub Unavailable",
  revoked: "Access Revoked",
  permission_mismatch: "Permissions Changed",
  timeout: "Check Timed Out",
  malformed_response: "Check Failed",
  oversized_response: "Source Too Large",
};

/**
 * What this source has to say for itself, or `undefined` when it has nothing.
 *
 * A source reports its status only when there is an update to adopt or a
 * problem to fix. Callers render the notice when it exists and render nothing
 * when it does not.
 */
export function externalSourceStatusNotice(status: string, updateAvailable = false): string | undefined {
  if (updateAvailable) {
    return "Update Available";
  }
  if (QUIET_STATUSES.has(status)) {
    return undefined;
  }
  return statusLabels[status] ?? "Check Failed";
}
