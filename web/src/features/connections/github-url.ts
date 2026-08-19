export function safeGitHubIssueURL(raw: string, repository: string, issueNumber: number): string | null {
  try {
    const url = new URL(raw);
    if (url.protocol !== "https:" || url.hostname !== "github.com" || url.port || url.username || url.password || url.search || url.hash) return null;
    const segments = repository.split("/");
    if (segments.length !== 2 || segments.some((segment) => !/^[A-Za-z0-9_.-]+$/.test(segment))) return null;
    if (!Number.isSafeInteger(issueNumber) || issueNumber <= 0 || url.pathname !== `/${segments[0]}/${segments[1]}/issues/${issueNumber}`) return null;
    return url.href;
  } catch {
    return null;
  }
}

// GitHub's documented entry point for preselecting the account an App is installed
// on. An already-installed account lands on its repository-selection screen, which
// is the only place a connection's repository grant can be changed.
export function safeGitHubInstallationManageURL(appSlug: string, accountID: string): string | null {
  if (!/^[a-z0-9](?:[a-z0-9-]{0,98}[a-z0-9])?$/.test(appSlug)) return null;
  if (!/^[1-9][0-9]{0,31}$/.test(accountID)) return null;
  return `https://github.com/apps/${appSlug}/installations/new/permissions?suggested_target_id=${accountID}`;
}
