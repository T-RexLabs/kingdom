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
