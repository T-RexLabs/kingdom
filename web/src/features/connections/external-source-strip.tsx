import { safeGitHubIssueURL } from "./github-url";
import { ExternalLink } from "lucide-react";

/**
 * The "this came from GitHub" strip.
 *
 * The decree form and the run view each grew their own copy, and the copies
 * drifted — different radius, different spacing, different gaps — for a line of
 * text and a couple of ghost buttons that should look identical wherever they
 * appear. One component, one geometry.
 *
 * The header is a single line: identity on the left, actions on the right,
 * sharing a centre line. It used to be top-aligned against 32px buttons inside
 * 12px padding, which made a strip half again as tall as its content and left
 * the label floating above the button labels it sat next to.
 */
export function ExternalSourceStrip({
  label,
  identity,
  actions,
  children,
}: {
  /** Accessible name for the region. */
  label: string;
  identity: React.ReactNode;
  actions?: React.ReactNode;
  /** Status lines, guidance, and any update review that belongs below. */
  children?: React.ReactNode;
}) {
  return (
    <section aria-label={label} className="min-w-0 rounded-lg border border-gold/25 bg-gold/5 px-3 py-1.5 text-xs">
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-x-3 gap-y-1">
        <p className="min-w-0 font-medium text-foreground">{identity}</p>
        {actions ? <div className="flex shrink-0 flex-wrap items-center gap-x-1 gap-y-1">{actions}</div> : null}
      </div>
      {children ? <div className="space-y-2 pb-1.5">{children}</div> : null}
    </section>
  );
}

/**
 * The linked `owner/repo#N` coordinate, or the bare text when the stored URL is
 * not one Kingdom is willing to send an operator to.
 */
export function ExternalSourceCoordinate({
  canonicalURL,
  repository,
  issueNumber,
}: {
  canonicalURL: string;
  repository: string;
  issueNumber: number;
}) {
  const url = safeGitHubIssueURL(canonicalURL, repository, issueNumber);
  const coordinate = `${repository}#${issueNumber}`;
  if (!url) return <span>{coordinate}</span>;
  return (
    <a href={url} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-gold underline">
      {coordinate}
      <ExternalLink className="h-3 w-3" />
    </a>
  );
}
