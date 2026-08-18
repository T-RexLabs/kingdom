import { memo } from "react";
import ReactMarkdown from "react-markdown";
import remarkBreaks from "remark-breaks";
import remarkGfm from "remark-gfm";
import { cn } from "@/lib/utils";

/** Hoisted to module scope — these never change between renders. */
const remarkPlugins = [remarkBreaks, remarkGfm];

const components = {
  p: ({ children }: { children?: React.ReactNode }) => (
    <p className="mb-3 text-wrap-anywhere last:mb-0">{children}</p>
  ),
  ul: ({ children }: { children?: React.ReactNode }) => (
    <ul className="mb-3 list-disc pl-4 text-wrap-anywhere last:mb-0">
      {children}
    </ul>
  ),
  ol: ({ children }: { children?: React.ReactNode }) => (
    <ol className="mb-3 list-decimal pl-4 text-wrap-anywhere last:mb-0">
      {children}
    </ol>
  ),
  a: ({ href, children }: { href?: string; children?: React.ReactNode }) => (
    <a
      href={safeMarkdownURL(href)}
      target="_blank"
      rel="noopener noreferrer"
      className="font-medium text-gold underline underline-offset-4 transition-colors hover:text-gold/80"
    >
      {children}
    </a>
  ),
  img: ({ alt }: { alt?: string }) => <span className="text-foreground-muted">[remote image blocked{alt ? `: ${alt}` : ""}]</span>,
  li: ({ children }: { children?: React.ReactNode }) => (
    <li className="mb-1">{children}</li>
  ),
  table: ({ children }: { children?: React.ReactNode }) => (
    <table className="mb-3 w-full border-collapse text-left text-sm">
      {children}
    </table>
  ),
  thead: ({ children }: { children?: React.ReactNode }) => (
    <thead className="border-b border-border">{children}</thead>
  ),
  tbody: ({ children }: { children?: React.ReactNode }) => (
    <tbody>{children}</tbody>
  ),
  tr: ({ children }: { children?: React.ReactNode }) => (
    <tr className="border-b border-border/60 last:border-b-0">{children}</tr>
  ),
  th: ({ children }: { children?: React.ReactNode }) => (
    <th className="px-2 py-1 font-semibold text-foreground">{children}</th>
  ),
  td: ({ children }: { children?: React.ReactNode }) => (
    <td className="px-2 py-1 align-top">{children}</td>
  ),
  hr: () => (
    <div className="mt-4 mb-6">
      <hr className="border-border/60" />
    </div>
  ),
  code: ({ children }: { children?: React.ReactNode }) => (
    <code className="text-wrap-anywhere rounded bg-surface-raised px-1 py-0.5 font-mono text-[11px]">
      {children}
    </code>
  ),
  pre: ({ children }: { children?: React.ReactNode }) => (
    <pre className="mb-3 overflow-x-auto rounded bg-surface-raised p-2 text-[11px] last:mb-0">
      {children}
    </pre>
  ),
};

function safeMarkdownURL(raw?: string): string | undefined {
  if (!raw) return undefined;
  try {
    const url = new URL(raw, window.location.origin);
    return url.protocol === "http:" || url.protocol === "https:" || url.protocol === "mailto:" ? raw : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Shared markdown renderer used across the plan panel, decree panel,
 * and mailbox message viewer.  Keeps styling consistent everywhere.
 *
 * Wrapped in React.memo — skips re-rendering (and re-parsing) when
 * content and className haven't changed.
 */
export const MarkdownBlock = memo(function MarkdownBlock({
  content,
  className,
}: {
  content: string;
  className?: string;
}) {
  if (!content.trim()) {
    return null;
  }
  return (
    <div className={cn("text-wrap-anywhere", className)}>
      <ReactMarkdown skipHtml remarkPlugins={remarkPlugins} components={components}>
        {content}
      </ReactMarkdown>
    </div>
  );
});
