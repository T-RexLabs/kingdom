import { ApiError, apiFetch } from "@/api/client";
import { useConnections } from "@/api/hooks";
import type { components } from "@/api/schema";
import { Link } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { Modal, ModalFooter } from "@/components/ui/modal-shell";
import { PopoverListbox } from "@/components/ui/popover-listbox";
import { SelectField } from "@/components/ui/select-field";
import { ToggleRow } from "@/components/ui/toggle-row";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { cn } from "@/lib/utils";
import { ExternalLink, Github, Link2, Loader2, Search } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { isConnectionUsable } from "./connection-readiness";
import { safeGitHubIssueURL } from "./github-url";

type Connection = components["schemas"]["Connection"];
type Repository = components["schemas"]["Repository"];
type RepositoryPage = components["schemas"]["RepositoryPage"];
type IssueSummary = components["schemas"]["IssueSummary"];
type IssuePage = components["schemas"]["IssuePage"];
type IssuePreview = components["schemas"]["IssuePreview"];
type ExternalComment = components["schemas"]["ExternalComment"];
type ExternalIntake = components["schemas"]["ExternalIntake"];

const COMMENT_PAGE_SIZE = 5;

export type IssueSelection = { intake: ExternalIntake; issue: components["schemas"]["NormalizedIssue"] };
type ErrorScope = "repositories" | "issues" | "preview" | "select";
type RequestError = { message: string; retryAt?: string; scope: ErrorScope };

function requestError(error: unknown): Omit<RequestError, "scope"> {
  if (error instanceof ApiError) return { message: error.message, retryAt: error.retryAt };
  return { message: error instanceof Error ? error.message : "GitHub request failed" };
}

export function IssuePicker({
  onSelect,
  actionLabel = "Use Issue",
  triggerLabel = "Import from GitHub Issue",
  compact = false,
}: {
  onSelect: (value: IssueSelection) => void | Promise<void>;
  actionLabel?: string;
  triggerLabel?: string;
  compact?: boolean;
}) {
  const [opened, setOpened] = useState(false);
  const [connectionId, setConnectionId] = useState("");
  const [repositories, setRepositories] = useState<Repository[]>([]);
  const [repositoryId, setRepositoryId] = useState("");
  const [repositoryCursor, setRepositoryCursor] = useState<string>();
  const [repositoryQuery, setRepositoryQuery] = useState("");
  const [debouncedRepositoryQuery, setDebouncedRepositoryQuery] = useState("");
  const [issues, setIssues] = useState<IssueSummary[]>([]);
  const [issueCursor, setIssueCursor] = useState<string>();
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [stateFilter, setStateFilter] = useState<"open" | "closed" | "all">("open");
  const [selected, setSelected] = useState<IssueSummary>();
  const [preview, setPreview] = useState<IssuePreview>();
  const [includeComments, setIncludeComments] = useState(false);
  const [commentPage, setCommentPage] = useState(0);
  const [loadingRepositories, setLoadingRepositories] = useState(false);
  const [loadingIssues, setLoadingIssues] = useState(false);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [incomplete, setIncomplete] = useState(false);
  const [error, setError] = useState<RequestError>();
  const [repositoryAttempt, setRepositoryAttempt] = useState(0);
  const [issueAttempt, setIssueAttempt] = useState(0);
  const [previewAttempt, setPreviewAttempt] = useState(0);
  const repositoryPagination = useRef<AbortController | undefined>(undefined);
  const issuePagination = useRef<AbortController | undefined>(undefined);
  const repositoryGeneration = useRef(0);
  const issueGeneration = useRef(0);

  useEffect(() => {
    const timer = window.setTimeout(() => setDebouncedRepositoryQuery(repositoryQuery.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [repositoryQuery]);

  useEffect(() => {
    const timer = window.setTimeout(() => setDebouncedQuery(query.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [query]);

  useEffect(() => {
    repositoryGeneration.current += 1;
    repositoryPagination.current?.abort();
    repositoryPagination.current = undefined;
    setRepositoryCursor(undefined);
    setLoadingRepositories(false);
    return () => repositoryPagination.current?.abort();
  }, [opened, connectionId, repositoryQuery, repositoryAttempt]);

  useEffect(() => {
    issueGeneration.current += 1;
    issuePagination.current?.abort();
    issuePagination.current = undefined;
    setIssueCursor(undefined);
    setLoadingIssues(false);
    return () => issuePagination.current?.abort();
  }, [opened, connectionId, repositoryId, query, stateFilter, issueAttempt]);

  // The trigger has to know whether importing is possible before it is
  // clicked, so connections load with the surface rather than with the modal.
  // Opening it re-reads them: the operator may have just repaired or added one
  // on the Connections page, and choosing a source they cannot read is a dead
  // end they only discover several clicks later.
  const connectionsQuery = useConnections();
  const refetchConnections = connectionsQuery.refetch;
  const allConnections = connectionsQuery.data?.items ?? [];
  const connections = allConnections.filter(isConnectionUsable);
  const connectionsLoaded = connectionsQuery.isSuccess;
  const loadingConnections = connectionsQuery.isPending;
  const connectionError = connectionsQuery.error ? requestError(connectionsQuery.error) : undefined;

  useEffect(() => {
    if (opened) void refetchConnections();
  }, [opened, refetchConnections]);

  useEffect(() => {
    setRepositories([]);
    setRepositoryId("");
    setRepositoryCursor(undefined);
    setIssues([]);
    setIssueCursor(undefined);
    setSelected(undefined);
    setPreview(undefined);
    if (!opened || !connectionId) return;
    const controller = new AbortController();
    setLoadingRepositories(true);
    setError(undefined);
    apiFetch<RepositoryPage>(`/connections/${connectionId}/repositories`, {
      signal: controller.signal,
      params: { search: debouncedRepositoryQuery || undefined },
    })
      .then((value) => {
        setRepositories(value.items);
        setRepositoryCursor(value.next_cursor);
      })
      .catch((cause) => {
        if (!controller.signal.aborted) setError({ ...requestError(cause), scope: "repositories" });
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoadingRepositories(false);
      });
    return () => controller.abort();
  }, [opened, connectionId, debouncedRepositoryQuery, repositoryAttempt]);

  useEffect(() => {
    setIssues([]);
    setIssueCursor(undefined);
    setSelected(undefined);
    setPreview(undefined);
    setIncomplete(false);
    if (!opened || !connectionId || !repositoryId) return;
    const controller = new AbortController();
    setLoadingIssues(true);
    setError(undefined);
    apiFetch<IssuePage>(`/connections/${connectionId}/repositories/${repositoryId}/issues`, {
      signal: controller.signal,
      params: { state: stateFilter, search: debouncedQuery || undefined },
    })
      .then((value) => {
        setIssues(value.items);
        setIssueCursor(value.next_cursor);
        setIncomplete(value.incomplete_results);
      })
      .catch((cause) => {
        if (!controller.signal.aborted) setError({ ...requestError(cause), scope: "issues" });
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoadingIssues(false);
      });
    return () => controller.abort();
  }, [opened, connectionId, repositoryId, stateFilter, debouncedQuery, issueAttempt]);

  useEffect(() => {
    setPreview(undefined);
    setCommentPage(0);
    if (!selected || !connectionId || !repositoryId) return;
    const controller = new AbortController();
    setLoadingPreview(true);
    setError(undefined);
    apiFetch<IssuePreview>(`/connections/${connectionId}/repositories/${repositoryId}/issues/${selected.issue.issue_number}/preview`, {
      signal: controller.signal,
      params: { include_comments: includeComments },
    })
      .then(setPreview)
      .catch((cause) => {
        if (!controller.signal.aborted) setError({ ...requestError(cause), scope: "preview" });
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoadingPreview(false);
      });
    return () => controller.abort();
  }, [selected, connectionId, repositoryId, includeComments, previewAttempt]);

  async function loadMoreRepositories() {
    if (!repositoryCursor || loadingRepositories) return;
    const generation = repositoryGeneration.current;
    const controller = new AbortController();
    repositoryPagination.current?.abort();
    repositoryPagination.current = controller;
    setLoadingRepositories(true);
    setError(undefined);
    try {
      const value = await apiFetch<RepositoryPage>(`/connections/${connectionId}/repositories`, {
        signal: controller.signal,
        params: { cursor: repositoryCursor, search: debouncedRepositoryQuery || undefined },
      });
      if (controller.signal.aborted || generation !== repositoryGeneration.current) return;
      setRepositories((current) => [...current, ...value.items]);
      setRepositoryCursor(value.next_cursor);
    } catch (cause) {
      if (!controller.signal.aborted && generation === repositoryGeneration.current)
        setError({ ...requestError(cause), scope: "repositories" });
    } finally {
      if (generation === repositoryGeneration.current && repositoryPagination.current === controller) {
        repositoryPagination.current = undefined;
        setLoadingRepositories(false);
      }
    }
  }

  async function loadMoreIssues() {
    if (!issueCursor || loadingIssues) return;
    const generation = issueGeneration.current;
    const controller = new AbortController();
    issuePagination.current?.abort();
    issuePagination.current = controller;
    setLoadingIssues(true);
    setError(undefined);
    try {
      const value = await apiFetch<IssuePage>(`/connections/${connectionId}/repositories/${repositoryId}/issues`, {
        signal: controller.signal,
        params: { cursor: issueCursor, state: stateFilter, search: debouncedQuery || undefined },
      });
      if (controller.signal.aborted || generation !== issueGeneration.current) return;
      setIssues((current) => [...current, ...value.items]);
      setIssueCursor(value.next_cursor);
      setIncomplete(value.incomplete_results);
    } catch (cause) {
      if (!controller.signal.aborted && generation === issueGeneration.current)
        setError({ ...requestError(cause), scope: "issues" });
    } finally {
      if (generation === issueGeneration.current && issuePagination.current === controller) {
        issuePagination.current = undefined;
        setLoadingIssues(false);
      }
    }
  }

  async function submit() {
    if (!preview) return;
    setSubmitting(true);
    setError(undefined);
    try {
      await onSelect({
        issue: preview.issue,
        intake: {
          connection_id: connectionId,
          repository_id: repositoryId,
          issue_number: preview.issue.issue_number,
          include_comments: includeComments,
        },
      });
      setOpened(false);
    } catch (cause) {
      setError({ ...requestError(cause), scope: "select" });
    } finally {
      setSubmitting(false);
    }
  }

  const shownIssue = preview?.issue ?? selected?.issue;
  const previewComments: ExternalComment[] = includeComments && preview?.profile.include_comments ? preview.comments.slice(0, 100) : [];
  const commentPageCount = Math.max(1, Math.ceil(previewComments.length / COMMENT_PAGE_SIZE));
  const shownComments = previewComments.slice(commentPage * COMMENT_PAGE_SIZE, (commentPage + 1) * COMMENT_PAGE_SIZE);
  const issueURL = shownIssue ? safeGitHubIssueURL(shownIssue.canonical_url, shownIssue.repository, shownIssue.issue_number) : null;

  return (
    <>
      <PickerTrigger
        label={triggerLabel}
        compact={compact}
        needsSetup={connectionsLoaded && allConnections.length === 0}
        disabled={loadingConnections}
        onOpen={() => setOpened(true)}
      />
      <Modal
        open={opened}
        onClose={() => setOpened(false)}
        title="Import from GitHub Issues"
        size="xl"
        height="workbench"
        bodyClassName="overflow-hidden p-0"
        overlayClassName="p-2 sm:p-4"
        footer={<ModalFooter><Button type="button" variant="ghost" onClick={() => setOpened(false)}>Cancel</Button><Button type="button" onClick={() => void submit()} disabled={!preview || loadingPreview || submitting}>{submitting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}{actionLabel}</Button></ModalFooter>}
      >
          <div data-testid="issue-picker-layout" className="grid h-full min-h-0 min-w-0 grid-cols-1 overflow-y-auto lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)] lg:overflow-hidden">
            <div className="flex min-w-0 flex-col gap-4 border-b border-border p-4 lg:overflow-hidden lg:border-b-0 lg:border-r sm:p-5">
              <ConnectionControls
                loading={loadingConnections}
                loaded={connectionsLoaded}
                loadError={connectionError}
                allConnections={allConnections}
                connections={connections}
                connectionId={connectionId}
                onConnectionChange={setConnectionId}
                onRetry={() => void refetchConnections()}
              />
              {connections.length > 0 ? (
                <div className="min-w-0">
                  <label htmlFor="github-repository" className={fieldLabelClass}>Repository</label>
                  <PopoverListbox
                    triggerId="github-repository"
                    triggerRole="combobox"
                    triggerAriaLabel="GitHub repository"
                    triggerDisabled={!connectionId}
                    triggerClassName={cn(fieldClass, "truncate text-left", !connectionId && "cursor-not-allowed opacity-50", !repositoryId && "text-foreground-subtle")}
                    triggerContent={repositories.find((item) => item.id === repositoryId)?.locator ?? "Select repository"}
                    items={repositories.map((item) => ({ id: item.id, label: item.locator }))}
                    selectedId={repositoryId || null}
                    onSelect={setRepositoryId}
                    search={{ value: repositoryQuery, onChange: setRepositoryQuery, placeholder: "Filter owner/name" }}
                    minWidth={280}
                    onReachEnd={() => void loadMoreRepositories()}
                    emptyState={<p className="px-3 py-2 text-xs text-foreground-subtle">{loadingRepositories ? "Loading repositories..." : repositoryCursor ? "No repositories match the pages loaded so far. Keep scrolling to load more." : "No repositories match."}</p>}
                    footer={repositoryCursor ? <p role="status" className="flex items-center justify-center gap-2 py-1 text-xs text-foreground-subtle">{loadingRepositories ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}Loading more repositories...</p> : undefined}
                  />
                </div>
              ) : null}
              {repositoryId ? (
                <>
                  <div className="flex min-w-0 flex-col gap-2 sm:flex-row">
                    <label className="relative min-w-0 flex-1"><Search className="absolute left-3 top-2.5 h-4 w-4 text-foreground-subtle" /><input aria-label="Search issues" className={cn(fieldClass, "py-2 pl-9 pr-3")} value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search this repository" /></label>
                    <SelectField
                      id="github-issue-state"
                      label="Issue State"
                      labelClassName="sr-only"
                      className="sm:w-32"
                      value={stateFilter}
                      options={[{ label: "Open", value: "open" }, { label: "Closed", value: "closed" }, { label: "All", value: "all" }]}
                      onChange={(next) => setStateFilter(next as typeof stateFilter)}
                    />
                  </div>
                  {incomplete ? <p role="status" className="text-xs text-status-warning">GitHub returned partial search results. Search is capped at 1,000 issues.</p> : null}
                  <div className="max-h-64 min-w-0 divide-y divide-border overflow-auto rounded-md border border-border lg:max-h-none lg:min-h-0 lg:flex-1">
                    {loadingIssues && issues.length === 0 ? <p className="flex items-center gap-2 p-3 text-xs text-foreground-muted"><Loader2 className="h-3.5 w-3.5 animate-spin" />Loading issues...</p> : issues.length === 0 ? <p className="p-3 text-xs text-foreground-muted">No issues found. Pull requests are excluded.</p> : issues.map((item) => <button type="button" key={`${item.native_id}-${item.issue.issue_number}`} onClick={() => setSelected(item)} className={cn("w-full min-w-0 p-3 text-left hover:bg-surface-raised", selected?.native_id === item.native_id && "bg-gold/10")}><span className="text-xs text-foreground-subtle">#{item.issue.issue_number} / {item.issue.state}</span><p className="break-words text-sm font-medium">{item.issue.title}</p></button>)}
                  </div>
                  {issueCursor ? <Button type="button" variant="ghost" size="sm" onClick={() => void loadMoreIssues()} disabled={loadingIssues}>{loadingIssues ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}Load More Issues</Button> : null}
                </>
              ) : null}
              {error && (error.scope === "repositories" || error.scope === "issues") ? <PickerError error={error} onRetry={() => error.scope === "repositories" ? setRepositoryAttempt((value) => value + 1) : setIssueAttempt((value) => value + 1)} /> : null}
            </div>
            <div className="flex min-w-0 flex-col p-4 lg:overflow-hidden sm:p-5">
              {shownIssue ? (
                // `min-h-0` keeps this column shrinkable. Without it the flex
                // item's automatic minimum size pins it to its content height,
                // the preview below never gets a bounded box to scroll in, and
                // the overflow is silently clipped by the pane's `overflow-hidden`.
                <div className="flex min-h-0 min-w-0 flex-1 flex-col">
                  <div className="min-w-0">
                    <p className="break-words font-semibold text-foreground">{shownIssue.title}</p>
                    <p className="mt-1 break-words text-xs text-foreground-muted">{issueURL ? <a href={issueURL} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-gold underline">{shownIssue.repository}#{shownIssue.issue_number}<ExternalLink className="h-3 w-3" /></a> : <span>{shownIssue.repository}#{shownIssue.issue_number}</span>} / updated {new Date(shownIssue.updated_at).toLocaleString()}</p>
                  </div>
                  <div className="mt-4 rounded-lg border border-border bg-background/40 px-3">
                    <ToggleRow
                      label="Include Comments"
                      checked={includeComments}
                      info="Imports the newest 100 comment bodies, capped at 32 KiB each and 128 KiB in total."
                      onChange={setIncludeComments}
                    />
                  </div>
                  {/* Always occupies one line: the preview refetches on every
                      toggle flip, and a status row that mounts and unmounts
                      would shift everything below it each time. */}
                  <p role="status" className="mt-2 min-h-4 text-xs text-foreground-subtle">
                    {loadingPreview ? "Loading preview..." : preview ? `Previewed ${preview.fetched_comment_count} comment bodies; ${preview.comment_count} comments reported.` : ""}
                  </p>
                  <div data-testid="issue-preview-body" aria-busy={loadingPreview} className="mt-3 min-h-0 flex-1 space-y-4 overflow-y-auto lg:pr-1">
                    <div className="min-w-0 rounded-lg border border-gold/20 bg-gold/5 p-3 text-sm"><MarkdownBlock content={shownIssue.body || "_No issue body._"} /></div>
                    {includeComments && preview?.profile.include_comments ? <CommentPreview comments={shownComments} allComments={previewComments} page={commentPage} pageCount={commentPageCount} preview={preview} onPageChange={setCommentPage} /> : null}
                    {error && (error.scope === "preview" || error.scope === "select") ? <PickerError error={error} onRetry={() => error.scope === "preview" ? setPreviewAttempt((value) => value + 1) : void submit()} /> : null}
                  </div>
                </div>
              ) : <div className="flex h-full min-h-40 items-center justify-center rounded-lg border border-dashed border-border p-6 text-center text-sm text-foreground-muted">Select an issue to preview its bounded import before continuing.</div>}
            </div>
          </div>
      </Modal>
    </>
  );
}

/**
 * The import trigger, or the step that has to happen before importing exists.
 *
 * Offering "Import from GitHub Issue" with nothing connected promises a
 * capability Kingdom does not have yet, and the operator only finds that out
 * two clicks deep. With no tracker configured the trigger becomes the setup
 * step instead. A tracker that is configured but unusable still opens the
 * dialog: the failure there is specific — disabled, unhealthy, credential
 * gone — and the dialog names it, which a generic setup link could not.
 */
function PickerTrigger({
  label,
  compact,
  needsSetup,
  disabled,
  onOpen,
}: {
  label: string;
  compact: boolean;
  needsSetup: boolean;
  disabled: boolean;
  onOpen: () => void;
}) {
  const shape = cn("w-full border-gold/35 bg-gold/5 text-gold hover:bg-gold/10 hover:text-gold sm:w-auto", !compact && "border-dashed");
  const iconClass = compact ? "h-3.5 w-3.5" : "h-4 w-4";
  if (needsSetup) {
    return (
      <Link to="/connections" className={cn(buttonVariants({ variant: "outline", size: compact ? "sm" : "default" }), shape)}>
        <Link2 className={iconClass} />
        Connect an Issue Tracker
      </Link>
    );
  }
  return (
    <Button type="button" variant="outline" size={compact ? "sm" : "default"} disabled={disabled} onClick={onOpen} className={shape}>
      <Github className={iconClass} />
      {label}
    </Button>
  );
}

function ConnectionControls({ loading, loaded, loadError, allConnections, connections, connectionId, onConnectionChange, onRetry }: { loading: boolean; loaded: boolean; loadError?: Omit<RequestError, "scope">; allConnections: Connection[]; connections: Connection[]; connectionId: string; onConnectionChange: (value: string) => void; onRetry: () => void }) {
  if (loading) return <p role="status" className="flex items-center gap-2 text-xs text-foreground-muted"><Loader2 className="h-3.5 w-3.5 animate-spin" />Loading connections...</p>;
  if (loadError) return <div role="alert" className="space-y-2 rounded-md border border-status-failed/30 bg-status-failed/10 p-3 text-xs text-status-failed"><p>Could not load GitHub connections: {loadError.message}</p><Button type="button" size="sm" variant="outline" onClick={onRetry}>Retry Connections</Button></div>;
  if (loaded && allConnections.length === 0) return <ConnectionEmptyState message="No GitHub connection is configured." />;
  if (loaded && connections.length === 0) return <ConnectionEmptyState message="No configured GitHub connection is ready to use. Validate or enable one in Connections." />;
  return (
    <SelectField
      id="github-connection"
      label="Connection"
      value={connectionId}
      placeholder="Select connection"
      options={connections.map((item) => ({ label: item.display_name, value: item.connection_id }))}
      onChange={onConnectionChange}
    />
  );
}

function ConnectionEmptyState({ message }: { message: string }) {
  return <div className="rounded-md border border-border bg-background/60 p-3 text-xs text-foreground-muted"><p>{message}</p><Link to="/connections" className="mt-2 inline-flex items-center gap-1 font-medium text-gold underline">Open Connections</Link></div>;
}

function PickerError({ error, onRetry }: { error: RequestError; onRetry: () => void }) {
  return <div role="alert" className="space-y-2 rounded-md border border-status-failed/30 bg-status-failed/10 p-3 text-xs text-status-failed"><p>{error.message}{error.retryAt ? <> Retry after <time dateTime={error.retryAt}>{new Date(error.retryAt).toLocaleString()}</time>.</> : null} Your current selection and form input were preserved.</p><Button type="button" size="sm" variant="outline" onClick={onRetry}>{error.scope === "select" ? "Try Selection Again" : "Retry"}</Button></div>;
}

function CommentPreview({ comments, allComments, page, pageCount, preview, onPageChange }: { comments: ExternalComment[]; allComments: ExternalComment[]; page: number; pageCount: number; preview: IssuePreview; onPageChange: (page: number) => void }) {
  return <section aria-labelledby="github-comment-preview-heading" className="border-t border-gold/20 pt-3"><div className="flex flex-wrap items-center justify-between gap-2"><h4 id="github-comment-preview-heading" className="text-sm font-semibold">Comment Preview</h4>{allComments.length > 0 ? <p aria-live="polite" className="text-xs text-foreground-subtle">Page {page + 1} of {pageCount}</p> : null}</div>{allComments.length === 0 ? <p role="status" className="mt-2 text-xs text-foreground-muted">No comment bodies are available for this issue.</p> : <ol className="mt-2 space-y-3">{comments.map((comment) => <li key={comment.provider_comment_id}><article className="min-w-0 rounded-md border border-border/80 bg-background/60 p-3"><header className="flex flex-wrap items-center justify-between gap-2 text-xs text-foreground-muted"><span>{comment.author || "Unknown GitHub author"}</span><time dateTime={comment.created_at}>{new Date(comment.created_at).toLocaleString()}</time></header><div className="mt-2 max-h-40 min-w-0 overflow-auto text-sm">{comment.body.trim() ? <MarkdownBlock content={comment.body} /> : <p className="italic text-foreground-muted">Empty comment.</p>}</div>{comment.truncation_reason ? <p className="mt-2 text-xs text-status-warning">This comment was truncated to Kingdom's {comment.truncation_reason === "comment_byte_limit" ? "per-comment" : "aggregate"} preview limit.</p> : null}</article></li>)}</ol>}{preview.omitted_comment_count > 0 ? <p className="mt-2 text-xs text-status-warning">{preview.omitted_comment_count} older comments were omitted by the bounded import.</p> : null}{pageCount > 1 ? <div className="mt-3 flex flex-wrap justify-end gap-2"><Button type="button" variant="outline" size="sm" aria-label="Previous comment page" disabled={page === 0} onClick={() => onPageChange(Math.max(0, page - 1))}>Previous</Button><Button type="button" variant="outline" size="sm" aria-label="Next comment page" disabled={page + 1 >= pageCount} onClick={() => onPageChange(Math.min(pageCount - 1, page + 1))}>Next</Button></div> : null}</section>;
}
