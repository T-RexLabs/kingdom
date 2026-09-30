import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { intersectAllObservers } from "@/test/setup";
import type { ReactElement, ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { IssuePicker } from "./issue-picker";

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, to }: { children: ReactNode; to: string }) => <a href={to}>{children}</a>,
}));

// A fresh client per test: connections are cached across the picker's surfaces
// in the app, so a shared client here would let one test's list satisfy the
// next test's query and hide a request that should have been made.
function renderPicker(ui: ReactElement) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>);
}

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "Content-Type": "application/json" } });
}

const issue = (number: number, title: string) => ({
  native_id: String(100 + number),
  comment_count: 2,
  issue: {
    title,
    body: `${title} body`,
    state: "open",
    repository: "owner/repo",
    issue_number: number,
    canonical_url: `https://github.com/owner/repo/issues/${number}`,
    created_at: "2026-08-01T00:00:00Z",
    updated_at: "2026-08-12T00:00:00Z",
  },
});

const readyConnection = {
  connection_id: "conn",
  provider: "github",
  display_name: "App",
  app_id: "1",
  client_id: "Iv1",
  installation_id: "2",
  lifecycle: "enabled",
  health_class: "healthy",
  credential_state: "present",
  created_at: "2026-08-01T00:00:00Z",
  updated_at: "2026-08-01T00:00:00Z",
};

function deferredResponse() {
  let resolve!: (response: Response) => void;
  const promise = new Promise<Response>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

afterEach(() => vi.unstubAllGlobals());

/**
 * Dropdowns are Kingdom's own listbox, not a native <select>: open the trigger,
 * then pick the option.
 */
const chooseOption = async (
  user: ReturnType<typeof userEvent.setup>,
  triggerLabel: string,
  optionLabel: string | RegExp,
) => {
  await user.click(await screen.findByLabelText(triggerLabel));
  await user.click(await screen.findByRole("option", { name: optionLabel }));
};

// The repository list loads the next page when its end scrolls into view.
// jsdom has no layout, so the observer is driven deliberately instead.
async function scrollRepositoriesToEnd() {
	await act(async () => {
		intersectAllObservers();
	});
}

describe("IssuePicker", () => {
  it("does not offer a nominally healthy connection whose credential is missing", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ items: [{ connection_id: "stale", provider: "github", display_name: "Stale", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "missing", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] })));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);
    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    expect(await screen.findByText(/no configured GitHub connection is ready to use/i)).toBeInTheDocument();
  });

  it("paginates repositories and issues and uses the profile-aware preview", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requests.push(url);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.includes("/repositories?") && url.includes("cursor=")) return json({ items: [{ id: "repo-2", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] });
      if (url.endsWith("/repositories")) return json({ items: [{ id: "repo-1", locator: "owner/first", canonical_url: "https://github.com/owner/first" }], next_cursor: "repo-next" });
      if (url.includes("/issues/2/preview")) return json({ native_id: "102", repository_id: "repo-2", issue: issue(2, "Second").issue, profile: { include_comments: true }, comment_count: 2, comments: [], fetched_comment_count: 2, etag_present: true });
      if (url.includes("/issues?") && url.includes("cursor=")) return json({ items: [issue(2, "Second")], incomplete_results: true, search_result_cap: 1000 });
      if (url.includes("/issues?")) return json({ items: [issue(1, "First")], next_cursor: "issue-next", incomplete_results: false, search_result_cap: 1000 });
      throw new Error(`unexpected request ${url}`);
    }));
    const onSelect = vi.fn();
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={onSelect} actionLabel="Prefill WorkOrder" />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await user.click(await screen.findByLabelText("GitHub repository"));
    await scrollRepositoriesToEnd();
    await user.click(await screen.findByRole("option", { name: "owner/repo" }));
    await screen.findByText("First");
    await user.click(screen.getByRole("button", { name: /load more issues/i }));
    await user.click(await screen.findByRole("button", { name: /second/i }));
    await user.click(screen.getByRole("switch", { name: /include comments/i }));
    await waitFor(() => expect(requests.some((url) => url.includes("/issues/2/preview") && url.includes("include_comments=true") )).toBe(true));
    expect(await screen.findByText(/partial search results/i)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Prefill WorkOrder" }));
    expect(onSelect).toHaveBeenCalledWith(expect.objectContaining({ intake: expect.objectContaining({ repository_id: "repo-2", issue_number: 2, include_comments: true }) }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Import from GitHub Issues" })).not.toBeInTheDocument());
  });

  it("renders sanitized bounded comment bodies with accessible pagination", async () => {
    const comments = Array.from({ length: 6 }, (_, index) => ({
      provider_comment_id: `comment-${index + 1}`,
      author: `author-${index + 1}`,
      body: index === 0 ? "**First comment** <script>alert('no')</script> ![tracker](https://tracker.example/pixel.png)" : `Comment body ${index + 1}`,
      canonical_url: `https://github.com/owner/repo/issues/1#issuecomment-${index + 1}`,
      created_at: `2026-08-0${index + 1}T00:00:00Z`,
      updated_at: `2026-08-0${index + 1}T00:00:00Z`,
      original_bytes: 20,
      retained_bytes: 20,
      truncation_reason: index === 5 ? "aggregate_byte_limit" : undefined,
    }));
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.endsWith("/repositories")) return json({ items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] });
      if (url.includes("/issues/1/preview")) return json({ native_id: "101", repository_id: "repo", issue: issue(1, "First").issue, profile: { include_comments: url.includes("include_comments=true") }, comment_count: 9, comments: url.includes("include_comments=true") ? comments : [], fetched_comment_count: url.includes("include_comments=true") ? 6 : 0, fetched_comment_pages: 1, omitted_comment_count: 3, omitted_comment_pages: 1, etag_present: true });
      if (url.includes("/issues?")) return json({ items: [issue(1, "First")], incomplete_results: false, search_result_cap: 1000 });
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    const { container } = renderPicker(<IssuePicker onSelect={() => undefined} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await user.click(await screen.findByRole("button", { name: /first/i }));
    expect(screen.queryByRole("heading", { name: /comment preview/i })).not.toBeInTheDocument();
    await user.click(screen.getByRole("switch", { name: /include comments/i }));

    expect(await screen.findByRole("heading", { name: /comment preview/i })).toBeInTheDocument();
    expect(screen.getByText("author-1")).toBeInTheDocument();
    expect(screen.getByText("author-5")).toBeInTheDocument();
    expect(screen.queryByText("author-6")).not.toBeInTheDocument();
    expect(screen.getByText(/3 older comments were omitted/i)).toBeInTheDocument();
    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    expect(screen.getByText(/remote image blocked/i)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /next comment page/i }));
    expect(screen.getByText("author-6")).toBeInTheDocument();
    expect(screen.queryByText("author-1")).not.toBeInTheDocument();
    expect(screen.getByText(/aggregate preview limit/i)).toBeInTheDocument();
    expect(screen.getByText(/page 2 of 2/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /next comment page/i })).toBeDisabled();
  });

  it("shows the rate-limit reset time without losing the selected issue", async () => {
    const retryAt = "2026-08-14T10:45:00Z";
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.endsWith("/repositories")) return json({ items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] });
      if (url.includes("/issues/1/preview")) return new Response(JSON.stringify({ error: { code: "dependency_blocked", message: "GitHub rate limit reached", retryable: false, category: "core", details: {}, correlation_id: "test", occurred_at: "2026-08-14T10:40:00Z", retry_at: retryAt } }), { status: 429, headers: { "Content-Type": "application/json" } });
      if (url.includes("/issues?")) return json({ items: [issue(1, "Selected issue")], incomplete_results: false, search_result_cap: 1000 });
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await user.click(await screen.findByRole("button", { name: /selected issue/i }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent(/retry after/i);
    expect(alert.querySelector("time")).toHaveAttribute("datetime", retryAt);
    expect(screen.getAllByText("Selected issue").length).toBeGreaterThan(0);
  });

  it("aborts a stale issue request when the search changes", async () => {
    let staleSignal: AbortSignal | undefined;
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections")) return Promise.resolve(json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] }));
      if (url.endsWith("/repositories")) return Promise.resolve(json({ items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] }));
      if (url.includes("search=new")) return Promise.resolve(json({ items: [], incomplete_results: false, search_result_cap: 1000 }));
      if (url.includes("/issues?")) {
        staleSignal = init?.signal ?? undefined;
        return new Promise<Response>((_resolve, reject) => staleSignal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError"))));
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);
    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await waitFor(() => expect(staleSignal).toBeDefined());
    await user.type(screen.getByLabelText("Search issues"), "new");
    await waitFor(() => expect(staleSignal?.aborted).toBe(true));
  });

  it("does not append delayed repository pagination after the filter changes", async () => {
    const stalePage = deferredResponse();
    let staleSignal: AbortSignal | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections"))
          return Promise.resolve(json({ items: [readyConnection] }));
        if (url.includes("cursor=repo-next")) {
          staleSignal = init?.signal ?? undefined;
          return stalePage.promise;
        }
        if (url.includes("search=fresh"))
          return Promise.resolve(
            json({
              items: [
                {
                  id: "repo-fresh",
                  locator: "owner/fresh",
                  canonical_url: "https://github.com/owner/fresh",
                },
              ],
            }),
          );
        if (url.endsWith("/repositories"))
          return Promise.resolve(
            json({
              items: [
                {
                  id: "repo-old",
                  locator: "owner/old",
                  canonical_url: "https://github.com/owner/old",
                },
              ],
              next_cursor: "repo-next",
            }),
          );
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await user.click(await screen.findByLabelText("GitHub repository"));
    await scrollRepositoriesToEnd();
    await user.type(await screen.findByLabelText("Filter owner/name"), "fresh");
    await waitFor(() => expect(staleSignal?.aborted).toBe(true));
    await act(async () => {
      stalePage.resolve(
        json({
          items: [
            {
              id: "repo-stale",
              locator: "owner/stale",
              canonical_url: "https://github.com/owner/stale",
            },
          ],
          next_cursor: "stale-cursor",
        }),
      );
      await stalePage.promise;
    });

    expect(await screen.findByRole("option", { name: "owner/fresh" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "owner/stale" })).not.toBeInTheDocument();
    expect(screen.queryByText(/loading more repositories/i)).not.toBeInTheDocument();
  });

  it.each(["search", "state", "repository"] as const)(
    "does not append delayed issue pagination after the %s changes",
    async (scopeChange) => {
      const stalePage = deferredResponse();
      let staleSignal: AbortSignal | undefined;
      vi.stubGlobal(
        "fetch",
        vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
          const url = String(input);
          if (url.endsWith("/connections"))
            return Promise.resolve(json({ items: [readyConnection] }));
          if (url.endsWith("/repositories"))
            return Promise.resolve(
              json({
                items: [
                  { id: "repo-a", locator: "owner/a", canonical_url: "https://github.com/owner/a" },
                  { id: "repo-b", locator: "owner/b", canonical_url: "https://github.com/owner/b" },
                ],
              }),
            );
          if (url.includes("cursor=issue-next")) {
            staleSignal = init?.signal ?? undefined;
            return stalePage.promise;
          }
          if (url.includes("/repo-b/issues?"))
            return Promise.resolve(
              json({ items: [issue(4, "Fresh repository issue")], incomplete_results: false }),
            );
          if (url.includes("search=fresh"))
            return Promise.resolve(
              json({ items: [issue(3, "Fresh search issue")], incomplete_results: false }),
            );
          if (url.includes("state=closed"))
            return Promise.resolve(
              json({ items: [issue(2, "Fresh closed issue")], incomplete_results: false }),
            );
          if (url.includes("/repo-a/issues?"))
            return Promise.resolve(
              json({
                items: [issue(1, "Initial issue")],
                next_cursor: "issue-next",
                incomplete_results: false,
              }),
            );
          throw new Error(`unexpected request ${url}`);
        }),
      );
      const user = userEvent.setup();
      renderPicker(<IssuePicker onSelect={() => undefined} />);

      await user.click(screen.getByRole("button", { name: /import from github issue/i }));
      await chooseOption(user, "Connection", "App");
      await chooseOption(user, "GitHub repository", "owner/a");
      await user.click(await screen.findByRole("button", { name: /load more issues/i }));
      if (scopeChange === "search")
        await user.type(screen.getByLabelText("Search issues"), "fresh");
      else if (scopeChange === "state")
        await chooseOption(user, "Issue State", /^Closed$/);
      else
        await chooseOption(user, "GitHub repository", "owner/b");
      await waitFor(() => expect(staleSignal?.aborted).toBe(true));
      await act(async () => {
        stalePage.resolve(
          json({
            items: [issue(99, "Stale paginated issue")],
            next_cursor: "stale-cursor",
            incomplete_results: true,
          }),
        );
        await stalePage.promise;
      });

      const expectedTitle =
        scopeChange === "search"
          ? "Fresh search issue"
          : scopeChange === "state"
            ? "Fresh closed issue"
            : "Fresh repository issue";
      expect(await screen.findByText(expectedTitle)).toBeInTheDocument();
      expect(screen.queryByText("Stale paginated issue")).not.toBeInTheDocument();
      expect(screen.queryByText(/partial search results/i)).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: /load more issues/i })).not.toBeInTheDocument();
    },
  );

  it("does not append delayed issue pagination after the picker closes", async () => {
    const stalePage = deferredResponse();
    let staleSignal: AbortSignal | undefined;
    let initialIssueLoads = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections"))
          return Promise.resolve(json({ items: [readyConnection] }));
        if (url.endsWith("/repositories"))
          return Promise.resolve(
            json({
              items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }],
            }),
          );
        if (url.includes("cursor=issue-next")) {
          staleSignal = init?.signal ?? undefined;
          return stalePage.promise;
        }
        if (url.includes("/issues?")) {
          initialIssueLoads += 1;
          return Promise.resolve(
            json({
              items: [issue(1, "Current issue")],
              next_cursor: initialIssueLoads === 1 ? "issue-next" : undefined,
              incomplete_results: false,
            }),
          );
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await user.click(await screen.findByRole("button", { name: /load more issues/i }));
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(staleSignal?.aborted).toBe(true));
    await act(async () => {
      stalePage.resolve(
        json({
          items: [issue(99, "Stale closed issue")],
          next_cursor: "stale-cursor",
          incomplete_results: true,
        }),
      );
      await stalePage.promise;
    });

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    expect(await screen.findByText("Current issue")).toBeInTheDocument();
    expect(screen.queryByText("Stale closed issue")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /load more issues/i })).not.toBeInTheDocument();
  });

  it("filters repositories and preserves the filter while paging", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requests.push(url);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.includes("search=widget") && url.includes("cursor=")) return json({ items: [{ id: "repo-2", locator: "owner/widget-two", canonical_url: "https://github.com/owner/widget-two" }] });
      if (url.includes("search=widget")) return json({ items: [{ id: "repo-1", locator: "owner/widget-one", canonical_url: "https://github.com/owner/widget-one" }], next_cursor: "next" });
      if (url.endsWith("/repositories")) return json({ items: [] });
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);
    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await user.click(await screen.findByLabelText("GitHub repository"));
    await user.type(await screen.findByLabelText("Filter owner/name"), "widget");
    await screen.findByRole("option", { name: "owner/widget-one" });
    await scrollRepositoriesToEnd();
    await screen.findByRole("option", { name: "owner/widget-two" });
    expect(requests.some((url) => url.includes("search=widget") && url.includes("cursor=next"))).toBe(true);
  });

  it("exposes exactly one comment switch and always captures the count", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requests.push(url);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.endsWith("/repositories")) return json({ items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] });
      if (url.includes("/issues/1/preview")) return json({ native_id: "101", repository_id: "repo", issue: issue(1, "First").issue, profile: { include_comments: url.includes("include_comments=true") }, comment_count: 4, comments: [], fetched_comment_count: 0, fetched_comment_pages: 0, omitted_comment_count: 0, omitted_comment_pages: 0, etag_present: true });
      if (url.includes("/issues?")) return json({ items: [issue(1, "First")], incomplete_results: false, search_result_cap: 1000 });
      throw new Error(`unexpected request ${url}`);
    }));
    const onSelect = vi.fn();
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={onSelect} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await user.click(await screen.findByRole("button", { name: /first/i }));

    // One switch, defaulting to off: comment bodies are the only real choice.
    await waitFor(() => expect(screen.getAllByRole("switch")).toHaveLength(1));
    expect(screen.getByRole("switch", { name: /include comments/i })).not.toBeChecked();

    // The count rides along on the issue payload, so it is never negotiated.
    await waitFor(() => expect(requests.some((url) => url.includes("/issues/1/preview"))).toBe(true));
    expect(requests.every((url) => !url.includes("include_comment_count"))).toBe(true);
    expect(await screen.findByText(/4 comments reported/i)).toBeInTheDocument();

    await user.click(await screen.findByRole("button", { name: "Use Issue" }));
    expect(onSelect).toHaveBeenCalledWith(expect.objectContaining({ intake: expect.objectContaining({ include_comments: false }) }));
    expect(onSelect.mock.calls[0][0].intake).not.toHaveProperty("include_comment_count");
  });

  it("keeps the modal selection and error open until onSelect succeeds", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.endsWith("/repositories")) return json({ items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] });
      if (url.includes("/issues/1/preview")) return json({ native_id: "101", repository_id: "repo", issue: issue(1, "Selected issue").issue, profile: { include_comments: false }, comment_count: 2, comments: [], fetched_comment_count: 0, fetched_comment_pages: 0, omitted_comment_count: 0, omitted_comment_pages: 0, etag_present: true });
      if (url.includes("/issues?")) return json({ items: [issue(1, "Selected issue")], incomplete_results: false, search_result_cap: 1000 });
      throw new Error(`unexpected request ${url}`);
    }));
    const onSelect = vi.fn().mockRejectedValueOnce(new Error("Capture failed")).mockResolvedValueOnce(undefined);
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={onSelect} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await user.click(await screen.findByRole("button", { name: /selected issue/i }));
    await user.click(await screen.findByRole("button", { name: "Use Issue" }));

    expect(await screen.findByText(/capture failed/i)).toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "Import from GitHub Issues" })).toBeInTheDocument();
    expect(screen.getAllByText("Selected issue").length).toBeGreaterThan(0);

    await user.click(screen.getByRole("button", { name: "Try Selection Again" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Import from GitHub Issues" })).not.toBeInTheDocument());
  });

  it("distinguishes connection load failure from no configured connection and offers retry", async () => {
    let reachable = false;
    vi.stubGlobal("fetch", vi.fn(async () => {
      if (reachable) return json({ items: [] });
      return new Response(JSON.stringify({ error: { code: "internal_error", message: "Connection service unavailable", retryable: true, category: "core", details: {}, correlation_id: "test", occurred_at: "2026-08-14T10:40:00Z" } }), { status: 503, headers: { "Content-Type": "application/json" } });
    }));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    // An unreachable list is not an empty list. Readiness is unknown, so the
    // import stays on offer and the dialog reports what actually went wrong.
    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    expect(await screen.findByText(/could not load GitHub connections/i)).toBeInTheDocument();
    expect(screen.queryByText(/no GitHub connection is configured/i)).not.toBeInTheDocument();
    reachable = true;
    await user.click(screen.getByRole("button", { name: "Retry Connections" }));
    expect(await screen.findByText(/no GitHub connection is configured/i)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /open connections/i })).toHaveAttribute("href", "/connections");
  });

  it("offers connection setup instead of an import the operator cannot complete", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ items: [] })));
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    // Nothing is connected, so importing is not a capability Kingdom has yet.
    // Offering it anyway sends the operator into a dialog that can only tell
    // them to leave again.
    expect(await screen.findByRole("link", { name: "Connect an Issue Tracker" })).toHaveAttribute("href", "/connections");
    expect(screen.queryByRole("button", { name: /import from github issue/i })).not.toBeInTheDocument();
  });

  it("offers the import once a usable connection exists", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ items: [readyConnection] })));
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    await waitFor(() => expect(screen.getByRole("button", { name: /import from github issue/i })).toBeEnabled());
    expect(screen.queryByRole("link", { name: "Connect an Issue Tracker" })).not.toBeInTheDocument();
  });

  it("uses a mobile-stacked layout that becomes two panes without fixed-width overflow", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({ items: [readyConnection] })));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    const layout = screen.getByTestId("issue-picker-layout");
    expect(layout).toHaveClass("grid-cols-1", "min-w-0");
    expect(layout.className).toContain("lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]");
    expect(screen.getByRole("dialog", { name: "Import from GitHub Issues" })).toHaveClass("min-w-0");
  });

  it("keeps the panel a fixed size while sections and toggles change", async () => {
    const comments = Array.from({ length: 6 }, (_, index) => ({
      provider_comment_id: `comment-${index + 1}`,
      author: `author-${index + 1}`,
      body: `Comment body ${index + 1}`,
      canonical_url: `https://github.com/owner/repo/issues/1#issuecomment-${index + 1}`,
      created_at: `2026-08-0${index + 1}T00:00:00Z`,
      updated_at: `2026-08-0${index + 1}T00:00:00Z`,
      original_bytes: 20,
      retained_bytes: 20,
    }));
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.endsWith("/repositories")) return json({ items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] });
      if (url.includes("/issues/1/preview")) return json({ native_id: "101", repository_id: "repo", issue: issue(1, "First").issue, profile: { include_comments: url.includes("include_comments=true") }, comment_count: 9, comments: url.includes("include_comments=true") ? comments : [], fetched_comment_count: url.includes("include_comments=true") ? 6 : 0, fetched_comment_pages: 1, omitted_comment_count: 3, omitted_comment_pages: 1, etag_present: true });
      if (url.includes("/issues?")) return json({ items: [issue(1, "First")], incomplete_results: false, search_result_cap: 1000 });
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));

    // The shell commits to a definite height rather than shrink-wrapping, so
    // mounting the repository field, the issue list, the preview pane and the
    // comment preview cannot resize or re-centre the dialog.
    const panel = screen.getByTestId("issue-picker-layout").closest("div[class*='rounded-2xl']");
    expect(panel?.className).toContain("h-[min(85vh,44rem)]");
    expect(panel?.className).not.toContain("max-h-[85vh]");

    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await user.click(await screen.findByRole("button", { name: /first/i }));
    await screen.findByText(/9 comments reported/i);

    // The status line is always mounted; the preview refetches on every flip,
    // so a row that came and went would shift the pane each time.
    const status = screen.getByRole("status");
    await user.click(screen.getByRole("switch", { name: /include comments/i }));
    expect(await screen.findByRole("heading", { name: /comment preview/i })).toBeInTheDocument();
    expect(screen.getByRole("status")).toBe(status);
    await user.click(screen.getByRole("switch", { name: /include comments/i }));
    await waitFor(() => expect(screen.queryByRole("heading", { name: /comment preview/i })).not.toBeInTheDocument());
    expect(screen.getByRole("status")).toBe(status);
    expect(panel?.className).toContain("h-[min(85vh,44rem)]");
  });

  it("keeps the issue preview scrollable instead of clipping a long body", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/connections")) return json({ items: [{ connection_id: "conn", provider: "github", display_name: "App", app_id: "1", client_id: "Iv1", installation_id: "2", lifecycle: "enabled", health_class: "healthy", credential_state: "present", created_at: "2026-08-01T00:00:00Z", updated_at: "2026-08-01T00:00:00Z" }] });
      if (url.endsWith("/repositories")) return json({ items: [{ id: "repo", locator: "owner/repo", canonical_url: "https://github.com/owner/repo" }] });
      if (url.includes("/issues/1/preview")) return json({ native_id: "101", repository_id: "repo", issue: issue(1, "First").issue, profile: { include_comments: false }, comment_count: 0, comments: [], fetched_comment_count: 0, fetched_comment_pages: 0, omitted_comment_count: 0, omitted_comment_pages: 0, etag_present: true });
      if (url.includes("/issues?")) return json({ items: [issue(1, "First")], incomplete_results: false, search_result_cap: 1000 });
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    renderPicker(<IssuePicker onSelect={() => undefined} />);

    await user.click(screen.getByRole("button", { name: /import from github issue/i }));
    await chooseOption(user, "Connection", "App");
    await chooseOption(user, "GitHub repository", "owner/repo");
    await user.click(await screen.findByRole("button", { name: /first/i }));

    // The scroller can only bound itself if every flex ancestor between it and
    // the pane that clips them is shrinkable. Miss one `min-h-0` and the
    // automatic minimum size pins the column to its content height, so the
    // body silently overflows the pane instead of scrolling inside it.
    const scroller = await screen.findByTestId("issue-preview-body");
    expect(scroller).toHaveClass("min-h-0", "flex-1", "overflow-y-auto");
    for (let node = scroller.parentElement; node && !node.dataset.testid; node = node.parentElement) {
      expect(node.className).toMatch(/min-h-0|overflow-hidden/);
    }
  });
});
