import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ExternalSourceComparison } from "./external-source-comparison";

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function snapshot(snapshotId: string) {
  return {
    snapshot_id: snapshotId,
    source_id: "source-1",
    issue: {
      title: "Issue title",
      body: "Issue body",
      state: "open",
      repository: "owner/repo",
      issue_number: 7,
      canonical_url: "https://github.com/owner/repo/issues/7",
      created_at: "2026-08-01T00:00:00Z",
      updated_at: "2026-08-12T00:00:00Z",
    },
    profile: { include_comments: true },
    provider_updated_at: "2026-08-12T00:00:00Z",
    fetched_at: "2026-08-13T00:00:00Z",
    comment_count: 2,
    fetched_comment_count: 2,
    retained_comment_count: 2,
    retained_comment_bytes: 24,
    omitted_comment_pages: 0,
    comments_truncated: false,
  };
}

function comment(id: string, body: string) {
  return {
    provider_comment_id: id,
    author: "octocat",
    body,
    canonical_url: `https://github.com/owner/repo/issues/7#issuecomment-${id}`,
    created_at: "2026-08-10T00:00:00Z",
    updated_at: "2026-08-11T00:00:00Z",
    original_bytes: body.length,
    retained_bytes: body.length,
  };
}

afterEach(() => vi.unstubAllGlobals());

describe("ExternalSourceComparison", () => {
  it("compares retained comment bodies across every page", async () => {
    const requests: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requests.push(url);

      if (url.endsWith("/external-source-snapshots/baseline")) return json(snapshot("baseline"));
      if (url.endsWith("/external-source-snapshots/candidate")) return json(snapshot("candidate"));
      if (url.endsWith("/external-source-snapshots/baseline/comments")) {
        return json({ items: [comment("1", "same comment")], next_cursor: "baseline-next" });
      }
      if (url.includes("/external-source-snapshots/baseline/comments?") && url.includes("cursor=baseline-next")) {
        return json({ items: [comment("2", "before text")] });
      }
      if (url.endsWith("/external-source-snapshots/candidate/comments")) {
        return json({ items: [comment("1", "same comment"), comment("2", "changed text")] });
      }
      throw new Error(`unexpected request ${url}`);
    }));

    render(<ExternalSourceComparison baselineId="baseline" candidateId="candidate" />);

    expect(await screen.findByText("1 source areas changed")).toBeInTheDocument();
    expect(screen.getByText("Comments")).toBeInTheDocument();
    expect(screen.getByText("2 reported, 2 retained")).toBeInTheDocument();
    expect(screen.getByText(/2 reported, 2 retained; 1 changed/)).toBeInTheDocument();
    await waitFor(() => expect(requests.some((url) => url.includes("cursor=baseline-next"))).toBe(true));
  });
});
