import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  RunOutput,
  RunPublicationPanel,
  PublicationResults,
} from "./run-output";

const state = vi.hoisted(() => ({
  artifactPages: [] as unknown[],
  artifactQuery: vi.fn(),
  publicationQuery: vi.fn(),
  agentsError: null as Error | null,
  retryAgents: vi.fn(),
  publicationPages: [] as unknown[],
}));
vi.mock("@/theme", () => ({
  useTheme: () => ({ theme: {} }),
  roleIdentity: (_theme: unknown, role: string) => ({
    name: role === "builder" ? "Builder" : "Reviewer",
  }),
}));
vi.mock("react-virtuoso", () => ({
  Virtuoso: ({
    data,
    itemContent,
  }: {
    data: unknown[];
    itemContent: (index: number, item: unknown) => ReactNode;
  }) => (
    <div>
      {data.map((item, index) => (
        <div key={index}>{itemContent(index, item)}</div>
      ))}
    </div>
  ),
}));
vi.mock("@/api/hooks-run-output", () => ({
  useRunArtifactAgents: () => ({
    data: {
      items: [
        { agent_id: "builder-1", role_id: "builder", display_name: "Builder" },
        {
          agent_id: "reviewer-1",
          role_id: "execution-reviewer",
          display_name: "Reviewer",
        },
      ],
    },
    isError: !!state.agentsError,
    error: state.agentsError,
    refetch: state.retryAgents,
  }),
  useRunArtifacts: (...args: unknown[]) => {
    state.artifactQuery(...args);
    return {
      data: { pages: state.artifactPages },
      refresh: vi.fn(),
      hasNextPage: false,
    };
  },
  useRunPublicationEntries: (...args: unknown[]) => {
    state.publicationQuery(...args);
    return {
      data: { pages: state.publicationPages },
      refresh: vi.fn(),
      hasNextPage: false,
    };
  },
  outputSnapshotChanged: () => false,
}));
beforeEach(() => {
  vi.clearAllMocks();
  state.agentsError = null;
  state.artifactPages = [];
  state.publicationPages = [];
});

const file = {
  artifact_id: "file",
  artifact_action: "write",
  lineage: "canonical_deliverable",
  path: "src/app.ts",
  name: "app.ts",
  attempt: 2,
  attempt_id: "a2",
  producer_role: "builder",
  size_bytes: 7,
  created_at: "2026-09-29T10:00:00Z",
  download_url: "/api/v1/artifacts/file/content",
};

describe("run output evidence", () => {
  it("distinguishes canonical deliverables, audit evidence and delete actions", () => {
    state.artifactPages = [
      {
        snapshot_id: "s",
        items: [
          file,
          {
            ...file,
            artifact_id: "delete",
            path: "old.ts",
            lineage: "audit",
            artifact_action: "delete",
            download_url: undefined,
          },
        ],
      },
    ];
    render(<RunOutput runId="r" />);
    expect(screen.getByText("Canonical Deliverable")).toBeInTheDocument();
    expect(screen.getByText("Audit Evidence")).toBeInTheDocument();
    expect(screen.getByText(/Delete Action/)).toBeInTheDocument();
    expect(
      screen.getAllByRole("link", { name: "Download src/app.ts" }),
    ).toHaveLength(1);
    expect(screen.getByRole("link", { name: "Download All" })).toHaveAttribute(
      "href",
      "/api/v1/runs/r/artifacts/archive",
    );
  });
  it("never renders artifact pages from mixed canonical snapshots", () => {
    state.artifactPages = [
      { snapshot_id: "old", items: [file] },
      { snapshot_id: "new", items: [file] },
    ];
    render(<RunOutput runId="r" />);
    expect(screen.queryByText("src/app.ts")).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Refresh Results" }),
    ).toBeInTheDocument();
  });
  it("requires summary refresh if a publication changed before its first page arrived", () => {
    state.publicationPages = [
      {
        entries_revision: 3,
        items: [
          {
            entry_id: "entry",
            path: "new.ts",
            action: "write",
            status: "published",
          },
        ],
      },
    ];
    render(
      <PublicationResults
        runId="r"
        attemptId="a2"
        publication={{
          batch_id: "batch",
          entries_revision: 2,
          target_mode: "direct",
          status: "partial_conflict",
          published_count: 1,
          conflicted_count: 1,
          failed_count: 0,
          created_at: "2026-09-29T10:00:00Z",
        }}
        onRefreshSummary={vi.fn()}
      />,
    );
    expect(screen.queryByText("new.ts")).not.toBeInTheDocument();
    expect(screen.getByText("Partial Conflict")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Refresh Results" }),
    ).toBeInTheDocument();
  });
  it("defaults to All and queries the selected producer independently of execution selection", async () => {
    render(<RunOutput runId="r" />);
    expect(
      screen.getByRole("button", { name: "Filter files by agent" }),
    ).toHaveTextContent("All");
    expect(state.artifactQuery).toHaveBeenLastCalledWith("r", "");
    await userEvent.click(
      screen.getByRole("button", { name: "Filter files by agent" }),
    );
    await userEvent.click(screen.getByRole("option", { name: "Builder" }));
    expect(state.artifactQuery).toHaveBeenLastCalledWith("r", "builder-1");
    expect(
      screen.getByRole("link", { name: "Download All Run Files" }),
    ).toHaveAttribute("href", "/api/v1/runs/r/artifacts/archive");
    expect(
      screen.getByText("No files have been recorded by this agent."),
    ).toBeVisible();
    await userEvent.click(
      screen.getByRole("button", { name: "Filter files by agent" }),
    );
    await userEvent.click(screen.getByRole("option", { name: "All" }));
    expect(state.artifactQuery).toHaveBeenLastCalledWith("r", "");
  });

  it("keeps exact downloads and readable lineage without raw provenance cards", () => {
    state.artifactPages = [{ snapshot_id: "s", items: [file] }];
    render(<RunOutput runId="r" />);
    const row = screen.getByRole("article");
    expect(within(row).getByText("Canonical Deliverable")).toBeVisible();
    expect(within(row).getByText(/Builder/)).toBeVisible();
    expect(
      within(row).queryByText(/Attempt|Provenance|Artifact ID|SHA-256/),
    ).not.toBeInTheDocument();
    expect(
      within(row).getByRole("link", { name: "Download src/app.ts" }),
    ).toHaveAttribute("href", file.download_url);
  });

  it("keeps publication discovery separate and does not request file results before opening it", async () => {
    const publication = {
      batch_id: "batch",
      entries_revision: 2,
      target_mode: "direct" as const,
      status: "succeeded" as const,
      published_count: 1,
      conflicted_count: 0,
      failed_count: 0,
      created_at: "2026-09-29T10:00:00Z",
    };
    const { rerender } = render(
      <RunPublicationPanel
        runId="r"
        picker={<div>Exact Attempt Picker</div>}
        isLoading={false}
        error={null}
        onRefresh={vi.fn()}
      />,
    );
    expect(screen.queryByText("Exact Attempt Picker")).not.toBeInTheDocument();
    expect(state.publicationQuery).not.toHaveBeenCalled();
    await userEvent.click(
      screen.getByRole("button", { name: "Publication Results" }),
    );
    expect(screen.getByText("Exact Attempt Picker")).toBeVisible();
    expect(
      screen.getByText("No publication results are recorded for this attempt."),
    ).toBeVisible();
    rerender(
      <RunPublicationPanel
        runId="r"
        attemptId="a2"
        publication={publication}
        picker={<div>Exact Attempt Picker</div>}
        isLoading={false}
        error={null}
        onRefresh={vi.fn()}
      />,
    );
    expect(state.publicationQuery).toHaveBeenLastCalledWith(
      "r",
      "a2",
      publication,
    );
  });

  it("keeps participant lookup errors actionable without claiming the file list is empty", async () => {
    state.agentsError = new Error("Agents could not be loaded.");
    state.artifactPages = [{ snapshot_id: "s", items: [file] }];
    render(<RunOutput runId="r" />);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Agents could not be loaded.",
    );
    expect(screen.getByText("src/app.ts")).toBeVisible();
    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(state.retryAgents).toHaveBeenCalledOnce();
  });
});
