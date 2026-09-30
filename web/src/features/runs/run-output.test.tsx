import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { RunOutput } from "./run-output";

const state = vi.hoisted(() => ({
  artifactPages: [] as unknown[],
  publicationPages: [] as unknown[],
}));
vi.mock("@/theme", () => ({
  RoleIdentity: ({ role }: { role: string }) => <span>{role}</span>,
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
  useRunArtifacts: () => ({
    data: { pages: state.artifactPages },
    refresh: vi.fn(),
    hasNextPage: false,
  }),
  useRunPublicationEntries: () => ({
    data: { pages: state.publicationPages },
    refresh: vi.fn(),
    hasNextPage: false,
  }),
  outputSnapshotChanged: () => false,
}));
beforeEach(() => {
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
    render(<RunOutput runId="r" onRefreshPublication={vi.fn()} />);
    expect(screen.getByText("Canonical Deliverable")).toBeInTheDocument();
    expect(screen.getByText("Audit Evidence")).toBeInTheDocument();
    expect(screen.getByText(/Delete Action/)).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: "Download File" })).toHaveLength(
      1,
    );
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
    render(<RunOutput runId="r" onRefreshPublication={vi.fn()} />);
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
      <RunOutput
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
        onRefreshPublication={vi.fn()}
      />,
    );
    expect(screen.queryByText("new.ts")).not.toBeInTheDocument();
    expect(screen.getByText("Partial Conflict")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Refresh Results" }),
    ).toBeInTheDocument();
  });
});
