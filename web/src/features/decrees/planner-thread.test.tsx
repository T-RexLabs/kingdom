import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { PlanningSessionDetail } from "@/api/hooks";
import { PlannerThread } from "./planner-thread";

vi.mock("./use-planner-message-pages", () => ({
  usePlannerMessagePages: () => ({
    messages: [],
    firstItemIndex: 0,
    totalCount: 0,
    hasPreviousPage: false,
    isFetchingPreviousPage: false,
    fetchPreviousPage: vi.fn(),
    error: null,
    isLoading: false,
  }),
}));

vi.mock("./planner-transcript", () => ({
  PlannerTranscript: () => <div>Planner transcript</div>,
}));

const session: PlanningSessionDetail = {
  planning_session_id: "planning-session-1",
  acp_session_id: "acp-session-1",
  title: "Planner",
  title_state: "custom",
  auto_title_eligible: false,
  lifecycle_state: "active",
  conversation_state: "ready",
  active_leaf_message_id: "message-1",
  created_at: "2026-08-24T10:00:00Z",
  updated_at: "2026-08-24T10:00:00Z",
  project_root: "/tmp/project",
  model_selection: { model: "github-copilot/gpt-5.4-mini" },
  external_sources: [],
};

describe("PlannerThread ACP query state", () => {
  it("surfaces a query error with retry while leaving a ready composer enabled", () => {
    const retry = vi.fn();
    render(
      <PlannerThread
        session={session}
        operationQueryState={{
          isLoading: false,
          isError: true,
          isRetrying: false,
          error: new Error("connection reset"),
          retry,
        }}
        onSubmitReply={() => {}}
        onRegenerate={() => {}}
        onActivateVariant={() => {}}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Could not refresh planner operation. Kingdom will retry automatically. connection reset",
    );
    expect(screen.getByPlaceholderText("Type your response...")).toBeEnabled();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(retry).toHaveBeenCalledTimes(1);
  });

  it("distinguishes loading from an active retry", () => {
    const { rerender } = render(
      <PlannerThread
        session={{ ...session, conversation_state: "processing" }}
        operationQueryState={{
          isLoading: true,
          isError: false,
          isRetrying: true,
          error: null,
          retry: vi.fn(),
        }}
        onSubmitReply={() => {}}
        onRegenerate={() => {}}
        onActivateVariant={() => {}}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent("Checking planner operation...");

    rerender(
      <PlannerThread
        session={{ ...session, conversation_state: "processing" }}
        operationQueryState={{
          isLoading: false,
          isError: true,
          isRetrying: true,
          error: new Error("offline"),
          retry: vi.fn(),
        }}
        onSubmitReply={() => {}}
        onRegenerate={() => {}}
        onActivateVariant={() => {}}
      />,
    );

    expect(screen.getByRole("button", { name: "Retrying..." })).toBeDisabled();
  });
});
