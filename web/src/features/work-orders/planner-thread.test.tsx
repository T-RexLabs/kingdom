import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import type { PlanningSessionDetail } from "@/api/hooks";
import { PlannerThread } from "./planner-thread";
import type { PlannerOperationState } from "./planner-operation-controller";

beforeEach(() => sessionStorage.clear());
const renderThread = (ui: ReactNode) => {
  const client = new QueryClient();
  return render(ui, { wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider> });
};

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
    renderThread(
      <PlannerThread
        session={session}
        replyRecoveryPending={false}
        onRetryReplyRecovery={() => {}}
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
    const { rerender } = renderThread(
      <PlannerThread
        session={{ ...session, conversation_state: "processing" }}
        replyRecoveryPending={false}
        onRetryReplyRecovery={() => {}}
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
        replyRecoveryPending={false}
        onRetryReplyRecovery={() => {}}
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

  it("keeps a new draft while explicitly retrying a retained reply", async () => {
    const onSubmitReply = vi.fn().mockResolvedValue(undefined);
    const onRetryReplyRecovery = vi.fn().mockResolvedValue(undefined);
    const { rerender } = renderThread(
      <PlannerThread
        session={session}
        replyRecoveryPending
        onRetryReplyRecovery={onRetryReplyRecovery}
        onSubmitReply={onSubmitReply}
        onRegenerate={() => {}}
        onActivateVariant={() => {}}
      />,
    );

    const composer = screen.getByPlaceholderText("Type your response...");
    fireEvent.change(composer, { target: { value: "Reply B" } });

    expect(composer).toBeEnabled();
    expect(composer).toHaveValue("Reply B");
    expect(screen.getByRole("status")).toHaveTextContent(
      "A previous reply could not be confirmed. Retry it with the original request and idempotency key before sending a different reply. Your current draft will be preserved.",
    );
    expect(screen.getByRole("button", { name: "Send" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    fireEvent.keyDown(composer, { key: "Enter" });
    expect(onSubmitReply).not.toHaveBeenCalled();
    expect(composer).toHaveValue("Reply B");

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Retry Previous Reply" }));
      await onRetryReplyRecovery.mock.results[0]?.value;
    });
    expect(onRetryReplyRecovery).toHaveBeenCalledTimes(1);
    expect(composer).toHaveValue("Reply B");

    rerender(
      <PlannerThread
        session={session}
        replyRecoveryPending={false}
        onRetryReplyRecovery={onRetryReplyRecovery}
        onSubmitReply={onSubmitReply}
        onRegenerate={() => {}}
        onActivateVariant={() => {}}
      />,
    );

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Send" }));
    });
    await waitFor(() => expect(onSubmitReply).toHaveBeenCalledWith({
      text: "Reply B",
      answer: { text: "Reply B" },
      draft: { branch: "message-1", generation: 1, kind: "composer" },
    }));
    expect(composer).toHaveValue("");
  });
});


describe("PlannerThread session ownership", () => {
  it("keeps drafts separate when switching sessions and branches", () => {
    const props = { replyRecoveryPending: false, onRetryReplyRecovery: vi.fn(), onSubmitReply: vi.fn(), onRegenerate: vi.fn(), onActivateVariant: vi.fn() };
    const view = renderThread(<PlannerThread {...props} session={session} />);
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "Session A draft" } });
    view.rerender(<PlannerThread {...props} session={{ ...session, planning_session_id: "session-b" }} />);
    expect(screen.getByRole("textbox")).toHaveValue("");
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "Session B draft" } });
    view.rerender(<PlannerThread {...props} session={session} />);
    expect(screen.getByRole("textbox")).toHaveValue("Session A draft");
    view.rerender(<PlannerThread {...props} session={{ ...session, active_leaf_message_id: "other-branch" }} />);
    expect(screen.getByRole("textbox")).toHaveValue("");
    view.rerender(<PlannerThread {...props} session={session} />);
    expect(screen.getByRole("textbox")).toHaveValue("Session A draft");
  });

  it("does not let an earlier send clear another session or later typing", async () => {
    let finish!: () => void;
    const onSubmitReply = vi.fn(() => new Promise<void>((resolve) => { finish = resolve; }));
    const props = { replyRecoveryPending: false, onRetryReplyRecovery: vi.fn(), onSubmitReply, onRegenerate: vi.fn(), onActivateVariant: vi.fn() };
    const view = renderThread(<PlannerThread {...props} session={session} />);
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "Submitted A" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "New A" } });
    view.rerender(<PlannerThread {...props} session={{ ...session, planning_session_id: "session-b" }} />);
    fireEvent.change(screen.getByRole("textbox"), { target: { value: "Unsent B" } });
    await act(async () => { finish(); });
    expect(screen.getByRole("textbox")).toHaveValue("Unsent B");
    view.rerender(<PlannerThread {...props} session={session} />);
    expect(screen.getByRole("textbox")).toHaveValue("New A");
  });
});


describe("PlannerThread response cancellation", () => {
  const operation: PlannerOperationState = {
    kind: "reply",
    operationId: "op-1",
    status: "streaming",
    lastEventSeq: 1,
    pendingReply: null,
    replyText: "",
    formalizeText: "",
    error: null,
    activityItems: [],
  };
  const props = {
    replyRecoveryPending: false,
    onRetryReplyRecovery: vi.fn(),
    onSubmitReply: vi.fn(),
    onRegenerate: vi.fn(),
    onActivateVariant: vi.fn(),
  };

  it("keeps the draft and reports Stopped only after canonical cancellation", async () => {
    const onStop = vi.fn().mockResolvedValue(undefined);
    const view = renderThread(<PlannerThread {...props} session={session} />);
    fireEvent.change(screen.getByRole("textbox"), {
      target: { value: "My next reply" },
    });
    view.rerender(
      <PlannerThread
        {...props}
        session={{ ...session, conversation_state: "processing" }}
        operation={operation}
        stop={{ available: true, pending: false, onStop }}
      />,
    );
    const stop = screen.getByRole("button", { name: "Stop response" });
    expect(stop.nextElementSibling).toBe(
      screen.getByRole("button", { name: "Send" }),
    );
    await act(async () => {
      fireEvent.click(stop);
    });
    expect(onStop).toHaveBeenCalledOnce();
    expect(screen.queryByText("Response Stopped")).not.toBeInTheDocument();
    expect(screen.getByRole("textbox")).toHaveValue("My next reply");

    view.rerender(
      <PlannerThread
        {...props}
        session={{ ...session, conversation_state: "processing" }}
        operation={operation}
        stop={{ available: true, pending: true, onStop }}
      />,
    );
    expect(screen.getByRole("button", { name: "Stopping response" })).toBe(stop);
    expect(
      screen.getByRole("button", { name: "Stopping response" }),
    ).toBeDisabled();
    expect(screen.getByRole("status")).toHaveTextContent("Stopping response…");
    expect(screen.queryByText("Response Stopped")).not.toBeInTheDocument();

    view.rerender(
      <PlannerThread
        {...props}
        session={session}
        operation={{
          ...operation,
          status: "cancelled",
          error: "planner run cancelled by user",
        }}
        stop={{ available: false, pending: false, onStop }}
      />,
    );
    const status = screen.getByRole("status");
    expect(status).toHaveTextContent("Response Stopped");
    expect(status.parentElement).toContainElement(screen.getByRole("textbox"));
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(
      screen.queryByText("planner run cancelled by user"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Stop response" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("textbox")).toHaveValue("My next reply");
    expect(screen.getByRole("textbox")).toBeEnabled();
  });

  it("retains stop errors and recovery blocking without claiming the response stopped", () => {
    const onStop = vi.fn();
    renderThread(
      <PlannerThread
        {...props}
        session={{ ...session, conversation_state: "processing" }}
        operation={operation}
        stop={{
          available: true,
          pending: false,
          error: "Stop outcome could not be verified",
          onStop,
        }}
        recoveryBlocked
      />,
    );
    const stop = screen.getByRole("button", { name: "Stop response" });
    expect(stop).toBeDisabled();
    fireEvent.click(stop);
    expect(onStop).not.toHaveBeenCalled();
    expect(
      screen.getByText("Stop outcome could not be verified"),
    ).toHaveAttribute("role", "alert");
    expect(
      screen.getByText(/previous planner request has an unresolved outcome/),
    ).toBeVisible();
    expect(screen.queryByText("Response Stopped")).not.toBeInTheDocument();
  });

  it("reports confirmed cancellation even when no raw reason was recorded", () => {
    renderThread(
      <PlannerThread
        {...props}
        session={session}
        operation={{ ...operation, status: "cancelled", error: null }}
      />,
    );
    expect(screen.getByRole("status")).toHaveTextContent("Response Stopped");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("keeps actual operation failure distinct from cancellation", () => {
    renderThread(
      <PlannerThread
        {...props}
        session={session}
        operation={{
          ...operation,
          status: "error",
          error: "Provider failed before settlement",
        }}
      />,
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Provider failed before settlement",
    );
    expect(screen.queryByText("Response Stopped")).not.toBeInTheDocument();
  });
});
