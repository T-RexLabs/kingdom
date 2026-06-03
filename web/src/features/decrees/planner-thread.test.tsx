import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PlannerThread } from "./planner-thread";

const mockUsePlannerMessagePages = vi.fn();
vi.mock("react-virtuoso", () => ({
  Virtuoso: ({ data, itemContent }: { data: unknown[]; itemContent: (index: number, item: unknown) => React.ReactNode }) => (
    <div data-testid="virtuoso-mock">{data.map((item, index) => <div key={index}>{itemContent(index, item)}</div>)}</div>
  ),
}));

vi.mock("./use-planner-message-pages", () => ({
  usePlannerMessagePages: (...args: unknown[]) => mockUsePlannerMessagePages(...args),
}));

vi.mock("./planner-composer", () => ({
  PlannerComposer: (props: {
    value: string;
    disabled: boolean;
    textareaRef?: React.RefObject<HTMLTextAreaElement | null>;
    onChange: (value: string) => void;
    onSend: () => void;
  }) => (
    <div>
      <label htmlFor="planner-composer">Composer</label>
      <textarea
        id="planner-composer"
        ref={props.textareaRef}
        value={props.value}
        disabled={props.disabled}
        onChange={(event) => props.onChange(event.target.value)}
      />
      <button type="button" disabled={props.disabled} onClick={props.onSend}>
        Send
      </button>
    </div>
  ),
}));

vi.mock("./planner-message", () => ({
  PlannerMessageCard: ({ message, children }: { message: { content_markdown: string }; children?: React.ReactNode }) => (
    <div>
      <div>{message.content_markdown}</div>
      {children}
    </div>
  ),
}));

describe("PlannerThread", () => {
  const onSubmitReply = vi.fn();
  const onRegenerate = vi.fn().mockResolvedValue(undefined);
  const onActivateVariant = vi.fn().mockResolvedValue(undefined);

  beforeEach(() => {
    onSubmitReply.mockReset();
    onSubmitReply.mockResolvedValue(undefined);
    onRegenerate.mockReset();
    onRegenerate.mockResolvedValue(undefined);
    onActivateVariant.mockReset();
    onActivateVariant.mockResolvedValue(undefined);
    mockUsePlannerMessagePages.mockReturnValue({
      messages: [],
      firstItemIndex: 0,
      totalCount: 0,
      hasPreviousPage: false,
      isFetchingPreviousPage: false,
      fetchPreviousPage: vi.fn(),
      error: null,
      isLoading: false,
    });
  });

  it("submits open-text answers as reply operations", async () => {
    const user = userEvent.setup();

    mockUsePlannerMessagePages.mockReturnValue({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "planning-session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          question_payload: {
            prompt: "What would you like to build?",
            type: "long_text",
          },
        },
      ],
      firstItemIndex: 0,
      totalCount: 1,
      hasPreviousPage: false,
      isFetchingPreviousPage: false,
      fetchPreviousPage: vi.fn(),
      error: null,
      isLoading: false,
    });

    render(
        <PlannerThread
          session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "ready",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
            model_selection: { model: "github-copilot/gpt-5.4" },
          }}
          operation={null}
          onSubmitReply={onSubmitReply}
          onRegenerate={onRegenerate}
          onActivateVariant={onActivateVariant}
        />
      );

    await user.type(screen.getByLabelText("Composer"), "Planner UX cleanup");
    await user.click(screen.getByRole("button", { name: "Send" }));

    expect(onSubmitReply).toHaveBeenCalledWith({
      text: "Planner UX cleanup",
      answer: { text: "Planner UX cleanup" },
    });
  });

  it("keeps single-select options actionable for the current canonical question", async () => {
    const user = userEvent.setup();

    mockUsePlannerMessagePages.mockReturnValue({
      messages: [
        {
          planning_message_id: "assistant-2",
          planning_session_id: "planning-session-1",
          turn_index: 2,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:01:00Z",
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [
              { label: "Lean", value: "lean" },
              { label: "Thorough", value: "thorough" },
            ],
          },
        },
      ],
      firstItemIndex: 0,
      totalCount: 1,
      hasPreviousPage: false,
      isFetchingPreviousPage: false,
      fetchPreviousPage: vi.fn(),
      error: null,
      isLoading: false,
    });

    render(
        <PlannerThread
          session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "ready",
          current_question_message_id: "assistant-2",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
            model_selection: { model: "github-copilot/gpt-5.4" },
          }}
          operation={null}
          onSubmitReply={onSubmitReply}
          onRegenerate={onRegenerate}
          onActivateVariant={onActivateVariant}
        />
      );

    await user.click(screen.getByRole("button", { name: "Lean" }));

    expect(onSubmitReply).toHaveBeenCalledWith({
      text: "Lean",
      answer: { text: "Lean", value: "lean" },
    });
  });

  it("focuses the composer when the active question offers type-your-answer", async () => {
    const user = userEvent.setup();

    mockUsePlannerMessagePages.mockReturnValue({
      messages: [
        {
          planning_message_id: "assistant-2",
          planning_session_id: "planning-session-1",
          turn_index: 2,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:01:00Z",
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [
              { label: "Lean", value: "lean" },
              { label: "Thorough", value: "thorough" },
            ],
          },
        },
      ],
      firstItemIndex: 0,
      totalCount: 1,
      hasPreviousPage: false,
      isFetchingPreviousPage: false,
      fetchPreviousPage: vi.fn(),
      error: null,
      isLoading: false,
    });

    render(
      <PlannerThread
        session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "ready",
          current_question_message_id: "assistant-2",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
          model_selection: { model: "github-copilot/gpt-5.4" },
        }}
        operation={null}
        onSubmitReply={onSubmitReply}
        onRegenerate={onRegenerate}
        onActivateVariant={onActivateVariant}
      />
    );

    const composer = screen.getByLabelText("Composer");
    expect(composer).not.toHaveFocus();

    await user.click(screen.getByRole("button", { name: "Type Your Answer" }));

    expect(composer).toHaveFocus();
    expect(onSubmitReply).not.toHaveBeenCalled();
  });

  it("disables composer while the conversation is processing", () => {
    render(
        <PlannerThread
          session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "processing",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
            model_selection: { model: "github-copilot/gpt-5.4" },
          }}
          operation={null}
          onSubmitReply={onSubmitReply}
          onRegenerate={onRegenerate}
          onActivateVariant={onActivateVariant}
        />
      );

    expect(screen.getByRole("button", { name: "Send" })).toBeDisabled();
  });

  it("disables composer immediately when a reply operation is pending", async () => {
    render(
      <PlannerThread
        session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "processing",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
          model_selection: { model: "github-copilot/gpt-5.4" },
        }}
        operation={{
          kind: "reply",
          operationId: "operation-1",
          status: "pending",
          lastEventSeq: 0,
          turnIndex: 2,
          userMessageId: "optimistic-user-1",
          userCreatedAt: "2026-04-21T10:05:01Z",
          assistantMessageId: "optimistic-assistant-1",
          assistantCreatedAt: "2026-04-21T10:05:01Z",
          pendingReply: {
            contentMarkdown: "Pending reply",
          },
          replyText: "",
          formalizeText: "",
          error: null,
        }}
        onSubmitReply={onSubmitReply}
        onRegenerate={onRegenerate}
        onActivateVariant={onActivateVariant}
      />
    );

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Send" })).toBeDisabled();
    });
    expect(screen.getByText("Pending reply")).toBeInTheDocument();
  });

  it("does not expose type-your-answer while the conversation is processing", () => {
    mockUsePlannerMessagePages.mockReturnValue({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "planning-session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          question_payload: {
            prompt: "What would you like to build?",
            type: "long_text",
          },
        },
      ],
      firstItemIndex: 0,
      totalCount: 1,
      hasPreviousPage: false,
      isFetchingPreviousPage: false,
      fetchPreviousPage: vi.fn(),
      error: null,
      isLoading: false,
    });

    render(
      <PlannerThread
        session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "processing",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
          model_selection: { model: "github-copilot/gpt-5.4" },
        }}
        operation={null}
        onSubmitReply={onSubmitReply}
        onRegenerate={onRegenerate}
        onActivateVariant={onActivateVariant}
      />
    );

    expect(screen.queryByRole("button", { name: "Type Your Answer" })).toBeNull();
  });

  it("renders the cancellation banner with neutral styling, not the failure colour", () => {
    // Shutdown is a system event, not a planner failure. The banner
    // must NOT carry the status-failed colour cue — that would tell
    // the user something went wrong with their content when really
    // the server just restarted.
    render(
      <PlannerThread
        session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "ready",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
          model_selection: { model: "github-copilot/gpt-5.4" },
        }}
        operation={{
          kind: "reply",
          operationId: "operation-cancelled",
          status: "cancelled",
          lastEventSeq: 5,
          turnIndex: 2,
          userMessageId: "msg-user-1",
          userCreatedAt: "2026-04-21T10:05:01Z",
          assistantMessageId: "msg-asst-1",
          assistantCreatedAt: "2026-04-21T10:05:01Z",
          pendingReply: null,
          replyText: "",
          formalizeText: "",
          error: "operation cancelled during shutdown",
        }}
        onSubmitReply={onSubmitReply}
        onRegenerate={onRegenerate}
        onActivateVariant={onActivateVariant}
      />
    );

    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent(/cancelled/i);
    // Failure styling uses status-failed/30 + status-failed/10. Neutral
    // styling uses border/40 + surface-raised/40. Asserting on the
    // *absence* of the failure colours is the right contract: the
    // exact neutral palette is a polish detail, but never showing red
    // for a shutdown is the rule.
    expect(alert.className).not.toMatch(/status-failed/);
    expect(alert.className).toMatch(/border\/40|surface-raised\/40|foreground-subtle/);
  });

  it("renders the failure banner with status-failed styling for genuine errors (regression guard)", () => {
    render(
      <PlannerThread
        session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "ready",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
          model_selection: { model: "github-copilot/gpt-5.4" },
        }}
        operation={{
          kind: "reply",
          operationId: "operation-failed",
          status: "error",
          lastEventSeq: 5,
          turnIndex: 2,
          userMessageId: "msg-user-1",
          userCreatedAt: "2026-04-21T10:05:01Z",
          assistantMessageId: "msg-asst-1",
          assistantCreatedAt: "2026-04-21T10:05:01Z",
          pendingReply: null,
          replyText: "",
          formalizeText: "",
          error: "Planner request failed",
        }}
        onSubmitReply={onSubmitReply}
        onRegenerate={onRegenerate}
        onActivateVariant={onActivateVariant}
      />
    );

    const alert = screen.getByRole("alert");
    expect(alert.className).toMatch(/status-failed/);
  });

  it("renders a neutral connection-lost banner when the SSE transport is failing", () => {
    // SSE backoff exhaustion is a transport problem, not an operation
    // failure. The banner stays informational while the server-side
    // operation continues and poll fallback reconciles in the background.
    render(
      <PlannerThread
        session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "processing",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
          model_selection: { model: "github-copilot/gpt-5.4" },
        }}
        operation={{
          kind: "reply",
          operationId: "operation-mid-flight",
          status: "pending",
          lastEventSeq: 1,
          turnIndex: 2,
          userMessageId: "msg-user-1",
          userCreatedAt: "2026-04-21T10:05:01Z",
          assistantMessageId: "msg-asst-1",
          assistantCreatedAt: "2026-04-21T10:05:01Z",
          pendingReply: { contentMarkdown: "still typing", answer: { text: "still typing" } },
          replyText: "",
          formalizeText: "",
          error: null,
        }}
        connection={{ state: "failed", attempt: 6, lastError: "EOF" }}
        onSubmitReply={onSubmitReply}
        onRegenerate={onRegenerate}
        onActivateVariant={onActivateVariant}
      />
    );

    const banner = screen.getByRole("status");
    expect(banner).toHaveTextContent(/connection lost/i);
    // Neutral chrome — never the red status-failed palette.
    expect(banner.className).not.toMatch(/status-failed/);
    expect(banner.className).toMatch(/border\/40|surface-raised\/40|foreground-subtle/);
    // No operation-error alert appears alongside (the operation is
    // genuinely still running; only the transport failed).
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("does not render the connection-lost banner while the SSE stream is healthy", () => {
    render(
      <PlannerThread
        session={{
          planning_session_id: "planning-session-1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "processing",
          current_question_message_id: "assistant-1",
          current_draft_id: undefined,
          context_snapshot_id: "planning-context-1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
          project_root: "/tmp/project",
          model_selection: { model: "github-copilot/gpt-5.4" },
        }}
        operation={{
          kind: "reply",
          operationId: "operation-mid-flight",
          status: "pending",
          lastEventSeq: 1,
          turnIndex: 2,
          userMessageId: "msg-user-1",
          userCreatedAt: "2026-04-21T10:05:01Z",
          assistantMessageId: "msg-asst-1",
          assistantCreatedAt: "2026-04-21T10:05:01Z",
          pendingReply: { contentMarkdown: "still typing", answer: { text: "still typing" } },
          replyText: "",
          formalizeText: "",
          error: null,
        }}
        connection={{ state: "open", attempt: 1 }}
        onSubmitReply={onSubmitReply}
        onRegenerate={onRegenerate}
        onActivateVariant={onActivateVariant}
      />
    );

    expect(screen.queryByRole("status")).toBeNull();
  });
});
