import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PlannerThread } from "./planner-thread";

const mockUsePlannerMessagePages = vi.fn();

vi.mock("./use-planner-message-pages", () => ({
  usePlannerMessagePages: (...args: unknown[]) => mockUsePlannerMessagePages(...args),
}));

vi.mock("./planner-composer", () => ({
  PlannerComposer: (props: { value: string; disabled?: boolean; onChange: (value: string) => void; onSend: () => void }) => (
    <div>
      <label htmlFor="planner-composer">Composer</label>
      <input
        id="planner-composer"
        value={props.value}
        onChange={(event) => props.onChange(event.target.value)}
      />
      <button type="button" disabled={props.disabled} onClick={props.onSend}>
        Send
      </button>
    </div>
  ),
}));

vi.mock("./planner-transcript", () => ({
  PlannerTranscript: (props: { followLatestRequest?: number; callbacks: { onRegenerate?: (turnIndex: number) => void } }) => (
    <div>
      <div data-testid="follow-latest-request">{props.followLatestRequest ?? 0}</div>
      <button type="button" onClick={() => props.callbacks.onRegenerate?.(2)}>
        Regenerate
      </button>
    </div>
  ),
}));

function buildSession() {
  return {
    planning_session_id: "planning-session-1",
    title: "Planner kickoff",
    title_state: "custom" as const,
    auto_title_eligible: false,
    lifecycle_state: "active" as const,
    conversation_state: "ready" as const,
    current_question_message_id: undefined,
    current_draft_id: undefined,
    context_snapshot_id: "planning-context-1",
    created_at: "2026-04-21T10:00:00Z",
    updated_at: "2026-04-21T10:05:00Z",
    project_root: "/tmp/project",
    model_selection: { model: "github-copilot/gpt-5.4" },
  };
}

function deferredPromise() {
  let resolve!: () => void;
  const promise = new Promise<void>((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

describe("PlannerThread followLatestRequest timing", () => {
  beforeEach(() => {
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

  it("increments followLatestRequest only after reply submission succeeds", async () => {
    const user = userEvent.setup();
    const pending = deferredPromise();
    const onSubmitReply = vi.fn(() => pending.promise);

    render(
      <PlannerThread
        session={buildSession()}
        operation={null}
        onSubmitReply={onSubmitReply}
        onRegenerate={vi.fn().mockResolvedValue(undefined)}
        onActivateVariant={vi.fn().mockResolvedValue(undefined)}
      />
    );

    await user.type(screen.getByLabelText("Composer"), "Follow after success");
    await user.click(screen.getByRole("button", { name: "Send" }));

    expect(onSubmitReply).toHaveBeenCalledWith({
      text: "Follow after success",
      answer: { text: "Follow after success" },
      truncateFromTurn: undefined,
    });
    expect(screen.getByTestId("follow-latest-request")).toHaveTextContent("0");

    await act(async () => {
      pending.resolve();
      await pending.promise;
    });

    await waitFor(() => {
      expect(screen.getByTestId("follow-latest-request")).toHaveTextContent("1");
    });
  });

  it("increments followLatestRequest only after regenerate succeeds", async () => {
    const user = userEvent.setup();
    const pending = deferredPromise();
    const onRegenerate = vi.fn(() => pending.promise);

    render(
      <PlannerThread
        session={buildSession()}
        operation={null}
        onSubmitReply={vi.fn().mockResolvedValue(undefined)}
        onRegenerate={onRegenerate}
        onActivateVariant={vi.fn().mockResolvedValue(undefined)}
      />
    );

    await user.click(screen.getByRole("button", { name: "Regenerate" }));

    expect(onRegenerate).toHaveBeenCalledWith({ turnIndex: 2 });
    expect(screen.getByTestId("follow-latest-request")).toHaveTextContent("0");

    await act(async () => {
      pending.resolve();
      await pending.promise;
    });

    await waitFor(() => {
      expect(screen.getByTestId("follow-latest-request")).toHaveTextContent("1");
    });
  });
});
