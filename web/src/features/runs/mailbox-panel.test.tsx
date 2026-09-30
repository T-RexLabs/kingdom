import { useMemo, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { MailboxFilters, MailboxMessage, MailboxMessageDetail } from "@/api/hooks";
import { MailboxPanel } from "./mailbox-panel";
import { buildMailboxPresentation } from "./mailbox-lanes";

function makeMessage(partial: Partial<MailboxMessage> & { message_id: string }): MailboxMessage {
  return {
    message_id: partial.message_id,
    run_id: partial.run_id ?? "run-1",
    node_id: partial.node_id ?? "",
    from_agent_id: partial.from_agent_id ?? "king",
    from_agent_name: partial.from_agent_name ?? "King",
    to_agent_id: partial.to_agent_id ?? "chancellor",
    to_agent_name: partial.to_agent_name ?? "Chancellor",
    thread_id: partial.thread_id ?? "thread-1",
    message_type: partial.message_type ?? "status_update",
    communication_type: partial.communication_type ?? "instruction_dispatched",
    message_source: partial.message_source ?? "communication",
    priority: partial.priority ?? "normal",
    created_at: partial.created_at ?? "2026-03-06T12:00:00.000Z",
    read_at: partial.read_at ?? null,
    reason_code: partial.reason_code,
    verdict_type: partial.verdict_type,
    verdict_status: partial.verdict_status,
    preview: partial.preview ?? "Preview",
    format: partial.format ?? "markdown",
  };
}

function makeDetail(message: MailboxMessage, instruction: string, response: string): MailboxMessageDetail {
  return {
    ...message,
    content: `${instruction}\n${response}`,
    payload_json: {
      instruction_full: instruction,
      response_full: response,
    },
  };
}

function MailboxHarness({
  messages,
  detailById,
  initialFilters = {},
  isLoading = false,
  errorMessage = null,
  onRetry,
}: {
  messages: MailboxMessage[];
  detailById: Record<string, MailboxMessageDetail>;
  initialFilters?: MailboxFilters;
  isLoading?: boolean;
  errorMessage?: string | null;
  onRetry?: () => void;
}) {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [filters, setFilters] = useState<MailboxFilters>(initialFilters);
  const detail = useMemo(() => (selectedId ? detailById[selectedId] ?? null : null), [detailById, selectedId]);

  return (
    <MailboxPanel
      presentation={buildMailboxPresentation(messages)}
      isLoading={isLoading}
      errorMessage={errorMessage}
      onRetry={onRetry}
      onSelectMessage={setSelectedId}
      selectedDetail={detail}
      filters={filters}
      onChangeFilters={setFilters}
    />
  );
}

describe("mailbox panel", () => {
  it("labels partial counts and retries older pages without hiding loaded messages", async () => {
    const onLoadMore = vi.fn();
    const props = {
      presentation: buildMailboxPresentation([makeMessage({ message_id: "loaded" })]),
      filters: {}, onChangeFilters: vi.fn(), onSelectMessage: vi.fn(), onLoadMore, hasMore: true,
    };
    const { rerender } = render(<MailboxPanel {...props} loadMoreError />);
    expect(screen.getByText("1 messages loaded")).toBeInTheDocument();
    expect(screen.getByText("Older messages could not be loaded.")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Retry Older Messages" }));
    expect(onLoadMore).toHaveBeenCalledOnce();
    rerender(<MailboxPanel {...props} isLoadingMore />);
    expect(screen.getByRole("button", { name: "Loading messages..." })).toBeDisabled();
    rerender(<MailboxPanel {...props} hasMore={false} />);
    expect(screen.queryByRole("button", { name: "Load Older Messages" })).not.toBeInTheDocument();
  });
  it("uses earliest message direction for lane labels", () => {
    const { lanes } = buildMailboxPresentation([
      makeMessage({
        message_id: "m-1",
        from_agent_id: "king",
        from_agent_name: "King",
        to_agent_id: "chancellor",
        to_agent_name: "Chancellor",
        created_at: "2026-03-06T10:00:00.000Z",
      }),
      makeMessage({
        message_id: "m-2",
        from_agent_id: "chancellor",
        from_agent_name: "Chancellor",
        to_agent_id: "king",
        to_agent_name: "King",
        communication_type: "response_received",
        created_at: "2026-03-06T10:03:00.000Z",
      }),
    ]);

    expect(lanes).toHaveLength(1);
    expect(lanes[0]?.laneLabel).toBe("King -> Chancellor");
  });

  it("builds lanes from every message it is given", () => {
    const presentation = buildMailboxPresentation([
      makeMessage({ message_id: "self-1", from_agent_id: "king", to_agent_id: "king", to_agent_name: "King" }),
      makeMessage({ message_id: "m-2", from_agent_id: "king", to_agent_id: "chancellor" }),
    ]);

    expect(presentation.visibleMessageCount).toBe(2);
    expect(presentation.lanes).toHaveLength(2);
  });

  it("shows a loading state while messages are resolving", () => {
    render(<MailboxHarness messages={[]} detailById={{}} isLoading />);

    expect(screen.getByText("Loading messages...")).toBeInTheDocument();
    expect(screen.queryByText("No messages yet.")).not.toBeInTheDocument();
  });

  it("uses a height-managed layout with dedicated lane and message scroll regions", () => {
    const message = makeMessage({
      message_id: "m-layout",
      preview: "Layout preview",
    });

    render(<MailboxHarness messages={[message]} detailById={{}} />);

    expect(screen.getByTestId("mailbox-panel")).toHaveClass("h-full");
    expect(screen.getByTestId("mailbox-panel")).toHaveClass("min-h-0");
    expect(screen.getByTestId("mailbox-panel-layout")).toHaveClass("flex-1");
    expect(screen.getByTestId("mailbox-lane-list")).toHaveClass("overflow-y-auto");
    expect(screen.getByTestId("mailbox-message-list")).toHaveClass("overflow-y-auto");
  });

  it("shows an error state with retry when message loading fails", async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();

    render(
      <MailboxHarness
        messages={[]}
        detailById={{}}
        errorMessage="network timeout"
        onRetry={onRetry}
      />,
    );

    expect(screen.getByText("Unable to load messages.")).toBeInTheDocument();
    expect(screen.getByText("network timeout")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it("keeps filter controls visible when no messages match", () => {
    render(<MailboxHarness messages={[]} detailById={{}} initialFilters={{ communicationType: "response_received" }} />);

    expect(screen.getByRole("button", { name: "Message type" })).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Clear filter" }).length).toBeGreaterThan(0);
    expect(screen.getByText("No messages match this filter.")).toBeInTheDocument();
  });

  it("does not auto-open detail on initial load or lane switch", async () => {
    const user = userEvent.setup();
    const laneOne = makeMessage({
      message_id: "m-lane-1",
      from_agent_id: "king",
      from_agent_name: "King",
      to_agent_id: "chancellor",
      to_agent_name: "Chancellor",
      created_at: "2026-03-06T10:00:00.000Z",
      preview: "Lane one",
    });
    const laneTwo = makeMessage({
      message_id: "m-lane-2",
      from_agent_id: "chancellor",
      from_agent_name: "Chancellor",
      to_agent_id: "council",
      to_agent_name: "Council Member",
      created_at: "2026-03-06T10:01:00.000Z",
      preview: "Lane two",
    });

    render(
      <MailboxHarness
        messages={[laneOne, laneTwo]}
        detailById={{
          "m-lane-1": makeDetail(laneOne, "lane one instruction", "lane one response"),
          "m-lane-2": makeDetail(laneTwo, "lane two instruction", "lane two response"),
        }}
      />,
    );

    expect(screen.queryByRole("dialog", { name: "Message Detail" })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Chancellor -> Council Member/i }));
    expect(screen.queryByRole("dialog", { name: "Message Detail" })).not.toBeInTheDocument();
  });

  it("opens and closes modal detail without clearing lane selection", async () => {
    const user = userEvent.setup();
    const older = makeMessage({
      message_id: "m-old",
      preview: "Older preview",
      created_at: "2026-03-06T10:00:00.000Z",
      communication_type: "instruction_dispatched",
    });
    const latest = makeMessage({
      message_id: "m-new",
      preview: "Latest preview",
      created_at: "2026-03-06T10:10:00.000Z",
      communication_type: "response_received",
    });

    render(
      <MailboxHarness
        messages={[older, latest]}
        detailById={{
          "m-old": makeDetail(older, "Full instruction body", "Full response body"),
        }}
      />,
    );

    expect(screen.queryByRole("dialog", { name: "Message Detail" })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Instruction/i }));
    const dialog = screen.getByRole("dialog", { name: "Message Detail" });
    const contentCard = within(dialog).getByTestId("message-detail-card");

    expect(dialog).toBeInTheDocument();
    expect(contentCard).toHaveClass("border-border");
    expect(contentCard).toHaveClass("bg-background");
    expect(contentCard).not.toHaveClass("border-gold/25");
    expect(within(dialog).getByText("King -> Chancellor")).toBeInTheDocument();
    expect(screen.getByText("Full instruction body")).toBeInTheDocument();
    expect(screen.queryByText("Full response body")).not.toBeInTheDocument();
    expect(within(dialog).queryByText(/^Instruction$/)).not.toBeInTheDocument();
    expect(within(dialog).queryByText(/^Response$/)).not.toBeInTheDocument();

	await user.click(screen.getByRole("button", { name: "Close dialog" }));
    expect(screen.queryByRole("dialog", { name: "Message Detail" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Instruction/i })).toHaveAttribute("aria-pressed", "true");
  });

  it("prefers response body for response messages", async () => {
    const user = userEvent.setup();
    const message = makeMessage({
      message_id: "m-response-detail",
      preview: "Response preview",
      communication_type: "response_received",
    });

    render(
      <MailboxHarness
        messages={[message]}
        detailById={{
          "m-response-detail": makeDetail(message, "Instruction body should stay hidden", "Response body should be shown"),
        }}
      />,
    );

    await user.click(within(screen.getByTestId("mailbox-message-list")).getByRole("button", { name: /Response/i }));

    const dialog = screen.getByRole("dialog", { name: "Message Detail" });
    expect(within(dialog).getByText("Response body should be shown")).toBeInTheDocument();
    expect(within(dialog).queryByText("Instruction body should stay hidden")).not.toBeInTheDocument();
  });

  it("uses messages wording in controls and helper copy", () => {
    render(<MailboxHarness messages={[]} detailById={{}} />);
    expect(screen.getByRole("button", { name: "Message type" })).toHaveTextContent("All messages");
    expect(screen.getByText("No messages yet.")).toBeInTheDocument();
  });

  it("uses the shared themed dropdown for message type filters", async () => {
    const user = userEvent.setup();
    render(<MailboxHarness messages={[]} detailById={{}} />);

    await user.click(screen.getByRole("button", { name: "Message type" }));
    await user.click(screen.getByRole("option", { name: "Response" }));

    expect(screen.getByRole("button", { name: "Message type" })).toHaveTextContent("Response");
    expect(screen.getAllByRole("button", { name: "Clear filter" }).length).toBeGreaterThan(0);
  });

  it("selects the highlighted dropdown option with Enter after arrow navigation", async () => {
    const user = userEvent.setup();
    render(<MailboxHarness messages={[]} detailById={{}} />);

    await user.click(screen.getByRole("button", { name: "Message type" }));
		await waitFor(() => expect(screen.getByRole("option", { name: "All messages" })).toHaveFocus());
    await user.keyboard("{Home}{ArrowDown}{ArrowDown}{Enter}");

    expect(screen.getByRole("button", { name: "Message type" })).toHaveTextContent("Response");
  });

  it("keeps the initially selected lane stable when fresher lanes arrive later", () => {
    const laneAlpha = makeMessage({
      message_id: "m-alpha",
      from_agent_id: "king",
      from_agent_name: "King",
      to_agent_id: "chancellor",
      to_agent_name: "Chancellor",
      created_at: "2026-03-06T10:00:00.000Z",
      preview: "Alpha",
    });
    const laneBeta = makeMessage({
      message_id: "m-beta",
      from_agent_id: "council",
      from_agent_name: "Council Member",
      to_agent_id: "reviewer",
      to_agent_name: "Reviewer Shieldmaiden",
      created_at: "2026-03-06T10:01:00.000Z",
      preview: "Beta",
    });

    const { rerender } = render(
      <MailboxHarness messages={[laneAlpha, laneBeta]} detailById={{}} />,
    );

    expect(screen.getByText("Conversation").parentElement).toHaveTextContent(
      "Council Member -> Reviewer Shieldmaiden",
    );

    const laneGamma = makeMessage({
      message_id: "m-gamma",
      from_agent_id: "builder",
      from_agent_name: "Builder Berserker",
      to_agent_id: "reviewer",
      to_agent_name: "Reviewer Shieldmaiden",
      created_at: "2026-03-06T10:02:00.000Z",
      preview: "Gamma",
    });

    rerender(<MailboxHarness messages={[laneAlpha, laneBeta, laneGamma]} detailById={{}} />);

    expect(screen.getByText("Conversation").parentElement).toHaveTextContent(
      "Council Member -> Reviewer Shieldmaiden",
    );
    expect(screen.getByText("Conversation").parentElement).not.toHaveTextContent(
      "Builder Berserker -> Reviewer Shieldmaiden",
    );
  });

  it("moves focus into message detail and restores it to the selected message on close", async () => {
    const user = userEvent.setup();
    const message = makeMessage({
      message_id: "m-focus",
      preview: "Focus preview",
      communication_type: "instruction_dispatched",
    });

    render(
      <MailboxHarness
        messages={[message]}
        detailById={{
          "m-focus": makeDetail(message, "Focus instruction", "Focus response"),
        }}
      />,
    );

    const messageButton = screen.getByRole("button", { name: /Instruction/i });
    await user.click(messageButton);

	const closeButton = screen.getByRole("button", { name: "Close dialog" });
    await waitFor(() => expect(closeButton).toHaveFocus());

    await user.click(closeButton);
    await waitFor(() => expect(messageButton).toHaveFocus());
  });

  it("does not show thread identifier text on each message row", () => {
    const message = makeMessage({
      message_id: "m-thread",
      thread_id: "thread-council-plan-drafting",
    });

    render(<MailboxHarness messages={[message]} detailById={{}} />);
    expect(screen.queryByText(/Thread: thread-council-plan-drafting/i)).not.toBeInTheDocument();
  });

  it("applies wrap-safe utility classes for long lane and preview text", () => {
    const longName = "ArchitecturalOversightCouncilWithExtraLongUnbrokenIdentifier";
    const longPreview = "SuperLongPreviewTokenWithoutNaturalBreaks_012345678901234567890123456789";
    const message = makeMessage({
      message_id: "m-wrap",
      from_agent_name: longName,
      to_agent_name: longName,
      preview: longPreview,
    });

    render(<MailboxHarness messages={[message]} detailById={{}} />);

    const laneButton = screen
      .getAllByRole("button", { name: new RegExp(longName) })
      .find((element) => element.getAttribute("aria-pressed") === "true");
    expect(laneButton).toBeTruthy();
    if (!laneButton) {
      throw new Error("expected active lane button to exist");
    }
    const wrapContainers = laneButton.querySelectorAll(".text-wrap-anywhere");
    expect(wrapContainers.length).toBeGreaterThan(0);

    const conversationCard = screen.getByText("Conversation").closest("div");
    expect(conversationCard).toBeTruthy();
    if (!conversationCard) {
      throw new Error("expected conversation card to exist");
    }
    const conversationHeading = within(conversationCard).getByText(new RegExp(`${longName} -> ${longName}`));
    expect(conversationHeading).toHaveClass("text-wrap-anywhere");
  });

  it("does not show verdict badge in response message rows", () => {
    const response = makeMessage({
      message_id: "m-response-verdict",
      communication_type: "response_received",
      preview: "VERDICT: PASS",
      created_at: "2026-03-06T10:00:00.000Z",
    });

    render(<MailboxHarness messages={[response]} detailById={{}} />);

    const responseRow = screen.getByRole("button", { name: /Response/i });
    expect(within(responseRow).queryByText(/^Pass$/i)).not.toBeInTheDocument();
  });

  it("surfaces review and validation verdict badges on thread cards from structured summary fields", () => {
    const review = makeMessage({
      message_id: "m-review",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review",
      communication_type: "response_received",
      preview: "Reviewer requested another pass.",
      verdict_type: "review",
      verdict_status: "rework",
      created_at: "2026-03-06T10:00:00.000Z",
    });
    const validation = makeMessage({
      message_id: "m-validation",
      node_id: "validation_gates_nightraven",
      thread_id: "thread-validation",
      communication_type: "response_received",
      preview: "Nightraven found unresolved issues.",
      verdict_type: "validation",
      verdict_status: "rework",
      from_agent_id: "intelligence-nightraven",
      from_agent_name: "Nightraven",
      created_at: "2026-03-06T10:01:00.000Z",
    });

    render(<MailboxHarness messages={[review, validation]} detailById={{}} />);

    const laneList = screen.getByTestId("mailbox-lane-list");
    expect(within(laneList).getByText("Review: Rework")).toBeInTheDocument();
    expect(within(laneList).getByText("Validation: Rework")).toBeInTheDocument();
  });

  it("shows invalid response when latest owner response is malformed", () => {
    const reviewVerdict = makeMessage({
      message_id: "m-review-verdict",
      node_id: "execution_review_shieldmaiden",
      communication_type: "response_received",
      preview: "VERDICT: REWORK",
      verdict_type: "review",
      verdict_status: "rework",
      created_at: "2026-03-06T10:00:00.000Z",
    });
    const newerWithoutVerdict = makeMessage({
      message_id: "m-review-followup",
      node_id: "execution_review_shieldmaiden",
      communication_type: "response_received",
      preview: "Follow-up planning note without verdict",
      reason_code: "llm_invalid_response",
      verdict_type: "review",
      verdict_status: "invalid",
      created_at: "2026-03-06T10:01:00.000Z",
    });

    render(<MailboxHarness messages={[reviewVerdict, newerWithoutVerdict]} detailById={{}} />);

    const laneList = screen.getByTestId("mailbox-lane-list");
    expect(within(laneList).queryByText("Review: Rework")).not.toBeInTheDocument();
    expect(within(laneList).getByText("Invalid Response")).toBeInTheDocument();
  });

  it("shows auto-succeeded at cap instead of stale rework on thread cards", () => {
    const reviewRework = makeMessage({
      message_id: "m-review-rework",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review",
      communication_type: "response_received",
      preview: "VERDICT: REWORK",
      verdict_type: "review",
      verdict_status: "rework",
      created_at: "2026-03-06T10:00:00.000Z",
    });
    const capReached = makeMessage({
      message_id: "m-review-cap",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review",
      communication_type: "response_received",
      preview: "Review cycle cap reached; auto-succeeded.",
      reason_code: "execution_review_cap_auto_succeeded",
      verdict_type: "review",
      verdict_status: "auto_succeeded",
      created_at: "2026-03-06T10:01:00.000Z",
    });

    render(<MailboxHarness messages={[reviewRework, capReached]} detailById={{}} />);

    const laneList = screen.getByTestId("mailbox-lane-list");
    expect(within(laneList).queryByText("Review: Rework")).not.toBeInTheDocument();
    expect(within(laneList).getByText("Auto-Succeeded at Cap")).toBeInTheDocument();
  });

  it("shows mixed outcomes on a lane when thread-level latest verdicts disagree", () => {
    const reworkThread = makeMessage({
      message_id: "m-review-mixed-rework",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review-a",
      communication_type: "response_received",
      preview: "VERDICT: REWORK",
      verdict_type: "review",
      verdict_status: "rework",
      created_at: "2026-03-06T10:00:00.000Z",
    });
    const autoSucceededThread = makeMessage({
      message_id: "m-review-mixed-cap",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review-b",
      communication_type: "response_received",
      preview: "Review cycle cap reached; auto-succeeded.",
      verdict_type: "review",
      verdict_status: "auto_succeeded",
      reason_code: "execution_review_cap_auto_succeeded",
      created_at: "2026-03-06T10:01:00.000Z",
    });

    render(<MailboxHarness messages={[reworkThread, autoSucceededThread]} detailById={{}} />);

    expect(screen.getByText("Mixed Outcomes")).toBeInTheDocument();
  });

  it("shows per-message verdict badges for operator-visible history", () => {
    const reviewRework = makeMessage({
      message_id: "m-review-history-rework",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review-history-a",
      communication_type: "response_received",
      preview: "VERDICT: REWORK",
      verdict_type: "review",
      verdict_status: "rework",
      created_at: "2026-03-06T10:00:00.000Z",
    });
    const autoSucceeded = makeMessage({
      message_id: "m-review-history-cap",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review-history-b",
      communication_type: "response_received",
      preview: "Review cycle cap reached; auto-succeeded.",
      verdict_type: "review",
      verdict_status: "auto_succeeded",
      reason_code: "execution_review_cap_auto_succeeded",
      created_at: "2026-03-06T10:01:00.000Z",
    });

    render(<MailboxHarness messages={[reviewRework, autoSucceeded]} detailById={{}} />);

    const messageList = screen.getByTestId("mailbox-message-list");
    expect(within(messageList).getByText("Review: Rework")).toBeInTheDocument();
    expect(within(messageList).getByText("Auto-Succeeded at Cap")).toBeInTheDocument();
  });

  it("renders invalid verdict detail from structured fields without reparsing message text", async () => {
    const user = userEvent.setup();
    const invalidMessage = makeMessage({
      message_id: "m-review-invalid-detail",
      node_id: "execution_review_shieldmaiden",
      communication_type: "response_received",
      preview: "Malformed review response",
      verdict_type: "review",
      verdict_status: "invalid",
      reason_code: "llm_invalid_response",
    });

    render(
      <MailboxHarness
        messages={[invalidMessage]}
        detailById={{
          "m-review-invalid-detail": makeDetail(invalidMessage, "", "Missing verdict control line."),
        }}
      />,
    );

    await user.click(within(screen.getByTestId("mailbox-message-list")).getByRole("button", { name: /Response/i }));
    const dialog = screen.getByRole("dialog", { name: "Message Detail" });
    expect(dialog).toBeInTheDocument();
    expect(within(dialog).getByText("Invalid Response")).toBeInTheDocument();
  });

  it("renders auto-succeeded verdict detail from structured fields", async () => {
    const user = userEvent.setup();
    const autoSucceededMessage = makeMessage({
      message_id: "m-review-auto-detail",
      node_id: "execution_review_shieldmaiden",
      communication_type: "response_received",
      preview: "Review cycle cap reached; auto-succeeded.",
      verdict_type: "review",
      verdict_status: "auto_succeeded",
      reason_code: "execution_review_cap_auto_succeeded",
    });

    render(
      <MailboxHarness
        messages={[autoSucceededMessage]}
        detailById={{
          "m-review-auto-detail": makeDetail(autoSucceededMessage, "", "Review cycle cap reached."),
        }}
      />,
    );

    await user.click(within(screen.getByTestId("mailbox-message-list")).getByRole("button", { name: /Response/i }));
    const dialog = screen.getByRole("dialog", { name: "Message Detail" });
    expect(dialog).toBeInTheDocument();
    expect(within(dialog).getByText("Auto-Succeeded at Cap")).toBeInTheDocument();
  });

  it("does not surface validation verdict badge for builder node responses", () => {
    const builderResponse = makeMessage({
      message_id: "m-builder-response",
      node_id: "delegated_execution_berserker",
      communication_type: "response_received",
      preview: "VERDICT: PASS\nBuilder completed implementation.",
      created_at: "2026-03-06T10:00:00.000Z",
    });

    render(<MailboxHarness messages={[builderResponse]} detailById={{}} />);

    expect(screen.queryByText("Validation: Pass")).not.toBeInTheDocument();
    expect(screen.queryByText("Review: Pass")).not.toBeInTheDocument();
  });

  it("does not infer thread verdict badges from preview text alone", () => {
    const reviewVerdict = makeMessage({
      message_id: "m-review-mixed-case",
      node_id: "execution_review_shieldmaiden",
      thread_id: "thread-review",
      communication_type: "response_received",
      preview: "verdict: pass",
      created_at: "2026-03-06T10:00:00.000Z",
    });

    render(<MailboxHarness messages={[reviewVerdict]} detailById={{}} />);

    expect(screen.queryByText("Review: Pass")).not.toBeInTheDocument();
  });

  it("renders roomier markdown spacing in message detail", async () => {
    const user = userEvent.setup();
    const message = makeMessage({
      message_id: "m-spacing",
      preview: "Spacing preview",
      communication_type: "instruction_dispatched",
    });

    render(
      <MailboxHarness
        messages={[message]}
        detailById={{
          "m-spacing": makeDetail(message, "First paragraph", "Second paragraph"),
        }}
      />,
    );

    await user.click(screen.getByRole("button", { name: /Instruction/i }));
    const paragraph = screen.getByText("First paragraph").closest("p");
    expect(paragraph).toHaveClass("mb-3");
  });

  it("adds visible spacing around markdown separators in message detail", async () => {
    const user = userEvent.setup();
    const message = makeMessage({
      message_id: "m-separator",
      preview: "Separator preview",
      communication_type: "instruction_dispatched",
    });

    render(
      <MailboxHarness
        messages={[message]}
        detailById={{
          "m-separator": makeDetail(message, "First block\n\n----\n\nSecond block", ""),
        }}
      />,
    );

    await user.click(screen.getByRole("button", { name: /Instruction/i }));
    const separator = document.querySelector("hr");
    expect(separator).not.toBeNull();
    const separatorWrapper = separator?.parentElement;
    expect(separatorWrapper).not.toBeNull();
    expect(separatorWrapper).toHaveClass("mt-4");
    expect(separatorWrapper).toHaveClass("mb-6");
  });

  it("renders markdown tables in message detail", async () => {
    const user = userEvent.setup();
    const message = makeMessage({
      message_id: "m-table",
      preview: "Table preview",
      communication_type: "instruction_dispatched",
    });

    render(
      <MailboxHarness
        messages={[message]}
        detailById={{
          "m-table": makeDetail(
            message,
            "| Constraint | Requirement |\n| --- | --- |\n| Endpoint | GET /hello |\n| Status | 200 |",
            "",
          ),
        }}
      />,
    );

    await user.click(screen.getByRole("button", { name: /Instruction/i }));
    expect(screen.getByRole("table")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Constraint" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "GET /hello" })).toBeInTheDocument();
  });

  it("renders markdown previews in live rows while keeping full markdown in detail", async () => {
    const user = userEvent.setup();
    const message = makeMessage({
      message_id: "m-preview-list",
      preview: "- First item\n- **Second** item",
      communication_type: "instruction_dispatched",
    });

    render(
      <MailboxHarness
        messages={[message]}
        detailById={{
          "m-preview-list": makeDetail(message, "- Keep detail list\n- Fully rendered", ""),
        }}
      />,
    );

    expect(screen.getAllByRole("list").length).toBeGreaterThan(0);

    const laneButton = screen.getByRole("button", { name: /King -> Chancellor/i });
    expect(within(laneButton).getByRole("list")).toBeInTheDocument();
    expect(laneButton).toHaveTextContent("First item");
    expect(laneButton).toHaveTextContent("Second item");

    const messageButton = screen.getByRole("button", { name: /Instruction/i });
    expect(within(messageButton).getByRole("list")).toBeInTheDocument();
    expect(messageButton).toHaveTextContent("First item");
    expect(messageButton).toHaveTextContent("Second item");

    await user.click(messageButton);

    const dialog = screen.getByRole("dialog", { name: "Message Detail" });
    expect(within(dialog).getByRole("list")).toBeInTheDocument();
    expect(within(dialog).getByText("Keep detail list")).toBeInTheDocument();
    expect(within(dialog).getByText("Fully rendered")).toBeInTheDocument();
  });
});
