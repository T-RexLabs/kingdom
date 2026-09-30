import { useState } from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { RunExecutionDetail } from "@/api/hooks";
import {
  RunInstructions,
  RunFinalResponse as CanonicalFinalResponse,
  type RunResponseDisclosure,
} from "./run-conversation";
import { repeatsSettledResponse } from "./run-conversation-state";
import type { ActivityItem } from "@/features/activity/activity-view";

vi.mock("@/api/hooks", () => ({
  useWorkOrder: () => ({
    data: {
      work_order_text: "## Requested Change\nKeep the full original brief.",
    },
  }),
}));

function RunFinalResponse(props: {
  detail: RunExecutionDetail;
  repeated?: boolean;
}) {
  const [disclosure, setDisclosure] = useState<RunResponseDisclosure>({
    response: false,
    source: false,
  });
  return (
    <CanonicalFinalResponse
      {...props}
      disclosure={disclosure}
      onDisclosureChange={setDisclosure}
    />
  );
}

const detail = {
  attempt_id: "a",
  provider_invocation_id: "inv",
  settlement_status: "succeeded",
  output_status: "settled",
  output_text: "## Answer\nThe result is ready.",
} as RunExecutionDetail;
const item: ActivityItem = {
  id: "part",
  eventId: "event",
  sequence: 1,
  kind: "message",
  label: "Provider Message",
  invocationId: "inv",
  messageContent: {
    availability: "available",
    preview: detail.output_text,
    byte_length: new TextEncoder().encode(detail.output_text).length,
  },
};
describe("run conversation final response", () => {
  it("shows the issued instructions as complete Markdown context without inventing a prompt", () => {
    render(<RunInstructions workOrderId="work" />);
    expect(
      screen.getByRole("region", { name: "Issued instructions" }),
    ).toBeVisible();
    expect(
      screen.getByText("The issued work brief for this run."),
    ).toBeVisible();
    expect(
      screen.getByRole("heading", { name: "Requested Change" }),
    ).toBeVisible();
    expect(screen.getByText("Keep the full original brief.")).toBeVisible();
  });
  it("keeps exact settled source accessible without a second default copy", async () => {
    render(<RunFinalResponse detail={detail} />);
    expect(document.querySelector("pre")).toBeNull();
    await userEvent.click(screen.getByText("Response Details"));
    expect(document.querySelector("pre")?.textContent).toBe(detail.output_text);
    expect(screen.getByText(/Attempt ID: a/)).toBeVisible();
  });
  it("renders canonical settled Markdown in the conversation", () => {
    render(<RunFinalResponse detail={detail} />);
    expect(screen.getByRole("heading", { name: "Answer" })).toBeVisible();
    expect(screen.queryByText("Outcome")).not.toBeInTheDocument();
    expect(screen.queryByText("Diagnostics")).not.toBeInTheDocument();
  });
  it("does not turn an observed response or pending settlement into a final response", () => {
    const view = render(
      <RunFinalResponse detail={{ ...detail, settlement_status: "pending" }} />,
    );
    expect(screen.queryByText("Final Response")).not.toBeInTheDocument();
    view.rerender(
      <RunFinalResponse detail={{ ...detail, output_status: "observed" }} />,
    );
    expect(
      screen.queryByRole("heading", { name: "Answer" }),
    ).not.toBeInTheDocument();
  });
  it("only quiets repeated final prose after exact complete text and invocation equality", async () => {
    expect(repeatsSettledResponse(detail, [item])).toBe(true);
    expect(
      repeatsSettledResponse(detail, [{ ...item, invocationId: "other" }]),
    ).toBe(false);
    expect(
      repeatsSettledResponse(detail, [
        {
          ...item,
          messageContent: { ...item.messageContent!, byte_length: 10000 },
        },
      ]),
    ).toBe(false);
    expect(
      repeatsSettledResponse(detail, [
        {
          ...item,
          messageContent: { ...item.messageContent!, preview: "different" },
        },
      ]),
    ).toBe(false);
    render(<RunFinalResponse detail={detail} repeated />);
    expect(
      screen.queryByRole("heading", { name: "Answer" }),
    ).not.toBeInTheDocument();
    await userEvent.click(screen.getByText("Final Response"));
    expect(screen.getByRole("heading", { name: "Answer" })).toBeVisible();
  });
});
