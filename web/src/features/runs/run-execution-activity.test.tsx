import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { RunExecutionActivity } from "./run-execution-activity";

const durableItems = [
  { id: "t1", kind: "thinking" as const, label: "Thinking", detail: "Reasoning text here", occurred_at: "2026-06-15T10:00:00Z" },
];

vi.mock("@/api/hooks-runs", () => ({
  useRunExecutionActivity: () => ({ data: { items: durableItems } }),
}));

vi.mock("./run-live-activity", () => ({
  useLiveRunActivity: () => ({}),
  selectLiveRunActivity: () => [],
}));

describe("RunExecutionActivity folding", () => {
  it("is collapsed by default and reveals thoughts only after expanding both disclosures", async () => {
    const user = userEvent.setup();
    render(<RunExecutionActivity runId="run-1" attemptId="att-1" isRunning={false} />);

    // Section header is present but collapsed; reasoning is not in the DOM yet.
    const activityToggle = screen.getByRole("button", { name: /Activity/ });
    expect(activityToggle).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText("Reasoning text here")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Thoughts/ })).not.toBeInTheDocument();

    // Expand the Activity section -> the Thoughts disclosure appears, itself collapsed.
    await user.click(activityToggle);
    const thoughtsToggle = screen.getByRole("button", { name: /Thoughts/ });
    expect(thoughtsToggle).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByText("Reasoning text here")).not.toBeInTheDocument();

    // Expand Thoughts -> the reasoning is finally shown.
    await user.click(thoughtsToggle);
    expect(screen.getByText("Reasoning text here")).toBeInTheDocument();
  });
});
