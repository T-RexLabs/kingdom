import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import type { RunNodeAttemptHistoryItem } from "@/api/hooks";
import { RunExecutionPicker } from "./run-execution-picker";

vi.mock("@/theme", async () => {
  const actual = await vi.importActual<typeof import("@/theme")>("@/theme");
  return { ...actual, useTheme: () => ({ theme: actual.builtInThemes[3] }) };
});

it("offers previous and current views symmetrically when used directly in contextual dialogs", async () => {
  const props: Parameters<typeof RunExecutionPicker>[0] = {
    nodes: [
      {
        node_id: "quality_validation",
        required_role: "quality-validator",
        execution_ownership: "provider_backed",
        status: "pending",
        attempt_count: 1,
      },
    ],
    attempts: [
      {
        attempt_id: "previous",
        attempt: 1,
        status: "failed",
      } as RunNodeAttemptHistoryItem,
    ],
    selectedNodeId: "quality_validation",
    selectedAttemptId: null,
    detail: null,
    onSelectStep: vi.fn(),
    onSelectAttempt: vi.fn(),
    onSelectCurrentAttempt: vi.fn(),
    hasMoreAttempts: false,
    onLoadMoreAttempts: vi.fn(),
    isLoadingMoreAttempts: false,
    isHistoryLoading: false,
  };
  const view = render(<RunExecutionPicker {...props} />);
  await userEvent.click(
    screen.getByRole("button", { name: "View Previous Attempt" }),
  );
  expect(props.onSelectAttempt).toHaveBeenCalledWith("previous");
  view.rerender(<RunExecutionPicker {...props} selectedAttemptId="previous" />);
  expect(
    screen.queryByRole("button", { name: "Select attempt" }),
  ).not.toBeInTheDocument();
  await userEvent.click(
    screen.getByRole("button", { name: "Return to Current Step" }),
  );
  expect(props.onSelectCurrentAttempt).toHaveBeenCalledOnce();
});
