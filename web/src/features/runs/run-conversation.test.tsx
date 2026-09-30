import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RunExecutionDetail } from "@/api/hooks";
import { ApiError } from "@/api/client";
import {
  RunInstructions,
  RunFinalResponse,
  RunRuntimeResponse,
} from "./run-conversation";

const instructionQuery = vi.hoisted(() => ({
  data: "  Full dispatched prompt\n\tExact whitespace.\n" as string | undefined,
  error: null as Error | null,
  isLoading: false,
  refetch: vi.fn(),
  call: vi.fn(),
}));
vi.mock("@/api/hooks-execution-instructions", () => ({
  useRunExecutionInstructions: (...args: unknown[]) => {
    instructionQuery.call(...args);
    return instructionQuery;
  },
}));
const detail = {
  attempt_id: "a",
  provider_invocation_id: "inv",
  settlement_status: "succeeded",
  output_status: "settled",
  output_text: "## Answer\nThe result is ready.",
} as RunExecutionDetail;

beforeEach(() => {
  instructionQuery.data = "  Full dispatched prompt\n\tExact whitespace.\n";
  instructionQuery.error = null;
  instructionQuery.isLoading = false;
  instructionQuery.call.mockClear();
});

describe("run conversation", () => {
  it("shows complete exact dispatched instructions for the selected attempt", () => {
    render(
      <RunInstructions
        runId="run"
        attemptId="historical"
        executionMode="provider_backed"
      />,
    );
    expect(instructionQuery.call).toHaveBeenCalledWith(
      "run",
      "historical",
      true,
    );
    expect(
      screen.getByLabelText("Exact attempt instructions").textContent,
    ).toBe(instructionQuery.data);
    expect(screen.queryByText(/issued work brief/)).not.toBeInTheDocument();
  });
  it("distinguishes runtime-owned instructions and missing recorded prompts without fallback", () => {
    const view = render(
      <RunInstructions
        runId="run"
        attemptId="runtime"
        executionMode="runtime_owned"
      />,
    );
    expect(instructionQuery.call).toHaveBeenCalledWith("run", "runtime", false);
    expect(screen.getByText(/without a provider prompt/)).toBeVisible();
    expect(
      screen.queryByLabelText("Exact attempt instructions"),
    ).not.toBeInTheDocument();
    instructionQuery.error = new ApiError(404, {
      code: "dependency_blocked",
      message: "Not recorded",
      retryable: false,
      category: "core",
      details: {},
      correlationId: "test",
    });
    instructionQuery.data = undefined;
    view.rerender(
      <RunInstructions
        runId="run"
        attemptId="unrecorded"
        executionMode="provider_backed"
      />,
    );
    expect(
      screen.getByText(
        "No provider instructions were recorded for this attempt.",
      ),
    ).toBeVisible();
    expect(
      screen.queryByLabelText("Exact attempt instructions"),
    ).not.toBeInTheDocument();
  });
  it("offers explicit retry for instruction read failure", async () => {
    instructionQuery.error = new Error("offline");
    instructionQuery.data = undefined;
    render(
      <RunInstructions
        runId="run"
        attemptId="a"
        executionMode="provider_backed"
      />,
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Retry Instructions" }),
    );
    expect(instructionQuery.refetch).toHaveBeenCalledOnce();
  });
  it("keeps canonical reply inline and exact source accessible", async () => {
    render(<RunFinalResponse detail={detail} />);
    expect(screen.getByRole("heading", { name: "Answer" })).toBeVisible();
    expect(document.querySelector("pre")).toBeNull();
    await userEvent.click(screen.getByText("Response Details"));
    expect(document.querySelector("pre")?.textContent).toBe(detail.output_text);
    expect(screen.getByText(/Attempt ID: a/)).toBeVisible();
  });
  it("never treats observed or pending text as a settled final response", () => {
    const view = render(
      <RunFinalResponse detail={{ ...detail, settlement_status: "pending" }} />,
    );
    expect(
      screen.queryByRole("region", { name: "Final response" }),
    ).not.toBeInTheDocument();
    view.rerender(
      <RunFinalResponse detail={{ ...detail, output_status: "observed" }} />,
    );
    expect(
      screen.queryByRole("heading", { name: "Answer" }),
    ).not.toBeInTheDocument();
  });
  it("shows runtime-owned guidance inline with exact original text available", async () => {
    const runtime = {
      ...detail,
      execution_mode: "runtime_owned" as const,
      settlement_status: "not_applicable" as const,
      output_status: "observed" as const,
      output_text:
        "Operator requested **rework**: preserve the original constraints.",
    };
    render(<RunRuntimeResponse detail={runtime} />);
    expect(screen.getByText("rework")).toBeVisible();
    await userEvent.click(screen.getByText("Response Details"));
    expect(
      screen.getByLabelText("Exact recorded runtime output").textContent,
    ).toBe(runtime.output_text);
  });
});
