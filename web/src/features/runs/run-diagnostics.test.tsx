import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import type {
  RunExecutionDetail,
  RunNodeAttemptHistoryItem,
} from "@/api/hooks";
import { RunDiagnostics } from "./run-diagnostics";

const detail = {
  attempt_id: "attempt-a",
  node_run_id: "node-run-a",
  node_id: "implementation",
  status: "cancelled",
  execution_mode: "provider_backed",
  queued_at: "2026-09-30T10:00:00Z",
  started_at: "2026-09-30T10:01:00Z",
  provider_invocation_id: "inv-a",
  provider_session_id: "session-a",
  failure_code: "provider_cancelled",
  failure_text: "Exact retained failure detail.",
  settlement_status: "cancelled",
  output_status: "settled",
  output_text: "This answer must remain only in conversation.",
} as RunExecutionDetail;

it("retains exact failure and provenance without duplicating the response or old outcome panel", () => {
  const history = {
    ...detail,
    attempt: 1,
    superseded: true,
    retry_relationships: [],
  } as unknown as RunNodeAttemptHistoryItem;
  render(
    <RunDiagnostics
      detail={detail}
      history={history}
      currentAttemptId="attempt-next"
      isLoading={false}
      errorMessage={null}
      onRetry={vi.fn()}
    />,
  );
  expect(screen.getByText("Exact retained failure detail.")).toBeVisible();
  expect(screen.getByText("provider_cancelled")).toBeVisible();
  expect(screen.getByText("attempt-a")).toBeVisible();
  expect(screen.getByText("inv-a")).toBeVisible();
  expect(screen.getByText("Superseded")).toBeVisible();
  expect(screen.queryByText(detail.output_text!)).not.toBeInTheDocument();
  expect(screen.queryByText("Outcome")).not.toBeInTheDocument();
  expect(screen.queryByText("Settled Response")).not.toBeInTheDocument();
});

it("offers retry when the exact attempt read fails without pretending no attempt exists", async () => {
  const retry = vi.fn();
  render(
    <RunDiagnostics
      detail={null}
      isLoading={false}
      errorMessage="Exact attempt unavailable"
      onRetry={retry}
    />,
  );
  expect(screen.getByRole("alert")).toHaveTextContent(
    "Exact attempt unavailable",
  );
  expect(
    screen.queryByText(/has not recorded an attempt/),
  ).not.toBeInTheDocument();
  await userEvent.click(
    screen.getByRole("button", { name: "Retry Diagnostics" }),
  );
  expect(retry).toHaveBeenCalledOnce();
});

it("retains exact runtime-owned plan decision output behind Recorded Output", async () => {
  const runtime = {
    ...detail,
    node_id: "plan_decision",
    status: "succeeded" as const,
    execution_mode: "runtime_owned" as const,
    provider_invocation_id: undefined,
    provider_session_id: undefined,
    settlement_status: "not_applicable" as const,
    output_status: "observed" as const,
    output_text:
      "Operator requested rework: keep the **original** constraints.",
  };
  render(
    <RunDiagnostics
      detail={runtime}
      isLoading={false}
      errorMessage={null}
      onRetry={vi.fn()}
    />,
  );
  expect(
    screen.getByLabelText("Exact recorded runtime output"),
  ).not.toBeVisible();
  await userEvent.click(screen.getByText("Recorded Output"));
  expect(screen.getByLabelText("Exact recorded runtime output")).toBeVisible();
  expect(
    screen.getByLabelText("Exact recorded runtime output").textContent,
  ).toBe(runtime.output_text);
  expect(screen.queryByText("Final Response")).not.toBeInTheDocument();
});

it("uses fresh exact detail over stale history while retaining history-only lineage", () => {
  const history = {
    ...detail,
    status: "running",
    provider_invocation_id: undefined,
    provider_session_id: undefined,
    superseded: true,
    retry_relationships: [],
  } as unknown as RunNodeAttemptHistoryItem;
  const fresh = {
    ...detail,
    status: "succeeded" as const,
    settlement_status: "succeeded" as const,
    failure_code: undefined,
    failure_text: undefined,
  };
  render(
    <RunDiagnostics
      detail={fresh}
      history={history}
      currentAttemptId="next"
      isLoading={false}
      errorMessage={null}
      onRetry={vi.fn()}
    />,
  );
  expect(screen.queryByText("Active")).not.toBeInTheDocument();
  expect(screen.getAllByText("Succeeded")).toHaveLength(2);
  expect(screen.getByText("inv-a")).toBeVisible();
  expect(screen.getByText("session-a")).toBeVisible();
  expect(screen.getByText("Superseded")).toBeVisible();
});
