import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { RunExecution, RunNodeExecution } from "@/api/hooks";
import { RunLiveStatus } from "./run-live-status";

vi.mock("@/theme", async () => {
  const actual = await vi.importActual<typeof import("@/theme")>("@/theme");
  return {
    ...actual,
    useTheme: () => ({ theme: actual.builtInThemes[3] }),
    useRoleIdentity: (role: string) =>
      actual.roleIdentity(actual.builtInThemes[3], role),
    ThemeIcon: () => <span aria-hidden="true" />,
  };
});

vi.mock("@/api/client", () => ({
  apiFetch: () => Promise.resolve({ items: [] }),
}));

function execution(args: {
  node: string;
  attemptId: string;
  requiredRole: string;
  activityAt?: string;
  status?: RunExecution["status"];
  executionMode?: RunExecution["execution_mode"];
}): RunExecution {
  return {
    node_id: args.node,
    attempt_id: args.attemptId,
    node_run_id: `${args.node}-run`,
    attempt: 1,
    agent_id: "agent",
    required_role: args.requiredRole,
    execution_mode: args.executionMode ?? "provider_backed",
    status: args.status ?? "running",
    activity_at: args.activityAt ?? "2026-06-15T10:00:00Z",
  } as RunExecution;
}

function renderStatus(executions: RunExecution[]) {
  const qc = new QueryClient();
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
  const nodes: RunNodeExecution[] = executions.map((current_execution) => ({
    node_id: current_execution.node_id,
    required_role: current_execution.required_role,
    execution_ownership: current_execution.execution_mode,
    status: "running",
    attempt_count: 1,
    current_execution,
  }));
  return render(<RunLiveStatus runId="run-1" nodes={nodes} />, { wrapper });
}

describe("compact run live bar", () => {
  it("omits settled and runtime-owned attempts", () => {
    const { container } = renderStatus([
      execution({
        node: "implementation",
        attemptId: "a",
        requiredRole: "builder",
        status: "succeeded",
      }),
      execution({
        node: "work_intake",
        attemptId: "b",
        requiredRole: "governance",
        executionMode: "runtime_owned",
      }),
    ]);
    expect(container).toBeEmptyDOMElement();
  });
  it("keeps starting and verifying work visible without a running pulse", () => {
    renderStatus([
      execution({
        node: "implementation",
        attemptId: "a",
        requiredRole: "builder",
        status: "starting",
      }),
    ]);
    expect(screen.getByText("Starting")).toBeInTheDocument();
    expect(screen.getByTestId("run-live-status")).not.toHaveClass(
      "animate-pulse",
    );
  });
});
