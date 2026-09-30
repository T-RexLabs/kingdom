import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { apiFetch } from "@/api/client";
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
  apiFetch: vi.fn(() => Promise.resolve({ items: [] })),
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

function renderStatus(
  executions: RunExecution[],
  props: Partial<Parameters<typeof RunLiveStatus>[0]> = {},
) {
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
  return render(<RunLiveStatus runId="run-1" nodes={nodes} {...props} />, {
    wrapper,
  });
}

describe("compact run live bar", () => {
  beforeEach(() => vi.mocked(apiFetch).mockResolvedValue({ items: [] }));
  it("shows two agents inline and expands the remaining active agents on request", async () => {
    const onSelect = vi.fn();
    renderStatus(
      [
        execution({
          node: "implementation",
          attemptId: "a",
          requiredRole: "builder",
        }),
        execution({
          node: "execution_review",
          attemptId: "b",
          requiredRole: "execution-reviewer",
        }),
        execution({
          node: "quality_validation",
          attemptId: "c",
          requiredRole: "quality-validator",
        }),
      ],
      { connected: true, onSelect },
    );
    expect(screen.getAllByRole("button")).toHaveLength(3);
    await userEvent.click(screen.getByRole("button", { name: "+1 More" }));
    expect(screen.getAllByRole("button")).toHaveLength(4);
    await userEvent.click(
      screen.getByRole("button", { name: /Quality Validator/ }),
    );
    expect(onSelect).toHaveBeenCalledWith("quality_validation");
    await userEvent.click(screen.getByRole("button", { name: "Show Less" }));
    expect(screen.getAllByRole("button")).toHaveLength(3);
  });
  it("retains exact provider event time in accessible inspection without a repeated visible timestamp", async () => {
    const absent = { availability: "not_provided" };
    vi.mocked(apiFetch).mockResolvedValue({
      items: [
        {
          id: "part",
          event_id: "event",
          sequence: 1,
          kind: "tool",
          label: "Read project file",
          tool_name: "read",
          occurred_at: "2026-06-15T10:00:00Z",
          recorded_at: "2026-06-15T10:00:02Z",
          input: {
            availability: "available",
            preview: '{"filePath":"/private/project/plan.ts"}',
          },
          output: absent,
          reasoning: absent,
          message: absent,
        },
      ],
    });
    const { container } = renderStatus(
      [
        execution({
          node: "implementation",
          attemptId: "a",
          requiredRole: "builder",
        }),
      ],
      { connected: false },
    );
    const label = await screen.findByText("Read plan.ts");
    expect(label.closest("button")).toHaveAccessibleDescription(/Occurred/);
    expect(label.closest("button")?.title).toContain("2026-06-15");
    expect(container.querySelector("time")).toBeNull();
    expect(screen.getByText("Reconnecting")).toBeVisible();
  });
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
