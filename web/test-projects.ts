import { defaultExclude, type TestProjectConfiguration } from "vitest/config";

// These tests exercise data or configuration without browser APIs, including
// in their runtime imports. Keep tests that import the browser API client in
// the DOM project: its API base is read from document at module initialization.
export const nodeTestFiles = [
  "dev-backend.test.ts",
  "test-workers.test.ts",
  "test-projects.test.ts",
  "src/api/activity-history.test.ts",
  "src/features/activity/activity-tool-summary.test.ts",
  "src/features/activity/activity-transport-regression.test.ts",
  "src/features/activity/activity-view.test.ts",
  "src/features/activity/activity-virtual-state.test.ts",
  "src/features/agents/agent-availability.test.ts",
  "src/features/connections/external-source-status.test.ts",
  "src/features/connections/github-url.test.ts",
  "src/features/runs/governance-actions.test.ts",
  "src/features/runs/manual-governance-state.test.ts",
  "src/features/runs/run-detail-tabs.test.ts",
  "src/features/runs/run-execution-presentation.test.ts",
  "src/features/runs/run-execution-selection.test.ts",
  "src/features/runs/run-graph-model.test.ts",
  "src/features/runs/run-live-activity.test.ts",
  "src/features/runs/run-live-execution-projection.test.ts",
  "src/features/runs/run-status-semantics.test.ts",
  "src/features/runs/timeline-milestones.test.ts",
  "src/features/work-orders/parse-streaming-question.test.ts",
  "src/features/work-orders/planner-message-pages.test.ts",
  "src/features/work-orders/planner-operation-controller.test.ts",
  "src/features/work-orders/planner-query-keys.test.ts",
  "src/features/work-orders/planner-transcript-view-model.test.ts",
  "src/features/work-orders/provider-model-selection.test.ts",
  "src/features/work-orders/strip-trailing-question.test.ts",
  "src/features/work-orders/work-order-route-search.test.ts",
  "src/lib/date-time.test.ts",
  "src/lib/idempotency.test.ts",
  "src/lib/status-system.test.ts",
  "src/lib/utils.test.ts",
];

export const testProjects: TestProjectConfiguration[] = [
  {
    extends: true,
    test: {
      name: "node",
      environment: "node",
      include: nodeTestFiles,
      isolate: true,
    },
  },
  {
    extends: true,
    test: {
      name: "dom",
      // Preserve jsdom storage when Node defines its own storage accessors.
      environment: "./src/test/jsdom-storage-env.ts",
      setupFiles: "./src/test/setup.ts",
      exclude: [...defaultExclude, ...nodeTestFiles],
      isolate: true,
    },
  },
];
