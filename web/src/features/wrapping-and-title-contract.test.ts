import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const rootDir = dirname(fileURLToPath(import.meta.url));

function readFeature(path: string): string {
  return readFileSync(resolve(rootDir, path), "utf8");
}

describe("wrapping and title contract", () => {
  it("applies wrap-safe utility across message and plan surfaces", () => {
    expect(readFeature("runs/mailbox-panel.tsx")).toContain("text-wrap-anywhere");
    expect(readFeature("runs/current-plan-panel.tsx")).toContain("text-wrap-anywhere");
    expect(readFeature("runs/run-detail-page.tsx")).toContain("text-wrap-anywhere");
    expect(readFeature("runs/agent-detail-panel.tsx")).toContain("text-wrap-anywhere");
    expect(readFeature("runs/run-execution-surface.tsx")).toContain("text-wrap-anywhere");
    expect(readFeature("runs/run-executions-panel.tsx")).toContain("text-wrap-anywhere");
    expect(readFeature("runs/governance-actions.tsx")).toContain("text-wrap-anywhere");
  });

  it("prefers decree title over run id in recent run labels", () => {
    expect(readFeature("decrees/recent-decree-runs-card.tsx")).toContain("run.decree_title || run.run_id");
    expect(readFeature("dashboard/dashboard-page.tsx")).toContain("run.decree_title || run.run_id");
  });

  it("keeps dashboard trend cluster right-aligned and run metadata wrap-safe", () => {
    expect(readFeature("dashboard/dashboard-page.tsx")).toContain("w-full");
    expect(readFeature("runs/run-detail-page.tsx")).toContain("flex-wrap");
  });
});
