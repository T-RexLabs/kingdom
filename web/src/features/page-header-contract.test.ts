import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const rootDir = dirname(fileURLToPath(import.meta.url));

const pages = [
  "dashboard/dashboard-page.tsx",
  "runs/runs-page.tsx",
  "decrees/decrees-workspace-layout.tsx",
  "agents/agents-page.tsx",
  "runs/run-detail-page.tsx",
  "realm-analytics/realm-analytics-page.tsx",
];

describe("page header contract", () => {
  it("applies shared header separator across major pages", () => {
    for (const page of pages) {
      const content = readFileSync(resolve(rootDir, page), "utf8");
      expect(content).toContain("PageHeader");
    }
  });

  it("keeps full-width header row alignment wrappers", () => {
    for (const page of pages) {
      const content = readFileSync(resolve(rootDir, page), "utf8");
      expect(content).toContain("w-full");
    }
  });

  it("uses matching fixed top chrome height between sidebar and page header", () => {
    const appShell = readFileSync(resolve(rootDir, "../components/app-shell.tsx"), "utf8");
    const pageLayout = readFileSync(resolve(rootDir, "../components/page-layout.tsx"), "utf8");

    expect(appShell).toContain("h-[76px]");
    expect(pageLayout).toContain("h-[76px]");
  });
});
