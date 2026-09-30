import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const rootDir = dirname(fileURLToPath(import.meta.url));

/**
 * Pages reachable from the sidebar. They are peers in the operator's mental
 * model, so their headers are typeset identically: one gold display title and
 * one muted sentence under it. Connections drifted to a smaller, ungilded
 * title with an icon precisely because it was missing from this list.
 */
const sectionPages = [
  "dashboard/dashboard-page.tsx",
  "runs/runs-page.tsx",
  "decrees/decrees-workspace-layout.tsx",
  "agents/agents-page.tsx",
  "connections/connections-page.tsx",
  "realm-analytics/realm-analytics-page.tsx",
];

// Run detail is a record page, not a section: its header carries a back arrow
// and the decree's own title, so it opts out of the title treatment above.
const pagesWithHeaderChrome = [...sectionPages, "runs/run-detail-page.tsx"];

function headerMarkup(page: string): string {
  const content = readFileSync(resolve(rootDir, page), "utf8");
  const header = content.match(/<PageHeader>([\s\S]*?)<\/PageHeader>/);
  expect(header, `${page} should render a PageHeader`).not.toBeNull();
  return header![1];
}

describe("page header contract", () => {
  it("applies shared header separator across major pages", () => {
    for (const page of pagesWithHeaderChrome) {
      expect(readFileSync(resolve(rootDir, page), "utf8")).toContain("PageHeader");
    }
  });

  it("keeps full-width header row alignment wrappers", () => {
    for (const page of pagesWithHeaderChrome) {
      expect(headerMarkup(page), page).toContain("w-full");
    }
  });

  it("typesets every section title the same way", () => {
    for (const page of sectionPages) {
      const header = headerMarkup(page);
      expect(header, page).toMatch(/<h1 className="font-display text-2xl font-bold text-gold">/);
      expect(header, page).toContain('<p className="text-sm text-foreground-muted">');
    }
  });

  it("uses matching fixed top chrome height between sidebar and page header", () => {
    const appShell = readFileSync(resolve(rootDir, "../components/app-shell.tsx"), "utf8");
    const pageLayout = readFileSync(resolve(rootDir, "../components/page-layout.tsx"), "utf8");

    expect(appShell).toContain("h-[76px]");
    expect(pageLayout).toContain("h-[76px]");
  });
});
