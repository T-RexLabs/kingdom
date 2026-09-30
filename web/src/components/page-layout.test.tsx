import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PageContent, PageHeader } from "./page-layout";

describe("page layout", () => {
  it("uses fixed chrome height so header separator aligns with sidebar", () => {
    render(
      <PageHeader>
        <div>Header content</div>
      </PageHeader>,
    );

    const header = screen.getByText("Header content").closest("header");
    expect(header).toHaveClass("h-[76px]");
    expect(header).not.toHaveClass("min-h-[76px]");
  });

  // An absolutely positioned descendant only gets clipped by a scroll viewport
  // that is its containing block. Without `relative` here, `sr-only` labels
  // (which are `position: absolute`) escape the page scroller and stretch the
  // document, producing a second page-level scrollbar beside the page one.
  it("makes the scroll viewport a containing block so hidden labels cannot escape it", () => {
    render(
      <PageContent>
        <span className="sr-only">Run scope</span>
      </PageContent>,
    );

    const content = screen.getByText("Run scope").closest("div");
    expect(content).toHaveClass("relative");
    expect(content).toHaveClass("overflow-auto");
    expect(content).toHaveClass("min-h-0");
  });
});
