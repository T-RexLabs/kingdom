import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PageHeader } from "./page-layout";

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
});
