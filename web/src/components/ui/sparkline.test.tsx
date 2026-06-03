import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Sparkline } from "./sparkline";

describe("sparkline", () => {
  it("reuses existing svg elements across state-only updates", () => {
    const { container, rerender } = render(
      <Sparkline
        ariaLabel="Run trend over the last 24 hours"
        data={[1, 3, 2, 4]}
        width={100}
        height={28}
      />,
    );

    const areaBefore = container.querySelector(".sparkline-area");
    const lineBefore = container.querySelector(".sparkline-line");
    const dotBefore = container.querySelector(".sparkline-dot");

    expect(areaBefore).toBeTruthy();
    expect(lineBefore).toBeTruthy();
    expect(dotBefore).toBeTruthy();

    rerender(
      <Sparkline
        ariaLabel="Run trend over the last 24 hours"
        data={[2, 4, 3, 5]}
        width={100}
        height={28}
      />,
    );

    expect(container.querySelector(".sparkline-area")).toBe(areaBefore);
    expect(container.querySelector(".sparkline-line")).toBe(lineBefore);
    expect(container.querySelector(".sparkline-dot")).toBe(dotBefore);
  });

  it("exposes an accessible chart label", () => {
    render(
      <Sparkline
        ariaLabel="Run trend over the last 24 hours"
        data={[1, 3, 2, 4]}
        width={100}
        height={28}
      />,
    );

    expect(screen.getByRole("img", { name: "Run trend over the last 24 hours" })).toBeInTheDocument();
  });

  it("supports empty-to-non-empty transitions without crashing", () => {
    const { rerender, getByText } = render(
      <Sparkline
        ariaLabel="Run trend over the last 24 hours"
        data={[]}
        width={100}
        height={28}
      />,
    );

    expect(getByText("--")).toBeInTheDocument();

    rerender(
      <Sparkline
        ariaLabel="Run trend over the last 24 hours"
        data={[1, 3, 2, 4]}
        width={100}
        height={28}
      />,
    );

    expect(screen.getByRole("img", { name: "Run trend over the last 24 hours" })).toBeInTheDocument();
  });
});
