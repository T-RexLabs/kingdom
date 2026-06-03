import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { HorizontalBarChart } from "./horizontal-bar-chart";

describe("horizontal bar chart", () => {
  it("reuses bars and labels across value updates", () => {
    const { container, rerender } = render(
      <HorizontalBarChart
        width={520}
        data={[
          { label: "Builder", value: 42 },
          { label: "Reviewer", value: 18 },
        ]}
      />,
    );

    const firstBarBefore = container.querySelector("rect.bar");
    const firstLabelBefore = container.querySelector("text.label");
    const firstValueBefore = container.querySelector("text.value");

    expect(firstBarBefore).toBeTruthy();
    expect(firstLabelBefore).toBeTruthy();
    expect(firstValueBefore).toBeTruthy();

    rerender(
      <HorizontalBarChart
        width={520}
        data={[
          { label: "Builder", value: 55 },
          { label: "Reviewer", value: 21 },
        ]}
      />,
    );

    expect(container.querySelector("rect.bar")).toBe(firstBarBefore);
    expect(container.querySelector("text.label")).toBe(firstLabelBefore);
    expect(container.querySelector("text.value")).toBe(firstValueBefore);
    expect(container.querySelector("text.value")?.textContent).toBe("55");
  });

  it("expands left margin for long role labels", () => {
    const { container } = render(
      <HorizontalBarChart
        width={520}
        data={[
          {
            label: "Intelligence NightRaven Extended Role Label For Visibility",
            value: 42,
          },
        ]}
      />,
    );

    const chartGroup = container.querySelector("svg g");
    expect(chartGroup).toBeTruthy();
    if (!chartGroup) {
      throw new Error("expected chart group to be rendered");
    }

    const transform = chartGroup.getAttribute("transform") ?? "";
    const match = transform.match(/translate\((\d+),/);
    expect(match).toBeTruthy();
    const leftMargin = Number(match?.[1] ?? 0);
    expect(leftMargin).toBeGreaterThan(120);
    expect(leftMargin).toBeLessThan(280);
  });

  it("uses explicit empty-state height for vertical centering", () => {
    const { getByText } = render(<HorizontalBarChart width={520} data={[]} />);

    const emptyState = getByText("No data");
    expect(emptyState).toBeTruthy();
    expect(emptyState?.getAttribute("style")).toContain("height:");
  });

  it("supports empty-to-non-empty transitions without crashing", () => {
    const { rerender, getByText, getByRole } = render(
      <HorizontalBarChart width={520} data={[]} ariaLabel="Average duration by role" />,
    );

    expect(getByText("No data")).toBeInTheDocument();

    rerender(
      <HorizontalBarChart
        width={520}
        ariaLabel="Average duration by role"
        data={[{ label: "Builder", value: 42 }]}
      />,
    );

    expect(getByRole("img", { name: "Average duration by role" })).toBeInTheDocument();
  });

	it("does not render a visible bar for zero values", () => {
		const { container } = render(
			<HorizontalBarChart
				width={520}
				data={[{ label: "Builder", value: 0 }]}
			/>,
		);

		const bar = container.querySelector("rect.bar");
		expect(bar).toBeTruthy();
		expect(Number(bar?.getAttribute("width") ?? Number.NaN)).toBe(0);
	});

  it("exposes an accessible chart label", () => {
    const { getByRole } = render(
      <HorizontalBarChart
        ariaLabel="Average duration by role"
        width={520}
        data={[{ label: "Builder", value: 42 }]}
      />,
    );

    expect(getByRole("img", { name: "Average duration by role" })).toBeInTheDocument();
  });
});
