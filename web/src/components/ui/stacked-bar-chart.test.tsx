import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { StackedBarChart } from "./stacked-bar-chart";
import { computeLegendLayout } from "./stacked-bar-chart-layout";

describe("stacked bar chart", () => {
  it("reuses bars, grid lines, and legend items across updates", () => {
    const { container, rerender } = render(
      <StackedBarChart
        ariaLabel="Run outcomes over time"
        width={520}
        height={220}
        data={[
          {
            label: "12:00",
            values: [
              { key: "succeeded", value: 3, color: "#22c55e" },
              { key: "failed", value: 1, color: "#ef4444" },
            ],
          },
          {
            label: "13:00",
            values: [
              { key: "succeeded", value: 2, color: "#22c55e" },
              { key: "failed", value: 2, color: "#ef4444" },
            ],
          },
        ]}
      />,
    );

    const firstSegmentBefore = container.querySelector(".stack-segment");
    const firstGridLineBefore = container.querySelector(".grid-line");
    const firstLegendItemBefore = container.querySelector(".legend-item");

    expect(firstSegmentBefore).toBeTruthy();
    expect(firstGridLineBefore).toBeTruthy();
    expect(firstLegendItemBefore).toBeTruthy();

    rerender(
      <StackedBarChart
        ariaLabel="Run outcomes over time"
        width={520}
        height={220}
        data={[
          {
            label: "12:00",
            values: [
              { key: "succeeded", value: 4, color: "#22c55e" },
              { key: "failed", value: 2, color: "#ef4444" },
            ],
          },
          {
            label: "13:00",
            values: [
              { key: "succeeded", value: 1, color: "#22c55e" },
              { key: "failed", value: 3, color: "#ef4444" },
            ],
          },
        ]}
      />,
    );

    expect(container.querySelector(".stack-segment")).toBe(firstSegmentBefore);
    expect(container.querySelector(".grid-line")).toBe(firstGridLineBefore);
    expect(container.querySelector(".legend-item")).toBe(firstLegendItemBefore);
  });

  it("computes uniform spacing between legend labels", () => {
    const keys = ["Succeeded", "Failed", "Cancelled"];
    const widths = new Map<string, number>([
      ["Succeeded", 54],
      ["Failed", 35],
      ["Cancelled", 49],
    ]);

    const layout = computeLegendLayout({
      keys,
      chartLeft: 34,
      chartWidth: 460,
      markerWidth: 8,
      markerToTextGap: 8,
      itemGap: 18,
      measureLabel: (label) => widths.get(label) ?? 0,
    });

    expect(layout.items).toHaveLength(3);
    expect(layout.items[0].x).toBe(34 + (460 - layout.totalWidth) / 2);

    const firstGap = layout.items[1].x - (layout.items[0].x + 8 + 8 + widths.get("Succeeded")!);
    const secondGap = layout.items[2].x - (layout.items[1].x + 8 + 8 + widths.get("Failed")!);

    expect(firstGap).toBe(18);
    expect(secondGap).toBe(18);
  });

  it("renders legend labels in title case", () => {
    render(
      <StackedBarChart
        ariaLabel="Run outcomes over time"
        width={520}
        height={220}
        data={[
          {
            label: "12:00",
            values: [
              { key: "succeeded", value: 3, color: "#22c55e" },
              { key: "failed", value: 1, color: "#ef4444" },
              { key: "cancelled", value: 1, color: "#a855f7" },
            ],
          },
        ]}
      />,
    );

    expect(screen.getByText("Succeeded")).toBeInTheDocument();
    expect(screen.getByText("Failed")).toBeInTheDocument();
    expect(screen.getByText("Cancelled")).toBeInTheDocument();
    expect(screen.queryByText("succeeded")).not.toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Run outcomes over time" })).toBeInTheDocument();
  });

  it("supports empty-to-non-empty transitions without crashing", () => {
    const { rerender, getByText } = render(
      <StackedBarChart
        ariaLabel="Run outcomes over time"
        width={520}
        height={220}
        data={[]}
      />,
    );

    expect(getByText("No data")).toBeInTheDocument();

    rerender(
      <StackedBarChart
        ariaLabel="Run outcomes over time"
        width={520}
        height={220}
        data={[
          {
            label: "12:00",
            values: [
              { key: "succeeded", value: 3, color: "#22c55e" },
              { key: "failed", value: 1, color: "#ef4444" },
            ],
          },
        ]}
      />,
    );

    expect(screen.getByRole("img", { name: "Run outcomes over time" })).toBeInTheDocument();
  });

	it("renders integer y-axis ticks for count charts when requested", () => {
		const { container } = render(
			<StackedBarChart
				ariaLabel="Run outcomes over time"
				width={520}
				height={220}
				integerTicks
				data={[
					{
						label: "12:00",
						values: [
							{ key: "succeeded", value: 1, color: "#22c55e" },
							{ key: "failed", value: 1, color: "#ef4444" },
						],
					},
					{
						label: "13:00",
						values: [
							{ key: "succeeded", value: 0, color: "#22c55e" },
							{ key: "failed", value: 1, color: "#ef4444" },
						],
					},
				]}
			/>,
		);

		const ticks = Array.from(container.querySelectorAll("text.y-axis-tick"))
			.map((node) => Number(node.textContent ?? "NaN"));
		expect(ticks.length).toBeGreaterThan(0);
		expect(ticks.every((tick) => Number.isInteger(tick))).toBe(true);
		expect(ticks).not.toContain(0.5);
		expect(ticks).not.toContain(1.5);
	});
});
