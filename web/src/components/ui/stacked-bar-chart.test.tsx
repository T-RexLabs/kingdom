import { act, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StackedBarChart } from "./stacked-bar-chart";

describe("stacked bar chart", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("lets the legend wrap outside the plot at narrow widths", () => {
    const { container } = render(
      <StackedBarChart
        width={180}
        height={250}
        data={[{
          label: "12:00",
          values: [
            { key: "succeeded", value: 3, color: "#22c55e" },
            { key: "failed", value: 1, color: "#ef4444" },
            { key: "cancelled", value: 1, color: "#a855f7" },
          ],
        }]}
      />,
    );

    const legend = container.querySelector(".legend");
    expect(legend?.closest("svg")).toBeNull();
    expect(legend).toHaveClass("flex-wrap");
    expect(screen.getByText("Succeeded")).toBeVisible();
    expect(screen.getByText("Failed")).toBeVisible();
    expect(screen.getByText("Cancelled")).toBeVisible();
  });

  it("sizes the plot to the remaining container without a minimum-width overflow", () => {
    let resize!: ResizeObserverCallback;
    vi.stubGlobal("ResizeObserver", class {
      constructor(callback: ResizeObserverCallback) { resize = callback; }
      observe() {}
      disconnect() {}
    });
    const { container } = render(
      <StackedBarChart data={[{
        label: "12:00",
        values: [{ key: "succeeded", value: 3, color: "#22c55e" }],
      }]} />,
    );

    act(() => resize(
      [{ contentRect: { width: 180, height: 160 } }] as ResizeObserverEntry[],
      {} as ResizeObserver,
    ));
    const svg = screen.getByRole("img");
    expect(svg).toHaveAttribute("width", "180");
    expect(svg).toHaveAttribute("height", "160");
    const chartRoot = container.querySelector(".chart-root")!;
    const left = Number(chartRoot.getAttribute("transform")?.match(/translate\(([^,]+)/)?.[1]);
    for (const bar of container.querySelectorAll(".stack-segment")) {
      expect(left + Number(bar.getAttribute("x")) + Number(bar.getAttribute("width"))).toBeLessThanOrEqual(180);
    }
  });

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
