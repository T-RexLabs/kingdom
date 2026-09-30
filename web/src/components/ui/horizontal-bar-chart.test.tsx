import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { HorizontalBarChart } from "./horizontal-bar-chart";

describe("horizontal bar chart", () => {
  it("retains long labels and values in a responsive chart without fixed SVG margins", () => {
    const label = "Architecture Advisor With a Long Department Name";
    const value = "1,234,567,890,123";
    const { container, getByText, getByRole } = render(
      <HorizontalBarChart
        width={180}
        data={[{ label, value: 1234567890123 }, { label: "Reviewer", value: 0 }]}
        formatValue={() => value}
      />,
    );

    expect(getByText(label)).toBeVisible();
    expect(getByText(label)).toHaveClass("break-words");
    expect(container.querySelector("svg")).toBeNull();
    expect(getByRole("img")).toHaveClass("max-w-full");
    const values = Array.from(container.querySelectorAll(".value"));
    expect(values[0]).toHaveTextContent(value);
    expect(values[0]).toHaveClass("break-all");
    const bars = container.querySelectorAll<HTMLElement>(".bar");
    expect(bars[0].style.width).toBe("100%");
    expect(bars[1].style.width).toBe("0%");
  });

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

    const firstBarBefore = container.querySelector(".bar");
    const firstLabelBefore = container.querySelector(".label");
    const firstValueBefore = container.querySelector(".value");

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

    expect(container.querySelector(".bar")).toBe(firstBarBefore);
    expect(container.querySelector(".label")).toBe(firstLabelBefore);
    expect(container.querySelector(".value")).toBe(firstValueBefore);
    expect(container.querySelector(".value")?.textContent).toBe("55");
  });

  it("preserves proportional bars with long role labels", () => {
    const { container, getByRole } = render(
      <HorizontalBarChart
        width={520}
        data={[
          {
            label: "Architecture Advisor With an Extended Role Label",
            value: 42,
          },
          { label: "Reviewer", value: 21 },
        ]}
      />,
    );

    const bars = container.querySelectorAll<HTMLElement>(".bar");
    expect(bars[0].style.width).toBe("100%");
    expect(bars[1].style.width).toBe("50%");
    expect(getByRole("img")).toHaveAccessibleDescription(
      "Architecture Advisor With an Extended Role Label: 42; Reviewer: 21",
    );
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

		const bar = container.querySelector<HTMLElement>(".bar");
		expect(bar).toBeTruthy();
		expect(bar?.style.width).toBe("0%");
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
