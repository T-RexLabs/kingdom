import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PopoverListbox } from "./popover-listbox";

describe("popover listbox", () => {
	it("keeps a stable trigger label while still rendering the current value", () => {
		render(
			<PopoverListbox
				triggerId="step-selector"
				triggerRole="button"
				triggerAriaLabel="Current step"
				triggerContent={<span>Plan Drafting</span>}
				items={[{ id: "plan_drafting", label: "Plan Drafting" }]}
				selectedId="plan_drafting"
				onSelect={() => {}}
			/>,
		);

		const button = screen.getByRole("button", { name: "Current step" });
		expect(button).toHaveTextContent("Plan Drafting");
	});
});
