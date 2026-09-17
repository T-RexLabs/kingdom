import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
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

	it("keeps the shared bottom-start and reference-width defaults", async () => {
		const user = userEvent.setup();
		render(
			<PopoverListbox
				triggerId="step-selector"
				triggerRole="button"
				triggerContent="Plan Drafting"
				items={[{ id: "plan_drafting", label: "Plan Drafting" }]}
				selectedId="plan_drafting"
				onSelect={() => {}}
			/>,
		);

		await user.click(screen.getByRole("button", { name: "Plan Drafting" }));
		const panel = screen.getAllByRole("listbox")[0];

		expect(panel).toHaveAttribute("data-popover-placement", "bottom-start");
		await waitFor(() => expect(panel).toHaveStyle({ minWidth: "160px" }));
		expect(panel?.style.maxWidth).toBe("");
	});

	it("navigates plain-button options deterministically", async () => {
		const user = userEvent.setup();
		const onSelect = vi.fn();
		render(
			<PopoverListbox
				triggerId="step-selector"
				triggerRole="button"
				triggerContent="Select a step"
				items={[
					{ id: "unavailable", label: "Unavailable", disabled: true, group: "Unavailable" },
					{ id: "plan", label: "Plan", group: "Available" },
					{ id: "build", label: "Build", group: "Available" },
				]}
				selectedId={null}
				onSelect={onSelect}
				groupBy
			/>,
		);

		await user.click(screen.getByRole("button", { name: "Select a step" }));
		await user.keyboard("{Home}{ArrowDown}{ArrowDown}{Enter}");

		expect(onSelect).toHaveBeenCalledOnce();
		expect(onSelect).toHaveBeenCalledWith("plan");
	});
});
