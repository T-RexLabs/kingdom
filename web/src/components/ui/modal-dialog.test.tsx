import { render, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { OVERLAY_ROOT_ID, OVERLAY_Z_INDEX, SCENE_ROOT_ID } from "@/lib/overlay-system";
import { ModalDialog } from "./modal-dialog";
import { PopoverListbox } from "./popover-listbox";

describe("modal dialog", () => {
	it("moves focus into the dialog before hiding the scene", async () => {
		const user = userEvent.setup();
		const sceneRoot = document.getElementById(SCENE_ROOT_ID) as HTMLElement;
		const overlayRoot = document.getElementById(OVERLAY_ROOT_ID) as HTMLElement;
		let focusWhenSceneWasHidden: Element | null = null;
		const originalSetAttribute = sceneRoot.setAttribute.bind(sceneRoot);
		vi.spyOn(sceneRoot, "setAttribute").mockImplementation((name, value) => {
			if (name === "aria-hidden" && value === "true") {
				focusWhenSceneWasHidden = document.activeElement;
			}
			originalSetAttribute(name, value);
		});

		function Harness() {
			const [open, setOpen] = useState(false);
			return (
				<>
					<button type="button" onClick={() => setOpen(true)}>Open modal</button>
					<ModalDialog open={open} onClose={() => setOpen(false)} ariaLabel="Focus modal" panelClassName="w-full max-w-sm">
						<button type="button">Primary action</button>
					</ModalDialog>
				</>
			);
		}

		const view = render(<Harness />, { container: sceneRoot });
		const opener = view.getByRole("button", { name: "Open modal" });
		await user.click(opener);

		const action = within(overlayRoot).getByRole("button", { name: "Primary action" });
		expect(action).toHaveFocus();
		expect(overlayRoot).toContainElement(focusWhenSceneWasHidden as HTMLElement | null);
		expect(sceneRoot).not.toContainElement(focusWhenSceneWasHidden as HTMLElement | null);
	});

	it("renders into the shared overlay root and obscures only the scene root", () => {
		const view = render(
			<ModalDialog open onClose={() => {}} ariaLabel="Focus modal" panelClassName="w-full max-w-sm">
				<button type="button">Primary action</button>
			</ModalDialog>,
		);

		const overlayRoot = document.getElementById(OVERLAY_ROOT_ID);
		const sceneRoot = document.getElementById(SCENE_ROOT_ID);
		expect(overlayRoot).not.toBeNull();
		expect(sceneRoot).not.toBeNull();

		const dialog = within(overlayRoot as HTMLElement).getByRole("dialog", { name: "Focus modal" });
		expect(dialog).toBeInTheDocument();
		expect(dialog.parentElement).toHaveStyle({ zIndex: `${OVERLAY_Z_INDEX.modal}` });
		expect(sceneRoot).toHaveAttribute("inert");
		expect(sceneRoot).toHaveAttribute("aria-hidden", "true");
		expect(sceneRoot).toHaveAttribute("data-modal-obscured", "true");
		expect(document.body.dataset.modalOpen).toBe("true");
		expect(overlayRoot).not.toHaveAttribute("aria-hidden");

		view.unmount();

		expect(sceneRoot).not.toHaveAttribute("inert");
		expect(sceneRoot).not.toHaveAttribute("aria-hidden");
		expect(sceneRoot).not.toHaveAttribute("data-modal-obscured");
		expect(document.body).not.toHaveAttribute("data-modal-open");
	});

	it("restores focus after the scene root is unobscured", async () => {
		const view = render(
			<>
				<button type="button">Open modal</button>
				<ModalDialog open onClose={() => {}} ariaLabel="Focus modal" panelClassName="w-full max-w-sm">
					<button type="button">Primary action</button>
				</ModalDialog>
			</>,
		);

		const opener = view.getByRole("button", { name: "Open modal" });
		opener.focus();
		view.rerender(
			<>
				<button type="button">Open modal</button>
				<ModalDialog open={false} onClose={() => {}} ariaLabel="Focus modal" panelClassName="w-full max-w-sm">
					<button type="button">Primary action</button>
				</ModalDialog>
			</>,
		);

		await waitFor(() => {
			expect(view.getByRole("button", { name: "Open modal" })).toHaveFocus();
		});
	});

	it("keeps focus trapped when a modal-owned popover is open", async () => {
		const user = userEvent.setup();
		render(
			<>
				<button data-testid="outside-action" type="button">Outside action</button>
				<ModalDialog open onClose={() => {}} ariaLabel="Focus modal" panelClassName="w-full max-w-sm">
					<div className="space-y-3">
						<PopoverListbox
							triggerId="modal-popover"
							triggerRole="combobox"
							triggerAriaLabel="Model selector"
							triggerContent={<span>Select model</span>}
							items={[{ id: "model-a", label: "Model A" }]}
							selectedId={null}
							onSelect={() => {}}
							search={{ value: "", onChange: () => {}, placeholder: "Search models" }}
							footer={<button type="button">Popover action</button>}
						/>
						<button type="button">Modal secondary action</button>
					</div>
				</ModalDialog>
			</>,
		);

		await user.click(within(document.getElementById(OVERLAY_ROOT_ID) as HTMLElement).getByRole("combobox", { name: "Model selector" }));

		const searchInput = within(document.getElementById(OVERLAY_ROOT_ID) as HTMLElement).getByRole("textbox", { name: "Search models" });
		await waitFor(() => {
			expect(searchInput).toHaveFocus();
		});

		await user.tab();
		expect(within(document.getElementById(OVERLAY_ROOT_ID) as HTMLElement).getByRole("button", { name: "Popover action" })).toHaveFocus();

		await user.tab();
		expect((document.getElementById(OVERLAY_ROOT_ID) as HTMLElement)).toContainElement(document.activeElement as HTMLElement | null);
		expect(document.body).not.toHaveFocus();
		expect(document.querySelector('[data-testid="outside-action"]') as HTMLElement | null).not.toHaveFocus();
	});
});
