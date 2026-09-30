import { useEffect } from "react";
import { render, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { OVERLAY_ROOT_ID, OVERLAY_Z_INDEX } from "@/lib/overlay-system";
import { ModalDialog } from "@/components/ui/modal-dialog";
import { useNotifications } from "./notification-context";
import { NotificationProvider } from "./notification-provider";
import { NotificationStack } from "./notification-stack";

function NotificationSeed() {
	const { addNotification } = useNotifications();

	useEffect(() => {
		addNotification({
			id: "seed",
			title: "Approval required",
			status: "info",
		});
	}, [addNotification]);

	return null;
}

describe("notification stack", () => {
	it("renders into the shared overlay root below blocking modals", async () => {
		render(
			<NotificationProvider>
				<NotificationSeed />
				<NotificationStack />
				<ModalDialog open onClose={() => {}} ariaLabel="Blocking modal" panelClassName="w-full max-w-sm">
					<button type="button">Continue</button>
				</ModalDialog>
			</NotificationProvider>,
		);

		const overlayRoot = document.getElementById(OVERLAY_ROOT_ID);
		expect(overlayRoot).not.toBeNull();

		const log = await within(overlayRoot as HTMLElement).findByRole("log", { name: "Notifications" });
		const dialog = within(overlayRoot as HTMLElement).getByRole("dialog", { name: "Blocking modal" });
		expect(log).toBeInTheDocument();
		expect(within(log).getByText("Approval required")).toBeInTheDocument();
		expect(log).toHaveStyle({ zIndex: `${OVERLAY_Z_INDEX.notification}` });
		expect(dialog.parentElement).toHaveStyle({ zIndex: `${OVERLAY_Z_INDEX.modal}` });
		expect(OVERLAY_Z_INDEX.notification).toBeLessThan(OVERLAY_Z_INDEX.modal);
	});
});
