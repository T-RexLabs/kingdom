export const SCENE_ROOT_ID = "scene-root";
export const OVERLAY_ROOT_ID = "overlay-root";
export const MODAL_PANEL_ATTRIBUTE = "data-modal-panel";

export const OVERLAY_Z_INDEX = {
	notification: 500,
	popover: 550,
	modal: 600,
	modalOwnedPopover: 650,
	tooltip: 700,
	modalOwnedTooltip: 750,
} as const;

const MODAL_PANEL_SELECTOR = `[${MODAL_PANEL_ATTRIBUTE}]`;

export function getSceneRootElement(): HTMLElement | null {
	if (typeof document === "undefined") {
		return null;
	}
	return document.getElementById(SCENE_ROOT_ID) ?? document.getElementById("root");
}

export function getOverlayRootElement(): HTMLElement | null {
	if (typeof document === "undefined") {
		return null;
	}
	return document.getElementById(OVERLAY_ROOT_ID);
}

export function resolveOverlayContainer(): HTMLElement | null {
	if (typeof document === "undefined") {
		return null;
	}
	return getOverlayRootElement() ?? document.body;
}

export function isElementInsideModal(element: Element | null): boolean {
	return element?.closest(MODAL_PANEL_SELECTOR) !== null;
}

export function popoverZIndexForOwner(owner: Element | null): number {
	return isElementInsideModal(owner) ? OVERLAY_Z_INDEX.modalOwnedPopover : OVERLAY_Z_INDEX.popover;
}

export function tooltipZIndexForOwner(owner: Element | null): number {
	return isElementInsideModal(owner) ? OVERLAY_Z_INDEX.modalOwnedTooltip : OVERLAY_Z_INDEX.tooltip;
}
