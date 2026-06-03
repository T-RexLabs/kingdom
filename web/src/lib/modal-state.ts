import { useSyncExternalStore } from "react";
import { getSceneRootElement } from "./overlay-system";

const listeners = new Set<() => void>();
let openModalCount = 0;

function emit() {
	for (const listener of listeners) {
		listener();
	}
}

function applyDocumentState() {
	if (typeof document === "undefined") {
		return;
	}
	const isOpen = openModalCount > 0;
	if (isOpen) {
		document.body.dataset.modalOpen = "true";
	} else {
		document.body.removeAttribute("data-modal-open");
	}
	const sceneRoot = getSceneRootElement();
	if (!sceneRoot) {
		return;
	}
	if (isOpen) {
		sceneRoot.dataset.modalObscured = "true";
		sceneRoot.setAttribute("inert", "");
		sceneRoot.setAttribute("aria-hidden", "true");
		return;
	}
	sceneRoot.removeAttribute("data-modal-obscured");
	sceneRoot.removeAttribute("inert");
	sceneRoot.removeAttribute("aria-hidden");
}

export function setModalPresence(open: boolean) {
	openModalCount = Math.max(0, openModalCount + (open ? 1 : -1));
	applyDocumentState();
	emit();
}

function subscribe(listener: () => void) {
	listeners.add(listener);
	return () => {
		listeners.delete(listener);
	};
}

function snapshot() {
	return openModalCount > 0;
}

export function modalIsOpen() {
	return snapshot();
}

export function useHasOpenModal() {
	return useSyncExternalStore(subscribe, snapshot, snapshot);
}
