import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, beforeEach } from "vitest";
import { OVERLAY_ROOT_ID, SCENE_ROOT_ID } from "../lib/overlay-system";

function ensureRoot(id: string): HTMLElement {
	let node = document.getElementById(id);
	if (!node) {
		node = document.createElement("div");
		node.id = id;
		document.body.appendChild(node);
	}
	return node;
}

function resetRoot(id: string) {
	const node = ensureRoot(id);
	node.replaceChildren();
	node.removeAttribute("inert");
	node.removeAttribute("aria-hidden");
	node.removeAttribute("data-modal-obscured");
}

beforeEach(() => {
	resetRoot(SCENE_ROOT_ID);
	resetRoot(OVERLAY_ROOT_ID);
	document.body.dataset.modalOpen = "false";
});

afterEach(() => {
	cleanup();
	resetRoot(SCENE_ROOT_ID);
	resetRoot(OVERLAY_ROOT_ID);
	document.body.removeAttribute("data-modal-open");
});
