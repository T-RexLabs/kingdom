import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, beforeEach } from "vitest";
import { OVERLAY_ROOT_ID, SCENE_ROOT_ID } from "../lib/overlay-system";

// jsdom does not implement IntersectionObserver, and components that load more
// content as it scrolls into view depend on it. The stub records its target so
// a test can drive the callback deliberately, which is the only useful shape
// here: jsdom has no layout, so nothing would ever intersect on its own.
class TestIntersectionObserver implements IntersectionObserver {
	static instances: TestIntersectionObserver[] = [];

	readonly root: Element | Document | null;
	readonly rootMargin = "0px";
	readonly thresholds: ReadonlyArray<number> = [0];
	readonly targets: Element[] = [];

	private readonly callback: IntersectionObserverCallback;

	constructor(callback: IntersectionObserverCallback, options?: IntersectionObserverInit) {
		this.callback = callback;
		this.root = options?.root ?? null;
		TestIntersectionObserver.instances.push(this);
	}

	observe(target: Element) {
		this.targets.push(target);
	}

	unobserve(target: Element) {
		const index = this.targets.indexOf(target);
		if (index >= 0) this.targets.splice(index, 1);
	}

	disconnect() {
		this.targets.length = 0;
		const index = TestIntersectionObserver.instances.indexOf(this);
		if (index >= 0) TestIntersectionObserver.instances.splice(index, 1);
	}

	takeRecords(): IntersectionObserverEntry[] {
		return [];
	}

	/** Drives the callback as if every observed target had scrolled into view. */
	intersect() {
		this.callback(
			this.targets.map((target) => ({ isIntersecting: true, target }) as IntersectionObserverEntry),
			this,
		);
	}
}

globalThis.IntersectionObserver = TestIntersectionObserver as unknown as typeof IntersectionObserver;

export function intersectAllObservers() {
	for (const observer of [...TestIntersectionObserver.instances]) observer.intersect();
}

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
	TestIntersectionObserver.instances.length = 0;
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
