import {
  autoUpdate,
  flip,
  offset,
  shift,
  size,
  useClick,
  useDismiss,
  useFloating,
  useRole,
  type ElementProps,
  type FloatingContext,
  type Placement,
} from "@floating-ui/react";
import { useCallback, useState } from "react";
import { isElementInsideModal, popoverZIndexForOwner } from "./overlay-system";

/**
 * The floating behaviour every Kingdom popover shares.
 *
 * Positioning, dismissal, and z-index are the parts that have to agree across
 * every popover in the app. A second, hand-rolled floating element would drift
 * on all three, and would z-fight the moment it opened inside a modal.
 *
 * This returns the floating context and the base interaction props rather than
 * finished handlers, because interaction hooks need the context this creates:
 * `PopoverListbox` layers list navigation and typeahead on top, while a panel
 * of controls layers nothing. Render the result through `PopoverSurface`.
 */

export interface PopoverFloatingOptions {
	/** Register related popovers in a FloatingTree for nested focus and dismissal. */
	nodeId?: string;
	open: boolean;
	onOpenChange(open: boolean): void;
	/** "listbox" for an option list, "dialog" for a panel of controls. */
	role: "listbox" | "dialog";
	/** Lower bound on the panel width; defaults to 160px. */
	minWidth?: number;
	/** Anchoring direction; defaults to bottom-start. */
	placement?: Placement;
	/** Keep a complete control panel visible when its preferred side has too little room. */
	allowFlip?: boolean;
	/** Lets responsive CSS own width while Floating UI caps it to available space. */
	widthMode?: "reference-min" | "css";
	/** Upper bound on panel height (px); defaults to 320 (content scrolls). */
	maxHeight?: number;
	triggerDisabled?: boolean;
	/**
	 * Suppresses outside-press and Escape dismissal. A panel that owns a modal
	 * has to stay open behind it, or acting on the modal would close the thing
	 * the operator is coming back to.
	 */
	dismissDisabled?: boolean;
}

export interface PopoverFloating {
	context: FloatingContext;
	setReference(node: HTMLElement | null): void;
	setPanel(node: HTMLDivElement | null): void;
	modalOwned: boolean;
	panelStyle: React.CSSProperties;
	baseInteractions: ElementProps[];
}

export function usePopoverFloating({
	nodeId,
	open,
	onOpenChange,
	role,
	minWidth = 160,
	placement = "bottom-start",
	allowFlip = false,
	widthMode = "reference-min",
	maxHeight = 320,
	triggerDisabled,
	dismissDisabled,
}: PopoverFloatingOptions): PopoverFloating {
	const { refs, floatingStyles, context } = useFloating({
		nodeId,
		open,
		onOpenChange,
		placement,
		whileElementsMounted: autoUpdate,
		middleware: [
			offset(4),
			// Lists stay below their trigger by default. Complete panels such as
			// calendars can opt in to flipping to keep their controls together.
			...(allowFlip ? [flip({ padding: 8 })] : []),
			shift({ padding: 8 }),
			size({
				padding: 8,
				apply({ rects, elements, availableHeight, availableWidth }) {
					if (widthMode === "css") {
						elements.floating.style.minWidth = "";
						elements.floating.style.maxWidth = `${Math.max(0, availableWidth)}px`;
					} else {
						elements.floating.style.minWidth = `${Math.max(minWidth, rects.reference.width)}px`;
						elements.floating.style.maxWidth = "";
					}
					Object.assign(elements.floating.style, {
						maxHeight: `${Math.min(maxHeight, availableHeight)}px`,
					});
				},
			}),
		],
	});

	const click = useClick(context, { enabled: !triggerDisabled });
	const dismiss = useDismiss(context, { enabled: !dismissDisabled, outsidePressEvent: "pointerdown" });
	const roleProps = useRole(context, { role });

	const setReference = useCallback((node: HTMLElement | null) => {
		refs.setReference(node);
	}, [refs]);

	const setPanel = useCallback((node: HTMLDivElement | null) => {
		refs.setFloating(node);
	}, [refs]);

	const owner = refs.reference.current instanceof Element ? refs.reference.current : null;

	return {
		context,
		setReference,
		setPanel,
		modalOwned: isElementInsideModal(owner),
		panelStyle: { ...floatingStyles, zIndex: popoverZIndexForOwner(owner) },
		baseInteractions: [click, dismiss, roleProps],
	};
}

/** Local open state plus the change handler the floating hook expects. */
export function usePopoverOpenState(onOpenChange?: (open: boolean) => void, onClose?: () => void) {
	const [open, setOpen] = useState(false);
	const handleOpenChange = useCallback((nextOpen: boolean) => {
		setOpen(nextOpen);
		if (!nextOpen) onClose?.();
		onOpenChange?.(nextOpen);
	}, [onClose, onOpenChange]);
	return { open, setOpen, handleOpenChange };
}
