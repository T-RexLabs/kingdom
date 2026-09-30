import { FloatingFocusManager, FloatingPortal, type FloatingContext } from "@floating-ui/react";
import { resolveOverlayContainer } from "@/lib/overlay-system";
import { cn } from "@/lib/utils";
import { useEffect } from "react";

/**
 * The portalled, focus-managed body every Kingdom popover renders into.
 *
 * Pairs with `usePopoverFloating`, which owns positioning and dismissal. Render
 * only while open — the portal and focus manager are what make an open panel
 * escape its parent's overflow and hand focus back to the trigger on close.
 */
export function PopoverSurface({
	context,
	panelRef,
	style,
	modalOwned,
	floatingProps,
	initialFocus = 0,
	className,
	onKeyDownCapture,
	children,
}: {
	context: FloatingContext;
	panelRef(node: HTMLDivElement | null): void;
	style: React.CSSProperties;
	modalOwned: boolean;
	floatingProps: Record<string, unknown>;
	/** -1 focuses the panel itself; use it when the panel owns its own focus. */
	initialFocus?: number;
	className?: string;
	onKeyDownCapture?: React.KeyboardEventHandler<HTMLDivElement>;
	children: React.ReactNode;
}) {
	const reference = context.elements.domReference;
	const floating = context.elements.floating;
	useEffect(() => {
		if (!floating) return;
		return () => {
			// A choice can open a dialog in the same commit. Check focus after
			// that handoff, rather than restoring from a pre-unmount snapshot.
			queueMicrotask(() => {
				const active = document.activeElement;
				if (active instanceof HTMLElement && active !== document.body && active !== document.documentElement && active.isConnected && !floating.contains(active)) return;
				if (reference instanceof HTMLElement && reference.isConnected && !reference.matches(":disabled") && !reference.closest("[inert]")) reference.focus();
			});
		};
	}, [reference, floating]);
	return (
		<FloatingPortal root={resolveOverlayContainer()}>
			<FloatingFocusManager context={context} modal={modalOwned} initialFocus={initialFocus} returnFocus={false}>
				<div
					ref={panelRef}
					style={style}
					data-popover-placement={context.placement}
					className={cn("theme-overlay flex flex-col overflow-hidden text-foreground", className)}
					{...floatingProps}
					onKeyDownCapture={onKeyDownCapture}
				>
					{children}
				</div>
			</FloatingFocusManager>
		</FloatingPortal>
	);
}
