import { FloatingFocusManager, FloatingPortal, type FloatingContext } from "@floating-ui/react";
import { resolveOverlayContainer } from "@/lib/overlay-system";

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
	children: React.ReactNode;
}) {
	return (
		<FloatingPortal root={resolveOverlayContainer()}>
			<FloatingFocusManager context={context} modal={modalOwned} initialFocus={initialFocus} returnFocus>
				<div
					ref={panelRef}
					style={style}
					data-popover-placement={context.placement}
					className={className ?? "flex flex-col overflow-hidden rounded-lg border border-border bg-background shadow-xl"}
					{...floatingProps}
				>
					{children}
				</div>
			</FloatingFocusManager>
		</FloatingPortal>
	);
}
