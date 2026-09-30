import type { HTMLAttributes, ReactNode } from "react";
import { X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { modalShellHeightClass } from "@/components/ui/modal-shell-styles";
import { cn } from "@/lib/utils";

export function ModalContentCard({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
	return <div className={cn("ui-card rounded-xl border border-border bg-background p-4", className)} {...props} />;
}

export function ModalContentSection({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
	return <div className={cn("ui-inset-panel space-y-1 rounded-lg border border-border bg-surface px-3 py-3", className)} {...props} />;
}

/**
 * `caption` names the surface the dialog belongs to — "Mailbox", "Work
 * Order", "Remove Connection". It is an orientation label, so it must never
 * rank or editorialise the choice inside ("Recommended", "Secondary Setup
 * Path"): the operator reads it before the title, and a ranking there argues
 * with them instead of telling them where they are. Omit it when the title is
 * already the whole answer.
 * Keep the dialog as one quiet surface: short confirmations do not need a
 * separate bordered header, warning panel, and footer. The selected theme
 * supplies the overlay surface and display type just as it does on pages.
 */
export function ModalShell({
	title,
	titleId,
	caption,
	description,
	onClose,
	children,
	footer,
	bodyClassName,
	className,
	height = "auto",
}: {
	title: ReactNode;
	titleId?: string;
	caption?: ReactNode;
	description?: ReactNode;
	onClose: () => void;
	children: ReactNode;
	footer?: ReactNode;
	bodyClassName?: string;
	className?: string;
	height?: "auto" | "workbench";
}) {
	return (
		<div className={cn("ui-window theme-overlay flex min-h-0 flex-col overflow-hidden text-foreground", modalShellHeightClass(height), className)}>
			<div className="ui-window-header px-6 pb-4 pt-6">
				<div className="ui-window-titlebar flex items-start justify-between gap-4">
					<div className="ui-window-heading min-w-0">
						{caption && caption !== title ? <p className="ui-window-caption mb-2 text-xs font-medium text-foreground-muted">{caption}</p> : null}
						<h3 id={titleId} className="ui-window-title font-display text-2xl leading-tight text-foreground text-wrap-anywhere sm:text-[1.75rem]">{title}</h3>
					</div>
					<Button className="ui-window-close" type="button" variant="ghost" size="icon" aria-label="Close dialog" onClick={onClose}>
						<X className="h-4 w-4" />
					</Button>
				</div>
				{description ? <p className="ui-window-description mt-3 pr-13 text-sm leading-relaxed text-foreground-muted">{description}</p> : null}
			</div>
			{/* `relative` for the same reason PageContent carries it: a scroll
			    viewport must be the containing block for its absolutely
			    positioned descendants, or they escape the clip. */}
			{children != null && <div className={cn("ui-window-body relative min-h-0 flex-1 overflow-y-auto px-6 pb-6", bodyClassName)}>{children}</div>}
			{footer ? <div className="ui-window-footer px-6 pb-6 pt-2">{footer}</div> : null}
		</div>
	);
}
