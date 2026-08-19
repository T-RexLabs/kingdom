import type { HTMLAttributes, ReactNode } from "react";
import { X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { modalShellHeightClass } from "@/components/ui/modal-shell-styles";
import { cn } from "@/lib/utils";

export function ModalContentCard({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
	return <div className={cn("rounded-xl border border-border bg-background p-4", className)} {...props} />;
}

export function ModalContentSection({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
	return <div className={cn("space-y-1 rounded-lg border border-border bg-surface px-3 py-3", className)} {...props} />;
}

/**
 * `caption` names the surface the dialog belongs to — "Mailbox", "Royal
 * Decree", "Remove Connection". It is an orientation label, so it must never
 * rank or editorialise the choice inside ("Recommended", "Secondary Setup
 * Path"): the operator reads it before the title, and a ranking there argues
 * with them instead of telling them where they are. Omit it when the title is
 * already the whole answer.
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
		<div className={cn("flex min-h-0 flex-col overflow-hidden rounded-2xl border border-border bg-surface shadow-2xl", modalShellHeightClass(height), className)}>
			<div className="flex items-start justify-between gap-4 border-b border-border px-5 py-4">
				<div className="min-w-0">
					{caption ? <p className="text-xs uppercase tracking-[0.22em] text-foreground-subtle">{caption}</p> : null}
					<h3 id={titleId} className={cn("text-lg font-semibold text-foreground", caption ? "mt-2" : "")}>{title}</h3>
					{description ? <p className="mt-1 text-sm text-foreground-muted">{description}</p> : null}
				</div>
				<Button type="button" variant="ghost" size="icon" aria-label="Close dialog" onClick={onClose}>
					<X className="h-4 w-4" />
				</Button>
			</div>
			{/* `relative` for the same reason PageContent carries it: a scroll
			    viewport must be the containing block for its absolutely
			    positioned descendants, or they escape the clip. */}
			<div className={cn("relative min-h-0 flex-1 overflow-y-auto px-5 py-4", bodyClassName)}>{children}</div>
			{footer ? <div className="border-t border-border px-5 py-3">{footer}</div> : null}
		</div>
	);
}
