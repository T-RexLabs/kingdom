type ModalShellSize = "sm" | "md" | "lg" | "xl";

const MODAL_SHELL_SIZE_CLASS: Record<ModalShellSize, string> = {
	sm: "max-w-md",
	md: "max-w-3xl",
	lg: "max-w-4xl",
	xl: "max-w-5xl",
};

export function modalShellSizeClass(size: ModalShellSize = "md"): string {
	return MODAL_SHELL_SIZE_CLASS[size];
}

/**
 * `auto` shrink-wraps the content up to a ceiling, which is right for modals
 * whose content is stable. `workbench` commits to a definite height so a
 * multi-pane dialog never resizes or re-centers as sections load and toggle;
 * its panes scroll instead.
 */
type ModalShellHeight = "auto" | "workbench";

const MODAL_SHELL_HEIGHT_CLASS: Record<ModalShellHeight, string> = {
	auto: "max-h-[85vh]",
	workbench: "h-[min(85vh,44rem)]",
};

export function modalShellHeightClass(height: ModalShellHeight = "auto"): string {
	return MODAL_SHELL_HEIGHT_CLASS[height];
}

export const MODAL_BODY_TEXT_CLASS = "text-sm text-foreground";
export const MODAL_SUPPORTING_TEXT_CLASS = "text-sm text-foreground-muted";
export const MODAL_LABEL_CLASS = "text-xs font-medium uppercase tracking-wide text-foreground-subtle";
