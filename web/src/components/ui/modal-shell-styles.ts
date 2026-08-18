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

export const MODAL_BODY_TEXT_CLASS = "text-sm text-foreground";
export const MODAL_SUPPORTING_TEXT_CLASS = "text-sm text-foreground-muted";
export const MODAL_LABEL_CLASS = "text-xs font-medium uppercase tracking-wide text-foreground-subtle";
