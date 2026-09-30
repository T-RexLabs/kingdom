import { cva } from "class-variance-authority";

export const buttonVariants = cva(
  "ui-button inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent disabled:pointer-events-none disabled:opacity-50 cursor-pointer",
  {
    variants: {
      variant: {
        default: "ui-button--default bg-accent text-accent-foreground hover:bg-accent-bright",
        secondary: "ui-button--secondary bg-surface-raised text-foreground hover:bg-border-strong",
        outline: "ui-button--outline border border-border text-foreground hover:bg-surface-raised hover:border-border-strong",
        ghost: "ui-button--ghost text-foreground-muted hover:text-foreground hover:bg-surface-raised",
        destructive: "ui-button--destructive bg-status-failed/15 text-status-failed hover:bg-status-failed/25",
      },
      size: {
        default: "h-9 px-4 py-2",
        sm: "h-8 px-3 text-xs",
        lg: "h-11 px-6",
        icon: "h-9 w-9",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
);
