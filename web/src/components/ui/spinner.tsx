import { cn } from "@/lib/utils";

export function Spinner({ className }: { className?: string }) {
  return (
    <div
      aria-hidden="true"
      className={cn(
        "animate-spin rounded-full border-2 border-gold border-t-transparent",
        className,
      )}
    />
  );
}
