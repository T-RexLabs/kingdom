import { Feather } from "lucide-react";

export function PlannerEditAction({ onEdit }: { onEdit: () => void }) {
  return (
    <div className="flex justify-end opacity-0 transition-opacity group-hover:opacity-100 focus-within:opacity-100">
      <button
        type="button"
        onClick={onEdit}
        className="flex h-6 w-6 items-center justify-center rounded-md border border-border/40 bg-surface-raised/80 text-foreground-subtle transition-colors hover:border-gold/40 hover:text-gold"
      >
        <Feather className="h-3 w-3" />
      </button>
    </div>
  );
}
