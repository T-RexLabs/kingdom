export function PlannerEditAction({ onEdit }: { onEdit: () => void }) {
  return (
    <div className="flex justify-end opacity-100 transition-opacity sm:opacity-0 group-hover:opacity-100 focus-within:opacity-100">
      <button
        type="button"
        onClick={onEdit}
        aria-label="Edit message"
        className="ui-button ui-button--ghost rounded px-2 py-1 text-xs text-foreground-subtle transition-colors hover:text-foreground focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
      >
        Edit
      </button>
    </div>
  );
}
