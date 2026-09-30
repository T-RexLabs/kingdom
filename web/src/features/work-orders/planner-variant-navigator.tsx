import { ChevronLeft, ChevronRight } from "lucide-react";
import { useListMessageVariants } from "@/api/hooks";

export function PlannerVariantNavigator({
  sessionId,
  messageId,
  currentVariantIndex,
  variantCount,
  onActivateVariant,
}: {
  sessionId: string;
  messageId: string;
  // Zero-based branch_index of the active message among its siblings, and the
  // total sibling (branch) count at this point in the conversation tree.
  currentVariantIndex: number;
  variantCount: number;
  onActivateVariant: (messageId: string) => void;
}) {
  const { data } = useListMessageVariants(sessionId, messageId);
  // Siblings are returned oldest-first, so branch_index is a direct array index.
  const branches = data?.items ?? [];

  if (variantCount <= 1) return null;

  const switchTo = (index: number) => {
    const target = branches[index];
    if (target) onActivateVariant(target.planning_message_id);
  };

  const variantsLoaded = branches.length > 0;

  return (
    <div className="flex items-center gap-1">
      <button
        type="button"
        onClick={() => switchTo(currentVariantIndex - 1)}
        disabled={currentVariantIndex <= 0 || !variantsLoaded}
        className="flex h-5 w-5 items-center justify-center rounded text-foreground-subtle transition-colors hover:text-accent disabled:opacity-30 disabled:hover:text-foreground-subtle"
      >
        <ChevronLeft className="h-3.5 w-3.5" />
      </button>
      <span className="min-w-[3ch] text-center text-xs tabular-nums text-foreground-subtle">
        {currentVariantIndex + 1} / {variantCount}
      </span>
      <button
        type="button"
        onClick={() => switchTo(currentVariantIndex + 1)}
        disabled={currentVariantIndex >= variantCount - 1 || !variantsLoaded}
        className="flex h-5 w-5 items-center justify-center rounded text-foreground-subtle transition-colors hover:text-accent disabled:opacity-30 disabled:hover:text-foreground-subtle"
      >
        <ChevronRight className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
