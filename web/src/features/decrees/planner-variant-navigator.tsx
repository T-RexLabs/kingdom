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
  currentVariantIndex: number;
  variantCount: number;
  onActivateVariant: (messageId: string) => void;
}) {
  const { data } = useListMessageVariants(sessionId, messageId);
  const variants = data?.items ?? [];

  if (variantCount <= 1) return null;

  const handlePrevious = () => {
    const target = variants.find((v) => v.variant_index === currentVariantIndex - 1);
    if (target) onActivateVariant(target.planning_message_id);
  };

  const handleNext = () => {
    const target = variants.find((v) => v.variant_index === currentVariantIndex + 1);
    if (target) onActivateVariant(target.planning_message_id);
  };

  const variantsLoaded = variants.length > 0;

  return (
    <div className="flex items-center gap-1">
      <button
        type="button"
        onClick={handlePrevious}
        disabled={currentVariantIndex <= 0 || !variantsLoaded}
        className="flex h-5 w-5 items-center justify-center rounded text-foreground-subtle transition-colors hover:text-gold disabled:opacity-30 disabled:hover:text-foreground-subtle"
      >
        <ChevronLeft className="h-3.5 w-3.5" />
      </button>
      <span className="min-w-[3ch] text-center text-xs tabular-nums text-foreground-subtle">
        {currentVariantIndex + 1} / {variantCount}
      </span>
      <button
        type="button"
        onClick={handleNext}
        disabled={currentVariantIndex >= variantCount - 1 || !variantsLoaded}
        className="flex h-5 w-5 items-center justify-center rounded text-foreground-subtle transition-colors hover:text-gold disabled:opacity-30 disabled:hover:text-foreground-subtle"
      >
        <ChevronRight className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
