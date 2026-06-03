import { Send } from "lucide-react";
import { useRef, type RefObject } from "react";
import { useAutosizeTextarea } from "./use-autosize-textarea";

export function PlannerComposer({
  value,
  disabled,
  textareaRef,
  onChange,
  onSend,
}: {
  value: string;
  disabled?: boolean;
  textareaRef?: RefObject<HTMLTextAreaElement | null>;
  onChange: (value: string) => void;
  onSend: () => void;
}) {
  const localTextareaRef = useRef<HTMLTextAreaElement>(null);
  const resolvedTextareaRef = textareaRef ?? localTextareaRef;
  const { isOverflowing } = useAutosizeTextarea({
    ref: resolvedTextareaRef,
    value,
    minHeight: 40,
    maxHeight: 160,
  });

  return (
    <div className="flex items-end gap-2">
      <textarea
        ref={resolvedTextareaRef}
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        onKeyDown={(event) => {
          if (disabled) return;
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            onSend();
          }
        }}
        placeholder="Type your response..."
        rows={1}
        className={`min-h-[40px] max-h-[160px] flex-1 resize-none rounded-xl border border-border/70 bg-background/80 px-4 py-2.5 text-sm text-foreground placeholder:text-foreground-subtle/50 focus:border-gold/50 focus:outline-none focus:ring-1 focus:ring-gold/30 disabled:cursor-not-allowed disabled:opacity-50 ${
          isOverflowing ? "overflow-y-auto" : "overflow-hidden"
        }`}
      />
      <button
        type="button"
        aria-label="Send"
        onClick={onSend}
        disabled={disabled || !value.trim()}
        className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gold/90 text-background transition-all hover:bg-gold disabled:cursor-not-allowed disabled:opacity-30"
      >
        <Send className="h-4 w-4" />
      </button>
    </div>
  );
}
