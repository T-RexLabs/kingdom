import { Send } from "lucide-react";
import { useRef, type RefObject } from "react";
import { useAutosizeTextarea } from "./use-autosize-textarea";

export function PlannerComposer({
  value,
  disabled,
  sendDisabled,
  textareaRef,
  onChange,
  onSend,
}: {
  value: string;
  disabled?: boolean;
  sendDisabled?: boolean;
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
    <div className="flex items-end gap-2 rounded-2xl border border-border bg-surface p-2 shadow-sm transition-colors focus-within:border-accent/60">
      <textarea
        ref={resolvedTextareaRef}
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        onKeyDown={(event) => {
          if (disabled || event.nativeEvent.isComposing || event.keyCode === 229) return;
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            if (sendDisabled) return;
            onSend();
          }
        }}
        aria-label="Message the planner"
        placeholder="Type your response..."
        rows={1}
        className={`min-h-[40px] max-h-[160px] flex-1 resize-none rounded-xl border-0 bg-transparent px-4 py-2.5 text-sm text-foreground placeholder:text-foreground-subtle/50 focus:outline-none disabled:cursor-not-allowed disabled:opacity-50 ${
          isOverflowing ? "overflow-y-auto" : "overflow-hidden"
        }`}
      />
      <button
        type="button"
        aria-label="Send"
        onClick={onSend}
        disabled={disabled || sendDisabled || !value.trim()}
        className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-accent/90 text-background transition-all hover:bg-accent disabled:cursor-not-allowed disabled:opacity-30"
      >
        <Send className="h-4 w-4" />
      </button>
    </div>
  );
}
