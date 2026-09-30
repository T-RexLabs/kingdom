import { useEffect, useRef, useState } from "react";
import { useAutosizeTextarea } from "./use-autosize-textarea";

export function PlannerMessageEditor({
  initialText,
  value,
  onChange,
  onSubmit,
  onCancel,
}: {
  initialText: string;
  value?: string;
  onChange?: (text: string) => void;
  onSubmit: (text: string) => void;
  onCancel: () => void;
}) {
  const [localText, setLocalText] = useState(initialText);
  const text = value ?? localText;
  const setText = onChange ?? setLocalText;
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const { isOverflowing } = useAutosizeTextarea({
    ref: textareaRef,
    value: text,
    minHeight: 80,
    maxHeight: 320,
  });

  useEffect(() => {
    const el = textareaRef.current;
    if (el) {
      el.focus({ preventScroll: true });
      el.setSelectionRange(el.value.length, el.value.length);
    }
  }, []);
  const handleSubmit = () => {
    const trimmed = text.trim();
    if (trimmed) {
      onSubmit(trimmed);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.nativeEvent.isComposing || e.keyCode === 229) return;
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
    if (e.key === "Escape") {
      e.preventDefault();
      onCancel();
    }
  };

  return (
    <div className="space-y-2">
      <textarea
        ref={textareaRef}
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={handleKeyDown}
        className={`w-full resize-none rounded-lg border border-accent/50 bg-background/80 px-3 py-2 text-sm text-foreground outline-none ring-0 transition-colors focus:border-accent/50 focus:ring-1 focus:ring-accent/30 ${
          isOverflowing ? "overflow-y-auto" : "overflow-hidden"
        }`}
        style={{ minHeight: "80px", maxHeight: "320px" }}
      />
      <div className="flex gap-2 justify-end">
        <button
          type="button"
          onClick={onCancel}
          className="rounded-md px-2.5 py-1 text-xs text-foreground-subtle transition-colors hover:text-foreground"
        >
          Cancel
        </button>
        <button
          type="button"
          onClick={handleSubmit}
          disabled={!text.trim()}
          className="rounded-md bg-accent/15 px-2.5 py-1 text-xs text-accent transition-colors hover:bg-accent/25 disabled:opacity-40"
        >
          Save & Send
        </button>
      </div>
    </div>
  );
}
