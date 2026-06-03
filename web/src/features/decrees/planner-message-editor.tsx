import { useEffect, useRef, useState } from "react";
import { useAutosizeTextarea } from "./use-autosize-textarea";

export function PlannerMessageEditor({
  initialText,
  onSubmit,
  onCancel,
}: {
  initialText: string;
  onSubmit: (text: string) => void;
  onCancel: () => void;
}) {
  const [text, setText] = useState(initialText);
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
        className={`w-full resize-none rounded-lg border border-gold/50 bg-background/80 px-3 py-2 text-sm text-foreground outline-none ring-0 transition-colors focus:border-gold/50 focus:ring-1 focus:ring-gold/30 ${
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
          className="rounded-md bg-gold/15 px-2.5 py-1 text-xs text-gold transition-colors hover:bg-gold/25 disabled:opacity-40"
        >
          Save & Send
        </button>
      </div>
    </div>
  );
}
