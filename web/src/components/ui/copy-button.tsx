import { Check, Copy } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Button } from "./button";
import { cn } from "@/lib/utils";

type CopyState = "idle" | "copied" | "failed";

const copiedMessage = "Copied to Clipboard";
const failedMessage = "Copy failed — select the text and copy it manually.";
const resetDelayMs = 2000;

/**
 * Copy-to-clipboard with visible and announced confirmation.
 *
 * The clipboard API rejects in insecure contexts and when permission is
 * denied, so failure is reported rather than swallowed: without it the
 * operator sees nothing change and cannot tell whether the copy happened.
 */
export function CopyButton({
  value,
  label,
  className,
}: {
  value: string;
  /** Accessible name for the control, e.g. "Copy GitHub code". */
  label: string;
  className?: string;
}) {
  const [state, setState] = useState<CopyState>("idle");
  const resetTimer = useRef<number | undefined>(undefined);

  useEffect(() => () => window.clearTimeout(resetTimer.current), []);

  const copy = async () => {
    window.clearTimeout(resetTimer.current);
    try {
      await navigator.clipboard.writeText(value);
      setState("copied");
    } catch {
      setState("failed");
    }
    resetTimer.current = window.setTimeout(() => setState("idle"), resetDelayMs);
  };

  return (
    <div className={cn("relative inline-flex", className)}>
      <Button size="sm" variant="outline" aria-label={label} onClick={() => void copy()}>
        {state === "copied" ? <Check className="h-4 w-4 text-status-succeeded" /> : <Copy className="h-4 w-4" />}
      </Button>
      {/* Out of flow: confirming a copy must not move the value being copied. */}
      <p
        aria-live="polite"
        className={cn(
          // w-max so the short confirmation stays on one line; max-w caps the longer failure text.
          "absolute left-1/2 top-full z-10 mt-1.5 w-max max-w-56 -translate-x-1/2 text-center text-[11px] leading-tight",
          state === "failed" ? "text-status-failed" : "text-foreground-muted",
        )}
      >
        {state === "copied" ? copiedMessage : state === "failed" ? failedMessage : ""}
      </p>
    </div>
  );
}
