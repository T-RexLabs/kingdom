import { useState } from "react";
import { Button } from "@/components/ui/button";
import { CheckCircle2, RotateCcw, Shield } from "lucide-react";
import { runStatusBannerClass } from "./run-detail-tones";

export type ApprovalAction = "confirm" | "rework";

interface ApprovalDecisionFormProps {
  description: string;
  notesLabel: string;
  notesHint?: string;
  notesPlaceholder: string;
  confirmLabel: string;
  confirmPendingLabel?: string;
  reworkLabel: string;
  reworkPendingLabel?: string;
  reworkValidationMessage: string;
  errorMessage?: string | null;
  disabled?: boolean;
  recoveryAction?: ApprovalAction | null;
  onConfirm: (notes: string) => Promise<void>;
  onRequestRework: (notes: string) => Promise<void>;
}

export function ApprovalDecisionForm({
  description,
  notesLabel,
  notesHint,
  notesPlaceholder,
  confirmLabel,
  confirmPendingLabel,
  reworkLabel,
  reworkPendingLabel,
  reworkValidationMessage,
  errorMessage,
  disabled = false,
  recoveryAction = null,
  onConfirm,
  onRequestRework,
}: ApprovalDecisionFormProps) {
  const [notes, setNotes] = useState("");
  const [validationMessage, setValidationMessage] = useState<string | null>(null);
  const [pendingAction, setPendingAction] = useState<ApprovalAction | null>(null);

  const submit = async (action: ApprovalAction) => {
    if (disabled || pendingAction !== null) {
      return;
    }

    const trimmedNotes = notes.trim();
    if (action === "rework" && trimmedNotes === "") {
      setValidationMessage(reworkValidationMessage);
      return;
    }

    setValidationMessage(null);
    setPendingAction(action);
    try {
      if (action === "confirm") {
        await onConfirm(trimmedNotes);
      } else {
        await onRequestRework(trimmedNotes);
      }
      setNotes("");
    } catch {
      // surfaced through errorMessage from the container mutation
    } finally {
      setPendingAction(null);
    }
  };

  return (
    <div className="space-y-3">
      <p className="text-xs text-foreground-muted">{description}</p>

      <div>
        <label
          htmlFor="approval-notes"
          className="mb-1 block text-xs font-medium text-foreground-muted"
        >
          {notesLabel}
        </label>
        <textarea
          id="approval-notes"
          value={notes}
          onChange={(event) => {
            setNotes(event.target.value);
            if (validationMessage && event.target.value.trim() !== "") {
              setValidationMessage(null);
            }
          }}
          rows={3}
          placeholder={notesPlaceholder}
          disabled={disabled || recoveryAction !== null}
          className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-foreground-subtle focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent resize-none"
        />
        {notesHint && (
          <p className="mt-1 text-xs text-foreground-subtle">{notesHint}</p>
        )}
      </div>

      {validationMessage && (
        <div className={`rounded-md border px-3 py-2 text-sm ${runStatusBannerClass("failed")}`}>
          {validationMessage}
        </div>
      )}

      {errorMessage && (
        <div className={`rounded-md border px-3 py-2 text-sm ${runStatusBannerClass("failed")}`}>
          {errorMessage}
        </div>
      )}

      <div className="flex flex-wrap gap-2">
        <Button
          type="button"
          size="sm"
          disabled={disabled || pendingAction !== null || (recoveryAction !== null && recoveryAction !== "confirm")}
          onClick={() => {
            void submit("confirm");
          }}
        >
          {pendingAction === "confirm" ? (
            <Shield className="h-3.5 w-3.5" />
          ) : (
            <CheckCircle2 className="h-3.5 w-3.5" />
          )}
          {pendingAction === "confirm"
            ? (confirmPendingLabel ?? "Submitting...")
            : recoveryAction === "confirm" ? `Retry ${confirmLabel}` : confirmLabel}
        </Button>
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={disabled || pendingAction !== null || (recoveryAction !== null && recoveryAction !== "rework")}
          onClick={() => {
            void submit("rework");
          }}
        >
          {pendingAction === "rework" ? (
            <Shield className="h-3.5 w-3.5" />
          ) : (
            <RotateCcw className="h-3.5 w-3.5" />
          )}
          {pendingAction === "rework"
            ? (reworkPendingLabel ?? "Submitting...")
            : recoveryAction === "rework" ? `Retry ${reworkLabel}` : reworkLabel}
        </Button>
      </div>
    </div>
  );
}
