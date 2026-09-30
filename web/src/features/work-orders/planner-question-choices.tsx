import { useMemo, useState } from "react";
import type { QuestionPayload } from "./planner-operation-controller";

type ChoiceOption = { label: string; value: string };

export function PlannerQuestionChoices({
  question,
  disabled,
  selectedValue,
  selectedValues,
  onTypeYourAnswer,
  onSelect,
  onSubmitMultiSelect,
}: {
  question?: QuestionPayload;
  disabled?: boolean;
  selectedValue?: string | null;
  selectedValues?: string[] | null;
  onTypeYourAnswer?: () => void;
  onSelect?: (option: ChoiceOption) => void;
  onSubmitMultiSelect?: (options: ChoiceOption[]) => void;
}) {
  const [draftValues, setDraftValues] = useState<string[]>([]);
  const resolvedSelectedValues = useMemo(
    () => selectedValues?.filter(Boolean) ?? draftValues,
    [draftValues, selectedValues]
  );

  if (!question) return null;

  const prompt = question.prompt?.trim();
  const isTextOnly = question.type === "short_text" || question.type === "long_text";
  const baseOptions = isTextOnly
    ? []
    : question.options ?? (question.type === "yes_no"
        ? [
            { label: "Yes", value: "Yes" },
            { label: "No", value: "No" },
          ]
        : []);

  if (!prompt && baseOptions.length === 0) return null;

  const options = baseOptions as ChoiceOption[];
  const isMultiSelect = question.type === "multi_select";
  // `disabled` (external) and `answered` (local) used to be conflated into a
  // single `isInert`. They mean different things — the first reflects an
  // outside lock (e.g. session is processing); the second reflects "the user
  // has committed to a choice". Keeping them separate lets us style answered
  // chips as a definitive selection while still allowing aria-pressed to
  // communicate the locked-in state to screen readers.
  const externallyDisabled = Boolean(disabled);
  const answered = Boolean(selectedValue);
  const multiSelectAnswered = Boolean(selectedValues && selectedValues.length > 0);
  const showTypeYourAnswer = !externallyDisabled && Boolean(onTypeYourAnswer);

  return (
    <div className="space-y-3">
      {prompt ? (
        <p className="text-sm font-medium text-foreground">{prompt}</p>
      ) : null}
      {options.length > 0 || showTypeYourAnswer ? (
        <>
          <div className="flex flex-wrap gap-2">
            {options.map((option) => {
              const isSelected = isMultiSelect
                ? resolvedSelectedValues.includes(option.value)
                : selectedValue === option.value;
              const localAnswered = isMultiSelect ? multiSelectAnswered : answered;
              const isInert = externallyDisabled || localAnswered;

              return (
                <button
                  key={option.value}
                  type="button"
                  aria-pressed={isSelected}
                  aria-disabled={isInert}
                  disabled={isInert}
                  onClick={
                    !isInert
                      ? () => {
                          if (isMultiSelect) {
                            setDraftValues((current) =>
                              current.includes(option.value)
                                ? current.filter((value) => value !== option.value)
                                : [...current, option.value]
                            );
                            return;
                          }
                          onSelect?.(option);
                        }
                      : undefined
                  }
                  className={`ui-button ui-button--outline ui-choice rounded-full px-3.5 py-1.5 text-xs font-medium transition-all ${
                    isSelected
                      ? "border border-accent/60 bg-accent/20 text-accent"
                      : answered || multiSelectAnswered
                        ? "cursor-default border border-border/30 bg-surface-raised/20 text-foreground-subtle/50"
                        : "border border-accent/30 bg-accent/5 text-foreground hover:border-accent/60 hover:bg-accent/15 disabled:cursor-not-allowed disabled:opacity-50"
                  }`}
                >
                  {option.label}
                </button>
              );
            })}
            {showTypeYourAnswer ? (
              <button
                type="button"
                onClick={onTypeYourAnswer}
                className="ui-button ui-button--outline rounded-full border border-border/60 bg-surface-raised/50 px-3.5 py-1.5 text-xs font-medium text-foreground-subtle transition-all hover:border-accent/40 hover:bg-surface-raised hover:text-foreground"
              >
                Type Your Answer
              </button>
            ) : null}
          </div>
          {isMultiSelect ? (
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => onSubmitMultiSelect?.(options.filter((option) => resolvedSelectedValues.includes(option.value)))}
                disabled={disabled || resolvedSelectedValues.length === 0 || multiSelectAnswered}
                className="ui-button ui-button--default rounded-full border border-accent/40 bg-accent/10 px-3.5 py-1.5 text-xs font-semibold text-accent transition-all hover:bg-accent/20 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Submit selection
              </button>
            </div>
          ) : null}
        </>
      ) : null}
    </div>
  );
}
