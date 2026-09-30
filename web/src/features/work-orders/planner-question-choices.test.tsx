import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { PlannerQuestionChoices } from "./planner-question-choices";

describe("planner question choices", () => {
  it("renders prompt for open-text questions without buttons", () => {
    render(
      <PlannerQuestionChoices
        question={{
          prompt: "What deployment surface matters most?",
          type: "long_text",
        }}
        onTypeYourAnswer={() => {}}
      />
    );

    expect(screen.getByText("What deployment surface matters most?")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Type Your Answer" })).toBeInTheDocument();
  });

  it("supports multi-select answers with explicit submit", async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();

    render(
      <PlannerQuestionChoices
        question={{
          prompt: "Which surfaces matter?",
          type: "multi_select",
          options: [
            { label: "Web", value: "web" },
            { label: "CLI", value: "cli" },
            { label: "Docs", value: "docs" },
          ],
        }}
        onSubmitMultiSelect={onSubmit}
      />
    );

    await user.click(screen.getByRole("button", { name: "Web" }));
    await user.click(screen.getByRole("button", { name: "Docs" }));
    await user.click(screen.getByRole("button", { name: "Submit selection" }));

    expect(onSubmit).toHaveBeenCalledWith([
      { label: "Web", value: "web" },
      { label: "Docs", value: "docs" },
    ]);
  });

  it("does not render the type-your-answer pill for non-interactive questions", () => {
    render(
      <PlannerQuestionChoices
        question={{
          prompt: "Choose a direction",
          type: "single_select",
          options: [
            { label: "Lean", value: "lean" },
            { label: "Thorough", value: "thorough" },
          ],
        }}
        disabled
      />
    );

    expect(screen.queryByRole("button", { name: "Type Your Answer" })).toBeNull();
  });

  it("routes the type-your-answer action separately from answer options", async () => {
    const user = userEvent.setup();
    const onTypeYourAnswer = vi.fn();
    const onSelect = vi.fn();

    render(
      <PlannerQuestionChoices
        question={{
          prompt: "Choose a direction",
          type: "single_select",
          options: [
            { label: "Lean", value: "lean" },
            { label: "Thorough", value: "thorough" },
          ],
        }}
        onTypeYourAnswer={onTypeYourAnswer}
        onSelect={onSelect}
      />
    );

    await user.click(screen.getByRole("button", { name: "Type Your Answer" }));

    expect(onTypeYourAnswer).toHaveBeenCalledTimes(1);
    expect(onSelect).not.toHaveBeenCalled();
  });
});
