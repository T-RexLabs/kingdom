import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { RunReworkSummary } from "./run-rework-summary";

const run = {
  workflow_id: "work-order-v1",
  run_until_all_approve: false,
  review_rework_cycles: 2,
  validation_rework_waves: 1,
  max_execution_review_cycles: 5,
  max_validation_rework_cycles: 3,
  review_auto_succeeded_count: 0,
  validation_auto_succeeded_count: 0,
};

describe("run rework summary", () => {
  it("shows actual rework counts and independent caps without implying attempts or progress", () => {
    render(<RunReworkSummary run={run} />);
    expect(screen.getByText("Review Rework").parentElement).toHaveTextContent(
      "2 / 5",
    );
    expect(
      screen.getByText("Validation Rework").parentElement,
    ).toHaveTextContent("1 / 3");
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
    expect(screen.queryByText(/Attempt/)).not.toBeInTheDocument();
  });
  it("shows uncapped rework without presenting the stored numeric cap as active", () => {
    render(<RunReworkSummary run={{ ...run, run_until_all_approve: true }} />);
    expect(screen.getAllByText("· No Cap")).toHaveLength(2);
    expect(screen.getByText("Review Rework").parentElement).toHaveTextContent(
      "2 · No Cap",
    );
    expect(screen.queryByText("/ 5")).not.toBeInTheDocument();
    expect(
      screen.getAllByTitle(
        "No rework cap; continues until approval or manual cancellation.",
      ),
    ).toHaveLength(2);
  });
  it("preserves zero caps and reports missing caps without inventing defaults", () => {
    render(
      <RunReworkSummary
        run={{
          ...run,
          max_execution_review_cycles: 0,
          max_validation_rework_cycles: undefined,
        }}
      />,
    );
    expect(screen.getByText("Review Rework").parentElement).toHaveTextContent(
      "2 / 0",
    );
    expect(
      screen.getByText("Validation Rework").parentElement,
    ).toHaveTextContent("1 · Cap Not Recorded");
    expect(screen.queryByText("· No Cap")).not.toBeInTheDocument();
  });
  it("distinguishes workflow-skipped gates from zero or uncapped rework", () => {
    render(
      <RunReworkSummary
        run={{ ...run, workflow_id: "work-order-skip-review-validation-v1" }}
      />,
    );
    const summary = screen.getByRole("region", {
      name: "Review and validation rework",
    });
    expect(within(summary).getByText("Skipped")).toBeVisible();
    expect(within(summary).queryByRole("definition")).not.toBeInTheDocument();
  });
  it("keeps actual auto-success-at-cap evidence attached to the corresponding gate", () => {
    render(
      <RunReworkSummary run={{ ...run, review_auto_succeeded_count: 2 }} />,
    );
    expect(screen.getByText("Review Rework").parentElement).toHaveTextContent(
      "2 Auto-Succeeded at Cap",
    );
    expect(
      screen.getByText("Validation Rework").parentElement,
    ).not.toHaveTextContent("Auto-Succeeded");
  });
});
