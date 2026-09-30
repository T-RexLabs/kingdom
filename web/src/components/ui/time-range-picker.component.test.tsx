import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { TimeRangePicker } from "./time-range-picker";

describe("time range picker component", () => {
  it("associates the From and To labels with their inputs", async () => {
    const user = userEvent.setup();

    render(<TimeRangePicker value={1440} onChange={() => undefined} />);

    await user.click(screen.getByRole("button", { name: "Custom time range" }));

    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
    expect(screen.getByLabelText("To")).toHaveValue("now");
  });

  it("does not revive an invalid draft when the selected value cycles", async () => {
    const user = userEvent.setup();
    const { rerender } = render(<TimeRangePicker value={1440} onChange={() => undefined} />);

    await user.click(screen.getByRole("button", { name: "Custom time range" }));
    const fromInput = screen.getByLabelText("From");
    await user.clear(fromInput);
    await user.type(fromInput, "invalid");
    await user.click(screen.getByRole("button", { name: "Apply time range" }));
    expect(screen.getByText(/Invalid 'From' expression/)).toBeInTheDocument();

    rerender(<TimeRangePicker value={60} onChange={() => undefined} />);

    expect(screen.getByLabelText("From")).toHaveValue("now-1h");
    expect(screen.getByLabelText("To")).toHaveValue("now");
    expect(screen.queryByText(/Invalid 'From' expression/)).not.toBeInTheDocument();

    rerender(<TimeRangePicker value={1440} onChange={() => undefined} />);

    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
    expect(screen.getByLabelText("To")).toHaveValue("now");
    expect(screen.queryByText(/Invalid 'From' expression/)).not.toBeInTheDocument();
  });

  it("does not revive an unsubmitted custom draft when the selected value cycles", async () => {
    const user = userEvent.setup();
    const { rerender } = render(<TimeRangePicker value={1440} onChange={() => undefined} />);

    await user.click(screen.getByRole("button", { name: "Custom time range" }));
    const fromInput = screen.getByLabelText("From");
    await user.clear(fromInput);
    await user.type(fromInput, "now-2h");

    rerender(<TimeRangePicker value={60} onChange={() => undefined} />);
    expect(screen.getByLabelText("From")).toHaveValue("now-1h");

    rerender(<TimeRangePicker value={1440} onChange={() => undefined} />);
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
  });
});
