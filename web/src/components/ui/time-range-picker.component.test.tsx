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
});
