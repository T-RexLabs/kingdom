import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TimeRangePicker } from "./time-range-picker";

describe("rolling time window", () => {
  it("preserves presets and describes the actual API rolling-window scope", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={1440} onChange={onChange} />);
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Time Window" }),
      "30",
    );
    expect(onChange).toHaveBeenCalledWith(30);
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Time Window" }),
      "custom",
    );
    expect(screen.getByRole("dialog")).toHaveTextContent("ending now");
    expect(screen.queryByLabelText("To")).not.toBeInTheDocument();
  });
  it("accepts minute-precision custom windows and rejects fractional/out-of-range values", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={1440} onChange={onChange} />);
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Time Window" }),
      "custom",
    );
    for (const invalid of ["", "0", "1.5", "525601"]) {
      await user.clear(screen.getByLabelText("Minutes"));
      if (invalid) await user.type(screen.getByLabelText("Minutes"), invalid);
      await user.click(screen.getByRole("button", { name: "Apply Window" }));
      expect(screen.getByRole("alert")).toBeInTheDocument();
      expect(onChange).not.toHaveBeenCalled();
    }
    await user.clear(screen.getByLabelText("Minutes"));
    await user.type(screen.getByLabelText("Minutes"), "97");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith(97);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });
  it("resets stale draft state when the selected URL window changes", async () => {
    const user = userEvent.setup();
    const change = vi.fn();
    const view = render(<TimeRangePicker value={1440} onChange={change} />);
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Time Window" }),
      "custom",
    );
    await user.clear(screen.getByLabelText("Minutes"));
    await user.type(screen.getByLabelText("Minutes"), "invalid");
    view.rerender(<TimeRangePicker value={60} onChange={change} />);
    expect(screen.getByLabelText("Minutes")).toHaveValue("60");
    view.rerender(<TimeRangePicker value={1440} onChange={change} />);
    expect(screen.getByLabelText("Minutes")).toHaveValue("1440");
  });
});
