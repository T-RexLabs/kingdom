import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TimeRangePicker } from "./time-range-picker";

describe("rolling time window", () => {
  it("preserves presets and describes the actual API rolling-window scope", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={1440} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Time Window" }));
    await user.click(screen.getByRole("option", { name: "Last 30 Minutes" }));
    expect(onChange).toHaveBeenCalledWith(30);
    await user.click(screen.getByRole("button", { name: "Time Window" }));
    await user.click(screen.getByRole("option", { name: "Custom Window…" }));
    expect(screen.getByRole("dialog")).toHaveTextContent("ending now");
    expect(screen.queryByLabelText("To")).not.toBeInTheDocument();
  });
  it("accepts minute-precision custom windows and rejects fractional/out-of-range values", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={1440} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Time Window" }));
    await user.click(screen.getByRole("option", { name: "Custom Window…" }));
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
    await user.click(screen.getByRole("button", { name: "Time Window" }));
    await user.click(screen.getByRole("option", { name: "Custom Window…" }));
    await user.clear(screen.getByLabelText("Minutes"));
    await user.type(screen.getByLabelText("Minutes"), "invalid");
    view.rerender(<TimeRangePicker value={60} onChange={change} />);
    expect(screen.getByLabelText("Minutes")).toHaveValue("60");
    view.rerender(<TimeRangePicker value={1440} onChange={change} />);
    expect(screen.getByLabelText("Minutes")).toHaveValue("1440");
  });
  it("keeps a custom duration numeric and hands keyboard focus into its dialog", async () => {
    const user = userEvent.setup();
    const change = vi.fn();
    render(<TimeRangePicker value={97} onChange={change} />, { container: document.getElementById("scene-root")! });
    const trigger = screen.getByRole("button", { name: "Time Window" });
    await user.click(trigger);
    await user.click(screen.getByRole("option", { name: "Last 97 Minutes" }));
    expect(change).toHaveBeenCalledWith(97);
    await user.click(trigger);
    await waitFor(() => expect(screen.getByRole("option", { name: "Last 97 Minutes" })).toHaveFocus());
    await user.keyboard("{End}{Enter}");
    expect(screen.getByRole("dialog", { name: "Custom Time Window" })).toContainElement(document.activeElement as HTMLElement);
    await user.keyboard("{Escape}");
    await waitFor(() => expect(trigger).toHaveFocus());
  });
});
