import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TimeRangePicker } from "./time-range-picker";

describe("rolling time window", () => {
  it("exposes compact one-click presets and marks the current window", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={1440} onChange={onChange} />);
    expect(screen.getByRole("button", { name: "Last 24 Hours" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "Last 30 Minutes" })).toHaveTextContent("30m");
    expect(screen.getByRole("button", { name: "Last 30 Days" })).toHaveTextContent("30d");
    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));
    expect(onChange).toHaveBeenCalledWith(30);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("opens an anchored custom window with an honest fixed Now endpoint", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={1440} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    const dialog = screen.getByRole("dialog", { name: "Custom Time Window" });
    expect(dialog).toHaveClass("ui-popover");
    expect(dialog).toHaveTextContent("ending now");
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
    expect(screen.getByLabelText("To")).toHaveValue("Now");
    expect(screen.getByLabelText("To")).toHaveAttribute("readonly");
    expect(document.body).toHaveAttribute("data-modal-open", "false");
  });

  it("accepts exact minute windows and rejects invalid or unsupported expressions", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={1440} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    for (const invalid of ["", "now", "now-0m", "now-1.5m", "now-525601m", "now-2y", "2026-09-01"]) {
      await user.clear(screen.getByLabelText("From"));
      if (invalid) await user.type(screen.getByLabelText("From"), invalid);
      await user.click(screen.getByRole("button", { name: "Apply Window" }));
      expect(screen.getByRole("alert")).toBeInTheDocument();
      expect(onChange).not.toHaveBeenCalled();
    }
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "now-97m{Enter}");
    expect(onChange).toHaveBeenCalledWith(97);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("supports readable hour and day durations without losing minute precision", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={97} onChange={onChange} />);
    const trigger = screen.getByRole("button", { name: "Custom time window" });
    expect(trigger).toHaveTextContent("97m");
    await user.click(trigger);
    expect(screen.getByLabelText("From")).toHaveValue("now-97m");
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "now-36h");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenLastCalledWith(2160);
    await user.click(trigger);
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "NOW - 365d");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenLastCalledWith(525600);
  });

  it("resets stale draft state when the selected URL window changes", async () => {
    const user = userEvent.setup();
    const change = vi.fn();
    const view = render(<TimeRangePicker value={1440} onChange={change} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "invalid");
    view.rerender(<TimeRangePicker value={60} onChange={change} />);
    expect(screen.getByLabelText("From")).toHaveValue("now-1h");
    view.rerender(<TimeRangePicker value={1440} onChange={change} />);
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
  });

  it("moves keyboard focus into the custom panel and restores it on Escape", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={97} onChange={vi.fn()} />, { container: document.getElementById("scene-root")! });
    const trigger = screen.getByRole("button", { name: "Custom time window" });
    trigger.focus();
    await user.keyboard("{Enter}");
    await waitFor(() => expect(screen.getByLabelText("From")).toHaveFocus());
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "invalid");
    await user.keyboard("{Escape}");
    await waitFor(() => expect(trigger).toHaveFocus());
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    await user.keyboard("{Enter}");
    expect(screen.getByLabelText("From")).toHaveValue("now-97m");
  });

  it("dismisses on an outside click without applying the draft", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<><TimeRangePicker value={1440} onChange={onChange} /><button>Outside</button></>);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "now-5d");
    await user.click(screen.getByRole("button", { name: "Outside" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Outside" })).toHaveFocus();
  });
});
