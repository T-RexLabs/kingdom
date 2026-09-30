import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TimeRangePicker } from "./time-range-picker";

describe("rolling time window", () => {
  it("exposes compact one-click presets and marks the current window", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    expect(screen.getByRole("button", { name: "Last 24 Hours" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "Last 30 Minutes" })).toHaveTextContent("30m");
    expect(screen.getByRole("button", { name: "Last 30 Days" })).toHaveTextContent("30d");
    await user.click(screen.getByRole("button", { name: "Last 30 Minutes" }));
    expect(onChange).toHaveBeenCalledWith({ kind: "relative", minutes: 30 });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("opens an anchored custom window with an honest fixed Now endpoint", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
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
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
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
    expect(onChange).toHaveBeenCalledWith({ kind: "relative", minutes: 97 });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("supports readable hour and day durations without losing minute precision", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 97 }} onChange={onChange} />);
    const trigger = screen.getByRole("button", { name: "Custom time window" });
    expect(trigger).toHaveTextContent("97m");
    await user.click(trigger);
    expect(screen.getByLabelText("From")).toHaveValue("now-97m");
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "now-36h");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenLastCalledWith({ kind: "relative", minutes: 2160 });
    await user.click(trigger);
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "NOW - 365d");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenLastCalledWith({ kind: "relative", minutes: 525600 });
  });

  it("resets stale draft state when the selected URL window changes", async () => {
    const user = userEvent.setup();
    const change = vi.fn();
    const view = render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={change} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "invalid");
    view.rerender(<TimeRangePicker value={{ kind: "relative", minutes: 60 }} onChange={change} />);
    expect(screen.getByLabelText("From")).toHaveValue("now-1h");
    view.rerender(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={change} />);
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
  });

  it("moves keyboard focus into the custom panel and restores it on Escape", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 97 }} onChange={vi.fn()} />, { container: document.getElementById("scene-root")! });
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
    render(<><TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} /><button>Outside</button></>);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await user.clear(screen.getByLabelText("From"));
    await user.type(screen.getByLabelText("From"), "now-5d");
    await user.click(screen.getByRole("button", { name: "Outside" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Outside" })).toHaveFocus();
  });
});


describe("calendar time window", () => {
  it("applies both selected local calendar days as exact absolute bounds", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await user.click(screen.getByRole("button", { name: "Date Range" }));
    const from = screen.getByLabelText("From");
    const to = screen.getByLabelText("To");
    expect(from).toHaveAttribute("type", "date");
    expect(to).toHaveAttribute("type", "date");
    fireEvent.change(from, { target: { value: "2026-03-29" } });
    fireEvent.change(to, { target: { value: "2026-03-29" } });
    await user.click(screen.getByRole("button", { name: "Apply Range" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute",
      startAt: new Date(2026, 2, 29).toISOString(),
      endAt: new Date(2026, 2, 30).toISOString(),
    });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });
});

it("restores a selected calendar range and switches back to a rolling preset", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{
    kind: "absolute",
    startAt: new Date(2026, 8, 1).toISOString(),
    endAt: new Date(2026, 9, 1).toISOString(),
  }} onChange={onChange} />);
  expect(screen.getByRole("button", { name: "Last 24 Hours" })).toHaveAttribute("aria-pressed", "false");
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  expect(screen.getByRole("button", { name: "Date Range" })).toHaveAttribute("aria-pressed", "true");
  expect(screen.getByLabelText("From")).toHaveValue("2026-09-01");
  expect(screen.getByLabelText("To")).toHaveValue("2026-09-30");
  await user.keyboard("{Escape}");
  await user.click(screen.getByRole("button", { name: "Last 24 Hours" }));
  expect(onChange).toHaveBeenCalledWith({ kind: "relative", minutes: 1440 });
});

it("rejects missing and reversed calendar ranges without applying", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  await user.click(screen.getByRole("button", { name: "Date Range" }));
  for (const [from, to] of [["", "2026-09-30"], ["2026-09-30", "2026-09-01"]]) {
    fireEvent.change(screen.getByLabelText("From"), { target: { value: from } });
    fireEvent.change(screen.getByLabelText("To"), { target: { value: to } });
    await user.click(screen.getByRole("button", { name: "Apply Range" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  }
});

it("keeps calendar drafts while exploring relative mode, without applying either", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  await user.click(screen.getByRole("button", { name: "Date Range" }));
  fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-02-01" } });
  fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-02-28" } });
  await user.click(screen.getByRole("button", { name: "Relative" }));
  expect(screen.getByLabelText("From")).toHaveValue("now-1d");
  await user.click(screen.getByRole("button", { name: "Date Range" }));
  expect(screen.getByLabelText("From")).toHaveValue("2026-02-01");
  expect(screen.getByLabelText("To")).toHaveValue("2026-02-28");
  await user.keyboard("{Escape}");
  expect(onChange).not.toHaveBeenCalled();
});


it.each([
  ["2018-11-04", "2018-11-04"],
  ["2025-10-26", "2026-10-25"],
  ["2024-01-01", "2026-09-30"],
])("uses calendar midnights for %s through %s without an elapsed-duration limit", async (from, to) => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  await user.click(screen.getByRole("button", { name: "Date Range" }));
  fireEvent.change(screen.getByLabelText("From"), { target: { value: from } });
  fireEvent.change(screen.getByLabelText("To"), { target: { value: to } });
  await user.click(screen.getByRole("button", { name: "Apply Range" }));
  const [year, month, day] = to.split("-").map(Number);
  expect(onChange).toHaveBeenCalledWith({
    kind: "absolute", startAt: new Date(`${from}T00:00:00`).toISOString(),
    endAt: new Date(year, month - 1, day + 1).toISOString(),
  });
});
