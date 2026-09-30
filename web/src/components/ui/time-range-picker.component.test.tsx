import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { AnalyticsTimeRange } from "@/lib/analytics-time-range";
import { TimeRangePicker } from "./time-range-picker";

function nativeCalendar(endpoint: "From" | "To"): HTMLInputElement {
  const button = screen.getByRole("button", { name: `Choose ${endpoint} date` });
  const calendar = document.getElementById(button.getAttribute("aria-controls")!) as HTMLInputElement;
  expect(calendar).toHaveAttribute("type", "date");
  expect(calendar).toHaveAttribute("aria-hidden", "true");
  expect(calendar).toHaveAttribute("tabindex", "-1");
  return calendar;
}

function localDate(timestamp: string | number): string {
  const date = new Date(timestamp);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function localTime(timestamp: string): string {
  const date = new Date(timestamp);
  const minutes = `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
  if (!date.getSeconds() && !date.getMilliseconds()) return minutes;
  const seconds = `${minutes}:${String(date.getSeconds()).padStart(2, "0")}`;
  return date.getMilliseconds() ? `${seconds}.${String(date.getMilliseconds()).padStart(3, "0")}` : seconds;
}

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

  it("opens one form with editable endpoints and adjacent calendar buttons", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    const dialog = screen.getByRole("dialog", { name: "Custom Time Window" });
    expect(dialog).toHaveClass("ui-popover");
    expect(screen.queryByRole("group", { name: "Window mode" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Choose From date" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Choose To date" })).toBeInTheDocument();
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
    expect(screen.getByLabelText("To")).toHaveValue("Now");
    expect(screen.getByLabelText("To")).not.toHaveAttribute("readonly");
    expect(document.body).toHaveAttribute("data-modal-open", "false");
  });

  it("accepts exact minute windows and rejects invalid or unsupported expressions", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    for (const invalid of ["", "now", "now-0m", "now-1.5m", "now-525601m", "now-2y", "2026-02-30", "2026-09-30T25:00:00Z"]) {
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
    const from = screen.getByLabelText("From");
    const to = screen.getByLabelText("To");
    expect(from).toHaveAttribute("type", "text");
    expect(to).toHaveAttribute("type", "text");
    fireEvent.change(from, { target: { value: "2026-03-29" } });
    fireEvent.change(to, { target: { value: "2026-03-29" } });
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
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
  for (const [from, to] of [["", "2026-09-30"], ["2026-09-01", ""], ["2026-09-30", "2026-09-01"], ["2026-09-30T12:00:00Z", "2026-09-30T12:00:00Z"]]) {
    fireEvent.change(screen.getByLabelText("From"), { target: { value: from } });
    fireEvent.change(screen.getByLabelText("To"), { target: { value: to } });
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  }
});

it("opens each endpoint's calendar and preserves the other field when choosing a date", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  const fromCalendar = nativeCalendar("From");
  const toCalendar = nativeCalendar("To");
  const showFrom = vi.fn();
  const showTo = vi.fn();
  Object.defineProperty(fromCalendar, "showPicker", { configurable: true, value: showFrom });
  Object.defineProperty(toCalendar, "showPicker", { configurable: true, value: showTo });

  await user.click(screen.getByRole("button", { name: "Choose From date" }));
  expect(showFrom).toHaveBeenCalledOnce();
  expect(showTo).not.toHaveBeenCalled();
  fireEvent.change(fromCalendar, { target: { value: "2026-02-01" } });
  expect(screen.getByLabelText("From")).toHaveValue("2026-02-01");
  expect(screen.getByLabelText("To")).toHaveValue("Now");

  await user.click(screen.getByRole("button", { name: "Choose To date" }));
  expect(showTo).toHaveBeenCalledOnce();
  expect(showFrom).toHaveBeenCalledOnce();
  fireEvent.change(toCalendar, { target: { value: "2026-02-28" } });
  expect(screen.getByLabelText("From")).toHaveValue("2026-02-01");
  expect(screen.getByLabelText("To")).toHaveValue("2026-02-28");
  expect(onChange).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(onChange).toHaveBeenCalledWith({
    kind: "absolute",
    startAt: new Date(2026, 1, 1).toISOString(),
    endAt: new Date(2026, 2, 1).toISOString(),
  });
});

it("leaves relative calendars unselected so choosing today replaces Now", async () => {
  const user = userEvent.setup();
  const onChange = vi.fn();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  expect(nativeCalendar("From")).toHaveValue("");
  expect(nativeCalendar("To")).toHaveValue("");
  const today = new Date();
  const date = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;
  fireEvent.change(nativeCalendar("To"), { target: { value: date } });
  expect(screen.getByLabelText("To")).toHaveValue(date);
  expect(screen.getByLabelText("From")).toHaveValue("now-1d");
  fireEvent.change(nativeCalendar("To"), { target: { value: "" } });
  expect(screen.getByLabelText("To")).toHaveValue("");
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(screen.getByRole("alert")).toBeInTheDocument();
  expect(onChange).not.toHaveBeenCalled();
  fireEvent.change(screen.getByLabelText("To"), { target: { value: "Now" } });
  expect(nativeCalendar("To")).toHaveValue("");
});

it("freezes a date-to-Now range when applied and preserves its exact end when reopened", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const now = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-30T12:34:56.789Z"));
  try {
    const view = render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-09-01" } });
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    const applied: AnalyticsTimeRange = {
      kind: "absolute",
      startAt: new Date(2026, 8, 1).toISOString(),
      endAt: "2026-09-30T12:34:56.789Z",
    };
    expect(onChange).toHaveBeenLastCalledWith(applied);
    view.rerender(<TimeRangePicker value={applied} onChange={onChange} />);
    now.mockReturnValue(Date.parse("2026-10-01T16:00:00Z"));
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-01");
    expect(screen.getByLabelText("To")).toHaveValue(localDate(applied.endAt));
    expect(screen.getByRole("textbox", { name: "To time" })).toHaveValue(localTime(applied.endAt));
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenLastCalledWith(applied);
  } finally {
    now.mockRestore();
  }
});

it("resolves both relative endpoints against the same current instant", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const now = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-30T12:34:56.789Z"));
  try {
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    fireEvent.change(screen.getByLabelText("From"), { target: { value: "now-2h" } });
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "now-30m" } });
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute",
      startAt: "2026-09-30T10:34:56.789Z",
      endAt: "2026-09-30T12:04:56.789Z",
    });
  } finally {
    now.mockRestore();
  }
});

it("combines a relative start with an inclusive calendar end", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const now = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-30T12:00:00Z"));
  try {
    render(<TimeRangePicker value={{ kind: "relative", minutes: 2880 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-09-29" } });
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute",
      startAt: "2026-09-28T12:00:00.000Z",
      endAt: new Date(2026, 8, 30).toISOString(),
    });
  } finally {
    now.mockRestore();
  }
});

it("preserves exact timestamp endpoints instead of expanding them to whole days", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const value = {
    kind: "absolute" as const,
    startAt: "2026-09-29T15:15:31.123Z",
    endAt: "2026-09-30T12:34:56.789Z",
  };
  render(<TimeRangePicker value={value} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  expect(screen.getByLabelText("From")).toHaveValue(localDate(value.startAt));
  expect(screen.getByLabelText("To")).toHaveValue(localDate(value.endAt));
  expect(screen.getByRole("textbox", { name: "From time" })).toHaveValue(localTime(value.startAt));
  expect(screen.getByRole("textbox", { name: "To time" })).toHaveValue(localTime(value.endAt));
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(onChange).toHaveBeenCalledWith(value);
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
  fireEvent.change(screen.getByLabelText("From"), { target: { value: from } });
  fireEvent.change(screen.getByLabelText("To"), { target: { value: to } });
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  const [year, month, day] = to.split("-").map(Number);
  expect(onChange).toHaveBeenCalledWith({
    kind: "absolute", startAt: new Date(`${from}T00:00:00`).toISOString(),
    endAt: new Date(year, month - 1, day + 1).toISOString(),
  });
});


it("provides editable HH:MM fields and Now/Reset shortcuts in the unified form", async () => {
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  expect(screen.getByRole("textbox", { name: "From time" })).toHaveAttribute("placeholder", "HH:MM");
  expect(screen.getByRole("textbox", { name: "To time" })).toHaveAttribute("placeholder", "HH:MM");
  expect(screen.getByRole("button", { name: "Now" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Reset" })).toBeInTheDocument();
});

it("restores only the end with Now and resets the complete draft without applying", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const value: AnalyticsTimeRange = {
    kind: "absolute",
    startAt: new Date(2026, 8, 29, 8, 15).toISOString(),
    endAt: new Date(2026, 8, 30, 16, 45).toISOString(),
  };
  render(<TimeRangePicker value={value} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  await user.click(screen.getByRole("button", { name: "Now" }));
  expect(screen.getByLabelText("From")).toHaveValue("2026-09-29");
  expect(screen.getByRole("textbox", { name: "From time" })).toHaveValue("08:15");
  expect(screen.getByLabelText("To")).toHaveValue("Now");
  expect(screen.getByRole("textbox", { name: "To time" })).toHaveValue("");
  expect(onChange).not.toHaveBeenCalled();

  fireEvent.change(screen.getByRole("textbox", { name: "From time" }), { target: { value: "25:00" } });
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(screen.getByRole("alert")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Reset" }));
  expect(screen.getByLabelText("From")).toHaveValue("now-1d");
  expect(screen.getByLabelText("To")).toHaveValue("Now");
  expect(screen.getByRole("textbox", { name: "From time" })).toHaveValue("");
  expect(screen.getByRole("textbox", { name: "To time" })).toHaveValue("");
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(onChange).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(onChange).toHaveBeenCalledWith({ kind: "relative", minutes: 1440 });
});

it.each([
  ["2026-09-30", "08:15", "2026-09-30", "16:45"],
  ["2026-09-29", "23:30", "2026-09-30", "00:00"],
])("applies explicit local times from %s %s to %s %s as exact bounds", async (from, fromTime, to, toTime) => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const view = render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  fireEvent.change(screen.getByLabelText("From"), { target: { value: from } });
  fireEvent.change(screen.getByLabelText("To"), { target: { value: to } });
  fireEvent.change(screen.getByRole("textbox", { name: "From time" }), { target: { value: fromTime } });
  fireEvent.change(screen.getByRole("textbox", { name: "To time" }), { target: { value: toTime } });
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  const applied: AnalyticsTimeRange = {
    kind: "absolute",
    startAt: new Date(`${from}T${fromTime}:00`).toISOString(),
    endAt: new Date(`${to}T${toTime}:00`).toISOString(),
  };
  expect(onChange).toHaveBeenCalledWith(applied);
  view.rerender(<TimeRangePicker value={applied} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  expect(screen.getByLabelText("To")).toHaveValue(to);
  expect(screen.getByRole("textbox", { name: "To time" })).toHaveValue(toTime);
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(onChange).toHaveBeenLastCalledWith(applied);
});

it("rejects invalid and reversed local times without normalizing them", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  fireEvent.change(screen.getByLabelText("From"), { target: { value: "2026-09-30" } });
  fireEvent.change(screen.getByLabelText("To"), { target: { value: "2026-09-30" } });
  fireEvent.change(screen.getByRole("textbox", { name: "To time" }), { target: { value: "12:00" } });
  for (const invalid of ["24:00", "12:60", "12:00:60", "not a time", "13:00", "12:00"]) {
    fireEvent.change(screen.getByRole("textbox", { name: "From time" }), { target: { value: invalid } });
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  }
});

it("preserves a chosen time when changing or clearing its calendar date", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  fireEvent.change(nativeCalendar("From"), { target: { value: "2026-09-28" } });
  fireEvent.change(screen.getByRole("textbox", { name: "From time" }), { target: { value: "08:15" } });
  fireEvent.change(nativeCalendar("From"), { target: { value: "2026-09-29" } });
  expect(screen.getByLabelText("From")).toHaveValue("2026-09-29");
  expect(screen.getByRole("textbox", { name: "From time" })).toHaveValue("08:15");
  expect(screen.getByLabelText("To")).toHaveValue("Now");
  fireEvent.change(nativeCalendar("From"), { target: { value: "" } });
  expect(screen.getByLabelText("From")).toHaveValue("");
  expect(screen.getByRole("textbox", { name: "From time" })).toHaveValue("08:15");
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(screen.getByRole("alert")).toBeInTheDocument();
  expect(onChange).not.toHaveBeenCalled();
});

it("turns relative endpoints into their resolved local dates when a time is entered", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const instant = new Date(2026, 8, 30, 0, 30).getTime();
  const now = vi.spyOn(Date, "now").mockReturnValue(instant);
  try {
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    fireEvent.change(screen.getByRole("textbox", { name: "From time" }), { target: { value: "09:00" } });
    fireEvent.change(screen.getByRole("textbox", { name: "To time" }), { target: { value: "10:00" } });
    expect(screen.getByLabelText("From")).toHaveValue(localDate(instant - 86_400_000));
    expect(screen.getByLabelText("To")).toHaveValue(localDate(instant));
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute",
      startAt: new Date(`${localDate(instant - 86_400_000)}T09:00:00`).toISOString(),
      endAt: new Date(`${localDate(instant)}T10:00:00`).toISOString(),
    });
  } finally {
    now.mockRestore();
  }
});

const skippedLocalTime = [
  { date: "2026-03-29", time: "02:30" },
  { date: "2018-11-04", time: "00:30" },
].find(({ date, time }) => localTime(`${date}T${time}:00`) !== time);

it.skipIf(!skippedLocalTime)("rejects a local time skipped by the daylight saving transition", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  fireEvent.change(screen.getByLabelText("From"), { target: { value: skippedLocalTime!.date } });
  fireEvent.change(screen.getByLabelText("To"), { target: { value: skippedLocalTime!.date } });
  fireEvent.change(screen.getByRole("textbox", { name: "From time" }), { target: { value: skippedLocalTime!.time } });
  fireEvent.change(screen.getByRole("textbox", { name: "To time" }), { target: { value: "12:00" } });
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(screen.getByRole("alert")).toBeInTheDocument();
  expect(onChange).not.toHaveBeenCalled();
});

it.each([
  ["2026-10-25T01:15:00.000Z", "2026-10-25T01:45:00.000Z"],
  ["2018-02-18T02:15:00.000Z", "2018-02-18T02:45:00.000Z"],
])("preserves the exact repeated-hour instant when reopening %s through %s", async (startAt, endAt) => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const value = { kind: "absolute" as const, startAt, endAt };
  render(<TimeRangePicker value={value} onChange={onChange} />);
  await user.click(screen.getByRole("button", { name: "Custom time window" }));
  expect(screen.getByLabelText("From")).toHaveValue(localDate(startAt));
  expect(screen.getByRole("textbox", { name: "From time" })).toHaveValue(localTime(startAt));
  expect(screen.getByLabelText("To")).toHaveValue(localDate(endAt));
  expect(screen.getByRole("textbox", { name: "To time" })).toHaveValue(localTime(endAt));
  await user.click(screen.getByRole("button", { name: "Apply Window" }));
  expect(onChange).toHaveBeenCalledWith(value);
});
