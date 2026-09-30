import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { AnalyticsTimeRange } from "@/lib/analytics-time-range";
import { TimeRangePicker } from "./time-range-picker";

function nativeCalendar(endpoint: "From" | "To"): HTMLInputElement {
  const button = screen.getByRole("button", { name: `Choose ${endpoint} date` });
  const calendar = document.getElementById(button.getAttribute("aria-controls")!) as HTMLInputElement;
  expect(calendar).toHaveAttribute("type", "date");
  expect(calendar).not.toHaveAttribute("step");
  expect(calendar).toHaveAttribute("aria-hidden", "true");
  expect(calendar).toHaveAttribute("tabindex", "-1");
  return calendar;
}

function localDateTime(timestamp: string): string {
  const date = new Date(timestamp);
  const day = `${String(date.getFullYear()).padStart(4, "0")}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
  const time = `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}:${String(date.getSeconds()).padStart(2, "0")}`;
  return `${day} ${time}${date.getMilliseconds() ? `.${String(date.getMilliseconds()).padStart(3, "0")}` : ""}`;
}

function changeEndpoint(label: "From" | "To", value: string) {
  fireEvent.change(screen.getByLabelText(label), { target: { value } });
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


describe("combined calendar time window", () => {
  it("uses one timestamp field per endpoint and identifies the browser time zone", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    expect(screen.getAllByRole("textbox")).toHaveLength(2);
    expect(screen.queryByRole("textbox", { name: "From time" })).not.toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "To time" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("From")).toHaveAttribute("placeholder", "YYYY-MM-DD HH:mm:ss");
    expect(screen.getByLabelText("To")).toHaveAttribute("placeholder", "YYYY-MM-DD HH:mm:ss");
    expect(screen.getByText("Browser Time Zone")).toBeInTheDocument();
    expect(screen.getByText(Intl.DateTimeFormat().resolvedOptions().timeZone)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Now" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Reset" })).toBeInTheDocument();
  });

  it("applies typed calendar-only dates as inclusive local days", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", "2026-03-29");
    changeEndpoint("To", "2026-03-29");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute",
      startAt: new Date(2026, 2, 29).toISOString(),
      endAt: new Date(2026, 2, 30).toISOString(),
    });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("restores exact midnight boundaries and switches back to a rolling preset", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const value: AnalyticsTimeRange = {
      kind: "absolute",
      startAt: new Date(2026, 8, 1).toISOString(),
      endAt: new Date(2026, 9, 1).toISOString(),
    };
    render(<TimeRangePicker value={value} onChange={onChange} />);
    expect(screen.getByRole("button", { name: "Last 24 Hours" })).toHaveAttribute("aria-pressed", "false");
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-01 00:00:00");
    expect(screen.getByLabelText("To")).toHaveValue("2026-10-01 00:00:00");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenLastCalledWith(value);
    await user.click(screen.getByRole("button", { name: "Last 24 Hours" }));
    expect(onChange).toHaveBeenLastCalledWith({ kind: "relative", minutes: 1440 });
  });

  it("rejects missing and reversed calendar ranges without applying", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    for (const [from, to] of [["", "2026-09-30"], ["2026-09-01", ""], ["2026-09-30", "2026-09-01"], ["2026-09-30T12:00:00Z", "2026-09-30T12:00:00Z"]]) {
      changeEndpoint("From", from);
      changeEndpoint("To", to);
      await user.click(screen.getByRole("button", { name: "Apply Window" }));
      expect(screen.getByRole("alert")).toBeInTheDocument();
      expect(onChange).not.toHaveBeenCalled();
    }
  });

  it("opens date-only calendars and applies fresh selections as inclusive local days", async () => {
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
      kind: "absolute", startAt: new Date(2026, 1, 1).toISOString(), endAt: new Date(2026, 2, 1).toISOString(),
    });
  });

  it("leaves relative and invalid calendar values unselected without inventing a date", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    expect(nativeCalendar("From")).toHaveValue("");
    expect(nativeCalendar("To")).toHaveValue("");
    for (const value of ["now-2h", "Now", "invalid", "2026-02-30", "2026-02-30 12:00:00"]) {
      changeEndpoint("To", value);
      expect(nativeCalendar("To")).toHaveValue("");
      expect(screen.getByLabelText("To")).toHaveValue(value);
    }
    changeEndpoint("To", "2026-09-30");
    expect(nativeCalendar("To")).toHaveValue("2026-09-30");
  });

  it("prefills dates and preserves typed local or RFC3339 clock precision when changing dates", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", "2026-09-29 08:15:31.123");
    expect(nativeCalendar("From")).toHaveValue("2026-09-29");
    const timestamp = "2026-09-30T12:34:56.789+02:00";
    const localClock = localDateTime(timestamp).split(" ")[1];
    changeEndpoint("To", timestamp);
    expect(nativeCalendar("To")).toHaveValue(localDateTime(timestamp).slice(0, 10));
    fireEvent.change(nativeCalendar("From"), { target: { value: "2026-09-30" } });
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-30 08:15:31.123");
    expect(screen.getByLabelText("To")).toHaveValue(timestamp);
    fireEvent.change(nativeCalendar("To"), { target: { value: "2026-10-01" } });
    expect(screen.getByLabelText("To")).toHaveValue(`2026-10-01 ${localClock}`);
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-30 08:15:31.123");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute", startAt: new Date("2026-09-30T08:15:31.123").toISOString(), endAt: new Date(`2026-10-01T${localClock}`).toISOString(),
    });
  });

  it("clears only the selected endpoint when clearing its native calendar", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("To", "2026-09-30 12:30:00");
    fireEvent.change(nativeCalendar("To"), { target: { value: "" } });
    expect(screen.getByLabelText("To")).toHaveValue("");
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  });

  it("freezes date-to-Now when applied and preserves its exact end when reopened", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const now = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-30T12:34:56.789Z"));
    try {
      const view = render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
      await user.click(screen.getByRole("button", { name: "Custom time window" }));
      changeEndpoint("From", "2026-09-01");
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
      expect(screen.getByLabelText("From")).toHaveValue("2026-09-01 00:00:00");
      expect(screen.getByLabelText("To")).toHaveValue(localDateTime(applied.endAt));
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
      changeEndpoint("From", "now-2h");
      changeEndpoint("To", "now-30m");
      await user.click(screen.getByRole("button", { name: "Apply Window" }));
      expect(onChange).toHaveBeenCalledWith({
        kind: "absolute", startAt: "2026-09-30T10:34:56.789Z", endAt: "2026-09-30T12:04:56.789Z",
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
      changeEndpoint("To", "2026-09-29");
      await user.click(screen.getByRole("button", { name: "Apply Window" }));
      expect(onChange).toHaveBeenCalledWith({
        kind: "absolute", startAt: "2026-09-28T12:00:00.000Z", endAt: new Date(2026, 8, 30).toISOString(),
      });
    } finally {
      now.mockRestore();
    }
  });

  it("preserves exact timestamp endpoints including milliseconds instead of expanding them", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const value: AnalyticsTimeRange = {
      kind: "absolute", startAt: "2026-09-29T15:15:31.123Z", endAt: "2026-09-30T12:34:56.789Z",
    };
    render(<TimeRangePicker value={value} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    expect(screen.getByLabelText("From")).toHaveValue(localDateTime(value.startAt));
    expect(screen.getByLabelText("To")).toHaveValue(localDateTime(value.endAt));
    expect(nativeCalendar("From")).toHaveValue(localDateTime(value.startAt).slice(0, 10));
    expect(nativeCalendar("To")).toHaveValue(localDateTime(value.endAt).slice(0, 10));
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
    changeEndpoint("From", from);
    changeEndpoint("To", to);
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    const [year, month, day] = to.split("-").map(Number);
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute", startAt: new Date(`${from}T00:00:00`).toISOString(),
      endAt: new Date(year, month - 1, day + 1).toISOString(),
    });
  });

  it("restores only the end with Now and resets the complete draft without applying", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const value: AnalyticsTimeRange = {
      kind: "absolute", startAt: new Date(2026, 8, 29, 8, 15).toISOString(), endAt: new Date(2026, 8, 30, 16, 45).toISOString(),
    };
    render(<TimeRangePicker value={value} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await user.click(screen.getByRole("button", { name: "Now" }));
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-29 08:15:00");
    expect(screen.getByLabelText("To")).toHaveValue("Now");
    expect(nativeCalendar("To")).toHaveValue("");
    expect(onChange).not.toHaveBeenCalled();
    changeEndpoint("From", "2026-09-29 25:00:00");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Reset" }));
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
    expect(screen.getByLabelText("To")).toHaveValue("Now");
    expect(nativeCalendar("From")).toHaveValue("");
    expect(nativeCalendar("To")).toHaveValue("");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({ kind: "relative", minutes: 1440 });
  });

  it.each([
    ["2026-09-30 08:15", "2026-09-30 16:45"],
    ["2026-09-29 23:30:25", "2026-09-30 00:00:00"],
    ["2026-09-30 08:15:31.123", "2026-09-30 16:45:56.789"],
  ])("applies combined local times from %s to %s as exact bounds", async (from, to) => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const view = render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", from);
    changeEndpoint("To", to);
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    const applied: AnalyticsTimeRange = {
      kind: "absolute", startAt: new Date(from.replace(" ", "T")).toISOString(), endAt: new Date(to.replace(" ", "T")).toISOString(),
    };
    expect(onChange).toHaveBeenCalledWith(applied);
    view.rerender(<TimeRangePicker value={applied} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    expect(screen.getByLabelText("From")).toHaveValue(localDateTime(applied.startAt));
    expect(screen.getByLabelText("To")).toHaveValue(localDateTime(applied.endAt));
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenLastCalledWith(applied);
  });

  it("rejects invalid and reversed combined local timestamps without normalizing them", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("To", "2026-09-30 12:00:00");
    for (const invalid of ["2026-09-30 24:00", "2026-09-30 12:60", "2026-09-30 12:00:60", "2026-09-30 12:00:00.1234", "2026-02-30 12:00:00", "2026-09-30 not a time", "2026-09-30 13:00", "2026-09-30 12:00"]) {
      changeEndpoint("From", invalid);
      await user.click(screen.getByRole("button", { name: "Apply Window" }));
      expect(screen.getByRole("alert")).toBeInTheDocument();
      expect(onChange).not.toHaveBeenCalled();
    }
  });

  it("accepts explicit RFC3339 offsets independently of the browser time zone", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", "2026-09-30T08:15:31.123+02:00");
    changeEndpoint("To", "2026-09-30T08:45:56.789-03:00");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute", startAt: "2026-09-30T06:15:31.123Z", endAt: "2026-09-30T11:45:56.789Z",
    });
  });

  const skippedLocalTime = ["2026-03-29 02:30:00", "2018-11-04 00:30:00"]
    .find((value) => localDateTime(value.replace(" ", "T")) !== value);

  it.skipIf(!skippedLocalTime)("rejects a local time skipped by the daylight saving transition", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", skippedLocalTime!);
    changeEndpoint("To", `${skippedLocalTime!.slice(0, 10)} 12:00:00`);
    expect(nativeCalendar("From")).toHaveValue("");
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
    expect(screen.getByLabelText("From")).toHaveValue(localDateTime(startAt));
    expect(screen.getByLabelText("To")).toHaveValue(localDateTime(endAt));
    fireEvent.change(nativeCalendar("From"), { target: { value: localDateTime(startAt).slice(0, 10) } });
    fireEvent.change(nativeCalendar("To"), { target: { value: localDateTime(endAt).slice(0, 10) } });
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith(value);
  });

  it("replaces an original exact instant when its calendar date is edited", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const startAt = "2026-10-25T01:15:00.000Z";
    const localClock = localDateTime(startAt).split(" ")[1];
    render(<TimeRangePicker value={{
      kind: "absolute", startAt, endAt: "2026-10-26T12:00:00.000Z",
    }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    fireEvent.change(nativeCalendar("From"), { target: { value: "2026-10-23" } });
    expect(screen.getByLabelText("From")).toHaveValue(`2026-10-23 ${localClock}`);
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute", startAt: new Date(`2026-10-23T${localClock}`).toISOString(), endAt: "2026-10-26T12:00:00.000Z",
    });
  });

  it.skipIf(!skippedLocalTime)("preserves a typed clock when a new calendar date would fall in a DST gap", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const gapDay = skippedLocalTime!.slice(0, 10);
    const [year, month, day] = gapDay.split("-").map(Number);
    const previousDay = localDateTime(new Date(year, month - 1, day - 1, 12).toISOString()).slice(0, 10);
    const clock = skippedLocalTime!.slice(11);
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", `${previousDay} ${clock}`);
    changeEndpoint("To", `${gapDay} 12:00:00`);
    expect(nativeCalendar("From")).toHaveValue(previousDay);
    fireEvent.change(nativeCalendar("From"), { target: { value: gapDay } });
    expect(screen.getByLabelText("From")).toHaveValue(skippedLocalTime);
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  });
});
