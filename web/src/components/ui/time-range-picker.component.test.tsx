import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { AnalyticsTimeRange } from "@/lib/analytics-time-range";
import { TimeRangePicker } from "./time-range-picker";

type TestUser = ReturnType<typeof userEvent.setup>;

async function openCalendar(user: TestUser, endpoint: "From" | "To") {
  await user.click(screen.getByRole("button", { name: `Choose ${endpoint} date` }));
  return screen.getByRole("dialog", { name: `${endpoint} Calendar` });
}

function calendarDay(calendar: HTMLElement, day: string) {
  const name = new Intl.DateTimeFormat(undefined, { dateStyle: "full" }).format(new Date(`${day}T12:00:00`));
  return within(calendar).getByRole("button", { name });
}

async function chooseCalendarDay(user: TestUser, endpoint: "From" | "To", day: string) {
  const calendar = await openCalendar(user, endpoint);
  // Navigate the same month controls used by people; selected and adjacent dates need no navigation.
  for (let months = 0; !calendar.querySelector(`button[data-date="${day}"]`) && months < 240; months += 1) {
    const days = Array.from(calendar.querySelectorAll<HTMLButtonElement>("button[data-date]"));
    const middleDay = days[Math.floor(days.length / 2)].dataset.date!;
    await user.click(within(calendar).getByRole("button", { name: day < middleDay ? "Previous month" : "Next month" }));
  }
  await user.click(calendarDay(calendar, day));
  expect(screen.queryByRole("dialog", { name: `${endpoint} Calendar` })).not.toBeInTheDocument();
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
  it("uses application calendars without native date controls or a separate Now button", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    expect(document.querySelector('input[type="date"], input[type="datetime-local"]')).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Now" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Reset" })).toBeInTheDocument();
  });

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
    expect(screen.queryByRole("button", { name: "Now" })).not.toBeInTheDocument();
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

  it("opens themed date-only calendars and applies fresh selections as inclusive local days", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await chooseCalendarDay(user, "From", "2026-02-01");
    expect(screen.getByLabelText("From")).toHaveValue("2026-02-01");
    expect(screen.getByLabelText("To")).toHaveValue("Now");
    await waitFor(() => expect(screen.getByRole("button", { name: "Choose From date" })).toHaveFocus());
    await chooseCalendarDay(user, "To", "2026-02-28");
    expect(screen.getByLabelText("From")).toHaveValue("2026-02-01");
    expect(screen.getByLabelText("To")).toHaveValue("2026-02-28");
    expect(onChange).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute", startAt: new Date(2026, 1, 1).toISOString(), endAt: new Date(2026, 2, 1).toISOString(),
    });
  });

  it("leaves relative and invalid calendar dates unselected and focuses today", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    const today = localDateTime(new Date().toISOString()).slice(0, 10);
    for (const value of ["now-2h", "Now", "invalid", "2026-02-30", "2026-02-30 12:00:00"]) {
      changeEndpoint("To", value);
      const calendar = await openCalendar(user, "To");
      expect(calendar.querySelector('[aria-selected="true"]')).not.toBeInTheDocument();
      expect(calendarDay(calendar, today)).toHaveAttribute("aria-current", "date");
      await waitFor(() => expect(calendarDay(calendar, today)).toHaveFocus());
      expect(screen.getByLabelText("To")).toHaveValue(value);
      await user.keyboard("{Escape}");
    }
    changeEndpoint("To", "2026-09-30");
    const calendar = await openCalendar(user, "To");
    expect(calendarDay(calendar, "2026-09-30").closest('[aria-selected]')).toHaveAttribute("aria-selected", "true");
  });

  it("preserves typed local or RFC3339 clock precision when selecting application calendar dates", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", "2026-09-29 08:15:31.123");
    const timestamp = "2026-09-30T12:34:56.789+02:00";
    const localClock = localDateTime(timestamp).split(" ")[1];
    changeEndpoint("To", timestamp);
    await chooseCalendarDay(user, "From", "2026-09-30");
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-30 08:15:31.123");
    expect(screen.getByLabelText("To")).toHaveValue(timestamp);
    await chooseCalendarDay(user, "To", "2026-10-01");
    expect(screen.getByLabelText("To")).toHaveValue(`2026-10-01 ${localClock}`);
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-30 08:15:31.123");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(onChange).toHaveBeenCalledWith({
      kind: "absolute", startAt: new Date("2026-09-30T08:15:31.123").toISOString(), endAt: new Date(`2026-10-01T${localClock}`).toISOString(),
    });
  });

  it("clears only the selected endpoint through its editable text field", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("To", "2026-09-30 12:30:00");
    await user.clear(screen.getByLabelText("To"));
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

  it("accepts typed now and resets the complete draft without applying", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    const value: AnalyticsTimeRange = {
      kind: "absolute", startAt: new Date(2026, 8, 29, 8, 15).toISOString(), endAt: new Date(2026, 8, 30, 16, 45).toISOString(),
    };
    render(<TimeRangePicker value={value} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await user.clear(screen.getByLabelText("To"));
    await user.type(screen.getByLabelText("To"), "now");
    expect(screen.getByLabelText("From")).toHaveValue("2026-09-29 08:15:00");
    expect(screen.getByLabelText("To")).toHaveValue("now");
    expect(onChange).not.toHaveBeenCalled();
    changeEndpoint("From", "2026-09-29 25:00:00");
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Reset" }));
    expect(screen.getByLabelText("From")).toHaveValue("now-1d");
    expect(screen.getByLabelText("To")).toHaveValue("Now");
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
    await chooseCalendarDay(user, "From", localDateTime(startAt).slice(0, 10));
    await chooseCalendarDay(user, "To", localDateTime(endAt).slice(0, 10));
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
    await chooseCalendarDay(user, "From", "2026-10-23");
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
    await chooseCalendarDay(user, "From", gapDay);
    expect(screen.getByLabelText("From")).toHaveValue(skippedLocalTime);
    await user.click(screen.getByRole("button", { name: "Apply Window" }));
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  });
});


describe("application calendar interaction", () => {
  it("uses the themed popover and focuses the selected date with one tabbable day", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", "2026-09-30 08:15:31.123");
    const calendar = await openCalendar(user, "From");
    expect(calendar).toHaveClass("ui-popover");
    expect(document.querySelector('input[type="date"], input[type="datetime-local"]')).not.toBeInTheDocument();
    const selectedDay = calendarDay(calendar, "2026-09-30");
    expect(selectedDay.closest('[aria-selected]')).toHaveAttribute("aria-selected", "true");
    await waitFor(() => expect(selectedDay).toHaveFocus());
    expect(calendar.querySelectorAll('button[data-date][tabindex="0"]')).toHaveLength(1);
    expect(within(calendar).getAllByRole("columnheader")[0]).toHaveTextContent(/Mo/i);
  });

  it("navigates months across years without changing the draft or applying", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", "2026-01-15");
    const calendar = await openCalendar(user, "From");
    await user.click(within(calendar).getByRole("button", { name: "Previous month" }));
    expect(calendarDay(calendar, "2025-12-15")).toBeInTheDocument();
    await user.click(within(calendar).getByRole("button", { name: "Next month" }));
    expect(calendarDay(calendar, "2026-01-15")).toBeInTheDocument();
    expect(screen.getByLabelText("From")).toHaveValue("2026-01-15");
    expect(onChange).not.toHaveBeenCalled();
  });

  it("navigates days, weeks, months and years by keyboard with month-end clamping", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    changeEndpoint("From", "2026-01-31 08:15:00");
    const calendar = await openCalendar(user, "From");
    await waitFor(() => expect(calendarDay(calendar, "2026-01-31")).toHaveFocus());
    const movements = [
      ["{PageDown}", "2026-02-28"],
      ["{Shift>}{PageDown}{/Shift}", "2027-02-28"],
      ["{Home}", "2027-02-22"],
      ["{End}", "2027-02-28"],
      ["{ArrowRight}", "2027-03-01"],
      ["{ArrowUp}", "2027-02-22"],
      ["{ArrowLeft}", "2027-02-21"],
      ["{ArrowDown}", "2027-02-28"],
      ["{Shift>}{PageUp}{/Shift}", "2026-02-28"],
      ["{PageUp}", "2026-01-28"],
    ];
    for (const [keys, expectedDay] of movements) {
      await user.keyboard(keys);
      await waitFor(() => expect(calendarDay(calendar, expectedDay)).toHaveFocus());
    }
    expect(screen.getByLabelText("From")).toHaveValue("2026-01-31 08:15:00");
    expect(onChange).not.toHaveBeenCalled();
    await user.keyboard("{Enter}");
    expect(screen.getByLabelText("From")).toHaveValue("2026-01-28 08:15:00");
    expect(screen.queryByRole("dialog", { name: "From Calendar" })).not.toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  });

  it("closes the calendar first on Escape and restores each parent trigger", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} />, { container: document.getElementById("scene-root")! });
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await openCalendar(user, "From");
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog", { name: "From Calendar" })).not.toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "Custom Time Window" })).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole("button", { name: "Choose From date" })).toHaveFocus());
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole("button", { name: "Custom time window" })).toHaveFocus());
    expect(onChange).not.toHaveBeenCalled();
  });

  it("switches endpoints with only one calendar open", async () => {
    const user = userEvent.setup();
    render(<TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await openCalendar(user, "From");
    await openCalendar(user, "To");
    expect(screen.queryByRole("dialog", { name: "From Calendar" })).not.toBeInTheDocument();
    expect(screen.getAllByRole("dialog")).toHaveLength(2);
    expect(screen.getByRole("dialog", { name: "Custom Time Window" })).toBeInTheDocument();
  });

  it("dismisses both calendar and parent on an outside click without applying", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    render(<><TimeRangePicker value={{ kind: "relative", minutes: 1440 }} onChange={onChange} /><button>Outside</button></>);
    await user.click(screen.getByRole("button", { name: "Custom time window" }));
    await openCalendar(user, "From");
    await user.click(screen.getByRole("button", { name: "Outside" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Outside" })).toHaveFocus();
    expect(onChange).not.toHaveBeenCalled();
  });
});
