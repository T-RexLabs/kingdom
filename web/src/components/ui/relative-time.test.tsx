import { act, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RelativeTime } from "./relative-time";

describe("RelativeTime", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("updates relative timestamps as time passes", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-06T10:01:35.000Z"));

    render(<RelativeTime iso="2026-03-06T10:01:00.000Z" />);

    expect(screen.getByText("35 s ago")).toBeInTheDocument();

    act(() => {
      vi.advanceTimersByTime(5_000);
    });

    expect(screen.getByText("40 s ago")).toBeInTheDocument();
  });
});
